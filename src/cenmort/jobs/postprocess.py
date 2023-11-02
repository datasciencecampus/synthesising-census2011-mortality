"""Functions for post-processing the encoded synthetic data."""

import json
import os

import numpy as np
from pydoop import hdfs
from pyspark.sql import functions as funcs

from cenmort import tools


def load_resources_for_part(spark, part, root):
    """Load in the data and its metadata for a given part."""

    prep = os.path.join(os.getenv("WORKSPACE"), "preprocessed", part)
    mappings = tools.read_json_as_dict(os.path.join(prep, "mappings.json"))
    dtypes = tools.read_json_as_dict(os.path.join(prep, "dtypes.json"))

    filenames = [
        file
        for file in hdfs.ls(os.path.join(root, part), recursive=True)
        if file.endswith(".parquet")
    ]

    with hdfs.open(os.path.join(root, part, "decompressings.json"), "rt") as d:
        decompressings = json.load(d)

    data = spark.read.parquet(*filenames).checkpoint()

    return data, mappings, dtypes, decompressings


def setup(epsilon, estimator_seed, total, chunk):
    """Read in the data and set things up for post-processing."""

    root = tools.out_directory(epsilon, estimator_seed)
    spark = tools.start_session(checkpoints=os.path.join(root, "checkpoints"))

    total_name = "full" if total is None else total
    out = os.path.join(root, "processed", *map(str, (total_name, chunk)))
    hdfs.mkdir(out)

    logger = tools.setup_logger(
        "postprocess", epsilon, estimator_seed, total_name, chunk
    )

    logger.info("Loading mappings, data and metadata...")
    census = load_resources_for_part(spark, "census", root)
    mortal = load_resources_for_part(spark, "mortal", root)

    return spark, census, mortal, out, logger


def decompress_all_columns(
    data, decompressings, prng=np.random.default_rng(0)
):
    """Decompress every column in this part of the data."""

    for column, (code, values, labels) in decompressings.items():

        data = tools.decode_column(data, column, list(map(str, labels)))
        data = data.withColumn(
            column + "_out", funcs.col(column + "_out").cast("double")
        )

        condition = funcs.when(
            funcs.col(column + "_out") == int(code),
            funcs.round(
                funcs.rand(seed=int(prng.integers(len(values))))
                * (max(values) - min(values))
                + min(values),
                0,
            ),
        ).otherwise(data[column + "_out"])

        data = data.withColumn(column + "_out", condition)
        data = data.drop(column).withColumnRenamed(column + "_out", column)

    return data


def decode_and_recast_all_columns(data, mappings, dtypes, decompressings):
    """Decompress, decode and recast every column in the synthetic data.

    Note: we also repartition here on a uniformly distributed column
    (`msoa_code`) to redistribute the data mass.
    """

    data = decompress_all_columns(data, decompressings)

    columns = data.columns
    for col, labels in mappings.items():
        data = tools.decode_column(data, col, labels)

    decoded = data.select(
        [
            funcs.col(col + "_out").cast(dtypes[col]).alias(col)
            for col in columns
        ]
    ).repartition("msoa_code")

    return decoded


def reinstate_missing_values(sdf):
    """Replace all the encoded missing values with actual NAs."""

    diffs = [c for c in sdf.columns if c.endswith("_diff")]
    others = [c for c in sdf.columns if c not in diffs]

    reinstated = (
        sdf.replace(-1, None, others)
        .replace("None", None, others)
        .replace(-99999, None, diffs)
    )

    reinstated.checkpoint()

    return reinstated


def attach_date_columns(census, mortality):
    """For easier computation, we add actual date-type columns."""

    census, mortality = (
        data.withColumn(
            "dob",
            funcs.to_date(
                funcs.concat_ws("-", "dob_year", "dob_month", "dob_day"),
                format="yyyy-MM-dd",
            ),
        )
        for data in (census, mortality)
    )

    mortality = mortality.withColumn(
        "dod",
        funcs.to_date(
            funcs.concat_ws(
                "-",
                "dodyr",
                funcs.lpad("dodmt", 2, "0"),
                funcs.lpad("doddy", 2, "0"),
            ),
            format="yyyy-MM-dd",
        ),
    )

    return census, mortality


def get_age_column(data, name, start, end):
    """Calculate age given two date columns."""

    ages = (funcs.datediff(end, start) / 365.25).cast("int")

    if name == "age_census":
        ages = funcs.lpad(ages.cast("string"), 3, "0")

    if name == "ageinyrs":
        ages = ages.cast("double")

    return data.withColumn(name, ages)


def reconstruct_age_columns(census, mortality):
    """Calculate age at census and death from DOB and DOD."""

    census, mortality = (
        get_age_column(data, "age_census", "dob", funcs.lit("2011-03-27"))
        for data in (census, mortality)
    )

    mortality = get_age_column(mortality, "age_death", "dob", "dod")

    census.checkpoint()
    mortality.checkpoint()

    return census, mortality


def remove_date_columns(census, mortality):
    """We don't need the date columns, so get rid of them."""

    return census.drop("dob"), mortality.drop("dob", "dod")


def recombine_records(census, mortality):
    """Attach the census-only to the census-mortality records."""

    to_add = set(mortality.columns).difference(census.columns)
    census = census.select("*", *(funcs.lit(None).alias(c) for c in to_add))

    combined = census.unionByName(mortality)
    combined = combined.repartition(
        tools.get_partitions(combined.count()), "msoa_code"
    )
    combined.checkpoint()

    return combined


def load_lookup_tables(spark):
    """Load in the geography lookups."""

    prep = os.path.join(os.getenv("WORKSPACE"), "preprocessed")
    census = spark.read.csv(
        os.path.join(prep, "census_lookup.csv"), header=True
    )
    mortal = spark.read.csv(
        os.path.join(prep, "mortal_lookup.csv"), header=True
    )

    return census, mortal


def reconstruct_geographies(combined, spark):
    """Reattach higher geographies using the lookup table."""

    census_lookup, mortal_lookup = load_lookup_tables(spark)

    combined = combined.join(census_lookup, on="msoa_code", how="left")
    combined = combined.join(mortal_lookup, on="laua", how="left")

    combined.checkpoint()

    return combined


def reconstruct_occupation_hierarchies(combined):
    """Occupation hierarchies are substrings of occupation codes."""

    combined = combined.select(
        "*",
        funcs.col("occ").substr(1, 3).alias("occpuk111"),
        funcs.col("occ").substr(1, 1).alias("occpuk113"),
    )

    combined.checkpoint()

    return combined
