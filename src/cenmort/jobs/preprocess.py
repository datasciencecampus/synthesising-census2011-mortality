"""Functions used to preprocess data."""

import os

import tqdm
from pydoop import hdfs
from pyspark.sql import functions as funcs

from cenmort import tools


def drop_rows_without_census_id(sdf):
    """Drop any row that is missing a census record ID.

    These records comprise primarily of all NAs, but also include a
    small number of registered deaths without a census record.
    """

    return sdf.filter(~funcs.isnull("census_person_id"))


def drop_rows_with_zero_date_cols(sdf):
    """Drop any row whose DOD day or month are listed as zero.

    There are only a handful of these rows and there is no available
    documentation to handle them, so we will ignore them.

    We do have to explicitly capture the records without DODs here.
    """

    sdf = sdf.filter(
        (funcs.isnull("doddy"))
        | ~((funcs.col("doddy") == 0) | (funcs.col("dodmt") == 0))
    )

    return sdf


def drop_extra_rows(sdf):
    """Drop any invalid row."""

    sdf = drop_rows_without_census_id(sdf)
    sdf = drop_rows_with_zero_date_cols(sdf)

    return sdf


def save_geography_lookup_tables(sdf):
    """Store the lookup tables between codes for MSOA, LA and region.

    There are two tables to collect: one for the census features and one
    for the mortality features. Not only are there additional LAs in the
    mortality data because of rezoning, there are UK deaths registered
    outside England and Wales.
    """

    census = (
        sdf.select("msoa_code", "la_code", "region_code")
        .distinct()
        .toPandas()
        .dropna()
    )

    mortal = sdf.select("laua", "rgn").distinct().toPandas().dropna()

    where = os.path.join(os.getenv("WORKSPACE"), "preprocessed")
    hdfs.mkdir(where)

    for name, lookup in zip(("census", "mortal"), (census, mortal)):
        with hdfs.open(os.path.join(where, f"{name}_lookup.csv"), "wt") as f:
            lookup.to_csv(f, index=False)

    return census, mortal


def drop_extra_columns(sdf):
    """Drop extra columns we do not need to synthesise.

    These include:

    - All ID columns. For obvious reasons. Note also that we do not
      synthesise household hierarchies because MST can't do that.
    - Age columns. We synthesise DOB, so we can work out age at census,
      and DOD gets us age at death.
    - Post-primary ICD-10 codes. Due to the enormous sample space
      (~7.6e47), we cannot synthesise this set of columns in a sensible
      way with MST.
    - Duplicates: DOB, language proficiency, marital status, sex. We
      take the census columns only. We also do not synthesise date of
      registration of death as it largely agrees with DOD.
    - Area codes. There are too many postcodes and LSOAs to fit MST. The
      country of birth is recorded on the census so we remove the copy
      from the mortality data. There are ~100k clashes but including
      both columns would likely result in even more clashes. To avoid
      clashes in geographical hierachies, we also only include the
      lowest level of geography for each column set: MSOA codes for
      census data and LA code for the mortality data. All higher levels
      of geography are reconstructed in post-processing via lookups.
    - Occupation group codes. The lowest level is granular enough and
      higher-level codes are formed by truncating the lower-level codes.
    - Economic columns. There are too many logical consistencies
      to worry about here, so we only synthesise economic activity,
      industry, and occupation. This means none are synthesised from the
      deaths register. Too complex.
    """

    ids = ["census_person_id", "household_id", "regdets", "ronunqid"]
    ages = ["age_census", "agec", "agecunit", "ageinyrs"]
    extra_icds = [f"fic10men{x}" for x in range(2, 16)]
    duplicates = [
        "deaths_marstat",
        "deaths_sex",
        "dobyr",
        "dobmt",
        "dobdy",
        "dor",
        "mainlangprf11",
    ]
    area_codes = [
        "ctrypob",
        "ctryr",
        "enumpc",
        "lsoa_code",
        "la_code",
        "region_code",
        "rgn",
    ]
    occupation = ["occpuk111", "occpuk113", "occtype"]
    economic = [
        "empstat",
        "indpuk",
        "nssec",
        "nsshuk11",
        "seccatdm",
        "soc2kdm",
    ]

    to_drop = (
        ids
        + ages
        + extra_icds
        + duplicates
        + area_codes
        + occupation
        + economic
    )

    return sdf.drop(*to_drop)


def separate_datasets(sdf):
    """Separate the dataset into census-only and linked rows."""

    mortal = sdf.filter(
        ~(
            funcs.isnull("doddy")
            | funcs.isnull("dodmt")
            | funcs.isnull("dodyr")
        )
    )

    mortality_columns = (
        "ceststay",
        "doddy",
        "dodmt",
        "dodyr",
        "fic10men1",
        "fic10und",
        "fimdth10",
        "laua",
        "postmort",
    )

    census = sdf.subtract(mortal).drop(*mortality_columns)

    return census, mortal


def fill_missing_values(sdf):
    """Fill in all missing values for the `pyspark.ml` tools."""

    sdf = sdf.na.fill("None")
    sdf = sdf.na.fill(-1)

    return sdf


def encode_all_columns(sdf, columns=None, distincts=None, partitions=None):
    """Encode all the columns, and track their mapping and sizes."""

    partitions = partitions or tools.get_partitions(sdf.count())
    columns = columns or sdf.columns
    distincts = distincts or {col: None for col in columns}
    extras = [col for col in sdf.columns if col not in columns]

    mappings, sizes = {}, {}
    for col in tqdm.tqdm(columns):
        sdf, mappings[col], sizes[col] = tools.encode_column(
            sdf, col, distincts[col]
        )

    indexed = []
    for col in columns:
        col_idx = col + "_idx"
        metadata = sdf.schema[col_idx].metadata
        metadata["ml_attr"]["name"] = col
        indexed.append(funcs.col(col_idx).alias(col, metadata=metadata))

    encoded = sdf.select(*indexed, *extras).repartition(partitions)

    return encoded, mappings, sizes


def save_outputs(sdf, mappings, sizes, dtypes, where):
    """Write the preprocessing outputs to file on HDFS."""

    hdfs.mkdir(where)

    tools.save_dict_as_json(sizes, os.path.join(where, "sizes.json"))
    tools.save_dict_as_json(mappings, os.path.join(where, "mappings.json"))
    tools.save_dict_as_json(dtypes, os.path.join(where, "dtypes.json"))

    sdf.write.parquet(os.path.join(where, "main"), mode="overwrite")
