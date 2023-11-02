import datetime
import itertools
import json
import logging
import math
import os
import pathlib

import pandas as pd
from orderedset import OrderedSet
from pydoop import hdfs
from pyspark.ml.feature import IndexToString, StringIndexer
from pyspark.sql import SparkSession


def setup_logger(name, *args, home=os.getenv("CENMORT_HOME")):
    """Create a logger for a job script run."""

    where = pathlib.Path(home) / "logs" / name
    where.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    args = list(map(str, args))
    filename = "_".join((timestamp, *args)) + ".log"

    logging.basicConfig(
        filename=where / filename,
        format="%(asctime)s - %(message)s",
        level=logging.INFO,
    )

    logger = logging.getLogger()
    logger.info(": ".join((name, ", ".join(args))))

    return logger


def out_directory(*args, make=False):
    """Get (and optionally create) an output directory on HDFS.

    Out directory structure is `epsilon/estimator_seed/rows/` but not
    all are required.
    """

    out = os.path.join(os.getenv("WORKSPACE"), "out", *map(str, args))

    if make:
        hdfs.mkdir(out)

    return out


def get_partitions(rows):
    """A rough estimate to get <100MB partitions where possible."""

    return 2 * math.ceil(rows * 3.5e-7)


def save_data(data, nrows, out, name):
    """Repartition and then save the data."""

    nrows = data.count() if nrows is None else nrows
    parts = get_partitions(nrows)
    data = data.repartition(parts, "msoa_code")
    data.write.parquet(os.path.join(out, name), mode="overwrite")


def start_session(
    name="cenmort",
    driver_memory="20g",
    driver_max_result="8g",
    executor_memory="8g",
    executor_memory_overhead="2g",
    executor_cores=5,
    max_executors=16,
    max_message_size="128",
    shuffle_partitions=200,
    progress=False,
    checkpoints=None,
):
    """Start up a session using the DAPCATS xlarge defaults."""

    spark = (
        SparkSession.builder.appName(name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.driver.maxResultSize", driver_max_result)
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.cores", executor_cores)
        .config("spark.yarn.executor.memoryOverhead", executor_memory_overhead)
        .config("spark.dynamicAllocation.maxExecutors", max_executors)
        .config("spark.rpc.message.maxSize", max_message_size)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT", 1)
        .config("spark.workerEnv.ARROW_PRE_0_15_IPC_FORMAT", 1)
        .config("spark.ui.showConsoleProgress", progress)
        .enableHiveSupport()
        .getOrCreate()
    )

    session = f"{os.getenv('CDSW_ENGINE_ID')}.{os.getenv('CDSW_DOMAIN')}"
    print(f"http://spark-{session}")

    if checkpoints:
        spark.sparkContext.setCheckpointDir(checkpoints)

    return spark


def end_session(spark):
    """Stop the session and clean up any of its checkpoints."""

    try:
        checkpoints = spark._jsc.sc().getCheckpointDir().get()
        hdfs.rm(checkpoints, recursive=True)
    except Exception:
        pass

    spark.stop()


def save_dict_as_json(dictionary, path):
    """Save a dictionary to HDFS as a JSON file."""

    with hdfs.open(path, "wt") as f:
        json.dump(dictionary, f)


def read_json_as_dict(path):
    """Read a JSON file on HDFS as a dictionary."""

    with hdfs.open(path, "rt") as f:
        dictionary = json.load(f)

    return dictionary


def encode_column(sdf, name, distinct=None, order="frequencyDesc"):
    """Encode the given column. Also track its mapping and size.

    Note that the mapping is given as a list where the index of an entry
    indicates the encoded value."""

    distinct = [] if distinct is None else distinct

    indexer = StringIndexer(
        inputCol=name, outputCol=name + "_idx", stringOrderType=order
    ).fit(sdf)

    mapping = list(OrderedSet(indexer.labels) | distinct)
    encoded = indexer.transform(sdf)

    return encoded, mapping, len(mapping)


def decode_column(sdf, name, labels):
    """Replace the encoded column values with the originals."""

    decoder = IndexToString(
        inputCol=name, outputCol=name + "_out", labels=labels
    )

    decoded = decoder.transform(sdf)

    return decoded


def _get_full_frame(columns, domain):
    """Make a dataframe with all the value sets in `domain[columns]`."""

    value_sets = itertools.product(*(range(domain[col]) for col in columns))
    full = pd.DataFrame(value_sets, columns=columns)

    return full


def calculate_marginal(data, columns, domain):
    """Get the `len(columns)`-way counts given some data and domain."""

    seen = data.groupBy(*columns).count().toPandas()
    full = _get_full_frame(columns, domain)

    counts = (
        pd.merge(seen, full, how="outer", on=columns)
        .fillna(0)
        .sort_values(list(columns), axis=0)
    )

    return counts["count"].values.flatten()
