"""Functions for generating synthetic data from an estimator.

Because the synthesis as-implemented relies on vectorised UDFs, most of
the heavy-lifting code has to remain in the job script. It's a real
shame but the current DAP framework makes it difficult to support
sharing `cenmort` to the workers.

As such, these functions are helpers, really.
"""

import os
import pickle

from pydoop import hdfs

from cenmort import tools


def calculate_chunks(spark, estimator, total, chunk):
    """Get the number of chunks to synthesise plus any extra rows."""

    nrows = estimator.nrows

    if total is not None:
        original_nrows = sum(
            spark.read.parquet(
                os.path.join("WORKSPACE"), "preprocessed", name, "main"
            )
            for name in ("census", "mortal")
        )

        prop = nrows / original_nrows
        nrows = round(prop * total)

    chunks, extra = divmod(nrows, chunk)

    return chunks, extra


def setup(spark, part, epsilon, estimator_seed, total, chunk):
    """Set up for synthesis."""

    root = tools.out_directory(epsilon, estimator_seed, part)

    total_name = "full" if total is None else total
    out = os.path.join(root, *map(str, (total_name, chunk)))

    with hdfs.open(os.path.join(root, "estimator.pkl"), "rb") as e:
        estimator = pickle.load(e)

    chunks, extra = calculate_chunks(spark, estimator, total, chunk)

    logger = tools.setup_logger(
        "synthesise", part, epsilon, estimator_seed, total_name, chunk
    )

    return out, estimator, chunks, extra, logger
