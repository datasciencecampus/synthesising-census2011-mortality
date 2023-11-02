"""Script for synthesising the data from a pickled model."""

import os

import numpy as np
import tqdm
from pyspark.sql import functions as funcs
from pyspark.sql.window import Window

from cenmort import parser, tools
from cenmort.jobs import synthesise


def column(counts, total, prng):
    """Synthesise a single column based on the marginal counts."""

    counts *= total / counts.sum()
    frac, integ = np.modf(counts)
    integ = integ.astype(int)
    extra = total - integ.sum()

    if extra > 0:
        idx = prng.choice(counts.size, extra, False, frac / frac.sum())
        integ[idx] += 1

    vals = np.repeat(np.arange(counts.size), integ)
    prng.shuffle(vals)

    return vals.astype(float)


def independent_column(marg, total, prng, col, spark):
    """Create a single column dataframe (with an index column)."""

    vals = column(marg, total, prng).reshape(-1, 1).tolist()
    sdf = spark.createDataFrame(vals, schema=[col])
    sdf = sdf.withColumn(
        "idx", funcs.row_number().over(Window.orderBy(funcs.lit(1)))
    )

    return sdf


def first_column(estimator, col, total, prng, spark):
    """Synthesise the first column."""

    marg = estimator.project([col]).datavector(flatten=False)
    sdf = independent_column(marg, total, prng, col, spark)

    return sdf


def process(pair):
    """Process an existing and new data pair."""

    existing, new = pair
    return dict(existing.asDict().items() + [("new_col", new)])


def synthesise_chunk(spark, out, estimator, seed, rows, logger):
    """Perform a single round of synthesis."""

    prng = np.random.default_rng(seed)
    cliques = [set(clique) for clique in estimator.cliques]
    order = estimator.elimination_order[::-1]
    col = order[0]

    logger.info(f"Synthesising first column: {col}")
    sdf = first_column(estimator, col, rows, prng, spark)
    used = [col]

    for col in order[1:]:

        relevant = [clique for clique in cliques if col in clique]
        proj = tuple(set.union(*relevant).intersection(used))
        logger.info(f"Synthesising {col} with {proj}")

        used.append(col)
        marg = estimator.project(proj + (col,)).datavector(flatten=False)
        schema = "idx integer," + ", ".join((c + " double" for c in used))

        @funcs.pandas_udf(schema, funcs.PandasUDFType.GROUPED_MAP)
        def synthesise_group(group):
            """Sparkly `mbi.GraphicalModel.synthetic_data.foo()`."""

            idx = tuple(group[list(proj)].iloc[0].astype(int))
            counts = marg[idx]
            group[col] = column(
                marg[idx],
                group.shape[0],
                np.random.default_rng(seed + int(sum(counts))),
            )

            return group

        if len(proj) >= 1:
            sdf = sdf.groupby(list(proj)).apply(synthesise_group)
        else:
            new = independent_column(marg, rows, prng, col, spark)
            sdf = sdf.join(new, "idx")

    logger.info("Saving synthetic data...")
    tools.save_data(
        sdf.drop("idx"), rows, out, os.path.join("encoded", str(seed))
    )

    logger.info("Done.")


def synthesise_part_of_data(part, epsilon, estimator_seed, total, chunk):
    """Perform a batch of synthesis runs for a part of the data."""

    spark = tools.start_session(executor_memory="16gb")

    out, estimator, chunks, extra, logger = synthesise.setup(
        spark, part, epsilon, estimator_seed, total, chunk
    )

    for seed in tqdm.trange(chunks + 1):
        rows = chunk if seed < chunks else extra
        logger.info(
            f"\n\nSYNTHESISING CHUNK {seed}/{chunks} WITH {rows:,} ROWS\n"
        )
        synthesise_chunk(spark, out, estimator, seed, rows, logger)

    tools.end_session(spark)


def main(epsilon, estimator_seed, total, chunk):
    """Synthesise both parts of the data."""

    for part in ("census", "mortal"):
        synthesise_part_of_data(part, epsilon, estimator_seed, total, chunk)


if __name__ == "__main__":
    args = parser.synthesise()
    main(**vars(args))
