"""Script for preprocessing the data and generating the domain file."""

import os

from cenmort import tools
from cenmort.jobs import preprocess


def main():
    """Preprocess the data and save the outputs."""

    logger = tools.setup_logger("preprocess")

    logger.info("Setting up Spark session...")
    spark = tools.start_session(executor_memory="12g")

    logger.info("Reading data...")
    sdf = spark.read.table("linked_2011_cen_dths.census_mortality_20211222")

    logger.info("Beginning preprocessing...")
    sdf = preprocess.drop_extra_rows(sdf)
    _ = preprocess.save_geography_lookup_tables(sdf)

    sdf = preprocess.drop_extra_columns(sdf)

    census, mortal = preprocess.separate_datasets(sdf)

    for name, data in zip(("census", "mortal"), (census, mortal)):
        logger.info(f"Processing {name}...")
        data = preprocess.fill_missing_values(data)
        data.cache()

        dtypes = dict(data.dtypes)
        encoded, mappings, sizes = preprocess.encode_all_columns(data)

        logger.info("Saving outputs...")
        where = os.path.join(os.getenv("WORKSPACE"), "preprocessed", name)
        preprocess.save_outputs(encoded, mappings, sizes, dtypes, where)
        data.unpersist()

    logger.info("Preprocessing complete!")

    tools.end_session(spark)


if __name__ == "__main__":
    main()
