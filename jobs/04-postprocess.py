"""Script for postprocessing the synthesised data."""

import calendar

import numpy as np
from pyspark.sql import functions as funcs

from cenmort import parser, tools
from cenmort.jobs import postprocess


def fix_invalid_dobs(group):
    """Randomly sample invalid dates from the year-month pair.

    For instance, 29 Feb only occurs on leap years and 31 Sep never
    exists. These would be replaced with integer samples drawn from
    {1,...,28} and {1,...,30}, respectively.

    This function is to be applied as a vectorised UDF after grouping
    the data by the year and month of the DOB.
    """

    year, month = map(int, group[["dob_year", "dob_month"]].iloc[0])
    _, days = calendar.monthrange(year, month)

    invalid = group["dob_day"].astype(float) > days
    prng = np.random.default_rng(year + month + sum(invalid))

    group.loc[invalid, "dob_day"] = [
        f"{x + 1:02}" for x in prng.integers(days, size=sum(invalid))
    ]

    return group


def fix_invalid_dods(group):
    """Equivalent of `fix_invalid_dobs` but for DODs.

    A separate function is required to avoid confusion with global
    variables. Again, this function should only be used as a vectorised
    UDF after grouping by the year and month of the DOD.

    Note that the elements of this group do not need to be recast or
    reformatted since they are stored as integers.
    """

    year, month = group[["dodyr", "dodmt"]].iloc[0]
    _, days = calendar.monthrange(year, month)

    invalid = group["doddy"] > days
    prng = np.random.default_rng(year + month + sum(invalid))

    group.loc[invalid, "doddy"] = prng.integers(days, size=sum(invalid)) + 1

    return group


def fix_all_invalid_dates(census, mortality):
    """Fix DOB and DOD column-sets to only include valid dates."""

    census_schema, mortal_schema = (
        ", ".join(" ".join(pair) for pair in data.dtypes)
        for data in (census, mortality)
    )

    census_dobs, mortal_dobs, mortal_dods = (
        funcs.pandas_udf(fixer, schema, funcs.PandasUDFType.GROUPED_MAP)
        for fixer, schema in zip(
            (fix_invalid_dobs, fix_invalid_dobs, fix_invalid_dods),
            (census_schema, mortal_schema, mortal_schema),
        )
    )

    census = census.groupby("dob_year", "dob_month").apply(census_dobs)
    mortality = mortality.groupby("dob_year", "dob_month").apply(mortal_dobs)
    mortality = mortality.groupby("dodyr", "dodmt").apply(mortal_dods)

    census.checkpoint()
    mortality.checkpoint()

    return census, mortality


def main(epsilon, estimator_seed, total, chunk):
    """Postprocess and recombine the data, and then write it to file."""

    spark, census_info, mortality_info, out, logger = postprocess.setup(
        epsilon, estimator_seed, total, chunk
    )

    logger.info("Decompressing, decoding and recasting data...")
    census = postprocess.decode_and_recast_all_columns(*census_info)
    mortality = postprocess.decode_and_recast_all_columns(*mortality_info)

    logger.info("Putting missing values back...")
    census = postprocess.reinstate_missing_values(census)
    mortality = postprocess.reinstate_missing_values(mortality)

    logger.info("Fixing invalid dates...")
    census, mortality = fix_all_invalid_dates(census, mortality)

    logger.info("Reconstructing age columns...")
    census, mortality = postprocess.attach_date_columns(census, mortality)
    census, mortality = postprocess.reconstruct_age_columns(census, mortality)
    census, mortality = postprocess.remove_date_columns(census, mortality)

    logger.info("Recombining records...")
    combined = postprocess.recombine_records(census, mortality)

    logger.info("Reconstructing hierarchies...")
    combined = postprocess.reconstruct_geographies(combined, spark)
    combined = postprocess.reconstruct_occupation_hierarchies(combined)

    logger.info("Writing data to file...")
    tools.save_data(combined, total, out, "main")

    tools.end_session(spark)
    logger.info("Done.")


if __name__ == "__main__":
    args = parser.postprocess()
    main(**vars(args))
