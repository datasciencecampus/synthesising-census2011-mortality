"""Life expectancy by ethnicity, age and sex from 2011 to 2019."""

import datetime
import os

import numpy as np
import pandas as pd
import tqdm
from pyspark.sql import DataFrame
from pyspark.sql import functions as funcs

from cenmort.jobs import preprocess


def remove_non_residents(data):
    """Filter out any records belonging to non-residents."""

    return data.filter(funcs.col("uresindpuk11") == 1)


def add_death_date(data):
    """Add a DOD column for figuring out state indicators later on."""

    data = data.withColumn(
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

    return data


def drop_unnecessary_columns(data):
    """Select only the columns required for this analysis."""

    return data.select(
        funcs.col("age_census").alias("age").cast("integer"),
        funcs.col("ethpuk11").alias("ethnicity"),
        "sex",
        "dod",
    )


def load_data(spark, which, out):
    """Load in and clean the data."""

    if which.lower() == "real":
        data = spark.read.table(
            "linked_2011_cen_dths.census_mortality_20211222"
        )
        data = preprocess.drop_rows_with_zero_date_cols(data)
        data = preprocess.drop_rows_without_census_id(data)

    elif which.lower().startswith("synth"):
        data = spark.read.parquet(os.path.join(out, "main"))

    else:
        raise ValueError(
            f"`which` must be `real` or `synth(etic)` not {which}"
        )

    data = remove_non_residents(data)
    data = add_death_date(data)
    data = drop_unnecessary_columns(data)
    data.cache()

    return data


def filter_previous_deaths(data, year):
    """Remove any records for people who died before the census year."""

    if isinstance(data, DataFrame):
        return data.filter(
            (funcs.col("dod") >= f"{year}-03-27") | (funcs.isnull("dod"))
        )

    return data.loc[data["dod"] >= datetime.date(year, 3, 27), :]


def add_death_indicator(data, year):
    """Add an indicator for whether an individual has died that year.

    In particular, we want to indicate whether an individual died during
    the census year in question. That is, on or after `year`-03-27 but
    before (`year` + 1)-03-27.
    """

    if isinstance(data, DataFrame):
        return data.withColumn(
            "state",
            funcs.when(funcs.col("dod") < f"{year + 1}-03-27", 1).otherwise(0),
        )

    data.loc[:, "state"] = (
        data["dod"] < datetime.date(year + 1, 3, 27)
    ).astype(int)

    return data


def process_year(data, year, indices):
    """Create the dataframe for a year's deaths."""

    data = filter_previous_deaths(data, year)
    data = add_death_indicator(data, year)

    if isinstance(data, DataFrame):
        counts = data.groupby(*indices).count().toPandas()
    else:
        counts = data.groupby(list(indices))["count"].sum().reset_index()

    counts.loc[:, "year"] = year
    if year > 2011:
        counts.loc[:, "age"] += 1

    return counts


def get_all_counts(data, indices=None):
    """Create the combined deaths dataframe for all years."""

    indices = indices or ("sex", "age", "ethnicity", "dod", "state")
    years = []
    for year in tqdm.trange(2011, 2020):
        data = process_year(data, year, indices)
        years.append(data.copy())

    indices = ["year", *(idx for idx in indices if idx != "dod")]
    combined = pd.concat(years, ignore_index=True)
    combined = combined.groupby(indices)["count"].sum().reset_index()

    return combined


def age_binner(age):
    """Bin a given age."""

    if age < 1:
        return 1, "<1"
    if age in range(1, 5):
        return 2, "1-4"
    if age >= 90:
        return 20, "90+"

    bin = age // 5
    lower = 5 * bin
    return bin + 2, f"{lower}-{lower + 4}"


def add_age_band(data):
    """Bin the ages into the standard groups."""

    data[["age_band_code", "age_band"]] = (
        data["age"].apply(age_binner).tolist()
    )
    return data


def summarise_age_bands(data, indices=None):
    """Summarise the population and death counts for age bands."""

    indices = indices or ("year", "sex", "ethnicity", "state")

    return (
        data.groupby([*indices, "age_band", "age_band_code"])["count"]
        .sum()
        .reset_index()
    )


def population_and_deaths(data, indices=None):
    """Get separate counts for population and deaths in each group."""

    indices = indices or (
        "year",
        "sex",
        "ethnicity",
        "age_band",
        "age_band_code",
    )

    population = (
        data.groupby(list(indices))["count"]
        .sum()
        .reset_index()
        .rename({"count": "size"}, axis=1)
    )
    deaths = (
        data[data["state"] == 1]
        .rename({"count": "deaths"}, axis=1)
        .drop("state", axis=1)
    )

    combined = pd.merge(population, deaths, how="left", on=indices)
    combined.loc[:, "deaths"] = combined["deaths"].fillna(0).astype(int)

    return combined


def decode_ethnicity(data, form="group"):
    """Replace ethnicity codes with their string values."""

    decoding = {}
    if form == "long":
        decoding = {
            "01": "White: English/Welsh/Scottish/Northern Irish/British",
            "02": "White: Irish",
            "03": "White: Gypsy or Irish Traveller",
            "04": "White: Other White",
            "05": "Mixed/multiple ethnic groups: White and Black Caribbean",
            "06": "Mixed/multiple ethnic groups: White and Black African",
            "07": "Mixed/multiple ethnic groups: White and Asian",
            "08": "Mixed/multiple ethnic groups: Other Mixed",
            "09": "Asian/Asian British: Indian",
            "10": "Asian/Asian British: Pakistani",
            "11": "Asian/Asian British: Bangladeshi",
            "12": "Asian/Asian British: Chinese",
            "13": "Asian/Asian British: Other",
            "14": "Black/African/Caribbean/Black British: African",
            "15": "Black/African/Caribbean/Black British: Caribbean",
            "16": "Black/African/Caribbean/Black British: Other Black",
            "17": "Other ethnic group: Arab",
            "18": "Other ethnic group: Any other ethnic group",
        }

    if form == "group":
        white = {f"0{i}": "White" for i in range(1, 5)}
        mixed = {f"0{i}": "Mixed" for i in range(5, 9)}
        asian = {
            "09": "Indian",
            "10": "Pakistani",
            "11": "Bangladeshi",
            "12": "Asian Other",
            "13": "Asian Other",
        }
        black = {
            "14": "Black African",
            "15": "Black Caribbean",
            "16": "Black Other",
        }
        other = {"17": "Other", "18": "Other"}
        decoding = {**white, **mixed, **asian, **black, **other}

    return data.replace({"ethnicity": decoding})


def create_overlapping_periods(data, indices=None):
    """Create three-year bins of populations and deaths."""

    groups = []
    for year in range(2011, 2018):
        group = data.loc[data["year"].isin(range(year, year + 3)), :]
        group.loc[:, "period"] = f"{year}-{year + 2}"
        groups.append(group.drop("year", axis=1))

    indices = indices or (
        "period",
        "sex",
        "age_band",
        "age_band_code",
        "ethnicity",
    )
    combined = (
        pd.concat(groups, ignore_index=True)
        .groupby(list(indices))[["size", "deaths"]]
        .sum()
        .reset_index()
    )

    return combined


def age_band_interval(band, mortality):
    """Get the width of an age band.

    The intervals for each age band are as follows:

    - `<1`: 1 year
    - `1-4`: 4 years
    - `90+`: `2 / m` years
    - all other bands: 5 years
    """

    if band == "<1":
        return 1
    if band == "1-4":
        return 4
    if band == "90+":
        return round(2 / mortality)
    return 5


def starting_size(group):
    """Get the starting population for each age band in the group."""

    start = group["age_band_code"].min()
    group = group.sort_values("age_band_code")

    group.loc[group["age_band_code"] == start, "start_size"] = 100_000

    for code in group["age_band_code"]:
        group.loc[group["age_band_code"] > start, "start_size"] = group[
            "prob_survive"
        ].shift(1) * group["start_size"].shift(1)

    return group


def death_estimate(group):
    """Get the estimated death count in each age band for a group."""

    end = group["age_band_code"].max()
    group = group.sort_values("age_band_code")

    group.loc[group["age_band_code"] == end, "deaths_band"] = group[
        "start_size"
    ]

    for _ in group["age_band_code"]:
        group.loc[group["age_band_code"] < end, "deaths_band"] = group[
            "start_size"
        ] - group["start_size"].shift(-1)

    return group


def person_years_lived(group):
    """Get the person-years lived in an age band, and cumulative sum."""

    end = group["age_band_code"].max()
    group = group.sort_values("age_band_code")

    group.loc[group["age_band_code"] == end, "person_years"] = (
        group["start_size"] / group["mortality"]
    )

    for _ in group["age_band_code"]:
        group.loc[group["age_band_code"] < end, "person_years"] = group[
            "interval"
        ] * (
            group["start_size"].shift(-1)
            + group["assumed_prop"] * group["deaths_band"]
        )

    group["total_person_years"] = group.sort_values(
        "age_band_code", ascending=False
    )["person_years"].cumsum()

    return group


def life_table_primaries(data):
    """Get the primary (simple) life table parameters.

    Following Python indexing syntax, we use 0 and -1 to indicate the
    first and final age bands in the parameter definitions below.

    - `m(x)`: mortality rate: `m(x) = deaths(x) / size(x)`
    - `a(x)`: assumed proportion of life spent in age band before death:
              * `a(x) = 0.1` if `x == "<1"`
              * `a(x) = 0.5` for other `x`
    - `i(x)`: interval, i.e. the width of each age band:
              * `a(x) = 1` if `x == "<1"`
              * `a(-1) = 2 / m(-1)`
              * `a(x) = 4` for other `x`
    - `q(x)`: probability of dying in an age band conditional on having
              survived to that band:
              * `q(-1) = 1`,
              * `q(x) = (i(x) * m(x)) / (1 + i(x) * (1 - a(x)) * m(x))`
                for other `x`
    - `p(x)`: probability of surviving from one age band into the next:
              `p(x) = 1 - q(x)`

    """

    data["mortality"] = data["deaths"] / data["size"]
    data["assumed_prop"] = np.where(data["age_band"] == "<1", 0.1, 0.5)
    data["interval"] = data.apply(
        lambda row: age_band_interval(row["age_band"], row["mortality"]),
        axis=1,
    )

    data["prob_death"] = (data["interval"] * data["mortality"]) / (
        1 + data["interval"] * (1 - data["assumed_prop"]) * data["mortality"]
    )
    data.loc[data["age_band_code"] == 20, "prob_death"] = 1
    data["prob_survive"] = 1 - data["prob_death"]

    return data


def group_secondaries(group):
    """Get the secondary (complex) life table parameters for a group."""

    return (
        group.pipe(starting_size).pipe(death_estimate).pipe(person_years_lived)
    )


def life_table_secondaries(data, indices=None):
    """Get the secondary (complex) life table parameters for all groups.

    Following Python indexing syntax, we use 0 and -1 to indicate the
    first and final age bands in the parameter definitions below.

    - `s(x)`: arbitrary starting population:
              * `s(0) = 100000`,
              * `s(x) = p(x - 1) * s(x - 1)` for `x > 0`
    - `d(x)`: number of people that die in the interval w.r.t. `s(x)`:
              * `d(-1) = s(-1)`,
              * `d(x) = s(x) - s(x + 1)` for other `x`
    - `l(x)`: number of person-years lived in an age interval:
              * `l(-1) = s(-1) / m(-1)`
              * `l(x) = i(x) * (s(x + 1) * a(x) * d(x))` for other `x`
    - `t(x)`: number of person-years lived from an age interval to the
              final age interval:
              * `t(-1) = l(-1)`
              * `t(x) = l(x) + l(x + 1)` for other `x`
    - `e(x)`: life expectancy: `e(x) = t(x) / s(x)`
    """

    indices = indices or ("ethnicity", "sex")
    data = (
        data.groupby(["period", *indices])
        .apply(group_secondaries)
        .reset_index(drop=True)
    )

    return data


def life_table(data, indices=None):
    """Create the life table parameters, including life expectancy.

    For each subpopulation group and each age band, `x`, we define two
    sets of parameters - primary and secondary - defined in their
    respective functions.
    """

    data = data.pipe(life_table_primaries).pipe(life_table_secondaries)
    data["life_expectancy"] = data["total_person_years"] / data["start_size"]

    return data


def prob_death_uncertainty(data):
    """Get variance of `q(x)` for each group and age band.

    For the final age band, the variance is a function of the observed
    death count and the calculated mortality rate:
    `var(q(-1)) = 4 / (deaths(-1) * m(-1) ** 2)`

    For all other x, it is slightly more complicated:
    `var(q(x)) = (
        (i(x)**2 * m(x) * (1 - a(x) * i(x) * m(x)))
        / (size(x) * (1 + (1 - a(x)) * i(x) * m(x)))
    )`

    Above, `size(x)` is the total observed count of the band `x`,
    whether they be alive or nay.
    """

    data.loc[data["age_band_code"] == 20, "prob_death_var"] = 4 / (
        data["deaths"] * data["mortality"] ** 2
    )
    data.loc[data["age_band_code"] != 20, "prob_death_var"] = (
        data["interval"] ** 2
        * data["mortality"]
        * (1 - data["assumed_prop"] * data["interval"] * data["mortality"])
    ) / (
        data["size"]
        * (
            1
            + data["interval"] * data["mortality"] * (1 - data["assumed_prop"])
        )
        ** 3
    )

    return data


def group_life_expectancy_uncertainty(group):
    """Get the preliminaries for variance of life expectancy.

    There are two preliminaries values, `P(x)` and `Q(x)`, defined as:

    - `P(-1) = var(q(-1)) * (l(-1) / 2) ** 2`
    - `P(x) = l(x) ** 2 * var(q(x)) * ((1 - a(x)) * i(x) + e(x + 1))`
      for other `x`
    - `Q(-1) = P(-1)`
    - `Q(x) = P(x) + Q(x + 1)` for other `x`
    """

    end = group["age_band_code"].max()
    group = group.sort_values("age_band_code")

    group.loc[group["age_band_code"] == end, "le_var_prelim"] = (
        group["prob_death_var"] * (group["start_size"] / 2) ** 2
    )

    for _ in group["age_band_code"]:
        group.loc[group["age_band_code"] < end, "le_var_prelim"] = (
            group["start_size"] ** 2
            * group["prob_death_var"]
            * (
                (1 - group["assumed_prop"]) * group["interval"]
                + group["life_expectancy"].shift(-1)
            )
            ** 2
        )

    group.loc[:, "cume_le_var_prelim"] = group.sort_values(
        "age_band_code", ascending=False
    )["le_var_prelim"].cumsum()

    return group


def life_expectancy_uncertainty(data, indices=None):
    """Get the variance of `e(x)` for each group and age band."""

    indices = indices or ("ethnicity", "sex")
    data = (
        data.groupby(["period", *indices])
        .apply(group_life_expectancy_uncertainty)
        .reset_index(drop=True)
    )

    data["life_expectancy_var"] = (
        data["cume_le_var_prelim"] / data["start_size"] ** 2
    )
    data["life_expectancy_std"] = data["life_expectancy_var"] ** 0.5

    data["life_expectancy_lower"] = (
        data["life_expectancy"] - 1.96 * data["life_expectancy_std"]
    )
    data["life_expectancy_upper"] = (
        data["life_expectancy"] + 1.96 * data["life_expectancy_std"]
    )

    return data


def estimate_with_uncertainty(data, indices=None):
    """Get the final life expectancy with uncertainty columns."""

    indices = indices or ("ethnicity", "sex")
    result = (
        data.pipe(life_table)
        .pipe(prob_death_uncertainty)
        .pipe(life_expectancy_uncertainty)[
            [
                "period",
                *indices,
                "age_band",
                "age_band_code",
                "size",
                "deaths",
                "life_expectancy",
                "life_expectancy_std",
                "life_expectancy_lower",
                "life_expectancy_upper",
            ]
        ]
    )

    return result
