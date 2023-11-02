"""Script for running and comparing life expectancy analyses."""

import os

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sbn
from pydoop import hdfs
from pyspark.sql import functions as funcs

from cenmort import parser, tools
from cenmort.utility import life_expectancy as life


def get_results(spark, out, form="group"):
    """Get the life table results for both datasets."""

    results = []
    for which in ("real", "synth"):
        data = life.load_data(spark, which, out).filter(
            funcs.col("ethpuk11") != "XX"
        )
        result = (
            life.get_all_counts(data)
            .pipe(life.decode_ethnicity, form=form)
            .pipe(life.add_age_band)
            .pipe(life.summarise_age_bands)
            .pipe(life.population_and_deaths)
            .rename({"year": "period"}, axis=1)
            .replace({"sex": {"1": "Male", "2": "Female"}})
            .pipe(life.estimate_with_uncertainty)
        )
        results.append(result)
        data.unpersist()

    combined = pd.concat(
        results, keys=["real", "synthetic"], names=["origin"]
    ).reset_index(level=0)

    return combined


def select_table_data(data, *columns, period="2011-2013"):
    """Select the from-birth life expectancy results."""

    return data.loc[
        (data["period"] == period) & (data["age_band"] == "<1"),
        ["origin", *columns],
    ]


def life_expectancy_gap(data, column, other, name=None):
    """Get the `column`-gap for life expectancy and the table order."""

    gaps = (
        data.groupby(["origin", other])["life_expectancy"]
        .apply(lambda x: x.max() - x.min())
        .reset_index()
    )

    name = name or f"{column}_gap"
    gaps[column] = name
    order = [*sorted(data[column].unique()), name]

    return gaps, order


def life_expectancy_table(
    data,
    period="2011-2013",
    x="sex",
    y="ethnicity",
    xname="Sex gap",
    yname="Ethnic group gap",
):
    """Create a comparison table for Table 1 from the HAPI analysis."""

    result = select_table_data(data, x, y, "life_expectancy", period=period)

    xgap, xorder = life_expectancy_gap(result, x, y, xname)
    ygap, yorder = life_expectancy_gap(result, y, x, yname)

    table = (
        pd.concat((result, xgap, ygap), ignore_index=True)
        .groupby([x, y])["life_expectancy"]
        .apply(lambda x: x.diff().iloc[-1] / x.iloc[0])
        .reset_index()
        .pivot(y, x, "life_expectancy")
        .reindex(xorder, axis=1)
        .reindex(yorder, axis=0)
    )

    return table


def group_cohens_d(group):
    """Get Cohen's d for real and synthetic life expectancy in group."""

    group = group.sort_values("origin")
    mr, ms = group["life_expectancy"]
    dr, ds = group["size"] - 1
    sr, ss = group["life_expectancy_std"]

    denominator = ((dr * sr**2 + ds * ss**2) / (dr + ds)) ** 0.5

    return abs(mr - ms) / denominator


def cohens_d(data, indices=("ethnicity", "sex"), period="2011-2013"):
    """Get Cohen's d for life expectancy estimates across all groups."""

    result = select_table_data(
        data,
        *indices,
        "life_expectancy",
        "life_expectancy_std",
        "size",
        period=period,
    )

    return (
        result.groupby(list(indices))
        .apply(group_cohens_d)
        .rename("cohens_d")
        .reset_index()
        .pivot(*indices, "cohens_d")
    )


def table_plot(
    table, figsize=(4, 5), label="Relative change", filename=None, **kwargs
):
    """Create heatmap from a table comparing real and synthetic data."""

    kwargs = {
        "annot": True,
        "fmt": ".3f",
        "linewidth": 1,
        "cmap": "crest",
        **kwargs,
    }
    sbn.set_style("ticks")

    _, ax = plt.subplots(dpi=300, figsize=figsize)
    ax = sbn.heatmap(data=table, ax=ax, **kwargs)

    ax.collections[0].colorbar.set_label(label, rotation=270, labelpad=10)
    ax.set(xlabel="", ylabel="")
    ax.xaxis.tick_top()

    plt.tight_layout()

    if filename is not None:
        plt.savefig(filename, dpi=300, bbox_inches="tight")

    return ax


def main(epsilon, estimator_seed, total, chunk):
    """Full pipeline for life expectancy analysis comparison."""

    total_name = "full" if total is None else total
    out = tools.out_directory(
        epsilon, estimator_seed, "processed", total_name, chunk
    )
    results = os.path.join(out, "results")
    hdfs.mkdir(results)

    spark = tools.start_session()

    combined = get_results(spark, out)
    path = os.path.join(results, "life_expectancy_table.csv")
    with hdfs.open(path, "wt") as f:
        combined.to_csv(f, index=False)

    table = life_expectancy_table(combined, period=2011)
    path = os.path.join(results, "life_expectancy_relative_change.csv")
    with hdfs.open(path, "wt") as f:
        table.to_csv(f)

    path = os.path.join(results, "life_expectancy_relative_change.png")
    with hdfs.open(path, "wb") as f:
        vmax = table.abs().max().max()
        cmap = sbn.diverging_palette(20, 220, as_cmap=True)
        _ = table_plot(table, vmin=-vmax, vmax=vmax, cmap=cmap, filename=f)

    cohens = cohens_d(combined, period=2011)
    path = os.path.join(results, "life_expectancy_cohens_d.csv")
    with hdfs.open(path, "wt") as f:
        cohens.to_csv(f)

    path = os.path.join(results, "life_expectancy_cohens_d.png")
    with hdfs.open(path, "wb") as f:
        _ = table_plot(
            cohens, label="Cohen's $d$", filename=f, vmin=0, fmt=".1f"
        )

    tools.end_session(spark)


if __name__ == "__main__":
    args = vars(parser.postprocess())
    main(**args)
