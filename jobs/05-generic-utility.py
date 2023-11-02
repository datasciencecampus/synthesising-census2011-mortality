"""Script for generic utility analysis."""

import itertools
import os
import pickle

import matplotlib.pyplot as plt
import seaborn as sbn
from matplotlib.patches import Rectangle
from pydoop import hdfs
from pyspark.sql import functions as funcs

from cenmort import parser, tools
from cenmort.jobs import postprocess, preprocess
from cenmort.utility import generic


def create_dates_for_real(real):
    """Attach DOB and DOD columns to the real dataset."""

    real = real.withColumn(
        "dob",
        funcs.to_date(
            funcs.concat_ws("-", "dob_year", "dob_month", "dob_day"),
            format="yyyy-MM-dd",
        ),
    )

    real = real.withColumn(
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

    return real


def load_data(spark, out):
    """Load in the real and synthetic datasets."""

    real = spark.read.table("linked_2011_cen_dths.census_mortality_20211222")
    real = preprocess.drop_extra_rows(real)
    real = create_dates_for_real(real)
    real = postprocess.get_age_column(real, "age_death", "dob", "dod")
    real = real.drop("dob", "dod").checkpoint()

    synth = (
        spark.read.parquet(os.path.join(out, "main"))
        .repartition("msoa_code")
        .checkpoint()
    )

    return real, synth


def save_figure(filename):
    """Save the figure if given a filename."""

    if filename is not None:
        plt.tight_layout()
        plt.savefig(filename, dpi=300, bbox_inches="tight")


def univariate_plot(results, column, order=None, filename=None):
    """Plot a column from the univariate results."""

    if isinstance(order, str):
        order = results.sort_values(order, ascending=False)["column"]

    _, ax = plt.subplots(dpi=300, figsize=(5, 10))

    ax = sbn.barplot(
        data=results,
        x=column,
        y="column",
        hue=f"{column}_metric",
        dodge=False,
        order=order,
        ax=ax,
    )

    ax.set(xlabel=column.title(), ylabel=None)

    legend = ax.legend(loc="upper center", ncol=2, bbox_to_anchor=(0.5, -0.06))
    legend.set_title(None)

    save_figure(filename)

    return ax


def load_estimators(epsilon, estimator_seed):
    """Load in both estimators."""

    estimators = []
    for part in ("census", "mortal"):
        where = tools.out_directory(epsilon, estimator_seed, part)
        with hdfs.open(os.path.join(where, "estimator.pkl"), "rb") as e:
            estimator = pickle.load(e)

        estimators.append(estimator)

    return estimators


def get_pairwise_plot_order(census_estimator, mortality_estimator, synth):
    """Arrange columns by synthesis order followed by the extras."""

    positions = {}
    for col in mortality_estimator.domain:
        score = mortality_estimator.elimination_order.index(col)
        if col in census_estimator.domain:
            score += census_estimator.elimination_order.index(col)

        positions[col] = score

    elimination = sorted(
        mortality_estimator.domain, key=positions.get, reverse=True
    )
    columns = synth.columns
    order = [
        *filter(lambda x: x in set(columns), elimination),
        *filter(lambda x: x not in set(elimination), columns),
    ]

    return order


def get_cliques_to_highlight(census_estimator, mortality_estimator, synth):
    """Get a set of two-way cliques to highlight in a pairwise plot."""

    cliques = {*census_estimator.cliques, *mortality_estimator.cliques}
    pairs = set()
    for clique in cliques:
        if not set(clique).issubset(synth.columns):
            continue
        elif len(clique) > 2:
            for pair in itertools.combinations(clique, 2):
                pairs.add(pair)
        else:
            pairs.add(clique)

    return pairs


def highlight_cliques(ax, pairs, order):
    """Add patches to highlight selected two-way cliques."""

    for a, b in pairs:
        x, y = order.index(a), order.index(b)
        ax.add_patch(Rectangle((x, y), 1, 1, fill=False, edgecolor="w", lw=1))
        ax.add_patch(Rectangle((y, x), 1, 1, fill=False, edgecolor="w", lw=1))

    return ax


def pairwise_plot(pivot, order=None, pairs=None, filename=None):
    """Make a heatmap of the pairwise trend comparisons."""

    _, ax = plt.subplots(dpi=300, figsize=(13, 11))

    if order is not None:
        pivot = pivot.reindex(order, axis=0).reindex(order, axis=1)

    ax = sbn.heatmap(
        pivot,
        cmap="crest",
        linewidth=0.1,
        linecolor="white",
        xticklabels=1,
        yticklabels=1,
        ax=ax,
    )

    if pairs is not None:
        order = list(pivot.index) if order is None else order
        ax = highlight_cliques(ax, pairs, order)

    ax.set(xlabel=None, ylabel=None)

    save_figure(filename)

    return ax


def specks_plot(results, filename=None):
    """Make a boxplot of the SPECKS results."""

    _, ax = plt.subplots(dpi=300, figsize=(5, 2))

    ax = sbn.boxplot(data=results, x="specks", palette="crest", ax=ax)
    ax.set(
        xlabel="Indistinguishability ($1 - SPECKS$)", xlim=(0, 1), yticks=[]
    )

    save_figure(filename)

    return ax


def main(epsilon, estimator_seed, total, chunk):
    """Perform the generic utility analysis for a synthetic dataset."""

    sbn.set()
    sbn.set_style("ticks", rc={"font.sans-serif": "Roboto"})
    sbn.set_palette("colorblind")

    total_name = "full" if total is None else total
    out = tools.out_directory(
        epsilon, estimator_seed, "processed", total_name, chunk
    )
    logger = tools.setup_logger(
        "generic", epsilon, estimator_seed, total_name, chunk
    )
    spark = tools.start_session(checkpoints=os.path.join(out, "checkpoints"))
    results = os.path.join(out, "results")
    hdfs.mkdir(results)

    logger.info("Reading data...")
    real, synth = load_data(spark, out)

    logger.info("Univariate summary...")
    univariate = generic.univariate_summary(real, synth)
    with hdfs.open(os.path.join(results, "univariate.csv"), "wt") as f:
        univariate.to_csv(f, index=False)

    for column in ("similarity", "coverage"):
        path = os.path.join(results, f"{column}.png")
        with hdfs.open(path, "wb") as f:
            _ = univariate_plot(univariate, column, order=column, filename=f)

    logger.info("Pairwise summary...")
    pairwise = generic.pairwise_summary(real, synth)
    with hdfs.open(os.path.join(results, "pairwise.csv"), "wt") as f:
        pairwise.to_csv(f, index=False)

    estimators = load_estimators(epsilon, estimator_seed)
    order = get_pairwise_plot_order(*estimators, synth)
    pairs = get_cliques_to_highlight(*estimators, synth)
    with hdfs.open(os.path.join(results, "pairwise.png"), "wb") as f:
        _ = pairwise_plot(
            pairwise.pivot("a", "b", "score"),
            order=order,
            pairs=pairs,
            filename=f,
        )

    logger.info("Distinguishability analysis...")
    specks = generic.specks(real, synth, precision=5, seeds=20)
    with hdfs.open(os.path.join(results, "specks.csv"), "wt") as f:
        specks.to_csv(f)
    with hdfs.open(os.path.join(results, "specks.png"), "wb") as f:
        _ = specks_plot(specks, filename=f)

    tools.end_session(spark)
    logger.info("Done.")


if __name__ == "__main__":
    args = vars(parser.postprocess())
    main(**args)
