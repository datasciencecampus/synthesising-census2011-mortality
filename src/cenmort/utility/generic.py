"""Generic utility measures."""

import itertools
import math

import numpy as np
import pandas as pd
import tqdm
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import Bucketizer, VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql import functions as funcs
from pyspark.sql.types import DoubleType

from cenmort.jobs import preprocess


def normalised_counts(data, column):
    """Get counts of `column` in `data` expressed as probabilities."""

    if isinstance(column, str):
        column = [column]

    counts = data.groupby(*column).count().toPandas()
    counts["prob"] = counts["count"] / counts["count"].sum()

    return counts.set_index(column)["prob"]


def tv_complement(real, synth, column):
    r"""Calculate Total Variation complement for a column.

    The Total Variation (TV) distance compares the differences between
    two probability distributions, :math:`R` and :math:`S`, defined over
    the same index set, :math:`I`, such that:

    .. math::
        tv(R, S) = \frac{1}{2} \sum_{i \in I} \abs{R_i - S_i}

    The complement of TV is given as :math:`1 - tv(R, S)`, so that
    higher scores indicate higher quality. A score of 0 indicates no
    similarity, while a score of 1 indicates absolute harmony.
    """

    rprob, sprob = (normalised_counts(data, column) for data in (real, synth))

    combined = pd.merge(rprob, sprob, on=column, how="outer").fillna(0)

    return 1 - combined.diff(axis=1).iloc[:, 1].abs().sum() / 2


def category_coverage(real, synth, column):
    """Calculate category coverage for a column.

    We can calculate coverage from two category sets by finding the size
    of their intersection. In our case, the `synth` set will always be a
    subset of the `real` set, improper or otherwise.

    A value of 0 means no categories were recovered (disjoint indices),
    while 1 means every category was covered (identical indices).
    """

    rcats, scats = (
        data.select(column).distinct().toPandas().set_index(column)
        for data in (real, synth)
    )

    return rcats.index.isin(scats.index).mean()


def empirical_cdf(data, column, precision):
    """Get the empirical CDF for the (rounded) data."""

    if precision is not None:
        data = data.withColumn(
            column, funcs.round(funcs.col(column), precision)
        )

    counts = (
        data.groupby(column).count().toPandas().set_index(column).sort_index()
    )
    counts["ecdf"] = counts["count"].cumsum() / counts["count"].sum()
    cdf = counts.drop("count", axis=1)

    return cdf


def ks_complement(real, synth, column, precision=None):
    r"""Calculate the Kolmogorov-Smirnov complement for a column.

    The Kolmogorov-Smirnov (KS) distance compares the differences
    between two (sampled) probability distributions, :math:`R` and
    :math:`S`, by finding the maximum difference between their empirical
    cumulative distribution functions, denoted :math:`F` and :math:`G`
    respectively:

    .. math::
        ks(R, S) = \sup_{x \in R \cup S} \abs{F(x) - G(x)}

    The complement of KS is given as :math:`1 - ks(R, S)`, so that
    higher scores indicate higher quality. A score of 0 indicates no
    similarity, while a score of 1 indicates absolute harmony.

    Note
    ----
    To reduce the size of the problem at hand, an optional `precision`
    can be used to round numeric values to `precision` decimal places.
    """

    rcdf, scdf = (
        empirical_cdf(data, column, precision) for data in (real, synth)
    )

    combined = (
        pd.merge(rcdf, scdf, on=column, how="outer", sort=True)
        .ffill()
        .fillna(0)
    )

    return 1 - combined.diff(axis=1).iloc[:, 1].abs().max()


def range_coverage(real, synth, column):
    r"""Calculate range coverage for a column.

    Range coverage determines how close (from within) the synthetic
    values, :math:`S`, lie to the real values, :math:`R`, such that:

    .. math::
        rc(R, S) = \max \left(
            1 - \left[
                \max \left(
                    \frac{\min S - \min R}{\max R - \min R}, 0
                \right) +
                \max \left(
                    \frac{\max R - \max S}{\max R - \min R}, 0
                \right)
            \right], 0
        \right)

    Note
    ----
    A score of zero indicates very poor coverage, and not necessarily
    disjoint ranges. Note also that this score does not penalise when
    the synthetic range extends beyond the real range.
    """

    (rmin, rmax), (smin, smax) = (
        data.agg(funcs.min(column), funcs.max(column)).collect()[0]
        for data in (real, synth)
    )

    denom = rmax - rmin
    complement = max((smin - rmin) / denom, 0) + max((rmax - smax) / denom, 0)

    return max(1 - complement, 0)


def summarise_column(real, synth, column, dtype, precision):
    """Calculate coverage and similarity of a column.

    For continuous features, use range coverage and the
    Kolmogorov-Smirnov distance complement, and use category coverage
    and the complement of Total Variation Distance for categorical
    columns.
    """

    if dtype in ("bigint", "double", "integer"):
        similarity = ks_complement(real, synth, column, precision)
        coverage = range_coverage(real, synth, column)

        return similarity, coverage

    similarity = tv_complement(real, synth, column)
    coverage = category_coverage(real, synth, column)

    return similarity, coverage


def univariate_summary(real, synth, precision=None):
    """Summarise all columns in terms of coverage and similarity."""

    results = [
        (
            column,
            dtype,
            *summarise_column(real, synth, column, dtype, precision),
        )
        for column, dtype in tqdm.tqdm(synth.dtypes)
    ]

    results = pd.DataFrame(
        results, columns=("column", "dtype", "similarity", "coverage")
    )

    results[["similarity_metric", "coverage_metric"]] = pd.DataFrame(
        results["dtype"]
        .apply(
            lambda d: (
                ("TV Complement", "Category Coverage")
                if d == "string"
                else ("KS Complement", "Range Coverage")
            )
        )
        .tolist(),
        index=results.index,
    )

    return results


def freedman_diaconis(data, column, nrows, vmin, vmax, tol):
    """Use the Freedman-Diaconis method to determine number of bins."""

    q1, q3 = data.approxQuantile(column, [0.25, 0.75], tol)
    width = 2 * (q3 - q1) * nrows ** (-1 / 3)

    return math.ceil((vmax - vmin) / width)


def sturges(nrows):
    """Use Sturges' method to determine the optimal number of bins."""

    return math.ceil(math.log(nrows, 2)) + 1


def get_bins(data, column, method, tol, vmin, vmax):
    """Get the number of bins to discretise the column."""

    assert isinstance(method, (str, int))

    if isinstance(method, str):
        nrows = data.count()

    if method == "auto":
        bins = max(
            freedman_diaconis(data, column, nrows, vmin, vmax, tol),
            sturges(nrows),
        )
    elif method == "sturges":
        bins = sturges(nrows)
    elif method == "fd":
        bins = freedman_diaconis(data, column, nrows, vmin, vmax, tol)
    else:
        bins = method

    assert isinstance(bins, int)

    return bins


def discretise(data, column, method=None, tol=0.01):
    """Bin the given numeric column after determining a number of bins.

    Mimics behaviour of `numpy.histogram()` in the most part. If
    `method` is `None`, then each value in `column` is treated as an
    individual category and no transformation is performed.
    """

    if method is not None:
        limits = data.agg(funcs.min(column), funcs.max(column)).collect()[0]
        bins = get_bins(data, column, method, tol, *limits)

        splits = np.linspace(*limits, bins + 1).tolist()
        str_splits = f"array({', '.join(map(str, splits))})"

        bin_column = f"{column}_bin"
        bucketer = Bucketizer(
            splits=splits, inputCol=column, outputCol=bin_column
        )

        data = (
            bucketer.transform(data)
            .withColumn(
                "delim",
                funcs.when(
                    funcs.col(bin_column) == len(splits) - 2, "]"
                ).otherwise(")"),
            )
            .withColumn(
                column,
                funcs.expr(
                    f"""
                    format_string(
                        '[%s, %s%s',
                        {str_splits}[int({bin_column})],
                        {str_splits}[int({bin_column}) + 1],
                        delim
                    )
                    """
                ),
            )
            .drop(bin_column, "delim")
        )

    return data


def discretise_data(data, numeric, method=None, tol=0.01):
    """Discreatise all the numeric columns of a dataset."""

    if not isinstance(method, dict):
        method = {col: method for col in numeric}

    for column, bins in tqdm.tqdm(method.items()):
        data = discretise(data, column, method=bins, tol=tol)

    data.checkpoint()

    return data


def correlation(data, col1, col2, method="spearman"):
    """Get the correlation coefficient of two columns in a dataset."""

    if method == "spearman":
        data.cache()

    assembler = VectorAssembler(
        inputCols=[col1, col2], outputCol="features", handleInvalid="skip"
    )
    features = assembler.transform(data).select("features")
    correlation = (
        Correlation.corr(features, "features", method=method)
        .collect()[0][f"{method}(features)"]
        .values[1]
    )

    return correlation


def correlation_comparison(real, synth, col1, col2, method="spearman"):
    r"""Get a comparison of a correlation coefficient from two datasets.

    Two correlation coefficients, :math:`R` and :math:`S`, are compared
    such that:

    .. math::
        comp(R, S) = 1 - \frac{\abs{R - S}}{2}

    Values for this comparison lie in the range 0 to 1, where a score of
    0 indicates that the correlation was not preserved well at all, and
    a score of 1 indicates the correlation was preserved precisely.

    Note
    ----
    While this scoring compares the correlation coefficients from a
    column pair in each dataset, it does not actually compare the
    relationships themselves. For instance, both datasets could have
    strong positive correlations (i.e. high coefficients) but the shapes
    of the correlation curves would not necessarily be identical.
    """

    rcorr, scorr = (
        correlation(data, col1, col2, method) for data in (real, synth)
    )

    comparison = 1 - abs(rcorr - scorr) / 2

    return comparison


def get_column_combinations(
    real,
    synth,
    numeric_dtypes=("bigint", "date", "double", "int", "timestamp"),
):
    """Get the various combination sets for the pairwise summary."""

    dtypes = set(real.dtypes).intersection(synth.dtypes)
    numeric = [col for col, dtype in dtypes if dtype in numeric_dtypes]
    categoric = [col for col, _ in dtypes if col not in numeric]

    numeric_pairs = list(itertools.combinations(numeric, r=2))
    mixed_pairs = [
        pair
        for pair in itertools.combinations([*categoric, *numeric], r=2)
        if pair not in numeric_pairs
    ]

    return numeric, categoric, numeric_pairs, mixed_pairs


def pairwise_summary(
    real,
    synth,
    numeric_dtypes=("bigint", "date", "double", "int", "timestamp"),
    corr="spearman",
    bins=None,
    tol=0.01,
):
    """Get a summary of the pairwise relationships.

    We use a different comparison method for the type of column pair:

    - Categorical-categorical: total variation complement for the real
      and synthetic two-way contingency tables.
    - Numeric-numeric: correlation coefficient using `corr` as the
      method. Only Spearman and Pearson are supported.
    - Numeric-categorical: discretise the numeric column using `bins`
      and treat the pair as categorical-categorical.

    The final dataset is presented in long form.
    """

    numeric, categoric, numeric_pairs, mixed_pairs = get_column_combinations(
        real, synth, numeric_dtypes
    )

    correlations = [(col, col, 1.0) for col in numeric]
    for a, b in tqdm.tqdm(numeric_pairs):
        score = correlation_comparison(real, synth, a, b, corr)
        correlations.extend([(a, b, score), (b, a, score)])

    real, synth = (
        discretise_data(data, numeric, bins, tol) for data in (real, synth)
    )

    real.persist()
    synth.persist()

    contingencies = [(col, col, 1.0) for col in categoric]
    for a, b in tqdm.tqdm(mixed_pairs):
        score = tv_complement(real, synth, [a, b])
        contingencies.extend([(a, b, score), (b, a, score)])

    real.unpersist()
    synth.unpersist()

    results = pd.DataFrame(
        [*correlations, *contingencies], columns=("a", "b", "score")
    )

    return results


def propensity_prep(real, synth, columns=None):
    """Prepare datasets for propensity score model."""

    columns = columns or list(set(real.columns).intersection(synth.columns))

    real = real.select(*columns).withColumn("label", funcs.lit(0))
    synth = synth.select(*columns).withColumn("label", funcs.lit(1))

    combined = real.union(synth)
    combined = preprocess.fill_missing_values(combined).cache()
    combined, mappings, sizes = preprocess.encode_all_columns(
        combined, columns=columns, partitions=100
    )

    assembler = VectorAssembler(
        inputCols=columns, outputCol="features", handleInvalid="skip"
    )
    transformed = assembler.transform(combined).select("features", "label")
    combined.unpersist()

    return transformed


def extract_propensity(vector):
    """Get the synthetic-prediction probability."""

    def get_prob_(v):
        """Inner function to get around submodule UDF restrictions."""

        try:
            return float(v[1])
        except ValueError:
            return None

    return funcs.udf(get_prob_, DoubleType())(vector)


def propensity_scores(transformed, seed=None):
    """Gather propensity scores for each dataset."""

    forest = RandomForestClassifier(maxBins=7201, seed=seed).fit(transformed)
    result = (
        forest.transform(transformed)
        .withColumn("propensity", extract_propensity(funcs.col("probability")))
        .select("propensity", "label")
        .checkpoint()
    )

    real = result.filter(funcs.col("label") == 0)
    synth = result.filter(funcs.col("label") == 1)

    return real, synth


def specks(real, synth, columns=None, precision=5, seeds=20):
    """Perform SPECKS distinguishability analysis.

    SPECKS measures the distinguishability of a real-synthetic dataset
    pair as the Kolmogorov-Smirnov distance between their propensity
    scores. To ease understanding with other metrics, we use the
    complement distance. That is, a score of 0 indicates absolutely no
    similarity in propensity scores (absolutely distinguishable).
    Meanwhile, a score of 1 represents identical propensity scores and
    indistinguishable datasets.

    We run multiple classifiers to get a more robust estimate for SPECKS
    and return them as a list.
    """

    transformed = propensity_prep(real, synth, columns)
    transformed.cache()

    results = []
    for seed in tqdm.trange(seeds):
        real_scores, synth_scores = propensity_scores(transformed, seed=seed)

        real_scores.cache()
        synth_scores.cache()

        result = ks_complement(
            real_scores, synth_scores, column="propensity", precision=precision
        )
        results.append(result)

        real_scores.unpersist()
        synth_scores.unpersist()

    transformed.unpersist()
    results = pd.DataFrame(enumerate(results), columns=("seed", "specks"))

    return results
