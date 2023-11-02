"""A PySpark-compatible implementation of MST."""

import itertools
import logging
import os

import networkx as nx
import numpy as np
import tqdm
from disjoint_set import DisjointSet
from mbi import Domain, FactoredInference
from pyspark.sql import functions as funcs
from scipy import sparse
from scipy.special import logsumexp

from cenmort import tools


def measure(
    data, domain, cliques, sigma, rng=np.random.default_rng(), weights=None
):
    """Apply DP to marginals for a set of cliques."""

    if weights is None:
        weights = np.ones(len(cliques))
    weights = np.array(weights) / np.linalg.norm(weights)

    measurements = []
    for clique, wgt in tqdm.tqdm(zip(cliques, weights), total=len(cliques)):
        logging.info(f"Measuring clique {clique}")
        x = tools.calculate_marginal(data, clique, domain)
        y = x + rng.normal(loc=0, scale=sigma / wgt, size=x.size)
        Q = sparse.eye(x.size)
        measurements.append((Q, y, sigma / wgt, clique))

    return measurements


def compress_column(data, column, support):
    """Compress rare values into a single category."""

    to_compress = [int(val) for val, sup in enumerate(support) if not sup]

    condition = funcs.when(
        funcs.col(column).isin(*to_compress), support.size
    ).otherwise(funcs.col(column))

    data = data.withColumn(column, condition)

    return data, support.size, to_compress


def recode_column(data, column, support):
    """Encode the column to have values in {0, 1, ..., support.size}."""

    width = int(np.log10(support.size)) + 1
    data = data.withColumn(
        column, funcs.lpad(funcs.col(column).cast("int"), width, "0")
    )
    data, labels, _ = tools.encode_column(data, column, order="alphabetAsc")
    data = data.drop(column).withColumnRenamed(column + "_idx", column)

    return data, labels


def update_measurement(y, support):
    """Update the noisy measurement to use the compressed domain."""

    y2 = np.append(y[support], y[~support].sum())
    Q = np.ones(y2.size)
    Q[-1] = 1.0 / np.sqrt(y.size - y2.size + 1.0)
    y2[-1] /= np.sqrt(y.size - y2.size + 1.0)
    Q = sparse.diags(Q)

    return Q, y2


def compress_domain(data, domain, measurements):
    """Compress and recode the data, its domain, and its measurements."""

    dat = data.alias("dat")
    dom = domain.config.copy()
    new_measurements = []
    decompressings = {}

    for Q, y, sigma, proj in measurements:

        column = proj[0]
        support = y >= 3 * sigma
        supportive = support.sum()

        # more than one value to compress
        if support.size - supportive > 1:
            dom[column] = supportive + 1
            dat, code, compressed = compress_column(dat, column, support)
            dat, labels = recode_column(dat, column, support)
            Q, y = update_measurement(y, support)

            decompressings[column] = (code, compressed, labels)

        new_measurements.append((Q, y, sigma, proj))

    return dat, Domain.fromdict(dom), new_measurements, decompressings


def exponential_mechanism(
    q, eps, sensitivity, prng=np.random, monotonic=False
):
    coef = 1.0 if monotonic else 0.5
    scores = coef * eps / sensitivity * q
    probas = np.exp(scores - logsumexp(scores))
    return prng.choice(q.size, p=probas)


def assess_cliques(data, domain, measurements, max_cells, logger):
    """Assess the importance of each clique via an interim model."""

    engine = FactoredInference(domain, iters=1000)
    est = engine.estimate(measurements)

    weights = {}
    candidates = list(itertools.combinations(domain.attrs, 2))
    for clique in candidates.copy():

        cells = domain.project(clique).size()
        if cells > max_cells:
            logger.info(f"{clique} marginal too large: {cells} cells")
            candidates.remove(clique)
        else:
            logger.info(f"Measuring marginal for {clique}")
            xhat = est.project(clique).datavector()
            x = tools.calculate_marginal(data, clique, domain)
            weights[clique] = np.linalg.norm(x - xhat, 1)

    return weights, candidates


def update_candidates(candidates, domain, disjoint, graph, limit=2e9):
    """Update the list of candidate two-way cliques.

    In the original implementation of MST, candidate pairs are limited
    only to those not connected in the disjoint set. Implementing MST
    for the census-mortality dataset revealed that there must be limits
    on the candidates that would create too-large domains.

    NumPy arrays can only be as large as physical memory permits, so we
    include a catch that stops pairs being considered if their inclusion
    would exceed that limit, which is set as 2GB by default.
    """

    sizes = {
        col: domain.project([col, *graph.neighbors(col)]).size()
        for col in domain
    }

    updated = []
    for a, b in candidates:
        if (
            not disjoint.connected(a, b)
            and max(sizes[a] * domain[b], sizes[b] * domain[a]) < limit
        ):
            updated.append((a, b))

    return updated


def determine_cliques(domain, weights, rho, known_cliques, prng):
    """Determine which cliques should be included in the model."""

    T = nx.Graph()
    T.add_nodes_from(domain.attrs)
    ds = DisjointSet()

    for e in known_cliques:
        T.add_edge(*e)
        ds.union(*e)

    candidates = list(weights)
    r = len(list(nx.connected_components(T)))
    epsilon = np.sqrt(8 * rho / (r - 1))
    for _ in range(r - 1):
        candidates = update_candidates(candidates, domain, ds, T)
        wgts = np.array([weights[e] for e in candidates])
        idx = exponential_mechanism(wgts, epsilon, sensitivity=1.0, prng=prng)
        e = candidates[idx]
        T.add_edge(*e)
        ds.union(*e)

    return list(T.edges)


def select(
    data,
    domain,
    rho,
    measurements,
    prng=np.random.default_rng(),
    known_cliques=[],
    max_cells=np.inf,
    logger=logging.getLogger(),
    out=None,
):
    """Choose a set of two-way marginals for synthesis."""

    logger.info("Assessing cliques...")
    weights, candidates = assess_cliques(
        data, domain, measurements, max_cells, logger
    )

    if out is not None:
        logger.info("Saving two-way weights...")
        weights_ = {"-".join(clq): wgt for clq, wgt in weights.items()}
        tools.save_dict_as_json(weights_, os.path.join(out, "weights.json"))

    logger.info("Fitting graph...")
    cliques = determine_cliques(domain, weights, rho, known_cliques, prng)

    logger.info("Two-way marginals selected.")

    return cliques
