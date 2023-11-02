"""Functions for fitting MST to the data."""

import json
import os
import pickle

import numpy as np
from mbi import Domain, FactoredInference
from pydoop import hdfs

from cenmort import tools
from cenmort.mechanisms import mst
from cenmort.mechanisms.cdp2adp import cdp_rho


def setup(spark, part, epsilon, delta, estimator_seed, logger):
    """Read in the data and set up the estimation parameters."""

    logger.info("Reading in data...")
    prep = os.path.join(os.getenv("WORKSPACE"), "preprocessed", part)
    data = spark.read.parquet(os.path.join(prep, "main"))
    domain = Domain.fromdict(
        tools.read_json_as_dict(os.path.join(prep, "sizes.json"))
    )

    logger.info("Setting up MST budget variables...")
    prng = np.random.default_rng(estimator_seed)
    rho = cdp_rho(epsilon, delta)
    sigma = np.sqrt(3 / (2 * rho))

    return spark, data, domain, prng, rho, sigma


def first_order_measurements(data, domain, sigma, prng, logger):
    """Take the first-order measurements and compress the domain."""

    cliques = [(col,) for col in domain]
    log1 = mst.measure(data, domain, cliques, sigma, prng)

    logger.info("Compressing domain...")
    data, domain, log1, decompressings = mst.compress_domain(
        data, domain, log1
    )

    logger.info(f"Domain compressed for {list(decompressings.keys())}")

    return data, domain, log1, decompressings


def second_order_measurements(
    data, domain, rho, sigma, log1, prng, known_cliques, max_cells, logger, out
):
    """Select and measure important two-way marginals for synthesis."""

    logger.info("Selecting two-way marginals...")
    cliques = mst.select(
        data,
        domain,
        rho / 3,
        log1,
        prng,
        known_cliques,
        max_cells,
        logger,
        out,
    )

    logger.info("Measuring selected two-way marginals...")
    log2 = mst.measure(data, domain, cliques, sigma, prng)

    return log2, cliques


def build_model(domain, measurements, out):
    """Build a graphical model for estimating the synthetic data."""

    engine = FactoredInference(domain, iters=5000)
    estimator = engine.estimate(measurements)

    return estimator


def save_estimator(estimator, out, nrows):
    """Save the model to HDFS with the number of rows in the data."""

    estimator.nrows = nrows
    with hdfs.open(os.path.join(out, "estimator.pkl"), "wb") as e:
        pickle.dump(estimator, e)


def save_clique_information(estimator, selected, out):
    """Extract and save information on the chosen cliques as JSON."""

    order = estimator.elimination_order[::-1]
    selected = [".".join(select) for select in selected]
    estimated = [".".join(clique) for clique in estimator.cliques]

    information = dict(order=order, selected=selected, estimated=estimated)

    with hdfs.open(os.path.join(out, "information.json"), "wt") as f:
        json.dump(information, f)


def save_outputs(estimator, selected, out, nrows, decompressings):
    """Create and save details of the estimator and selected tree."""

    save_estimator(estimator, out, nrows)
    save_clique_information(estimator, selected, out)

    with hdfs.open(os.path.join(out, "decompressings.json"), "wt") as d:
        json.dump(decompressings, d)
