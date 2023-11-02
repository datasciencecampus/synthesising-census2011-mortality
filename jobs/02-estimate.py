"""Script for fitting the MST estimator: measure, select, measure."""

from cenmort import parser, tools
from cenmort.jobs import estimate


def main(part, epsilon, delta, known_cliques, max_cells, estimator_seed):
    """Get noisy marginals, select and measure two-ways, fit model."""

    logger = tools.setup_logger("estimate", part, epsilon, estimator_seed)

    out = tools.out_directory(epsilon, estimator_seed, part, make=True)
    logger.info(f"Out directory set as {out}")

    logger.info("Starting Spark session...")
    spark = tools.start_session(executor_memory="20g")

    spark, data, domain, prng, rho, sigma = estimate.setup(
        spark, part, epsilon, delta, estimator_seed, logger
    )

    logger.info("First measurement with individual-feature cliques.")
    data, domain, log1, decompressings = estimate.first_order_measurements(
        data, domain, sigma, prng, logger
    )

    logger.info("Selection and measurement of two-way marginals.")
    log2, selected = estimate.second_order_measurements(
        data,
        domain,
        rho,
        sigma,
        log1,
        prng,
        known_cliques,
        max_cells,
        logger,
        out,
    )

    logger.info("Fitting model...")
    estimator = estimate.build_model(domain, log1 + log2, out)

    logger.info("Saving outputs...")
    estimate.save_outputs(
        estimator, selected, out, data.count(), decompressings
    )

    tools.end_session(spark)
    logger.info("Done.")


if __name__ == "__main__":
    args = parser.estimate()
    for part in ("census", "mortal"):
        main(part, **vars(args))
