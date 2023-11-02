"""Argument parsers for each job script."""

import argparse
import os

base = argparse.ArgumentParser(add_help=False)
base.add_argument(
    "-e",
    "--epsilon",
    default=os.getenv("EPSILON", 1.0),
    help="Privacy-loss budget. Defaults to $EPSILON then 1.",
    type=float,
)
base.add_argument(
    "-s",
    "--estimator-seed",
    default=os.getenv("ESTIMATOR_SEED"),
    help=" ".join(
        (
            "Random seed used to fit the model.",
            "Defaults to $ESTIMATOR_SEED then None.",
        )
    ),
    type=int,
)


def estimate():
    """Gather the estimation arguments."""

    parser = argparse.ArgumentParser(
        description="Fit an MST estimator.", parents=[base]
    )

    parser.add_argument(
        "-d",
        "--delta",
        default=1e-9,
        help=" ".join(
            (
                "Privacy relaxation parameter, which should be",
                "at most one over the number of data points.",
                "Defaults to 1e-9.",
            )
        ),
        type=float,
    )

    parser.add_argument(
        "-k",
        "--known-cliques",
        nargs="*",
        default=[],
        help=" ".join(
            (
                "Cliques forced into the model.",
                "Separate features in the same clique with a full stop.",
                "For example, `-k foo.bar` forces the inclusion of the",
                "foo by bar two-way marginal. Not used by default.",
            )
        ),
        type=lambda x: tuple(x.split(".")),
    )

    parser.add_argument(
        "-m",
        "--max-cells",
        default=os.getenv("MAX_CELLS", 1e6),
        help=" ".join(
            (
                "Maximum allowable number of cells in a marginal table.",
                "Defaults to $MAX_CELLS then 1e6.",
            )
        ),
        type=float,
    )

    return parser.parse_args()


def _add_total_and_chunk_size(parser):
    """Add the total and chunk arguments to a parser."""

    parser.add_argument(
        "-t",
        "--total",
        default=os.getenv("TOTAL", None),
        help="Number of rows in the synthetic dataset. Defaults to original.",
    )

    parser.add_argument(
        "-c",
        "--chunk",
        default=os.getenv("ROWS", os.getenv("CHUNK", 10000)),
        help=" ".join(
            (
                "Number of rows sampled in a chunk.",
                "Defaults to $ROWS or $CHUNK then 10,000.",
                "If None, will use the size of the original data.",
            )
        ),
        type=lambda rows: rows if rows is None else int(float(rows)),
    )

    return parser


def synthesise():
    """Gather the synthesis arguments."""

    parser = argparse.ArgumentParser(
        description="Sample tabular data from an estimator.", parents=[base]
    )

    parser = _add_total_and_chunk_size(parser)

    return parser.parse_args()


def postprocess():
    """Gather the post-processing arguments."""

    parser = argparse.ArgumentParser(
        description="Process a sampled synthetic dataset.", parents=[base]
    )

    parser = _add_total_and_chunk_size(parser)

    return parser.parse_args()
