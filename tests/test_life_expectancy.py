"""Tests for the life expectancy code."""

import numpy as np
import pandas as pd

from cenmort.utility import life_expectancy as life


def test_estimate_with_uncertainty():
    """Regression test for the life table creator.

    Example dataset adapted from the HSLE template file.
    """

    data = pd.DataFrame(
        {
            "period": 0,
            "ethnicity": 0,
            "sex": 0,
            "age_band": [
                "<1",
                "1-4",
                *(f"{x}-{x+4}" for x in range(5, 86, 5)),
                "90+",
            ],
            "age_band_code": range(1, 21),
            "size": [
                1005542,
                4203864,
                5350085,
                4872586,
                4823327,
                5440173,
                5798527,
                5639503,
                5432176,
                5154308,
                5703333,
                5770923,
                5153251,
                4392680,
                4235825,
                3689149,
                2526639,
                1799800,
                999717,
                458507,
            ],
            "deaths": [
                4239,
                618,
                405,
                471,
                1451,
                2641,
                3344,
                4445,
                6033,
                8872,
                14512,
                21350,
                29178,
                39996,
                60803,
                83213,
                99738,
                123540,
                123961,
                107388,
            ],
        }
    )

    result = life.estimate_with_uncertainty(data)
    ends = result[result["age_band"].isin(("<1", "90+"))]

    for column, expected in {
        "life_expectancy": [79.626661, 4.269630],
        "life_expectancy_lower": [79.597640, 4.244093],
        "life_expectancy_upper": [79.655682, 4.295167],
    }.items():
        assert np.isclose(ends[column], expected)
