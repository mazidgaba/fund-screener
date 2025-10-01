from __future__ import annotations

import pandas as pd

from src.signals import detect_death_crossover, detect_golden_crossover


def test_golden_and_death_cross_detection():
    # Create a simple crossover
    dates = pd.date_range("2024-01-01", periods=5, freq="D").date
    df = pd.DataFrame({
        "date": dates,
        "sma_50": [1, 1, 2, 2, 1],
        "sma_200": [2, 2, 1, 1, 2],
    })
    golden = detect_golden_crossover(df)
    death = detect_death_crossover(df)
    # Golden on day index 2, Death on day index 4
    assert dates[2] in golden
    assert dates[4] in death
