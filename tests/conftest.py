from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture()
def price_df_simple():
    dates = pd.date_range("2024-01-01", periods=220, freq="B")
    close = pd.Series(range(220), index=dates, dtype=float)
    high = close + 1
    low = close - 1
    df = pd.DataFrame({
        "date": dates.date,
        "open": close.values,
        "high": high.values,
        "low": low.values,
        "close": close.values,
        "volume": 1000,
    })
    return df
