from __future__ import annotations

import pandas as pd

from src.models import RawData, PriceBar
from src.processor import process_data


def test_sma_calculation():
    # Build synthetic prices 220 days
    dates = pd.date_range("2024-01-01", periods=220, freq="B").date
    prices = [
        PriceBar(date=d, open=float(i), high=float(i + 1), low=float(i - 1), close=float(i), volume=1000)
        for i, d in enumerate(dates)
    ]
    raw = RawData(ticker="TEST", prices=prices, fundamentals=None)
    out = process_data(raw)
    assert "sma_50" in out.columns and "sma_200" in out.columns
    # Last row must have non-null SMAs due to min_periods settings
    last = out.sort_values("date").iloc[-1]
    assert pd.notna(last["sma_50"]) and pd.notna(last["sma_200"])  # computed
