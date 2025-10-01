from __future__ import annotations

from datetime import date
from typing import List

import pandas as pd


def detect_golden_crossover(df: pd.DataFrame) -> List[date]:
    """Detect dates where 50SMA crosses above 200SMA.

    Args:
        df: DataFrame with columns [date, sma_50, sma_200]

    Returns:
        List of dates where golden cross occurs.
    """
    s50 = df["sma_50"]
    s200 = df["sma_200"]
    cond = (s50 > s200) & (s50.shift(1) <= s200.shift(1))
    return [pd.to_datetime(d).date() for d in df.loc[cond.fillna(False), "date"].tolist()]


def detect_death_crossover(df: pd.DataFrame) -> List[date]:
    """Detect dates where 50SMA crosses below 200SMA."""
    s50 = df["sma_50"]
    s200 = df["sma_200"]
    cond = (s50 < s200) & (s50.shift(1) >= s200.shift(1))
    return [pd.to_datetime(d).date() for d in df.loc[cond.fillna(False), "date"].tolist()]
