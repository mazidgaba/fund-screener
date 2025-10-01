from __future__ import annotations

import logging
from typing import List

import numpy as np
import pandas as pd

from .models import DailyMetrics, RawData

logger = logging.getLogger(__name__)


def _compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("date").copy()
    # SMAs with min_periods to allow recent IPOs
    df["sma_50"] = df["close"].rolling(window=50, min_periods=10).mean()
    df["sma_200"] = df["close"].rolling(window=200, min_periods=20).mean()
    # 52-week high ~ 252 trading days
    df["high_52w"] = df["high"].rolling(window=252, min_periods=20).max()
    df["pct_from_52w_high"] = np.where(
        df["high_52w"].notna(), (df["close"] / df["high_52w"] - 1.0) * 100.0, np.nan
    )
    return df


def _compute_fundamentals(df: pd.DataFrame) -> pd.DataFrame:
    # Book Value Per Share (BVPS) = total equity / shares_outstanding
    equity = df.get("total_shareholder_equity")
    shares = df.get("shares_outstanding")
    close = df["close"]
    if equity is not None and shares is not None:
        with np.errstate(divide="ignore", invalid="ignore"):
            bvps = np.where((shares > 0), (equity / shares).astype(float), np.nan)
    else:
        # fallback to book_value per share if provided by info
        bvps = df.get("book_value", pd.Series(index=df.index, dtype=float))
    df["book_value_per_share"] = bvps

    with np.errstate(divide="ignore", invalid="ignore"):
        df["price_to_book"] = np.where(
            df["book_value_per_share"].notna() & (df["book_value_per_share"] != 0),
            close / df["book_value_per_share"],
            np.nan,
        )

    # Enterprise Value (simplified): if info provided, use it; else market cap + debt - cash
    if "enterprise_value" not in df.columns or df["enterprise_value"].isna().all():
        # Approximate market cap using shares * close
        if shares is not None:
            market_cap = shares * close
        else:
            market_cap = pd.Series(index=df.index, data=np.nan)
        total_debt = df.get("total_debt", pd.Series(index=df.index, data=np.nan))
        cash = df.get(
            "cash_and_short_term_investments", pd.Series(index=df.index, data=np.nan)
        )
        df["enterprise_value"] = market_cap + total_debt - cash

    return df


def process_data(raw_data: RawData) -> pd.DataFrame:
    """Merge prices with fundamentals and compute indicators/ratios.

    Args:
        raw_data: Validated raw data.

    Returns:
        DataFrame with daily metrics.
    """
    prices = pd.DataFrame([p.model_dump() for p in raw_data.prices])

    # Fundamentals as-of merge by forward-fill
    fund = raw_data.fundamentals
    if fund:
        fdf = pd.DataFrame([fund.model_dump()])
        # forward-fill across all dates; implicitly assume point-in-time until next filing
        fdf["date"] = prices["date"].min()
        merged = prices.merge(fdf, on="date", how="left")
        merged = merged.sort_values("date").ffill()
    else:
        merged = prices.copy()

    merged = _compute_indicators(merged)
    merged = _compute_fundamentals(merged)

    merged["ticker"] = raw_data.ticker

    # Validate rows via Pydantic model
    records: List[DailyMetrics] = []
    for _, row in merged.iterrows():
        try:
            records.append(DailyMetrics(**row.to_dict()))
        except Exception as e:
            logger.warning("Validation error on date=%s: %s", row.get("date"), e)

    out = pd.DataFrame([r.model_dump() for r in records])
    return out
