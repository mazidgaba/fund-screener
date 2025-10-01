from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional

import pandas as pd
import yfinance as yf

from .config import AppConfig
from .models import PriceBar, RawData, RawFundamentals

logger = logging.getLogger(__name__)


def _safe_decimal(val: Optional[float | int]) -> Optional[Decimal]:
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None


def fetch_stock_data(ticker: str, cfg: AppConfig) -> RawData:
    """Fetch daily OHLCV and fundamentals from yfinance with fallbacks.

    Args:
        ticker: Ticker symbol (e.g., NVDA, RELIANCE.NS)
        cfg: AppConfig with data settings.

    Returns:
        RawData validated by Pydantic.
    """
    t = yf.Ticker(ticker)

    # Prices
    period = cfg.data_settings.historical_period
    alt_periods = [period, "2y", "1y", "6mo", "3mo", "1mo", "5d"]
    hist = None
    for p in alt_periods:
        try:
            logger.info("Fetching price history for %s period=%s", ticker, p)
            hist_try = t.history(period=p, interval="1d", auto_adjust=False)
            if hist_try is not None and not hist_try.empty:
                hist = hist_try
                break
        except Exception as e:
            logger.warning("History fetch failed for %s period=%s: %s", ticker, p, e)
    if hist is None or hist.empty:
        raise RuntimeError(f"No price history returned for {ticker}")

    hist = hist.reset_index()
    # yfinance returns 'Date' as datetime64; rename for consistency
    if "Date" in hist.columns:
        hist.rename(columns={"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
    else:
        hist.rename(columns={"index": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)

    prices = []
    for _, row in hist.iterrows():
        d = row["date"].date() if isinstance(row["date"], (pd.Timestamp, datetime)) else row["date"]
        prices.append(
            PriceBar(
                date=d,
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(row["volume"]) if not pd.isna(row["volume"]) else None,
            )
        )

    # Fundamentals with fallback
    fundamentals = None
    source_used = None
    as_of = None
    currency = None

    try:
        qbs = t.quarterly_balance_sheet
        if qbs is not None and not qbs.empty:
            source_used = "quarterly_balance_sheet"
            # Take latest column
            col = qbs.columns[0]
            as_of = col.date() if hasattr(col, "date") else None
            total_equity = _safe_decimal(qbs.get("Total Stockholder Equity", pd.Series([None])).iloc[0] if "Total Stockholder Equity" in qbs.index else None)
            total_debt = _safe_decimal(qbs.get("Total Debt", pd.Series([None])).iloc[0] if "Total Debt" in qbs.index else None)
            cash = _safe_decimal(qbs.get("Cash And Cash Equivalents", pd.Series([None])).iloc[0] if "Cash And Cash Equivalents" in qbs.index else None)
        else:
            raise ValueError("quarterly empty")
    except Exception:
        try:
            abs_ = t.balance_sheet
            if abs_ is not None and not abs_.empty:
                source_used = "annual_balance_sheet"
                col = abs_.columns[0]
                as_of = col.date() if hasattr(col, "date") else None
                total_equity = _safe_decimal(abs_.get("Total Stockholder Equity", pd.Series([None])).iloc[0] if "Total Stockholder Equity" in abs_.index else None)
                total_debt = _safe_decimal(abs_.get("Total Debt", pd.Series([None])).iloc[0] if "Total Debt" in abs_.index else None)
                cash = _safe_decimal(abs_.get("Cash And Cash Equivalents", pd.Series([None])).iloc[0] if "Cash And Cash Equivalents" in abs_.index else None)
            else:
                raise ValueError("annual empty")
        except Exception:
            info = t.info or {}
            source_used = "info"
            total_equity = None
            total_debt = _safe_decimal(info.get("totalDebt"))
            cash = _safe_decimal(info.get("totalCash"))
            currency = info.get("currency")

    # Shares and per-share book value
    info = {}
    try:
        info = t.info or {}
        currency = currency or info.get("currency")
        shares = info.get("sharesOutstanding")
        book_value_ps = info.get("bookValue")  # per share
        enterprise_value = info.get("enterpriseValue")
    except Exception:
        shares = None
        book_value_ps = None
        enterprise_value = None

    fundamentals = RawFundamentals(
        total_shareholder_equity=total_equity,
        shares_outstanding=shares,
        total_debt=total_debt,
        cash_and_short_term_investments=cash,
        book_value=_safe_decimal(book_value_ps),
        enterprise_value=_safe_decimal(enterprise_value),
        currency=currency,
        as_of=as_of,
        source=source_used,
    )

    raw = RawData(ticker=ticker, prices=prices, fundamentals=fundamentals)
    logger.info("Fetched %d price rows and fundamentals source=%s", len(prices), source_used)
    return raw
