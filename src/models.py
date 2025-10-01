from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, model_validator


# ---- Raw Data Schemas ----


class PriceBar(BaseModel):
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: Optional[int] = None

    @model_validator(mode="after")
    def validate_ohlc(self):
        if self.high is not None and self.low is not None and self.high < self.low:
            raise ValueError("High must be >= Low")
        return self


class RawFundamentals(BaseModel):
    # Quarterly or annual fields; optional due to yfinance sparsity
    total_shareholder_equity: Optional[Decimal] = None
    shares_outstanding: Optional[int] = None
    total_debt: Optional[Decimal] = None
    cash_and_short_term_investments: Optional[Decimal] = None
    book_value: Optional[Decimal] = None  # from info (per share)
    enterprise_value: Optional[Decimal] = None
    currency: Optional[str] = None
    as_of: Optional[date] = None
    source: Optional[str] = None  # which fallback used


class RawData(BaseModel):
    ticker: str
    prices: List[PriceBar]
    fundamentals: Optional[RawFundamentals] = None


# ---- Processed Metrics Schemas ----


class DailyMetrics(BaseModel):
    ticker: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: Optional[int] = None
    sma_50: Optional[float] = None
    sma_200: Optional[float] = None
    high_52w: Optional[float] = None
    pct_from_52w_high: Optional[float] = None
    book_value_per_share: Optional[float] = None
    price_to_book: Optional[float] = None
    enterprise_value: Optional[float] = None

    @model_validator(mode="after")
    def validate_prices(self):
        if self.high is not None and self.low is not None and self.high < self.low:
            raise ValueError("High must be >= Low")
        return self


class SignalEvent(BaseModel):
    ticker: str
    date: date
    signal_type: str

    @model_validator(mode="after")
    def validate_signal_type(self):
        if self.signal_type not in {"golden_cross", "death_cross"}:
            raise ValueError("signal_type must be 'golden_cross' or 'death_cross'")
        return self


class ExportPayload(BaseModel):
    ticker: str
    generated_at: datetime
    notes: List[str]
    signals: List[SignalEvent]
    last_metrics: List[DailyMetrics]
