from __future__ import annotations

import logging
from datetime import date
from typing import Iterable, List, Optional

import pandas as pd
from sqlalchemy import (
    Column,
    Date,
    Float,
    Integer,
    MetaData,
    String,
    UniqueConstraint,
    create_engine,
    select,
    text,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    metadata = MetaData()


class Ticker(Base):
    __tablename__ = "tickers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String, unique=True, index=True)
    market: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    currency: Mapped[Optional[str]] = mapped_column(String, nullable=True)


class DailyMetric(Base):
    __tablename__ = "daily_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticker_symbol: Mapped[str] = mapped_column(String, index=True)
    date: Mapped[date] = mapped_column(Date, index=True)
    open: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    high: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    low: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    close: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    volume: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sma_50: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    sma_200: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    high_52w: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pct_from_52w_high: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    book_value_per_share: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    price_to_book: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    enterprise_value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("ticker_symbol", "date", name="uix_daily_metrics_symbol_date"),
    )


class SignalEvent(Base):
    __tablename__ = "signal_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticker_symbol: Mapped[str] = mapped_column(String, index=True)
    date: Mapped[date] = mapped_column(Date, index=True)
    signal_type: Mapped[str] = mapped_column(String)

    __table_args__ = (
        UniqueConstraint("ticker_symbol", "date", "signal_type", name="uix_symbol_date_type"),
    )


def get_engine(db_path: str):
    url = f"sqlite:///{db_path}"
    return create_engine(url, future=True)


def init_db(db_path: str) -> None:
    engine = get_engine(db_path)
    Base.metadata.create_all(engine)
    logger.info("Initialized database at %s", db_path)


def upsert_ticker(db_path: str, symbol: str, market: Optional[str], name: Optional[str], currency: Optional[str]) -> None:
    engine = get_engine(db_path)
    with Session(engine) as session:
        stmt = sqlite_upsert(Ticker).values(
            symbol=symbol, market=market, name=name, currency=currency
        )
        stmt = stmt.on_conflict_do_update(index_elements=[Ticker.symbol], set_={
            "market": stmt.excluded.market,
            "name": stmt.excluded.name,
            "currency": stmt.excluded.currency,
        })
        session.execute(stmt)
        session.commit()


def save_daily_metrics(db_path: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    engine = get_engine(db_path)
    with Session(engine) as session:
        for _, row in df.iterrows():
            stmt = sqlite_upsert(DailyMetric).values(
                ticker_symbol=row["ticker"],
                date=row["date"],
                open=row.get("open"),
                high=row.get("high"),
                low=row.get("low"),
                close=row.get("close"),
                volume=int(row["volume"]) if pd.notna(row.get("volume")) else None,
                sma_50=row.get("sma_50"),
                sma_200=row.get("sma_200"),
                high_52w=row.get("high_52w"),
                pct_from_52w_high=row.get("pct_from_52w_high"),
                book_value_per_share=row.get("book_value_per_share"),
                price_to_book=row.get("price_to_book"),
                enterprise_value=row.get("enterprise_value"),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[DailyMetric.ticker_symbol, DailyMetric.date],
                set_={
                    "open": stmt.excluded.open,
                    "high": stmt.excluded.high,
                    "low": stmt.excluded.low,
                    "close": stmt.excluded.close,
                    "volume": stmt.excluded.volume,
                    "sma_50": stmt.excluded.sma_50,
                    "sma_200": stmt.excluded.sma_200,
                    "high_52w": stmt.excluded.high_52w,
                    "pct_from_52w_high": stmt.excluded.pct_from_52w_high,
                    "book_value_per_share": stmt.excluded.book_value_per_share,
                    "price_to_book": stmt.excluded.price_to_book,
                    "enterprise_value": stmt.excluded.enterprise_value,
                },
            )
            session.execute(stmt)
        session.commit()


def save_signal_events(db_path: str, symbol: str, dates: Iterable[date], signal_type: str) -> None:
    engine = get_engine(db_path)
    with Session(engine) as session:
        for d in dates:
            stmt = sqlite_upsert(SignalEvent).values(
                ticker_symbol=symbol, date=d, signal_type=signal_type
            )
            stmt = stmt.on_conflict_do_nothing(index_elements=[
                SignalEvent.ticker_symbol, SignalEvent.date, SignalEvent.signal_type
            ])
            session.execute(stmt)
        session.commit()
