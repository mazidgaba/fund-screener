from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import typer

from .config import AppConfig, load_config, setup_logging
from .data_fetcher import fetch_stock_data
from .database import init_db, save_daily_metrics, save_signal_events, upsert_ticker
from .models import DailyMetrics, ExportPayload, SignalEvent
from .processor import process_data
from .signals import detect_death_crossover, detect_golden_crossover

app = typer.Typer(add_completion=False)
logger = logging.getLogger(__name__)


@app.command()
def run(
    ticker: str = typer.Option(..., "--ticker", help="Ticker symbol, e.g., NVDA or RELIANCE.NS"),
    output: Optional[str] = typer.Option(None, "--output", help="Output JSON filepath"),
    config: Optional[str] = typer.Option(None, "--config", help="Config YAML path"),
):
    """Run full pipeline: fetch, process, detect signals, save to DB and JSON."""
    cfg: AppConfig = load_config(config)
    setup_logging(cfg.logging.level)

    # Initialize DB
    init_db(cfg.database.path)

    notes: list[str] = []

    # Fetch
    try:
        raw = fetch_stock_data(ticker, cfg)
        if raw.fundamentals and raw.fundamentals.source:
            notes.append(f"fundamentals_source={raw.fundamentals.source}")
        if raw.fundamentals and raw.fundamentals.currency:
            currency = raw.fundamentals.currency
        else:
            currency = None
        # best-effort ticker info
        upsert_ticker(cfg.database.path, symbol=ticker, market=None, name=None, currency=currency)
    except RuntimeError as e:
        # Common for very recent listings or unavailable tickers
        logger.warning("Fetch returned no history for %s: %s", ticker, e)
        notes.append("no_price_history")
        payload = ExportPayload(
            ticker=ticker,
            generated_at=datetime.utcnow(),
            notes=notes,
            signals=[],
            last_metrics=[],
        )
        if output:
            out_path = Path(output)
            out_path.write_text(json.dumps(payload.model_dump(mode="json"), indent=2), encoding="utf-8")
            logger.info("Wrote analysis JSON to %s", out_path)
        else:
            print(json.dumps(payload.model_dump(mode="json"), indent=2))
        return
    except Exception as e:
        logger.exception("Failed to fetch data: %s", e)
        raise typer.Exit(code=1)

    # Process
    try:
        df = process_data(raw)
    except Exception as e:
        logger.exception("Failed to process data: %s", e)
        raise typer.Exit(code=1)

    # Detect signals
    golden = detect_golden_crossover(df)
    death = detect_death_crossover(df)

    # Save to DB
    try:
        save_daily_metrics(cfg.database.path, df)
        save_signal_events(cfg.database.path, ticker, golden, "golden_cross")
        save_signal_events(cfg.database.path, ticker, death, "death_cross")
    except Exception as e:
        logger.exception("Failed to save to database: %s", e)
        # proceed to export JSON even if DB fails
        notes.append("database_save_failed")

    # Build export payload
    last_metrics_records = []
    if not df.empty:
        # Take last 30 rows
        tail = df.sort_values("date").tail(30)
        for _, row in tail.iterrows():
            last_metrics_records.append(DailyMetrics(**row.to_dict()))

    signals = [
        *(SignalEvent(ticker=ticker, date=d, signal_type="golden_cross") for d in golden),
        *(SignalEvent(ticker=ticker, date=d, signal_type="death_cross") for d in death),
    ]
    payload = ExportPayload(
        ticker=ticker,
        generated_at=datetime.utcnow(),
        notes=notes,
        signals=signals,
        last_metrics=last_metrics_records,
    )

    # Export JSON
    if output:
        out_path = Path(output)
        out_path.write_text(json.dumps(payload.model_dump(mode="json"), indent=2), encoding="utf-8")
        logger.info("Wrote analysis JSON to %s", out_path)
    else:
        print(json.dumps(payload.model_dump(mode="json"), indent=2))


if __name__ == "__main__":
    app()
