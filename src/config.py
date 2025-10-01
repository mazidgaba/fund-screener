from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import yaml


@dataclass
class DatabaseConfig:
    path: str = "financial_data.db"


@dataclass
class LoggingConfig:
    level: str = "INFO"


@dataclass
class DataSettings:
    historical_period: str = "5y"
    min_trading_days_for_sma: int = 200


@dataclass
class AppConfig:
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    data_settings: DataSettings = field(default_factory=DataSettings)


def load_config(path: Optional[str] = None) -> AppConfig:
    """Load YAML config or return defaults.

    Args:
        path: Optional path to config.yaml.

    Returns:
        AppConfig instance.
    """
    cfg = AppConfig()
    file_path = path or os.environ.get("FINANCIAL_ANALYZER_CONFIG", "config.yaml")
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            data: Dict[str, Any] = yaml.safe_load(f) or {}
        if "database" in data:
            cfg.database = DatabaseConfig(**data["database"])
        if "logging" in data:
            cfg.logging = LoggingConfig(**data["logging"])
        if "data_settings" in data:
            cfg.data_settings = DataSettings(**data["data_settings"])
    return cfg


def setup_logging(level: str) -> None:
    """Configure root logger.

    Args:
        level: Logging level string.
    """
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
