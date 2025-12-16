"""Daedalus - A modular, high-throughput market data ingestion + ETL pipeline."""

__version__ = "0.1.0"
__author__ = "Daedalus Contributors"
__license__ = "MIT"

from config import DaedalusConfig, load_config

__all__ = ["DaedalusConfig", "load_config", "__version__"]
