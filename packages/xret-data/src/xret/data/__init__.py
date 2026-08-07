"""Trusted market data infrastructure for Xret."""

from __future__ import annotations

from importlib.metadata import version as _version

from xret.data.config import MarketDataConfig
from xret.data.dataset import BarDataset
from xret.data.live import LiveMarketData
from xret.data.market_data import MarketData
from xret.data.models import BarUpdate, PartialScanResult, SyncResult

__version__ = _version("xret-data")

__all__ = [
    "MarketData",
    "MarketDataConfig",
    "BarDataset",
    "BarUpdate",
    "LiveMarketData",
    "SyncResult",
    "PartialScanResult",
]
