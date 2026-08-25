"""Trusted market data infrastructure for Xret."""

from __future__ import annotations

from importlib.metadata import version as _version

from xret.data.config import MarketDataConfig
from xret.data.dataset import BarDataset
from xret.data.live import LiveMarketData
from xret.data.market_data import MarketData
from xret.data.models import (
    Availability,
    BarFinality,
    BarUpdate,
    CapabilityNotice,
    FetchResult,
    LiveSubscription,
    OperationCapability,
    PartialScanResult,
    ProviderEvidence,
    SyncResult,
    TimeBarCapability,
)

__version__ = _version("xret-data")

__all__ = [
    "MarketData",
    "MarketDataConfig",
    "BarDataset",
    "BarUpdate",
    "BarFinality",
    "Availability",
    "CapabilityNotice",
    "OperationCapability",
    "TimeBarCapability",
    "ProviderEvidence",
    "FetchResult",
    "LiveSubscription",
    "LiveMarketData",
    "SyncResult",
    "PartialScanResult",
]
