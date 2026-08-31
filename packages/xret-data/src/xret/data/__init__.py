"""Trusted market data infrastructure for Xret."""

from __future__ import annotations

from importlib.metadata import version as _version

from xret.data.config import MarketDataConfig
from xret.data.dataset import BarDataset
from xret.data.funding import (
    FundingFetchResult,
    FundingPartialScanResult,
    FundingSyncResult,
    SettledFundingDataset,
)
from xret.data.live import LiveMarketData
from xret.data.market_data import MarketData
from xret.data.models import (
    Availability,
    BarFetchMode,
    BarFinality,
    BarUpdate,
    CapabilityNotice,
    DatasetFamily,
    FetchResult,
    LiveSubscription,
    OpenInterestKey,
    OperationCapability,
    PartialScanResult,
    ProviderEvidence,
    ReferenceBarCapability,
    ReferenceBarKey,
    ReferencePriceKind,
    SettledFundingKey,
    SyncResult,
    TimeBarCapability,
)
from xret.data.open_interest import (
    OpenInterestDataset,
    OpenInterestFetchResult,
    OpenInterestPartialScanResult,
    OpenInterestSyncResult,
)
from xret.data.reference import (
    ReferenceBarDataset,
    ReferenceBarFetchResult,
    ReferenceBarPartialScanResult,
    ReferenceBarSyncResult,
)

__version__ = _version("xret-data")

__all__ = [
    "MarketData",
    "MarketDataConfig",
    "BarDataset",
    "SettledFundingDataset",
    "FundingFetchResult",
    "FundingSyncResult",
    "FundingPartialScanResult",
    "ReferenceBarDataset",
    "ReferenceBarFetchResult",
    "ReferenceBarSyncResult",
    "ReferenceBarPartialScanResult",
    "OpenInterestDataset",
    "OpenInterestFetchResult",
    "OpenInterestSyncResult",
    "OpenInterestPartialScanResult",
    "BarUpdate",
    "BarFetchMode",
    "BarFinality",
    "Availability",
    "CapabilityNotice",
    "DatasetFamily",
    "ReferencePriceKind",
    "ReferenceBarCapability",
    "SettledFundingKey",
    "ReferenceBarKey",
    "OpenInterestKey",
    "OperationCapability",
    "TimeBarCapability",
    "ProviderEvidence",
    "FetchResult",
    "LiveSubscription",
    "LiveMarketData",
    "SyncResult",
    "PartialScanResult",
]
