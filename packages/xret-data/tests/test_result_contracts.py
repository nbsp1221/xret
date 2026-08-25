"""Provider provenance and structured remote-result contracts."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest
from xret.data import (
    Availability,
    FetchResult,
    OperationCapability,
    ProviderEvidence,
)
from xret.data.errors import InvalidRequestError, ProviderError
from xret.data.models import CoverageInterval, CoverageStatus, DatasetKey, DataWarning, Market
from xret.data.schema import OHLCV_SCHEMA
from xret.data.warnings import normalized_warnings


def _key() -> DatasetKey:
    return DatasetKey(
        exchange="binance",
        symbol="BTC/USDT",
        market=Market.SPOT,
        settle="",
        timeframe="1m",
    )


def _source() -> ProviderEvidence:
    return ProviderEvidence(
        provider_name="ccxt",
        provider_version="4.5.65",
        provider_api_version=1,
        native_market_id="BTCUSDT",
        native_symbol="BTC/USDT",
    )


def test_operation_capability_validates_availability() -> None:
    capability = OperationCapability(Availability.AVAILABLE)

    assert capability.availability is Availability.AVAILABLE
    with pytest.raises(InvalidRequestError, match="must be an Availability"):
        OperationCapability("available")  # type: ignore[arg-type]


def test_fetch_result_exposes_completeness_without_proxying_the_dataframe() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 1, 1, 0, 1, tzinfo=UTC)
    gap = CoverageInterval(start, end, CoverageStatus.MISSING)
    result = FetchResult(
        dataset_key=_key(),
        data=pl.DataFrame(schema=OHLCV_SCHEMA),
        covered=(),
        gaps=(gap,),
        source=_source(),
    )

    assert not result.is_complete
    with pytest.raises(ProviderError, match="did not fully complete"):
        result.require_complete()


def test_fetch_result_require_complete_returns_itself() -> None:
    result = FetchResult(
        dataset_key=_key(),
        data=pl.DataFrame(schema=OHLCV_SCHEMA),
        covered=(),
        source=_source(),
    )

    assert result.require_complete() is result


def test_structured_warnings_are_deduplicated_and_stably_ordered() -> None:
    later = datetime(2026, 1, 2, tzinfo=UTC)
    earlier = datetime(2026, 1, 1, tzinfo=UTC)
    values = (
        DataWarning("z.warning", "later", later, later),
        DataWarning("b.warning", "same range", earlier, later),
        DataWarning("a.warning", "same range", earlier, later),
        DataWarning("a.warning", "same range", earlier, later),
    )

    assert [warning.code for warning in normalized_warnings(values)] == [
        "a.warning",
        "b.warning",
        "z.warning",
    ]
