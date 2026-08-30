from datetime import UTC, datetime, timedelta

import pytest
from xret.data import MarketData


@pytest.mark.network
def test_btcusdt_known_duplicate_archive_qualifies() -> None:
    start = datetime(2020, 9, 1, tzinfo=UTC)
    result = (
        MarketData(provider="binance-data-vision")
        .open_interest(
            exchange="binance",
            symbol="BTC/USDT",
            market="perpetual",
            settle="USDT",
            timeframe="5m",
        )
        .fetch(start, start + timedelta(days=1))
        .require_complete()
    )
    assert result.data.height == 288
    assert result.sources[0].source_rows == 576
    assert result.sources[0].duplicate_rows == 288
    assert result.sources[0].checksum_algorithm == "sha256"
