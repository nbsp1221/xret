"""Explicit live-network qualification probes for the built-in CCXT adapter."""

from __future__ import annotations

import asyncio

import pytest
from xret.data import BarUpdate, MarketData


@pytest.mark.network
@pytest.mark.parametrize("exchange", ["binance", "bybit", "okx"])
def test_spot_btc_usdt_one_minute_live_update(exchange: str) -> None:
    async def scenario() -> None:
        market_data = MarketData()
        bars = market_data.bars(
            exchange=exchange,
            symbol="BTC/USDT",
            market="spot",
            timeframe="1m",
        )
        async with market_data.live(exchange=exchange) as live:
            await live.subscribe_bar_updates(bars)
            async with asyncio.timeout(45):
                update = await anext(live)
        assert isinstance(update, BarUpdate)
        assert update.identity == bars.identity
        assert update.timeframe == bars.timeframe
        assert update.high >= max(update.open, update.low, update.close)
        assert update.low <= min(update.open, update.high, update.close)

    asyncio.run(scenario())
