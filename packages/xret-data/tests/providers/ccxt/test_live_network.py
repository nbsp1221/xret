"""Explicit live-network qualification probes for the built-in CCXT adapter."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from xret.data import BarFinality, BarUpdate, MarketData, MarketDataConfig


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


@pytest.mark.network
@pytest.mark.parametrize("exchange", ["binance", "bybit", "okx"])
def test_spot_btc_usdt_recent_bootstrap_handoff(
    exchange: str,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        config = MarketDataConfig(
            state_dir=tmp_path / "state",
            data_dir=tmp_path / "data",
        )
        market_data = MarketData(config=config)
        bars = market_data.bars(
            exchange=exchange,
            symbol="BTC/USDT",
            market="spot",
            timeframe="1m",
        )
        async with market_data.live(exchange=exchange) as live:
            async with asyncio.timeout(45):
                await live.subscribe_bar_updates(bars, bootstrap=True)
                updates = [await anext(live) for _ in range(3)]

        assert all(isinstance(update, BarUpdate) for update in updates)
        assert [update.timestamp for update in updates] == sorted(
            update.timestamp for update in updates
        )
        assert all(isinstance(update.finality, BarFinality) for update in updates)
        assert updates[-1].finality in {
            BarFinality.FORMING,
            BarFinality.PROVISIONAL,
        }
        assert not config.state_dir.exists()
        assert not config.data_dir.exists()

    asyncio.run(scenario())
