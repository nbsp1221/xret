"""Deterministic tests for qualified CCXT candle semantic corrections."""

from __future__ import annotations

import pytest
from xret.data.errors import UnsupportedMarketError
from xret.data.models import MarketIdentity
from xret.data.providers import DerivativeInterpretation, ResolvedBarMarket
from xret.data.providers.ccxt import compatibility, semantics


@pytest.mark.parametrize(
    "client_id",
    ["apex", "gate", "hashkey", "kucoinfutures", "mexc", "toobit"],
)
def test_qualified_linear_contract_volume_is_converted_to_base_quantity(client_id: str) -> None:
    rows = ((1.0, 100.0, 101.0, 99.0, 100.0, 250.0),)

    assert semantics.normalize_ohlcv(
        client_id,
        {"swap": True, "linear": True, "inverse": False, "contractSize": 0.001},
        rows,
    ) == ((1.0, 100.0, 101.0, 99.0, 100.0, 0.25),)


def test_spot_and_already_unified_clients_are_unchanged() -> None:
    rows = ((1.0, 100.0, 101.0, 99.0, 100.0, 2.0),)

    assert semantics.normalize_ohlcv("gate", {"spot": True}, rows) is rows
    assert (
        semantics.normalize_ohlcv(
            "okx",
            {"swap": True, "linear": True, "contractSize": 0.001},
            rows,
        )
        is rows
    )


def test_contract_count_inverse_market_is_excluded_instead_of_approximated() -> None:
    market = {"swap": True, "linear": False, "inverse": True, "contractSize": 100.0}

    assert not semantics.supports_canonical_volume("gate", market)
    with pytest.raises(UnsupportedMarketError, match="do not expose exact base-asset volume"):
        semantics.normalize_ohlcv(
            "gate",
            market,
            ((1.0, 100.0, 101.0, 99.0, 100.0, 2.0),),
        )


def test_kucoin_live_contract_count_is_converted_to_base_volume() -> None:
    market = ResolvedBarMarket(
        identity=MarketIdentity(
            exchange="kucoin",
            symbol="BTC/USDT",
            market="perpetual",
            settle="USDT",
        ),
        native_market_id="XBTUSDTM",
        native_symbol="BTC/USDT:USDT",
        timeframes=frozenset({"1m"}),
        derivative=DerivativeInterpretation(
            linear=True,
            inverse=False,
            contract_size="0.001",
        ),
    )

    assert semantics.normalize_live_volume("kucoinfutures", market, 250.0) == 0.25


def test_explicit_live_volume_policy_overrides_the_historical_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market = ResolvedBarMarket(
        identity=MarketIdentity(
            exchange="example",
            symbol="BTC/USDT",
            market="perpetual",
            settle="USDT",
        ),
        native_market_id="BTCUSDT",
        native_symbol="BTC/USDT:USDT",
        timeframes=frozenset({"1m"}),
        derivative=DerivativeInterpretation(
            linear=True,
            inverse=False,
            contract_size="0.001",
        ),
    )
    monkeypatch.setattr(
        semantics,
        "compatibility_policy",
        lambda *_args: compatibility.CompatibilityPolicy(
            volume_mode=compatibility.VolumeMode.LINEAR_CONTRACT_COUNT,
            live_volume_mode=compatibility.VolumeMode.BASE_ASSET,
        ),
    )

    assert semantics.normalize_live_volume("example", market, 250.0) == 250.0


def test_live_contract_count_requires_exact_linear_contract_metadata() -> None:
    market = ResolvedBarMarket(
        identity=MarketIdentity(
            exchange="mexc",
            symbol="BTC/USDT",
            market="perpetual",
            settle="USDT",
        ),
        native_market_id="BTC_USDT",
        native_symbol="BTC/USDT:USDT",
        timeframes=frozenset({"1m"}),
        derivative=DerivativeInterpretation(
            linear=False,
            inverse=True,
            contract_size="100",
        ),
    )

    with pytest.raises(UnsupportedMarketError, match="do not expose exact base-asset volume"):
        semantics.normalize_live_volume("mexc", market, 2.0)


def test_proven_incompatible_endpoint_timeframes_are_removed_by_scope() -> None:
    advertised = {"1s", "1m", "3d", "1w"}

    assert semantics.canonical_timeframes("binance", advertised) == frozenset({"1s", "1m", "1w"})
    assert semantics.canonical_timeframes("binanceusdm", advertised) == frozenset({"1m"})
    assert semantics.canonical_timeframes("okx", advertised) == frozenset(advertised)
    assert semantics.canonical_timeframes("deribit", {"1m", "3h", "6h", "12h", "1d"}) == frozenset(
        {"1m"}
    )
    assert semantics.canonical_timeframes("xt", {"1m", "3d"}) == frozenset({"1m"})
    assert semantics.canonical_timeframes("aster", {"1m", "3d"}) == frozenset({"1m"})
    assert semantics.canonical_timeframes("hyperliquid", {"1m", "1w", "1M"}) == frozenset({"1m"})


def test_market_specific_timeframe_mismatch_does_not_hide_valid_sibling_scope() -> None:
    advertised = {"1m", "2h", "4h"}

    assert semantics.canonical_timeframes("bitget", advertised, {"spot": True}) == frozenset(
        {"1m", "4h"}
    )
    assert semantics.canonical_timeframes("bitget", advertised, {"swap": True}) == frozenset(
        advertised
    )


def test_mexc_historical_anchor_mismatch_is_limited_to_spot() -> None:
    advertised = {"1m", "8h", "1w", "1M"}

    assert semantics.canonical_timeframes("mexc", advertised, {"spot": True}) == frozenset({"1m"})
    assert semantics.canonical_timeframes("mexc", advertised, {"swap": True}) == frozenset(
        advertised
    )


def test_woo_spot_excludes_the_reproducibly_conflicting_hourly_endpoint() -> None:
    advertised = {"1m", "1h", "4h"}

    assert semantics.canonical_timeframes("woo", advertised, {"spot": True}) == frozenset({"1m"})
    assert semantics.canonical_timeframes("woo", advertised, {"swap": True}) == frozenset(
        {"1m", "1h"}
    )


def test_bingx_noncanonical_long_interval_anchors_are_limited_to_spot() -> None:
    advertised = {"1m", "4h", "6h", "12h", "1d", "3d", "1w", "1M"}

    assert semantics.canonical_timeframes("bingx", advertised, {"spot": True}) == frozenset(
        {"1m", "4h"}
    )
    assert semantics.canonical_timeframes("bingx", advertised, {"swap": True}) == frozenset(
        advertised - {"1M"}
    )


def test_bitrue_non_utc_calendar_bars_are_limited_to_spot() -> None:
    advertised = {"1m", "1h", "1d", "1w"}

    assert semantics.canonical_timeframes("bitrue", advertised, {"spot": True}) == frozenset(
        {"1m", "1h"}
    )
    assert semantics.canonical_timeframes("bitrue", advertised, {"swap": True}) == frozenset(
        advertised
    )
