"""Deterministic tests for private CCXT endpoint compatibility policy."""

from __future__ import annotations

from datetime import timedelta

import pytest
from xret.data.errors import InvalidRequestError, ProviderError, UnsupportedMarketError
from xret.data.providers.ccxt import compatibility


def test_client_wide_and_market_specific_semantics_are_merged() -> None:
    spot = compatibility.compatibility_policy("bingx", "spot")
    perpetual = compatibility.compatibility_policy("bingx", "perpetual")

    assert spot.excluded_timeframes == frozenset({"6h", "12h", "1d", "3d", "1w", "1M"})
    assert perpetual.excluded_timeframes == frozenset({"1M"})


def test_temporary_family_profile_can_qualify_a_settlement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(
        compatibility._OBSERVATION_PROFILES,
        compatibility.EndpointScope("candidate", "perpetual"),
        compatibility.ObservationProfile(max_bars=1000),
    )

    assert compatibility.observation_profile("candidate", "perpetual", "USDT").max_bars == 1000


def test_exact_settlement_profile_does_not_enable_a_sibling_settlement() -> None:
    assert compatibility.observation_profile("hyperliquid", "perpetual", "USDC").max_bars == 1000

    with pytest.raises(UnsupportedMarketError, match="hyperliquid/perpetual/USDT"):
        compatibility.observation_profile("hyperliquid", "perpetual", "USDT")


@pytest.mark.parametrize(
    ("client_id", "settle"),
    [
        ("apex", "USDT"),
        ("binanceusdm", "USDT"),
        ("bingx", "USDT"),
        ("bitget", "USDT"),
        ("bybit", "USDT"),
        ("coinbase", "USDC"),
        ("cryptocom", "USD"),
        ("deribit", "USDC"),
        ("hashkey", "USDT"),
        ("hyperliquid", "USDC"),
        ("krakenfutures", "USD"),
        ("kucoinfutures", "USDT"),
        ("mexc", "USDT"),
        ("okx", "USDT"),
        ("phemex", "USDT"),
    ],
)
def test_verified_derivative_scope_does_not_enable_an_unverified_settlement(
    client_id: str,
    settle: str,
) -> None:
    assert compatibility.observation_profile(client_id, "perpetual", settle).max_bars > 0

    with pytest.raises(UnsupportedMarketError):
        compatibility.observation_profile(client_id, "perpetual", "UNVERIFIED")


def test_failed_semantic_scope_is_not_enabled_by_its_spot_sibling() -> None:
    assert compatibility.observation_profile("toobit", "spot").max_bars == 100

    with pytest.raises(UnsupportedMarketError, match="toobit/perpetual/USDT"):
        compatibility.observation_profile("toobit", "perpetual", "USDT")


def test_qualified_closed_window_and_derived_end_profiles_are_explicit() -> None:
    kucoin = compatibility.observation_profile("kucoinfutures", "perpetual", "USDT")
    htx = compatibility.observation_profile("htx", "spot")

    assert kucoin.send_unified_until is False
    assert kucoin.accept_end_boundary is True
    assert htx.send_unified_until is True
    assert htx.accept_end_boundary is True


def test_unknown_market_family_fails_before_policy_lookup() -> None:
    with pytest.raises(UnsupportedMarketError, match="unsupported CCXT market family"):
        compatibility.observation_profile("binance", "future")


def test_registry_validation_rejects_invalid_timeframe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(
        compatibility._COMPATIBILITY_POLICIES,
        compatibility.CompatibilityScope("broken"),
        compatibility.CompatibilityPolicy(excluded_timeframes=frozenset({"not-a-timeframe"})),
    )

    with pytest.raises(InvalidRequestError, match="invalid timeframe"):
        compatibility.validate_registries()


def test_registry_validation_rejects_nonpositive_observation_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(
        compatibility._OBSERVATION_PROFILES,
        compatibility.EndpointScope("broken", "spot"),
        compatibility.ObservationProfile(max_bars=0, max_span=timedelta(0)),
    )

    with pytest.raises(ProviderError, match="max_bars must be positive"):
        compatibility.validate_registries()


def test_current_registry_is_self_consistent() -> None:
    compatibility.validate_registries()


def test_documented_candle_rate_limit_overrides_optimistic_client_default() -> None:
    assert compatibility.transport_policy("hyperliquid").minimum_ohlcv_interval_seconds == 2.0
