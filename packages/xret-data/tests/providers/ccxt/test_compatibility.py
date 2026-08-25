"""Deterministic tests for private CCXT endpoint compatibility policy."""

from __future__ import annotations

from datetime import timedelta

import pytest
from xret.data.errors import InvalidRequestError, ProviderError, UnsupportedMarketError
from xret.data.models import VerificationStatus
from xret.data.providers.ccxt import compatibility, verification


def test_client_wide_and_market_specific_semantics_are_merged() -> None:
    spot = compatibility.compatibility_policy("bingx", "spot")
    perpetual = compatibility.compatibility_policy("bingx", "perpetual")

    assert spot.historical_excluded_timeframes == frozenset({"6h", "12h", "1d", "3d", "1w", "1M"})
    assert perpetual.historical_excluded_timeframes == frozenset({"1M"})
    assert spot.live_excluded_timeframes == frozenset()
    assert perpetual.live_excluded_timeframes == frozenset()
    assert compatibility.has_explicit_compatibility_policy("bingx", "perpetual")
    assert not compatibility.has_explicit_compatibility_policy("unknown", "perpetual")


def test_family_profile_cannot_qualify_an_exact_settlement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(
        compatibility._OBSERVATION_PROFILES,
        compatibility.EndpointScope("candidate", "perpetual"),
        compatibility.ObservationProfile(max_bars=1000),
    )

    with pytest.raises(UnsupportedMarketError, match="candidate/perpetual/USDT"):
        compatibility.observation_profile("candidate", "perpetual", "USDT")


def test_exact_settlement_profile_does_not_enable_a_sibling_settlement() -> None:
    assert compatibility.observation_profile("hyperliquid", "perpetual", "USDC").max_bars == 1000

    with pytest.raises(UnsupportedMarketError, match="hyperliquid/perpetual/USDT"):
        compatibility.observation_profile("hyperliquid", "perpetual", "USDT")


@pytest.mark.parametrize(
    ("client_id", "settle"),
    [
        ("apex", "USDT"),
        ("aster", "USDT"),
        ("binanceusdm", "USDT"),
        ("bingx", "USDT"),
        ("bitget", "USDT"),
        ("bitfinex", "USDT"),
        ("bybit", "USDT"),
        ("coinbase", "USDC"),
        ("cryptocom", "USD"),
        ("deribit", "USDC"),
        ("dydx", "USDC"),
        ("hashkey", "USDT"),
        ("hyperliquid", "USDC"),
        ("krakenfutures", "USD"),
        ("kucoinfutures", "USDT"),
        ("mexc", "USDT"),
        ("okx", "USDT"),
        ("pacifica", "USDC"),
        ("phemex", "USDT"),
        ("woo", "USDT"),
        ("xt", "USDT"),
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


def test_native_data_violations_exclude_the_entire_affected_timeframe() -> None:
    assert compatibility.compatibility_policy("aster", "spot").historical_excluded_timeframes == {
        "1h",
        "3d",
    }
    assert compatibility.compatibility_policy(
        "woo", "perpetual"
    ).historical_excluded_timeframes == {
        "4h",
        "12h",
        "1d",
        "1w",
        "1M",
    }


def test_qualified_closed_window_and_derived_end_profiles_are_explicit() -> None:
    kucoin = compatibility.observation_profile("kucoinfutures", "perpetual", "USDT")
    htx_spot = compatibility.observation_profile("htx", "spot")
    htx_perpetual = compatibility.observation_profile("htx", "perpetual", "USDT")
    bitso = compatibility.observation_profile("bitso", "spot")

    assert kucoin.send_unified_until is False
    assert kucoin.accept_end_boundary is True
    assert htx_spot.send_unified_until is True
    assert htx_spot.accept_end_boundary is True
    assert htx_perpetual.send_page_limit is False
    assert htx_perpetual.accept_end_boundary is True
    assert bitso.max_bars == 1000
    assert bitso.accept_end_boundary is True

    xt = compatibility.observation_profile("xt", "perpetual", "USDT")
    assert xt.until_inclusive is False
    assert xt.accept_end_boundary is True


def test_xt_perpetual_contract_count_is_converted_to_base_volume() -> None:
    policy = compatibility.compatibility_policy("xt", "perpetual")

    assert policy.volume_mode is compatibility.VolumeMode.LINEAR_CONTRACT_COUNT


def test_dydx_uses_the_exact_official_iso_window_parameter_names() -> None:
    profile = compatibility.observation_profile("dydx", "perpetual", "USDC")

    assert profile.native_window_parameters == compatibility.NativeWindowParameters(
        start="fromISO",
        end="toISO",
        format=compatibility.WindowParameterFormat.RFC3339_MILLISECONDS,
    )


def test_pacifica_excludes_the_reproducibly_malformed_hourly_endpoint() -> None:
    policy = compatibility.compatibility_policy("pacifica", "perpetual")

    assert policy.historical_excluded_timeframes == frozenset({"1h"})


def test_unknown_market_family_fails_before_policy_lookup() -> None:
    with pytest.raises(UnsupportedMarketError, match="unsupported CCXT market family"):
        compatibility.observation_profile("binance", "future")


def test_registry_validation_rejects_invalid_timeframe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(
        compatibility._COMPATIBILITY_POLICIES,
        compatibility.CompatibilityScope("broken"),
        compatibility.CompatibilityPolicy(
            historical_excluded_timeframes=frozenset({"not-a-timeframe"})
        ),
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


def test_registry_validation_rejects_family_wide_perpetual_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(
        compatibility._OBSERVATION_PROFILES,
        compatibility.EndpointScope("broken", "perpetual"),
        compatibility.ObservationProfile(max_bars=100),
    )

    with pytest.raises(ProviderError, match="must settle exactly"):
        compatibility.validate_registries()


def test_current_registry_is_self_consistent() -> None:
    compatibility.validate_registries()


def test_qualification_evidence_is_independent_and_exact() -> None:
    assert verification.historical("binance", "spot", None).status is VerificationStatus.VERIFIED
    assert verification.historical("kraken", "spot", None).status is VerificationStatus.UNVERIFIED
    assert verification.live("binance", "spot", None, "1m").status is VerificationStatus.VERIFIED
    assert verification.live("binance", "spot", None, "5m").status is VerificationStatus.UNVERIFIED


def test_documented_candle_rate_limit_overrides_optimistic_client_default() -> None:
    assert compatibility.transport_policy("bitfinex").minimum_ohlcv_interval_seconds == 2.2
    assert compatibility.transport_policy("dydx").minimum_ohlcv_interval_seconds == 1.0
    assert compatibility.transport_policy("hyperliquid").minimum_ohlcv_interval_seconds == 2.0
    assert compatibility.transport_policy("pacifica").minimum_ohlcv_interval_seconds == 8.0
