"""Qualified corrections from native CCXT candle units to Xret semantics."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from xret.data.errors import UnsupportedMarketError
from xret.data.providers.ccxt.compatibility import (
    CompatibilityPolicy,
    MarketFamily,
    VolumeMode,
    compatibility_policy,
)
from xret.data.providers.contracts import ResolvedBarMarket

RawOHLCVRow = Sequence[float]


def _market_family(market: Mapping[str, Any] | None) -> MarketFamily | None:
    if market is None:
        return None
    if market.get("spot") is True:
        return "spot"
    if market.get("swap") is True:
        return "perpetual"
    return None


def canonical_timeframes(
    client_id: str,
    advertised: set[str],
    market: Mapping[str, Any] | None = None,
    *,
    policy: CompatibilityPolicy | None = None,
    operation: Literal["historical", "live"] = "historical",
) -> frozenset[str]:
    """Remove only endpoint/timeframe pairs proven incompatible with Xret."""
    effective = policy or compatibility_policy(client_id, _market_family(market))
    excluded = (
        effective.historical_excluded_timeframes
        if operation == "historical"
        else effective.live_excluded_timeframes
    )
    return frozenset(advertised - excluded)


def supports_canonical_volume(
    client_id: str,
    market: Mapping[str, Any],
    *,
    policy: CompatibilityPolicy | None = None,
) -> bool:
    """Whether one native market can express exact Xret base volume."""
    effective = policy or compatibility_policy(client_id, _market_family(market))
    if effective.volume_mode is VolumeMode.BASE_ASSET:
        return True
    contract_size = market.get("contractSize")
    return (
        market.get("linear") is True
        and isinstance(contract_size, int | float)
        and math.isfinite(contract_size)
        and contract_size > 0
    )


def normalize_ohlcv(
    client_id: str,
    market: Mapping[str, Any],
    rows: tuple[tuple[float, ...], ...],
    *,
    policy: CompatibilityPolicy | None = None,
) -> tuple[tuple[float, ...], ...]:
    """Convert a qualified native candle volume unit to base quantity."""
    effective = policy or compatibility_policy(client_id, _market_family(market))
    if effective.volume_mode is VolumeMode.BASE_ASSET:
        return rows
    if not supports_canonical_volume(client_id, market, policy=effective):
        raise UnsupportedMarketError(
            f"{client_id} inverse perpetual candles do not expose exact base-asset volume"
        )
    contract_size = market.get("contractSize")
    if not isinstance(contract_size, int | float) or contract_size <= 0:
        raise UnsupportedMarketError(
            f"{client_id} perpetual market has no positive CCXT contractSize"
        )
    return tuple((*row[:5], float(row[5]) * float(contract_size), *row[6:]) for row in rows)


def normalize_live_volume(
    client_id: str,
    market: ResolvedBarMarket,
    volume: float,
) -> float:
    """Convert a qualified CCXT Pro candle volume to base quantity.

    Historical and WebSocket adapters can select different native fields. The
    live channel inherits the historical representation unless qualification
    records an explicit delivery-channel override.
    """
    policy = compatibility_policy(client_id, market.identity.market.value)
    live_volume_mode = policy.live_volume_mode or policy.volume_mode
    if live_volume_mode is VolumeMode.BASE_ASSET:
        return volume
    derivative = market.derivative
    if derivative is None or derivative.linear is not True:
        raise UnsupportedMarketError(
            f"{client_id} live candles do not expose exact base-asset volume"
        )
    try:
        contract_size = Decimal(derivative.contract_size or "")
    except InvalidOperation as exc:
        raise UnsupportedMarketError(
            f"{client_id} live perpetual market has no valid contract size"
        ) from exc
    if not contract_size.is_finite() or contract_size <= 0:
        raise UnsupportedMarketError(
            f"{client_id} live perpetual market has no positive contract size"
        )
    return volume * float(contract_size)
