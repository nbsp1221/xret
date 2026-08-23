"""Qualified corrections from native CCXT candle units to Xret semantics."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from xret.data.errors import UnsupportedMarketError
from xret.data.providers.ccxt.compatibility import (
    CompatibilityPolicy,
    MarketFamily,
    VolumeMode,
    compatibility_policy,
)

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
) -> frozenset[str]:
    """Remove only endpoint/timeframe pairs proven incompatible with Xret."""
    effective = policy or compatibility_policy(client_id, _market_family(market))
    return frozenset(advertised - effective.excluded_timeframes)


def supports_canonical_volume(
    client_id: str,
    market: Mapping[str, Any],
    *,
    policy: CompatibilityPolicy | None = None,
) -> bool:
    """Whether one native market can express exact Xret base volume."""
    effective = policy or compatibility_policy(client_id, _market_family(market))
    return not (
        effective.volume_mode is VolumeMode.LINEAR_CONTRACT_COUNT
        and market.get("linear") is not True
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
