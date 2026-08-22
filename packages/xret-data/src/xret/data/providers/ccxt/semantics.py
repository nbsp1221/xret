"""Qualified corrections from native CCXT candle units to Xret semantics."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Final

from xret.data.errors import UnsupportedMarketError

RawOHLCVRow = Sequence[float]

# These CCXT endpoint families expose derivative candle volume as contract
# count even though Xret's canonical volume is base-asset quantity. Each entry
# is backed by a live trade/candle cross-check and the venue's native contract
# metadata. Spot rows are never transformed.
_CONTRACT_COUNT_CANDLE_CLIENTS: Final[frozenset[str]] = frozenset(
    {"apex", "gate", "hashkey", "kucoinfutures", "mexc", "toobit"}
)

# CCXT occasionally advertises a syntactically canonical interval that the
# scoped endpoint rejects or anchors differently from Xret's canonical grid.
_INCOMPATIBLE_TIMEFRAMES: Final[dict[str, frozenset[str]]] = {
    "aster": frozenset({"3d"}),
    "bingx": frozenset({"1M"}),
    "binance": frozenset({"3d"}),
    "binanceusdm": frozenset({"1s", "3d"}),
    "deribit": frozenset({"3h", "6h", "12h", "1d"}),
    "hyperliquid": frozenset({"1w", "1M"}),
    "xt": frozenset({"3d"}),
}


def canonical_timeframes(
    client_id: str,
    advertised: set[str],
    market: Mapping[str, Any] | None = None,
) -> frozenset[str]:
    """Remove only endpoint/timeframe pairs proven incompatible with Xret."""
    incompatible = set(_INCOMPATIBLE_TIMEFRAMES.get(client_id, frozenset()))
    if client_id == "bitget" and market is not None and market.get("spot") is True:
        incompatible.add("2h")
    if client_id == "mexc" and market is not None and market.get("spot") is True:
        # MEXC advertises 8h spot candles but rejects that interval. Its early
        # 1M history is anchored to UTC+08 before switching to UTC, so there is
        # no single lossless Xret time-bar interpretation for the full series.
        incompatible.update({"8h", "1M"})
    if client_id == "bingx" and market is not None and market.get("spot") is True:
        # BingX's spot endpoint anchors six-hour-and-longer bars on a grid
        # incompatible with Xret's UTC calendar contract. Shorter intervals
        # remain canonical and are qualified independently from perpetuals.
        incompatible.update({"6h", "12h", "1d", "3d", "1w", "1M"})
    if client_id == "bitrue" and market is not None and market.get("spot") is True:
        incompatible.update({"1d", "1w"})
    return frozenset(advertised - incompatible)


def supports_canonical_volume(client_id: str, market: Mapping[str, Any]) -> bool:
    """Whether one native market can express exact Xret base volume."""
    return not (
        client_id in _CONTRACT_COUNT_CANDLE_CLIENTS
        and market.get("swap") is True
        and market.get("linear") is not True
    )


def normalize_ohlcv(
    client_id: str,
    market: Mapping[str, Any],
    rows: tuple[tuple[float, ...], ...],
) -> tuple[tuple[float, ...], ...]:
    """Convert a qualified native candle volume unit to base quantity."""
    if client_id not in _CONTRACT_COUNT_CANDLE_CLIENTS or market.get("swap") is not True:
        return rows
    if not supports_canonical_volume(client_id, market):
        raise UnsupportedMarketError(
            f"{client_id} inverse perpetual candles do not expose exact base-asset volume"
        )
    contract_size = market.get("contractSize")
    if not isinstance(contract_size, int | float) or contract_size <= 0:
        raise UnsupportedMarketError(
            f"{client_id} perpetual market has no positive CCXT contractSize"
        )
    return tuple((*row[:5], float(row[5]) * float(contract_size), *row[6:]) for row in rows)
