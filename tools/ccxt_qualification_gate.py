#!/usr/bin/env python3
"""Manual, live-network qualification gate for the CCXT Xret provider.

This is intentionally outside CI. It is a maintained experiment runner: keep
the policy and harness here, but write every run's JSON, logs, and temporary
Xret state to an output directory chosen by the operator (normally /tmp).
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import hashlib
import json
import math
import random
import shutil
import sys
import tempfile
import threading
import time
import traceback
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

VENUES = (
    "binance",
    "coinbase",
    "okx",
    "bybit",
    "bitget",
    "gate",
    "mexc",
    "kucoin",
    "kraken",
    "htx",
    "upbit",
    "bingx",
    "cryptocom",
    "bitfinex",
    "bitstamp",
    "bithumb",
    "lbank",
    "hashkey",
    "toobit",
    "bitso",
    "bitvavo",
    "phemex",
    "woo",
    "xt",
    "bitrue",
    "btcturk",
    "deribit",
    "hyperliquid",
    "aster",
    "lighter",
    "apex",
    "extended",
    "pacifica",
    "paradex",
    "dydx",
    "derive",
)
PREFERRED_SYMBOLS = (
    "HYPE/USDC",
    "BTC/USDT",
    "BTC/USDC",
    "ETH/USDT",
    "ETH/USDC",
    "SOL/USDT",
    "SOL/USDC",
)
TIMEFRAME_DAYS = {
    "1s": 1 / 86400,
    "1m": 1 / 1440,
    "3m": 1 / 480,
    "5m": 1 / 288,
    "15m": 1 / 96,
    "30m": 1 / 48,
    "1h": 1 / 24,
    "2h": 1 / 12,
    "4h": 1 / 6,
    "6h": 1 / 4,
    "8h": 1 / 3,
    "12h": 1 / 2,
    "1d": 1,
    "1w": 7,
    "1M": 31,
}


@dataclass(frozen=True, slots=True)
class PlanScope:
    market: str
    settle: str | None

    @classmethod
    def from_definition(cls, definition: Any) -> PlanScope:
        identity = definition.identity
        settle = identity.settle if identity.market.value == "perpetual" else None
        return cls(identity.market.value, settle)

    @property
    def key(self) -> str:
        return _scope_key(self.market, self.settle)


@dataclass(frozen=True, slots=True)
class PlanCase:
    market: str
    symbol: str
    settle: str | None
    timeframe: str
    scenario: str
    bars: int
    end_anchor: str
    end_offset_days: int
    stratum: str
    kind: str
    page_bars: int
    max_span_seconds: float | None = None
    lifecycle: str = "standard"


@dataclass(slots=True)
class RequestMetrics:
    load_markets_calls: int = 0
    fetch_ohlcv_calls: int = 0
    fetch_ohlcv_rows: int = 0
    fetch_ohlcv_seconds: float = 0.0
    fetch_trades_calls: int = 0
    fetch_trades_rows: int = 0
    fetch_trades_seconds: float = 0.0


_STATISTICAL_OFFSETS_DAYS = (0, 7, 45, 180, 540, 1000)
_STATISTICAL_WINDOW_BARS = (32, 96, 257)


def required_zero_failure_samples(target_rate: float, confidence: float) -> int:
    """Minimum independent observations for a zero-failure upper bound."""
    if not 0 < target_rate < 1 or not 0 < confidence < 1:
        raise ValueError("target_rate and confidence must be in (0, 1)")
    return math.ceil(math.log(1 - confidence) / math.log(1 - target_rate))


def _timeframe_days(timeframe: str) -> float:
    return TIMEFRAME_DAYS.get(timeframe, 1.0)


def _symbol_stratum(symbol: str, preferred: tuple[str, ...]) -> str:
    if symbol in preferred[:2]:
        return "major"
    if symbol in preferred:
        return "secondary"
    return "fallback"


def choose_symbols(
    definitions: tuple[Any, ...], limit: int, preferred: tuple[str, ...]
) -> list[Any]:
    active = [item for item in definitions if item.active is not False]
    selected: list[Any] = []
    for symbol in preferred:
        item = next(
            (candidate for candidate in active if candidate.identity.symbol == symbol), None
        )
        if item is not None and item not in selected:
            selected.append(item)
    for item in active:
        if item not in selected:
            selected.append(item)
    return selected[:limit]


def select_settlements(
    definitions: tuple[Any, ...],
    market_name: str,
    settlements: list[str] | None,
) -> tuple[Any, ...]:
    """Restrict perpetual evidence to the settlements named by its support claim."""
    if market_name != "perpetual" or not settlements:
        return definitions
    return tuple(
        definition for definition in definitions if definition.identity.settle in settlements
    )


def plan_cases(
    definitions: dict[str, tuple[Any, ...]],
    *,
    symbol_limit: int,
    target_samples: int,
    preferred: tuple[str, ...],
    seed: int,
    page_bars_by_scope: dict[PlanScope, int],
    max_span_seconds_by_scope: dict[PlanScope, float | None] | None = None,
) -> list[PlanCase]:
    """Build mandatory risk cases plus a separate statistical sample.

    Mandatory cases never count toward ``target_samples``. Every canonical
    timeframe advertised by each market family gets a structural case, and
    history-derived boundary cases exercise the failure-prone Xret lifecycle.
    A deterministic stratified pool then contributes exactly the requested
    number of statistical cases for every exact market-family/settlement scope.
    """
    max_span_seconds_by_scope = max_span_seconds_by_scope or {}
    selected_by_scope: dict[PlanScope, list[tuple[Any, str]]] = {}
    all_by_scope_timeframe: dict[tuple[PlanScope, str], Any] = {}
    for values in definitions.values():
        grouped: dict[PlanScope, list[Any]] = {}
        for definition in values:
            scope = PlanScope.from_definition(definition)
            grouped.setdefault(scope, []).append(definition)
        for scope, scoped_definitions in grouped.items():
            chosen = choose_symbols(tuple(scoped_definitions), symbol_limit, preferred)
            selected = selected_by_scope.setdefault(scope, [])
            for definition in chosen:
                for timeframe in definition.timeframes:
                    all_by_scope_timeframe.setdefault((scope, timeframe), definition)
                    selected.append((definition, timeframe))
            for definition in scoped_definitions:
                if definition.active is False:
                    continue
                for timeframe in definition.timeframes:
                    all_by_scope_timeframe.setdefault((scope, timeframe), definition)
    cases: list[PlanCase] = []
    seen: set[tuple[str, str | None, str, str, str, str]] = set()

    def add(
        definition: Any,
        timeframe: str,
        scenario: str,
        bars: int,
        end_anchor: str,
        end_offset_days: int,
        stratum: str,
        kind: str,
        page_bars: int,
        lifecycle: str = "standard",
    ) -> bool:
        identity = definition.identity
        key = (
            identity.market.value,
            identity.settle,
            identity.symbol,
            timeframe,
            scenario,
            kind,
        )
        if key not in seen:
            seen.add(key)
            cases.append(
                PlanCase(
                    identity.market.value,
                    identity.symbol,
                    identity.settle,
                    timeframe,
                    scenario,
                    bars,
                    end_anchor,
                    end_offset_days,
                    stratum,
                    kind,
                    page_bars,
                    max_span_seconds_by_scope.get(PlanScope.from_definition(definition)),
                    lifecycle,
                )
            )
            return True
        return False

    for (scope, timeframe), definition in sorted(
        all_by_scope_timeframe.items(),
        key=lambda item: (
            item[0][0].market,
            item[0][0].settle or "",
            TIMEFRAME_DAYS.get(item[0][1], 999),
            item[0][1],
        ),
    ):
        add(
            definition,
            timeframe,
            "advertised-timeframe",
            16,
            "recent",
            0,
            "timeframe-complete",
            "mandatory",
            page_bars_by_scope[scope],
        )

    primary_by_scope = {
        scope: selected[0][0] for scope, selected in selected_by_scope.items() if selected
    }
    for scope, definition in primary_by_scope.items():
        page_bars = page_bars_by_scope[scope]
        available = definition.timeframes
        fixed = sorted(
            (value for value in available if value[-1] in "smhd"),
            key=lambda value: (_timeframe_days(value), value),
        )
        shortest = fixed[0] if fixed else min(available, key=_timeframe_days)
        lifecycle_timeframe = "1h" if "1h" in available else shortest
        add(
            definition,
            shortest,
            "one-bar",
            1,
            "recent",
            0,
            f"{scope.key}:minimal-range",
            "mandatory",
            page_bars,
        )
        add(
            definition,
            shortest,
            "finality-boundary",
            2,
            "recent",
            0,
            f"{scope.key}:closed-bar-boundary",
            "mandatory",
            page_bars,
        )
        for multiple, suffix in (
            (page_bars - 1, "n-minus-1"),
            (page_bars, "n"),
            (page_bars + 1, "n-plus-1"),
            (2 * page_bars + 1, "two-n-plus-1"),
        ):
            add(
                definition,
                shortest,
                f"pagination-{suffix}",
                multiple,
                "recent",
                0,
                f"{scope.key}:pagination-boundary",
                "mandatory",
                page_bars,
            )
        add(
            definition,
            lifecycle_timeframe,
            "month-partition-boundary",
            8,
            "month",
            0,
            f"{scope.key}:calendar-storage",
            "mandatory",
            page_bars,
        )
        add(
            definition,
            lifecycle_timeframe,
            "year-boundary",
            8,
            "year",
            0,
            f"{scope.key}:calendar-storage",
            "mandatory",
            page_bars,
        )
        add(
            definition,
            lifecycle_timeframe,
            "incremental-left-right",
            24,
            "recent",
            7,
            f"{scope.key}:incremental-sync",
            "mandatory",
            page_bars,
            "incremental",
        )
        add(
            definition,
            lifecycle_timeframe,
            "concurrent-same-dataset",
            12,
            "recent",
            14,
            f"{scope.key}:locking",
            "mandatory",
            page_bars,
            "concurrent",
        )
        add(
            definition,
            lifecycle_timeframe,
            "reject-zero-length",
            1,
            "recent",
            0,
            f"{scope.key}:invalid-range",
            "mandatory",
            page_bars,
            "invalid-zero",
        )
        add(
            definition,
            lifecycle_timeframe,
            "reject-misaligned",
            2,
            "recent",
            0,
            f"{scope.key}:invalid-range",
            "mandatory",
            page_bars,
            "invalid-misaligned",
        )
        if "1h" in available:
            add(
                definition,
                "1h",
                "long-history-1y",
                24 * 365,
                "recent",
                0,
                f"{scope.key}:deep-history",
                "mandatory",
                page_bars,
            )
        if "1d" in available:
            add(
                definition,
                "1d",
                "long-history-3y",
                365 * 3,
                "recent",
                0,
                f"{scope.key}:deep-history",
                "mandatory",
                page_bars,
            )

    for scope, selected in sorted(
        selected_by_scope.items(), key=lambda item: (item[0].market, item[0].settle or "")
    ):
        rng = random.Random(f"{seed}:{scope.key}")
        pool = [
            (definition, timeframe, offset, bars)
            for definition, timeframe in selected
            for offset in _STATISTICAL_OFFSETS_DAYS
            for bars in _STATISTICAL_WINDOW_BARS
        ]
        by_stratum: dict[str, list[tuple[Any, str, int, int]]] = {}
        for item in pool:
            by_stratum.setdefault(_symbol_stratum(item[0].identity.symbol, preferred), []).append(
                item
            )
        for values in by_stratum.values():
            rng.shuffle(values)
        ordered_pool: list[tuple[Any, str, int, int]] = []
        strata = sorted(by_stratum)
        while any(by_stratum.values()):
            for stratum in strata:
                values = by_stratum[stratum]
                if values:
                    ordered_pool.append(values.pop())
        statistical_count = 0
        for definition, timeframe, offset, bars in ordered_pool:
            if statistical_count >= target_samples:
                break
            symbol_class = _symbol_stratum(definition.identity.symbol, preferred)
            added = add(
                definition,
                timeframe,
                f"sample-o{offset}-b{bars}",
                bars,
                "recent",
                offset,
                f"{scope.key}:{symbol_class}:offset-{offset}",
                "statistical",
                page_bars_by_scope[scope],
            )
            statistical_count += int(added)
        if statistical_count != target_samples:
            raise ValueError(
                f"statistical sample universe for {scope.key} has "
                f"{statistical_count} unique cases, fewer than requested {target_samples}"
            )
    return cases


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _retreat_bars(end: datetime, timeframe: str, bars: int) -> datetime:
    from xret.data.timeframe import TimeBar

    bar = TimeBar.parse(timeframe)
    fixed_step_ms = bar.fixed_step_ms
    if fixed_step_ms is not None:
        return end - timedelta(milliseconds=fixed_step_ms * bars)
    if timeframe == "1w":
        return end - timedelta(days=7 * bars)
    month_index = end.year * 12 + end.month - 1 - bars
    return end.replace(year=month_index // 12, month=month_index % 12 + 1)


def _range(now: datetime, case: PlanCase) -> tuple[datetime, datetime]:
    from xret.data.timeframe import TimeBar

    bar = TimeBar.parse(case.timeframe)
    if case.end_anchor == "month":
        anchor = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif case.end_anchor == "year":
        anchor = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        anchor = now - timedelta(days=case.end_offset_days)
    end = bar.floor(anchor)
    return _retreat_bars(end, case.timeframe, case.bars), end


def _estimated_pages(case: PlanCase) -> int:
    multiplier = 3 if case.lifecycle == "incremental" else 2
    if case.lifecycle.startswith("invalid-"):
        return 0
    effective_page_bars = case.page_bars
    if case.max_span_seconds is not None:
        timeframe_seconds = _timeframe_days(case.timeframe) * 86_400
        span_bars = max(1, math.floor(case.max_span_seconds / timeframe_seconds))
        effective_page_bars = min(effective_page_bars, span_bars)
    return multiplier * math.ceil(case.bars / effective_page_bars)


def _prepare_directories(output: Path, temp_root: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    temp_root.mkdir(parents=True, exist_ok=True)


def _frame_rows(frame: Any) -> list[tuple[Any, ...]]:
    return [
        tuple(row)
        for row in frame.select(["timestamp", "open", "high", "low", "close", "volume"]).iter_rows()
    ]


def _trade_base_amount(trade: dict[str, Any], market: dict[str, Any]) -> float:
    amount = float(trade["amount"])
    if not market.get("contract"):
        return amount
    price = float(trade["price"])
    contract_size = float(market["contractSize"])
    if market.get("inverse"):
        return amount * contract_size / price
    raw = trade.get("info")
    raw_contracts = raw.get("a") if isinstance(raw, dict) else None
    if isinstance(raw_contracts, int | float | str):
        try:
            parsed_contracts = float(raw_contracts)
        except ValueError:
            pass
        else:
            if _numbers_close(
                amount,
                parsed_contracts * contract_size,
                relative=1e-12,
                absolute=1e-12,
            ):
                # Some adapters, notably XT, already convert the native
                # contract quantity to base amount but safe_trade computes
                # `cost` as if it were still a contract count.
                return amount
    cost = trade.get("cost")
    if cost is not None and price:
        # CCXT adapters differ on whether derivative trade `amount` is already
        # base quantity or remains contract count. Unified `cost` is quote
        # notional, so cost / price is the representation-independent oracle.
        return float(cost) / price
    return amount * contract_size


def _aggregate_trades(trades: list[dict[str, Any]], market: dict[str, Any]) -> list[float]:
    ordered = sorted(trades, key=lambda trade: (int(trade["timestamp"]), str(trade.get("id", ""))))
    prices = [float(trade["price"]) for trade in ordered]
    return [
        prices[0],
        max(prices),
        min(prices),
        prices[-1],
        sum(_trade_base_amount(trade, market) for trade in ordered),
    ]


def _semantic_field_matches(
    actual: list[float], trades: list[dict[str, Any]], market: dict[str, Any]
) -> dict[str, bool]:
    timestamps = [int(trade["timestamp"]) for trade in trades]
    first_timestamp = min(timestamps)
    last_timestamp = max(timestamps)
    first_prices = [
        float(trade["price"]) for trade in trades if int(trade["timestamp"]) == first_timestamp
    ]
    last_prices = [
        float(trade["price"]) for trade in trades if int(trade["timestamp"]) == last_timestamp
    ]
    expected = _aggregate_trades(trades, market)
    precision = market.get("precision")
    raw_tick = precision.get("price") if isinstance(precision, dict) else None
    tick = (
        float(raw_tick) * 1.000001 if isinstance(raw_tick, int | float) and raw_tick > 0 else 1e-12
    )
    raw_amount_precision = precision.get("amount") if isinstance(precision, dict) else None
    if isinstance(raw_amount_precision, int) and raw_amount_precision >= 0:
        volume_tolerance = 10.0 ** (-raw_amount_precision)
    elif (
        isinstance(raw_amount_precision, float)
        and math.isfinite(raw_amount_precision)
        and raw_amount_precision > 0
    ):
        volume_tolerance = raw_amount_precision
    else:
        volume_tolerance = 1e-12
    return {
        # Millisecond timestamps lose execution order among trades sharing a
        # boundary millisecond. Membership is the strongest lossless check.
        "open": any(
            _numbers_close(actual[0], price, relative=1e-9, absolute=tick) for price in first_prices
        ),
        "high": _numbers_close(actual[1], expected[1], relative=1e-9, absolute=tick),
        "low": _numbers_close(actual[2], expected[2], relative=1e-9, absolute=tick),
        "close": any(
            _numbers_close(actual[3], price, relative=1e-9, absolute=tick) for price in last_prices
        ),
        # The endpoint may round its aggregate to the declared amount
        # precision. No broader relative approximation is accepted.
        "volume": _numbers_close(
            actual[4],
            expected[4],
            relative=1e-9,
            absolute=volume_tolerance,
        ),
    }


def _complete_trade_buckets(
    trades: list[dict[str, Any]], *, step_ms: int
) -> list[tuple[int, list[dict[str, Any]]]]:
    valid = [
        trade
        for trade in trades
        if trade.get("timestamp") is not None
        and trade.get("price") is not None
        and trade.get("amount") is not None
        and float(trade["price"]) > 0
        and float(trade["amount"]) > 0
    ]
    if not valid:
        return []
    earliest = min(int(trade["timestamp"]) for trade in valid)
    latest = max(int(trade["timestamp"]) for trade in valid)
    grouped: dict[int, list[dict[str, Any]]] = {}
    for trade in valid:
        timestamp = int(trade["timestamp"])
        bucket = timestamp - timestamp % step_ms
        grouped.setdefault(bucket, []).append(trade)
    return sorted(
        (
            (bucket, values)
            for bucket, values in grouped.items()
            if earliest < bucket and latest >= bucket + step_ms
        ),
        reverse=True,
    )


def _watch_complete_trade_minute(
    client_id: str,
    native_symbol: str,
    *,
    timeout_seconds: float,
) -> tuple[int, list[dict[str, Any]]]:
    """Observe one complete public-trade minute through CCXT Pro."""

    async def observe() -> tuple[int, list[dict[str, Any]]]:
        import ccxt.pro as ccxtpro

        exchange_class = getattr(ccxtpro, client_id, None)
        if exchange_class is None:
            raise RuntimeError(f"CCXT Pro has no {client_id!r} client")
        exchange = exchange_class({"enableRateLimit": True, "newUpdates": True})
        started = time.monotonic()
        target_start_ms: int | None = None
        collected_by_id: dict[object, dict[str, Any]] = {}
        idless: list[dict[str, Any]] = []
        try:
            await exchange.load_markets()
            if not exchange.has.get("watchTrades"):
                raise RuntimeError(f"{client_id} does not provide CCXT Pro watchTrades")
            while True:
                remaining = timeout_seconds - (time.monotonic() - started)
                if remaining <= 0:
                    raise TimeoutError(
                        f"no complete public-trade minute for {native_symbol} on {client_id}"
                    )
                now_ms = int(time.time() * 1000)
                if target_start_ms is not None:
                    target_end_ms = target_start_ms + 60_000
                    if now_ms >= target_end_ms + 2_000 and (collected_by_id or idless):
                        return target_start_ms, [*collected_by_id.values(), *idless]
                    until_closed = max(0.1, (target_end_ms + 2_000 - now_ms) / 1000)
                    wait_seconds = min(remaining, until_closed)
                else:
                    wait_seconds = remaining
                try:
                    trades = await asyncio.wait_for(
                        exchange.watch_trades(native_symbol),
                        timeout=wait_seconds,
                    )
                except TimeoutError:
                    if (
                        target_start_ms is not None
                        and int(time.time() * 1000) >= target_start_ms + 62_000
                        and (collected_by_id or idless)
                    ):
                        return target_start_ms, [*collected_by_id.values(), *idless]
                    continue
                if target_start_ms is None:
                    observed_ms = int(time.time() * 1000)
                    target_start_ms = observed_ms - observed_ms % 60_000 + 60_000
                target_end_ms = target_start_ms + 60_000
                for trade in trades:
                    timestamp = trade.get("timestamp")
                    if (
                        timestamp is None
                        or trade.get("price") is None
                        or trade.get("amount") is None
                        or float(trade["price"]) <= 0
                        or float(trade["amount"]) <= 0
                        or not target_start_ms <= int(timestamp) < target_end_ms
                    ):
                        continue
                    trade_id = trade.get("id")
                    if trade_id is None:
                        idless.append(trade)
                    else:
                        collected_by_id[trade_id] = trade
        finally:
            await exchange.close()

    return asyncio.run(observe())


def _pro_semantic_probe(
    client_id: str,
    market_name: str,
    exchange: Any,
    candidates: list[Any],
    *,
    timeout_seconds: float,
    rest_failure: str,
    pacer: Any,
    retry: Any,
) -> dict[str, Any]:
    """Use public CCXT Pro trades when the REST trade surface is unavailable."""
    from xret.data.providers.ccxt import client, compatibility, markets, pagination, semantics

    attempted: list[dict[str, Any]] = []
    for definition in candidates[:3]:
        native = markets.resolve(definition.identity, exchange)
        native_market = exchange.market(native.native_symbol)
        try:
            start_ms, trades = _watch_complete_trade_minute(
                client_id,
                native.native_symbol,
                timeout_seconds=timeout_seconds,
            )
            attempted.append(
                {
                    "symbol": definition.identity.symbol,
                    "settle": definition.identity.settle,
                    "returned_trades": len(trades),
                }
            )
            profile = compatibility.observation_profile(
                client_id,
                market_name,
                definition.identity.settle,
            )
            since_ms, params = pagination._request_window(
                profile,
                start_ms=start_ms,
                end_ms=start_ms + 60_000,
            )
            candles = client.fetch_page(
                exchange,
                native.native_symbol,
                "1m",
                since_ms,
                2 if profile.send_page_limit else None,
                params,
                retry,
                pacer,
            )
            matching = [row for row in candles if int(row[0]) == start_ms]
            if len(matching) != 1:
                continue
            normalized = semantics.normalize_ohlcv(
                client_id,
                native_market,
                (tuple(float(value) for value in matching[0]),),
            )[0]
            expected = _aggregate_trades(trades, native_market)
            actual = [float(value) for value in normalized[1:6]]
            matches = _semantic_field_matches(actual, trades, native_market)
            return {
                "status": "pass" if all(matches.values()) else "fail",
                "source": "ccxt_pro_watch_trades",
                "rest_failure": rest_failure,
                "client_id": client_id,
                "symbol": definition.identity.symbol,
                "native_symbol": native.native_symbol,
                "settle": definition.identity.settle,
                "minute": datetime.fromtimestamp(start_ms / 1000, tz=UTC).isoformat(),
                "trades": len(trades),
                "actual_ohlcv": actual,
                "trade_aggregate_ohlcv": expected,
                "field_matches": matches,
                "attempted": attempted,
            }
        except Exception as exc:  # noqa: BLE001 - preserve per-symbol evidence
            attempted.append(
                {
                    "symbol": definition.identity.symbol,
                    "settle": definition.identity.settle,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
    return {
        "status": "unresolved",
        "source": "ccxt_pro_watch_trades",
        "rest_failure": rest_failure,
        "reason": "no complete public-trade minute was observed through CCXT Pro",
        "attempted": attempted,
    }


def _numbers_close(left: float, right: float, *, relative: float, absolute: float) -> bool:
    return math.isclose(left, right, rel_tol=relative, abs_tol=absolute)


def _semantic_probe(
    exchange_id: str,
    definitions: dict[str, tuple[Any, ...]],
    factory: Any,
    *,
    symbol_limit: int,
    pro_timeout_seconds: float,
) -> dict[str, Any]:
    """Cross-check venue candles against a complete public-trade minute.

    The first and last minute represented in a bounded trade response are
    excluded because either may be truncated by the endpoint's row limit or
    by the request time. A successful middle minute independently proves the
    candle's trade-price OHLC and base-asset volume semantics.
    """
    from xret.data.models import Market
    from xret.data.providers.ccxt import client, compatibility, markets, pagination, semantics

    results: dict[str, Any] = {}
    for market_name, market_family in (("spot", Market.SPOT), ("perpetual", Market.PERPETUAL)):
        available = definitions.get(market_name, ())
        if not available:
            continue
        client_id = markets.scoped_client_id(exchange_id, market_family)
        exchange = factory(client_id)
        pacer = client.RequestPacer(
            compatibility.transport_policy(client_id).minimum_ohlcv_interval_seconds
        )
        retry = client.RetryPolicy(
            max_retries=2,
            backoff=lambda attempt: client.exponential_backoff(attempt, base=0.5),
            sleep=time.sleep,
        )
        candidates: list[Any] = []
        try:
            exchange.load_markets()
            attempted: list[dict[str, Any]] = []
            candidates = choose_symbols(available, symbol_limit, PREFERRED_SYMBOLS)
            candidates.sort(
                key=lambda definition: (
                    definition.derivative is not None and not definition.derivative.linear
                )
            )
            if not exchange.has.get("fetchTrades"):
                results[market_name] = _pro_semantic_probe(
                    client_id,
                    market_name,
                    exchange,
                    candidates,
                    timeout_seconds=pro_timeout_seconds,
                    rest_failure=f"{client_id} does not provide fetchTrades",
                    pacer=pacer,
                    retry=retry,
                )
                continue
            first_failure: dict[str, Any] | None = None
            for definition in candidates:
                native = markets.resolve(definition.identity, exchange)
                native_market = exchange.market(native.native_symbol)
                trades = exchange.fetch_trades(native.native_symbol, None, 1000)
                buckets = _complete_trade_buckets(trades, step_ms=60_000)
                attempted.append(
                    {
                        "symbol": definition.identity.symbol,
                        "settle": definition.identity.settle,
                        "returned_trades": len(trades),
                        "complete_trade_minutes": len(buckets),
                    }
                )
                for start_ms, bucket_trades in buckets[:3]:
                    profile = compatibility.observation_profile(
                        client_id,
                        market_name,
                        definition.identity.settle,
                    )
                    since_ms, params = pagination._request_window(
                        profile,
                        start_ms=start_ms,
                        end_ms=start_ms + 60_000,
                    )
                    candles = client.fetch_page(
                        exchange,
                        native.native_symbol,
                        "1m",
                        since_ms,
                        2 if profile.send_page_limit else None,
                        params,
                        retry,
                        pacer,
                    )
                    matching = [row for row in candles if int(row[0]) == start_ms]
                    if len(matching) != 1:
                        continue
                    normalized = semantics.normalize_ohlcv(
                        client_id,
                        native_market,
                        (tuple(float(value) for value in matching[0]),),
                    )[0]
                    expected = _aggregate_trades(bucket_trades, native_market)
                    actual = [float(value) for value in normalized[1:6]]
                    matches = _semantic_field_matches(actual, bucket_trades, native_market)
                    evidence = {
                        "status": "pass" if all(matches.values()) else "fail",
                        "client_id": client_id,
                        "symbol": definition.identity.symbol,
                        "native_symbol": native.native_symbol,
                        "settle": definition.identity.settle,
                        "minute": datetime.fromtimestamp(start_ms / 1000, tz=UTC).isoformat(),
                        "trades": len(bucket_trades),
                        "actual_ohlcv": actual,
                        "trade_aggregate_ohlcv": expected,
                        "field_matches": matches,
                        "attempted": attempted,
                    }
                    if evidence["status"] == "pass":
                        results[market_name] = evidence
                        break
                    if first_failure is None:
                        first_failure = evidence
                if results.get(market_name, {}).get("status") == "pass":
                    break
            else:
                if first_failure is not None:
                    first_failure["failed_probe_minutes"] = sum(
                        min(3, item["complete_trade_minutes"]) for item in attempted
                    )
                    pro_evidence = _pro_semantic_probe(
                        client_id,
                        market_name,
                        exchange,
                        candidates,
                        timeout_seconds=pro_timeout_seconds,
                        rest_failure=(
                            "REST public-trade buckets disagreed with the candle; "
                            "the bounded response may be truncated"
                        ),
                        pacer=pacer,
                        retry=retry,
                    )
                    pro_evidence["rest_evidence"] = first_failure
                    results[market_name] = pro_evidence
                else:
                    pro_evidence = _pro_semantic_probe(
                        client_id,
                        market_name,
                        exchange,
                        candidates,
                        timeout_seconds=pro_timeout_seconds,
                        rest_failure=(
                            "no complete public-trade minute was available within the REST "
                            "probe limit"
                        ),
                        pacer=pacer,
                        retry=retry,
                    )
                    pro_evidence["rest_evidence"] = {"attempted": attempted}
                    results[market_name] = pro_evidence
        except Exception as exc:  # noqa: BLE001 - semantic evidence keeps attribution
            results[market_name] = _pro_semantic_probe(
                client_id,
                market_name,
                exchange,
                candidates,
                timeout_seconds=pro_timeout_seconds,
                rest_failure=f"{type(exc).__name__}: {exc}",
                pacer=pacer,
                retry=retry,
            )
    return results


def _scope_key(market_name: str, settle: str | None) -> str:
    if market_name == "spot":
        return market_name
    return f"{market_name}/{settle or 'unsettled'}"


def _expected_scope_keys(definitions: dict[str, tuple[Any, ...]]) -> set[str]:
    keys: set[str] = set()
    for market_name, available in definitions.items():
        if market_name == "spot" and available:
            keys.add(_scope_key(market_name, None))
        else:
            keys.update(_scope_key(market_name, item.identity.settle) for item in available)
    return keys


def _full_gate_status(
    *,
    market_errors: dict[str, str],
    cases: list[dict[str, Any]],
    expected_scopes: set[str],
    witnessed_scopes: set[str],
    semantic_scopes: set[str],
    unresolved_assumptions: set[str] | None = None,
) -> str:
    accepted = {"pass", "pass_expected_unavailability"}
    return (
        "pass"
        if not market_errors
        and not unresolved_assumptions
        and cases
        and all(case["status"] in accepted for case in cases)
        and expected_scopes <= witnessed_scopes
        and expected_scopes <= semantic_scopes
        else "candidate"
    )


def _semantic_probes_by_scope(
    exchange_id: str,
    definitions: dict[str, tuple[Any, ...]],
    factory: Any,
    *,
    symbol_limit: int,
    pro_timeout_seconds: float,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {}
    for market_name, available in definitions.items():
        grouped: dict[str | None, list[Any]] = {}
        for definition in available:
            settle = definition.identity.settle if market_name == "perpetual" else None
            grouped.setdefault(settle, []).append(definition)
        for settle, candidates in grouped.items():
            result = _semantic_probe(
                exchange_id,
                {market_name: tuple(candidates)},
                factory,
                symbol_limit=symbol_limit,
                pro_timeout_seconds=pro_timeout_seconds,
            )
            evidence[_scope_key(market_name, settle)] = result[market_name]
    return evidence


def _assert_frame_invariants(frame: Any, *, timeframe: str, start: datetime, end: datetime) -> None:
    from xret.data.timeframe import TimeBar

    bar = TimeBar.parse(timeframe)
    rows = _frame_rows(frame)
    timestamps: list[datetime] = []
    for row in rows:
        if len(row) < 6:
            raise AssertionError("canonical frame row has fewer than six OHLCV fields")
        timestamp = row[0]
        if timestamp.tzinfo is None or timestamp != bar.floor(timestamp):
            raise AssertionError(f"timestamp is not aligned to {timeframe}: {timestamp!r}")
        if not start <= timestamp < end:
            raise AssertionError(f"timestamp outside request: {timestamp!r}")
        values = row[1:6]
        if any(not math.isfinite(float(value)) for value in values):
            raise AssertionError("non-finite OHLCV value")
        open_, high, low, close, volume = map(float, values)
        if not low <= min(open_, close) or not high >= max(open_, close) or volume < 0:
            raise AssertionError("OHLCV relationship invariant failed")
        timestamps.append(timestamp)
    if timestamps != sorted(timestamps) or len(timestamps) != len(set(timestamps)):
        raise AssertionError("timestamps are not strictly ascending and unique")


def _case_key(case: PlanCase) -> str:
    return f"{case.market}:{case.symbol}:{case.settle or ''}:{case.timeframe}:{case.scenario}"


def _bars_for_case(exchange_id: str, provider: Any, case: PlanCase, directory: Path) -> Any:
    from xret.data import MarketData, MarketDataConfig

    market_data = MarketData(
        config=MarketDataConfig(state_dir=directory / "state", data_dir=directory / "data"),
        provider=provider,
    )
    return market_data, market_data.bars(
        exchange=exchange_id,
        symbol=case.symbol,
        market=case.market,
        settle=case.settle,
        timeframe=case.timeframe,
    )


def _assert_invalid_case(bars: Any, case: PlanCase, start: datetime, end: datetime) -> None:
    from xret.data.errors import InvalidRequestError
    from xret.data.timeframe import TimeBar

    if case.lifecycle == "invalid-zero":
        invalid_start = invalid_end = end
    else:
        bar = TimeBar.parse(case.timeframe)
        fixed_step_ms = bar.fixed_step_ms
        invalid_start = (
            start + timedelta(days=1)
            if fixed_step_ms is None
            else start + timedelta(milliseconds=max(1, fixed_step_ms // 2))
        )
        invalid_end = end
    for operation in (bars.fetch, bars.sync):
        try:
            operation(invalid_start, invalid_end)
        except InvalidRequestError:
            continue
        raise AssertionError(f"{operation.__name__} accepted an invalid bar range")


def _assert_complete_lifecycle(
    market_data: Any,
    bars: Any,
    *,
    start: datetime,
    end: datetime,
    timeframe: str,
) -> tuple[int, int]:
    scanned = bars.scan(start, end).collect()
    _assert_frame_invariants(scanned, timeframe=timeframe, start=start, end=end)
    no_op = bars.sync(start, end)
    if no_op.changed or no_op.fetched_rows or no_op.written_partitions:
        raise AssertionError("identical synchronization was not a no-op")
    partial = bars.scan_partial(start, end)
    if not partial.is_complete:
        raise AssertionError("partial scan disagrees with strict scan")
    partial_frame = partial.data.collect()
    if partial_frame.shape != scanned.shape or not partial_frame.equals(scanned):
        raise AssertionError("partial scan data disagrees with strict scan")
    market_data.maintenance.validate()
    market_data.maintenance.rebuild_catalog()
    market_data.maintenance.validate()
    rebuilt = bars.scan(start, end).collect()
    if rebuilt.shape != scanned.shape or not rebuilt.equals(scanned):
        raise AssertionError("catalog rebuild changed strict scan result")
    return scanned.height, no_op.fetched_rows


def _assert_partial_lifecycle(
    bars: Any,
    *,
    fetched: Any,
    start: datetime,
    end: datetime,
    timeframe: str,
) -> tuple[int, list[str]]:
    """Prove that genuine provider omissions remain explicit Xret gaps."""
    from xret.data.errors import CoverageError

    partial = bars.scan_partial(start, end)
    if partial.is_complete or not partial.gaps:
        raise AssertionError("incomplete synchronization did not report explicit gaps")
    partial_frame = partial.data.collect()
    _assert_frame_invariants(partial_frame, timeframe=timeframe, start=start, end=end)
    if partial_frame.shape != fetched.shape or not partial_frame.equals(fetched):
        raise AssertionError("partial scan does not preserve the provider rows that were available")
    try:
        bars.scan(start, end).collect()
    except CoverageError:
        pass
    else:
        raise AssertionError("strict scan accepted incomplete provider coverage")
    return partial_frame.height, [str(gap) for gap in partial.gaps]


def _assert_explicit_incomplete_coverage(
    bars: Any,
    *,
    start: datetime,
    end: datetime,
    timeframe: str,
) -> None:
    """Require partial visibility and strict rejection for a native omission."""
    from xret.data.errors import CoverageError

    partial = bars.scan_partial(start, end)
    if partial.is_complete or not partial.gaps:
        raise AssertionError("incomplete synchronization did not report explicit gaps")
    _assert_frame_invariants(partial.data.collect(), timeframe=timeframe, start=start, end=end)
    try:
        bars.scan(start, end).collect()
    except CoverageError:
        return
    raise AssertionError("strict scan accepted incomplete provider coverage")


def _run_incremental_case(
    market_data: Any,
    bars: Any,
    case: PlanCase,
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    left_end = _retreat_bars(end, case.timeframe, case.bars // 3)
    middle_start = _retreat_bars(end, case.timeframe, 2 * case.bars // 3)
    phases = (
        ("initial-middle", middle_start, left_end),
        ("left-extension", start, left_end),
        ("right-extension", start, end),
    )
    phase_results: list[dict[str, Any]] = []
    for name, phase_start, phase_end in phases:
        sync = bars.sync(phase_start, phase_end)
        phase_results.append(
            {
                "phase": name,
                "changed": sync.changed,
                "is_complete": sync.is_complete,
                "fetched_rows": sync.fetched_rows,
                "gaps": [str(gap) for gap in sync.gaps],
            }
        )
        if not sync.is_complete:
            _assert_explicit_incomplete_coverage(
                bars,
                start=phase_start,
                end=phase_end,
                timeframe=case.timeframe,
            )
            return {"status": "pass_expected_unavailability", "phases": phase_results}
    scan_rows, _ = _assert_complete_lifecycle(
        market_data, bars, start=start, end=end, timeframe=case.timeframe
    )
    return {"status": "pass", "phases": phase_results, "scan_rows": scan_rows}


def _run_concurrent_case(
    market_data: Any,
    bars: Any,
    case: PlanCase,
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        syncs = list(pool.map(lambda _index: bars.sync(start, end), range(2)))
    if any(not sync.is_complete for sync in syncs):
        if any(sync.is_complete for sync in syncs) or syncs[0].gaps != syncs[1].gaps:
            raise AssertionError(
                "concurrent synchronization produced divergent completion or gap results"
            )
        _assert_explicit_incomplete_coverage(
            bars,
            start=start,
            end=end,
            timeframe=case.timeframe,
        )
        return {
            "status": "pass_expected_unavailability",
            "syncs": [
                {
                    "changed": sync.changed,
                    "is_complete": sync.is_complete,
                    "fetched_rows": sync.fetched_rows,
                    "gaps": [str(gap) for gap in sync.gaps],
                }
                for sync in syncs
            ],
        }
    if sum(bool(sync.changed) for sync in syncs) != 1:
        raise AssertionError("concurrent synchronization did not produce one writer and one no-op")
    scan_rows, _ = _assert_complete_lifecycle(
        market_data, bars, start=start, end=end, timeframe=case.timeframe
    )
    return {
        "status": "pass",
        "syncs": [{"changed": sync.changed, "fetched_rows": sync.fetched_rows} for sync in syncs],
        "scan_rows": scan_rows,
    }


def _run_case(
    exchange_id: str,
    provider: Any,
    case: PlanCase,
    state_root: Path,
    now: datetime,
) -> dict[str, Any]:
    from xret.data import BarFetchMode

    start, end = _range(now, case)
    result: dict[str, Any] = {
        "key": _case_key(case),
        "plan": asdict(case),
        "start": _iso(start),
        "end": _iso(end),
        "estimated_pages": _estimated_pages(case),
    }
    try:
        directory = state_root / hashlib.sha256(result["key"].encode()).hexdigest()[:16]
        market_data, bars = _bars_for_case(exchange_id, provider, case, directory)
        if case.lifecycle.startswith("invalid-"):
            _assert_invalid_case(bars, case, start, end)
            result.update({"status": "pass", "expected_rejection": True})
            return result
        if case.lifecycle == "incremental":
            result.update(_run_incremental_case(market_data, bars, case, start, end))
            return result
        if case.lifecycle == "concurrent":
            result.update(_run_concurrent_case(market_data, bars, case, start, end))
            return result
        fetched = bars.fetch(start, end, mode=BarFetchMode.FINAL)
        _assert_frame_invariants(fetched, timeframe=case.timeframe, start=start, end=end)
        sync = bars.sync(start, end)
        result["sync"] = {
            "is_complete": sync.is_complete,
            "gaps": [str(gap) for gap in sync.gaps],
            "fetched_rows": sync.fetched_rows,
        }
        if not sync.is_complete:
            partial_rows, gaps = _assert_partial_lifecycle(
                bars,
                fetched=fetched,
                start=start,
                end=end,
                timeframe=case.timeframe,
            )
            result.update(
                {
                    "status": "pass_expected_unavailability",
                    "fetch_rows": fetched.height,
                    "partial_rows": partial_rows,
                    "reported_gaps": gaps,
                }
            )
            return result
        scan_rows, _ = _assert_complete_lifecycle(
            market_data, bars, start=start, end=end, timeframe=case.timeframe
        )
        result.update({"status": "pass", "fetch_rows": fetched.height, "scan_rows": scan_rows})
    except Exception as exc:  # noqa: BLE001 - persisted evidence needs the exact cause
        result.update(
            {
                "status": "fail",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "traceback": traceback.format_exc(limit=4),
            }
        )
    return result


def _run_exchange(exchange_id: str, output: Path, args: argparse.Namespace) -> dict[str, Any]:
    import ccxt
    from xret.data.providers import Market
    from xret.data.providers.ccxt import CcxtProvider, compatibility
    from xret.data.providers.ccxt.markets import scoped_client_id

    # Qualification must exercise an unapproved endpoint rather than confuse
    # the production allowlist with a venue failure. A small profile is
    # installed only in this short-lived process. The bounded-window and
    # pagination edge cases below decide whether that assumption is valid;
    # production remains fail-closed until the resulting endpoint family is
    # promoted deliberately.
    market_pairs = tuple(
        (name, market)
        for name, market in (("spot", Market.SPOT), ("perpetual", Market.PERPETUAL))
        if not args.market or name in args.market
    )
    assumed_profiles: list[str] = []
    added_profile_scopes: list[Any] = []

    metrics = RequestMetrics()
    metrics_lock = threading.Lock()

    def instrument(exchange: Any) -> Any:
        original_load_markets = exchange.load_markets
        original_fetch_ohlcv = exchange.fetch_ohlcv
        original_fetch_trades = exchange.fetch_trades

        def load_markets(*call_args: Any, **call_kwargs: Any) -> Any:
            with metrics_lock:
                metrics.load_markets_calls += 1
            return original_load_markets(*call_args, **call_kwargs)

        def fetch_ohlcv(*call_args: Any, **call_kwargs: Any) -> Any:
            started = time.perf_counter()
            try:
                rows = original_fetch_ohlcv(*call_args, **call_kwargs)
            finally:
                elapsed = time.perf_counter() - started
                with metrics_lock:
                    metrics.fetch_ohlcv_calls += 1
                    metrics.fetch_ohlcv_seconds += elapsed
            with metrics_lock:
                metrics.fetch_ohlcv_rows += len(rows)
            return rows

        def fetch_trades(*call_args: Any, **call_kwargs: Any) -> Any:
            started = time.perf_counter()
            try:
                rows = original_fetch_trades(*call_args, **call_kwargs)
            finally:
                elapsed = time.perf_counter() - started
                with metrics_lock:
                    metrics.fetch_trades_calls += 1
                    metrics.fetch_trades_seconds += elapsed
            with metrics_lock:
                metrics.fetch_trades_rows += len(rows)
            return rows

        exchange.load_markets = load_markets
        exchange.fetch_ohlcv = fetch_ohlcv
        exchange.fetch_trades = fetch_trades
        return exchange

    def factory(client_id: str) -> Any:
        return instrument(
            getattr(ccxt, client_id)({"enableRateLimit": True, "timeout": args.timeout_ms})
        )

    provider = CcxtProvider(
        exchange_factory=factory,
        page_limit=args.page_limit,
        max_retries=args.retries,
        retry_backoff_base=args.backoff,
    )
    result: dict[str, Any] = {
        "exchange": exchange_id,
        "ccxt": ccxt.__version__,
        "status": "error",
        "qualification_only_profiles": assumed_profiles,
        "cases": [],
    }
    exchange_started = datetime.now(UTC)
    try:
        definitions: dict[str, tuple[Any, ...]] = {}
        errors: dict[str, str] = {}
        for name, market in market_pairs:
            try:
                fetched_definitions = provider.fetch_markets(exchange=exchange_id, market=market)
                definitions[name] = select_settlements(
                    fetched_definitions,
                    name,
                    args.settle,
                )
            except Exception as exc:  # noqa: BLE001 - persisted evidence
                errors[name] = f"{type(exc).__name__}: {exc}"
        result["market_counts"] = {name: len(values) for name, values in definitions.items()}
        result["market_errors"] = errors
        market_by_name = dict(market_pairs)
        plan_scopes = {
            PlanScope.from_definition(definition)
            for available in definitions.values()
            for definition in available
        }
        qualification_max_spans = {"bitget": timedelta(days=90)}
        for plan_scope in plan_scopes:
            client_id = scoped_client_id(exchange_id, market_by_name[plan_scope.market])
            scope = compatibility.EndpointScope(
                client_id,
                plan_scope.market,
                plan_scope.settle,
            )
            if scope in compatibility._OBSERVATION_PROFILES:
                continue
            compatibility._OBSERVATION_PROFILES[scope] = compatibility.ObservationProfile(
                max_bars=args.qualification_page_bars,
                max_span=qualification_max_spans.get(client_id),
                send_unified_until=client_id not in args.omit_until,
                until_inclusive=client_id != "bitvavo",
                accept_end_boundary=client_id in args.accept_end_boundary,
            )
            added_profile_scopes.append(scope)
            assumed_profiles.append(
                f"{client_id}/{plan_scope.market}"
                + (f"/{plan_scope.settle}" if plan_scope.settle is not None else "")
            )
        unresolved_assumptions = {
            scope.key
            for scope in plan_scopes
            if scope.market == "perpetual"
            and compatibility.EndpointScope(
                scoped_client_id(exchange_id, market_by_name[scope.market]),
                scope.market,
                scope.settle,
            )
            in added_profile_scopes
            and not compatibility.has_explicit_compatibility_policy(
                scoped_client_id(exchange_id, market_by_name[scope.market]),
                scope.market,
            )
        }
        assumed_profiles.sort()
        compatibility.validate_registries()
        result["qualification_only_profiles"] = assumed_profiles
        result["unresolved_qualification_assumptions"] = sorted(unresolved_assumptions)
        profiles_by_scope = {
            scope: compatibility.observation_profile(
                scoped_client_id(exchange_id, market_by_name[scope.market]),
                scope.market,
                scope.settle,
            )
            for scope in plan_scopes
        }
        page_bars_by_scope = {
            scope: min(args.page_limit, profile.max_bars)
            for scope, profile in profiles_by_scope.items()
        }
        cases = plan_cases(
            definitions,
            symbol_limit=args.symbols,
            target_samples=args.samples,
            preferred=PREFERRED_SYMBOLS,
            seed=args.seed,
            page_bars_by_scope=page_bars_by_scope,
            max_span_seconds_by_scope={
                scope: profile.max_span.total_seconds() if profile.max_span is not None else None
                for scope, profile in profiles_by_scope.items()
            },
        )
        result["planned_cases"] = [
            {**asdict(case), "estimated_pages": _estimated_pages(case)} for case in cases
        ]
        result["effective_page_bars_by_scope"] = {
            scope.key: page_bars for scope, page_bars in page_bars_by_scope.items()
        }
        result["estimated_pages"] = sum(_estimated_pages(case) for case in cases)
        statistical_by_scope = {
            scope.key: sum(
                case.kind == "statistical"
                and PlanScope(case.market, case.settle if case.market == "perpetual" else None)
                == scope
                for case in cases
            )
            for scope in plan_scopes
        }
        mandatory_by_scope = {
            scope.key: sum(
                case.kind == "mandatory"
                and PlanScope(case.market, case.settle if case.market == "perpetual" else None)
                == scope
                for case in cases
            )
            for scope in plan_scopes
        }
        result["plan_counts"] = {
            "mandatory": sum(case.kind == "mandatory" for case in cases),
            "statistical": sum(case.kind == "statistical" for case in cases),
            "mandatory_by_scope": mandatory_by_scope,
            "statistical_by_scope": statistical_by_scope,
        }
        if args.plan_only:
            result["status"] = "planned"
        elif args.semantic_only:
            result["semantic_evidence"] = _semantic_probes_by_scope(
                exchange_id,
                definitions,
                factory,
                symbol_limit=args.semantic_symbols,
                pro_timeout_seconds=args.semantic_pro_timeout,
            )
            expected_scopes = _expected_scope_keys(definitions)
            semantic_scopes = {
                name
                for name, evidence in result["semantic_evidence"].items()
                if evidence["status"] == "pass"
            }
            result["semantically_verified_scopes"] = sorted(semantic_scopes)
            result["status"] = (
                "semantic_pass"
                if not errors
                and not unresolved_assumptions
                and expected_scopes
                and expected_scopes <= semantic_scopes
                else "candidate"
            )
        else:
            # Keep the independent trade/candle witness ahead of the bulk
            # campaign so a new CCXT client does not inherit an exhausted
            # venue-wide anonymous-IP budget from hundreds of prior requests.
            result["semantic_evidence"] = _semantic_probes_by_scope(
                exchange_id,
                definitions,
                factory,
                symbol_limit=args.semantic_symbols,
                pro_timeout_seconds=args.semantic_pro_timeout,
            )
            state_root = Path(
                tempfile.mkdtemp(prefix=f"xret-qualification-{exchange_id}-", dir=args.temp_root)
            )
            try:
                now = datetime.now(UTC)
                for case in cases:
                    case_result = _run_case(exchange_id, provider, case, state_root, now)
                    result["cases"].append(case_result)
                    if (
                        sum(item["status"] == "fail" for item in result["cases"])
                        >= args.max_failures
                    ):
                        result["early_stop"] = {
                            "reason": "maximum failure evidence reached",
                            "max_failures": args.max_failures,
                            "remaining_cases": len(cases) - len(result["cases"]),
                        }
                        break
            finally:
                if not args.keep_state:
                    shutil.rmtree(state_root, ignore_errors=True)
            witnessed_scopes = {
                _scope_key(case["plan"]["market"], case["plan"].get("settle"))
                for case in result["cases"]
                if case.get("fetch_rows", case.get("scan_rows", 0)) > 0
            }
            expected_scopes = _expected_scope_keys(definitions)
            semantic_scopes = {
                name
                for name, evidence in result["semantic_evidence"].items()
                if evidence["status"] == "pass"
            }
            result["nonempty_witnessed_scopes"] = sorted(witnessed_scopes)
            result["semantically_verified_scopes"] = sorted(semantic_scopes)
            result["status"] = _full_gate_status(
                market_errors=errors,
                cases=result["cases"],
                expected_scopes=expected_scopes,
                witnessed_scopes=witnessed_scopes,
                semantic_scopes=semantic_scopes,
                unresolved_assumptions=unresolved_assumptions,
            )
    except Exception as exc:  # noqa: BLE001 - persisted evidence
        result.update(
            {
                "error_type": type(exc).__name__,
                "error": str(exc),
                "traceback": traceback.format_exc(limit=5),
            }
        )
    finally:
        for scope in added_profile_scopes:
            compatibility._OBSERVATION_PROFILES.pop(scope, None)
        result["started_at"] = exchange_started.isoformat()
        result["finished_at"] = datetime.now(UTC).isoformat()
        result["elapsed_seconds"] = (
            datetime.fromisoformat(result["finished_at"]) - exchange_started
        ).total_seconds()
        result["request_metrics"] = asdict(metrics)
    (output / f"{exchange_id}.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False, default=str) + "\n"
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, required=True, help="Run artifact directory; use /tmp for live runs."
    )
    parser.add_argument(
        "--exchange",
        action="append",
        help="CCXT client ID to qualify; repeat for multiple exchanges.",
    )
    parser.add_argument(
        "--all-ccxt",
        action="store_true",
        help="Qualify every client ID exposed by the installed CCXT version.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Parallel exchanges, never parallel cases within one exchange.",
    )
    parser.add_argument("--symbols", type=int, default=6)
    parser.add_argument(
        "--market",
        action="append",
        choices=("spot", "perpetual"),
        help="Qualify only the selected market family; repeat to select both.",
    )
    parser.add_argument(
        "--settle",
        action="append",
        help="Qualify only these perpetual settlement assets; repeat for multiple values.",
    )
    parser.add_argument(
        "--semantic-symbols",
        type=int,
        default=20,
        help="Maximum symbols tried per market family for trade-derived semantic evidence.",
    )
    parser.add_argument(
        "--semantic-pro-timeout",
        type=float,
        default=150.0,
        help="Maximum seconds per CCXT Pro symbol when REST public trades are unavailable.",
    )
    parser.add_argument(
        "--samples",
        type=int,
        help=(
            "Statistical cases per exact market-family/settlement scope; "
            "defaults to the configured confidence minimum."
        ),
    )
    parser.add_argument("--confidence", type=float, default=0.999)
    parser.add_argument("--target-failure-rate", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=20260823)
    parser.add_argument("--timeout-ms", type=int, default=15000)
    parser.add_argument("--page-limit", type=int, default=1000)
    parser.add_argument(
        "--qualification-page-bars",
        type=int,
        default=100,
        help="Conservative temporary page width for endpoint families not yet promoted.",
    )
    parser.add_argument(
        "--omit-until",
        action="append",
        default=[],
        help=(
            "Qualification-only CCXT client ID whose adapter derives its native end "
            "from since+limit and must not receive the unified until parameter."
        ),
    )
    parser.add_argument(
        "--accept-end-boundary",
        action="append",
        default=[],
        help=(
            "Qualification-only CCXT client ID whose proven native closed window may "
            "return the exact right-boundary candle; that witness is discarded."
        ),
    )
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--backoff", type=float, default=0.5)
    parser.add_argument(
        "--max-failures",
        type=int,
        default=10,
        help="Stop one venue after this many failed cases; passing venues always run exhaustively.",
    )
    parser.add_argument("--temp-root", type=Path, default=Path("/tmp"))
    parser.add_argument("--keep-state", action="store_true")
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument(
        "--semantic-only",
        action="store_true",
        help="Run only the independent trade-derived OHLCV semantic cross-check.",
    )
    args = parser.parse_args()
    started_at = datetime.now(UTC)
    try:
        required_samples = required_zero_failure_samples(args.target_failure_rate, args.confidence)
    except ValueError as exc:
        parser.error(str(exc))
    if args.samples is None:
        args.samples = required_samples
    if (
        args.workers < 1
        or args.symbols < 1
        or args.semantic_symbols < 1
        or args.semantic_pro_timeout <= 0
        or args.samples < required_samples
        or args.qualification_page_bars < 1
        or args.max_failures < 1
    ):
        parser.error(
            "workers, symbols, qualification page bars, and max failures must be positive; "
            "samples must be at least "
            f"{required_samples} for the configured confidence target"
        )
    _prepare_directories(args.output, args.temp_root)
    import ccxt

    known_venues = set(ccxt.exchanges)
    requested = set(args.exchange or ())
    unknown = requested - known_venues
    if unknown:
        parser.error("unknown CCXT exchange(s): " + ", ".join(sorted(unknown)))
    unknown_omit_until = set(args.omit_until) - known_venues
    if unknown_omit_until:
        parser.error(
            "unknown --omit-until CCXT client(s): " + ", ".join(sorted(unknown_omit_until))
        )
    unknown_end_boundary = set(args.accept_end_boundary) - known_venues
    if unknown_end_boundary:
        parser.error(
            "unknown --accept-end-boundary CCXT client(s): "
            + ", ".join(sorted(unknown_end_boundary))
        )
    if args.all_ccxt and requested:
        parser.error("--all-ccxt cannot be combined with --exchange")
    if args.plan_only and args.semantic_only:
        parser.error("--plan-only cannot be combined with --semantic-only")
    venues = sorted(known_venues) if args.all_ccxt else (args.exchange or VENUES)
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(args.workers, len(venues))) as pool:
        rows = list(pool.map(lambda venue: _run_exchange(venue, args.output, args), venues))
    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "python": sys.version,
        "configuration": {
            "workers": args.workers,
            "symbols": args.symbols,
            "markets": args.market or ["spot", "perpetual"],
            "settlements": args.settle or [],
            "semantic_symbols": args.semantic_symbols,
            "semantic_pro_timeout": args.semantic_pro_timeout,
            "samples": args.samples,
            "confidence": args.confidence,
            "target_failure_rate": args.target_failure_rate,
            "seed": args.seed,
            "timeout_ms": args.timeout_ms,
            "page_limit": args.page_limit,
            "qualification_page_bars": args.qualification_page_bars,
            "omit_until": sorted(set(args.omit_until)),
            "accept_end_boundary": sorted(set(args.accept_end_boundary)),
            "retries": args.retries,
            "backoff": args.backoff,
            "max_failures": args.max_failures,
            "plan_only": args.plan_only,
            "semantic_only": args.semantic_only,
        },
        "gate": (
            "not_run"
            if args.plan_only or args.semantic_only
            else "pass"
            if rows and all(row["status"] == "pass" for row in rows)
            else "candidate"
        ),
        "semantic_gate": (
            "pass"
            if args.semantic_only and rows and all(row["status"] == "semantic_pass" for row in rows)
            else "candidate"
            if args.semantic_only
            else "included_in_full_gate"
        ),
        "confidence": {
            "target_failure_rate": args.target_failure_rate,
            "level": args.confidence,
            "zero_failure_independent_samples": required_samples,
            "actual_statistical_samples_per_scope": args.samples,
            "mandatory_cases_excluded_from_sample_count": True,
        },
        "elapsed_seconds": (datetime.now(UTC) - started_at).total_seconds(),
        "request_metrics": {
            "load_markets_calls": sum(row["request_metrics"]["load_markets_calls"] for row in rows),
            "fetch_ohlcv_calls": sum(row["request_metrics"]["fetch_ohlcv_calls"] for row in rows),
            "fetch_ohlcv_rows": sum(row["request_metrics"]["fetch_ohlcv_rows"] for row in rows),
            "fetch_ohlcv_seconds": sum(
                row["request_metrics"]["fetch_ohlcv_seconds"] for row in rows
            ),
            "fetch_trades_calls": sum(row["request_metrics"]["fetch_trades_calls"] for row in rows),
            "fetch_trades_rows": sum(row["request_metrics"]["fetch_trades_rows"] for row in rows),
            "fetch_trades_seconds": sum(
                row["request_metrics"]["fetch_trades_seconds"] for row in rows
            ),
        },
        "venues": len(rows),
        "planned": [row["exchange"] for row in rows if row["status"] == "planned"],
        "passed": [row["exchange"] for row in rows if row["status"] == "pass"],
        "semantic_passed": [row["exchange"] for row in rows if row["status"] == "semantic_pass"],
        "candidates": [row["exchange"] for row in rows if row["status"] == "candidate"],
        "failed": [row["exchange"] for row in rows if row["status"] == "error"],
    }
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
