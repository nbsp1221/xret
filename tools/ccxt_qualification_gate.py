#!/usr/bin/env python3
"""Manual, live-network qualification gate for the CCXT Xret provider.

This is intentionally outside CI. It is a maintained experiment runner: keep
the policy and harness here, but write every run's JSON, logs, and temporary
Xret state to an output directory chosen by the operator (normally /tmp).
"""

from __future__ import annotations

import argparse
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
    "okx",
    "bybit",
    "gate",
    "hashkey",
    "woo",
    "bitrue",
    "coinbase",
    "bitget",
    "mexc",
    "bingx",
    "bitfinex",
    "toobit",
    "hyperliquid",
    "extended",
    "bitvavo",
    "xt",
    "deribit",
    "lighter",
    "apex",
)
PREFERRED_SYMBOLS = ("BTC/USDT", "BTC/USDC", "ETH/USDT", "ETH/USDC", "SOL/USDT", "SOL/USDC")
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
    lifecycle: str = "standard"


@dataclass(slots=True)
class RequestMetrics:
    load_markets_calls: int = 0
    fetch_ohlcv_calls: int = 0
    fetch_ohlcv_rows: int = 0
    fetch_ohlcv_seconds: float = 0.0


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


def plan_cases(
    definitions: dict[str, tuple[Any, ...]],
    *,
    symbol_limit: int,
    target_samples: int,
    preferred: tuple[str, ...],
    seed: int,
    page_bars: int,
) -> list[PlanCase]:
    """Build mandatory risk cases plus a separate statistical sample.

    Mandatory cases never count toward ``target_samples``. Every canonical
    timeframe advertised by each market family gets a structural case, and
    history-derived boundary cases exercise the failure-prone Xret lifecycle.
    A deterministic stratified pool then contributes exactly the requested
    number of statistical cases.
    """
    selected: list[tuple[Any, str]] = []
    all_by_market_timeframe: dict[tuple[str, str], Any] = {}
    for market, values in definitions.items():
        chosen = choose_symbols(values, symbol_limit, preferred)
        for definition in chosen:
            for timeframe in definition.timeframes:
                all_by_market_timeframe.setdefault((market, timeframe), definition)
        for definition in values:
            for timeframe in definition.timeframes:
                all_by_market_timeframe.setdefault((market, timeframe), definition)
        for definition in chosen:
            for timeframe in definition.timeframes:
                selected.append((definition, timeframe))
    cases: list[PlanCase] = []
    seen: set[tuple[str, str, str, str, str]] = set()

    def add(
        definition: Any,
        timeframe: str,
        scenario: str,
        bars: int,
        end_anchor: str,
        end_offset_days: int,
        stratum: str,
        kind: str,
        lifecycle: str = "standard",
    ) -> None:
        identity = definition.identity
        key = (identity.market.value, identity.symbol, timeframe, scenario, kind)
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
                    lifecycle,
                )
            )

    for (_market, timeframe), definition in sorted(
        all_by_market_timeframe.items(),
        key=lambda item: (item[0][0], TIMEFRAME_DAYS.get(item[0][1], 999), item[0][1]),
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
        )

    primary_by_market: dict[str, Any] = {}
    for definition, _timeframe in selected:
        primary_by_market.setdefault(definition.identity.market.value, definition)
    for market, definition in primary_by_market.items():
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
            f"{market}:minimal-range",
            "mandatory",
        )
        add(
            definition,
            shortest,
            "finality-boundary",
            2,
            "recent",
            0,
            f"{market}:closed-bar-boundary",
            "mandatory",
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
                f"{market}:pagination-boundary",
                "mandatory",
            )
        add(
            definition,
            lifecycle_timeframe,
            "month-partition-boundary",
            8,
            "month",
            0,
            f"{market}:calendar-storage",
            "mandatory",
        )
        add(
            definition,
            lifecycle_timeframe,
            "year-boundary",
            8,
            "year",
            0,
            f"{market}:calendar-storage",
            "mandatory",
        )
        add(
            definition,
            lifecycle_timeframe,
            "incremental-left-right",
            24,
            "recent",
            7,
            f"{market}:incremental-sync",
            "mandatory",
            "incremental",
        )
        add(
            definition,
            lifecycle_timeframe,
            "concurrent-same-dataset",
            12,
            "recent",
            14,
            f"{market}:locking",
            "mandatory",
            "concurrent",
        )
        add(
            definition,
            lifecycle_timeframe,
            "reject-zero-length",
            1,
            "recent",
            0,
            f"{market}:invalid-range",
            "mandatory",
            "invalid-zero",
        )
        add(
            definition,
            lifecycle_timeframe,
            "reject-misaligned",
            2,
            "recent",
            0,
            f"{market}:invalid-range",
            "mandatory",
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
                f"{market}:deep-history",
                "mandatory",
            )
        if "1d" in available:
            add(
                definition,
                "1d",
                "long-history-3y",
                365 * 3,
                "recent",
                0,
                f"{market}:deep-history",
                "mandatory",
            )

    rng = random.Random(seed)
    pool = [
        (definition, timeframe, offset, bars)
        for definition, timeframe in selected
        for offset in _STATISTICAL_OFFSETS_DAYS
        for bars in _STATISTICAL_WINDOW_BARS
    ]
    rng.shuffle(pool)
    statistical_count = 0
    for definition, timeframe, offset, bars in pool:
        if statistical_count >= target_samples:
            break
        symbol_class = _symbol_stratum(definition.identity.symbol, preferred)
        add(
            definition,
            timeframe,
            f"sample-o{offset}-b{bars}",
            bars,
            "recent",
            offset,
            f"{symbol_class}:offset-{offset}",
            "statistical",
        )
        statistical_count += 1
    if statistical_count != target_samples:
        raise ValueError(
            f"statistical sample universe has {statistical_count} unique cases, "
            f"fewer than requested {target_samples}"
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


def _estimated_pages(case: PlanCase, page_limit: int) -> int:
    multiplier = 3 if case.lifecycle == "incremental" else 2
    if case.lifecycle.startswith("invalid-"):
        return 0
    return multiplier * math.ceil(case.bars / page_limit)


def _frame_rows(frame: Any) -> list[tuple[Any, ...]]:
    return [
        tuple(row)
        for row in frame.select(["timestamp", "open", "high", "low", "close", "volume"]).iter_rows()
    ]


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
        if fixed_step_ms is None:
            raise AssertionError("misalignment probe requires a fixed-duration timeframe")
        invalid_start = start + timedelta(milliseconds=max(1, fixed_step_ms // 2))
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
            return {"status": "coverage_review", "phases": phase_results}
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
        return {
            "status": "coverage_review",
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
    page_limit: int,
) -> dict[str, Any]:
    start, end = _range(now, case)
    result: dict[str, Any] = {
        "key": _case_key(case),
        "plan": asdict(case),
        "start": _iso(start),
        "end": _iso(end),
        "estimated_pages": _estimated_pages(case, page_limit),
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
        fetched = bars.fetch(start, end)
        _assert_frame_invariants(fetched, timeframe=case.timeframe, start=start, end=end)
        sync = bars.sync(start, end)
        result["sync"] = {
            "is_complete": sync.is_complete,
            "gaps": [str(gap) for gap in sync.gaps],
            "fetched_rows": sync.fetched_rows,
        }
        if not sync.is_complete:
            partial = bars.scan_partial(start, end)
            result.update({"status": "coverage_review", "partial_complete": partial.is_complete})
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
    from xret.data.providers.ccxt import CcxtProvider
    from xret.data.providers.ccxt.markets import scoped_client_id
    from xret.data.providers.ccxt.pagination import _PROFILES

    metrics = RequestMetrics()
    metrics_lock = threading.Lock()

    def instrument(exchange: Any) -> Any:
        original_load_markets = exchange.load_markets
        original_fetch_ohlcv = exchange.fetch_ohlcv

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

        exchange.load_markets = load_markets
        exchange.fetch_ohlcv = fetch_ohlcv
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
        "cases": [],
    }
    exchange_started = datetime.now(UTC)
    try:
        definitions: dict[str, tuple[Any, ...]] = {}
        errors: dict[str, str] = {}
        for name, market in (("spot", Market.SPOT), ("perpetual", Market.PERPETUAL)):
            try:
                definitions[name] = provider.fetch_markets(exchange=exchange_id, market=market)
            except Exception as exc:  # noqa: BLE001 - persisted evidence
                errors[name] = f"{type(exc).__name__}: {exc}"
        result["market_counts"] = {name: len(values) for name, values in definitions.items()}
        result["market_errors"] = errors
        profile_limits = [
            _PROFILES[client_id].max_bars
            for market in (Market.SPOT, Market.PERPETUAL)
            if (client_id := scoped_client_id(exchange_id, market)) in _PROFILES
        ]
        if not profile_limits:
            raise ValueError(f"{exchange_id} has no qualified pagination profile")
        page_bars = min(args.page_limit, min(profile_limits))
        cases = plan_cases(
            definitions,
            symbol_limit=args.symbols,
            target_samples=args.samples,
            preferred=PREFERRED_SYMBOLS,
            seed=args.seed,
            page_bars=page_bars,
        )
        result["planned_cases"] = [
            {**asdict(case), "estimated_pages": _estimated_pages(case, page_bars)} for case in cases
        ]
        result["effective_page_bars"] = page_bars
        result["estimated_pages"] = sum(_estimated_pages(case, page_bars) for case in cases)
        result["plan_counts"] = {
            "mandatory": sum(case.kind == "mandatory" for case in cases),
            "statistical": sum(case.kind == "statistical" for case in cases),
        }
        if args.plan_only:
            result["status"] = "planned"
        else:
            state_root = Path(
                tempfile.mkdtemp(prefix=f"xret-qualification-{exchange_id}-", dir=args.temp_root)
            )
            try:
                now = datetime.now(UTC)
                for case in cases:
                    result["cases"].append(
                        _run_case(exchange_id, provider, case, state_root, now, page_bars)
                    )
            finally:
                if not args.keep_state:
                    shutil.rmtree(state_root, ignore_errors=True)
            result["status"] = (
                "pass"
                if result["cases"] and all(case["status"] == "pass" for case in result["cases"])
                else "candidate"
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
        "--samples",
        type=int,
        help="Statistical cases per exchange; defaults to the configured confidence minimum.",
    )
    parser.add_argument("--confidence", type=float, default=0.999)
    parser.add_argument("--target-failure-rate", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=20260823)
    parser.add_argument("--timeout-ms", type=int, default=15000)
    parser.add_argument("--page-limit", type=int, default=1000)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--backoff", type=float, default=0.5)
    parser.add_argument("--temp-root", type=Path, default=Path("/tmp"))
    parser.add_argument("--keep-state", action="store_true")
    parser.add_argument("--plan-only", action="store_true")
    args = parser.parse_args()
    started_at = datetime.now(UTC)
    try:
        required_samples = required_zero_failure_samples(args.target_failure_rate, args.confidence)
    except ValueError as exc:
        parser.error(str(exc))
    if args.samples is None:
        args.samples = required_samples
    if args.workers < 1 or args.symbols < 1 or args.samples < required_samples:
        parser.error(
            "workers and symbols must be positive; samples must be at least "
            f"{required_samples} for the configured confidence target"
        )
    args.output.mkdir(parents=True, exist_ok=True)
    import ccxt

    known_venues = set(ccxt.exchanges)
    requested = set(args.exchange or ())
    unknown = requested - known_venues
    if unknown:
        parser.error("unknown CCXT exchange(s): " + ", ".join(sorted(unknown)))
    if args.all_ccxt and requested:
        parser.error("--all-ccxt cannot be combined with --exchange")
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
            "samples": args.samples,
            "confidence": args.confidence,
            "target_failure_rate": args.target_failure_rate,
            "seed": args.seed,
            "timeout_ms": args.timeout_ms,
            "page_limit": args.page_limit,
            "retries": args.retries,
            "backoff": args.backoff,
            "plan_only": args.plan_only,
        },
        "gate": "pass" if rows and all(row["status"] == "pass" for row in rows) else "candidate",
        "confidence": {
            "target_failure_rate": args.target_failure_rate,
            "level": args.confidence,
            "zero_failure_independent_samples": required_samples,
            "actual_statistical_samples_per_venue": args.samples,
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
        },
        "venues": len(rows),
        "passed": [row["exchange"] for row in rows if row["status"] == "pass"],
        "candidates": [row["exchange"] for row in rows if row["status"] == "candidate"],
        "failed": [row["exchange"] for row in rows if row["status"] == "error"],
    }
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
