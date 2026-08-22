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
    window: str
    days: float
    stratum: str


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
) -> list[PlanCase]:
    """Build a deterministic all-timeframe smoke plan plus risk samples.

    Every canonical timeframe advertised by every selected market family gets
    a smoke case. Remaining cases are filled deterministically from a
    stratified symbol/time-window pool until the confidence target is met.
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
    seen: set[tuple[str, str, str, str]] = set()

    def add(definition: Any, timeframe: str, window: str, days: float, stratum: str) -> None:
        identity = definition.identity
        key = (identity.market.value, identity.symbol, timeframe, window)
        if key not in seen:
            seen.add(key)
            cases.append(
                PlanCase(
                    identity.market.value,
                    identity.symbol,
                    identity.settle,
                    timeframe,
                    window,
                    days,
                    stratum,
                )
            )

    for (_market, timeframe), definition in sorted(
        all_by_market_timeframe.items(),
        key=lambda item: (item[0][0], TIMEFRAME_DAYS.get(item[0][1], 999), item[0][1]),
    ):
        add(
            definition,
            timeframe,
            "smoke",
            min(7, max(1 / 86400, _timeframe_days(timeframe) * 128)),
            "timeframe-complete",
        )

    primary_by_market: dict[str, Any] = {}
    for definition, _timeframe in selected:
        primary_by_market.setdefault(definition.identity.market.value, definition)
    for market, definition in primary_by_market.items():
        available = definition.timeframes
        if "1h" in available:
            add(definition, "1h", "deep", 365, f"{market}:deep-history")
        if "1d" in available:
            add(definition, "1d", "deep", 1095, f"{market}:deep-history")

    rng = random.Random(seed)
    pool = list(selected)
    rng.shuffle(pool)
    for definition, timeframe in pool:
        if len(cases) >= target_samples:
            break
        symbol_class = _symbol_stratum(definition.identity.symbol, preferred)
        add(
            definition,
            timeframe,
            "recent",
            min(30, max(1 / 86400, _timeframe_days(timeframe) * 96)),
            f"{symbol_class}:recent",
        )
        if len(cases) >= target_samples:
            break
        add(
            definition,
            timeframe,
            "boundary",
            min(1095, max(1 / 86400, _timeframe_days(timeframe) * 512)),
            f"{symbol_class}:boundary",
        )
    return cases


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _range(now: datetime, case: PlanCase) -> tuple[datetime, datetime]:
    from xret.data.timeframe import TimeBar

    bar = TimeBar.parse(case.timeframe)
    end = now.replace(second=0, microsecond=0)
    end = bar.floor(end)
    start = bar.floor(end - timedelta(days=case.days))
    return start, end


def _estimated_pages(case: PlanCase, page_limit: int) -> int:
    bars = max(1, math.ceil(case.days / _timeframe_days(case.timeframe)))
    return math.ceil(bars / page_limit)


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
    return f"{case.market}:{case.symbol}:{case.settle or ''}:{case.timeframe}:{case.window}"


def _run_case(
    exchange_id: str,
    provider: Any,
    case: PlanCase,
    state_root: Path,
    now: datetime,
    page_limit: int,
) -> dict[str, Any]:
    from xret.data import MarketData, MarketDataConfig

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
        market_data = MarketData(
            config=MarketDataConfig(state_dir=directory / "state", data_dir=directory / "data"),
            provider=provider,
        )
        bars = market_data.bars(
            exchange=exchange_id,
            symbol=case.symbol,
            market=case.market,
            settle=case.settle,
            timeframe=case.timeframe,
        )
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
        scanned = bars.scan(start, end).collect()
        _assert_frame_invariants(scanned, timeframe=case.timeframe, start=start, end=end)
        no_op = bars.sync(start, end)
        if no_op.changed or no_op.fetched_rows or no_op.written_partitions:
            raise AssertionError("identical synchronization was not a no-op")
        partial = bars.scan_partial(start, end)
        if not partial.is_complete:
            raise AssertionError("partial scan disagrees with strict scan")
        market_data.maintenance.validate()
        market_data.maintenance.rebuild_catalog()
        market_data.maintenance.validate()
        rebuilt = bars.scan(start, end).collect()
        if rebuilt.shape != scanned.shape or not rebuilt.equals(scanned):
            raise AssertionError("catalog rebuild changed strict scan result")
        result.update({"status": "pass", "fetch_rows": fetched.height, "scan_rows": scanned.height})
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

    def factory(client_id: str) -> Any:
        return getattr(ccxt, client_id)({"enableRateLimit": True, "timeout": args.timeout_ms})

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
        cases = plan_cases(
            definitions,
            symbol_limit=args.symbols,
            target_samples=args.samples,
            preferred=PREFERRED_SYMBOLS,
            seed=args.seed,
        )
        result["planned_cases"] = [
            {**asdict(case), "estimated_pages": _estimated_pages(case, args.page_limit)}
            for case in cases
        ]
        result["estimated_pages"] = sum(_estimated_pages(case, args.page_limit) for case in cases)
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
                        _run_case(exchange_id, provider, case, state_root, now, args.page_limit)
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
    (output / f"{exchange_id}.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False, default=str) + "\n"
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, required=True, help="Run artifact directory; use /tmp for live runs."
    )
    parser.add_argument("--exchange", action="append", choices=VENUES)
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
        default=59,
        help="Minimum planned cases per exchange, including timeframe smoke cases.",
    )
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
    if args.workers < 1 or args.symbols < 1 or args.samples < 1:
        parser.error("workers, symbols, and samples must be positive")
    args.output.mkdir(parents=True, exist_ok=True)
    venues = args.exchange or VENUES
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
            "seed": args.seed,
            "timeout_ms": args.timeout_ms,
            "page_limit": args.page_limit,
            "retries": args.retries,
            "backoff": args.backoff,
            "plan_only": args.plan_only,
        },
        "gate": "pass" if rows and all(row["status"] == "pass" for row in rows) else "candidate",
        "confidence": {
            "target_failure_rate": 0.05,
            "level": 0.95,
            "zero_failure_independent_samples": required_zero_failure_samples(0.05, 0.95),
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
