"""Structured data-quality validation for canonical OHLCV batches.

Fatal findings reject the whole batch: nothing anomalous is ever silently
rewritten or dropped. Canonical trust validation excludes request-range checks
and warning policy; those remain part of ingestion evaluation. Warnings
(expected timeframe gaps, statistical anomalies) never block ingestion.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl
from xret.data.errors import ProviderError, SyncError, XretDataError
from xret.data.models import BarRequest, QualitySeverity
from xret.data.schema import IDENTITY_COLUMNS, OHLC_COLUMNS, OHLCV_SCHEMA
from xret.data.timeframe import TimeBar

__all__ = [
    "QualityFinding",
    "QualityResult",
    "evaluate_canonical_ohlcv",
    "enforce_canonical_ohlcv",
    "evaluate_ohlcv_batch",
    "enforce_ohlcv_batch",
]

#: Absolute fractional close-to-close move treated as a statistical outlier.
DEFAULT_OUTLIER_RETURN_THRESHOLD: float = 0.25


@dataclass(frozen=True, slots=True)
class QualityFinding:
    """One structured fatal or warning quality finding."""

    severity: QualitySeverity
    code: str
    message: str
    row_count: int = 0


@dataclass(frozen=True, slots=True)
class QualityResult:
    """The full set of findings from validating one OHLCV batch."""

    findings: tuple[QualityFinding, ...] = ()

    @property
    def fatal(self) -> tuple[QualityFinding, ...]:
        return tuple(f for f in self.findings if f.severity is QualitySeverity.FATAL)

    @property
    def warnings(self) -> tuple[QualityFinding, ...]:
        return tuple(f for f in self.findings if f.severity is QualitySeverity.WARNING)

    @property
    def is_valid(self) -> bool:
        return not self.fatal


def _fatal(code: str, message: str, *, row_count: int = 0) -> QualityFinding:
    return QualityFinding(
        severity=QualitySeverity.FATAL, code=code, message=message, row_count=row_count
    )


def _warning(code: str, message: str, *, row_count: int = 0) -> QualityFinding:
    return QualityFinding(
        severity=QualitySeverity.WARNING, code=code, message=message, row_count=row_count
    )


# --------------------------------------------------------------------------
# Fatal checks
# --------------------------------------------------------------------------


def _check_schema(data: pl.DataFrame) -> list[QualityFinding]:
    findings: list[QualityFinding] = []
    actual_columns = data.columns
    expected_columns = list(OHLCV_SCHEMA.names())
    missing = [c for c in expected_columns if c not in actual_columns]
    extra = [c for c in actual_columns if c not in expected_columns]
    if missing:
        findings.append(_fatal("schema.missing_columns", f"missing columns: {missing}"))
    if extra:
        findings.append(_fatal("schema.unexpected_columns", f"unexpected columns: {extra}"))
    for column, expected_dtype in OHLCV_SCHEMA.items():
        if column not in actual_columns:
            continue
        actual_dtype = data.schema[column]
        if column == "timestamp":
            # Time-zone correctness is `_check_utc`'s dedicated fatal
            # category; here we only enforce the Datetime/time-unit shape.
            if not (isinstance(actual_dtype, pl.Datetime) and actual_dtype.time_unit == "ms"):
                findings.append(
                    _fatal(
                        "schema.dtype_mismatch",
                        f"column {column!r} has dtype {actual_dtype!r}, expected a "
                        "'ms'-precision Datetime",
                    )
                )
            continue
        if actual_dtype != expected_dtype:
            findings.append(
                _fatal(
                    "schema.dtype_mismatch",
                    f"column {column!r} has dtype {actual_dtype!r}, expected {expected_dtype!r}",
                )
            )
    return findings


def _check_nulls(data: pl.DataFrame) -> list[QualityFinding]:
    findings: list[QualityFinding] = []
    for column in OHLCV_SCHEMA.names():
        if column == "settle":
            # `settle` is legitimately `null` for every `spot` row (IR-2);
            # nullable settle identity is validated by `_check_schema`'s
            # dtype check, not a blanket null-disallowed rule here.
            continue
        null_count = data[column].null_count()
        if null_count:
            findings.append(
                _fatal(
                    "null.disallowed",
                    f"column {column!r} contains {null_count} null value(s)",
                    row_count=null_count,
                )
            )
    return findings


def _check_utc(data: pl.DataFrame) -> list[QualityFinding]:
    dtype = data.schema.get("timestamp")
    if dtype is None:
        return []
    time_zone = getattr(dtype, "time_zone", None)
    if time_zone != "UTC":
        return [
            _fatal(
                "timestamp.not_utc",
                f"timestamp column time zone is {time_zone!r}, expected 'UTC'",
            )
        ]
    return []


def _check_ohlc_invariants(data: pl.DataFrame) -> list[QualityFinding]:
    open_, high, low, close = OHLC_COLUMNS
    invalid = data.filter(
        (pl.col(low) > pl.col(open_))
        | (pl.col(low) > pl.col(close))
        | (pl.col(low) > pl.col(high))
        | (pl.col(high) < pl.col(open_))
        | (pl.col(high) < pl.col(close))
    )
    if invalid.height:
        return [
            _fatal(
                "ohlc.invariant_violation",
                f"{invalid.height} row(s) violate low <= open,close <= high",
                row_count=invalid.height,
            )
        ]
    return []


def _check_finite_positive_prices(data: pl.DataFrame) -> list[QualityFinding]:
    findings: list[QualityFinding] = []
    for column in OHLC_COLUMNS:
        non_finite = data.filter(~pl.col(column).is_finite())
        if non_finite.height:
            findings.append(
                _fatal(
                    "price.non_finite",
                    f"column {column!r} has {non_finite.height} non-finite value(s)",
                    row_count=non_finite.height,
                )
            )
        non_positive = data.filter(pl.col(column) <= 0)
        if non_positive.height:
            findings.append(
                _fatal(
                    "price.non_positive",
                    f"column {column!r} has {non_positive.height} non-positive value(s)",
                    row_count=non_positive.height,
                )
            )
    return findings


def _check_nonnegative_volume(data: pl.DataFrame) -> list[QualityFinding]:
    invalid = data.filter(pl.col("volume") < 0)
    if invalid.height:
        return [
            _fatal(
                "volume.negative",
                f"volume has {invalid.height} negative value(s)",
                row_count=invalid.height,
            )
        ]
    return []


def _check_duplicate_identity(data: pl.DataFrame) -> list[QualityFinding]:
    duplicate_count = data.height - data.unique(subset=list(IDENTITY_COLUMNS)).height
    if duplicate_count:
        return [
            _fatal(
                "identity.duplicate",
                f"{duplicate_count} row(s) duplicate canonical identity {IDENTITY_COLUMNS}",
                row_count=duplicate_count,
            )
        ]
    return []


def _check_ordering(data: pl.DataFrame) -> list[QualityFinding]:
    if data.height < 2:
        return []
    if not data["timestamp"].is_sorted():
        return [
            _fatal(
                "timestamp.unordered",
                "timestamp column is not sorted in strictly ascending order",
            )
        ]
    duplicate_ts = data.height - data["timestamp"].n_unique()
    if duplicate_ts:
        return [
            _fatal(
                "timestamp.non_monotonic",
                f"{duplicate_ts} row(s) share a timestamp with another row",
                row_count=duplicate_ts,
            )
        ]
    return []


def _check_request_range(data: pl.DataFrame, request: BarRequest) -> list[QualityFinding]:
    out_of_range = data.filter(
        (pl.col("timestamp") < request.start) | (pl.col("timestamp") >= request.end)
    )
    if out_of_range.height:
        return [
            _fatal(
                "range.out_of_request",
                f"{out_of_range.height} row(s) fall outside the requested "
                f"[{request.start.isoformat()}, {request.end.isoformat()})",
                row_count=out_of_range.height,
            )
        ]
    return []


def _check_timestamp_alignment(data: pl.DataFrame, time_bar: TimeBar) -> list[QualityFinding]:
    misaligned = sum(
        time_bar.floor(timestamp) != timestamp
        for timestamp in data.get_column("timestamp").to_list()
    )
    if misaligned:
        return [
            _fatal(
                "timestamp.off_timeframe_boundary",
                (
                    f"{misaligned} row(s) are not aligned to "
                    f"{time_bar.amount}{time_bar.unit} boundaries"
                ),
                row_count=misaligned,
            )
        ]
    return []


_FATAL_CHECKS = (
    _check_nulls,
    _check_utc,
    _check_ohlc_invariants,
    _check_finite_positive_prices,
    _check_nonnegative_volume,
    _check_duplicate_identity,
    _check_ordering,
)


# --------------------------------------------------------------------------
# Warning checks
# --------------------------------------------------------------------------


def _check_timeframe_gaps(data: pl.DataFrame, time_bar: TimeBar) -> list[QualityFinding]:
    """Warn on candles missing between the calendar-aware expected boundaries.

    Uses `TimeBar.next_boundary` rather than a fixed `timedelta` diff, so
    calendar units (`w`/`M`) are counted correctly across variable-length
    weeks/months and year rollovers.
    """
    if data.height < 2:
        return []
    timestamps = data["timestamp"].to_list()
    gap_count = 0
    missing_candles = 0
    for previous, current in zip(timestamps, timestamps[1:], strict=False):
        expected = time_bar.next_boundary(previous)
        if current <= expected:
            continue
        gap_count += 1
        cursor = expected
        while cursor < current:
            missing_candles += 1
            cursor = time_bar.next_boundary(cursor)
    if gap_count:
        return [
            _warning(
                "coverage.timeframe_gap",
                f"{gap_count} gap(s) detected spanning approximately "
                f"{missing_candles} missing candle(s)",
                row_count=missing_candles,
            )
        ]
    return []


def _check_statistical_outliers(
    data: pl.DataFrame, *, threshold: float = DEFAULT_OUTLIER_RETURN_THRESHOLD
) -> list[QualityFinding]:
    if data.height < 2:
        return []
    returns = data["close"].pct_change().drop_nulls().abs()
    outliers = returns.filter(returns > threshold)
    if outliers.len():
        return [
            _warning(
                "statistics.outlier_return",
                f"{outliers.len()} close-to-close move(s) exceed {threshold:.0%} in magnitude",
                row_count=outliers.len(),
            )
        ]
    return []


# --------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------


def evaluate_canonical_ohlcv(data: pl.DataFrame, timeframe: str) -> QualityResult:
    """Run the fatal invariant checks required before trusting canonical rows."""
    schema_findings = _check_schema(data)
    if schema_findings:
        return QualityResult(findings=tuple(schema_findings))

    findings: list[QualityFinding] = []
    for check in _FATAL_CHECKS:
        findings.extend(check(data))
    if not any(f.code == "timestamp.not_utc" for f in findings):
        findings.extend(_check_timestamp_alignment(data, TimeBar.parse(timeframe)))
    return QualityResult(findings=tuple(findings))


def enforce_canonical_ohlcv(
    data: pl.DataFrame,
    timeframe: str,
    *,
    error_cls: type[XretDataError] = SyncError,
) -> QualityResult:
    """Validate canonical rows and raise `error_cls` on a fatal invariant."""
    result = evaluate_canonical_ohlcv(data, timeframe)
    if not result.is_valid:
        codes = ", ".join(f"{f.code} ({f.message})" for f in result.fatal)
        raise error_cls(f"canonical OHLCV rows failed fatal quality checks: {codes}")
    return result


def evaluate_ohlcv_batch(data: pl.DataFrame, request: BarRequest) -> QualityResult:
    """Run every fatal and warning check against `data`. Never raises.

    Schema/dtype violations are checked first: if the batch does not match
    `OHLCV_SCHEMA`, downstream checks that assume typed columns are skipped
    to avoid a confusing secondary failure, and only the schema finding(s)
    are returned.
    """
    canonical = evaluate_canonical_ohlcv(data, request.timeframe)
    if any(f.code.startswith("schema.") for f in canonical.findings):
        return canonical
    findings = list(canonical.findings)
    utc_ok = not any(f.code == "timestamp.not_utc" for f in findings)
    if utc_ok:
        # Cross-timezone comparisons against the (UTC-aware) request bounds
        # would raise, so range/gap checks only run once UTC is confirmed.
        time_bar = TimeBar.parse(request.timeframe)
        findings.extend(_check_request_range(data, request))
        findings.extend(_check_timeframe_gaps(data, time_bar))
    findings.extend(_check_statistical_outliers(data))

    return QualityResult(findings=tuple(findings))


def enforce_ohlcv_batch(
    data: pl.DataFrame,
    request: BarRequest,
    *,
    error_cls: type[XretDataError] = ProviderError,
) -> QualityResult:
    """Evaluate `data` and raise `error_cls` if any fatal finding exists.

    `error_cls` maps to the calling verb (P-1): `fetch` uses the default
    `ProviderError`; `sync` passes `error_cls=SyncError` so a fatal
    finding on a sync-fetched batch never surfaces as `ProviderError`.

    Returns the `QualityResult` (which may still carry warnings) when the
    batch passes every fatal check.
    """
    result = evaluate_ohlcv_batch(data, request)
    if not result.is_valid:
        codes = ", ".join(f"{f.code} ({f.message})" for f in result.fatal)
        raise error_cls(f"OHLCV batch failed fatal quality checks: {codes}")
    return result
