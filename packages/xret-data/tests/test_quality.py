"""Deterministic tests for `xret.data.quality`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest
from xret.data.errors import ProviderError, SyncError
from xret.data.models import BarRequest, MarketIdentity
from xret.data.quality import (
    _count_timeframe_gaps_python,
    count_timeframe_gaps,
    enforce_ohlcv_batch,
    evaluate_ohlcv_batch,
)
from xret.data.schema import OHLCV_SCHEMA
from xret.data.timeframe import TimeBar

IDENTITY = MarketIdentity(exchange="fakeex", symbol="BTC/USDT", market="spot")
TIMEFRAME = "1m"


def _request(start: datetime, end: datetime) -> BarRequest:
    return BarRequest(identity=IDENTITY, timeframe=TIMEFRAME, start=start, end=end)


def _frame(
    timestamps: list[datetime],
    *,
    opens: list[float] | None = None,
    highs: list[float] | None = None,
    lows: list[float] | None = None,
    closes: list[float] | None = None,
    volumes: list[float] | None = None,
    exchanges: list[str] | None = None,
    symbols: list[str] | None = None,
    markets: list[str] | None = None,
    settles: list[str | None] | None = None,
    timeframes: list[str] | None = None,
    schema: pl.Schema = OHLCV_SCHEMA,
) -> pl.DataFrame:
    n = len(timestamps)
    data = {
        "exchange": exchanges or [IDENTITY.exchange] * n,
        "symbol": symbols or [IDENTITY.symbol] * n,
        "market": markets or [IDENTITY.market.value] * n,
        # `settle` is legitimately null for every spot row (IR-2).
        "settle": settles or [None] * n,
        "timeframe": timeframes or [TIMEFRAME] * n,
        "timestamp": timestamps,
        "open": opens or [100.0] * n,
        "high": highs or [101.0] * n,
        "low": lows or [99.0] * n,
        "close": closes or [100.5] * n,
        "volume": volumes or [10.0] * n,
    }
    return pl.DataFrame(data, schema=schema)


def _ts(*minutes: int) -> list[datetime]:
    base = datetime(2024, 1, 1, tzinfo=UTC)
    return [base + timedelta(minutes=m) for m in minutes]


# --------------------------------------------------------------------------
# Valid batch
# --------------------------------------------------------------------------


def test_valid_batch_has_no_findings() -> None:
    timestamps = _ts(0, 1, 2)
    data = _frame(timestamps)
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    assert result.is_valid
    assert result.fatal == ()
    assert result.warnings == ()


def test_off_timeframe_timestamp_is_fatal() -> None:
    timestamps = [datetime(2024, 1, 1, 0, 0, 30, tzinfo=UTC)]
    data = _frame(timestamps)
    request = _request(timestamps[0], timestamps[0] + timedelta(minutes=1))

    result = evaluate_ohlcv_batch(data, request)

    assert not result.is_valid
    finding = next(f for f in result.fatal if f.code == "timestamp.off_timeframe_boundary")
    assert finding.row_count == 1


def test_enforce_returns_result_when_batch_is_valid() -> None:
    timestamps = _ts(0, 1)
    data = _frame(timestamps)
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = enforce_ohlcv_batch(data, request)
    assert result.is_valid


def test_null_settle_is_allowed_for_spot_rows() -> None:
    # `settle` is exempt from the blanket null-disallowed rule (IR-2): a
    # spot batch's `settle` column is null for every row by contract.
    timestamps = _ts(0, 1)
    data = _frame(timestamps, settles=[None, None])
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    assert result.is_valid
    assert not any(f.code == "null.disallowed" for f in result.findings)


# --------------------------------------------------------------------------
# Fatal: schema
# --------------------------------------------------------------------------


def test_missing_column_is_fatal() -> None:
    timestamps = _ts(0)
    data = _frame(timestamps).drop("volume")
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    assert not result.is_valid
    assert any(f.code == "schema.missing_columns" for f in result.fatal)


def test_wrong_dtype_is_fatal() -> None:
    timestamps = _ts(0)
    data = _frame(timestamps).with_columns(pl.col("open").cast(pl.Int64))
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    assert not result.is_valid
    assert any(f.code == "schema.dtype_mismatch" for f in result.fatal)


def test_schema_violation_skips_downstream_checks() -> None:
    # A batch with a dtype mismatch should not also report null/order/etc
    # findings for columns whose type is already wrong.
    timestamps = _ts(0)
    data = _frame(timestamps).with_columns(pl.col("open").cast(pl.Int64))
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    codes = {f.code for f in result.findings}
    assert codes == {"schema.dtype_mismatch"}


# --------------------------------------------------------------------------
# Fatal: null / UTC
# --------------------------------------------------------------------------


def test_null_value_is_fatal() -> None:
    timestamps = _ts(0, 1)
    data = _frame(timestamps, volumes=[10.0, None])
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    assert not result.is_valid
    assert any(f.code == "null.disallowed" for f in result.fatal)


def test_non_utc_timestamp_is_fatal() -> None:
    timestamps_naive = [datetime(2024, 1, 1)]  # no tzinfo
    data = pl.DataFrame(
        {
            "exchange": [IDENTITY.exchange],
            "symbol": [IDENTITY.symbol],
            "market": [IDENTITY.market.value],
            "settle": pl.Series("settle", [None], dtype=pl.String),
            "timeframe": [TIMEFRAME],
            "timestamp": pl.Series(timestamps_naive, dtype=pl.Datetime(time_unit="ms")),
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [10.0],
        }
    )
    request = _request(datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, 0, 1, tzinfo=UTC))
    result = evaluate_ohlcv_batch(data, request)
    assert not result.is_valid
    assert any(f.code == "timestamp.not_utc" for f in result.fatal)


# --------------------------------------------------------------------------
# Fatal: OHLC invariants / prices / volume
# --------------------------------------------------------------------------


def test_ohlc_invariant_violation_is_fatal() -> None:
    timestamps = _ts(0)
    # low above high violates the invariant.
    data = _frame(timestamps, lows=[200.0], highs=[101.0])
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    assert not result.is_valid
    assert any(f.code == "ohlc.invariant_violation" for f in result.fatal)


def test_non_positive_price_is_fatal() -> None:
    timestamps = _ts(0)
    data = _frame(timestamps, opens=[0.0])
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    assert not result.is_valid
    assert any(f.code == "price.non_positive" for f in result.fatal)


def test_negative_volume_is_fatal() -> None:
    timestamps = _ts(0)
    data = _frame(timestamps, volumes=[-1.0])
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    assert not result.is_valid
    assert any(f.code == "volume.negative" for f in result.fatal)


def test_zero_volume_is_allowed() -> None:
    timestamps = _ts(0)
    data = _frame(timestamps, volumes=[0.0])
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    assert result.is_valid


# --------------------------------------------------------------------------
# Fatal: identity / ordering / range
# --------------------------------------------------------------------------


def test_duplicate_identity_is_fatal() -> None:
    # Both rows share a null `settle`: identity checks must be null-aware
    # (null == null) so this still counts as a duplicate.
    timestamps = _ts(0, 0)
    data = _frame(timestamps)
    request = _request(timestamps[0], timestamps[0] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    assert not result.is_valid
    assert any(f.code == "identity.duplicate" for f in result.fatal)


def test_unordered_timestamps_is_fatal() -> None:
    timestamps = _ts(1, 0)
    data = _frame(timestamps)
    request = _request(timestamps[1], timestamps[0] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    assert not result.is_valid
    assert any(f.code == "timestamp.unordered" for f in result.fatal)


def test_duplicate_timestamp_without_other_column_differences_is_fatal() -> None:
    # Same timestamp twice is caught by both identity and non-monotonic checks.
    timestamps = _ts(0, 0)
    data = _frame(timestamps)
    request = _request(timestamps[0], timestamps[0] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    codes = {f.code for f in result.fatal}
    assert "identity.duplicate" in codes
    assert "timestamp.non_monotonic" in codes


def test_out_of_request_range_is_fatal() -> None:
    timestamps = _ts(0, 1, 2)
    data = _frame(timestamps)
    request = _request(timestamps[0], timestamps[2])  # half-open end excludes the t=2 row
    result = evaluate_ohlcv_batch(data, request)
    assert not result.is_valid
    finding = next(f for f in result.fatal if f.code == "range.out_of_request")
    assert finding.row_count == 1


# --------------------------------------------------------------------------
# Warnings: gaps / statistical anomalies
# --------------------------------------------------------------------------


def test_timeframe_gap_is_a_warning_not_fatal() -> None:
    timestamps = _ts(0, 1, 5)  # 3-minute gap between t=1 and t=5
    data = _frame(timestamps)
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    assert result.is_valid
    assert any(f.code == "coverage.timeframe_gap" for f in result.warnings)
    gap_finding = next(f for f in result.warnings if f.code == "coverage.timeframe_gap")
    assert gap_finding.row_count == 3  # candles at t=2,3,4 are missing


def test_no_gap_warning_for_contiguous_candles() -> None:
    timestamps = _ts(0, 1, 2, 3)
    data = _frame(timestamps)
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    assert not any(f.code == "coverage.timeframe_gap" for f in result.warnings)


def test_calendar_month_gap_check_uses_actual_month_length_not_a_fixed_duration() -> None:
    # `1M` is a true calendar unit (TimeBar): Jan -> Feb -> Mar are 31- and
    # 29-day (2024 is a leap year) steps, never a fixed 30-day duration, so
    # three exactly-consecutive monthly candles must never warn.
    timestamps = [
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 2, 1, tzinfo=UTC),
        datetime(2024, 3, 1, tzinfo=UTC),
    ]
    data = _frame(timestamps, timeframes=["1M"] * 3)
    request = BarRequest(
        identity=IDENTITY,
        timeframe="1M",
        start=timestamps[0],
        end=datetime(2024, 4, 1, tzinfo=UTC),
    )
    result = evaluate_ohlcv_batch(data, request)
    assert result.is_valid
    assert not any(f.code == "coverage.timeframe_gap" for f in result.warnings)


def test_calendar_month_gap_is_detected_when_a_month_is_actually_missing() -> None:
    timestamps = [datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 3, 1, tzinfo=UTC)]  # Feb missing
    data = _frame(timestamps, timeframes=["1M"] * 2)
    request = BarRequest(
        identity=IDENTITY,
        timeframe="1M",
        start=timestamps[0],
        end=datetime(2024, 4, 1, tzinfo=UTC),
    )
    result = evaluate_ohlcv_batch(data, request)
    assert result.is_valid
    assert any(f.code == "coverage.timeframe_gap" for f in result.warnings)


def test_statistical_outlier_is_a_warning_not_fatal() -> None:
    timestamps = _ts(0, 1, 2)
    data = _frame(
        timestamps,
        closes=[100.0, 100.5, 500.0],  # huge jump on the 3rd candle
        highs=[101.0, 101.5, 501.0],
        lows=[99.0, 99.5, 499.0],
        opens=[100.0, 100.5, 500.0],
    )
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    assert result.is_valid
    assert any(f.code == "statistics.outlier_return" for f in result.warnings)


def test_small_close_moves_do_not_trigger_outlier_warning() -> None:
    timestamps = _ts(0, 1, 2)
    data = _frame(timestamps, closes=[100.0, 100.5, 101.0])
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    assert not any(f.code == "statistics.outlier_return" for f in result.warnings)


# --------------------------------------------------------------------------
# enforce_ohlcv_batch raising: error_cls maps to the calling verb (P-1)
# --------------------------------------------------------------------------


def test_enforce_raises_provider_error_by_default() -> None:
    # `fetch` never passes `error_cls`, so a fetch-path fatal quality
    # finding surfaces as `ProviderError`, folding the retired `QualityError`.
    timestamps = _ts(0)
    data = _frame(timestamps, opens=[-1.0])
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    with pytest.raises(ProviderError, match="price.non_positive"):
        enforce_ohlcv_batch(data, request)


def test_enforce_raises_sync_error_when_mapped_for_the_sync_path() -> None:
    # `sync` passes `error_cls=SyncError` explicitly so the same fatal
    # finding never surfaces as `ProviderError` on that path.
    timestamps = _ts(0)
    data = _frame(timestamps, opens=[-1.0])
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    with pytest.raises(SyncError, match="price.non_positive"):
        enforce_ohlcv_batch(data, request, error_cls=SyncError)


def test_enforce_does_not_raise_for_warnings_only() -> None:
    timestamps = _ts(0, 1, 5)
    data = _frame(timestamps)
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = enforce_ohlcv_batch(data, request)
    assert result.is_valid
    assert result.warnings


# --------------------------------------------------------------------------
# count_timeframe_gaps vectorization (M-B)
# --------------------------------------------------------------------------


def _gap_ts(*minutes: int) -> pl.Series:
    """Timestamps as a Polars Series for gap testing."""
    base = datetime(2024, 1, 1, tzinfo=UTC)
    return pl.Series(
        "timestamp",
        [base + timedelta(minutes=m) for m in minutes],
        dtype=pl.Datetime("ms", "UTC"),
    )


def test_count_timeframe_gaps_contiguous() -> None:
    ts = _gap_ts(0, 1, 2, 3, 4)
    assert count_timeframe_gaps(ts, TimeBar.parse("1m")) == (0, 0)


def test_count_timeframe_gaps_single_gap() -> None:
    # 0,1 then jump to 5: missing 2,3,4 → 3 candles
    ts = _gap_ts(0, 1, 5)
    assert count_timeframe_gaps(ts, TimeBar.parse("1m")) == (1, 3)


def test_count_timeframe_gaps_multiple_gaps() -> None:
    # 0,1 gap 3,4 gap 8 → missing 2 (1 candle) + 5,6,7 (3 candles)
    ts = _gap_ts(0, 1, 3, 4, 8)
    assert count_timeframe_gaps(ts, TimeBar.parse("1m")) == (2, 4)


def test_count_timeframe_gaps_fixed_units() -> None:
    bar_1h = TimeBar.parse("1h")
    base = datetime(2024, 1, 1, tzinfo=UTC)
    # 3 contiguous hours then 1 gap (2 missing)
    ts = pl.Series(
        "timestamp",
        [base + timedelta(hours=h) for h in (0, 1, 2, 5)],
        dtype=pl.Datetime("ms", "UTC"),
    )
    assert count_timeframe_gaps(ts, bar_1h) == (1, 2)

    bar_4h = TimeBar.parse("4h")
    ts4 = pl.Series(
        "timestamp",
        [base + timedelta(hours=h) for h in (0, 4, 12)],
        dtype=pl.Datetime("ms", "UTC"),
    )
    assert count_timeframe_gaps(ts4, bar_4h) == (1, 1)  # missing hour-8


def test_count_timeframe_gaps_weekly() -> None:
    bar = TimeBar.parse("1w")
    # Monday 2024-01-01, Monday 2024-01-08, Monday 2024-01-22 (skip 15th)
    ts = pl.Series(
        "timestamp",
        [
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 8, tzinfo=UTC),
            datetime(2024, 1, 22, tzinfo=UTC),
        ],
        dtype=pl.Datetime("ms", "UTC"),
    )
    assert count_timeframe_gaps(ts, bar) == (1, 1)


def test_count_timeframe_gaps_monthly() -> None:
    bar = TimeBar.parse("1M")
    # Jan, Feb, Mar contiguous → no gap
    ts_ok = pl.Series(
        "timestamp",
        [
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 2, 1, tzinfo=UTC),
            datetime(2024, 3, 1, tzinfo=UTC),
        ],
        dtype=pl.Datetime("ms", "UTC"),
    )
    assert count_timeframe_gaps(ts_ok, bar) == (0, 0)

    # Jan, Mar → Feb missing
    ts_gap = pl.Series(
        "timestamp",
        [
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 3, 1, tzinfo=UTC),
        ],
        dtype=pl.Datetime("ms", "UTC"),
    )
    assert count_timeframe_gaps(ts_gap, bar) == (1, 1)

    # Dec → Jan year rollover
    ts_rollover = pl.Series(
        "timestamp",
        [
            datetime(2024, 12, 1, tzinfo=UTC),
            datetime(2025, 1, 1, tzinfo=UTC),
        ],
        dtype=pl.Datetime("ms", "UTC"),
    )
    assert count_timeframe_gaps(ts_rollover, bar) == (0, 0)

    # Leap year Feb
    ts_leap = pl.Series(
        "timestamp",
        [
            datetime(2024, 2, 1, tzinfo=UTC),
            datetime(2024, 3, 1, tzinfo=UTC),
        ],
        dtype=pl.Datetime("ms", "UTC"),
    )
    assert count_timeframe_gaps(ts_leap, bar) == (0, 0)


def test_count_timeframe_gaps_empty_and_single() -> None:
    bar = TimeBar.parse("1m")
    empty = pl.Series("timestamp", [], dtype=pl.Datetime("ms", "UTC"))
    assert count_timeframe_gaps(empty, bar) == (0, 0)

    single = pl.Series(
        "timestamp", [datetime(2024, 1, 1, tzinfo=UTC)], dtype=pl.Datetime("ms", "UTC")
    )
    assert count_timeframe_gaps(single, bar) == (0, 0)


def test_count_timeframe_gaps_boundary_values() -> None:
    bar = TimeBar.parse("1m")
    step_ms = 60_000
    base = datetime(2024, 1, 1, tzinfo=UTC)
    base_ms = int(base.timestamp() * 1000)

    # diff == step → no gap
    ts_exact = pl.Series(
        "timestamp", [base, base + timedelta(minutes=1)], dtype=pl.Datetime("ms", "UTC")
    )
    assert count_timeframe_gaps(ts_exact, bar) == (0, 0)

    # diff == step + 1ms → 1 gap, 1 missing
    ts_plus1 = pl.Series(
        "timestamp",
        [base, datetime.fromtimestamp((base_ms + step_ms + 1) / 1000, tz=UTC)],
        dtype=pl.Datetime("ms", "UTC"),
    )
    assert count_timeframe_gaps(ts_plus1, bar) == (1, 1)

    # diff == 2 * step → 1 gap, 1 missing
    ts_2x = pl.Series(
        "timestamp", [base, base + timedelta(minutes=2)], dtype=pl.Datetime("ms", "UTC")
    )
    assert count_timeframe_gaps(ts_2x, bar) == (1, 1)

    # diff == 2 * step + 1ms → 1 gap, 2 missing
    ts_2x_plus1 = pl.Series(
        "timestamp",
        [base, datetime.fromtimestamp((base_ms + 2 * step_ms + 1) / 1000, tz=UTC)],
        dtype=pl.Datetime("ms", "UTC"),
    )
    assert count_timeframe_gaps(ts_2x_plus1, bar) == (1, 2)


def test_count_timeframe_gaps_matches_python_off_boundary() -> None:
    """Off-boundary timestamps fall back to Python loop and match."""

    bar = TimeBar.parse("1m")
    base = datetime(2024, 1, 1, tzinfo=UTC)
    # Off-boundary: 30 seconds offset
    ts = pl.Series(
        "timestamp",
        [
            base + timedelta(seconds=30),
            base + timedelta(seconds=90),
            base + timedelta(seconds=210),
        ],
        dtype=pl.Datetime("ms", "UTC"),
    )
    result = count_timeframe_gaps(ts, bar)
    expected = _count_timeframe_gaps_python(ts, bar)
    assert result == expected


def test_count_timeframe_gaps_matches_python_unordered() -> None:
    """Unordered timestamps fall back to Python loop and match."""

    bar = TimeBar.parse("1m")
    base = datetime(2024, 1, 1, tzinfo=UTC)
    ts = pl.Series(
        "timestamp",
        [
            base + timedelta(minutes=2),
            base + timedelta(minutes=0),
            base + timedelta(minutes=5),
        ],
        dtype=pl.Datetime("ms", "UTC"),
    )
    result = count_timeframe_gaps(ts, bar)
    expected = _count_timeframe_gaps_python(ts, bar)
    assert result == expected


# --------------------------------------------------------------------------
# _check_finite_positive_prices vectorization (M-5)
# --------------------------------------------------------------------------


def test_finite_positive_all_clean() -> None:
    timestamps = _ts(0, 1, 2)
    data = _frame(timestamps)
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    assert not any(f.code.startswith("price.") for f in result.fatal)


def test_finite_positive_empty_frame() -> None:
    data = _frame([])
    request = _request(_ts(0)[0], _ts(1)[0])
    result = evaluate_ohlcv_batch(data, request)
    assert not any(f.code.startswith("price.") for f in result.fatal)


@pytest.mark.parametrize(
    ("value", "expected_codes"),
    [
        (float("nan"), ["price.non_finite"]),
        (float("inf"), ["price.non_finite"]),
        (float("-inf"), ["price.non_finite", "price.non_positive"]),
        (-1.0, ["price.non_positive"]),
        (0.0, ["price.non_positive"]),
    ],
)
def test_finite_positive_price_classification(value: float, expected_codes: list[str]) -> None:
    timestamps = _ts(0)
    data = _frame(timestamps, opens=[value])
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    open_findings = [f for f in result.fatal if "'open'" in f.message]
    assert [f.code for f in open_findings] == expected_codes


def test_finite_positive_multiple_columns_order() -> None:
    timestamps = _ts(0)
    data = _frame(
        timestamps,
        opens=[float("nan")],
        highs=[float("-inf")],
        lows=[-1.0],
        closes=[0.0],
    )
    request = _request(timestamps[0], timestamps[-1] + timedelta(minutes=1))
    result = evaluate_ohlcv_batch(data, request)
    price_findings = [f for f in result.fatal if f.code.startswith("price.")]
    codes_and_columns = [(f.code, f.message.split("'")[1]) for f in price_findings]
    assert codes_and_columns == [
        ("price.non_finite", "open"),
        ("price.non_finite", "high"),
        ("price.non_positive", "high"),
        ("price.non_positive", "low"),
        ("price.non_positive", "close"),
    ]
    for f in price_findings:
        assert f.row_count == 1
