"""Deterministic tests for `xret.data.quality`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest
from xret.data.errors import ProviderError, SyncError
from xret.data.models import BarRequest, MarketIdentity
from xret.data.quality import enforce_ohlcv_batch, evaluate_ohlcv_batch
from xret.data.schema import OHLCV_SCHEMA

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
