"""Read-path partition selection and row filtering contracts.

`local_read` answers two questions that are easy to conflate: which month
partitions must hold rows, and which rows the caller may see. See
`local_read._required_months` for why the first cannot be derived from elapsed
coverage time. These tests pin both answers, including the boundary cases that
already worked, so an incorrect widening or narrowing fails closed.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest
from xret.data.errors import CatalogError
from xret.data.models import (
    CoverageInterval,
    CoverageStatus,
    DatasetKey,
    Market,
    YearMonth,
)
from xret.data.schema import OHLCV_SCHEMA
from xret.data.storage import parquet, paths
from xret.data.storage.catalog import (
    CATALOG_FILE_NAME,
    Catalog,
    CoverageSegment,
    FileMetadata,
)
from xret.data.storage.local_read import (
    LocalReadFacts,
    _required_months,
    lazy_frame_for_facts,
    read_local_facts_for_key,
)
from xret.data.storage.parquet import ProviderProvenance


def _at(*parts: int) -> datetime:
    return datetime(*parts, tzinfo=UTC)


def _key(timeframe: str) -> DatasetKey:
    return DatasetKey(
        exchange="binance",
        symbol="BTC/USDT",
        market=Market.SPOT,
        settle="",
        timeframe=timeframe,
    )


def _facts(timeframe: str, start: datetime, end: datetime) -> LocalReadFacts:
    return LocalReadFacts(
        _key(timeframe),
        start,
        end,
        (CoverageInterval(start, end, CoverageStatus.AVAILABLE),),
        (),
    )


# --------------------------------------------------------------------------
# Which partitions a covered interval requires
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("timeframe", "start", "end", "expected"),
    [
        pytest.param("1h", _at(2024, 1, 1), _at(2024, 2, 1), ["2024-01"], id="hourly-whole-month"),
        pytest.param(
            "1h",
            _at(2024, 1, 31, 23),
            _at(2024, 2, 1, 1),
            ["2024-01", "2024-02"],
            id="hourly-spans-two",
        ),
        pytest.param("1d", _at(2024, 1, 1), _at(2024, 2, 1), ["2024-01"], id="daily-whole-month"),
        pytest.param(
            "1M",
            _at(2023, 11, 1),
            _at(2024, 3, 1),
            ["2023-11", "2023-12", "2024-01", "2024-02"],
            id="monthly",
        ),
        pytest.param(
            "1w", _at(2024, 1, 1), _at(2024, 1, 29), ["2024-01"], id="weekly-inside-month"
        ),
        pytest.param(
            "1w", _at(2024, 1, 1), _at(2024, 2, 5), ["2024-01"], id="weekly-crosses-into-february"
        ),
        pytest.param(
            "1w",
            _at(2024, 1, 1),
            _at(2024, 3, 4),
            ["2024-01", "2024-02"],
            id="weekly-crosses-into-march",
        ),
        pytest.param(
            "60d", _at(2024, 1, 1), _at(2024, 5, 1), ["2024-01", "2024-03"], id="bar-skips-a-month"
        ),
    ],
)
def test_required_months_follow_bar_starts(
    timeframe: str, start: datetime, end: datetime, expected: list[str]
) -> None:
    months = _required_months(_facts(timeframe, start, end))

    assert [str(month) for month in months] == expected


def test_a_month_the_coverage_only_elapses_through_is_not_required() -> None:
    """February is touched by coverage but owns no weekly bar start."""
    crossing = _required_months(_facts("1w", _at(2024, 1, 1), _at(2024, 2, 5)))
    reaching = _required_months(_facts("1w", _at(2024, 1, 1), _at(2024, 2, 12)))

    assert YearMonth(year=2024, month=2) not in crossing
    assert YearMonth(year=2024, month=2) in reaching


def test_disjoint_coverage_requires_only_its_own_months() -> None:
    facts = LocalReadFacts(
        _key("1h"),
        _at(2024, 1, 1),
        _at(2024, 4, 1),
        (
            CoverageInterval(_at(2024, 1, 1), _at(2024, 1, 2), CoverageStatus.AVAILABLE),
            CoverageInterval(_at(2024, 3, 1), _at(2024, 3, 2), CoverageStatus.AVAILABLE),
        ),
        (CoverageInterval(_at(2024, 1, 2), _at(2024, 3, 1), CoverageStatus.MISSING),),
    )

    assert [str(month) for month in _required_months(facts)] == ["2024-01", "2024-03"]


def test_absent_coverage_requires_nothing() -> None:
    facts = LocalReadFacts(
        _key("1h"),
        _at(2024, 1, 1),
        _at(2024, 2, 1),
        (),
        (CoverageInterval(_at(2024, 1, 1), _at(2024, 2, 1), CoverageStatus.MISSING),),
    )

    assert _required_months(facts) == []


# --------------------------------------------------------------------------
# Which rows the caller may see
# --------------------------------------------------------------------------


def _publish_january(
    data_dir: Path,
    state_dir: Path,
    *,
    hours: int,
    covered: Sequence[tuple[int, int]],
) -> DatasetKey:
    """Publish `hours` hourly bars while covering only `covered` hour ranges."""
    key = _key("1h")
    timestamps = [_at(2024, 1, 1) + timedelta(hours=index) for index in range(hours)]
    frame = pl.DataFrame(
        {
            "exchange": ["binance"] * hours,
            "symbol": ["BTC/USDT"] * hours,
            "market": ["spot"] * hours,
            "settle": [None] * hours,
            "timeframe": ["1h"] * hours,
            "timestamp": timestamps,
            "open": [1.0] * hours,
            "high": [1.0] * hours,
            "low": [1.0] * hours,
            "close": [1.0] * hours,
            "volume": [1.0] * hours,
        },
        schema=OHLCV_SCHEMA,
    )
    committed = parquet.publish_prepared_file(
        parquet.prepare_month(
            data_dir,
            key,
            YearMonth(year=2024, month=1),
            frame,
            provider=ProviderProvenance(
                name="fake",
                version="1",
                api_version=1,
                market_id="BTCUSDT",
                native_symbol="BTC/USDT",
            ),
        )
    )
    catalog = Catalog.open(state_dir / CATALOG_FILE_NAME)
    try:
        catalog.ensure_dataset(key)
        catalog.set_coverage(
            key,
            [
                CoverageSegment(
                    _at(2024, 1, 1) + timedelta(hours=lower),
                    _at(2024, 1, 1) + timedelta(hours=upper),
                    CoverageStatus.AVAILABLE,
                )
                for lower, upper in covered
            ],
        )
        catalog.record_file(
            FileMetadata(
                dataset_key=key,
                relative_path=committed.relative_path,
                year=2024,
                month=1,
                row_count=committed.row_count,
                min_timestamp=committed.min_timestamp,
                max_timestamp=committed.max_timestamp,
                physical_hash=committed.physical_hash,
                schema_version=committed.schema_version,
            )
        )
    finally:
        catalog.close()
    return key


def _store(tmp_path: Path) -> tuple[Path, Path]:
    state_dir, data_dir = tmp_path / "state", tmp_path / "data"
    state_dir.mkdir()
    data_dir.mkdir()
    return state_dir, data_dir


def test_rows_past_the_covered_tail_are_not_returned(tmp_path: Path) -> None:
    """A file may hold more than coverage claims after an interrupted sync."""
    state_dir, data_dir = _store(tmp_path)
    key = _publish_january(data_dir, state_dir, hours=96, covered=[(0, 24)])

    facts = read_local_facts_for_key(state_dir, data_dir, key, _at(2024, 1, 1), _at(2024, 1, 5))
    frame = lazy_frame_for_facts(data_dir, facts).collect()

    assert frame.height == 24
    assert frame["timestamp"].max() == _at(2024, 1, 1, 23)
    assert [interval.end for interval in facts.covered] == [_at(2024, 1, 2)]
    assert [gap.status for gap in facts.gaps] == [CoverageStatus.MISSING]


def test_rows_inside_an_interior_gap_are_not_returned(tmp_path: Path) -> None:
    """Rows can sit between two covered intervals, and must stay hidden."""
    state_dir, data_dir = _store(tmp_path)
    key = _publish_january(data_dir, state_dir, hours=96, covered=[(0, 12), (72, 84)])

    facts = read_local_facts_for_key(state_dir, data_dir, key, _at(2024, 1, 1), _at(2024, 1, 5))
    frame = lazy_frame_for_facts(data_dir, facts).collect()

    assert len(facts.covered) == 2
    assert frame.height == 24
    assert frame["timestamp"].to_list() == [
        _at(2024, 1, 1) + timedelta(hours=hour) for hour in list(range(12)) + list(range(72, 84))
    ]


def test_a_missing_required_partition_still_fails_closed(tmp_path: Path) -> None:
    state_dir, data_dir = _store(tmp_path)
    key = _publish_january(data_dir, state_dir, hours=24, covered=[(0, 24)])
    facts = read_local_facts_for_key(state_dir, data_dir, key, _at(2024, 1, 1), _at(2024, 1, 2))
    paths.month_file_path(data_dir, key, YearMonth(year=2024, month=1)).unlink()

    with pytest.raises(CatalogError, match="missing canonical file"):
        lazy_frame_for_facts(data_dir, facts)
