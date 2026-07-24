"""`BarDataset`: one bound market identity + timeframe (Decision 2).

Binding (`MarketData.bars(...)`) performs no I/O. Each verb below owns its
own explicit provider/store access; there is no implicit fetch-on-read
(Decision 12):

- `fetch` always uses the remote provider and never touches canonical
  local state.
- `sync` reconciles local coverage against the provider, fetching only
  gaps, and may write canonical Parquet/catalog state.
- `scan` reads complete local coverage only, never the network, and
  raises `CoverageError` on any gap.
- `scan_partial` reads whatever local coverage exists, never the network,
  and reports gaps as structured `DataWarning`/`CoverageInterval` values
  instead of raising.

`state_dir`/`data_dir` resolution honors an explicit `MarketData(config=...)`
threaded privately through `MarketData.bars(...)` (Decision 22): each verb
uses that bound config when present, falling back to
`xret.data.config.resolve_config()` (or the module-private test-seam
override below) only when no explicit config was given.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import polars as pl
from xret.data import provider, quality
from xret.data.config import resolve_config
from xret.data.errors import (
    CatalogError,
    CoverageError,
    InvalidRequestError,
    ProviderError,
    SyncError,
)
from xret.data.models import (
    BarRequest,
    CoverageInterval,
    CoverageStatus,
    DatasetKey,
    DataWarning,
    Market,
    PartialScanResult,
    SyncResult,
    YearMonth,
)
from xret.data.storage import catalog as catalog_storage
from xret.data.storage import local_read, locking, paths
from xret.data.storage.catalog import (
    CATALOG_FILE_NAME,
    SCHEMA_VERSION,
    Catalog,
    CoverageSegment,
    FileMetadata,
    IngestionRunMetadata,
    QualityEventMetadata,
    _CommitUncertainCatalogError,
)
from xret.data.storage.parquet import (
    discard_prepared_file,
    prepare_month,
    publish_prepared_file,
    split_by_year_month,
)
from xret.data.timeframe import TimeBar, parse_time_input, validate_range

if TYPE_CHECKING:
    from xret.data.config import MarketDataConfig
    from xret.data.models import MarketIdentity

__all__ = ["BarDataset"]


# --------------------------------------------------------------------------
# Module-private test seams (P-3): no public config/clock injection on
# `BarDataset` itself (Decision 22).
# --------------------------------------------------------------------------

_config_override: MarketDataConfig | None = None
_clock_override: Callable[[], datetime] | None = None


def _set_config_override(config: MarketDataConfig | None) -> None:
    global _config_override
    _config_override = config


def _set_clock_override(clock: Callable[[], datetime] | None) -> None:
    global _clock_override
    _clock_override = clock


def _reset_test_seams() -> None:
    _set_config_override(None)
    _set_clock_override(None)


def _resolve_dataset_config() -> MarketDataConfig:
    return _config_override if _config_override is not None else resolve_config()


def _current_clock() -> datetime:
    return _clock_override() if _clock_override is not None else datetime.now(UTC)


# --------------------------------------------------------------------------
# Bar-level coverage marking (per-candle, so a provider-internal gap is
# never silently rounded up to a fully `available` range)
# --------------------------------------------------------------------------


def _bar_segments_for_range(
    present: set[datetime], time_bar: TimeBar, start: datetime, end: datetime
) -> list[CoverageSegment]:
    aligned_start = time_bar.floor(start)
    if aligned_start >= end:
        return []
    boundaries = [
        (max(start, bar_start), min(end, bar_end), bar_start)
        for bar_start, bar_end in time_bar.iter_intervals(aligned_start, end)
        if max(start, bar_start) < min(end, bar_end)
    ]
    segments: list[CoverageSegment] = []
    current_status: CoverageStatus | None = None
    current_start: datetime | None = None
    current_end: datetime | None = None
    for segment_start, segment_end, bar_start in boundaries:
        status = CoverageStatus.AVAILABLE if bar_start in present else CoverageStatus.UNAVAILABLE
        if status is current_status and current_end == segment_start:
            current_end = segment_end
        else:
            if current_status is not None:
                assert current_start is not None and current_end is not None
                segments.append(CoverageSegment(current_start, current_end, current_status))
            current_status, current_start, current_end = status, segment_start, segment_end
    if current_status is not None:
        assert current_start is not None and current_end is not None
        segments.append(CoverageSegment(current_start, current_end, current_status))
    return segments


def _finalizable_end(time_bar: TimeBar, request_end: datetime, observed_at: datetime) -> datetime:
    """Return the request boundary whose bars were final at observation time."""
    return min(
        request_end,
        time_bar.floor(observed_at - provider.DEFAULT_FINALITY_GRACE),
    )


def _validate_aligned_range(time_bar: TimeBar, start: datetime, end: datetime) -> None:
    if time_bar.floor(start) != start or time_bar.floor(end) != end:
        raise InvalidRequestError(
            f"bar ranges must align to {time_bar.amount}{time_bar.unit!s} boundaries: "
            f"[{start.isoformat()}, {end.isoformat()})"
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class BarDataset:
    """One bound `MarketIdentity` and timeframe.

    Constructed only through `MarketData.bars(...)`. Immutable and
    I/O-free: it carries stable identity, not an open connection or
    session (Decision 22).
    """

    identity: MarketIdentity
    timeframe: str

    #: Private, unpublished seam (Decision 22): set only by
    #: `MarketData.bars()` via `object.__setattr__` after construction, so
    #: this dataclass's public constructor signature never gains a `config`
    #: parameter. `None` means "no explicit config bound" -- verbs then
    #: fall back to `_resolve_dataset_config()` (module override or
    #: `resolve_config()`).
    _config: MarketDataConfig | None = field(default=None, init=False, repr=False, compare=False)

    def _effective_config(self) -> MarketDataConfig:
        return self._config if self._config is not None else _resolve_dataset_config()

    def __post_init__(self) -> None:
        # Eager syntax validation only (Decision 11); still no I/O.
        TimeBar.parse(self.timeframe)

    # -- fetch -----------------------------------------------------------

    def fetch(
        self,
        start: str | datetime,
        end: str | datetime | None = None,
    ) -> pl.DataFrame:
        """Fetch completed bars directly from the provider (Decision 12).

        Always uses the remote provider. Never reads or writes canonical
        files or catalog coverage. Returns an eager, canonical Polars
        `DataFrame` containing only completed bars (Decision 13) inside the
        half-open `[start, end)` range (Decision 10).

        `start` is required. When `end` is omitted, it resolves to the end
        of the latest completed bar at call time, honoring the provider's
        finalization grace (IR-3).

        Raises:
            UnsupportedMarketError: an unlisted symbol, an unsupported
                timeframe, or ambiguous/absent perpetual settlement
                inference.
            ProviderError: the provider call failed, or the fetched batch
                failed fatal data-quality validation (P-1).
        """
        time_bar = TimeBar.parse(self.timeframe)
        start_dt = parse_time_input(start)
        end_dt = provider.default_end(time_bar) if end is None else parse_time_input(end)
        validate_range(start_dt, end_dt)
        _validate_aligned_range(time_bar, start_dt, end_dt)
        frame = provider.fetch_bars(self.identity, self.timeframe, start_dt, end_dt)
        request = BarRequest(
            identity=self.identity, timeframe=self.timeframe, start=start_dt, end=end_dt
        )
        quality.enforce_ohlcv_batch(frame, request, error_cls=ProviderError)
        return frame

    # -- sync --------------------------------------------------------------

    def sync(
        self,
        start: str | datetime,
        end: str | datetime | None = None,
    ) -> SyncResult:
        """Reconcile local coverage with the provider (Decision 12).

        For a perpetual dataset with `settle` omitted, safe settlement is
        first inferred from live provider metadata (Decision 9) before any
        local coverage or `DatasetKey` is derived, so an omitted perpetual
        `settle` never leaks a storage-layer sentinel error.

        Reads local coverage, fetches only missing/gap intervals, validates
        each fetched batch, publishes monthly Parquet partitions, and records
        their catalog state under the per-dataset lock.
        A fully covered request is an observable no-op: `changed=False`,
        `fetched_rows=0`, `written_partitions=0`. After Parquet publication,
        a catalog failure raises `SyncError` rather than returning a partial
        result. Published Parquet remains canonical; run
        `maintenance.validate()` before retrying.

        Raises:
            UnsupportedMarketError: an unlisted symbol, an unsupported
                timeframe, or ambiguous/absent perpetual settlement
                inference.
            ProviderError: the provider call failed.
            SyncError: a fetched batch failed fatal data-quality
                validation (P-1), the dataset lock timed out, or catalog
                recording failed after Parquet publication.
        """
        time_bar = TimeBar.parse(self.timeframe)
        start_dt = parse_time_input(start)
        end_dt = provider.default_end(time_bar) if end is None else parse_time_input(end)
        validate_range(start_dt, end_dt)
        _validate_aligned_range(time_bar, start_dt, end_dt)

        resolved_identity = self.identity
        if self.identity.market is Market.PERPETUAL and self.identity.settle is None:
            resolved_identity = provider.resolve_identity(self.identity)
        dataset_key = DatasetKey.from_identity(resolved_identity, timeframe=self.timeframe)
        config = self._effective_config()
        run_id = uuid.uuid4().hex
        db_path = config.state_dir / CATALOG_FILE_NAME
        started_at = _current_clock()

        with locking.dataset_lock(config.state_dir, dataset_key):
            with locking.catalog_gate(config.state_dir):
                if (
                    not db_path.exists()
                    and paths.classify_managed_storage(config.data_dir) != "empty"
                ):
                    raise CatalogError(
                        "catalog is absent while managed storage evidence exists; "
                        "run maintenance.rebuild_catalog()"
                    )
                with Catalog.open(db_path) as catalog:
                    _covered, known_gaps = catalog.coverage_and_gaps(dataset_key, start_dt, end_dt)
                    fetch_ranges = [
                        gap for gap in known_gaps if gap.status is CoverageStatus.MISSING
                    ]
                    catalog.record_ingestion_run(
                        IngestionRunMetadata(
                            run_id=run_id,
                            dataset_key=dataset_key,
                            requested_start=start_dt,
                            requested_end=end_dt,
                            started_at=started_at,
                            schema_version=SCHEMA_VERSION,
                            status="running",
                        )
                    )

            observations = []
            quality_warnings: list[tuple[CoverageInterval, quality.QualityFinding]] = []
            for gap in fetch_ranges:
                observation = provider._observe_bars(
                    resolved_identity, self.timeframe, gap.start, gap.end
                )
                if observation.frame.height:
                    result = quality.enforce_ohlcv_batch(
                        observation.frame,
                        BarRequest(
                            identity=resolved_identity,
                            timeframe=self.timeframe,
                            start=gap.start,
                            end=gap.end,
                        ),
                        error_cls=SyncError,
                    )
                    quality_warnings.extend((gap, finding) for finding in result.warnings)
                observations.append((gap, observation))

            prepared = []
            coverage_changed = False
            terminal_run: IngestionRunMetadata | None = None
            try:
                monthly_batches: dict[YearMonth, list[pl.DataFrame]] = {}
                monthly_observations: dict[YearMonth, provider._Observation] = {}
                for _gap, observation in observations:
                    for year_month, batch in split_by_year_month(observation.frame):
                        monthly_batches.setdefault(year_month, []).append(batch)
                        monthly_observations.setdefault(year_month, observation)
                for year_month, batches in monthly_batches.items():
                    observation = monthly_observations[year_month]
                    prepared.append(
                        prepare_month(
                            config.data_dir,
                            dataset_key,
                            year_month,
                            pl.concat(batches, how="vertical").sort("timestamp"),
                            provider=observation.provider,
                            derivative=observation.derivative,
                        )
                    )
                with (
                    locking.catalog_gate(config.state_dir),
                    Catalog.open(db_path) as catalog,
                ):
                    catalog.record_ingestion_run(
                        IngestionRunMetadata(
                            run_id=run_id,
                            dataset_key=dataset_key,
                            requested_start=start_dt,
                            requested_end=end_dt,
                            started_at=started_at,
                            schema_version=SCHEMA_VERSION,
                            status="running",
                        )
                    )
                    for artifact in prepared:
                        publish_prepared_file(artifact)
                    terminal_run = IngestionRunMetadata(
                        run_id=run_id,
                        dataset_key=dataset_key,
                        requested_start=start_dt,
                        requested_end=end_dt,
                        started_at=started_at,
                        schema_version=SCHEMA_VERSION,
                        status="completed",
                        completed_at=_current_clock(),
                        row_count=sum(item.frame.height for _, item in observations),
                    )

                    with catalog.transaction():
                        new_segments: list[CoverageSegment] = []
                        for gap, observation in observations:
                            finalizable_end = _finalizable_end(
                                time_bar, gap.end, observation.completed_at
                            )
                            if finalizable_end <= gap.start:
                                continue
                            present = set(observation.frame.get_column("timestamp").to_list())
                            for _month, month_start, month_end in paths.iter_month_slices(
                                gap.start, finalizable_end
                            ):
                                clipped_start = max(gap.start, month_start)
                                clipped_end = min(finalizable_end, month_end)
                                new_segments.extend(
                                    _bar_segments_for_range(
                                        present, time_bar, clipped_start, clipped_end
                                    )
                                )
                        if new_segments:
                            catalog.apply_coverage_batch(dataset_key, new_segments)
                            coverage_changed = True
                        for artifact in prepared:
                            committed = artifact.committed_file
                            catalog.record_file(
                                FileMetadata(
                                    dataset_key=dataset_key,
                                    relative_path=committed.relative_path,
                                    year=committed.year_month.year,
                                    month=committed.year_month.month,
                                    row_count=committed.row_count,
                                    min_timestamp=committed.min_timestamp,
                                    max_timestamp=committed.max_timestamp,
                                    physical_hash=committed.physical_hash,
                                    schema_version=committed.schema_version,
                                ),
                                run_id=run_id,
                            )
                        for _gap, finding in quality_warnings:
                            catalog.record_quality_event(
                                dataset_key,
                                QualityEventMetadata(
                                    severity=finding.severity,
                                    code=finding.code,
                                    message=finding.message,
                                    run_id=run_id,
                                ),
                            )
                        catalog.record_ingestion_run(terminal_run)
                        covered, gaps = catalog.coverage_and_gaps(dataset_key, start_dt, end_dt)
            except CatalogError as exc:
                for artifact in prepared:
                    discard_prepared_file(artifact)
                if (
                    isinstance(exc, _CommitUncertainCatalogError)
                    and terminal_run is not None
                    and catalog_storage.terminal_commit_is_visible(
                        db_path,
                        terminal_run,
                        tuple(
                            (
                                artifact.committed_file.relative_path,
                                artifact.committed_file.physical_hash,
                            )
                            for artifact in prepared
                        ),
                    )
                ):
                    facts = local_read.read_local_facts_for_key(
                        config.state_dir, config.data_dir, dataset_key, start_dt, end_dt
                    )
                    covered, gaps = facts.covered, facts.gaps
                elif any(artifact.published for artifact in prepared):
                    raise SyncError(
                        "catalog update failed after publishing canonical Parquet data; "
                        "run maintenance.validate() before retrying"
                    ) from exc
                else:
                    raise
            except Exception as exc:
                for artifact in prepared:
                    discard_prepared_file(artifact)
                if any(artifact.published for artifact in prepared):
                    raise SyncError(
                        "unexpected failure after publishing canonical Parquet data; "
                        "run maintenance.validate() before retrying"
                    ) from exc
                raise

        return SyncResult(
            dataset_key=dataset_key,
            run_id=run_id,
            changed=bool(prepared) or coverage_changed,
            fetched_rows=sum(item.frame.height for _, item in observations),
            written_partitions=len(prepared),
            covered=covered,
            gaps=gaps,
            warnings=tuple(
                DataWarning(finding.code, finding.message, gap.start, gap.end)
                for gap, finding in quality_warnings
            ),
        )

    # -- scan / scan_partial ------------------------------------------------

    def scan(
        self,
        start: str | datetime,
        end: str | datetime | None = None,
    ) -> pl.LazyFrame:
        """Read complete local coverage only (Decision 12). Never touches the
        network or mutates local state.

        When `end` is omitted, it resolves to the local `TimeBar` boundary
        at call time -- no provider finalization grace, since this never
        consults the provider.

        Raises:
            InvalidRequestError: `settle` is omitted for a perpetual
                dataset and cannot be uniquely resolved from local
                coverage alone.
            CoverageError: `[start, end)` is not fully covered locally.
        """
        time_bar = TimeBar.parse(self.timeframe)
        start_dt = parse_time_input(start)
        end_dt = time_bar.floor(_current_clock()) if end is None else parse_time_input(end)
        validate_range(start_dt, end_dt)
        _validate_aligned_range(time_bar, start_dt, end_dt)

        config = self._effective_config()
        facts = local_read.read_local_facts(
            config.state_dir,
            config.data_dir,
            self.identity,
            self.timeframe,
            start_dt,
            end_dt,
        )
        if facts.gaps:
            message = (
                f"scan requires complete local coverage for {facts.dataset_key!r} over "
                f"[{start_dt.isoformat()}, {end_dt.isoformat()}); "
                f"{len(facts.gaps)} gap interval(s) remain"
            )
            raise CoverageError(message)
        return local_read.lazy_frame_for_facts(config.data_dir, facts)

    def scan_partial(
        self,
        start: str | datetime,
        end: str | datetime | None = None,
    ) -> PartialScanResult:
        """Read whatever local coverage exists (Decision 12). Never touches
        the network or mutates local state; never raises for gaps.

        When no local rows exist at all, returns a canonical-schema empty
        `LazyFrame`, no covered intervals, and the entire request as a gap.

        Raises:
            InvalidRequestError: `settle` is omitted for a perpetual
                dataset and cannot be uniquely resolved from local
                coverage alone.
        """
        time_bar = TimeBar.parse(self.timeframe)
        start_dt = parse_time_input(start)
        end_dt = time_bar.floor(_current_clock()) if end is None else parse_time_input(end)
        validate_range(start_dt, end_dt)
        _validate_aligned_range(time_bar, start_dt, end_dt)

        config = self._effective_config()
        facts = local_read.read_local_facts(
            config.state_dir,
            config.data_dir,
            self.identity,
            self.timeframe,
            start_dt,
            end_dt,
        )
        if not facts.covered:
            return PartialScanResult(
                dataset_key=facts.dataset_key,
                data=local_read.lazy_frame_for_facts(config.data_dir, facts),
                covered=(),
                gaps=facts.gaps or (CoverageInterval(start_dt, end_dt, CoverageStatus.MISSING),),
                warnings=(),
            )

        lazy = local_read.lazy_frame_for_facts(config.data_dir, facts)
        warnings = tuple(
            DataWarning(
                code=f"coverage.{gap.status.value}",
                message=f"{gap.status.value} gap in local coverage",
                start=gap.start,
                end=gap.end,
            )
            for gap in facts.gaps
        )
        return PartialScanResult(
            dataset_key=facts.dataset_key,
            data=lazy,
            covered=facts.covered,
            gaps=facts.gaps,
            warnings=warnings,
        )
