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
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from xret.data import quality
from xret.data.config import resolve_config
from xret.data.errors import (
    CatalogError,
    CoverageError,
    InvalidRequestError,
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
from xret.data.providers import DerivativeInterpretation, HistoricalBarProvider
from xret.data.providers import runtime as provider_runtime
from xret.data.providers.discovery import ProviderHandle
from xret.data.providers.runtime import (
    ProviderRuntime,
    ValidatedBarObservation,
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
    ProviderProvenance,
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


def _record_failed_run_best_effort(
    original: Exception,
    *,
    db_path: Path,
    state_dir: Path,
    running_run: IngestionRunMetadata,
) -> None:
    """Best-effort: mark the ingestion run as failed without masking *original*."""
    failed_run = replace(running_run, status="failed", completed_at=_current_clock())
    try:
        with locking.catalog_gate(state_dir), Catalog.open(db_path) as catalog:
            catalog.record_ingestion_run(failed_run)
    except Exception as terminal_error:
        original.add_note(f"also failed to record ingestion run as failed: {terminal_error}")


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
        time_bar.floor(observed_at - provider_runtime.DEFAULT_FINALITY_GRACE),
    )


def _validate_aligned_range(time_bar: TimeBar, start: datetime, end: datetime) -> None:
    if time_bar.floor(start) != start or time_bar.floor(end) != end:
        raise InvalidRequestError(
            f"bar ranges must align to {time_bar.amount}{time_bar.unit!s} boundaries: "
            f"[{start.isoformat()}, {end.isoformat()})"
        )


# --------------------------------------------------------------------------
# Gap coalescing: merge nearby MISSING gaps into wider provider requests
# --------------------------------------------------------------------------

DEFAULT_COALESCE_WINDOW_BARS = 1000


@dataclass(frozen=True, slots=True)
class _FetchWindow:
    """One coalesced provider request and its constituent missing gaps."""

    start: datetime
    end: datetime
    gaps: tuple[CoverageInterval, ...]

    def __post_init__(self) -> None:
        if not self.gaps:
            raise ValueError("fetch window must contain at least one gap")
        if self.start >= self.end:
            raise ValueError("fetch window start must be before end")


def _advance_bars(time_bar: TimeBar, start: datetime, bars: int) -> datetime:
    """Advance *start* by *bars* timeframe boundaries."""
    cursor = start
    for _ in range(bars):
        cursor = time_bar.next_boundary(cursor)
    return cursor


def _coalesce_fetch_ranges(
    gaps: Sequence[CoverageInterval],
    time_bar: TimeBar,
    *,
    max_window_bars: int = DEFAULT_COALESCE_WINDOW_BARS,
) -> tuple[_FetchWindow, ...]:
    """Merge consecutive missing gaps while the total candidate request span,
    including covered bars between them, does not exceed *max_window_bars*.

    A single gap wider than *max_window_bars* is kept as-is (pagination
    handles the size internally).  Input gaps must be sorted and disjoint.
    """
    if max_window_bars <= 0:
        raise ValueError("max_window_bars must be positive")
    if not gaps:
        return ()

    windows: list[_FetchWindow] = []
    window_start = gaps[0].start
    window_end = gaps[0].end
    window_limit = _advance_bars(time_bar, window_start, max_window_bars)
    member_gaps: list[CoverageInterval] = [gaps[0]]

    for gap in gaps[1:]:
        if gap.end <= window_limit:
            window_end = gap.end
            member_gaps.append(gap)
        else:
            windows.append(_FetchWindow(window_start, window_end, tuple(member_gaps)))
            window_start = gap.start
            window_end = gap.end
            window_limit = _advance_bars(time_bar, window_start, max_window_bars)
            member_gaps = [gap]

    windows.append(_FetchWindow(window_start, window_end, tuple(member_gaps)))
    return tuple(windows)


def _filter_frame_to_ranges(
    frame: pl.DataFrame, ranges: tuple[CoverageInterval, ...]
) -> pl.DataFrame:
    """Keep only rows whose timestamp falls within any of *ranges*."""
    if frame.height == 0:
        return frame
    if not ranges:
        return frame.head(0)
    ts = pl.col("timestamp")
    mask = pl.lit(False)
    for r in ranges:
        mask = mask | ((ts >= r.start) & (ts < r.end))
    return frame.filter(mask)


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
    _provider: ProviderHandle | None = field(default=None, init=False, repr=False, compare=False)

    def _effective_config(self) -> MarketDataConfig:
        return self._config if self._config is not None else _resolve_dataset_config()

    def _effective_provider(self) -> HistoricalBarProvider:
        handle = self._provider if self._provider is not None else ProviderHandle(None)
        return handle.get()

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
        end_dt = provider_runtime.default_end(time_bar) if end is None else parse_time_input(end)
        validate_range(start_dt, end_dt)
        _validate_aligned_range(time_bar, start_dt, end_dt)
        request = BarRequest(
            identity=self.identity, timeframe=self.timeframe, start=start_dt, end=end_dt
        )
        return ProviderRuntime(self._effective_provider()).observe(request).frame

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

        Reads local coverage, fetches only missing/gap intervals through
        qualified exhaustive provider windows, validates each fetched batch
        and its observation evidence, publishes monthly Parquet partitions,
        and records their catalog state under the per-dataset lock.
        A fully covered request is an observable no-op: `changed=False`,
        `fetched_rows=0`, `written_partitions=0`. After Parquet publication,
        a catalog failure raises `SyncError` rather than returning a partial
        result. Published Parquet remains canonical; run
        `maintenance.validate()` before retrying.

        Raises:
            UnsupportedMarketError: an unlisted symbol, an unsupported
                timeframe, an unqualified exhaustive pagination contract,
                or ambiguous/absent perpetual settlement inference.
            ProviderError: the provider call failed.
            SyncError: a fetched batch failed fatal data-quality
                validation (P-1), the dataset lock timed out, or catalog
                recording failed after Parquet publication.
        """
        time_bar = TimeBar.parse(self.timeframe)
        start_dt = parse_time_input(start)
        end_dt = provider_runtime.default_end(time_bar) if end is None else parse_time_input(end)
        validate_range(start_dt, end_dt)
        _validate_aligned_range(time_bar, start_dt, end_dt)

        provider_instance: HistoricalBarProvider | None = None
        runtime_context: ProviderRuntime | None = None
        resolved_identity = self.identity
        if self.identity.market is Market.PERPETUAL and self.identity.settle is None:
            provider_instance = self._effective_provider()
            runtime_context = ProviderRuntime(provider_instance)
            resolved_identity = runtime_context.resolve_market(self.identity).identity
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
                    missing_gaps = [
                        gap for gap in known_gaps if gap.status is CoverageStatus.MISSING
                    ]
                    fetch_windows = _coalesce_fetch_ranges(missing_gaps, time_bar)
                    source_lineage = catalog.get_source_lineage(dataset_key)
                    running_run = IngestionRunMetadata(
                        run_id=run_id,
                        dataset_key=dataset_key,
                        requested_start=start_dt,
                        requested_end=end_dt,
                        started_at=started_at,
                        schema_version=SCHEMA_VERSION,
                        status="running",
                    )
                    catalog.record_ingestion_run(running_run)

            observations = []
            quality_warnings: list[tuple[CoverageInterval, quality.QualityFinding]] = []
            prepared = []
            try:
                if fetch_windows:
                    if provider_instance is None:
                        provider_instance = self._effective_provider()
                    if runtime_context is None:
                        runtime_context = ProviderRuntime(provider_instance)
                    descriptor = runtime_context.descriptor
                    if source_lineage is not None and source_lineage != descriptor.name:
                        raise CatalogError(
                            f"dataset source lineage is {source_lineage!r}, not {descriptor.name!r}"
                        )
                resolved_market = None
                for window in fetch_windows:
                    assert provider_instance is not None
                    assert runtime_context is not None
                    request = BarRequest(
                        identity=resolved_identity,
                        timeframe=self.timeframe,
                        start=window.start,
                        end=window.end,
                    )
                    if resolved_market is None:
                        resolved_market = runtime_context.resolve_market(resolved_identity)
                    observation = runtime_context.observe(
                        request,
                        market=resolved_market,
                    )
                    if observation.frame.height:
                        result = quality.enforce_ohlcv_batch(
                            observation.frame,
                            request,
                            error_cls=SyncError,
                        )
                        warning_range = CoverageInterval(
                            window.start, window.end, CoverageStatus.MISSING
                        )
                        quality_warnings.extend(
                            (warning_range, finding) for finding in result.warnings
                        )
                    new_rows = _filter_frame_to_ranges(observation.frame, window.gaps)
                    observations.append((window, observation, new_rows))

                monthly_batches: dict[YearMonth, list[pl.DataFrame]] = {}
                monthly_observations: dict[YearMonth, ValidatedBarObservation] = {}
                source_name = observations[0][1].source.descriptor.name if observations else None
                for _window, _observation, new_rows in observations:
                    if _observation.source.descriptor.name != source_name:
                        raise SyncError("one sync run produced multiple source lineages")
                    for year_month, batch in split_by_year_month(new_rows):
                        monthly_batches.setdefault(year_month, []).append(batch)
                        monthly_observations.setdefault(year_month, _observation)
                for year_month, batches in monthly_batches.items():
                    observation = monthly_observations[year_month]
                    prepared.append(
                        prepare_month(
                            config.data_dir,
                            dataset_key,
                            year_month,
                            pl.concat(batches, how="vertical").sort("timestamp"),
                            provider=ProviderProvenance(
                                name=observation.source.descriptor.name,
                                version=observation.source.descriptor.version,
                                api_version=observation.source.descriptor.api_version,
                                market_id=observation.source.native_market_id,
                                native_symbol=observation.source.native_symbol,
                            ),
                            derivative=(
                                DerivativeInterpretation(
                                    linear=observation.market.derivative.linear,
                                    inverse=observation.market.derivative.inverse,
                                    contract_size=observation.market.derivative.contract_size,
                                )
                                if observation.market.derivative is not None
                                else None
                            ),
                        )
                    )
            except Exception as exc:
                for artifact in prepared:
                    discard_prepared_file(artifact)
                _record_failed_run_best_effort(
                    exc,
                    db_path=db_path,
                    state_dir=config.state_dir,
                    running_run=running_run,
                )
                raise

            coverage_changed = False
            terminal_run: IngestionRunMetadata | None = None
            try:
                with (
                    locking.catalog_gate(config.state_dir),
                    Catalog.open(db_path) as catalog,
                ):
                    catalog.record_ingestion_run(running_run)
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
                        provider_name=(
                            observations[0][1].source.descriptor.name if observations else None
                        ),
                        provider_version=(
                            observations[0][1].source.descriptor.version if observations else None
                        ),
                        provider_api_version=(
                            observations[0][1].source.descriptor.api_version
                            if observations
                            else None
                        ),
                        provider_market_id=(
                            observations[0][1].source.native_market_id if observations else None
                        ),
                        native_symbol=(
                            observations[0][1].source.native_symbol if observations else None
                        ),
                        completed_at=_current_clock(),
                        row_count=sum(obs.frame.height for _, obs, _ in observations),
                    )

                    with catalog.transaction():
                        new_segments: list[CoverageSegment] = []
                        for fetch_window, observation, _nr in observations:
                            present = set(observation.frame.get_column("timestamp").to_list())
                            for missing_gap in fetch_window.gaps:
                                finalizable_end = _finalizable_end(
                                    time_bar, missing_gap.end, observation.evidence_at
                                )
                                if finalizable_end <= missing_gap.start:
                                    continue
                                for _month, month_start, month_end in paths.iter_month_slices(
                                    missing_gap.start, finalizable_end
                                ):
                                    clipped_start = max(missing_gap.start, month_start)
                                    clipped_end = min(finalizable_end, month_end)
                                    new_segments.extend(
                                        _bar_segments_for_range(
                                            present, time_bar, clipped_start, clipped_end
                                        )
                                    )
                        has_canonical_provider_facts = bool(prepared) or bool(new_segments)
                        if has_canonical_provider_facts:
                            catalog.bind_source_lineage(
                                dataset_key,
                                observations[0][1].source.descriptor.name,
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
                    if not isinstance(exc, _CommitUncertainCatalogError):
                        _record_failed_run_best_effort(
                            exc,
                            db_path=db_path,
                            state_dir=config.state_dir,
                            running_run=running_run,
                        )
                    raise
            except Exception as exc:
                for artifact in prepared:
                    discard_prepared_file(artifact)
                if any(artifact.published for artifact in prepared):
                    raise SyncError(
                        "unexpected failure after publishing canonical Parquet data; "
                        "run maintenance.validate() before retrying"
                    ) from exc
                _record_failed_run_best_effort(
                    exc,
                    db_path=db_path,
                    state_dir=config.state_dir,
                    running_run=running_run,
                )
                raise

        return SyncResult(
            dataset_key=dataset_key,
            run_id=run_id,
            changed=bool(prepared) or coverage_changed,
            fetched_rows=sum(obs.frame.height for _, obs, _ in observations),
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
