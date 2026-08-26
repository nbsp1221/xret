"""Explicit codecs for non-trade canonical family artifacts.

Trade artifact schema 5 remains owned by :mod:`storage.parquet`. New-family
artifacts always carry an explicit family discriminator and independently
versioned artifact schema.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final, cast

import polars as pl
from xret.data.errors import CatalogError, InvalidRequestError, SyncError, XretDataError
from xret.data.funding_quality import enforce_settled_funding
from xret.data.models import (
    DatasetFamily,
    Market,
    MarketIdentity,
    OpenInterestKey,
    ProviderEvidence,
    ReferenceBarKey,
    ReferencePriceKind,
    SettledFundingKey,
    StorageKey,
    YearMonth,
    storage_identity,
)
from xret.data.open_interest_quality import enforce_open_interest
from xret.data.reference_quality import enforce_reference_bars
from xret.data.schema import OPEN_INTEREST_SCHEMA, REFERENCE_BAR_SCHEMA, SETTLED_FUNDING_SCHEMA
from xret.data.storage import paths
from xret.data.storage.parquet import (
    CommittedFile,
    PreparedFile,
    ProviderProvenance,
    _fsync_directory,
    _require_safe_managed_path,
    cleanup_stale_temp_files,
    compute_content_hash,
)

ARTIFACT_SCHEMA_VERSIONS: Final[dict[DatasetFamily, int]] = {
    DatasetFamily.SETTLED_FUNDING: 1,
    DatasetFamily.REFERENCE_BARS: 1,
    DatasetFamily.OPEN_INTEREST: 1,
}

_META_KEYS: Final[frozenset[str]] = frozenset(
    {
        "artifact_schema_version",
        "dataset_family",
        "variant",
        "exchange",
        "symbol",
        "market",
        "settle",
        "timeframe",
        "year",
        "month",
        "row_count",
        "min_timestamp",
        "max_timestamp",
        "provider_name",
        "provider_version",
        "provider_api_version",
        "provider_market_id",
        "native_symbol",
        "reference_target_scope",
        "derivative_linear",
        "derivative_inverse",
        "contract_size",
        "source_field_mapping",
        "contributors",
    }
)


@dataclass(slots=True)
class PreparedFamilyDeletion:
    dataset_key: OpenInterestKey
    year_month: YearMonth
    relative_path: str
    absolute_path: Path
    data_dir: Path
    contributors: tuple[ProviderEvidence, ...]
    published: bool = False


def _validate_contributors(
    contributors: tuple[ProviderEvidence, ...], *, error_cls: type[XretDataError]
) -> tuple[ProviderEvidence, ...]:
    previous: datetime | None = None
    for contributor in contributors:
        if not isinstance(contributor, ProviderEvidence):
            raise error_cls("artifact contributors must be ProviderEvidence values")
        if contributor.contributed_start is None or contributor.contributed_end is None:
            raise error_cls("artifact contributor ranges are required")
        if previous is not None and contributor.contributed_start < previous:
            raise error_cls("artifact contributors must be ordered and non-overlapping")
        previous = contributor.contributed_end
    return contributors


def _contributors_json(contributors: tuple[ProviderEvidence, ...]) -> str:
    _validate_contributors(contributors, error_cls=InvalidRequestError)
    records = []
    for item in contributors:
        assert item.contributed_start is not None and item.contributed_end is not None
        records.append(
            {
                "provider_name": item.provider_name,
                "provider_version": item.provider_version,
                "provider_api_version": item.provider_api_version,
                "native_market_id": item.native_market_id,
                "native_symbol": item.native_symbol,
                "normalizations": list(item.normalizations),
                "derivative_linear": item.derivative_linear,
                "derivative_inverse": item.derivative_inverse,
                "contract_size": item.contract_size,
                "source_field_mapping": item.source_field_mapping,
                "start": item.contributed_start.isoformat(),
                "end": item.contributed_end.isoformat(),
                "source_route": item.source_route,
                "object_key": item.object_key,
                "checksum_algorithm": item.checksum_algorithm,
                "checksum_value": item.checksum_value,
                "source_revision": item.source_revision,
                "retrieved_at": item.retrieved_at.isoformat() if item.retrieved_at else None,
                "source_rows": item.source_rows,
                "canonical_rows": item.canonical_rows,
                "duplicate_rows": item.duplicate_rows,
            }
        )
    return json.dumps(records, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _contributors_from_json(path: Path, value: str) -> tuple[ProviderEvidence, ...]:
    try:
        raw = json.loads(value)
        if not isinstance(raw, list):
            raise ValueError("manifest is not a list")
        contributors = tuple(
            ProviderEvidence(
                provider_name=item["provider_name"],
                provider_version=item["provider_version"],
                provider_api_version=item["provider_api_version"],
                native_market_id=item["native_market_id"],
                native_symbol=item["native_symbol"],
                normalizations=tuple(item["normalizations"]),
                derivative_linear=item["derivative_linear"],
                derivative_inverse=item["derivative_inverse"],
                contract_size=item["contract_size"],
                source_field_mapping=item["source_field_mapping"],
                contributed_start=datetime.fromisoformat(item["start"]),
                contributed_end=datetime.fromisoformat(item["end"]),
                source_route=item["source_route"],
                object_key=item["object_key"],
                checksum_algorithm=item["checksum_algorithm"],
                checksum_value=item["checksum_value"],
                source_revision=item["source_revision"],
                retrieved_at=datetime.fromisoformat(item["retrieved_at"])
                if item["retrieved_at"]
                else None,
                source_rows=item["source_rows"],
                canonical_rows=item["canonical_rows"],
                duplicate_rows=item["duplicate_rows"],
            )
            for item in raw
        )
        return _validate_contributors(contributors, error_cls=CatalogError)
    except (KeyError, TypeError, ValueError, XretDataError) as exc:
        raise CatalogError(f"{path}: invalid contributor manifest") from exc


def _schema_and_timestamp(key: StorageKey) -> tuple[pl.Schema, str]:
    if isinstance(key, SettledFundingKey):
        return SETTLED_FUNDING_SCHEMA, "effective_at"
    if isinstance(key, ReferenceBarKey):
        return REFERENCE_BAR_SCHEMA, "timestamp"
    if isinstance(key, OpenInterestKey):
        return OPEN_INTEREST_SCHEMA, "timestamp"
    raise InvalidRequestError("family artifact codec does not accept trade-bar keys")


def _validate(
    frame: pl.DataFrame,
    key: StorageKey,
    year_month: YearMonth,
    error_cls: type[XretDataError],
) -> str:
    schema, timestamp = _schema_and_timestamp(key)
    if frame.schema != schema:
        raise error_cls(f"family artifact schema mismatch: got {frame.schema!r}")
    if isinstance(key, SettledFundingKey):
        enforce_settled_funding(frame, key, error_cls=error_cls)
    elif isinstance(key, ReferenceBarKey):
        enforce_reference_bars(frame, key, error_cls=error_cls)
    elif isinstance(key, OpenInterestKey):
        enforce_open_interest(frame, key, error_cls=error_cls)
    if frame.height == 0:
        raise error_cls("cannot commit an empty family artifact")
    if frame.filter(
        (pl.col(timestamp).dt.year() != year_month.year)
        | (pl.col(timestamp).dt.month() != year_month.month)
    ).height:
        raise error_cls(f"family artifact contains rows outside {year_month}")
    return timestamp


def _key_from_metadata(path: Path, metadata: dict[str, str]) -> tuple[StorageKey, YearMonth, int]:
    missing = _META_KEYS.difference(metadata)
    unknown = set(metadata).difference(_META_KEYS | {"ARROW:schema"})
    if missing:
        raise CatalogError(f"{path}: missing required family metadata: {sorted(missing)!r}")
    if unknown:
        raise CatalogError(f"{path}: unsupported family metadata keys: {sorted(unknown)!r}")
    try:
        family = DatasetFamily(metadata["dataset_family"])
        if family is DatasetFamily.TRADE_BARS:
            raise ValueError("trade bars cannot use a family artifact")
        version = int(metadata["artifact_schema_version"])
        if version != ARTIFACT_SCHEMA_VERSIONS[family]:
            raise ValueError("unsupported family artifact schema")
        market_identity = MarketIdentity(
            exchange=metadata["exchange"],
            symbol=metadata["symbol"],
            market=Market(metadata["market"]),
            settle=metadata["settle"],
        )
        variant = metadata["variant"]
        timeframe = metadata["timeframe"]
        if family is DatasetFamily.SETTLED_FUNDING:
            if variant or timeframe:
                raise ValueError("funding variant/timeframe must be empty")
            key: StorageKey = SettledFundingKey(identity=market_identity)
        elif family is DatasetFamily.REFERENCE_BARS:
            key = ReferenceBarKey(
                identity=market_identity,
                kind=ReferencePriceKind(variant),
                timeframe=timeframe,
            )
        else:
            if variant:
                raise ValueError("open-interest variant must be empty")
            key = OpenInterestKey(identity=market_identity, timeframe=timeframe)
        year_month = YearMonth(int(metadata["year"]), int(metadata["month"]))
    except (KeyError, TypeError, ValueError, XretDataError) as exc:
        raise CatalogError(f"{path}: invalid family identity metadata") from exc
    return key, year_month, version


def _validate_provider_semantics(
    key: StorageKey,
    provider: ProviderProvenance,
    contributors: tuple[ProviderEvidence, ...],
) -> None:
    """Validate family-specific provenance needed to interpret canonical rows."""
    if isinstance(key, OpenInterestKey):
        evidence = (provider, *contributors)
        for item in evidence:
            if item.derivative_linear is not True or item.derivative_inverse is not False:
                raise CatalogError("open-interest artifact requires linear, non-inverse provenance")
            try:
                contract_size = Decimal(item.contract_size or "")
            except (InvalidOperation, ValueError) as exc:
                raise CatalogError(
                    "open-interest artifact has invalid contract-size provenance"
                ) from exc
            if not contract_size.is_finite() or contract_size <= 0:
                raise CatalogError("open-interest artifact has invalid contract-size provenance")
        return

    if any(
        value not in (None, "")
        for value in (
            provider.derivative_linear,
            provider.derivative_inverse,
            provider.contract_size,
            provider.source_field_mapping,
        )
    ):
        raise CatalogError("non-OI family artifact contains derivative provenance")
    if isinstance(key, SettledFundingKey) and provider.reference_target_scope:
        raise CatalogError("funding artifact contains reference-target provenance")
    if isinstance(key, ReferenceBarKey):
        allowed_scopes = (
            ("contract", "pair") if key.kind is ReferencePriceKind.INDEX else ("contract",)
        )
        if provider.reference_target_scope not in allowed_scopes:
            raise CatalogError("reference artifact has invalid target-scope provenance")


def read_family_committed_file(data_dir: Path, path: Path) -> CommittedFile:
    """Deep-read one explicit ``_xret`` family artifact."""
    if not paths.is_canonical_month_file_path(data_dir, path):
        raise CatalogError(f"{path}: malformed reserved family path")
    try:
        frame = pl.read_parquet(path)
        metadata = pl.read_parquet_metadata(path)
    except Exception as exc:
        raise CatalogError(f"failed to read {path}: {exc}") from exc
    key, year_month, version = _key_from_metadata(path, metadata)
    expected_path = paths.month_file_path(data_dir, key, year_month)
    if path.resolve() != expected_path.resolve():
        raise CatalogError(f"{path}: family metadata does not match canonical path {expected_path}")
    timestamp = _validate(frame, key, year_month, CatalogError)
    row_count = frame.height
    minimum = cast("datetime", frame.get_column(timestamp).min())
    maximum = cast("datetime", frame.get_column(timestamp).max())
    identity = storage_identity(key)
    expected = {
        "artifact_schema_version": str(version),
        "dataset_family": identity.family.value,
        "variant": identity.variant,
        "exchange": identity.exchange,
        "symbol": identity.symbol,
        "market": identity.market.value,
        "settle": identity.settle,
        "timeframe": identity.timeframe,
        "year": f"{year_month.year:04d}",
        "month": f"{year_month.month:02d}",
        "row_count": str(row_count),
        "min_timestamp": minimum.isoformat(),
        "max_timestamp": maximum.isoformat(),
    }
    for name, value in expected.items():
        if metadata.get(name) != value:
            raise CatalogError(f"{path}: metadata {name!r} does not match file rows")
    try:
        if metadata["derivative_linear"] not in ("", "true", "false") or metadata[
            "derivative_inverse"
        ] not in ("", "true", "false"):
            raise ValueError("invalid derivative interpretation metadata")
        provider = ProviderProvenance(
            metadata["provider_name"],
            metadata["provider_version"],
            int(metadata["provider_api_version"]),
            metadata["provider_market_id"],
            metadata["native_symbol"],
            metadata["reference_target_scope"],
            (
                None
                if metadata["derivative_linear"] == ""
                else metadata["derivative_linear"] == "true"
            ),
            (
                None
                if metadata["derivative_inverse"] == ""
                else metadata["derivative_inverse"] == "true"
            ),
            metadata["contract_size"],
            metadata["source_field_mapping"],
        )
        contributors = _contributors_from_json(path, metadata["contributors"])
        _validate_provider_semantics(key, provider, contributors)
    except (KeyError, TypeError, ValueError, XretDataError) as exc:
        raise CatalogError(f"{path}: invalid provider metadata") from exc
    return CommittedFile(
        key,
        year_month,
        paths.relative_month_file_path(data_dir, key, year_month),
        path,
        row_count,
        minimum,
        maximum,
        compute_content_hash(path),
        version,
        provider,
        contributors,
    )


def family_metadata(
    key: StorageKey,
    year_month: YearMonth,
    frame: pl.DataFrame,
    provider: ProviderProvenance,
    contributors: tuple[ProviderEvidence, ...] = (),
) -> dict[str, str]:
    """Build exact metadata after deep validation for a later publisher."""
    timestamp = _validate(frame, key, year_month, InvalidRequestError)
    identity = storage_identity(key)
    if identity.family is DatasetFamily.TRADE_BARS:
        raise InvalidRequestError("trade bars use artifact schema 5")
    minimum = cast("datetime", frame.get_column(timestamp).min())
    maximum = cast("datetime", frame.get_column(timestamp).max())
    return {
        "artifact_schema_version": str(ARTIFACT_SCHEMA_VERSIONS[identity.family]),
        "dataset_family": identity.family.value,
        "variant": identity.variant,
        "exchange": identity.exchange,
        "symbol": identity.symbol,
        "market": identity.market.value,
        "settle": identity.settle,
        "timeframe": identity.timeframe,
        "year": f"{year_month.year:04d}",
        "month": f"{year_month.month:02d}",
        "row_count": str(frame.height),
        "min_timestamp": minimum.isoformat(),
        "max_timestamp": maximum.isoformat(),
        "provider_name": provider.name,
        "provider_version": provider.version,
        "provider_api_version": str(provider.api_version),
        "provider_market_id": provider.market_id,
        "native_symbol": provider.native_symbol,
        "reference_target_scope": provider.reference_target_scope,
        "derivative_linear": (
            "" if provider.derivative_linear is None else str(provider.derivative_linear).lower()
        ),
        "derivative_inverse": (
            "" if provider.derivative_inverse is None else str(provider.derivative_inverse).lower()
        ),
        "contract_size": provider.contract_size,
        "source_field_mapping": provider.source_field_mapping,
        "contributors": _contributors_json(contributors),
    }


def split_family_by_year_month(
    frame: pl.DataFrame, *, timestamp: str
) -> list[tuple[YearMonth, pl.DataFrame]]:
    """Split nonempty family rows by their economic timestamp month."""
    if frame.height == 0:
        return []
    tagged = frame.with_columns(
        _year=pl.col(timestamp).dt.year(), _month=pl.col(timestamp).dt.month()
    )
    return [
        (YearMonth(year, month), group.drop("_year", "_month"))
        for (year, month), group in tagged.group_by(["_year", "_month"], maintain_order=True)
    ]


def _overlaps(left: ProviderEvidence, right: ProviderEvidence) -> bool:
    assert left.contributed_start is not None and left.contributed_end is not None
    assert right.contributed_start is not None and right.contributed_end is not None
    return (
        left.contributed_start < right.contributed_end
        and right.contributed_start < left.contributed_end
    )


def _merge_contributors(
    existing: tuple[ProviderEvidence, ...], incoming: tuple[ProviderEvidence, ...]
) -> tuple[ProviderEvidence, ...]:
    if not incoming:
        return existing
    retained = list(existing)
    for new in incoming:
        for old in tuple(retained):
            if not _overlaps(old, new):
                continue
            if old.provider_name != new.provider_name:
                raise SyncError(
                    f"provider-owned range belongs to {old.provider_name!r}, "
                    f"not {new.provider_name!r}"
                )
            if (
                old.contributed_start != new.contributed_start
                or old.contributed_end != new.contributed_end
            ):
                raise SyncError("source revision must exactly replace its owned certified interval")
            retained.remove(old)
    merged = tuple(sorted((*retained, *incoming), key=lambda item: item.contributed_start))
    return _validate_contributors(merged, error_cls=SyncError)


def _merge_family(
    existing: pl.DataFrame | None,
    batch: pl.DataFrame,
    *,
    timestamp: str,
    existing_contributors: tuple[ProviderEvidence, ...] = (),
    incoming_contributors: tuple[ProviderEvidence, ...] = (),
) -> tuple[pl.DataFrame, tuple[ProviderEvidence, ...]]:
    contributors = _merge_contributors(existing_contributors, incoming_contributors)
    if existing is None:
        return batch.sort(timestamp), contributors
    if incoming_contributors:
        retained = existing
        for contributor in incoming_contributors:
            assert contributor.contributed_start is not None
            assert contributor.contributed_end is not None
            retained = retained.filter(
                (pl.col(timestamp) < contributor.contributed_start)
                | (pl.col(timestamp) >= contributor.contributed_end)
            )
        parts = [frame for frame in (retained, batch) if not frame.is_empty()]
        if not parts:
            return batch.head(0), contributors
        merged = pl.concat(parts, how="vertical").sort(timestamp)
        if merged.get_column(timestamp).n_unique() != merged.height:
            raise SyncError("conflicting canonical family rows after source revision")
        return merged, contributors
    rows: dict[datetime, dict[str, object]] = {}
    for row in (*existing.rows(named=True), *batch.rows(named=True)):
        row_key = cast("datetime", row[timestamp])
        previous = rows.get(row_key)
        if previous is not None and previous != row:
            raise SyncError(f"conflicting canonical family rows at {row_key.isoformat()}")
        rows[row_key] = row
    return (
        pl.DataFrame([rows[row_key] for row_key in sorted(rows)], schema=batch.schema),
        contributors,
    )


def prepare_open_interest_revision_month(
    data_dir: Path,
    key: OpenInterestKey,
    year_month: YearMonth,
    batch: pl.DataFrame,
    *,
    provider: ProviderProvenance,
    contributors: tuple[ProviderEvidence, ...],
) -> PreparedFile | PreparedFamilyDeletion | None:
    """Prepare a contributor set-replacement, including an empty resulting month."""
    if batch.schema != OPEN_INTEREST_SCHEMA:
        raise InvalidRequestError("family artifact schema mismatch")
    enforce_open_interest(batch, key, error_cls=InvalidRequestError)
    directory = paths.month_dir(data_dir, key, year_month)
    final_path = paths.month_file_path(data_dir, key, year_month)
    _require_safe_managed_path(data_dir, final_path, error_cls=SyncError)
    if not final_path.is_file():
        if batch.is_empty():
            return None
        return prepare_family_month(
            data_dir,
            key,
            year_month,
            batch,
            provider=provider,
            contributors=contributors,
        )
    committed = read_family_committed_file(data_dir, final_path)
    if committed.dataset_key != key:
        raise SyncError(f"{final_path}: canonical family identity changed")
    for field_name in ("derivative_linear", "derivative_inverse", "contract_size"):
        if getattr(committed.provider, field_name) != getattr(provider, field_name):
            raise SyncError(
                f"{final_path}: conflicting open-interest provenance for {field_name!r}"
            )
    merged, merged_contributors = _merge_family(
        pl.read_parquet(final_path),
        batch,
        timestamp="timestamp",
        existing_contributors=committed.contributors,
        incoming_contributors=contributors,
    )
    if merged.is_empty():
        return PreparedFamilyDeletion(
            key,
            year_month,
            paths.relative_month_file_path(data_dir, key, year_month),
            final_path,
            data_dir,
            contributors,
        )
    directory.mkdir(parents=True, exist_ok=True)
    cleanup_stale_temp_files(directory)
    metadata = family_metadata(key, year_month, merged, provider, merged_contributors)
    temp_path = paths.new_temp_path(directory)
    _require_safe_managed_path(data_dir, temp_path, error_cls=SyncError)
    try:
        with temp_path.open("wb") as handle:
            merged.write_parquet(handle, metadata=metadata)
            handle.flush()
            os.fsync(handle.fileno())
        reopened = read_family_committed_file_from_temp(
            data_dir,
            temp_path,
            key,
            year_month,
            provider,
            contributors=merged_contributors,
        )
        physical_hash = compute_content_hash(temp_path)
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise
    return PreparedFile(
        CommittedFile(
            key,
            year_month,
            paths.relative_month_file_path(data_dir, key, year_month),
            final_path,
            reopened.row_count,
            reopened.min_timestamp,
            reopened.max_timestamp,
            physical_hash,
            ARTIFACT_SCHEMA_VERSIONS[DatasetFamily.OPEN_INTEREST],
            provider,
            merged_contributors,
        ),
        temp_path,
        data_dir,
    )


def publish_family_deletion(prepared: PreparedFamilyDeletion) -> None:
    if prepared.published:
        raise SyncError("prepared family deletion has already been published")
    _require_safe_managed_path(prepared.data_dir, prepared.absolute_path, error_cls=SyncError)
    prepared.absolute_path.unlink(missing_ok=True)
    prepared.published = True
    _fsync_directory(prepared.absolute_path.parent)


def prepare_family_month(
    data_dir: Path,
    key: StorageKey,
    year_month: YearMonth,
    batch: pl.DataFrame,
    *,
    provider: ProviderProvenance,
    contributors: tuple[ProviderEvidence, ...] = (),
) -> PreparedFile:
    """Prepare one deeply validated family artifact for atomic publication."""
    timestamp = _validate(batch, key, year_month, InvalidRequestError)
    directory = paths.month_dir(data_dir, key, year_month)
    _require_safe_managed_path(data_dir, directory, error_cls=SyncError)
    directory.mkdir(parents=True, exist_ok=True)
    cleanup_stale_temp_files(directory)
    final_path = paths.month_file_path(data_dir, key, year_month)
    _require_safe_managed_path(data_dir, final_path, error_cls=SyncError)
    existing: pl.DataFrame | None = None
    existing_contributors: tuple[ProviderEvidence, ...] = ()
    if final_path.is_file():
        committed = read_family_committed_file(data_dir, final_path)
        if committed.dataset_key != key:
            raise SyncError(f"{final_path}: canonical family identity changed")
        existing_contributors = committed.contributors
        if committed.provider.name != provider.name and not (
            isinstance(key, OpenInterestKey) and contributors and existing_contributors
        ):
            raise SyncError(
                f"{final_path}: source lineage is {committed.provider.name!r}, "
                f"not {provider.name!r}"
            )
        if isinstance(key, OpenInterestKey):
            semantic_fields = (
                "derivative_linear",
                "derivative_inverse",
                "contract_size",
            )
            for field_name in semantic_fields:
                if getattr(committed.provider, field_name) != getattr(provider, field_name):
                    raise SyncError(
                        f"{final_path}: conflicting open-interest provenance for {field_name!r}"
                    )
        existing = pl.read_parquet(final_path)
    merged, merged_contributors = _merge_family(
        existing,
        batch,
        timestamp=timestamp,
        existing_contributors=existing_contributors,
        incoming_contributors=contributors,
    )
    metadata = family_metadata(key, year_month, merged, provider, contributors=merged_contributors)
    temp_path = paths.new_temp_path(directory)
    _require_safe_managed_path(data_dir, temp_path, error_cls=SyncError)
    try:
        with temp_path.open("wb") as handle:
            merged.write_parquet(handle, metadata=metadata)
            try:
                handle.flush()
                os.fsync(handle.fileno())
            except OSError as exc:
                raise SyncError(f"failed to flush prepared artifact {temp_path}") from exc
        reopened = read_family_committed_file_from_temp(
            data_dir,
            temp_path,
            key,
            year_month,
            provider,
            contributors=merged_contributors,
        )
        physical_hash = compute_content_hash(temp_path)
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise
    return PreparedFile(
        CommittedFile(
            key,
            year_month,
            paths.relative_month_file_path(data_dir, key, year_month),
            final_path,
            reopened.row_count,
            reopened.min_timestamp,
            reopened.max_timestamp,
            physical_hash,
            ARTIFACT_SCHEMA_VERSIONS[storage_identity(key).family],
            provider,
            merged_contributors,
        ),
        temp_path,
        data_dir,
    )


def read_family_committed_file_from_temp(
    data_dir: Path,
    path: Path,
    key: StorageKey,
    year_month: YearMonth,
    provider: ProviderProvenance,
    *,
    contributors: tuple[ProviderEvidence, ...] = (),
) -> CommittedFile:
    """Deep-check a prepared family file before it has its canonical path."""
    try:
        frame = pl.read_parquet(path)
        metadata = pl.read_parquet_metadata(path)
    except Exception as exc:
        raise SyncError(f"failed to reopen prepared family artifact {path}: {exc}") from exc
    timestamp = _validate(frame, key, year_month, SyncError)
    expected = family_metadata(key, year_month, frame, provider, contributors=contributors)
    for name, value in expected.items():
        if metadata.get(name) != value:
            raise SyncError(f"prepared family artifact metadata {name!r} differs")
    return CommittedFile(
        key,
        year_month,
        paths.relative_month_file_path(data_dir, key, year_month),
        paths.month_file_path(data_dir, key, year_month),
        frame.height,
        cast("datetime", frame.get_column(timestamp).min()),
        cast("datetime", frame.get_column(timestamp).max()),
        compute_content_hash(path),
        ARTIFACT_SCHEMA_VERSIONS[storage_identity(key).family],
        provider,
        contributors,
    )
