"""Exceptional catalog replacement preserves user-owned uncertainty."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pytest
from xret.data.config import MarketDataConfig
from xret.data.errors import CatalogError
from xret.data.storage import paths
from xret.data.storage.catalog import Catalog, detect_incompatible_state
from xret.data.storage.recovery import rebuild_catalog_state, validate_catalog_state


@contextmanager
def _gate(_: Path):
    yield


def _config(tmp_path: Path) -> MarketDataConfig:
    return MarketDataConfig(state_dir=tmp_path / "state", data_dir=tmp_path / "data")


def _legacy(path: Path) -> None:
    path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE foreign_history (value TEXT)")
        connection.execute("INSERT INTO foreign_history VALUES ('preserve')")


@pytest.mark.parametrize("suffixes", [("-wal",), ("-shm",), ("-wal", "-shm")])
def test_exceptional_rebuild_refuses_each_sidecar_without_changes(
    tmp_path: Path, suffixes: tuple[str, ...]
) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    _legacy(db_path)
    before = db_path.read_bytes()
    parquet_path = config.data_dir / "binance" / "foreign.bin"
    parquet_path.parent.mkdir(parents=True)
    parquet_path.write_bytes(b"canonical parquet")
    parquet_before = parquet_path.read_bytes()
    for suffix in suffixes:
        db_path.with_name(db_path.name + suffix).write_bytes(suffix.encode())
    validation = validate_catalog_state(db_path, config, file_source=lambda: [])
    assert not validation.is_valid
    assert db_path.read_bytes() == before
    for suffix in suffixes:
        assert db_path.with_name(db_path.name + suffix).read_bytes() == suffix.encode()

    with pytest.raises(CatalogError, match="sidecars"):
        rebuild_catalog_state(db_path, config, file_source=lambda: [], gate_factory=_gate)

    assert db_path.read_bytes() == before
    for suffix in suffixes:
        assert db_path.with_name(db_path.name + suffix).read_bytes() == suffix.encode()
    assert parquet_path.read_bytes() == parquet_before


def test_incompatible_catalog_is_reported_without_repair(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    _legacy(db_path)
    before = db_path.read_bytes()
    result = validate_catalog_state(db_path, config)
    assert not result.is_valid
    assert "incompatible" in result.issues[0]
    assert db_path.read_bytes() == before


@pytest.mark.parametrize("contents", [b"", None], ids=["zero-byte", "empty-sqlite"])
def test_schema_less_catalogs_are_incompatible_without_validation_mutation(
    tmp_path: Path, contents: bytes | None
) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    db_path.parent.mkdir(parents=True)
    if contents is None:
        with sqlite3.connect(db_path) as connection:
            connection.execute("PRAGMA user_version = 1")
    else:
        db_path.write_bytes(contents)
    before = db_path.read_bytes()

    assert detect_incompatible_state(db_path)
    result = validate_catalog_state(db_path, config, file_source=lambda: [])

    assert not result.is_valid
    assert db_path.read_bytes() == before


def test_current_catalog_rebuild_allows_an_active_prepared_temp(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    with Catalog.open(db_path):
        pass
    prepared_temp = config.data_dir / f"{paths.TEMP_FILE_PREFIX}active-run"
    prepared_temp.parent.mkdir(parents=True)
    prepared_temp.write_bytes(b"prepared parquet")

    result = rebuild_catalog_state(db_path, config, file_source=lambda: [], gate_factory=_gate)

    assert result.recovered_files == 0
    assert prepared_temp.read_bytes() == b"prepared parquet"


def test_absent_rebuild_refuses_ambiguous_prepared_temp(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    prepared_temp = config.data_dir / f"{paths.TEMP_FILE_PREFIX}unknown"
    prepared_temp.parent.mkdir(parents=True)
    prepared_temp.write_bytes(b"unclassified prepared parquet")

    with pytest.raises(CatalogError, match="ambiguous"):
        rebuild_catalog_state(db_path, config, file_source=lambda: [], gate_factory=_gate)

    assert not db_path.exists()
    assert prepared_temp.read_bytes() == b"unclassified prepared parquet"


def test_absent_rebuild_replaces_only_catalog_and_preserves_foreign_data(tmp_path: Path) -> None:
    config = _config(tmp_path)
    foreign = config.data_dir / "foreign" / "keep.txt"
    foreign.parent.mkdir(parents=True)
    foreign.write_text("user-owned")
    db_path = config.state_dir / "catalog.sqlite3"
    rebuild_catalog_state(db_path, config, file_source=lambda: [], gate_factory=_gate)
    assert db_path.is_file()
    assert foreign.read_text() == "user-owned"


def test_unreadable_catalog_without_sidecars_is_replaced_and_revalidated(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    db_path = config.state_dir / "catalog.sqlite3"
    db_path.parent.mkdir(parents=True)
    db_path.write_bytes(b"not sqlite")
    foreign = config.data_dir / "foreign" / "keep.txt"
    foreign.parent.mkdir(parents=True)
    foreign.write_text("user-owned")

    result = rebuild_catalog_state(db_path, config, file_source=lambda: [], gate_factory=_gate)

    assert result.recovered_files == 0
    with sqlite3.connect(db_path) as connection:
        assert connection.execute("SELECT version FROM schema_migrations").fetchone() is not None
    assert foreign.read_text() == "user-owned"
    assert validate_catalog_state(db_path, config, file_source=lambda: []).is_valid
