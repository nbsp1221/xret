"""Canonical readable on-disk paths for Xret Parquet data.

Parquet metadata is the authoritative dataset identity. Paths are deterministic,
readable projections and are deliberately not decoded back into identities.

Layout relative to ``data_dir``::

    <exchange>/<market>/<instrument-slug>/<timeframe>/year=<YYYY>/month=<MM>/data.parquet
"""

from __future__ import annotations

import re
import unicodedata
import uuid
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Final

from xret.data.models import DatasetKey, Market, YearMonth

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = [
    "DATA_FILE_NAME",
    "TEMP_FILE_PREFIX",
    "LOCK_DIR_NAME",
    "MAINTENANCE_LOCK_FILE_NAME",
    "encode_slug_component",
    "instrument_slug",
    "dataset_dir",
    "month_dir",
    "month_file_path",
    "relative_month_file_path",
    "new_temp_path",
    "is_temp_file",
    "iter_temp_files",
    "iter_month_slices",
    "iter_canonical_files",
    "is_canonical_month_file_path",
    "lock_file_path",
    "maintenance_lock_file_path",
    "classify_managed_storage",
    "is_within",
]

DATA_FILE_NAME: Final[str] = "data.parquet"
TEMP_FILE_PREFIX: Final[str] = ".data.parquet.tmp-"
LOCK_DIR_NAME: Final[str] = "locks"
MAINTENANCE_LOCK_FILE_NAME: Final[str] = "_maintenance.lock"

_YEAR_DIR_RE: Final[re.Pattern[str]] = re.compile(r"^year=(\d{4})$")
_MONTH_DIR_RE: Final[re.Pattern[str]] = re.compile(r"^month=(\d{2})$")
_CANONICAL_PATH_DEPTH: Final[int] = 7

# Separators, filesystem syntax, Windows-reserved syntax, and characters that
# are not visibly readable in a path must never appear literally in a slug.
_SLUG_ESCAPED_ASCII: Final[frozenset[str]] = frozenset(
    {"-", "%", "/", "\\", "\0", "<", ">", ":", '"', "|", "?", "*"}
)


def encode_slug_component(value: str) -> str:
    """Return one readable, collision-free encoded instrument component.

    NFC makes canonically equivalent Unicode spellings share one projection.
    Readable Unicode and visible filesystem-safe characters remain literal;
    every escaped code point is encoded as uppercase UTF-8 ``%HH`` bytes.
    """
    normalized = unicodedata.normalize("NFC", value)
    encoded: list[str] = []
    for index, char in enumerate(normalized):
        category = unicodedata.category(char)
        unsafe_dot = char == "." and index in {0, len(normalized) - 1}
        if char in _SLUG_ESCAPED_ASCII or unsafe_dot or char.isspace() or category.startswith("C"):
            encoded.extend(f"%{byte:02X}" for byte in char.encode("utf-8"))
        else:
            encoded.append(char)
    return "".join(encoded)


def _symbol_components(symbol: str) -> tuple[str, str]:
    """Split a validated ``BASE/QUOTE`` symbol at its sole boundary."""
    base, quote = symbol.split("/", 1)
    return base, quote


def instrument_slug(key: DatasetKey) -> str:
    """Return the readable instrument projection for ``key``.

    Spot slugs contain base and quote; perpetual slugs additionally contain
    settlement. The internal spot sentinel is never exposed in paths.
    """
    base, quote = _symbol_components(key.symbol)
    components = [encode_slug_component(base), encode_slug_component(quote)]
    if key.market is Market.PERPETUAL:
        components.append(encode_slug_component(key.settle))
    return "-".join(components)


def _key_segments(key: DatasetKey) -> tuple[str, str, str, str]:
    """The readable identity segments of ``key`` in canonical path order."""
    return (key.exchange, key.market.value, instrument_slug(key), key.timeframe)


def dataset_dir(data_dir: Path, key: DatasetKey) -> Path:
    """Directory holding every monthly file for one dataset."""
    return data_dir.joinpath(*_key_segments(key))


def month_dir(data_dir: Path, key: DatasetKey, year_month: YearMonth) -> Path:
    """Directory holding the one canonical file for ``year_month``."""
    return (
        dataset_dir(data_dir, key) / f"year={year_month.year:04d}" / f"month={year_month.month:02d}"
    )


def month_file_path(data_dir: Path, key: DatasetKey, year_month: YearMonth) -> Path:
    """Absolute path to the canonical Parquet file for one dataset/month."""
    return month_dir(data_dir, key, year_month) / DATA_FILE_NAME


def relative_month_file_path(data_dir: Path, key: DatasetKey, year_month: YearMonth) -> str:
    """POSIX path to the canonical file relative to ``data_dir``."""
    absolute = month_file_path(data_dir, key, year_month)
    return PurePosixPath(*absolute.relative_to(data_dir).parts).as_posix()


def new_temp_path(directory: Path) -> Path:
    """A unique same-directory temp path for a monthly commit-in-progress."""
    return directory / f"{TEMP_FILE_PREFIX}{uuid.uuid4().hex}"


def is_temp_file(path: Path) -> bool:
    """Whether ``path`` names a leftover commit temp file."""
    return path.name.startswith(TEMP_FILE_PREFIX)


def iter_temp_files(directory: Path) -> Iterator[Path]:
    """Yield leftover temp files directly inside ``directory``, if any."""
    if not directory.is_dir():
        return
    for entry in directory.iterdir():
        if entry.is_file() and is_temp_file(entry):
            yield entry


def iter_month_slices(
    start: datetime, end: datetime
) -> Iterator[tuple[YearMonth, datetime, datetime]]:
    """Yield ``[start, end)`` split into clipped calendar-month slices."""
    year, month = start.year, start.month
    while True:
        month_start = datetime(year, month, 1, tzinfo=UTC)
        month_end = (
            datetime(year + 1, 1, 1, tzinfo=UTC)
            if month == 12
            else datetime(year, month + 1, 1, tzinfo=UTC)
        )
        yield YearMonth(year=year, month=month), max(start, month_start), min(end, month_end)
        if month_end >= end:
            return
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)


def is_canonical_month_file_path(data_dir: Path, path: Path) -> bool:
    """Whether ``path`` has the current canonical structural path shape.

    This deliberately validates structure only; identity comes from Parquet
    metadata and cannot be inferred from a readable projection.
    """
    try:
        relative = path.resolve().relative_to(data_dir.resolve())
    except ValueError:
        return False
    parts = relative.parts
    return (
        len(parts) == _CANONICAL_PATH_DEPTH
        and parts[-1] == DATA_FILE_NAME
        and bool(_YEAR_DIR_RE.match(parts[-3]))
        and bool(_MONTH_DIR_RE.match(parts[-2]))
    )


def classify_managed_storage(data_dir: Path) -> str:
    """Classify Xret-owned storage structurally without interpreting identity."""
    if not data_dir.is_dir():
        return "empty"
    canonical_files = tuple(iter_canonical_files(data_dir))
    if any(not is_canonical_month_file_path(data_dir, path) for path in canonical_files):
        return "ambiguous"
    if any(data_dir.rglob(f"{TEMP_FILE_PREFIX}*")):
        return "ambiguous"
    return "canonical" if canonical_files else "empty"


def iter_canonical_files(data_dir: Path) -> Iterator[Path]:
    """Yield every committed file named ``data.parquet`` under ``data_dir``."""
    if not data_dir.is_dir():
        return
    yield from data_dir.rglob(DATA_FILE_NAME)


def lock_file_path(state_dir: Path, key: DatasetKey) -> Path:
    """Return the readable collision-free per-dataset inter-process lock path."""
    return state_dir / LOCK_DIR_NAME / f"{'__'.join(_key_segments(key))}.lock"


def maintenance_lock_file_path(state_dir: Path) -> Path:
    """Catalog-wide exclusive maintenance lock file path (rebuild/validate)."""
    return state_dir / LOCK_DIR_NAME / MAINTENANCE_LOCK_FILE_NAME


def is_within(root: Path, path: Path) -> bool:
    """Whether ``path`` resolves to somewhere inside ``root`` (symlink-safe)."""
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True
