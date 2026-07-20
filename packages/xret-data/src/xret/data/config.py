"""Xret configuration resolution: `MarketDataConfig`, `XRET_CONFIG`, `config.toml`.

`MarketData(config=...)` accepts an explicit, already-built
`MarketDataConfig` and, when given one, bypasses resolution entirely: no
environment variable or `config.toml` is ever consulted for that
instance. Otherwise `resolve_config()` picks the highest-precedence
source that is present:

1. `XRET_CONFIG` -- must name an existing, parseable TOML file;
2. `~/.xret/config.toml` -- used only if it exists;
3. built-in defaults (`~/.xret` for `state_dir`, `<state_dir>/data` for
   `data_dir`).

Config files are strict: an unknown top-level key, or a `state_dir`/
`data_dir` value that is not a string, is a hard `ConfigurationError`.
There is no lenient or legacy parsing mode. A relative `state_dir`/
`data_dir` value resolves against the config file's own parent
directory, never the process working directory.

Nothing in this module performs I/O at import time. All environment and
filesystem access happens inside `resolve_config()`, which callers invoke
explicitly (e.g. from the `MarketData` facade constructor).
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from xret.data.errors import ConfigurationError

__all__ = [
    "MarketDataConfig",
    "resolve_config",
    "ENV_XRET_CONFIG",
    "DEFAULT_CONFIG_FILE",
]

#: Environment variable naming an explicit `config.toml` path.
ENV_XRET_CONFIG: Final[str] = "XRET_CONFIG"

#: Fallback config file location consulted when `XRET_CONFIG` is unset.
DEFAULT_CONFIG_FILE: Final[Path] = Path.home() / ".xret" / "config.toml"

_DEFAULT_STATE_DIR_NAME: Final[str] = ".xret"
_DEFAULT_DATA_DIR_NAME: Final[str] = "data"

#: The only two top-level keys a `config.toml` may declare.
_ALLOWED_KEYS: Final[frozenset[str]] = frozenset({"state_dir", "data_dir"})


@dataclass(frozen=True, slots=True)
class MarketDataConfig:
    """Resolved, immutable Xret market data configuration.

    `state_dir` holds the SQLite coverage/provenance catalog and lock
    files. `data_dir` holds the canonical Parquet datasets. Both are
    plain `Path` values; constructing one performs no I/O.
    """

    state_dir: Path
    data_dir: Path


def _default_state_dir() -> Path:
    return Path.home() / _DEFAULT_STATE_DIR_NAME


def _resolve_relative_to(value: str, *, base_dir: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve(strict=False)


def _find_config_path() -> Path | None:
    env_value = os.environ.get(ENV_XRET_CONFIG)
    if env_value:
        explicit_path = Path(env_value).expanduser()
        if not explicit_path.is_file():
            raise ConfigurationError(
                f"{ENV_XRET_CONFIG} points at a nonexistent file: {explicit_path}"
            )
        return explicit_path
    if DEFAULT_CONFIG_FILE.is_file():
        return DEFAULT_CONFIG_FILE
    return None


def _load_config_values(config_path: Path) -> dict[str, str]:
    try:
        with config_path.open("rb") as handle:
            document = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigurationError(f"failed to read {config_path}: {exc}") from exc

    unknown_keys = sorted(set(document) - _ALLOWED_KEYS)
    if unknown_keys:
        raise ConfigurationError(
            f"{config_path}: unknown key(s) {unknown_keys!r}; "
            f"only {sorted(_ALLOWED_KEYS)!r} are recognized"
        )

    values: dict[str, str] = {}
    for key in _ALLOWED_KEYS:
        if key not in document:
            continue
        value = document[key]
        if not isinstance(value, str):
            raise ConfigurationError(f"{config_path}: {key!r} must be a string, got {value!r}")
        values[key] = value
    return values


def resolve_config() -> MarketDataConfig:
    """Resolve the effective Xret market data configuration.

    Precedence (highest wins): `XRET_CONFIG` (must name an existing,
    parseable TOML file) > `~/.xret/config.toml` (used only if present)
    > built-in defaults (`~/.xret` for `state_dir`, `<state_dir>/data`
    for `data_dir`).

    Callers that already hold a `MarketDataConfig` should pass it
    directly to `MarketData(config=...)` instead of calling this
    function: an explicit config bypasses resolution entirely.

    Raises:
        ConfigurationError: `XRET_CONFIG` names a missing file, or the
            resolved config file cannot be parsed, declares an unknown
            key, or gives `state_dir`/`data_dir` a non-string value.
    """
    config_path = _find_config_path()
    if config_path is None:
        state_dir = _default_state_dir()
        return MarketDataConfig(state_dir=state_dir, data_dir=state_dir / _DEFAULT_DATA_DIR_NAME)

    values = _load_config_values(config_path)
    base_dir = config_path.parent

    state_dir = (
        _resolve_relative_to(values["state_dir"], base_dir=base_dir)
        if "state_dir" in values
        else _default_state_dir()
    )
    data_dir = (
        _resolve_relative_to(values["data_dir"], base_dir=base_dir)
        if "data_dir" in values
        else state_dir / _DEFAULT_DATA_DIR_NAME
    )
    return MarketDataConfig(state_dir=state_dir, data_dir=data_dir)
