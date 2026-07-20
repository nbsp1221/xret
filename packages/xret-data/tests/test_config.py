"""Tests for `xret.data.config`: `MarketDataConfig` resolution (S5)."""

from __future__ import annotations

from pathlib import Path

import pytest
from xret.data.config import ENV_XRET_CONFIG, MarketDataConfig, resolve_config
from xret.data.errors import ConfigurationError


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure `XRET_CONFIG` never leaks in from the host environment."""
    monkeypatch.delenv(ENV_XRET_CONFIG, raising=False)


@pytest.fixture(autouse=True)
def _isolate_default_config_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Point the `~/.xret/config.toml` fallback at a path that never exists."""
    monkeypatch.setattr("xret.data.config.DEFAULT_CONFIG_FILE", tmp_path / "unused" / "config.toml")


def test_market_data_config_is_frozen() -> None:
    config = MarketDataConfig(state_dir=Path("/a"), data_dir=Path("/b"))
    with pytest.raises(AttributeError):
        config.state_dir = Path("/c")  # type: ignore[misc]


def test_default_resolution_with_no_config_present() -> None:
    config = resolve_config()
    assert config.state_dir == Path.home() / ".xret"
    assert config.data_dir == Path.home() / ".xret" / "data"


def test_env_xret_config_missing_file_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(ENV_XRET_CONFIG, str(tmp_path / "does-not-exist.toml"))
    with pytest.raises(ConfigurationError, match="nonexistent file"):
        resolve_config()


def test_env_xret_config_explicit_values(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text('state_dir = "state"\ndata_dir = "data"\n')
    monkeypatch.setenv(ENV_XRET_CONFIG, str(config_path))

    config = resolve_config()

    assert config.state_dir == tmp_path / "state"
    assert config.data_dir == tmp_path / "data"


def test_relative_paths_resolve_against_config_file_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    nested = tmp_path / "nested"
    nested.mkdir()
    config_path = nested / "config.toml"
    config_path.write_text('state_dir = "../state"\n')
    monkeypatch.setenv(ENV_XRET_CONFIG, str(config_path))

    config = resolve_config()

    assert config.state_dir == (nested / "../state").resolve()
    # data_dir defaults relative to the resolved state_dir, not the config file.
    assert config.data_dir == config.state_dir / "data"


def test_absolute_paths_are_used_as_is(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(f'state_dir = "{tmp_path / "abs-state"}"\n')
    monkeypatch.setenv(ENV_XRET_CONFIG, str(config_path))

    config = resolve_config()

    assert config.state_dir == tmp_path / "abs-state"
    assert config.data_dir == config.state_dir / "data"


def test_data_dir_only_overrides_data_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text('data_dir = "custom-data"\n')
    monkeypatch.setenv(ENV_XRET_CONFIG, str(config_path))

    config = resolve_config()

    assert config.state_dir == Path.home() / ".xret"
    assert config.data_dir == tmp_path / "custom-data"


def test_unknown_key_raises_configuration_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text('bogus_key = "x"\n')
    monkeypatch.setenv(ENV_XRET_CONFIG, str(config_path))

    with pytest.raises(ConfigurationError, match="unknown key"):
        resolve_config()


def test_non_string_value_raises_configuration_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text("state_dir = 123\n")
    monkeypatch.setenv(ENV_XRET_CONFIG, str(config_path))

    with pytest.raises(ConfigurationError, match="must be a string"):
        resolve_config()


def test_malformed_toml_raises_configuration_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text("this is not valid toml [[[")
    monkeypatch.setenv(ENV_XRET_CONFIG, str(config_path))

    with pytest.raises(ConfigurationError, match="failed to read"):
        resolve_config()


def test_default_config_file_used_when_xret_config_unset(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    default_path = tmp_path / ".xret" / "config.toml"
    default_path.parent.mkdir(parents=True)
    default_path.write_text('state_dir = "from-default-file"\n')
    monkeypatch.setattr("xret.data.config.DEFAULT_CONFIG_FILE", default_path)

    config = resolve_config()

    assert config.state_dir == tmp_path / ".xret" / "from-default-file"


def test_default_config_file_ignored_when_absent() -> None:
    # The autouse `_isolate_default_config_file` fixture points
    # DEFAULT_CONFIG_FILE at a path that never exists.
    config = resolve_config()
    assert config.state_dir == Path.home() / ".xret"
