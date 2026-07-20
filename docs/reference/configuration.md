# Configuration

## Explicit configuration

```python
from pathlib import Path

from xret.data import MarketData, MarketDataConfig

config = MarketDataConfig(
    state_dir=Path("/srv/xret"),
    data_dir=Path("/srv/xret/data"),
)
market_data = MarketData(config=config)
```

An explicit `MarketDataConfig` bypasses configuration discovery for that instance.

- `state_dir` stores the SQLite coverage/provenance catalog and lock files.
- `data_dir` stores canonical Parquet datasets.

## Discovery

When `MarketData()` receives no config, Xret resolves the first available source:

1. `XRET_CONFIG`, which must point to an existing TOML file.
2. `~/.xret/config.toml`, when present.
3. Built-in defaults: `~/.xret` for state and `~/.xret/data` for data.

A TOML file may contain only:

```toml
state_dir = "/srv/xret"
data_dir = "/mnt/market-data"
```

Both values must be strings. Relative values resolve against the configuration file's directory, not the process working directory. Unknown keys and invalid types raise `ConfigurationError`.

Xret does not use directory-specific environment-variable overrides. Importing `xret.data` performs no configuration or filesystem I/O.
