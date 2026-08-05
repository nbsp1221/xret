"""`MarketData`: the public top-level facade (Decision 1).

Construction never performs I/O. A historical-bar provider may be bound
directly; omitting it selects the built-in CCXT implementation. When
`config` is omitted, `MarketData` resolves it via
`xret.data.config.resolve_config()` (`XRET_CONFIG` -> `~/.xret/config.toml`
-> built-in defaults); an explicit `config` bypasses that resolution
entirely (S5).

`bars(...)` is the no-I/O binding step that returns a `BarDataset`.
`maintenance` exposes catalog-only upkeep (`validate()`,
`rebuild_catalog()`) that never touches canonical Parquet data.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import cast

from xret.data.config import MarketDataConfig, resolve_config
from xret.data.dataset import BarDataset
from xret.data.errors import CatalogError, UnsupportedMarketError
from xret.data.models import (
    CatalogRebuildResult,
    CatalogValidationResult,
    Market,
    MarketIdentity,
    _coerce_market,
    _validate_exchange,
)
from xret.data.providers import HistoricalBarProvider, MarketDefinition
from xret.data.providers.discovery import ProviderHandle
from xret.data.providers.runtime import MarketDefinitionRuntime
from xret.data.storage.catalog import CATALOG_FILE_NAME
from xret.data.storage.recovery import RecoveryService

__all__ = ["MarketData"]


@dataclass(frozen=True, slots=True)
class _Maintenance:
    """Catalog-only validation and recovery bound to one `MarketData` config.

    Validation is read-only. Rebuild derives SQLite state exclusively from
    current canonical Parquet files and never changes those files.
    """

    config: MarketDataConfig

    @property
    def _db_path(self) -> Path:
        return self.config.state_dir / CATALOG_FILE_NAME

    def _recovery_service(self) -> RecoveryService:
        return RecoveryService(db_path=self._db_path, config=self.config)

    def validate(self) -> CatalogValidationResult:
        """Compare the SQLite catalog against canonical Parquet metadata.

        Read-only: never creates, migrates, locks, or repairs catalog state.
        A clean absent catalog is valid; incompatible or unreadable state is
        reported without opening it for mutation.

        Raises:
            CatalogError: validation could not complete.
        """
        try:
            return self._recovery_service().validate()
        except CatalogError:
            raise
        except Exception as exc:
            raise CatalogError(f"catalog validation failed: {exc}") from exc

    def rebuild_catalog(self) -> CatalogRebuildResult:
        """Rebuild the SQLite catalog from canonical Parquet file metadata.

        Rebuild records only facts proved by current canonical Parquet files.
        It never reconstructs operational history or repairs Parquet data.

        Raises:
            CatalogError: rebuild could not complete.
        """
        try:
            return self._recovery_service().rebuild()
        except CatalogError:
            raise
        except Exception as exc:
            raise CatalogError(f"catalog rebuild failed: {exc}") from exc


class MarketData:
    """Public entry point for Xret market data.

    ```python
    market_data = MarketData()
    bars = market_data.bars(
        exchange="binance", symbol="BTC/USDT", market="perpetual",
        settle="USDT", timeframe="1h",
    )
    ```
    """

    def __init__(
        self,
        config: MarketDataConfig | None = None,
        *,
        provider: HistoricalBarProvider | str | None = None,
    ) -> None:
        self._config = config if config is not None else resolve_config()
        self._provider = ProviderHandle(provider)

    @property
    def config(self) -> MarketDataConfig:
        """The resolved (or explicitly supplied) configuration in effect."""
        return self._config

    @property
    def maintenance(self) -> _Maintenance:
        """Catalog validation/rebuild namespace bound to this instance's config."""
        return _Maintenance(self._config)

    def fetch_markets(
        self,
        *,
        exchange: str,
        market: str,
    ) -> tuple[MarketDefinition, ...]:
        """Fetch one provider's current market definitions for a scope.

        This is an eager remote metadata operation. It never reads or changes
        canonical Parquet data, catalog state, coverage, or source lineage.
        Search, sorting, filtering, and result caching belong to the caller.

        `timeframes` on each returned definition are provider-advertised values
        that Xret can express, not verified-support claims.
        """
        canonical_exchange = _validate_exchange(exchange)
        canonical_market = _coerce_market(market)
        if canonical_market not in (Market.SPOT, Market.PERPETUAL):
            raise UnsupportedMarketError(
                f"market {canonical_market.value!r} is not supported in V1: structurally "
                "ambiguous without contract-attribute fields; only 'spot' and "
                "'perpetual' are operable"
            )
        provider = self._provider.get()
        return MarketDefinitionRuntime(provider).fetch_markets(
            exchange=canonical_exchange,
            market=canonical_market,
        )

    def bars(
        self,
        *,
        exchange: str,
        symbol: str,
        market: str,
        settle: str | None = None,
        timeframe: str,
    ) -> BarDataset:
        """Bind one canonical market identity and timeframe. No I/O (Decision 9).

        Identity resolution against provider metadata (including safe
        settlement inference) happens lazily, inside `fetch`/`sync`, never
        here. The returned `BarDataset` privately carries this instance's
        resolved `config` (Decision 22): every verb call uses it, with no
        public `config` parameter added to `BarDataset` or any verb.
        """
        identity = MarketIdentity(
            exchange=exchange,
            symbol=symbol,
            market=cast("Market", market),
            settle=settle,
        )
        dataset = BarDataset(identity=identity, timeframe=timeframe)
        object.__setattr__(dataset, "_config", self._config)
        object.__setattr__(dataset, "_provider", self._provider)
        return dataset
