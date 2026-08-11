"""Public import-surface tests for `xret.data` (S5).

`xret.data` exports exactly eight public names: the `MarketData` facade,
its `MarketDataConfig`, `BarDataset`, `LiveMarketData`, live `BarUpdate` and
`BarFinality`, and the two result types (`SyncResult`, `PartialScanResult`).
Every exception lives in `xret.data.errors` only -- never re-exported at the
package top level.
"""

from __future__ import annotations

from importlib.util import find_spec

import pytest


def test_package_is_importable() -> None:
    import xret.data

    assert xret.data.__doc__ == "Trusted market data infrastructure for Xret."


def test_public_surface_is_exactly_eight_names() -> None:
    import xret.data

    assert set(xret.data.__all__) == {
        "MarketData",
        "MarketDataConfig",
        "BarDataset",
        "BarUpdate",
        "BarFinality",
        "LiveMarketData",
        "SyncResult",
        "PartialScanResult",
    }


@pytest.mark.parametrize(
    "name",
    [
        "MarketData",
        "MarketDataConfig",
        "BarDataset",
        "BarUpdate",
        "BarFinality",
        "LiveMarketData",
        "SyncResult",
        "PartialScanResult",
    ],
)
def test_every_declared_public_name_is_importable(name: str) -> None:
    import xret.data

    assert hasattr(xret.data, name)


@pytest.mark.parametrize(
    "name",
    [
        # Legacy facade/config names removed rather than aliased.
        "DataCatalog",
        "XretDataConfig",
        "resolve_config",
        # Exceptions must not be re-exported at the top level.
        "XretDataError",
        "ConfigurationError",
        "InvalidRequestError",
        "UnsupportedMarketError",
        "ProviderError",
        "CoverageError",
        "SyncError",
        "CatalogError",
        # Retired legacy result/domain names.
        "CatalogValidationResult",
        "CatalogRebuildResult",
        "OHLCVRequest",
        "DatasetKey",
        "YearMonth",
        "CoverageInterval",
        "CoverageStatus",
        "SupportTier",
        "QualitySeverity",
        "OHLCV_SCHEMA",
        "OHLCV_COLUMNS",
        "IDENTITY_COLUMNS",
        "OHLC_COLUMNS",
    ],
)
def test_legacy_and_non_public_names_are_not_exported(name: str) -> None:
    import xret.data

    assert not hasattr(xret.data, name)
    assert name not in xret.data.__all__


def test_exceptions_are_importable_from_errors_submodule() -> None:
    from xret.data.errors import (
        CatalogError,
        ConfigurationError,
        CoverageError,
        InvalidRequestError,
        ProviderError,
        SyncError,
        UnsupportedMarketError,
        XretDataError,
    )

    for exc_type in (
        ConfigurationError,
        InvalidRequestError,
        UnsupportedMarketError,
        ProviderError,
        CoverageError,
        SyncError,
        CatalogError,
    ):
        assert issubclass(exc_type, XretDataError)


def test_provider_author_surface_is_explicit_and_importable() -> None:
    import xret.data.providers

    assert set(xret.data.providers.__all__) == {
        "PROVIDER_API_VERSION",
        "PROVIDER_BAR_SCHEMA",
        "BarObservation",
        "BarRequest",
        "DerivativeInterpretation",
        "HistoricalBarProvider",
        "LiveBarProvider",
        "LiveBarSession",
        "Market",
        "MarketDefinition",
        "MarketDefinitionProvider",
        "MarketIdentity",
        "ObservedWindow",
        "ProviderDescriptor",
        "ProviderBarUpdate",
        "ResolvedBarMarket",
    }
    for name in xret.data.providers.__all__:
        assert hasattr(xret.data.providers, name)


@pytest.mark.parametrize(
    "module_name",
    [
        "xret.data.provider",
        "xret.data.provider_pagination",
        "xret.data.providers.registry",
        "xret.data.providers._ccxt",
        "xret.data.providers._ccxt_pagination",
    ],
)
def test_retired_provider_module_paths_have_no_compatibility_shim(
    module_name: str,
) -> None:
    assert find_spec(module_name) is None
