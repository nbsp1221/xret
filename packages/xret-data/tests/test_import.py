"""Public import-surface tests for `xret.data` (S5).

`xret.data` exports exactly five public names: the `MarketData` facade,
its `MarketDataConfig`, `BarDataset`, and the two result types
(`SyncResult`, `PartialScanResult`). Every exception lives in
`xret.data.errors` only -- never re-exported at the package top level.
"""

from __future__ import annotations

import pytest


def test_package_is_importable() -> None:
    import xret.data

    assert xret.data.__doc__ == "Trusted market data infrastructure for Xret."


def test_public_surface_is_exactly_five_names() -> None:
    import xret.data

    assert set(xret.data.__all__) == {
        "MarketData",
        "MarketDataConfig",
        "BarDataset",
        "SyncResult",
        "PartialScanResult",
    }


@pytest.mark.parametrize(
    "name",
    ["MarketData", "MarketDataConfig", "BarDataset", "SyncResult", "PartialScanResult"],
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
