"""Qualified CCXT venues stay resolvable with the installed `ccxt`.

Every other test under `providers/ccxt/` uses fakes, so none of them observe
what real CCXT classes advertise. That blind spot let a refactor make 20 of
90 CCXT venues unresolvable -- including one listed in
`docs/quality/verified-support.md` -- because Xret validated each venue's
entire advertised catalog instead of the timeframe the caller asked for.

These tests read `ccxt.<id>().timeframes`, a static class attribute that
needs no network, and assert the boundary contract for every venue Xret
claims a qualified pagination profile for. Deriving the venue list from
`pagination._PROFILES` rather than restating it keeps a newly qualified
venue covered automatically; a hardcoded copy would silently miss it.
"""

from __future__ import annotations

from collections.abc import Mapping

import ccxt
import pytest
from xret.data.errors import InvalidRequestError
from xret.data.models import Market, MarketIdentity
from xret.data.providers.ccxt.markets import supported_timeframes
from xret.data.providers.ccxt.pagination import _PROFILES
from xret.data.providers.contracts import ResolvedBarMarket
from xret.data.timeframe import TimeBar

_QUALIFIED_CLIENT_IDS = sorted(_PROFILES)

#: Every qualified venue offers hourly bars, and `verified-support.md` claims
#: `1h` for each venue it lists. Filtering must never drop it.
_BASELINE_TIMEFRAME = "1h"


def _inexpressible(timeframes: Mapping[str, object]) -> list[str]:
    """Advertised entries Xret's canonical grammar cannot represent."""
    return sorted(str(key) for key in timeframes if not _is_canonical(str(key)))


def _is_canonical(timeframe: str) -> bool:
    try:
        TimeBar.parse(timeframe)
    except InvalidRequestError:
        return False
    return True


@pytest.mark.parametrize("client_id", _QUALIFIED_CLIENT_IDS)
def test_advertised_metadata_satisfies_the_resolved_market_contract(client_id: str) -> None:
    """The `supported_timeframes` -> `ResolvedBarMarket` boundary must hold.

    This is the exact production step at `provider.py:104`. `ResolvedBarMarket`
    validates its own `timeframes`, so this fails closed if the adapter ever
    passes a venue's raw catalog through again. The identity is a placeholder:
    `ResolvedBarMarket` does not cross-check it against `client_id`, and this
    test is about the timeframe boundary only.
    """
    exchange = getattr(ccxt, client_id)()

    resolved = ResolvedBarMarket(
        identity=MarketIdentity(exchange="binance", symbol="BTC/USDT", market=Market.SPOT),
        native_market_id="BTCUSDT",
        native_symbol="BTC/USDT",
        timeframes=supported_timeframes(exchange),
    )

    assert resolved.timeframes


@pytest.mark.parametrize("client_id", _QUALIFIED_CLIENT_IDS)
def test_qualified_venue_still_offers_the_baseline_timeframe(client_id: str) -> None:
    exchange = getattr(ccxt, client_id)()

    assert _BASELINE_TIMEFRAME in supported_timeframes(exchange)


@pytest.mark.parametrize("client_id", _QUALIFIED_CLIENT_IDS)
def test_supported_timeframes_only_removes_advertised_entries(client_id: str) -> None:
    """Filtering never invents, rewrites, or renames an entry."""
    exchange = getattr(ccxt, client_id)()
    advertised = {str(key) for key in exchange.timeframes}

    accepted = supported_timeframes(exchange)

    assert accepted <= advertised
    assert accepted == frozenset(key for key in advertised if _is_canonical(key))


def test_installed_ccxt_still_advertises_bar_types_xret_cannot_express() -> None:
    """Guards the premise the parametrized cases rely on.

    If CCXT ever aligned every qualified venue with Xret's grammar, the cases
    above would stop exercising filtering. This says so rather than leaving a
    silently vacuous suite.
    """
    found = {
        client_id: _inexpressible(getattr(ccxt, client_id)().timeframes)
        for client_id in _QUALIFIED_CLIENT_IDS
    }

    assert any(found.values()), (
        "no qualified venue advertises a bar type outside Xret's vocabulary; "
        f"the filtered cases above no longer exercise filtering: {found}"
    )
