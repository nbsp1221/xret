"""Official Binance Data Vision USDⓈ-M historical open-interest provider."""

from __future__ import annotations

import csv
import hashlib
import io
import math
import re
import urllib.error
import urllib.request
import zipfile
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Final

import polars as pl
from xret.data.errors import ProviderError, UnsupportedMarketError
from xret.data.models import Market, MarketIdentity
from xret.data.providers.contracts import (
    PROVIDER_API_VERSION,
    PROVIDER_OPEN_INTEREST_SCHEMA,
    DerivativeInterpretation,
    ObservedWindow,
    OpenInterestObservation,
    OpenInterestRequest,
    OpenInterestSourceEvidence,
    ProviderDescriptor,
    ResolvedOpenInterestMarket,
)

_BASE_URL: Final = "https://data.binance.vision/data/futures/um/daily/metrics"
_MAX_OBJECT_BYTES: Final = 32 * 1024 * 1024
_MAX_CHECKSUM_BYTES: Final = 512
_MAX_UNCOMPRESSED_BYTES: Final = 64 * 1024 * 1024
_MAX_DAYS: Final = 366
_TIMEOUT_SECONDS: Final = 30.0
_CHECKSUM_RE: Final = re.compile(rb"^([0-9a-f]{64})[ \t]+\*?([^/\r\n]+)\r?\n?$")
_HEADER: Final = (
    "create_time",
    "symbol",
    "sum_open_interest",
    "sum_open_interest_value",
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio",
)


class _NotPublished(Exception):
    pass


Transport = Callable[[str, int], bytes]


def _download(url: str, limit: int) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "xret-data/0.5"})
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS) as response:
            length = response.headers.get("Content-Length")
            if length is not None:
                try:
                    declared = int(length)
                except ValueError as exc:
                    raise ProviderError("Data Vision response has invalid Content-Length") from exc
                if declared < 0 or declared > limit:
                    raise ProviderError("Data Vision response exceeds the bounded size limit")
            payload = response.read(limit + 1)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise _NotPublished from exc
        raise ProviderError(f"Data Vision HTTP request failed with status {exc.code}") from exc
    except (OSError, urllib.error.URLError) as exc:
        raise ProviderError("Data Vision transport failed") from exc
    if len(payload) > limit:
        raise ProviderError("Data Vision response exceeds the bounded size limit")
    return payload


def _positive_float(value: str, *, field: str) -> float:
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ProviderError(f"Data Vision {field} is not a decimal") from exc
    if not number.is_finite() or number < 0:
        raise ProviderError(f"Data Vision {field} must be finite and nonnegative")
    converted = float(number)
    if not math.isfinite(converted):
        raise ProviderError(f"Data Vision {field} is not representable as Float64")
    return 0.0 if converted == 0.0 else converted


def _timestamp(value: str) -> datetime:
    try:
        if value.isascii() and value.isdigit():
            timestamp = datetime.fromtimestamp(int(value) / 1000, UTC)
        else:
            timestamp = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    except (OverflowError, ValueError) as exc:
        raise ProviderError("Data Vision create_time is not an exact UTC timestamp") from exc
    if timestamp.microsecond:
        raise ProviderError("Data Vision create_time must have whole-second precision")
    return timestamp


def _object_names(symbol: str, day: date) -> tuple[str, str, str]:
    filename = f"{symbol}-metrics-{day.isoformat()}.zip"
    key = f"{symbol}/{filename}"
    return key, filename, f"{filename[:-4]}.csv"


class BinanceDataVisionProvider:
    """Historical OI only; never delegates to CCXT or Binance REST."""

    descriptor = ProviderDescriptor("binance-data-vision", "1", PROVIDER_API_VERSION)

    def __init__(self, *, transport: Transport | None = None) -> None:
        self._transport = transport or _download

    def resolve_open_interest_market(self, identity: MarketIdentity) -> ResolvedOpenInterestMarket:
        if (
            identity.exchange != "binance"
            or identity.market is not Market.PERPETUAL
            or identity.settle not in (None, "USDT")
            or identity.symbol != "BTC/USDT"
        ):
            raise UnsupportedMarketError(
                "binance-data-vision supports only qualified Binance BTC/USDT "
                "USDⓈ-M perpetual history"
            )
        base, quote = identity.symbol.split("/")
        if quote != "USDT" or not base.isascii() or not base.isalnum() or not base.isupper():
            raise UnsupportedMarketError(
                "binance-data-vision requires an uppercase BASE/USDT symbol"
            )
        resolved = MarketIdentity(
            exchange="binance",
            symbol=identity.symbol,
            market=Market.PERPETUAL,
            settle="USDT",
        )
        native = f"{base}USDT"
        return ResolvedOpenInterestMarket(
            resolved,
            native,
            native,
            frozenset({"5m"}),
            DerivativeInterpretation(True, False, "1"),
        )

    def observe_open_interest(
        self,
        request: OpenInterestRequest,
        market: ResolvedOpenInterestMarket,
    ) -> OpenInterestObservation:
        if request.timeframe != "5m":
            raise UnsupportedMarketError("binance-data-vision supports only 5m open interest")
        if request.start.time() != time() or request.end.time() != time():
            raise ProviderError("Data Vision requests must use whole UTC daily boundaries")
        days = (request.end.date() - request.start.date()).days
        if days <= 0 or days > _MAX_DAYS:
            raise ProviderError(f"Data Vision request must span 1..{_MAX_DAYS} whole days")
        all_rows: list[tuple[datetime, float, float]] = []
        observed: list[ObservedWindow] = []
        evidence: list[OpenInterestSourceEvidence] = []
        for offset in range(days):
            day = request.start.date() + timedelta(days=offset)
            day_start = datetime.combine(day, time(), UTC)
            day_end = day_start + timedelta(days=1)
            result = self._read_day(market.native_market_id, day, day_start, day_end)
            if result is None:
                continue
            rows, item = result
            all_rows.extend(rows)
            evidence.append(item)
            if len(rows) == 288:
                observed.append(ObservedWindow(day_start, day_end))
            else:
                present = {row[0] for row in rows}
                for index in range(288):
                    slot = day_start + timedelta(minutes=5 * index)
                    if slot in present:
                        observed.append(ObservedWindow(slot, slot + timedelta(minutes=5)))
        frame = pl.DataFrame(
            {
                "timestamp": [row[0] for row in all_rows],
                "open_interest_amount": [row[1] for row in all_rows],
                "open_interest_value": [row[2] for row in all_rows],
            },
            schema=PROVIDER_OPEN_INTEREST_SCHEMA,
        )
        return OpenInterestObservation(frame, tuple(observed), tuple(evidence))

    def _read_day(
        self, symbol: str, day: date, start: datetime, end: datetime
    ) -> tuple[list[tuple[datetime, float, float]], OpenInterestSourceEvidence] | None:
        key, filename, member_name = _object_names(symbol, day)
        url = f"{_BASE_URL}/{key}"
        try:
            checksum_payload = self._transport(f"{url}.CHECKSUM", _MAX_CHECKSUM_BYTES)
            archive = self._transport(url, _MAX_OBJECT_BYTES)
            if (
                not isinstance(checksum_payload, bytes)
                or len(checksum_payload) > _MAX_CHECKSUM_BYTES
            ):
                raise ProviderError("Data Vision checksum response exceeds the bounded size limit")
            if not isinstance(archive, bytes) or len(archive) > _MAX_OBJECT_BYTES:
                raise ProviderError("Data Vision archive response exceeds the bounded size limit")
        except _NotPublished:
            return None
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            raise ProviderError(f"Data Vision HTTP request failed with status {exc.code}") from exc
        match = _CHECKSUM_RE.fullmatch(checksum_payload)
        if match is None or match.group(2).decode("ascii", "strict") != filename:
            raise ProviderError("Data Vision checksum file has invalid object identity or syntax")
        checksum = match.group(1).decode("ascii")
        if hashlib.sha256(archive).hexdigest() != checksum:
            raise ProviderError("Data Vision archive checksum mismatch")
        try:
            with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
                members = bundle.infolist()
                if len(members) != 1 or members[0].filename != member_name or members[0].is_dir():
                    raise ProviderError(
                        "Data Vision ZIP must contain exactly its expected CSV member"
                    )
                member = members[0]
                if member.file_size > _MAX_UNCOMPRESSED_BYTES:
                    raise ProviderError("Data Vision CSV exceeds the uncompressed size limit")
                if member.compress_size == 0 and member.file_size:
                    raise ProviderError("Data Vision ZIP has an invalid compression size")
                if member.compress_size and member.file_size > member.compress_size * 200:
                    raise ProviderError("Data Vision ZIP exceeds the compression-ratio limit")
                payload = bundle.read(member)
        except (OSError, zipfile.BadZipFile, RuntimeError) as exc:
            raise ProviderError("Data Vision archive is not a valid bounded ZIP") from exc
        try:
            text = payload.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise ProviderError("Data Vision CSV must be UTF-8") from exc
        reader = csv.reader(io.StringIO(text, newline=""), strict=True)
        try:
            header = tuple(next(reader))
        except (StopIteration, csv.Error) as exc:
            raise ProviderError("Data Vision CSV has no valid header") from exc
        if header != _HEADER:
            raise ProviderError(f"Data Vision CSV header mismatch: {header!r}")
        parsed: list[tuple[datetime, float, float]] = []
        try:
            for number, row in enumerate(reader, start=2):
                if len(row) != len(_HEADER):
                    raise ProviderError(f"Data Vision CSV row {number} has the wrong field count")
                if row[1] != symbol:
                    raise ProviderError(f"Data Vision CSV row {number} has the wrong symbol")
                timestamp = _timestamp(row[0])
                if not start <= timestamp < end or timestamp.minute % 5 or timestamp.second:
                    raise ProviderError(
                        f"Data Vision CSV row {number} is outside its exact day grid"
                    )
                parsed.append(
                    (
                        timestamp,
                        _positive_float(row[2], field="sum_open_interest"),
                        _positive_float(row[3], field="sum_open_interest_value"),
                    )
                )
        except csv.Error as exc:
            raise ProviderError("Data Vision CSV syntax is invalid") from exc
        by_timestamp: dict[datetime, tuple[datetime, float, float]] = {}
        duplicates = 0
        for row in parsed:
            previous = by_timestamp.get(row[0])
            if previous is not None:
                if previous != row:
                    raise ProviderError(
                        f"Data Vision CSV has conflicting duplicates at {row[0].isoformat()}"
                    )
                duplicates += 1
            else:
                by_timestamp[row[0]] = row
        rows = [by_timestamp[key] for key in sorted(by_timestamp)]
        normalizations = [
            "open_interest.decimal_to_float64",
            "open_interest.signed_zero_to_positive",
        ]
        if parsed != sorted(parsed, key=lambda row: row[0]):
            normalizations.append("open_interest.stable_timestamp_sort")
        if duplicates:
            normalizations.append("open_interest.identical_duplicate_dedup")
        retrieved_at = datetime.now(UTC)
        evidence = OpenInterestSourceEvidence(
            start=start,
            end=end,
            source_route="official-data-vision-usdm-daily-metrics",
            object_key=f"data/futures/um/daily/metrics/{key}",
            checksum_algorithm="sha256",
            checksum_value=checksum,
            source_revision=f"sha256:{checksum}",
            retrieved_at=retrieved_at,
            source_rows=len(parsed),
            canonical_rows=len(rows),
            duplicate_rows=duplicates,
            normalizations=tuple(normalizations),
        )
        return rows, evidence

    def _open_interest_source_field_mapping(self, market: ResolvedOpenInterestMarket) -> str:
        del market
        return (
            "sum_open_interest->open_interest_amount;sum_open_interest_value->open_interest_value"
        )
