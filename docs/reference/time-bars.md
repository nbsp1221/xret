# Canonical time bars

Xret exposes one provider-independent trade-OHLCV contract for 24/7 crypto spot and perpetual markets. A provider value is canonical only when its interval, event source, finality, and units satisfy this page; a familiar timeframe name or six-element OHLCV shape is not sufficient.

## Interval identity

- `timestamp` is the inclusive actual start of the represented interval, normalized to a UTC millisecond instant.
- Every bar covers a half-open interval `[timestamp, next_boundary)`.
- Historical results contain only completed intervals. Live observations expose Xret's receipt-time finality separately.
- Fixed `s`, `m`, `h`, and `d` multiples use `1970-01-01T00:00:00Z` as their origin.
- `1w` is a calendar week beginning Monday at `00:00:00Z`.
- `1M` is a calendar month beginning on its first day at `00:00:00Z`.

The epoch origin makes arbitrary fixed multiples deterministic. It is an Xret convention, not an assumption about a venue's native aggregation. For example, `7d` follows the fixed epoch grid whose boundaries fall on Thursdays, while `1w` follows the Monday UTC calendar grid. They have the same duration but are different timeframes. A provider-native name such as `3d` or `1w` is directly compatible only when its duration, origin, timezone, and interval label all match Xret.

## Price event universe

Current Xret time bars are trade bars:

- `open` is the first eligible executed-trade price in the interval;
- `high` is the maximum eligible executed-trade price;
- `low` is the minimum eligible executed-trade price; and
- `close` is the last eligible executed-trade price.

The eligible event universe is the venue's official executed-trade series for the resolved market as exposed or exactly normalized by the provider. Provider-specific trade-condition filtering must remain consistent for the endpoint family and be part of its qualification evidence.

Mark price, index price, settlement price, midpoint, and quote-derived candles do not satisfy this contract. Xret may introduce separately named bar types for those event universes in the future; they must never be silently stored as trade OHLCV.

## Volume

`volume` is the sum of eligible base-asset quantity in the interval. For `BTC/USDT`, it is BTC quantity for both spot and perpetual trade bars regardless of quote or settlement currency.

A provider may satisfy this meaning in either of two ways:

1. return an official base-quantity field directly; or
2. convert a native quantity losslessly using verified, stable instrument metadata such as an exact contract multiplier.

Quote notional and contract count are not canonical base volume. A provider must reject an endpoint when it cannot convert those values exactly. In particular, Xret does not estimate base volume from quote volume using a candle's open, close, midpoint, or typical price because no single candle price can reconstruct the sum of trade-level base quantities.

“Exact” describes the semantic unit and transformation, not arbitrary-precision arithmetic. The current research schema represents OHLCV values as `Float64`.

## Empty intervals and coverage

Xret does not synthesize a candle when the official trade series contains no eligible trade in an interval. A successful exhaustive observation that omits a completed boundary records that interval as `unavailable`; a range that was not exhaustively observed remains `missing`. Provider, transport, validation, or storage failures never create negative coverage.

Strict scans require canonical rows for the complete requested grid and therefore reject unavailable intervals. Partial scans return available rows and expose gaps. Xret does not carry forward a close, manufacture zero volume, or add venue-specific exceptions for known outages, listing boundaries, retention limits, or no-trade intervals.

## Provider compatibility

One provider endpoint and timeframe is compatible only when all of the following are proven:

- canonical market identity and settlement;
- interval duration, origin, timezone, label, and finality;
- executed-trade price event universe;
- exact base-asset volume;
- exhaustive bounded observation and missing-interval behavior; and
- canonical ordering, uniqueness, finiteness, and OHLC relationships.

Compatibility can be direct, selected through an official parameter or native identifier, or produced by lossless reaggregation from a completely observed finer canonical source. Timestamp relabeling, approximate unit conversion, synthetic filling, and silently substituting another event universe are never compatible transformations.
