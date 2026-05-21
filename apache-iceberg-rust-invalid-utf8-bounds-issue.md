# Manifest read fails on invalid UTF-8 string bounds even when bounds are not needed

## Summary

`iceberg-rust` eagerly decodes `lower_bounds` and `upper_bounds` into typed `Datum` values while reading manifest entries. If a manifest contains an invalid UTF-8 byte sequence for a string bound, manifest loading fails even for callers that only need unrelated fields like `file_size_in_bytes`.

This differs from Apache Iceberg Java, which keeps bounds as raw `ByteBuffer` values on `DataFile` and only decodes them lazily when a caller explicitly uses typed bounds.

## Observed Error

A size/statistics-only command that reads live data files failed with:

```text
Error: failed to load current data file size statistics

Caused by:
    0: Unexpected => handling invalid utf-8 characters, source: incomplete utf-8 byte sequence from index 15
    1: incomplete utf-8 byte sequence from index 15
```

The caller did not need `lower_bounds` or `upper_bounds`; it only needed data file sizes.

## Root Cause

Manifest deserialization currently parses bound bytes eagerly here:

```rust
parse_bytes_entry(...)
  -> Datum::try_from_bytes(&entry.value, data_type)
  -> PrimitiveType::String => std::str::from_utf8(bytes)?
```

So one malformed optional string bound makes the entire manifest unreadable.

## Why This Is Surprising

Iceberg bounds are optional metrics. A malformed or legacy bad bound should not prevent operations that do not need bounds, such as:

- Listing data files
- Summing `file_size_in_bytes`
- Reading paths/counts/sizes from manifests
- Other metadata-only operations that do not evaluate string bounds

Apache Iceberg Java appears to avoid this by storing bounds as `Map<Integer, ByteBuffer>` and decoding through `Conversions.fromByteBuffer(...)` only when the typed value is needed.

## Expected Behavior

Reading a manifest should not fail solely because an optional string lower/upper bound contains invalid UTF-8. Malformed string bounds should either be decoded lazily when explicitly needed or treated as absent optional metrics by consumers that can safely proceed without them.

## Actual Behavior

Manifest loading fails eagerly before callers can access unrelated fields.

## Possible Fixes

One option is to align more closely with Java and store manifest lower/upper bounds as raw bytes, decoding lazily only where typed bounds are required.

A smaller compatibility fix is to skip malformed string bounds during manifest deserialization, treating them like absent optional metrics, while preserving strict decoding for other bound types. With this approach, callers that later inspect bounds for that field see the bound as missing rather than receiving a UTF-8 decode error. This is conservative for scan planning and metadata analysis because missing bounds prevent metric-based pruning/proofs instead of producing incorrect results.

I have a local patch that implements the smaller fix by ignoring invalid UTF-8 string bounds in `parse_bytes_entry`, with a regression test verifying:

- Valid non-string bounds are still decoded
- Invalid string bounds are omitted
- Manifest parsing succeeds

## Relevant Code

- `crates/iceberg/src/spec/manifest/_serde.rs`
- `parse_bytes_entry`
- `Datum::try_from_bytes`
- `PrimitiveType::String`

## Notes

This is likely caused by a malformed/truncated string metric in an existing manifest. Even if the producer should not write invalid UTF-8, readers should probably avoid making optional metrics fatal for unrelated metadata operations.
