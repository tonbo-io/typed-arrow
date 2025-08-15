# arrow-native

Compile‑time Arrow schemas for Rust.

arrow-native provides a strongly typed, fully compile-time way to define Arrow columns and schemas in Rust.
It maps Rust types directly to arrow-rs typed builders/arrays and `arrow_schema::DataType` — without any
runtime `DataType` switching — enabling fast, monomorphized column construction and ergonomic row-based APIs.

## Why compile-time Arrow?

- Performance: monomorphized builders/arrays with zero dynamic dispatch; avoids runtime `DataType` matching.
- Safety: column types, names, and nullability live in the type system; mismatches fail at compile time.
- Interop: uses `arrow-array`/`arrow-schema` types directly; no bespoke runtime layer to learn.

## Quick Start

```rust
use arrow_native::{prelude::*, schema::SchemaMeta};
use arrow_native::{ListNullable, Dictionary, TimestampTz, Millisecond, Utc};

#[derive(arrow_native::Record)]
struct Address { city: String, zip: Option<i32> }

#[derive(arrow_native::Record)]
struct Person {
    id: i64,
    #[nested]
    address: Option<Address>,
    tags: Option<ListNullable<i32>>,          // List column with nullable items
    code: Option<Dictionary<i32, String>>,    // Dictionary<i32, Utf8>
    joined: TimestampTz<Millisecond, Utc>,    // Timestamp(ms) with timezone (UTC)
}

fn main() {
    // Build from owned rows
    let rows = vec![
        Person {
            id: 1,
            address: Some(Address { city: "NYC".into(), zip: None }),
            tags: Some(ListNullable(vec![Some(1), None, Some(3)])),
            code: Some(Dictionary("gold".into(), std::marker::PhantomData)),
            joined: TimestampTz::<Millisecond, Utc>(
                1_700_000_000_000,
                std::marker::PhantomData,
            ),
        },
        Person {
            id: 2,
            address: None,
            tags: None,
            code: None,
            joined: TimestampTz::<Millisecond, Utc>(
                1_700_000_100_000,
                std::marker::PhantomData,
            ),
        },
    ];

    let mut b = <Person as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    // Compile-time schema + RecordBatch
    let batch = arrays.into_record_batch();
    assert_eq!(batch.schema().fields().len(), <Person as Record>::LEN);
    println!("rows={}, field0={}", batch.num_rows(), batch.schema().field(0).name());
}
```

Add to your `Cargo.toml` (derives enabled by default):

```toml
[dependencies]
arrow-native = { version = "0.x" }
```

When working in this repository/workspace:

```toml
[dependencies]
arrow-native = { path = "." }
```

## Examples

Run the included examples to see end-to-end usage:

- `01_primitives` — derive `Record`, inspect `DataType`, build primitives
- `02_lists` — `List<T>` and `ListNullable<T>`
- `03_dictionary` — `Dictionary<K, String>`
- `04_timestamps` — `Timestamp<U>` units
- `04b_timestamps_tz` — `TimestampTz<U, Z>` with `Utc` and custom markers
- `05_structs` — nested structs → `StructArray`
- `06_rows_flat` — row-based building for flat records
- `07_rows_nested` — row-based building with `#[nested]`
- `08_record_batch` — compile-time schema + `RecordBatch`

Run:

```bash
cargo run --example 08_record_batch
```

## Core Concepts

- `Record`: implemented by the derive macro for structs with named fields.
- `ColAt<I>`: per-column associated items `Rust`, `ColumnBuilder`, `ColumnArray`, `NULLABLE`, `NAME`, and `data_type()`.
- `ArrowBinding`: compile-time mapping from a Rust value type to its Arrow builder, array, and `DataType`.
- `BuildRows`: derive generates `<Type>Builders` and `<Type>Arrays` with `append_row(s)` and `finish`.
- `SchemaMeta`: derive provides `fields()` and `schema()`; arrays structs provide `into_record_batch()`.
- `AppendStruct` and `StructMeta`: enable nested struct fields and `StructArray` building.

### Metadata (Compile-time)

- Schema-level: annotate with `#[schema_metadata(k = "owner", v = "data")]`.
- Field-level: annotate with `#[metadata(k = "pii", v = "email")]`.
- You can repeat attributes to add multiple pairs; later duplicates win.

### Nested Type Wrappers

- Lists: `List<T>` (non-null items), `ListNullable<T>` (nullable items). Use `Option<List<_>>` for list-level nulls.
- Dictionary: `Dictionary<K, String>` for dictionary-encoded Utf8 with integral keys (`i8/i16/i32/i64/u8/u16/u32/u64`).
- Timestamps: `Timestamp<Second|Millisecond|Microsecond|Nanosecond>` and `TimestampTz<U, Z>` (built-in `Utc`, custom markers via `TimeZoneSpec`).

> Note: `Vec<u8>` maps to Arrow `Binary`, so lists use explicit `List<T>`/`ListNullable<T>` wrappers to avoid
> conflicts with `Vec<T>`.

## Status & Roadmap

- Implemented: primitives, Utf8/Binary, List, Struct, Dictionary<String>, Timestamp, TimestampTz (UTC/custom markers),
  row-based building (including `Option<Record>` rows), compile-time schema + `RecordBatch`.
- Planned: `LargeList`, `FixedSizeList`, `Map`; broader Dictionary values; bulk append helpers.

See `AGENTS.md` and docs in `src/bridge.rs` for deeper details and examples.

---

This project builds on the `arrow-array` and `arrow-schema` crates from the Arrow Rust ecosystem.
