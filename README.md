# typed-arrow

Compile‑time Arrow schemas for Rust.

typed-arrow provides a strongly typed, fully compile-time way to define Arrow columns and schemas in Rust.
It maps Rust types directly to arrow-rs typed builders/arrays and `arrow_schema::DataType` — without any
runtime `DataType` switching — enabling fast, monomorphized column construction and ergonomic row-based APIs.

## Why compile-time Arrow?

- Performance: monomorphized builders/arrays with zero dynamic dispatch; avoids runtime `DataType` matching.
- Safety: column types, names, and nullability live in the type system; mismatches fail at compile time.
- Interop: uses `arrow-array`/`arrow-schema` types directly; no bespoke runtime layer to learn.

## Quick Start

```rust
use typed_arrow::{prelude::*, schema::SchemaMeta};
use typed_arrow::{Dictionary, TimestampTz, Millisecond, Utc, List};

#[derive(typed_arrow::Record)]
struct Address { city: String, zip: Option<i32> }

#[derive(typed_arrow::Record)]
struct Person {
    id: i64,
    #[record(nested)]
    address: Option<Address>,
    tags: Option<List<Option<i32>>>,          // List column with nullable items
    code: Option<Dictionary<i32, String>>,    // Dictionary<i32, Utf8>
    joined: TimestampTz<Millisecond, Utc>,    // Timestamp(ms) with timezone (UTC)
}

fn main() {
    // Build from owned rows
    let rows = vec![
        Person {
            id: 1,
            address: Some(Address { city: "NYC".into(), zip: None }),
            tags: Some(List(vec![Some(1), None, Some(3)])),
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
typed-arrow = { version = "0.x" }
```

When working in this repository/workspace:

```toml
[dependencies]
typed-arrow = { path = "." }
```

## Examples

Run the included examples to see end-to-end usage:

- `01_primitives` — derive `Record`, inspect `DataType`, build primitives
- `02_lists` — `List<T>` and `List<Option<T>>`
- `03_dictionary` — `Dictionary<K, String>`
- `04_timestamps` — `Timestamp<U>` units
- `04b_timestamps_tz` — `TimestampTz<U, Z>` with `Utc` and custom markers
- `05_structs` — nested structs → `StructArray`
- `06_rows_flat` — row-based building for flat records
- `07_rows_nested` — row-based building with `#[record(nested)]`
- `08_record_batch` — compile-time schema + `RecordBatch`
- `09_duration_interval` — Duration and Interval types
- `10_union` — Dense Union as a Record column (with attributes)
- `11_map` — Map (incl. `Option<V>` values) + as a Record column

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

- Lists: `List<T>` (non-null items), `List<Option<T>>` (nullable items). Use `Option<List<_>>` for list-level nulls.
- Dictionary: dictionary-encoded values with integral keys (`i8/i16/i32/i64/u8/u16/u32/u64`):
  - `Dictionary<K, String>` (Utf8)
  - `Dictionary<K, Vec<u8>>` (Binary)
  - `Dictionary<K, T>` for primitives `T ∈ { i8, i16, i32, i64, u8, u16, u32, u64, f32, f64 }`

## Arrow DataType Coverage

Supported (arrow-rs v56):

- Primitives: Int8/16/32/64, UInt8/16/32/64, Float16/32/64, Boolean
- Strings/Binary: Utf8, LargeUtf8, Binary, LargeBinary, FixedSizeBinary (via `[u8; N]`)
- Temporal: Timestamp (with/without TZ; s/ms/us/ns), Date32/64, Time32(s/ms), Time64(us/ns), Duration(s/ms/us/ns), Interval(YearMonth/DayTime/MonthDayNano)
- Decimal: Decimal128, Decimal256 (const generic precision/scale)
- Nested: List (including nullable items), LargeList, FixedSizeList (nullable/non-null items), Struct,
  Map (Vec<(K,V)>; use `Option<V>` for nullable values), OrderedMap (BTreeMap<K,V>) with `keys_sorted = true`
- Union: Dense and Sparse (via `#[derive(Union)]` on enums)
- Dictionary: keys = all integral types; values = Utf8 (String), LargeUtf8, Binary (Vec<u8>), LargeBinary, FixedSizeBinary (`[u8; N]`), primitives (i*, u*, f32, f64)

Missing:

- BinaryView, Utf8View
- Utf8View
- ListView, LargeListView
- RunEndEncoded
