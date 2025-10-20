# typed-arrow-dyn

`typed-arrow-dyn` is the runtime half of the typed-arrow story. Where the main crate gives you a fully compile-time schema, this crate builds Arrow arrays and `RecordBatch`es from schemas that are only known at runtime. It keeps the API surface focused on batch construction so you can plug dynamic data sources into the `typed-arrow-unified` facade without sprinkling `match DataType` throughout your code.

## What It Provides
- `DynSchema`: a thin `Arc<Schema>` wrapper that feeds the unified `SchemaLike` trait.
- `DynBuilders`: one builder per field, created directly from the runtime schema and monomorphized per Arrow logical type.
- `DynRow` and `DynCell`: ergonomics for appending rows where every cell is either a value or `None`.
- `DynError`: structured diagnostics for arity, type mismatches, builder failures, and deferred nullability violations.
- `validate_nullability`: a post-build walk that enforces field and item nullability, returning precise paths such as `person.address.street[]`.

Everything is designed to mirror the infallible typed path: builder allocation happens up front, appends stream through with minimal branching, and nullability is validated once via `try_finish_into_batch`.

## Quick Start

```rust
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use typed_arrow_dyn::{DynBuilders, DynCell, DynRow, DynSchema, DynError};

fn build_batch() -> Result<arrow_array::RecordBatch, DynError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(
            "events",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![
                    Arc::new(Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), false)),
                    Arc::new(Field::new("payload", DataType::Utf8, true)),
                ].into()),
                true,
            ))),
            true,
        ),
    ]));

    let mut builders = DynBuilders::new(Arc::clone(&schema), 0);
    // row0: id=1, name="alice", events=[{ts: 10, payload: null}]
    builders.append_option_row(Some(DynRow(vec![
        Some(DynCell::I64(1)),
        Some(DynCell::Str("alice".into())),
        Some(DynCell::List(vec![Some(DynCell::Struct(vec![
            Some(DynCell::I64(10)),
            None,
        ]))])),
    ])))?;

    // row1: id=2, name=null, events=null
    builders.append_option_row(Some(DynRow(vec![
        Some(DynCell::I64(2)),
        None,
        None,
    ])))?;

    // Prefer the fallible finish: it validates nullability paths and surfaces Arrow errors.
    builders.try_finish_into_batch()
}
```

For ad hoc debugging you can still call `finish_into_batch()`, which will panic if Arrow rejects the arrays. Production code should stick with `try_finish_into_batch()` to get `DynError::Nullability` or `DynError::Builder` with context.

## Rows & Cells
- `DynRow(Vec<Option<DynCell>>)` lines up with the schema width. Passing `None` at the top level appends a null to the entire column.
- `DynCell` enumerates every value shape the factory understands: booleans, signed/unsigned integers, floating point, UTF-8 strings, binary blobs, dictionary payloads, and the nested variants:
  - `Struct(Vec<Option<DynCell>>)`—one entry per child field.
  - `List(Vec<Option<DynCell>>)`, reused for `List` and `LargeList`.
  - `FixedSizeList(Vec<Option<DynCell>>)`—length must match the field’s declared width.
  - `Map(Vec<(DynCell, Option<DynCell>)>)`—each entry is a `(key, value)` pair; keys must be non-null and values obey the schema’s nullability.
- Dictionary columns accept the payload type (`Str`, `Bin`, or primitive variants); the key handling stays inside the builder.

`DynRow::append_into_with_fields` performs a lightweight type check before mutating builders, so arity/type mistakes fail fast without leaving partially-written columns.

## Dynamic Builders
`DynBuilders::new(schema, capacity)` constructs one concrete builder per field by calling [`new_dyn_builder`](src/factory.rs) with the logical type and the capacity hint. The factory is the only place that matches on `arrow_schema::DataType`; every builder is stored behind the `DynColumnBuilder` trait object with methods:

```rust
trait DynColumnBuilder {
    fn data_type(&self) -> &DataType;
    fn append_null(&mut self);
    fn append_dyn(&mut self, value: DynCell) -> Result<(), DynError>;
    fn finish(&mut self) -> ArrayRef;
    fn try_finish(&mut self) -> Result<ArrayRef, DynError>;
}
```

High-level users rarely call the trait directly—the unified facade hands out `DynBuilders` and keeps the append API aligned with the typed path (`append_option_row`, `append_rows`, etc.).

## Error Model

`DynError` keeps the dynamic path predictable without drowning you in variants. Appends return `Result<(), DynError>`, capturing whether the row shape matches the schema, the value fits the Arrow type, or the builder rejected the insert. Finishing returns `Result<RecordBatch, DynError>` and adds nullability validation so callers know exactly why Arrow construction failed—no panics, just structured context you can surface to users or logs.

## Nullability Enforcement

Dynamic builders defer nullability checks until the batch is sealed. `validate_nullability(schema, arrays)` walks the resulting arrays and enforces:

- Non-nullable columns have no null slots.
- Struct children obey their own nullability only where the parent is valid.
- List, LargeList, and FixedSizeList items respect child nullability.
- Map columns reject null keys and enforce the value field’s nullability.

Violations bubble up as `DynError::Nullability` with `col`, `path`, and `index` for precise diagnostics, allowing the unified facade to report user-friendly messages instead of panicking.

## Integration Points

- `DynSchema` satisfies `typed_arrow_unified::SchemaLike` for runtime cases, so you can switch between typed and dynamic implementations behind a single API.
- `DynBuilders` implements the unified `BuildersLike` contract; typed builders use a zero-cost `NoError`, while dynamic builders return `Result`.
- Lower-level consumers can call `new_dyn_builder(data_type, capacity_hint)` to embed dynamic columns into custom pipelines without adopting the whole facade.

## Supported Data Types

The factory builds the following Arrow logical types (Arrow RS v56):

- Null, Boolean
- Int8/16/32/64, UInt8/16/32/64, Float32/64
- Date32/64, Timestamp (all units, optional timezone), Duration (all units), Time32 (Second/Millisecond), Time64 (Microsecond/Nanosecond)
- Utf8, LargeUtf8, Binary, LargeBinary, FixedSizeBinary
- Dictionary with the above strings/binary types or primitive values
- Struct, List, LargeList, FixedSizeList, Map (including nested combinations)

Unsupported types currently fall back to a `NullBuilder`. Extend `new_dyn_builder` as Arrow gains new logical types.

## Examples & Tests

- `cargo run -p typed-arrow-dyn --example nested_struct_list` shows nested structs and lists.
- `cargo test -p typed-arrow-dyn` exercises dictionaries, deep nesting, and nullability validation.

## License

This crate shares the repository license; see [`LICENSE`](../LICENSE).
