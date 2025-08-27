# Dynamic Runtime Design (typed-arrow-dyn)

## Overview
- Purpose: Build Arrow arrays and RecordBatches from runtime schemas (`arrow_schema::Schema`) without compile-time type information.
- Scope: Provide a small, focused dynamic facade that mirrors typed behavior where reasonable, keeping per-append overhead low. Nullability invariants (columns/fields/items) are validated at try-finish and returned as structured errors.

## Goals
- Single `DataType` switch: map `arrow_schema::DataType` to a concrete builder once per column (factory).
- Minimal append-time checks: pre-validate row arity and value type compatibility only; avoid expensive checks per append.
- Nullability: validate column/field/item constraints before finishing and return `DynError::Nullability { col, path, index, message }`; avoid panics from arrow-rs by catching issues earlier.
- Unified surface: interoperate with `typed-arrow-unified` (`SchemaLike`/`BuildersLike`) so typed and dynamic paths feel symmetric.

## Public API Surface
- `DynSchema`: thin wrapper over `Arc<Schema>` for the dynamic path.
- `DynBuilders`:
  - `new(schema: Arc<Schema>, capacity: usize) -> DynBuilders`
  - `append_option_row(row: Option<DynRow>) -> Result<(), DynError>`
  - `finish_into_batch(self) -> RecordBatch`
  - `try_finish_into_batch(self) -> Result<RecordBatch, DynError>`
- `DynRow(Vec<Option<DynCell>>)` and `DynCell` enum for dynamic values.
- `DynColumnBuilder` (trait object) implemented by the factory output.
- Factory: `new_dyn_builder(dt: &DataType) -> Box<dyn DynColumnBuilder>`.

## Semantics
- Appends
  - `None` (at the row level) appends null to every column.
  - Per-column `None`/`DynCell::Null` appends a null cell for that column.
  - `DynRow::append_into` first checks:
    - Arity: row cell count must equal schema width → `DynError::ArityMismatch`.
    - Type compatibility: each `DynCell` must be acceptable for the column `DataType` → `DynError::TypeMismatch`.
  - On success, dynamic builders receive either `append_null()` or `append_dyn(v)` (which can still return a builder error).

- Finish
  - `DynBuilders::finish_into_batch` calls `finish()` on each column builder and then `RecordBatch::try_new`.
  - `DynBuilders::try_finish_into_batch` runs a nullability validator against the arrays using the `Schema`; if any violation is found, it returns a `DynError::Nullability` pointing to the first offending path and index.

## Errors and Panics
- Structured errors (`DynError`):
  - `ArityMismatch { expected, got }` — row length differs from schema width.
  - `TypeMismatch { col, expected }` — a cell’s Rust value type does not match the target Arrow `DataType`.
  - `Builder { message }` — underlying Arrow builder returned an error (e.g., FixedSizeBinary width mismatch).
  - `Append { col, message }` — contextualized `Builder` error with column index.
- Panics:
  - Panics from arrow-rs due to nullability should be avoided by using `try_finish_into_batch`; `finish_into_batch` may still panic if a caller bypasses validation.

## Type Compatibility (pre-validation)
- Scalars:
  - `Boolean` ← `DynCell::Bool`
  - `Int*`/`UInt*` ← corresponding `DynCell::I*`/`DynCell::U*`
  - `Float32/64` ← `DynCell::F32/F64`
  - `Date32/64` ← `DynCell::I32/I64`
  - `Time32(_)` ← `DynCell::I32`; `Time64(_)` ← `DynCell::I64`
  - `Duration(_)` ← `DynCell::I64`
  - `Timestamp(_, _)` ← `DynCell::I64`
  - `Utf8/LargeUtf8` ← `DynCell::Str`
  - `Binary/LargeBinary/FixedSizeBinary(w)` ← `DynCell::Bin` (builder enforces fixed width)
- Dictionary values: accept the same `DynCell` as the value type; key width is handled by the builder.
- Nested:
  - `Struct(fields)` ← `DynCell::Struct(Vec<Option<DynCell>>)` with matching arity.
  - `List/LargeList(item)` ← `DynCell::List(Vec<Option<DynCell>>)`.
  - `FixedSizeList(item, len)` ← `DynCell::FixedSizeList(Vec<Option<DynCell>>)` with exact length.

## Nested Builders (invariants)
- Struct:
  - `append_null()`: appends null to each child, then marks the parent invalid.
  - `append_struct(cells)`: checks child arity, appends per-child cells, marks valid.
- List / LargeList:
  - `append_null()`: repeats last offset, appends invalid to parent validity.
  - `append_list(items)`: appends each item to the child, advances offsets by item count, marks valid.
- FixedSizeList:
  - Enforces exact child length; on `append_null()` writes `len` child nulls, then marks parent invalid.

## Dictionary Support
- Keys: all integral types `i8/i16/i32/i64/u8/u16/u32/u64`.
- Values:
  - Utf8/LargeUtf8 via `StringDictionaryBuilder`.
  - Binary/LargeBinary/FixedSizeBinary via `Binary*DictionaryBuilder` (F.S. width enforced by builder).
  - Primitive numeric/float values via a small trait-object wrapper that avoids an overly large enum in the factory.

## Factory Design
- `new_dyn_builder(&DataType)` holds the single `match DataType` in the dynamic crate.
- Returns a `Box<dyn DynColumnBuilder>` implemented by a small struct wrapping an internal enum of concrete builders.
- Nested types recursively call the factory for children.

## Performance Notes
- Append-time checks are intentionally light (arity + type compatibility) to avoid partial writes and costly per-item checks.
- Capacity: `DynBuilders::new` accepts a capacity hint; individual Arrow builders may not preallocate yet — future work.
- `DynColumnBuilder: Send` so trait objects can be moved across threads when needed.

## Coverage (current vs. planned)
- Implemented:
  - Primitives, Boolean, Utf8/LargeUtf8, Binary/LargeBinary/FixedSizeBinary
  - Date/Time/Duration/Timestamp
  - Struct, List, LargeList, FixedSizeList
  - Dictionary (keys: all integrals; values: Utf8/LargeUtf8, Binary/LargeBinary/FixedSizeBinary, numeric/float primitives)
- Planned:
  - Map/OrderedMap builders (dynamic)
  - Union (dense/sparse) builders (dynamic)
  - Decimal128/256, Interval types (dynamic)
  - Capacity preallocation across dynamic builders
  - Convenience helper to bind a runtime `Schema` to a typed `R` when shapes match

## Usage Examples
Basic dynamic build
```rust
use std::sync::Arc;
use arrow_schema::{DataType, Field, Schema};
use typed_arrow_dyn::{DynBuilders, DynCell, DynRow};

let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, true),
 ]));
let mut b = DynBuilders::new(schema, 2);
b.append_option_row(Some(DynRow(vec![Some(DynCell::I64(1)), Some(DynCell::Str("a".into()))])))?;
b.append_option_row(Some(DynRow(vec![Some(DynCell::I64(2)), None])))?;
let batch = b.try_finish_into_batch()?; // returns error if nullability is violated
```

Nested and nullability
```rust
use std::sync::Arc;
use arrow_schema::{DataType, Field, Fields, Schema};
use typed_arrow_dyn::{DynBuilders, DynCell, DynRow};

let person_fields = vec![
    Arc::new(Field::new("name", DataType::Utf8, false)),
    Arc::new(Field::new("age", DataType::Int32, true)),
];
let schema = Arc::new(Schema::new(vec![
    Field::new("person", DataType::Struct(Fields::from(person_fields)), true),
]));

let mut b = DynBuilders::new(Arc::clone(&schema), 0);
// Entire struct None allowed (parent nullable)
b.append_option_row(Some(DynRow(vec![None])))?;
// Child None where non-nullable (name) — validation will catch this
b.append_option_row(Some(DynRow(vec![Some(DynCell::Struct(vec![
    None,
    Some(DynCell::I32(10)),
]))])))?;
let err = b.try_finish_into_batch().unwrap_err();
assert!(format!("{}", err).contains("nullability"));
```

## Invariants & Internal Notes
- `DynRow::append_into` includes one `unreachable!()` arm after arity pre-check — the iterator cannot be exhausted early.
- Struct/List/LargeList/FSL builders maintain validity and offsets consistent with arrow-rs expectations.
- FixedSizeBinary width and FixedSizeList item counts are enforced by the builders and return `DynError::Builder` on mismatch.

## See Also
- `docs/unified-facade.md` — unified typed/dynamic traits (`SchemaLike`, `BuildersLike`).
- `AGENTS.md` — project goals, high-level architecture, and guidelines.
