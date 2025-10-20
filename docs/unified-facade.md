# Unified Static/Dynamic Facade — Compile-Time Arrow Schema

## Overview
- Goal: Provide a single ergonomic surface that can operate over either a compile-time `Record` or a runtime-loaded Arrow `Schema`, without sacrificing monomorphized, typed dispatch where available.
- Approach: Two facades over one core mapping. Keep `ArrowBinding` and wrappers (List/Map/Dictionary/Timestamp/etc.) as the single source of truth for leaf type semantics. Layer a dynamic facade next to the existing static one and offer an enum wrapper to unify common tasks (schema, ingest/builders, `RecordBatch`).

## Design Summary
- Shared core: `bridge::ArrowBinding` drives Rust type → Arrow builder/array/DataType for both static and dynamic paths.
- Static API: `Record`, `ColAt<I>`, `ForEachCol`, `BuildRows`, `SchemaMeta` (unchanged). No runtime `DataType` switches; all code monomorphizes.
- Dynamic API: Minimal runtime builders facade with a single `DataType` switch in a factory. Produces `ArrayRef`s and `RecordBatch` for arbitrary `SchemaRef`.
- Unified facade: Implemented as traits in `typed-arrow-unified` — `SchemaLike` and `BuildersLike` — with `Typed<R>`, `DynSchema`, and `Arc<Schema>` as implementors. Shared operations include building `RecordBatch`es from rows and constructing builders with capacity. Builders expose `finish_into_batch()` and `try_finish_into_batch()`; the latter returns diagnostic errors when available (dynamic path).

## Workspace Structure (Crates + Features)

Keep everything in a single repository as a workspace with focused crates. Make the dynamic/unified layers optional for users who only need static typing.

- typed-arrow (core)
  - Contents: `Record`, `ColAt<I>`, `ForEachCol`, `BuildRows`, `SchemaMeta`, and the `ArrowBinding` mapping + wrappers.
  - Dependencies: `arrow-schema` (and minimal `arrow-array` only when required by types), no dynamic layer deps.
  - Features:
    - `derive` (existing): pulls `typed-arrow-derive`.
    - `unified` (new): re-exports items from `typed-arrow-unified` for convenience.

- typed-arrow-derive (proc-macro)
  - Contents: `#[derive(Record)]` (and future `#[derive(Union)]`).
  - Depends only on syn/quote/proc-macro2; no Arrow runtime.

- typed-arrow-dyn (dynamic facade)
  - Contents: `DynSchema`, `DynBuilders`, `DynColumnBuilder`, `DynCell`/`DynRow`, and `new_dyn_builder(dt, capacity)` factory. Holds the only `match DataType`. Column/field/item nullability is not passed to the factory; Arrow enforces nullability when arrays/RecordBatches are constructed.
  - Dependencies: `typed-arrow` (for ArrowBinding/wrappers), `arrow-array`, `arrow-schema`, `arrow-buffer`.
  - Feature gates: optional per-container coverage or Arrow version gates if needed.

- typed-arrow-unified (unified traits facade)
  - Contents: `SchemaLike`, `BuildersLike`, `Typed<R>` marker, and implementations for `DynSchema` and `Arc<Schema>`.
  - Dependencies: `typed-arrow` and `typed-arrow-dyn`.

Developer ergonomics and re-exports
- Users wanting only static APIs: depend on `typed-arrow` (default `derive` feature).
- Dynamic-only users: depend on `typed-arrow-dyn`.
- Unified facade: depend on `typed-arrow-unified` directly.
  - Note: re-exporting `typed-arrow-unified` from `typed-arrow` creates a cycle (`typed-arrow → typed-arrow-unified → typed-arrow`). To avoid this, keep `typed-arrow-unified` as a separate crate, or introduce a small aggregator crate (e.g., `typed-arrow-full`) that depends on both and re-exports convenience APIs.

CI/test matrix
- Core only: `typed-arrow` with default features.
- Unified crate: build/test `typed-arrow-unified` (pulls `typed-arrow-dyn`).
- Dynamic crate direct: build/test `typed-arrow-dyn` independently to ensure factory coverage.

## Types and API Surface

### Unified Traits and Types
```rust
// typed-arrow-unified
pub trait BuildersLike {
    type Row;
    type Error: std::error::Error;
    fn append_row(&mut self, row: Self::Row) -> Result<(), Self::Error>;
    fn append_option_row(&mut self, row: Option<Self::Row>) -> Result<(), Self::Error>;
    fn finish_into_batch(self) -> arrow_array::RecordBatch;
    fn try_finish_into_batch(self) -> Result<arrow_array::RecordBatch, Self::Error> where Self: Sized { /* default: Ok(finish) */ }
}

pub trait SchemaLike {
    type Row;
    type Builders: BuildersLike<Row = Self::Row>;
    fn schema_ref(&self) -> std::sync::Arc<arrow_schema::Schema>;
    fn new_builders(&self, capacity: usize) -> Self::Builders;
    fn build_batch<I>(&self, rows: I) -> Result<arrow_array::RecordBatch, <Self::Builders as BuildersLike>::Error>
    where
        I: IntoIterator<Item = Self::Row>;
}

// Implementors
pub struct Typed<R> { /* marker */ }
impl<R: typed_arrow::schema::BuildRows + typed_arrow::schema::SchemaMeta> SchemaLike for Typed<R> { /* ... */ }

impl SchemaLike for typed_arrow_dyn::DynSchema { /* ... */ }
impl SchemaLike for std::sync::Arc<arrow_schema::Schema> { /* ... */ }
```

### Typed Path
```rust
use typed_arrow::schema::{BuildRows, SchemaMeta};
use typed_arrow_unified::{SchemaLike, Typed};

#[derive(typed_arrow::Record)]
struct Person { id: i64, name: Option<String> }

let schema = Typed::<Person>::default();
let rows = vec![
    Person { id: 1, name: Some("a".into()) },
    Person { id: 2, name: None },
];
let batch = schema.build_batch(rows).unwrap();
```

### Dynamic Path
```rust
use std::sync::Arc;
use arrow_schema::{Field, Schema, DataType};
use typed_arrow_unified::SchemaLike;
use typed_arrow_dyn::{DynRow, DynCell, DynSchema};

let schema = Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, true),
]);
let dyn_schema = DynSchema::new(schema);
let rows = vec![
    DynRow(vec![Some(DynCell::I64(1)), Some(DynCell::Str("a".into()))]),
    DynRow(vec![Some(DynCell::I64(2)), None]),
];
let batch = dyn_schema.build_batch(rows).unwrap();
```

### Builders Interface
The unified builders interface is provided by `BuildersLike`.

```rust
use typed_arrow_unified::SchemaLike;

// Typed
let mut b = typed_arrow_unified::Typed::<Person>::default().new_builders(2);
b.append_row(Person { id: 1, name: None })?;
let batch = b.finish_into_batch();

// Dynamic
let mut b = dyn_schema.new_builders(2);
b.append_row(DynRow(vec![Some(DynCell::I64(1)), None]))?;
let batch = b.finish_into_batch();
```

### Dynamic Builders and Factory
```rust
pub trait DynColumnBuilder {
    fn data_type(&self) -> &arrow_schema::DataType;
    fn append_null(&mut self);
    fn append_dyn(&mut self, v: DynCell) -> Result<(), DynError>;
    fn finish(&mut self) -> arrow_array::ArrayRef;
}

pub struct DynBuilders {
    schema: arrow_schema::SchemaRef,
    cols: Vec<Box<dyn DynColumnBuilder>>,
    len: usize,
}

impl DynBuilders {
    pub fn new(schema: arrow_schema::SchemaRef, capacity: usize) -> Self {
        let cols = schema
            .fields()
            .iter()
            .map(|f| new_dyn_builder(f.data_type(), capacity))
            .collect();
        Self { schema, cols, len: 0 }
    }

    pub fn append_option_row(&mut self, row: Option<DynRow>) {
        match row {
            None => { for c in &mut self.cols { c.append_null(); } }
            Some(r) => { r.append_into(&mut self.cols); }
        }
        self.len += 1;
    }

    pub fn finish_into_batch(mut self, name: Option<&str>) -> arrow_array::RecordBatch {
        let arrays: Vec<_> = self.cols.iter_mut().map(|c| c.finish()).collect();
        let schema = if let Some(n) = name {
            arrow_schema::SchemaRef::new(self.schema.as_ref().clone().with_metadata({
                let mut m = self.schema.metadata().clone();
                m.insert("name".into(), n.into());
                m
            }))
        } else { self.schema.clone() };
// Dynamic builders validate nullability at try-finish and return errors with path context.
        arrow_array::RecordBatch::try_new(schema, arrays).expect("shape verified")
    }
}
```

Notes on nullability in the dynamic path
- Appends treat `None`/`DynCell::Null` as null and do not check field/item nullability at append-time.
- Arrow enforces column/field/item nullability when building arrays and the `RecordBatch`. If a non-nullable field/item contains nulls, construction will panic at finish.

## What Unifies vs. What Doesn’t
- Unifies well:
  - Schema: `SchemaLike::schema_ref()` returns `Arc<Schema>` for both typed and dynamic.
  - Ingestion: `BuildersLike` unifies append and finish across typed and dynamic builders; `SchemaLike::build_batch` builds `RecordBatch` from rows.
- Does not unify:
  - Typed column-generic kernels (`ForEachCol::for_each_col::<V>()`) require compile-time `R`; dynamic schemas don’t participate. Iterate `Schema::fields()` for dynamic inspection.

## Column Iteration
- Typed: use `ForEachCol::for_each_col::<V>()` to visit columns at compile time.
- Dynamic: iterate `schema_ref().fields()` directly.

```rust
let schema = dyn_schema.schema.clone();
for (i, f) in schema.fields().iter().enumerate() {
    println!("{}: {:?} nullable={}", i, f.data_type(), f.is_nullable());
}
```

## Examples
### Typed-only
```rust
use typed_arrow::prelude::*;
use typed_arrow_unified::{SchemaLike, Typed};

#[derive(Record)]
struct Person { id: i64, email: Option<String> }

let rows = vec![
    Person { id: 1, email: None },
    Person { id: 2, email: Some("x".into()) },
];
let schema = Typed::<Person>::default();
let batch = schema.build_batch(rows).unwrap();
```

### Dynamic-only
```rust
use std::sync::Arc;
use arrow_schema::{Field, Schema, DataType};
use typed_arrow_unified::SchemaLike;
use typed_arrow_dyn::{DynCell, DynRow, DynSchema};

let schema = Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("email", DataType::Utf8, true),
]);
let dyn_schema = DynSchema::new(schema);
let rows = vec![
    DynRow(vec![Some(DynCell::I64(1)), None]),
    DynRow(vec![Some(DynCell::I64(2)), Some(DynCell::Str("x".into()))]),
];
let batch = dyn_schema.build_batch(rows).unwrap();
```

## Validation and Errors
- Typed builders: appends are infallible (`NoError`), finish returns typed arrays converted to `RecordBatch`.
- Dynamic builders: appends return `DynError` for arity/type/builder issues. Nullability violations are enforced by arrow-rs at array/RecordBatch construction and will panic on violation.
- Factory is the only place with a `DataType` switch for dynamic builders.

## Rollout Plan
- Phase A0: Scaffold crates `typed-arrow-dyn` and `typed-arrow-unified`. Add `unified` feature to `typed-arrow` that re-exports the unified API.
- Phase A: Implement `SchemaLike`/`BuildersLike` with `Typed<R>`, `DynSchema`, and `Arc<Schema>`; add examples that build a `RecordBatch` from both typed and dynamic paths.
- Phase B: Implement dynamic factory for primitives, Utf8, Binary, Struct, Lists and FixedSizeList; `RecordBatch` assembly.
- Phase C: Extend dynamic support to Map/Timestamp/Decimal/Dictionary (values and primitives) via the same `ArrowBinding` mapping.
- Phase D: Optional Union dynamic builders.

## Open Questions
- Dynamic row representation (`DynRow`/`DynCell`): JSON-like shape, or a typed enum of supported scalars/containers? Favor a thin enum tree mirroring Arrow logical types for zero-copy where possible.
- Schema equality: use strict field-order and `keys_sorted`/timezone metadata equality (`equals_deterministic`).
- Timezone strategy: start with `TimestampTz<_, Utc>`; revisit general TZs later.
- Feature gating across arrow-rs versions: guard dynamic builder impls behind crate features to track API differences.

## Notes
- This facade does not replace the typed path. It surfaces shared operations and keeps typed kernels fast and monomorphized when `R` is known at compile time.
- Keep the dynamic layer small and focused on ingestion and bridging.
