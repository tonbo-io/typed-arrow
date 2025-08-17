# Unified Static/Dynamic Facade — Compile-Time Arrow Schema

## Overview
- Goal: Provide a single ergonomic surface that can operate over either a compile-time `Record` or a runtime-loaded Arrow `Schema`, without sacrificing monomorphized, typed dispatch where available.
- Approach: Two facades over one core mapping. Keep `ArrowBinding` and wrappers (List/Map/Dictionary/Timestamp/etc.) as the single source of truth for leaf type semantics. Layer a dynamic facade next to the existing static one and offer an enum wrapper to unify common tasks (schema, ingest/builders, `RecordBatch`).

## Design Summary
- Shared core: `bridge::ArrowBinding` drives Rust type → Arrow builder/array/DataType for both static and dynamic paths.
- Static API: `Record`, `ColAt<I>`, `ForEachCol`, `BuildRows`, `SchemaMeta` (unchanged). No runtime `DataType` switches; all code monomorphizes.
- Dynamic API: Minimal runtime builders facade with a single `DataType` switch in a factory. Produces `ArrayRef`s and `RecordBatch` for arbitrary `SchemaRef`.
- Unifying enum: `UnifiedRecord<R = DynMarker>` wraps either compile-time type info or a runtime schema. It exposes the shared operations; typed-only operations remain gated behind `R: Record`.

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
  - Contents: `DynSchema`, `DynBuilders`, `DynColumnBuilder`, `DynCell`/`DynRow`, and `new_dyn_builder(dt, nullable)` factory. Holds the only `match DataType`.
  - Dependencies: `typed-arrow` (for ArrowBinding/wrappers), `arrow-array`, `arrow-schema`, `arrow-buffer`.
  - Feature gates: optional per-container coverage or Arrow version gates if needed.

- typed-arrow-unified (enum facade)
  - Contents: `DynMarker`, `UnifiedRecord<R = DynMarker>`, `UnifiedBuilders`, `UnifiedRow`, downcast helpers (`try_bind`/`try_as_static`).
  - Dependencies: `typed-arrow` and `typed-arrow-dyn`.

Developer ergonomics and re-exports
- Users wanting only static APIs: depend on `typed-arrow` (default `derive` feature).
- Dynamic-only users: depend on `typed-arrow-dyn`.
- Unified facade: either depend on `typed-arrow-unified` directly, or enable `typed-arrow` feature `unified` and import from `typed_arrow::unified`.

CI/test matrix
- Core only: `typed-arrow` with default features.
- Unified enabled: `typed-arrow` with `--features unified` (brings in unified + dyn).
- Dynamic crate direct: build/test `typed-arrow-dyn` independently to ensure factory coverage.

## Types and API Surface

### Marker and Enum
```rust
pub struct DynMarker;

pub enum UnifiedRecord<R = DynMarker> {
    CompileTime(std::marker::PhantomData<R>),
    Runtime(DynSchema),
}
```

- Default type parameter `DynMarker` enables `UnifiedRecord` to be used without generics for dynamic-only scenarios.
- The enum itself is unbounded; typed-only methods are provided in an inherent `impl<R: Record>` block.

### Typed Surface (only when R: Record)
```rust
impl<R: Record> UnifiedRecord<R> {
    pub fn compile_time() -> Self { Self::CompileTime(std::marker::PhantomData) }

    pub fn schema_ref(&self) -> arrow_schema::SchemaRef {
        match self {
            Self::CompileTime(_) => typed_arrow::schema::<R>(),
            Self::Runtime(d) => d.schema.clone(),
        }
    }

    pub fn builders(&self, cap: usize) -> UnifiedBuilders<R> {
        match self {
            Self::CompileTime(_) => UnifiedBuilders::CompileTime(<R as BuildRows>::new_builders(cap)),
            Self::Runtime(d) => UnifiedBuilders::Runtime(DynBuilders::new(d.schema.clone(), cap)),
        }
    }

    pub fn with_static<T>(&self, f: impl FnOnce(StaticFacade<R>) -> T) -> Option<T> {
        match self {
            Self::CompileTime(_) => Some(f(StaticFacade::<R> { _pd: std::marker::PhantomData })),
            Self::Runtime(_) => None,
        }
    }

    pub fn try_as_static(&self) -> Result<StaticFacade<R>, DynMismatch> {
        match self {
            Self::CompileTime(_) => Ok(StaticFacade::<R> { _pd: std::marker::PhantomData }),
            Self::Runtime(d) => {
                if typed_arrow::schema::<R>().equals_deterministic(&d.schema) {
                    Ok(StaticFacade::<R> { _pd: std::marker::PhantomData })
                } else {
                    Err(DynMismatch { /* shape details */ })
                }
            }
        }
    }
}

pub struct StaticFacade<R: Record> { pub(crate) _pd: std::marker::PhantomData<R> }
```

### Dynamic Surface (when R = DynMarker)
```rust
pub struct DynSchema { pub schema: arrow_schema::SchemaRef }

impl UnifiedRecord<DynMarker> {
    pub fn runtime(schema: arrow_schema::SchemaRef) -> Self { Self::Runtime(DynSchema { schema }) }

    pub fn schema_ref(&self) -> arrow_schema::SchemaRef {
        match self {
            Self::Runtime(d) => d.schema.clone(),
            Self::CompileTime(_) => unreachable!("dynamic-only"),
        }
    }

    pub fn builders(&self, cap: usize) -> UnifiedBuilders<DynMarker> {
        match self {
            Self::Runtime(d) => UnifiedBuilders::Runtime(DynBuilders::new(d.schema.clone(), cap)),
            Self::CompileTime(_) => unreachable!("dynamic-only"),
        }
    }

    pub fn try_bind<R: Record>(&self) -> Result<UnifiedRecord<R>, DynMismatch> {
        let d = match self { UnifiedRecord::Runtime(d) => d, _ => unreachable!() };
        if typed_arrow::schema::<R>().equals_deterministic(&d.schema) {
            Ok(UnifiedRecord::<R>::CompileTime(std::marker::PhantomData))
        } else {
            Err(DynMismatch { /* shape details */ })
        }
    }
}
```

### Unified Builders
```rust
pub enum UnifiedBuilders<R = DynMarker> {
    CompileTime(<R as BuildRows>::Builders),
    Runtime(DynBuilders),
}

pub enum UnifiedRow<R = DynMarker> {
    Static(Option<R>),
    Dynamic(Option<DynRow>),
}

impl<R: Record> UnifiedBuilders<R> {
    pub fn append_null_row(&mut self) { match self {
        Self::CompileTime(b) => b.append_null_row(),
        Self::Runtime(b) => b.append_null_row(),
    } }

    pub fn append_unified_row(&mut self, row: UnifiedRow<R>) -> Result<(), AppendError> {
        match (self, row) {
            (Self::CompileTime(b), UnifiedRow::Static(r)) => { b.append_option_row(r); Ok(()) }
            (Self::Runtime(b), UnifiedRow::Dynamic(r)) => { b.append_option_row(r); Ok(()) }
            _ => Err(AppendError::RowVariantMismatch),
        }
    }

    pub fn finish_into_batch(self, name: Option<&str>) -> arrow_array::RecordBatch {
        match self {
            Self::CompileTime(b) => {
                let arrays = b.finish();
                let schema = typed_arrow::schema::<R>();
                typed_arrow::into_record_batch::<R>(name, schema, arrays)
            }
            Self::Runtime(b) => b.finish_into_batch(name),
        }
    }
}
```

### Dynamic Builders and Factory
```rust
pub trait DynColumnBuilder {
    fn data_type(&self) -> &arrow_schema::DataType;
    fn append_null(&mut self);
    fn append_dyn(&mut self, v: DynCell) -> Result<(), AppendError>;
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
            .map(|f| new_dyn_builder(f.data_type(), f.is_nullable()))
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
        arrow_array::RecordBatch::try_new(schema, arrays).expect("shape verified")
    }
}
```

## What Unifies vs. What Doesn’t
- Unifies well:
  - Schema: `schema_ref()` returns `SchemaRef` from both variants.
  - Ingestion: one builders handle that finishes to `RecordBatch`.
  - Dynamic column iteration: visit index/name/`DataType`/nullable.
  - Downcast: `try_as_static`/`try_bind` when runtime schema matches `R` by shape.
- Does not unify:
  - Typed column-generic kernels (`ForEachCol::for_each_col::<V>()`) need compile-time `T`; cannot run over the dynamic variant. Provide dynamic visitors for name/`DataType` if needed.

## Column Iteration
```rust
pub trait DynColumnVisitor {
    fn visit(&mut self, i: usize, name: &str, dt: &arrow_schema::DataType, nullable: bool);
}

impl<R: ForEachCol> StaticFacade<R> {
    pub fn for_each_typed<V: ColumnVisitor>(&self) { R::for_each_col::<V>(); }
}

impl DynSchema {
    pub fn for_each_dyn<V: DynColumnVisitor>(&self, v: &mut V) {
        for (i, f) in self.schema.fields().iter().enumerate() {
            v.visit(i, f.name(), f.data_type(), f.is_nullable());
        }
    }
}
```

## Examples
### Static-only
```rust
#[derive(Record)]
struct Person { id: i64, email: Option<String> }

let rec = UnifiedRecord::<Person>::compile_time();
let mut b = rec.builders(2);
b.append_unified_row(UnifiedRow::Static(Some(Person { id: 1, email: None })));
b.append_null_row();
let batch = b.finish_into_batch(Some("people"));
```

### Dynamic-only
```rust
let schema: arrow_schema::SchemaRef = load_schema_somehow();
let rec: UnifiedRecord = UnifiedRecord::runtime(schema.clone()); // DynMarker default
let mut b = rec.builders(1);
b.append_unified_row(UnifiedRow::Dynamic(Some(dyn_row_from_values(&schema))));
let batch = b.finish_into_batch(Some("people"));
```

### Downcast dynamic to static (if shapes match)
```rust
let dyn_rec: UnifiedRecord = UnifiedRecord::runtime(schema.clone());
if let Ok(static_rec) = dyn_rec.try_bind::<Person>() {
    static_rec.with_static(|s| s.for_each_typed::<DebugColumns>());
}
```

## Validation and Errors
- `RowVariantMismatch`: returned when a `UnifiedBuilders::CompileTime` receives a `UnifiedRow::Dynamic` (or vice versa).
- Shape mismatch (`DynMismatch`): returned by `try_bind`/`try_as_static` when the runtime `Schema` is not identical to the static `R` schema (use deterministic equality).
- Dynamic builder factory is the only place with a `DataType` switch. It must reject unsupported types with precise errors.

## Rollout Plan
- Phase A0: Scaffold crates `typed-arrow-dyn` and `typed-arrow-unified`. Add `unified` feature to `typed-arrow` that re-exports the unified API.
- Phase A: Introduce `DynMarker`, `UnifiedRecord`, `UnifiedBuilders`, `UnifiedRow`, and `DynSchema` types (no external behavior change). Add docs and an example that builds a `RecordBatch` from both static and dynamic paths.
- Phase B: Implement `DynColumnBuilder` + factory for primitives, Utf8, Binary, and Struct. Add `RecordBatch` assembly.
- Phase C: Extend dynamic support to List/LargeList/FixedSizeList/Map and Timestamp/Decimal wrappers via the same `ArrowBinding` mapping.
- Phase D: Optional Dictionary and Union dynamic builders.

## Open Questions
- Dynamic row representation (`DynRow`/`DynCell`): JSON-like shape, or a typed enum of supported scalars/containers? Favor a thin enum tree mirroring Arrow logical types for zero-copy where possible.
- Schema equality: use strict field-order and `keys_sorted`/timezone metadata equality (`equals_deterministic`).
- Timezone strategy: start with `TimestampTz<_, Utc>`; revisit general TZs later.
- Feature gating across arrow-rs versions: guard dynamic builder impls behind crate features to track API differences.

## Notes
- This facade does not replace the typed path. It surfaces shared operations and keeps typed kernels fast and monomorphized when `R` is known at compile time.
- Keep the dynamic layer small and focused on ingestion and bridging.
