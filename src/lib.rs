#![deny(missing_docs)]
//! Compile-time Arrow schema definition using Rust types.
//!
//! `typed-arrow` maps Rust structs directly to Arrow schemas, builders, and arrays
//! without runtime `DataType` switching. This enables zero-cost, monomorphized
//! column construction with compile-time type safety.
//!
//! # Quick Start
//!
//! ```
//! use typed_arrow::prelude::*;
//!
//! #[derive(Record)]
//! struct Person {
//!     id: i64,
//!     name: String,
//!     score: Option<f64>,
//! }
//!
//! // Build arrays from rows
//! let rows = vec![
//!     Person {
//!         id: 1,
//!         name: "Alice".into(),
//!         score: Some(95.5),
//!     },
//!     Person {
//!         id: 2,
//!         name: "Bob".into(),
//!         score: None,
//!     },
//! ];
//!
//! let mut builders = <Person as BuildRows>::new_builders(rows.len());
//! builders.append_rows(rows);
//! let batch = builders.finish().into_record_batch();
//!
//! assert_eq!(batch.num_rows(), 2);
//! assert_eq!(batch.num_columns(), 3);
//! ```
//!
//! # Cargo Features
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `derive` | ✓ | Enables [`#[derive(Record)]`](Record) and [`#[derive(Union)]`](Union) macros |
//! | `views` | ✓ | Zero-copy views for reading [`RecordBatch`](arrow_array::RecordBatch) data |
//! | `ext-hooks` | | Extensibility hooks for custom derive behavior |
//! | `arrow-55` | | Use Arrow 55.x crates |
//! | `arrow-56` | | Use Arrow 56.x crates |
//! | `arrow-57` | ✓ | Use Arrow 57.x crates |
//!
//! Exactly one Arrow feature must be enabled.
//!
//! # Derive Macros
//!
//! ## `#[derive(Record)]`
//!
//! Generates Arrow schema traits for structs. See [`schema::Record`] for the marker trait.
//!
//! ```
//! use typed_arrow::prelude::*;
//!
//! #[derive(Record)]
//! struct Event {
//!     id: i64,              // Non-null Int64
//!     name: Option<String>, // Nullable Utf8
//!     #[record(name = "eventType")] // Override Arrow field name
//!     event_type: String,
//! }
//! ```
//!
//! **Field attributes:**
//! - `#[record(name = "...")]` — Override the Arrow field name
//! - `#[arrow(nullable)]` — Force nullability even without `Option<T>`
//! - `#[metadata(k = "key", v = "value")]` — Add field-level metadata
//! - `#[schema_metadata(k = "key", v = "value")]` — Add schema-level metadata (on struct)
//!
//! ## `#[derive(Union)]`
//!
//! Generates Arrow Union type bindings for enums. Implements
//! [`ArrowBinding`](bridge::ArrowBinding).
//!
//! ```
//! use typed_arrow::prelude::*;
//!
//! #[derive(Union)]
//! #[union(mode = "dense")] // or "sparse"
//! enum Value {
//!     #[union(tag = 0)]
//!     Int(i32),
//!     #[union(tag = 1, field = "text")]
//!     Str(String),
//! }
//! ```
//!
//! **Container attributes:**
//! - `#[union(mode = "dense"|"sparse")]` — Union mode (default: dense)
//! - `#[union(null_variant = "None")]` — Designate a null-carrier variant
//! - `#[union(tags(A = 0, B = 1))]` — Set all variant tags at once
//!
//! **Variant attributes:**
//! - `#[union(tag = N)]` — Set type ID for this variant
//! - `#[union(field = "name")]` — Override Arrow field name
//! - `#[union(null)]` — Mark as the null-carrier variant
//!
//! # Core Traits
//!
//! ## Schema Traits (in [`schema`] module)
//!
//! | Trait | Description |
//! |-------|-------------|
//! | [`Record`](schema::Record) | Marker for structs with `const LEN: usize` columns |
//! | [`ColAt<I>`](schema::ColAt) | Per-column metadata: `Native`, `ColumnArray`, `ColumnBuilder`, `NULLABLE`, `NAME`, `data_type()` |
//! | [`ForEachCol`](schema::ForEachCol) | Compile-time column iteration via [`ColumnVisitor`](schema::ColumnVisitor) |
//! | [`SchemaMeta`](schema::SchemaMeta) | Runtime schema access: `fields()`, `schema()`, `metadata()` |
//! | [`StructMeta`](schema::StructMeta) | Nested struct support: `child_fields()`, `new_struct_builder()` |
//!
//! ## Row Building Traits (in [`schema`] module)
//!
//! | Trait | Description |
//! |-------|-------------|
//! | [`BuildRows`](schema::BuildRows) | Entry point: `new_builders(capacity)` → `Builders` |
//! | [`RowBuilder<T>`](schema::RowBuilder) | `append_row()`, `append_rows()`, `append_option_row()`, `finish()` |
//! | [`IntoRecordBatch`](schema::IntoRecordBatch) | Convert finished arrays to [`RecordBatch`](arrow_array::RecordBatch) |
//! | [`AppendStruct`](schema::AppendStruct) | Append struct fields into a `StructBuilder` |
//!
//! ## Type Binding Trait (in [`bridge`] module)
//!
//! | Trait | Description |
//! |-------|-------------|
//! | [`ArrowBinding`](bridge::ArrowBinding) | Maps Rust types to Arrow: `Builder`, `Array`, `data_type()`, `append_value()`, `finish()` |
//!
//! # Supported Types
//!
//! ## Primitives
//!
//! | Rust Type | Arrow Type |
//! |-----------|------------|
//! | `i8`, `i16`, `i32`, `i64` | `Int8`, `Int16`, `Int32`, `Int64` |
//! | `u8`, `u16`, `u32`, `u64` | `UInt8`, `UInt16`, `UInt32`, `UInt64` |
//! | `f32`, `f64` | `Float32`, `Float64` |
//! | [`half::f16`] | `Float16` |
//! | `bool` | `Boolean` |
//!
//! ## Strings & Binary
//!
//! | Rust Type | Arrow Type |
//! |-----------|------------|
//! | `String` | `Utf8` |
//! | [`LargeUtf8`] | `LargeUtf8` (64-bit offsets) |
//! | `Vec<u8>` | `Binary` |
//! | [`LargeBinary`] | `LargeBinary` (64-bit offsets) |
//! | `[u8; N]` | `FixedSizeBinary(N)` |
//!
//! ## Nullability
//!
//! | Rust Type | Arrow Nullability |
//! |-----------|-------------------|
//! | `T` | Non-nullable column |
//! | `Option<T>` | Nullable column |
//! | [`Null`] | `Null` type (always null) |
//!
//! ## Temporal Types
//!
//! | Rust Type | Arrow Type |
//! |-----------|------------|
//! | [`Date32`] | `Date32` (days since epoch) |
//! | [`Date64`] | `Date64` (milliseconds since epoch) |
//! | [`Time32<U>`](Time32) | `Time32` with unit `U` ([`Second`], [`Millisecond`]) |
//! | [`Time64<U>`](Time64) | `Time64` with unit `U` ([`Microsecond`], [`Nanosecond`]) |
//! | [`Timestamp<U>`] | `Timestamp` without timezone |
//! | [`TimestampTz<U, Z>`] | `Timestamp` with timezone `Z` (e.g., [`Utc`]) |
//! | [`Duration<U>`](Duration) | `Duration` with unit `U` |
//!
//! ## Intervals
//!
//! | Rust Type | Arrow Type |
//! |-----------|------------|
//! | [`IntervalYearMonth`] | `Interval(YearMonth)` |
//! | [`IntervalDayTime`] | `Interval(DayTime)` |
//! | [`IntervalMonthDayNano`] | `Interval(MonthDayNano)` |
//!
//! ## Decimal
//!
//! | Rust Type | Arrow Type |
//! |-----------|------------|
//! | [`Decimal128<P, S>`](Decimal128) | `Decimal128(P, S)` |
//! | [`Decimal256<P, S>`](Decimal256) | `Decimal256(P, S)` |
//!
//! ## Nested Types
//!
//! | Rust Type | Arrow Type |
//! |-----------|------------|
//! | `#[derive(Record)]` struct | `Struct` |
//! | [`List<T>`] | `List` (non-null items) |
//! | [`List<Option<T>>`](List) | `List` (nullable items) |
//! | [`LargeList<T>`](LargeList) | `LargeList` (64-bit offsets) |
//! | [`FixedSizeList<T, N>`](FixedSizeList) | `FixedSizeList(N)` (non-null items) |
//! | [`FixedSizeListNullable<T, N>`](FixedSizeListNullable) | `FixedSizeList(N)` (nullable items) |
//! | [`Map<K, V>`] | `Map` (non-null values) |
//! | [`Map<K, Option<V>>`](Map) | `Map` (nullable values) |
//! | [`OrderedMap<K, V>`] | `Map` with `keys_sorted = true` |
//! | [`Dictionary<K, V>`] | `Dictionary` (K: integral, V: string/binary/primitive) |
//! | `#[derive(Union)]` enum | `Union` (Dense or Sparse) |
//!
//! # Zero-Copy Views (requires `views` feature)
//!
//! Read [`RecordBatch`](arrow_array::RecordBatch) data without allocation.
//! Use [`AsViewsIterator::iter_views`] to iterate over borrowed row views,
//! and [`.try_into()`](TryInto::try_into) to convert views to owned records.
//!
//! See the [`schema`] module for detailed documentation and examples.
//!
//! # Extensibility (requires `ext-hooks` feature)
//!
//! Customize derive behavior with hooks:
//!
//! ```ignore
//! #[derive(Record)]
//! #[record(visit(MyVisitor))]                    // Inject compile-time visitor
//! #[record(field_macro = my_ext::per_field)]     // Call macro per field
//! #[record(record_macro = my_ext::per_record)]   // Call macro per record
//! struct MyRecord {
//!     #[record(ext(custom_tag))]                 // Tag fields with markers
//!     field: i32,
//! }
//! ```
//!
//! See `examples/12_ext_hooks.rs` for usage.

#[cfg(all(
    feature = "arrow-55",
    any(feature = "arrow-56", feature = "arrow-57")
))]
compile_error!("Select exactly one Arrow feature: arrow-55, arrow-56, or arrow-57.");
#[cfg(all(feature = "arrow-56", feature = "arrow-57"))]
compile_error!("Select exactly one Arrow feature: arrow-55, arrow-56, or arrow-57.");
#[cfg(not(any(feature = "arrow-55", feature = "arrow-56", feature = "arrow-57")))]
compile_error!("Enable one Arrow feature: arrow-55, arrow-56, or arrow-57.");

#[cfg(feature = "arrow-55")]
pub use arrow_array_55 as arrow_array;
#[cfg(feature = "arrow-56")]
pub use arrow_array_56 as arrow_array;
#[cfg(feature = "arrow-57")]
pub use arrow_array_57 as arrow_array;

#[cfg(feature = "arrow-55")]
pub use arrow_buffer_55 as arrow_buffer;
#[cfg(feature = "arrow-56")]
pub use arrow_buffer_56 as arrow_buffer;
#[cfg(feature = "arrow-57")]
pub use arrow_buffer_57 as arrow_buffer;

#[cfg(feature = "arrow-55")]
pub use arrow_data_55 as arrow_data;
#[cfg(feature = "arrow-56")]
pub use arrow_data_56 as arrow_data;
#[cfg(feature = "arrow-57")]
pub use arrow_data_57 as arrow_data;

#[cfg(feature = "arrow-55")]
pub use arrow_schema_55 as arrow_schema;
#[cfg(feature = "arrow-56")]
pub use arrow_schema_56 as arrow_schema;
#[cfg(feature = "arrow-57")]
pub use arrow_schema_57 as arrow_schema;

pub mod bridge;
pub mod error;
pub mod schema;

/// Prelude exporting the most common traits and markers.
pub mod prelude {
    // Re-export derive macros when enabled
    #[cfg(feature = "derive")]
    pub use typed_arrow_derive::{Record, Union};

    #[cfg(feature = "views")]
    pub use crate::AsViewsIterator;
    #[cfg(feature = "views")]
    pub use crate::error::ViewAccessError;
    #[cfg(feature = "views")]
    pub use crate::schema::{FromRecordBatch, ViewResultIteratorExt};
    pub use crate::{
        error::SchemaError,
        schema::{BuildRows, ColAt, ColumnVisitor, FieldMeta, ForEachCol, Record},
    };
}

// Re-export the derive macro when enabled
// Re-export Arrow crates so derives can reference a stable path
// and downstream users don't need to depend on Arrow directly.
#[cfg(feature = "derive")]
pub use typed_arrow_derive::{Record, Union};

// Public re-exports for convenience
pub use crate::bridge::{
    Date32, Date64, Decimal128, Decimal256, Dictionary, Duration, FixedSizeList,
    FixedSizeListNullable, IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth, LargeBinary,
    LargeList, LargeUtf8, List, Map, Microsecond, Millisecond, Nanosecond, Null, OrderedMap,
    Second, Time32, Time64, TimeZoneSpec, Timestamp, TimestampTz, Utc,
};

/// Extension trait for creating typed view iterators from `RecordBatch`.
#[cfg(feature = "views")]
pub trait AsViewsIterator {
    /// Iterate over typed views of rows in this RecordBatch.
    ///
    /// This provides zero-copy access to the data as borrowed references.
    ///
    /// # Errors
    /// Returns `SchemaError` if the RecordBatch schema doesn't match the expected Record type.
    ///
    /// # Example
    /// ```
    /// use typed_arrow::prelude::*;
    ///
    /// #[derive(Record)]
    /// struct Row {
    ///     id: i32,
    ///     name: String,
    /// }
    ///
    /// // Build a RecordBatch
    /// let rows = vec![
    ///     Row {
    ///         id: 1,
    ///         name: "Alice".to_string(),
    ///     },
    ///     Row {
    ///         id: 2,
    ///         name: "Bob".to_string(),
    ///     },
    /// ];
    /// let mut b = <Row as BuildRows>::new_builders(rows.len());
    /// b.append_rows(rows);
    /// let arrays = b.finish();
    /// let batch = arrays.into_record_batch();
    ///
    /// // Iterate with zero-copy views (using convenience method to handle errors)
    /// let views = batch.iter_views::<Row>()?.try_flatten()?;
    /// for row in views {
    ///     println!("{}: {}", row.id, row.name);
    /// }
    /// # Ok::<_, typed_arrow::error::SchemaError>(())
    /// ```
    fn iter_views<T: schema::FromRecordBatch>(&self) -> Result<T::Views<'_>, error::SchemaError>;
}

#[cfg(feature = "views")]
impl AsViewsIterator for crate::arrow_array::RecordBatch {
    fn iter_views<T: schema::FromRecordBatch>(&self) -> Result<T::Views<'_>, error::SchemaError> {
        T::from_record_batch(self)
    }
}
