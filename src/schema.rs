//! Core schema traits for compile-time Arrow typing.

use std::{
    collections::HashMap, iter::IntoIterator, marker::PhantomData, option::Option, sync::Arc,
};

use arrow_array::{
    builder::{ArrayBuilder, StructBuilder},
    Array, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema};

/// A record (row) with a fixed, compile-time number of columns.
pub trait Record {
    /// Number of columns in this record.
    const LEN: usize;
}

/// Per-column metadata for a record at index `I`.
pub trait ColAt<const I: usize>: Record {
    /// The Native value type of this column (without nullability).
    type Native;

    /// The typed Arrow array for this column.
    type ColumnArray: Array;

    /// The typed Arrow builder for this column.
    type ColumnBuilder: ArrayBuilder;

    /// Whether this column is nullable.
    const NULLABLE: bool;

    /// Column name.
    const NAME: &'static str;

    /// Arrow-rs DataType for this column.
    fn data_type() -> DataType;
}

/// Simple compile-time column metadata passed to visitors.
pub struct FieldMeta<R> {
    /// Column name.
    pub name: &'static str,

    /// Whether this column is nullable.
    pub nullable: bool,

    _phantom: PhantomData<R>,
}

impl<R> FieldMeta<R> {
    /// Construct a new `FieldMeta`.
    pub const fn new(name: &'static str, nullable: bool) -> Self {
        Self {
            name,
            nullable,
            _phantom: PhantomData,
        }
    }
}

/// A visitor invoked at compile time for each column of a `Record`.
pub trait ColumnVisitor {
    /// Process a column at index `I` with Rust type `R`.
    fn visit<const I: usize, R>(_m: FieldMeta<R>);
}

/// Trait emitted by derive/macro to enable `for_each_col` expansion.
pub trait ForEachCol: Record {
    /// Invoke `V::visit` for each column at compile time.
    fn for_each_col<V: ColumnVisitor>();
}

// No Arrow markers: ColAt exposes DATA_TYPE/ColumnBuilder/ColumnArray

/// Metadata and builder utilities for nested Struct fields.
pub trait StructMeta: Record {
    /// Child fields (names, data types, nullability) for this struct.
    fn child_fields() -> Vec<Field>;

    /// Construct a `StructBuilder` with appropriate child builders for this struct.
    fn new_struct_builder(capacity: usize) -> StructBuilder;
}

/// Arrow runtime schema metadata for a top-level Record.
pub trait SchemaMeta: Record {
    /// Top-level fields: (name, data_type, nullable) represented as `Field`s.
    fn fields() -> Vec<Field>;

    /// Optional top-level schema key/value metadata.
    fn metadata() -> HashMap<String, String> {
        Default::default()
    }

    /// Construct an `Arc<arrow_schema::Schema>` from `fields()`.
    fn schema() -> Arc<Schema> {
        let fields: Vec<Arc<Field>> = Self::fields().into_iter().map(Arc::new).collect();
        Arc::new(Schema::new_with_metadata(fields, Self::metadata()))
    }
}

/// Row-based building interface: construct typed column builders, append owned rows,
/// and finish into typed arrays.
pub trait BuildRows: Record + Sized {
    /// Generated builders struct for this record.
    type Builders: RowBuilder<Self>;

    /// Generated arrays struct for this record.
    type Arrays: IntoRecordBatch;

    /// Create builders with a capacity hint.
    fn new_builders(capacity: usize) -> Self::Builders;
}

/// Trait implemented by derive-generated builders to append rows of `Row`
/// and finish into a typed arrays struct.
pub trait RowBuilder<Row> {
    /// The arrays struct produced by `finish`.
    type Arrays: IntoRecordBatch;

    /// Append a non-null row.
    fn append_row(&mut self, row: Row);
    /// Append a null row.
    fn append_null_row(&mut self);
    /// Append an optional row.
    fn append_option_row(&mut self, row: Option<Row>);
    /// Append an iterator of non-null rows.
    fn append_rows<I: IntoIterator<Item = Row>>(&mut self, rows: I);
    /// Append an iterator of optional rows.
    fn append_option_rows<I: IntoIterator<Item = Option<Row>>>(&mut self, rows: I);
    /// Finish and produce arrays.
    fn finish(self) -> Self::Arrays;
}

/// Trait implemented by derive-generated arrays to assemble a `RecordBatch`.
pub trait IntoRecordBatch {
    /// Assemble and return an `arrow_array::RecordBatch`.
    fn into_record_batch(self) -> RecordBatch;
}

// Identity conversion for dynamic path output (RecordBatch already assembled).
impl IntoRecordBatch for RecordBatch {
    fn into_record_batch(self) -> RecordBatch {
        self
    }
}

/// Trait implemented by `#[derive(Record)]` structs to append their fields into a
/// `StructBuilder`. Used by row-based APIs to handle nested struct fields.
pub trait AppendStruct {
    /// Append this struct's child values into the provided `StructBuilder`.
    /// Caller is responsible for setting the parent validity via `append(true)`.
    fn append_owned_into(self, b: &mut StructBuilder);

    /// Append nulls for each child into the provided `StructBuilder` to align lengths.
    /// Caller is responsible for `append(false)` for the parent validity.
    fn append_null_into(b: &mut StructBuilder);
}

/// Trait implemented by `#[derive(Record)]` structs to append their fields into a
/// `StructBuilder` from a borrowed reference. This enables container builders (e.g.,
/// lists of structs) to append child values without taking ownership of the struct.
pub trait AppendStructRef {
    /// Append this struct's child values into the provided `StructBuilder` using borrows.
    /// Caller is responsible for setting the parent validity via `append(true)`.
    fn append_borrowed_into(&self, b: &mut StructBuilder);
}
