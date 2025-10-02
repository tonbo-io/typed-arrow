//! Core schema traits for compile-time Arrow typing.

use std::{
    collections::HashMap, iter::IntoIterator, marker::PhantomData, option::Option, sync::Arc,
};

use arrow_array::{
    builder::{ArrayBuilder, StructBuilder},
    Array, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema};

/// Error type for schema validation failures when creating views from RecordBatch.
#[derive(Debug, Clone)]
pub struct SchemaError {
    /// Human-readable error message
    pub message: String,
}

impl SchemaError {
    /// Create a new schema error
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Schema validation error: {}", self.message)
    }
}

impl std::error::Error for SchemaError {}

/// Error type for view access failures when reading from Arrow arrays.
#[cfg(feature = "views")]
#[derive(Debug, Clone)]
pub enum ViewAccessError {
    /// Index out of bounds
    OutOfBounds {
        /// The invalid index
        index: usize,
        /// The array length
        len: usize,
        /// Optional field name for context
        field_name: Option<&'static str>,
    },
    /// Unexpected null value
    UnexpectedNull {
        /// The index where null was found
        index: usize,
        /// Optional field name for context
        field_name: Option<&'static str>,
    },
    /// Type mismatch during array downcast
    TypeMismatch {
        /// Expected type name
        expected: String,
        /// Actual data type
        actual: String,
        /// Optional field name for context
        field_name: Option<&'static str>,
    },
}

#[cfg(feature = "views")]
impl std::fmt::Display for ViewAccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutOfBounds {
                index,
                len,
                field_name,
            } => {
                write!(f, "index {index} out of bounds (len {len})")?;
                if let Some(name) = field_name {
                    write!(f, " for field '{name}'")?;
                }
                Ok(())
            }
            Self::UnexpectedNull { index, field_name } => {
                write!(f, "unexpected null at index {index}")?;
                if let Some(name) = field_name {
                    write!(f, " for field '{name}'")?;
                }
                Ok(())
            }
            Self::TypeMismatch {
                expected,
                actual,
                field_name,
            } => {
                write!(f, "type mismatch: expected {expected}, got {actual}")?;
                if let Some(name) = field_name {
                    write!(f, " for field '{name}'")?;
                }
                Ok(())
            }
        }
    }
}

#[cfg(feature = "views")]
impl std::error::Error for ViewAccessError {}

#[cfg(feature = "views")]
impl From<ViewAccessError> for SchemaError {
    fn from(err: ViewAccessError) -> Self {
        SchemaError::new(format!("View access error: {}", err))
    }
}

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

    /// Arrow-rs `DataType` for this column.
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
    #[must_use]
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
    /// Top-level fields: (name, `data_type`, nullable) represented as `Field`s.
    fn fields() -> Vec<Field>;

    /// Optional top-level schema key/value metadata.
    #[must_use]
    fn metadata() -> HashMap<String, String> {
        HashMap::default()
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

/// Trait for creating zero-copy views over a RecordBatch.
///
/// Implemented automatically by `#[derive(Record)]` to generate a view struct
/// (`{Name}View<'a>`) and an iterator (`{Name}Views<'a>`) that provide borrowed
/// access to RecordBatch rows without copying data.
#[cfg(feature = "views")]
pub trait FromRecordBatch: Record + Sized {
    /// The view type representing a single row with borrowed references.
    type View<'a>;

    /// The iterator type yielding Result-wrapped views over all rows.
    ///
    /// Each item is a `Result<View, ViewAccessError>` to handle potential errors
    /// during view access (e.g., type mismatches, unexpected nulls, out of bounds).
    type Views<'a>: Iterator<Item = Result<Self::View<'a>, ViewAccessError>>;

    /// Create an iterator of views over the RecordBatch rows.
    ///
    /// # Errors
    /// Returns `SchemaError` if the RecordBatch schema doesn't match this Record's schema.
    /// This includes mismatched column names, types, or field counts.
    fn from_record_batch(batch: &RecordBatch) -> Result<Self::Views<'_>, SchemaError>;
}

/// Extension trait providing convenience methods for iterators over `Result<T, ViewAccessError>`.
///
/// This trait is automatically implemented for any iterator yielding `Result<T, ViewAccessError>`,
/// such as the iterators returned by [`FromRecordBatch::from_record_batch`].
#[cfg(feature = "views")]
pub trait ViewResultIteratorExt: Iterator + Sized {
    /// The success type of the Result items.
    type Item;

    /// Flatten the Result iterator, returning all views or the first error.
    ///
    /// This consumes the iterator and returns a `Result` containing either:
    /// - `Ok(Vec<T>)` with all successfully accessed views
    /// - `Err(ViewAccessError)` with the first error encountered
    ///
    /// # Errors
    /// Returns the first `ViewAccessError` encountered while iterating.
    ///
    /// # Example
    /// ```
    /// use typed_arrow::prelude::*;
    ///
    /// #[derive(typed_arrow::Record)]
    /// struct Row {
    ///     id: i32,
    ///     name: String,
    /// }
    ///
    /// # let rows = vec![Row { id: 1, name: "Alice".into() }];
    /// # let mut b = <Row as BuildRows>::new_builders(1);
    /// # b.append_rows(rows);
    /// # let batch = b.finish().into_record_batch();
    /// // Returns all views or first error
    /// let views = batch.iter_views::<Row>()?.try_flatten()?;
    /// for row in views {
    ///     println!("{}: {}", row.id, row.name);
    /// }
    /// # Ok::<_, typed_arrow::schema::SchemaError>(())
    /// ```
    fn try_flatten(self) -> Result<Vec<<Self as ViewResultIteratorExt>::Item>, ViewAccessError>
    where
        Result<Vec<<Self as ViewResultIteratorExt>::Item>, ViewAccessError>:
            std::iter::FromIterator<<Self as Iterator>::Item>,
    {
        self.collect()
    }
}

#[cfg(feature = "views")]
impl<I, T> ViewResultIteratorExt for I
where
    I: Iterator<Item = Result<T, ViewAccessError>>,
{
    type Item = T;
}

/// Trait for creating a view from a StructArray at a specific index.
///
/// This is automatically implemented by `#[derive(Record)]` and used internally
/// to support nested struct views.
#[cfg(feature = "views")]
pub trait StructView: Record + Sized {
    /// The view type for this struct with borrowed references.
    type View<'a>;

    /// Extract a view at the given index from a StructArray.
    ///
    /// # Errors
    /// Returns `ViewAccessError` if the index is out of bounds, the value is null when expected to
    /// be non-null, or if there's a type mismatch during field extraction.
    fn view_at(
        array: &arrow_array::StructArray,
        index: usize,
    ) -> Result<Self::View<'_>, ViewAccessError>;

    /// Check if the struct value at the given index is null.
    fn is_null_at(array: &arrow_array::StructArray, index: usize) -> bool;
}
