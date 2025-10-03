#![deny(missing_docs)]
//! typed-arrow core: compile-time Arrow schema traits and primitive markers.

pub mod bridge;
pub mod error;
pub mod schema;

/// Prelude exporting the most common traits and markers.
pub mod prelude {
    #[cfg(feature = "views")]
    pub use crate::error::ViewAccessError;
    #[cfg(feature = "views")]
    pub use crate::schema::{FromRecordBatch, ViewResultIteratorExt};
    #[cfg(feature = "views")]
    pub use crate::AsViewsIterator;
    pub use crate::{
        error::SchemaError,
        schema::{BuildRows, ColAt, ColumnVisitor, FieldMeta, ForEachCol, Record},
    };
}

// Re-export the derive macro when enabled
// Re-export Arrow crates so derives can reference a stable path
// and downstream users don't need to depend on Arrow directly.
pub use arrow_array;
pub use arrow_buffer;
pub use arrow_schema;
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
    /// #[derive(typed_arrow::Record)]
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
impl AsViewsIterator for arrow_array::RecordBatch {
    fn iter_views<T: schema::FromRecordBatch>(&self) -> Result<T::Views<'_>, error::SchemaError> {
        T::from_record_batch(self)
    }
}
