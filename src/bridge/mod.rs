//! Bridge from Rust types to Arrow typed arrays and `DataType`.
//!
//! This module provides a compile-time mapping from Rust value types to
//! arrow-rs typed builders/arrays and their corresponding `arrow_schema::DataType`,
//! avoiding any runtime `DataType` matching.
//!
//! - Core trait: [`ArrowBinding`] (Rust type → typed builder/array + `DataType`).
//! - Primitives: `i{8,16,32,64}`, `u{8,16,32,64}`, `f{32,64}`, `bool`.
//! - Utf8/Binary: `String` → `Utf8`, `Vec<u8>` → `Binary`.
//! - Nested containers:
//!   - [`List<T>`] with non-null items, and [`List<Option<T>>`] for nullable items.
//!   - [`Dictionary<K, String>`] → dictionary-encoded Utf8 values.
//!   - [`Timestamp<U>`] with unit markers ([`Second`], [`Millisecond`], [`Microsecond`],
//!     [`Nanosecond`]) and [`TimestampTz<U, Z>`] for timezone-aware timestamps.
//!   - Any `T: Record + StructMeta` binds to an Arrow `StructArray`.
//!
//! See tests for end-to-end examples and usage patterns.

use arrow_array::Array;
use arrow_schema::DataType;

/// Binding from a Rust type to Arrow typed builders/arrays and `DataType`.
///
/// Implementations of this trait provide a zero-cost, monomorphized mapping
/// between a Rust value type and its Arrow representation.
pub trait ArrowBinding {
    /// Concrete Arrow builder type used for this Rust type.
    type Builder;

    /// Concrete Arrow array type produced by `finish`.
    type Array: Array;

    /// The Arrow `DataType` corresponding to this Rust type.
    fn data_type() -> DataType;

    /// Create a new builder with an optional capacity hint.
    fn new_builder(capacity: usize) -> Self::Builder;

    /// Append a non-null value to the builder.
    fn append_value(b: &mut Self::Builder, v: &Self);

    /// Append a null to the builder.
    fn append_null(b: &mut Self::Builder);

    /// Finish the builder and produce a typed Arrow array.
    fn finish(b: Self::Builder) -> Self::Array;
}

/// View binding from an Arrow array to borrowed Rust reference types.
///
/// Implementations of this trait provide zero-copy access to Arrow array values
/// by returning borrowed references with a lifetime tied to the array.
///
/// Note: This trait is separate from `ArrowBinding` to allow types to opt-in
/// to view support. Complex types like `List`, `Map`, etc. may not implement this.
#[cfg(feature = "views")]
pub trait ArrowBindingView {
    /// The Arrow array type this view reads from.
    type Array: Array;

    /// The borrowed view type returned when accessing array elements.
    /// For example: `&'a str` for `StringArray`, `i64` for `Int64Array`.
    type View<'a>
    where
        Self: 'a;

    /// Extract a view at the given index from the array.
    ///
    /// # Errors
    /// - Returns `ViewAccessError::OutOfBounds` if index >= array.len()
    /// - Returns `ViewAccessError::UnexpectedNull` if the value is null
    /// - Returns `ViewAccessError::TypeMismatch` if array downcast fails
    fn get_view(
        array: &Self::Array,
        index: usize,
    ) -> Result<Self::View<'_>, crate::schema::ViewAccessError>;

    /// Check if the value at the given index is null.
    fn is_null(array: &Self::Array, index: usize) -> bool;
}

mod binary;
mod column;
mod decimals;
mod dictionary;
mod intervals;
mod lists;
mod map;
mod null_type;
mod primitives;
mod record_struct;
mod strings;
mod temporal;

// Public re-exports for convenience
pub use binary::LargeBinary;
pub use column::{data_type_of, ColumnBuilder};
pub use decimals::{Decimal128, Decimal256};
pub use dictionary::{DictKey, Dictionary};
pub use intervals::{IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth};
pub use lists::{FixedSizeList, FixedSizeListNullable, LargeList, List};
pub use map::{Map, OrderedMap};
pub use null_type::Null;
pub use strings::LargeUtf8;
pub use temporal::{
    Date32, Date64, Duration, Microsecond, Millisecond, Nanosecond, Second, Time32, Time64,
    TimeZoneSpec, Timestamp, TimestampTz, Utc,
};
