//! Arrow `Null` type binding.

#[cfg(feature = "views")]
use arrow_array::Array;
use arrow_array::{NullArray, builder::NullBuilder};
use arrow_schema::DataType;

use super::ArrowBinding;
#[cfg(feature = "views")]
use super::ArrowBindingView;

/// Marker type for Arrow `DataType::Null` columns.
///
/// A `Null` column contains only nulls. Appending a value or a null both append
/// a null slot. This maps to `arrow_array::NullArray` and uses `NullBuilder`.
#[derive(Debug, Clone)]
pub struct Null;

impl ArrowBinding for Null {
    type Builder = NullBuilder;
    type Array = NullArray;
    const NULLABLE: bool = true;
    fn data_type() -> DataType {
        DataType::Null
    }
    fn new_builder(_capacity: usize) -> Self::Builder {
        NullBuilder::new()
    }
    fn append_value(b: &mut Self::Builder, _v: &Self) {
        b.append_null();
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

#[cfg(feature = "views")]
impl ArrowBindingView for Null {
    type Array = NullArray;
    type View<'a> = Null;

    fn get_view(
        array: &Self::Array,
        index: usize,
    ) -> Result<Self::View<'_>, crate::schema::ViewAccessError> {
        if index >= array.len() {
            return Err(crate::schema::ViewAccessError::OutOfBounds {
                index,
                len: array.len(),
                field_name: None,
            });
        }
        // NullArray has no non-null values; treat the marker as the value.
        Ok(Null)
    }
}
