//! Arrow `Null` type binding.

use arrow_array::{builder::NullBuilder, NullArray};
use arrow_schema::DataType;

use super::ArrowBinding;

/// Marker type for Arrow `DataType::Null` columns.
///
/// A `Null` column contains only nulls. Appending a value or a null both append
/// a null slot. This maps to `arrow_array::NullArray` and uses `NullBuilder`.
pub struct Null;

impl ArrowBinding for Null {
    type Builder = NullBuilder;
    type Array = NullArray;
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
