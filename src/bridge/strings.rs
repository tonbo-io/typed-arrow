//! Utf8 and LargeUtf8 string bindings.

use arrow_array::{builder::*, LargeStringArray, StringArray};
use arrow_schema::DataType;

use super::ArrowBinding;

// Utf8/String
impl ArrowBinding for String {
    type Builder = StringBuilder;
    type Array = StringArray;
    fn data_type() -> DataType {
        DataType::Utf8
    }
    fn new_builder(capacity: usize) -> Self::Builder {
        StringBuilder::with_capacity(capacity, 0)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        b.append_value(v.as_str());
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

/// Wrapper denoting Arrow `LargeUtf8` values. Use when individual strings can be
/// extremely large or when 64-bit offsets are preferred.
pub struct LargeUtf8(pub String);

impl ArrowBinding for LargeUtf8 {
    type Builder = LargeStringBuilder;
    type Array = LargeStringArray;
    fn data_type() -> DataType {
        DataType::LargeUtf8
    }
    fn new_builder(capacity: usize) -> Self::Builder {
        LargeStringBuilder::with_capacity(capacity, 0)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        b.append_value(v.0.as_str());
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}
