//! Binary family bindings (Binary, LargeBinary, FixedSizeBinary).

use arrow_array::{builder::*, FixedSizeBinaryArray, LargeBinaryArray};
use arrow_schema::DataType;

use super::ArrowBinding;

// Binary / Vec<u8>
impl ArrowBinding for Vec<u8> {
    type Builder = BinaryBuilder;
    type Array = arrow_array::BinaryArray;
    fn data_type() -> DataType {
        DataType::Binary
    }
    fn new_builder(capacity: usize) -> Self::Builder {
        BinaryBuilder::with_capacity(capacity, 0)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        b.append_value(v.as_slice());
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// FixedSizeBinary: [u8; N]
impl<const N: usize> super::ArrowBinding for [u8; N] {
    type Builder = FixedSizeBinaryBuilder;
    type Array = FixedSizeBinaryArray;
    fn data_type() -> DataType {
        DataType::FixedSizeBinary(N as i32)
    }
    fn new_builder(capacity: usize) -> Self::Builder {
        FixedSizeBinaryBuilder::with_capacity(capacity, N as i32)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        let _ = b.append_value(v);
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

/// Wrapper denoting Arrow `LargeBinary` values. Use when individual binary values
/// can exceed 2GB or when 64-bit offsets are preferred.
pub struct LargeBinary(pub Vec<u8>);

impl ArrowBinding for LargeBinary {
    type Builder = LargeBinaryBuilder;
    type Array = LargeBinaryArray;
    fn data_type() -> DataType {
        DataType::LargeBinary
    }
    fn new_builder(capacity: usize) -> Self::Builder {
        LargeBinaryBuilder::with_capacity(capacity, 0)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        b.append_value(v.0.as_slice());
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}
