//! Decimal128/Decimal256 bindings.

use arrow_array::{
    builder::{Decimal128Builder, Decimal256Builder},
    Decimal128Array, Decimal256Array,
};
use arrow_buffer::i256;
use arrow_schema::DataType;

use super::ArrowBinding;

/// Fixed-precision decimal stored in 128 bits.
/// The value is represented as a scaled integer of type `i128`.
pub struct Decimal128<const P: u8, const S: i8>(pub i128);

impl<const P: u8, const S: i8> ArrowBinding for Decimal128<P, S> {
    type Builder = Decimal128Builder;
    type Array = Decimal128Array;

    fn data_type() -> DataType {
        DataType::Decimal128(P, S)
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        Decimal128Builder::with_capacity(capacity).with_data_type(DataType::Decimal128(P, S))
    }

    fn append_value(b: &mut Self::Builder, v: &Self) {
        b.append_value(v.0);
    }

    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }

    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

/// Fixed-precision decimal stored in 256 bits.
/// The value is represented as a scaled integer of type `i256`.
pub struct Decimal256<const P: u8, const S: i8>(pub i256);

impl<const P: u8, const S: i8> ArrowBinding for Decimal256<P, S> {
    type Builder = Decimal256Builder;
    type Array = Decimal256Array;

    fn data_type() -> DataType {
        DataType::Decimal256(P, S)
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        Decimal256Builder::with_capacity(capacity).with_data_type(DataType::Decimal256(P, S))
    }

    fn append_value(b: &mut Self::Builder, v: &Self) {
        b.append_value(v.0);
    }

    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }

    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}
