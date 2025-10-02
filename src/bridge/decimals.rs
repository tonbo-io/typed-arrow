//! Decimal128/Decimal256 bindings.

use arrow_array::{
    builder::{Decimal128Builder, Decimal256Builder},
    Array, Decimal128Array, Decimal256Array,
};
use arrow_buffer::i256;
use arrow_schema::DataType;

use super::ArrowBinding;
#[cfg(feature = "views")]
use super::ArrowBindingView;

/// Fixed-precision decimal stored in 128 bits.
/// The value is represented as a scaled integer of type `i128`.
pub struct Decimal128<const P: u8, const S: i8>(i128);
impl<const P: u8, const S: i8> Decimal128<P, S> {
    /// Construct a new `Decimal128<P,S>` from a scaled integer value.
    #[inline]
    #[must_use]
    pub fn new(value: i128) -> Self {
        Self(value)
    }
    /// Return the scaled integer value.
    #[inline]
    #[must_use]
    pub fn value(&self) -> i128 {
        self.0
    }
    /// Consume and return the scaled integer value.
    #[inline]
    #[must_use]
    pub fn into_value(self) -> i128 {
        self.0
    }
}

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

#[cfg(feature = "views")]
impl<const P: u8, const S: i8> ArrowBindingView for Decimal128<P, S> {
    type Array = Decimal128Array;
    type View<'a> = Decimal128<P, S>;

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
        if array.is_null(index) {
            return Err(crate::schema::ViewAccessError::UnexpectedNull {
                index,
                field_name: None,
            });
        }
        Ok(Decimal128::new(array.value(index)))
    }

    fn is_null(array: &Self::Array, index: usize) -> bool {
        array.is_null(index)
    }
}

/// Fixed-precision decimal stored in 256 bits.
/// The value is represented as a scaled integer of type `i256`.
pub struct Decimal256<const P: u8, const S: i8>(i256);
impl<const P: u8, const S: i8> Decimal256<P, S> {
    /// Construct a new `Decimal256<P,S>` from a scaled integer value.
    #[inline]
    #[must_use]
    pub fn new(value: i256) -> Self {
        Self(value)
    }
    /// Return the scaled integer value.
    #[inline]
    #[must_use]
    pub fn value(&self) -> i256 {
        self.0
    }
    /// Consume and return the scaled integer value.
    #[inline]
    #[must_use]
    pub fn into_value(self) -> i256 {
        self.0
    }
}

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

#[cfg(feature = "views")]
impl<const P: u8, const S: i8> ArrowBindingView for Decimal256<P, S> {
    type Array = Decimal256Array;
    type View<'a> = Decimal256<P, S>;

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
        if array.is_null(index) {
            return Err(crate::schema::ViewAccessError::UnexpectedNull {
                index,
                field_name: None,
            });
        }
        Ok(Decimal256::new(array.value(index)))
    }

    fn is_null(array: &Self::Array, index: usize) -> bool {
        array.is_null(index)
    }
}
