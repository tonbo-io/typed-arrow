//! Binary family bindings (Binary, `LargeBinary`, `FixedSizeBinary`).

use crate::arrow_array::{
    FixedSizeBinaryArray, LargeBinaryArray,
    builder::{BinaryBuilder, FixedSizeBinaryBuilder, LargeBinaryBuilder},
};
use crate::arrow_schema::DataType;

use super::ArrowBinding;
#[cfg(feature = "views")]
use crate::arrow_array::Array;
#[cfg(feature = "views")]
use super::ArrowBindingView;

// Binary / Vec<u8>
impl ArrowBinding for Vec<u8> {
    type Builder = BinaryBuilder;
    type Array = crate::arrow_array::BinaryArray;
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

#[cfg(feature = "views")]
impl ArrowBindingView for Vec<u8> {
    type Array = crate::arrow_array::BinaryArray;
    type View<'a> = &'a [u8];

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
        Ok(array.value(index))
    }
}

// FixedSizeBinary: [u8; N]
impl<const N: usize> super::ArrowBinding for [u8; N] {
    type Builder = FixedSizeBinaryBuilder;
    type Array = FixedSizeBinaryArray;
    fn data_type() -> DataType {
        DataType::FixedSizeBinary(i32::try_from(N).expect("width fits i32"))
    }
    fn new_builder(capacity: usize) -> Self::Builder {
        FixedSizeBinaryBuilder::with_capacity(capacity, i32::try_from(N).expect("width fits i32"))
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

#[cfg(feature = "views")]
impl<const N: usize> super::ArrowBindingView for [u8; N] {
    type Array = FixedSizeBinaryArray;
    type View<'a> = &'a [u8];

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
        Ok(array.value(index))
    }
}

/// Wrapper denoting Arrow `LargeBinary` values. Use when individual binary values
/// can exceed 2GB or when 64-bit offsets are preferred.
pub struct LargeBinary(Vec<u8>);

impl LargeBinary {
    /// Construct a new `LargeBinary` from the given bytes.
    #[inline]
    #[must_use]
    pub fn new(value: Vec<u8>) -> Self {
        Self(value)
    }
    /// Return the underlying bytes as a slice.
    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }
    /// Consume and return the underlying byte vector.
    #[inline]
    #[must_use]
    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

impl From<Vec<u8>> for LargeBinary {
    #[inline]
    fn from(value: Vec<u8>) -> Self {
        Self::new(value)
    }
}

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

#[cfg(feature = "views")]
impl ArrowBindingView for LargeBinary {
    type Array = LargeBinaryArray;
    type View<'a> = &'a [u8];

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
        Ok(array.value(index))
    }
}
