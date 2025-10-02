//! Primitive Arrow bindings (integers, floats, bool, f16).

use arrow_array::{
    builder::PrimitiveBuilder,
    types::{
        Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
    Array, PrimitiveArray,
};
use arrow_schema::DataType;
use half::f16;

use super::ArrowBinding;
#[cfg(feature = "views")]
use super::ArrowBindingView;

// Primitive integers/floats
macro_rules! impl_primitive_binding {
    ($rust:ty, $atype:ty, $dt:expr) => {
        impl ArrowBinding for $rust {
            type Builder = PrimitiveBuilder<$atype>;
            type Array = PrimitiveArray<$atype>;
            fn data_type() -> DataType {
                $dt
            }
            fn new_builder(capacity: usize) -> Self::Builder {
                PrimitiveBuilder::<$atype>::with_capacity(capacity)
            }
            fn append_value(b: &mut Self::Builder, v: &Self) {
                b.append_value(*v);
            }
            fn append_null(b: &mut Self::Builder) {
                b.append_null();
            }
            fn finish(mut b: Self::Builder) -> Self::Array {
                b.finish()
            }
        }

        #[cfg(feature = "views")]
        impl ArrowBindingView for $rust {
            type Array = PrimitiveArray<$atype>;
            type View<'a> = $rust;

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

            fn is_null(array: &Self::Array, index: usize) -> bool {
                array.is_null(index)
            }
        }
    };
}

impl_primitive_binding!(i8, Int8Type, DataType::Int8);
impl_primitive_binding!(i16, Int16Type, DataType::Int16);
impl_primitive_binding!(i32, Int32Type, DataType::Int32);
impl_primitive_binding!(i64, Int64Type, DataType::Int64);
impl_primitive_binding!(u8, UInt8Type, DataType::UInt8);
impl_primitive_binding!(u16, UInt16Type, DataType::UInt16);
impl_primitive_binding!(u32, UInt32Type, DataType::UInt32);
impl_primitive_binding!(u64, UInt64Type, DataType::UInt64);
impl_primitive_binding!(f32, Float32Type, DataType::Float32);
impl_primitive_binding!(f64, Float64Type, DataType::Float64);

// Float16 (half-precision)
impl ArrowBinding for f16 {
    type Builder = PrimitiveBuilder<Float16Type>;
    type Array = PrimitiveArray<Float16Type>;
    fn data_type() -> DataType {
        DataType::Float16
    }
    fn new_builder(capacity: usize) -> Self::Builder {
        PrimitiveBuilder::<Float16Type>::with_capacity(capacity)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        b.append_value(*v);
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

#[cfg(feature = "views")]
impl ArrowBindingView for f16 {
    type Array = PrimitiveArray<Float16Type>;
    type View<'a> = f16;

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

    fn is_null(array: &Self::Array, index: usize) -> bool {
        array.is_null(index)
    }
}

// Boolean
impl ArrowBinding for bool {
    type Builder = arrow_array::builder::BooleanBuilder;
    type Array = arrow_array::BooleanArray;
    fn data_type() -> DataType {
        DataType::Boolean
    }
    fn new_builder(capacity: usize) -> Self::Builder {
        arrow_array::builder::BooleanBuilder::with_capacity(capacity)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        b.append_value(*v);
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

#[cfg(feature = "views")]
impl ArrowBindingView for bool {
    type Array = arrow_array::BooleanArray;
    type View<'a> = bool;

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

    fn is_null(array: &Self::Array, index: usize) -> bool {
        array.is_null(index)
    }
}
