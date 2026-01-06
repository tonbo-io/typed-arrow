//! Factory for dynamic builders, mapping Arrow `DataType` to concrete builders.

use crate::arrow_array as arrow_array;
use crate::arrow_schema as arrow_schema;

use std::sync::Arc;

use arrow_array::{ArrayRef, builder as b, types as t};
use arrow_schema::{DataType, UnionFields, UnionMode};

use crate::{
    DynError,
    cell::DynCell,
    dyn_builder::{DynColumnBuilder, FinishedColumn},
    nested::{FixedSizeListCol, LargeListCol, ListCol, MapCol, StructCol},
    union::{DenseUnionCol, SparseUnionCol},
};

// All concrete builder variants wrapped under a single enum used by the factory.
enum Inner {
    Null(b::NullBuilder),
    Bool(b::BooleanBuilder),
    I8(b::PrimitiveBuilder<t::Int8Type>),
    I16(b::PrimitiveBuilder<t::Int16Type>),
    I32(b::PrimitiveBuilder<t::Int32Type>),
    I64(b::PrimitiveBuilder<t::Int64Type>),
    U8(b::PrimitiveBuilder<t::UInt8Type>),
    U16(b::PrimitiveBuilder<t::UInt16Type>),
    U32(b::PrimitiveBuilder<t::UInt32Type>),
    U64(b::PrimitiveBuilder<t::UInt64Type>),
    F32(b::PrimitiveBuilder<t::Float32Type>),
    F64(b::PrimitiveBuilder<t::Float64Type>),
    // Fixed-size binary
    FixedSizeBinary(b::FixedSizeBinaryBuilder),
    // Dates
    Date32(b::PrimitiveBuilder<t::Date32Type>),
    Date64(b::PrimitiveBuilder<t::Date64Type>),
    // Time32/Time64
    Time32Second(b::PrimitiveBuilder<t::Time32SecondType>),
    Time32Millisecond(b::PrimitiveBuilder<t::Time32MillisecondType>),
    Time64Microsecond(b::PrimitiveBuilder<t::Time64MicrosecondType>),
    Time64Nanosecond(b::PrimitiveBuilder<t::Time64NanosecondType>),
    // Duration
    DurationSecond(b::PrimitiveBuilder<t::DurationSecondType>),
    DurationMillisecond(b::PrimitiveBuilder<t::DurationMillisecondType>),
    DurationMicrosecond(b::PrimitiveBuilder<t::DurationMicrosecondType>),
    DurationNanosecond(b::PrimitiveBuilder<t::DurationNanosecondType>),
    // Timestamps (tz captured in DataType only)
    TimestampSecond(b::PrimitiveBuilder<t::TimestampSecondType>),
    TimestampMillisecond(b::PrimitiveBuilder<t::TimestampMillisecondType>),
    TimestampMicrosecond(b::PrimitiveBuilder<t::TimestampMicrosecondType>),
    TimestampNanosecond(b::PrimitiveBuilder<t::TimestampNanosecondType>),
    Utf8(b::StringBuilder),
    LargeUtf8(b::LargeStringBuilder),
    Binary(b::BinaryBuilder),
    LargeBinary(b::LargeBinaryBuilder),
    // Dictionary (Utf8/LargeUtf8 and Binary/LargeBinary/FixedSizeBinary)
    DictUtf8I8(b::StringDictionaryBuilder<t::Int8Type>),
    DictUtf8I16(b::StringDictionaryBuilder<t::Int16Type>),
    DictUtf8I32(b::StringDictionaryBuilder<t::Int32Type>),
    DictUtf8I64(b::StringDictionaryBuilder<t::Int64Type>),
    DictUtf8U8(b::StringDictionaryBuilder<t::UInt8Type>),
    DictUtf8U16(b::StringDictionaryBuilder<t::UInt16Type>),
    DictUtf8U32(b::StringDictionaryBuilder<t::UInt32Type>),
    DictUtf8U64(b::StringDictionaryBuilder<t::UInt64Type>),
    DictLargeUtf8I8(b::LargeStringDictionaryBuilder<t::Int8Type>),
    DictLargeUtf8I16(b::LargeStringDictionaryBuilder<t::Int16Type>),
    DictLargeUtf8I32(b::LargeStringDictionaryBuilder<t::Int32Type>),
    DictLargeUtf8I64(b::LargeStringDictionaryBuilder<t::Int64Type>),
    DictLargeUtf8U8(b::LargeStringDictionaryBuilder<t::UInt8Type>),
    DictLargeUtf8U16(b::LargeStringDictionaryBuilder<t::UInt16Type>),
    DictLargeUtf8U32(b::LargeStringDictionaryBuilder<t::UInt32Type>),
    DictLargeUtf8U64(b::LargeStringDictionaryBuilder<t::UInt64Type>),
    DictBinaryI8(b::BinaryDictionaryBuilder<t::Int8Type>),
    DictBinaryI16(b::BinaryDictionaryBuilder<t::Int16Type>),
    DictBinaryI32(b::BinaryDictionaryBuilder<t::Int32Type>),
    DictBinaryI64(b::BinaryDictionaryBuilder<t::Int64Type>),
    DictBinaryU8(b::BinaryDictionaryBuilder<t::UInt8Type>),
    DictBinaryU16(b::BinaryDictionaryBuilder<t::UInt16Type>),
    DictBinaryU32(b::BinaryDictionaryBuilder<t::UInt32Type>),
    DictBinaryU64(b::BinaryDictionaryBuilder<t::UInt64Type>),
    DictLargeBinaryI8(b::LargeBinaryDictionaryBuilder<t::Int8Type>),
    DictLargeBinaryI16(b::LargeBinaryDictionaryBuilder<t::Int16Type>),
    DictLargeBinaryI32(b::LargeBinaryDictionaryBuilder<t::Int32Type>),
    DictLargeBinaryI64(b::LargeBinaryDictionaryBuilder<t::Int64Type>),
    DictLargeBinaryU8(b::LargeBinaryDictionaryBuilder<t::UInt8Type>),
    DictLargeBinaryU16(b::LargeBinaryDictionaryBuilder<t::UInt16Type>),
    DictLargeBinaryU32(b::LargeBinaryDictionaryBuilder<t::UInt32Type>),
    DictLargeBinaryU64(b::LargeBinaryDictionaryBuilder<t::UInt64Type>),
    DictFixedSizeBinaryI8(b::FixedSizeBinaryDictionaryBuilder<t::Int8Type>),
    DictFixedSizeBinaryI16(b::FixedSizeBinaryDictionaryBuilder<t::Int16Type>),
    DictFixedSizeBinaryI32(b::FixedSizeBinaryDictionaryBuilder<t::Int32Type>),
    DictFixedSizeBinaryI64(b::FixedSizeBinaryDictionaryBuilder<t::Int64Type>),
    DictFixedSizeBinaryU8(b::FixedSizeBinaryDictionaryBuilder<t::UInt8Type>),
    DictFixedSizeBinaryU16(b::FixedSizeBinaryDictionaryBuilder<t::UInt16Type>),
    DictFixedSizeBinaryU32(b::FixedSizeBinaryDictionaryBuilder<t::UInt32Type>),
    DictFixedSizeBinaryU64(b::FixedSizeBinaryDictionaryBuilder<t::UInt64Type>),
    // Nested
    Struct(StructCol),
    List(ListCol),
    LargeList(LargeListCol),
    FixedSizeList(FixedSizeListCol),
    Map(MapCol),
    // Primitive dictionary via trait object
    DictPrimitive(Box<dyn DictPrimBuilder>),
    UnionDense(DenseUnionCol),
    UnionSparse(SparseUnionCol),
}

// Minimal trait object to handle primitive dictionary builders without exploding the enum.
trait DictPrimBuilder: Send {
    fn append_cell(&mut self, v: DynCell) -> Result<(), DynError>;
    fn append_null(&mut self);
    fn finish(&mut self) -> ArrayRef;
}

struct DictPrimImpl<K, V>
where
    K: arrow_array::types::ArrowDictionaryKeyType,
    V: arrow_array::types::ArrowPrimitiveType,
{
    b: b::PrimitiveDictionaryBuilder<K, V>,
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> DictPrimImpl<K, V>
where
    K: arrow_array::types::ArrowDictionaryKeyType,
    V: arrow_array::types::ArrowPrimitiveType,
{
    fn new() -> Self {
        Self {
            b: b::PrimitiveDictionaryBuilder::<K, V>::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

macro_rules! impl_dict_prim_builder {
    ($name:ident, $cell_pat:pat, $val:expr) => {
        impl<K> DictPrimBuilder for DictPrimImpl<K, t::$name>
        where
            K: arrow_array::types::ArrowDictionaryKeyType + Send,
        {
            fn append_cell(&mut self, v: DynCell) -> Result<(), DynError> {
                match v {
                    $cell_pat => {
                        let _ = self.b.append($val);
                        Ok(())
                    }
                    _other => Err(DynError::Builder {
                        message: format!(
                            "type mismatch for primitive dict value: expected {:?}",
                            <t::$name as arrow_array::types::ArrowPrimitiveType>::DATA_TYPE
                        ),
                    }),
                }
            }
            fn append_null(&mut self) {
                self.b.append_null();
            }
            fn finish(&mut self) -> ArrayRef {
                Arc::new(self.b.finish())
            }
        }
    };
}

impl_dict_prim_builder!(Int8Type, DynCell::I8(x), x);
impl_dict_prim_builder!(Int16Type, DynCell::I16(x), x);
impl_dict_prim_builder!(Int32Type, DynCell::I32(x), x);
impl_dict_prim_builder!(Int64Type, DynCell::I64(x), x);
impl_dict_prim_builder!(UInt8Type, DynCell::U8(x), x);
impl_dict_prim_builder!(UInt16Type, DynCell::U16(x), x);
impl_dict_prim_builder!(UInt32Type, DynCell::U32(x), x);
impl_dict_prim_builder!(UInt64Type, DynCell::U64(x), x);
impl_dict_prim_builder!(Float32Type, DynCell::F32(x), x);
impl_dict_prim_builder!(Float64Type, DynCell::F64(x), x);

// The trait-object wrapper for a dynamic builder.
struct Col {
    dt: DataType,
    inner: Inner,
}

impl DynColumnBuilder for Col {
    fn data_type(&self) -> &DataType {
        &self.dt
    }

    fn append_null(&mut self) {
        match &mut self.inner {
            Inner::Null(b) => b.append_null(),
            Inner::Bool(b) => b.append_null(),
            Inner::I8(b) => b.append_null(),
            Inner::I16(b) => b.append_null(),
            Inner::I32(b) => b.append_null(),
            Inner::I64(b) => b.append_null(),
            Inner::U8(b) => b.append_null(),
            Inner::U16(b) => b.append_null(),
            Inner::U32(b) => b.append_null(),
            Inner::U64(b) => b.append_null(),
            Inner::F32(b) => b.append_null(),
            Inner::F64(b) => b.append_null(),
            Inner::FixedSizeBinary(b) => b.append_null(),
            Inner::Date32(b) => b.append_null(),
            Inner::Date64(b) => b.append_null(),
            Inner::Time32Second(b) => b.append_null(),
            Inner::Time32Millisecond(b) => b.append_null(),
            Inner::Time64Microsecond(b) => b.append_null(),
            Inner::Time64Nanosecond(b) => b.append_null(),
            Inner::DurationSecond(b) => b.append_null(),
            Inner::DurationMillisecond(b) => b.append_null(),
            Inner::DurationMicrosecond(b) => b.append_null(),
            Inner::DurationNanosecond(b) => b.append_null(),
            Inner::TimestampSecond(b) => b.append_null(),
            Inner::TimestampMillisecond(b) => b.append_null(),
            Inner::TimestampMicrosecond(b) => b.append_null(),
            Inner::TimestampNanosecond(b) => b.append_null(),
            Inner::Utf8(b) => b.append_null(),
            Inner::LargeUtf8(b) => b.append_null(),
            Inner::Binary(b) => b.append_null(),
            Inner::LargeBinary(b) => b.append_null(),
            Inner::DictUtf8I8(b) => b.append_null(),
            Inner::DictUtf8I16(b) => b.append_null(),
            Inner::DictUtf8I32(b) => b.append_null(),
            Inner::DictUtf8I64(b) => b.append_null(),
            Inner::DictUtf8U8(b) => b.append_null(),
            Inner::DictUtf8U16(b) => b.append_null(),
            Inner::DictUtf8U32(b) => b.append_null(),
            Inner::DictUtf8U64(b) => b.append_null(),
            Inner::DictLargeUtf8I8(b) => b.append_null(),
            Inner::DictLargeUtf8I16(b) => b.append_null(),
            Inner::DictLargeUtf8I32(b) => b.append_null(),
            Inner::DictLargeUtf8I64(b) => b.append_null(),
            Inner::DictLargeUtf8U8(b) => b.append_null(),
            Inner::DictLargeUtf8U16(b) => b.append_null(),
            Inner::DictLargeUtf8U32(b) => b.append_null(),
            Inner::DictLargeUtf8U64(b) => b.append_null(),
            Inner::DictBinaryI8(b) => b.append_null(),
            Inner::DictBinaryI16(b) => b.append_null(),
            Inner::DictBinaryI32(b) => b.append_null(),
            Inner::DictBinaryI64(b) => b.append_null(),
            Inner::DictBinaryU8(b) => b.append_null(),
            Inner::DictBinaryU16(b) => b.append_null(),
            Inner::DictBinaryU32(b) => b.append_null(),
            Inner::DictBinaryU64(b) => b.append_null(),
            Inner::DictLargeBinaryI8(b) => b.append_null(),
            Inner::DictLargeBinaryI16(b) => b.append_null(),
            Inner::DictLargeBinaryI32(b) => b.append_null(),
            Inner::DictLargeBinaryI64(b) => b.append_null(),
            Inner::DictLargeBinaryU8(b) => b.append_null(),
            Inner::DictLargeBinaryU16(b) => b.append_null(),
            Inner::DictLargeBinaryU32(b) => b.append_null(),
            Inner::DictLargeBinaryU64(b) => b.append_null(),
            Inner::DictFixedSizeBinaryI8(b) => b.append_null(),
            Inner::DictFixedSizeBinaryI16(b) => b.append_null(),
            Inner::DictFixedSizeBinaryI32(b) => b.append_null(),
            Inner::DictFixedSizeBinaryI64(b) => b.append_null(),
            Inner::DictFixedSizeBinaryU8(b) => b.append_null(),
            Inner::DictFixedSizeBinaryU16(b) => b.append_null(),
            Inner::DictFixedSizeBinaryU32(b) => b.append_null(),
            Inner::DictFixedSizeBinaryU64(b) => b.append_null(),
            Inner::Struct(b) => b.append_null(),
            Inner::List(b) => b.append_null(),
            Inner::LargeList(b) => b.append_null(),
            Inner::FixedSizeList(b) => b.append_null(),
            Inner::Map(b) => b.append_null(),
            Inner::DictPrimitive(b) => b.append_null(),
            Inner::UnionDense(b) => b.append_null(),
            Inner::UnionSparse(b) => b.append_null(),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn append_dyn(&mut self, v: DynCell) -> Result<(), DynError> {
        match (&mut self.inner, v) {
            (Inner::Null(b), DynCell::Null) => {
                b.append_null();
                Ok(())
            }
            (Inner::Bool(b), DynCell::Bool(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::I8(b), DynCell::I8(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::I16(b), DynCell::I16(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::I32(b), DynCell::I32(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::I64(b), DynCell::I64(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::U8(b), DynCell::U8(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::U16(b), DynCell::U16(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::U32(b), DynCell::U32(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::U64(b), DynCell::U64(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::F32(b), DynCell::F32(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::F64(b), DynCell::F64(x)) => {
                b.append_value(x);
                Ok(())
            }
            // FixedSizeBinary
            (Inner::FixedSizeBinary(b), DynCell::Bin(bs)) => {
                b.append_value(bs.as_slice())
                    .map_err(|e| DynError::Builder {
                        message: e.to_string(),
                    })?;
                Ok(())
            }
            // Dates
            (Inner::Date32(b), DynCell::I32(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::Date64(b), DynCell::I64(x)) => {
                b.append_value(x);
                Ok(())
            }
            // Time32/Time64
            (Inner::Time32Second(b), DynCell::I32(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::Time32Millisecond(b), DynCell::I32(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::Time64Microsecond(b), DynCell::I64(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::Time64Nanosecond(b), DynCell::I64(x)) => {
                b.append_value(x);
                Ok(())
            }
            // Duration
            (Inner::DurationSecond(b), DynCell::I64(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::DurationMillisecond(b), DynCell::I64(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::DurationMicrosecond(b), DynCell::I64(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::DurationNanosecond(b), DynCell::I64(x)) => {
                b.append_value(x);
                Ok(())
            }
            // Timestamp
            (Inner::TimestampSecond(b), DynCell::I64(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::TimestampMillisecond(b), DynCell::I64(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::TimestampMicrosecond(b), DynCell::I64(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::TimestampNanosecond(b), DynCell::I64(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Inner::Utf8(b), DynCell::Str(s)) => {
                b.append_value(s.as_str());
                Ok(())
            }
            (Inner::LargeUtf8(b), DynCell::Str(s)) => {
                b.append_value(s.as_str());
                Ok(())
            }
            (Inner::Binary(b), DynCell::Bin(bs)) => {
                b.append_value(bs.as_slice());
                Ok(())
            }
            (Inner::LargeBinary(b), DynCell::Bin(bs)) => {
                b.append_value(bs.as_slice());
                Ok(())
            }
            // Dictionary (Utf8 values)
            (Inner::DictUtf8I8(b), DynCell::Str(s)) => {
                b.append(s.as_str()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictUtf8I16(b), DynCell::Str(s)) => {
                b.append(s.as_str()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictUtf8I32(b), DynCell::Str(s)) => {
                b.append(s.as_str()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictUtf8I64(b), DynCell::Str(s)) => {
                b.append(s.as_str()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictUtf8U8(b), DynCell::Str(s)) => {
                b.append(s.as_str()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictUtf8U16(b), DynCell::Str(s)) => {
                b.append(s.as_str()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictUtf8U32(b), DynCell::Str(s)) => {
                b.append(s.as_str()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictUtf8U64(b), DynCell::Str(s)) => {
                b.append(s.as_str()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            // Dictionary (LargeUtf8 values)
            (Inner::DictLargeUtf8I8(b), DynCell::Str(s)) => {
                b.append(s.as_str()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictLargeUtf8I16(b), DynCell::Str(s)) => {
                b.append(s.as_str()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictLargeUtf8I32(b), DynCell::Str(s)) => {
                b.append(s.as_str()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictLargeUtf8I64(b), DynCell::Str(s)) => {
                b.append(s.as_str()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictLargeUtf8U8(b), DynCell::Str(s)) => {
                b.append(s.as_str()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictLargeUtf8U16(b), DynCell::Str(s)) => {
                b.append(s.as_str()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictLargeUtf8U32(b), DynCell::Str(s)) => {
                b.append(s.as_str()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictLargeUtf8U64(b), DynCell::Str(s)) => {
                b.append(s.as_str()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            // Dictionary (Binary values)
            (Inner::DictBinaryI8(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictBinaryI16(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictBinaryI32(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictBinaryI64(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictBinaryU8(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictBinaryU16(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictBinaryU32(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictBinaryU64(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictLargeBinaryI8(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictLargeBinaryI16(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictLargeBinaryI32(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictLargeBinaryI64(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictLargeBinaryU8(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictLargeBinaryU16(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictLargeBinaryU32(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictLargeBinaryU64(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictFixedSizeBinaryI8(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictFixedSizeBinaryI16(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictFixedSizeBinaryI32(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictFixedSizeBinaryI64(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictFixedSizeBinaryU8(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictFixedSizeBinaryU16(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictFixedSizeBinaryU32(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            (Inner::DictFixedSizeBinaryU64(b), DynCell::Bin(bs)) => {
                b.append(bs.as_slice()).map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })?;
                Ok(())
            }
            // Primitive dictionary values
            (Inner::DictPrimitive(b), other) => b.append_cell(other),
            // Nested
            (Inner::Struct(b), DynCell::Struct(values)) => b.append_struct(values),
            (Inner::List(b), DynCell::List(values)) => b.append_list(values),
            (Inner::LargeList(b), DynCell::List(values)) => b.append_list(values),
            (Inner::FixedSizeList(b), DynCell::FixedSizeList(values)) => b.append_fixed(values),
            (Inner::Map(b), DynCell::Map(entries)) => b.append_map(entries),
            (Inner::UnionDense(b), DynCell::Union { type_id, value }) => {
                b.append_union(type_id, value)?;
                Ok(())
            }
            (Inner::UnionSparse(b), DynCell::Union { type_id, value }) => {
                b.append_union(type_id, value)?;
                Ok(())
            }
            (_inner, DynCell::Null) => {
                self.append_null();
                Ok(())
            }
            (_inner, _other) => Err(DynError::Builder {
                message: format!("type mismatch for {:?}", self.dt),
            }),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match &mut self.inner {
            Inner::Null(b) => Arc::new(b.finish()),
            Inner::Bool(b) => Arc::new(b.finish()),
            Inner::I8(b) => Arc::new(b.finish()),
            Inner::I16(b) => Arc::new(b.finish()),
            Inner::I32(b) => Arc::new(b.finish()),
            Inner::I64(b) => Arc::new(b.finish()),
            Inner::U8(b) => Arc::new(b.finish()),
            Inner::U16(b) => Arc::new(b.finish()),
            Inner::U32(b) => Arc::new(b.finish()),
            Inner::U64(b) => Arc::new(b.finish()),
            Inner::F32(b) => Arc::new(b.finish()),
            Inner::F64(b) => Arc::new(b.finish()),
            Inner::FixedSizeBinary(b) => Arc::new(b.finish()),
            Inner::Date32(b) => Arc::new(b.finish()),
            Inner::Date64(b) => Arc::new(b.finish()),
            Inner::Time32Second(b) => Arc::new(b.finish()),
            Inner::Time32Millisecond(b) => Arc::new(b.finish()),
            Inner::Time64Microsecond(b) => Arc::new(b.finish()),
            Inner::Time64Nanosecond(b) => Arc::new(b.finish()),
            Inner::DurationSecond(b) => Arc::new(b.finish()),
            Inner::DurationMillisecond(b) => Arc::new(b.finish()),
            Inner::DurationMicrosecond(b) => Arc::new(b.finish()),
            Inner::DurationNanosecond(b) => Arc::new(b.finish()),
            Inner::TimestampSecond(b) => Arc::new(b.finish()),
            Inner::TimestampMillisecond(b) => Arc::new(b.finish()),
            Inner::TimestampMicrosecond(b) => Arc::new(b.finish()),
            Inner::TimestampNanosecond(b) => Arc::new(b.finish()),
            Inner::Utf8(b) => Arc::new(b.finish()),
            Inner::LargeUtf8(b) => Arc::new(b.finish()),
            Inner::Binary(b) => Arc::new(b.finish()),
            Inner::LargeBinary(b) => Arc::new(b.finish()),
            Inner::DictUtf8I8(b) => Arc::new(b.finish()),
            Inner::DictUtf8I16(b) => Arc::new(b.finish()),
            Inner::DictUtf8I32(b) => Arc::new(b.finish()),
            Inner::DictUtf8I64(b) => Arc::new(b.finish()),
            Inner::DictUtf8U8(b) => Arc::new(b.finish()),
            Inner::DictUtf8U16(b) => Arc::new(b.finish()),
            Inner::DictUtf8U32(b) => Arc::new(b.finish()),
            Inner::DictUtf8U64(b) => Arc::new(b.finish()),
            Inner::DictLargeUtf8I8(b) => Arc::new(b.finish()),
            Inner::DictLargeUtf8I16(b) => Arc::new(b.finish()),
            Inner::DictLargeUtf8I32(b) => Arc::new(b.finish()),
            Inner::DictLargeUtf8I64(b) => Arc::new(b.finish()),
            Inner::DictLargeUtf8U8(b) => Arc::new(b.finish()),
            Inner::DictLargeUtf8U16(b) => Arc::new(b.finish()),
            Inner::DictLargeUtf8U32(b) => Arc::new(b.finish()),
            Inner::DictLargeUtf8U64(b) => Arc::new(b.finish()),
            Inner::DictBinaryI8(b) => Arc::new(b.finish()),
            Inner::DictBinaryI16(b) => Arc::new(b.finish()),
            Inner::DictBinaryI32(b) => Arc::new(b.finish()),
            Inner::DictBinaryI64(b) => Arc::new(b.finish()),
            Inner::DictBinaryU8(b) => Arc::new(b.finish()),
            Inner::DictBinaryU16(b) => Arc::new(b.finish()),
            Inner::DictBinaryU32(b) => Arc::new(b.finish()),
            Inner::DictBinaryU64(b) => Arc::new(b.finish()),
            Inner::DictLargeBinaryI8(b) => Arc::new(b.finish()),
            Inner::DictLargeBinaryI16(b) => Arc::new(b.finish()),
            Inner::DictLargeBinaryI32(b) => Arc::new(b.finish()),
            Inner::DictLargeBinaryI64(b) => Arc::new(b.finish()),
            Inner::DictLargeBinaryU8(b) => Arc::new(b.finish()),
            Inner::DictLargeBinaryU16(b) => Arc::new(b.finish()),
            Inner::DictLargeBinaryU32(b) => Arc::new(b.finish()),
            Inner::DictLargeBinaryU64(b) => Arc::new(b.finish()),
            Inner::DictFixedSizeBinaryI8(b) => Arc::new(b.finish()),
            Inner::DictFixedSizeBinaryI16(b) => Arc::new(b.finish()),
            Inner::DictFixedSizeBinaryI32(b) => Arc::new(b.finish()),
            Inner::DictFixedSizeBinaryI64(b) => Arc::new(b.finish()),
            Inner::DictFixedSizeBinaryU8(b) => Arc::new(b.finish()),
            Inner::DictFixedSizeBinaryU16(b) => Arc::new(b.finish()),
            Inner::DictFixedSizeBinaryU32(b) => Arc::new(b.finish()),
            Inner::DictFixedSizeBinaryU64(b) => Arc::new(b.finish()),
            Inner::Struct(b) => Arc::new(b.finish()),
            Inner::List(b) => Arc::new(b.finish()),
            Inner::LargeList(b) => Arc::new(b.finish()),
            Inner::FixedSizeList(b) => Arc::new(b.finish()),
            Inner::Map(b) => Arc::new(b.finish()),
            Inner::DictPrimitive(b) => b.finish(),
            Inner::UnionDense(b) => b.finish_array(),
            Inner::UnionSparse(b) => b.finish_array(),
        }
    }

    fn try_finish(&mut self) -> Result<FinishedColumn, DynError> {
        match &mut self.inner {
            Inner::Null(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::Bool(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::I8(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::I16(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::I32(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::I64(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::U8(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::U16(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::U32(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::U64(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::F32(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::F64(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::FixedSizeBinary(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::Date32(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::Date64(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::Time32Second(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::Time32Millisecond(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::Time64Microsecond(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::Time64Nanosecond(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DurationSecond(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DurationMillisecond(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DurationMicrosecond(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DurationNanosecond(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::TimestampSecond(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::TimestampMillisecond(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::TimestampMicrosecond(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::TimestampNanosecond(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::Utf8(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::LargeUtf8(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::Binary(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::LargeBinary(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictUtf8I8(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictUtf8I16(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictUtf8I32(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictUtf8I64(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictUtf8U8(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictUtf8U16(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictUtf8U32(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictUtf8U64(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictLargeUtf8I8(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictLargeUtf8I16(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictLargeUtf8I32(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictLargeUtf8I64(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictLargeUtf8U8(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictLargeUtf8U16(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictLargeUtf8U32(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictLargeUtf8U64(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictBinaryI8(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictBinaryI16(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictBinaryI32(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictBinaryI64(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictBinaryU8(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictBinaryU16(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictBinaryU32(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictBinaryU64(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictLargeBinaryI8(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictLargeBinaryI16(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictLargeBinaryI32(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictLargeBinaryI64(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictLargeBinaryU8(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictLargeBinaryU16(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictLargeBinaryU32(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictLargeBinaryU64(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictFixedSizeBinaryI8(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictFixedSizeBinaryI16(b) => {
                Ok(FinishedColumn::from_array(Arc::new(b.finish())))
            }
            Inner::DictFixedSizeBinaryI32(b) => {
                Ok(FinishedColumn::from_array(Arc::new(b.finish())))
            }
            Inner::DictFixedSizeBinaryI64(b) => {
                Ok(FinishedColumn::from_array(Arc::new(b.finish())))
            }
            Inner::DictFixedSizeBinaryU8(b) => Ok(FinishedColumn::from_array(Arc::new(b.finish()))),
            Inner::DictFixedSizeBinaryU16(b) => {
                Ok(FinishedColumn::from_array(Arc::new(b.finish())))
            }
            Inner::DictFixedSizeBinaryU32(b) => {
                Ok(FinishedColumn::from_array(Arc::new(b.finish())))
            }
            Inner::DictFixedSizeBinaryU64(b) => {
                Ok(FinishedColumn::from_array(Arc::new(b.finish())))
            }
            Inner::Struct(b) => b
                .try_finish()
                .map(|(array, metadata)| FinishedColumn {
                    array: Arc::new(array) as ArrayRef,
                    union_metadata: metadata,
                })
                .map_err(|e| DynError::Builder {
                    message: e.to_string(),
                }),
            Inner::List(b) => b
                .try_finish()
                .map(|(array, metadata)| FinishedColumn {
                    array: Arc::new(array) as ArrayRef,
                    union_metadata: metadata,
                })
                .map_err(|e| DynError::Builder {
                    message: e.to_string(),
                }),
            Inner::LargeList(b) => b
                .try_finish()
                .map(|(array, metadata)| FinishedColumn {
                    array: Arc::new(array) as ArrayRef,
                    union_metadata: metadata,
                })
                .map_err(|e| DynError::Builder {
                    message: e.to_string(),
                }),
            Inner::FixedSizeList(b) => b
                .try_finish()
                .map(|(array, metadata)| FinishedColumn {
                    array: Arc::new(array) as ArrayRef,
                    union_metadata: metadata,
                })
                .map_err(|e| DynError::Builder {
                    message: e.to_string(),
                }),
            Inner::Map(b) => b
                .try_finish()
                .map(|(array, metadata)| FinishedColumn {
                    array: Arc::new(array) as ArrayRef,
                    union_metadata: metadata,
                })
                .map_err(|e| DynError::Builder {
                    message: e.to_string(),
                }),
            Inner::DictPrimitive(b) => Ok(FinishedColumn::from_array(b.finish())),
            Inner::UnionDense(b) => b.try_finish_array(),
            Inner::UnionSparse(b) => b.try_finish_array(),
        }
    }
}

fn new_prim_dict_inner(key: &DataType, value: &DataType) -> Option<Inner> {
    macro_rules! value_switch_for_key {
        ($K:ty) => {
            match value {
                DataType::Int8 => Some(Inner::DictPrimitive(Box::new(DictPrimImpl::<
                    $K,
                    t::Int8Type,
                >::new()))),
                DataType::Int16 => Some(Inner::DictPrimitive(Box::new(DictPrimImpl::<
                    $K,
                    t::Int16Type,
                >::new()))),
                DataType::Int32 => Some(Inner::DictPrimitive(Box::new(DictPrimImpl::<
                    $K,
                    t::Int32Type,
                >::new()))),
                DataType::Int64 => Some(Inner::DictPrimitive(Box::new(DictPrimImpl::<
                    $K,
                    t::Int64Type,
                >::new()))),
                DataType::UInt8 => Some(Inner::DictPrimitive(Box::new(DictPrimImpl::<
                    $K,
                    t::UInt8Type,
                >::new()))),
                DataType::UInt16 => Some(Inner::DictPrimitive(Box::new(DictPrimImpl::<
                    $K,
                    t::UInt16Type,
                >::new()))),
                DataType::UInt32 => Some(Inner::DictPrimitive(Box::new(DictPrimImpl::<
                    $K,
                    t::UInt32Type,
                >::new()))),
                DataType::UInt64 => Some(Inner::DictPrimitive(Box::new(DictPrimImpl::<
                    $K,
                    t::UInt64Type,
                >::new()))),
                DataType::Float32 => Some(Inner::DictPrimitive(Box::new(DictPrimImpl::<
                    $K,
                    t::Float32Type,
                >::new()))),
                DataType::Float64 => Some(Inner::DictPrimitive(Box::new(DictPrimImpl::<
                    $K,
                    t::Float64Type,
                >::new()))),
                _ => None,
            }
        };
    }
    match key {
        DataType::Int8 => value_switch_for_key!(t::Int8Type),
        DataType::Int16 => value_switch_for_key!(t::Int16Type),
        DataType::Int32 => value_switch_for_key!(t::Int32Type),
        DataType::Int64 => value_switch_for_key!(t::Int64Type),
        DataType::UInt8 => value_switch_for_key!(t::UInt8Type),
        DataType::UInt16 => value_switch_for_key!(t::UInt16Type),
        DataType::UInt32 => value_switch_for_key!(t::UInt32Type),
        DataType::UInt64 => value_switch_for_key!(t::UInt64Type),
        _ => None,
    }
}

fn inner_for_primitives(dt: &DataType, capacity: usize) -> Option<Inner> {
    Some(match dt {
        DataType::Boolean => Inner::Bool(b::BooleanBuilder::with_capacity(capacity)),
        DataType::Int8 => Inner::I8(b::PrimitiveBuilder::<t::Int8Type>::with_capacity(capacity)),
        DataType::Int16 => Inner::I16(b::PrimitiveBuilder::<t::Int16Type>::with_capacity(capacity)),
        DataType::Int32 => Inner::I32(b::PrimitiveBuilder::<t::Int32Type>::with_capacity(capacity)),
        DataType::Int64 => Inner::I64(b::PrimitiveBuilder::<t::Int64Type>::with_capacity(capacity)),
        DataType::UInt8 => Inner::U8(b::PrimitiveBuilder::<t::UInt8Type>::with_capacity(capacity)),
        DataType::UInt16 => Inner::U16(b::PrimitiveBuilder::<t::UInt16Type>::with_capacity(
            capacity,
        )),
        DataType::UInt32 => Inner::U32(b::PrimitiveBuilder::<t::UInt32Type>::with_capacity(
            capacity,
        )),
        DataType::UInt64 => Inner::U64(b::PrimitiveBuilder::<t::UInt64Type>::with_capacity(
            capacity,
        )),
        DataType::Float32 => Inner::F32(b::PrimitiveBuilder::<t::Float32Type>::with_capacity(
            capacity,
        )),
        DataType::Float64 => Inner::F64(b::PrimitiveBuilder::<t::Float64Type>::with_capacity(
            capacity,
        )),
        DataType::FixedSizeBinary(w) => {
            Inner::FixedSizeBinary(b::FixedSizeBinaryBuilder::with_capacity(capacity, *w))
        }
        DataType::Date32 => Inner::Date32(b::PrimitiveBuilder::<t::Date32Type>::with_capacity(
            capacity,
        )),
        DataType::Date64 => Inner::Date64(b::PrimitiveBuilder::<t::Date64Type>::with_capacity(
            capacity,
        )),
        DataType::Time32(arrow_schema::TimeUnit::Second) => Inner::Time32Second(
            b::PrimitiveBuilder::<t::Time32SecondType>::with_capacity(capacity),
        ),
        DataType::Time32(arrow_schema::TimeUnit::Millisecond) => Inner::Time32Millisecond(
            b::PrimitiveBuilder::<t::Time32MillisecondType>::with_capacity(capacity),
        ),
        DataType::Time64(arrow_schema::TimeUnit::Microsecond) => Inner::Time64Microsecond(
            b::PrimitiveBuilder::<t::Time64MicrosecondType>::with_capacity(capacity),
        ),
        DataType::Time64(arrow_schema::TimeUnit::Nanosecond) => Inner::Time64Nanosecond(
            b::PrimitiveBuilder::<t::Time64NanosecondType>::with_capacity(capacity),
        ),
        DataType::Duration(arrow_schema::TimeUnit::Second) => Inner::DurationSecond(
            b::PrimitiveBuilder::<t::DurationSecondType>::with_capacity(capacity),
        ),
        DataType::Duration(arrow_schema::TimeUnit::Millisecond) => Inner::DurationMillisecond(
            b::PrimitiveBuilder::<t::DurationMillisecondType>::with_capacity(capacity),
        ),
        DataType::Duration(arrow_schema::TimeUnit::Microsecond) => Inner::DurationMicrosecond(
            b::PrimitiveBuilder::<t::DurationMicrosecondType>::with_capacity(capacity),
        ),
        DataType::Duration(arrow_schema::TimeUnit::Nanosecond) => Inner::DurationNanosecond(
            b::PrimitiveBuilder::<t::DurationNanosecondType>::with_capacity(capacity),
        ),
        DataType::Timestamp(arrow_schema::TimeUnit::Second, _tz) => Inner::TimestampSecond(
            b::PrimitiveBuilder::<t::TimestampSecondType>::with_capacity(capacity),
        ),
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _tz) => {
            Inner::TimestampMillisecond(
                b::PrimitiveBuilder::<t::TimestampMillisecondType>::with_capacity(capacity),
            )
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _tz) => {
            Inner::TimestampMicrosecond(
                b::PrimitiveBuilder::<t::TimestampMicrosecondType>::with_capacity(capacity),
            )
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _tz) => Inner::TimestampNanosecond(
            b::PrimitiveBuilder::<t::TimestampNanosecondType>::with_capacity(capacity),
        ),
        DataType::Utf8 => Inner::Utf8(b::StringBuilder::with_capacity(capacity, capacity * 32)),
        DataType::LargeUtf8 => Inner::LargeUtf8(b::LargeStringBuilder::with_capacity(
            capacity,
            capacity * 32,
        )),
        DataType::Binary => Inner::Binary(b::BinaryBuilder::with_capacity(capacity, capacity * 32)),
        DataType::LargeBinary => Inner::LargeBinary(b::LargeBinaryBuilder::with_capacity(
            capacity,
            capacity * 32,
        )),
        _ => return None,
    })
}

#[allow(clippy::too_many_lines)]
fn inner_for_dictionary(key: &DataType, value: &DataType) -> Option<Inner> {
    Some(match (key, value) {
        // Utf8 dictionaries with signed/unsigned integer keys
        (DataType::Int8, DataType::Utf8) => {
            Inner::DictUtf8I8(b::StringDictionaryBuilder::<t::Int8Type>::new())
        }
        (DataType::Int16, DataType::Utf8) => {
            Inner::DictUtf8I16(b::StringDictionaryBuilder::<t::Int16Type>::new())
        }
        (DataType::Int32, DataType::Utf8) => {
            Inner::DictUtf8I32(b::StringDictionaryBuilder::<t::Int32Type>::new())
        }
        (DataType::Int64, DataType::Utf8) => {
            Inner::DictUtf8I64(b::StringDictionaryBuilder::<t::Int64Type>::new())
        }
        (DataType::UInt8, DataType::Utf8) => {
            Inner::DictUtf8U8(b::StringDictionaryBuilder::<t::UInt8Type>::new())
        }
        (DataType::UInt16, DataType::Utf8) => {
            Inner::DictUtf8U16(b::StringDictionaryBuilder::<t::UInt16Type>::new())
        }
        (DataType::UInt32, DataType::Utf8) => {
            Inner::DictUtf8U32(b::StringDictionaryBuilder::<t::UInt32Type>::new())
        }
        (DataType::UInt64, DataType::Utf8) => {
            Inner::DictUtf8U64(b::StringDictionaryBuilder::<t::UInt64Type>::new())
        }
        // LargeUtf8 dictionaries with signed/unsigned integer keys
        (DataType::Int8, DataType::LargeUtf8) => {
            Inner::DictLargeUtf8I8(b::LargeStringDictionaryBuilder::<t::Int8Type>::new())
        }
        (DataType::Int16, DataType::LargeUtf8) => {
            Inner::DictLargeUtf8I16(b::LargeStringDictionaryBuilder::<t::Int16Type>::new())
        }
        (DataType::Int32, DataType::LargeUtf8) => {
            Inner::DictLargeUtf8I32(b::LargeStringDictionaryBuilder::<t::Int32Type>::new())
        }
        (DataType::Int64, DataType::LargeUtf8) => {
            Inner::DictLargeUtf8I64(b::LargeStringDictionaryBuilder::<t::Int64Type>::new())
        }
        (DataType::UInt8, DataType::LargeUtf8) => {
            Inner::DictLargeUtf8U8(b::LargeStringDictionaryBuilder::<t::UInt8Type>::new())
        }
        (DataType::UInt16, DataType::LargeUtf8) => {
            Inner::DictLargeUtf8U16(b::LargeStringDictionaryBuilder::<t::UInt16Type>::new())
        }
        (DataType::UInt32, DataType::LargeUtf8) => {
            Inner::DictLargeUtf8U32(b::LargeStringDictionaryBuilder::<t::UInt32Type>::new())
        }
        (DataType::UInt64, DataType::LargeUtf8) => {
            Inner::DictLargeUtf8U64(b::LargeStringDictionaryBuilder::<t::UInt64Type>::new())
        }
        // Binary/LargeBinary
        (DataType::Int8, DataType::Binary) => {
            Inner::DictBinaryI8(b::BinaryDictionaryBuilder::<t::Int8Type>::new())
        }
        (DataType::Int16, DataType::Binary) => {
            Inner::DictBinaryI16(b::BinaryDictionaryBuilder::<t::Int16Type>::new())
        }
        (DataType::Int32, DataType::Binary) => {
            Inner::DictBinaryI32(b::BinaryDictionaryBuilder::<t::Int32Type>::new())
        }
        (DataType::Int64, DataType::Binary) => {
            Inner::DictBinaryI64(b::BinaryDictionaryBuilder::<t::Int64Type>::new())
        }
        (DataType::UInt8, DataType::Binary) => {
            Inner::DictBinaryU8(b::BinaryDictionaryBuilder::<t::UInt8Type>::new())
        }
        (DataType::UInt16, DataType::Binary) => {
            Inner::DictBinaryU16(b::BinaryDictionaryBuilder::<t::UInt16Type>::new())
        }
        (DataType::UInt32, DataType::Binary) => {
            Inner::DictBinaryU32(b::BinaryDictionaryBuilder::<t::UInt32Type>::new())
        }
        (DataType::UInt64, DataType::Binary) => {
            Inner::DictBinaryU64(b::BinaryDictionaryBuilder::<t::UInt64Type>::new())
        }
        (DataType::Int8, DataType::LargeBinary) => {
            Inner::DictLargeBinaryI8(b::LargeBinaryDictionaryBuilder::<t::Int8Type>::new())
        }
        (DataType::Int16, DataType::LargeBinary) => {
            Inner::DictLargeBinaryI16(b::LargeBinaryDictionaryBuilder::<t::Int16Type>::new())
        }
        (DataType::Int32, DataType::LargeBinary) => {
            Inner::DictLargeBinaryI32(b::LargeBinaryDictionaryBuilder::<t::Int32Type>::new())
        }
        (DataType::Int64, DataType::LargeBinary) => {
            Inner::DictLargeBinaryI64(b::LargeBinaryDictionaryBuilder::<t::Int64Type>::new())
        }
        (DataType::UInt8, DataType::LargeBinary) => {
            Inner::DictLargeBinaryU8(b::LargeBinaryDictionaryBuilder::<t::UInt8Type>::new())
        }
        (DataType::UInt16, DataType::LargeBinary) => {
            Inner::DictLargeBinaryU16(b::LargeBinaryDictionaryBuilder::<t::UInt16Type>::new())
        }
        (DataType::UInt32, DataType::LargeBinary) => {
            Inner::DictLargeBinaryU32(b::LargeBinaryDictionaryBuilder::<t::UInt32Type>::new())
        }
        (DataType::UInt64, DataType::LargeBinary) => {
            Inner::DictLargeBinaryU64(b::LargeBinaryDictionaryBuilder::<t::UInt64Type>::new())
        }
        // FixedSizeBinary dictionaries
        (DataType::Int8, DataType::FixedSizeBinary(w)) => Inner::DictFixedSizeBinaryI8(
            b::FixedSizeBinaryDictionaryBuilder::<t::Int8Type>::new(*w),
        ),
        (DataType::Int16, DataType::FixedSizeBinary(w)) => Inner::DictFixedSizeBinaryI16(
            b::FixedSizeBinaryDictionaryBuilder::<t::Int16Type>::new(*w),
        ),
        (DataType::Int32, DataType::FixedSizeBinary(w)) => Inner::DictFixedSizeBinaryI32(
            b::FixedSizeBinaryDictionaryBuilder::<t::Int32Type>::new(*w),
        ),
        (DataType::Int64, DataType::FixedSizeBinary(w)) => Inner::DictFixedSizeBinaryI64(
            b::FixedSizeBinaryDictionaryBuilder::<t::Int64Type>::new(*w),
        ),
        (DataType::UInt8, DataType::FixedSizeBinary(w)) => Inner::DictFixedSizeBinaryU8(
            b::FixedSizeBinaryDictionaryBuilder::<t::UInt8Type>::new(*w),
        ),
        (DataType::UInt16, DataType::FixedSizeBinary(w)) => Inner::DictFixedSizeBinaryU16(
            b::FixedSizeBinaryDictionaryBuilder::<t::UInt16Type>::new(*w),
        ),
        (DataType::UInt32, DataType::FixedSizeBinary(w)) => Inner::DictFixedSizeBinaryU32(
            b::FixedSizeBinaryDictionaryBuilder::<t::UInt32Type>::new(*w),
        ),
        (DataType::UInt64, DataType::FixedSizeBinary(w)) => Inner::DictFixedSizeBinaryU64(
            b::FixedSizeBinaryDictionaryBuilder::<t::UInt64Type>::new(*w),
        ),
        // Primitive dictionary values (numeric & float)
        (k, v) => return new_prim_dict_inner(k, v),
    })
}

fn inner_for_nested(dt: &DataType, capacity: usize) -> Result<Option<Inner>, DynError> {
    Ok(Some(match dt {
        DataType::Struct(fields) => {
            let children: Result<Vec<_>, _> = fields
                .iter()
                .map(|f| try_new_dyn_builder_with_capacity(f.data_type(), capacity))
                .collect();
            Inner::Struct(StructCol::new_with_children(fields.clone(), children?))
        }
        DataType::List(item) => {
            // For nested types, we don't know the child capacity, so use 0
            let child = try_new_dyn_builder_with_capacity(item.data_type(), 0)?;
            Inner::List(ListCol::new_with_child(item.clone(), child))
        }
        DataType::LargeList(item) => {
            let child = try_new_dyn_builder_with_capacity(item.data_type(), 0)?;
            Inner::LargeList(LargeListCol::new_with_child(item.clone(), child))
        }
        DataType::FixedSizeList(item, len) => {
            // For fixed-size list, child capacity = parent capacity * list length
            let child_capacity = capacity.saturating_mul((*len).max(0) as usize);
            let child = try_new_dyn_builder_with_capacity(item.data_type(), child_capacity)?;
            Inner::FixedSizeList(FixedSizeListCol::new_with_child(item.clone(), *len, child))
        }
        DataType::Map(entry_field, ordered) => {
            let DataType::Struct(children) = entry_field.data_type() else {
                return Ok(None);
            };
            if children.len() != 2 {
                return Ok(None);
            }
            let key_builder = try_new_dyn_builder_with_capacity(children[0].data_type(), 0)?;
            let value_builder = try_new_dyn_builder_with_capacity(children[1].data_type(), 0)?;
            Inner::Map(MapCol::new_with_children(
                entry_field.clone(),
                *ordered,
                key_builder,
                value_builder,
            ))
        }
        _ => return Ok(None),
    }))
}

fn inner_for_union(dt: &DataType, capacity: usize) -> Result<Option<Inner>, DynError> {
    match dt {
        DataType::Union(fields, mode) => {
            // Union children share capacity (only one variant active per row)
            let children: Result<Vec<_>, _> = fields
                .iter()
                .map(|(_tag, field)| try_new_dyn_builder_with_capacity(field.data_type(), capacity))
                .collect();
            let children = children?;
            let fields_owned: UnionFields = fields
                .iter()
                .map(|(tag, field)| (tag, field.clone()))
                .collect();
            let inner = match mode {
                UnionMode::Dense => Inner::UnionDense(DenseUnionCol::new(fields_owned, children)?),
                UnionMode::Sparse => {
                    Inner::UnionSparse(SparseUnionCol::new(fields_owned, children)?)
                }
            };
            Ok(Some(inner))
        }
        _ => Ok(None),
    }
}

fn try_build_inner(dt: &DataType, capacity: usize) -> Result<Inner, DynError> {
    if let Some(inner) = inner_for_primitives(dt, capacity) {
        return Ok(inner);
    }
    if let DataType::Dictionary(k, v) = dt
        && let Some(inner) = inner_for_dictionary(k, v)
    {
        return Ok(inner);
    }
    if let Some(inner) = inner_for_nested(dt, capacity)? {
        return Ok(inner);
    }
    if let Some(inner) = inner_for_union(dt, capacity)? {
        return Ok(inner);
    }
    if matches!(dt, DataType::Null) {
        return Ok(Inner::Null(b::NullBuilder::new()));
    }
    Err(DynError::Builder {
        message: format!("unsupported DataType: {dt:?}"),
    })
}

/// Factory function that returns a dynamic builder for a given `DataType` with capacity hint.
///
/// This is the only place intended to perform a `match DataType`.
///
/// # Errors
/// Returns `DynError::Builder` if the schema contains invalid union definitions
/// (e.g., duplicate type ids, empty unions, or field/builder count mismatches),
/// or if the `DataType` is not supported.
pub fn try_new_dyn_builder_with_capacity(
    dt: &DataType,
    capacity: usize,
) -> Result<Box<dyn DynColumnBuilder>, DynError> {
    let dt_cloned = dt.clone();
    let inner = try_build_inner(&dt_cloned, capacity)?;
    Ok(Box::new(Col {
        dt: dt_cloned,
        inner,
    }))
}

/// Factory function that returns a dynamic builder for a given `DataType`.
///
/// This is the only place intended to perform a `match DataType`.
///
/// # Errors
/// Returns `DynError::Builder` if the schema contains invalid union definitions
/// (e.g., duplicate type ids, empty unions, or field/builder count mismatches),
/// or if the `DataType` is not supported.
pub fn try_new_dyn_builder(dt: &DataType) -> Result<Box<dyn DynColumnBuilder>, DynError> {
    try_new_dyn_builder_with_capacity(dt, 0)
}

/// Factory function that returns a dynamic builder for a given `DataType`.
///
/// This is the only place intended to perform a `match DataType`.
///
/// # Panics
/// Panics if the schema contains invalid or unsupported definitions.
/// Prefer [`try_new_dyn_builder`] for fallible construction.
#[must_use]
pub fn new_dyn_builder(dt: &DataType) -> Box<dyn DynColumnBuilder> {
    try_new_dyn_builder(dt).expect("valid DataType for builder construction")
}
