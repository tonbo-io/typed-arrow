//! Factory for dynamic builders, mapping Arrow `DataType` to concrete builders.

use std::sync::Arc;

use arrow_array::{builder as b, types as t, ArrayRef};
use arrow_schema::DataType;

use crate::{
    cell::DynCell,
    dyn_builder::DynColumnBuilder,
    nested::{FixedSizeListCol, LargeListCol, ListCol, StructCol},
    DynError,
};

/// Factory function that returns a dynamic builder for a given `DataType`.
///
/// This is the only place intended to perform a `match DataType`.
pub fn new_dyn_builder(dt: &DataType, nullable: bool) -> Box<dyn DynColumnBuilder> {
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
        // Dictionary (initial support for Utf8/LargeUtf8 values and
        // Binary/LargeBinary/FixedSizeBinary)
        DictUtf8I8(b::StringDictionaryBuilder<t::Int8Type>),
        DictUtf8I16(b::StringDictionaryBuilder<t::Int16Type>),
        DictUtf8I32(b::StringDictionaryBuilder<t::Int32Type>),
        DictUtf8I64(b::StringDictionaryBuilder<t::Int64Type>),
        DictLargeUtf8I8(b::LargeStringDictionaryBuilder<t::Int8Type>),
        DictLargeUtf8I16(b::LargeStringDictionaryBuilder<t::Int16Type>),
        DictLargeUtf8I32(b::LargeStringDictionaryBuilder<t::Int32Type>),
        DictLargeUtf8I64(b::LargeStringDictionaryBuilder<t::Int64Type>),
        DictBinaryI8(b::BinaryDictionaryBuilder<t::Int8Type>),
        DictBinaryI16(b::BinaryDictionaryBuilder<t::Int16Type>),
        DictBinaryI32(b::BinaryDictionaryBuilder<t::Int32Type>),
        DictBinaryI64(b::BinaryDictionaryBuilder<t::Int64Type>),
        DictLargeBinaryI8(b::LargeBinaryDictionaryBuilder<t::Int8Type>),
        DictLargeBinaryI16(b::LargeBinaryDictionaryBuilder<t::Int16Type>),
        DictLargeBinaryI32(b::LargeBinaryDictionaryBuilder<t::Int32Type>),
        DictLargeBinaryI64(b::LargeBinaryDictionaryBuilder<t::Int64Type>),
        DictFixedSizeBinaryI8(b::FixedSizeBinaryDictionaryBuilder<t::Int8Type>),
        DictFixedSizeBinaryI16(b::FixedSizeBinaryDictionaryBuilder<t::Int16Type>),
        DictFixedSizeBinaryI32(b::FixedSizeBinaryDictionaryBuilder<t::Int32Type>),
        DictFixedSizeBinaryI64(b::FixedSizeBinaryDictionaryBuilder<t::Int64Type>),
        // Nested
        Struct(StructCol),
        List(ListCol),
        LargeList(LargeListCol),
        FixedSizeList(FixedSizeListCol),
    }

    struct Col {
        dt: DataType,
        inner: Inner,
        nullable: bool,
    }

    impl DynColumnBuilder for Col {
        fn data_type(&self) -> &DataType {
            &self.dt
        }
        fn is_nullable(&self) -> bool {
            self.nullable
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
                Inner::DictLargeUtf8I8(b) => b.append_null(),
                Inner::DictLargeUtf8I16(b) => b.append_null(),
                Inner::DictLargeUtf8I32(b) => b.append_null(),
                Inner::DictLargeUtf8I64(b) => b.append_null(),
                Inner::DictBinaryI8(b) => b.append_null(),
                Inner::DictBinaryI16(b) => b.append_null(),
                Inner::DictBinaryI32(b) => b.append_null(),
                Inner::DictBinaryI64(b) => b.append_null(),
                Inner::DictLargeBinaryI8(b) => b.append_null(),
                Inner::DictLargeBinaryI16(b) => b.append_null(),
                Inner::DictLargeBinaryI32(b) => b.append_null(),
                Inner::DictLargeBinaryI64(b) => b.append_null(),
                Inner::DictFixedSizeBinaryI8(b) => b.append_null(),
                Inner::DictFixedSizeBinaryI16(b) => b.append_null(),
                Inner::DictFixedSizeBinaryI32(b) => b.append_null(),
                Inner::DictFixedSizeBinaryI64(b) => b.append_null(),
                Inner::Struct(b) => b.append_null(),
                Inner::List(b) => b.append_null(),
                Inner::LargeList(b) => b.append_null(),
                Inner::FixedSizeList(b) => b.append_null(),
            }
        }
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
                    let _ = b.append(s.as_str());
                    Ok(())
                }
                (Inner::DictUtf8I16(b), DynCell::Str(s)) => {
                    let _ = b.append(s.as_str());
                    Ok(())
                }
                (Inner::DictUtf8I32(b), DynCell::Str(s)) => {
                    let _ = b.append(s.as_str());
                    Ok(())
                }
                (Inner::DictUtf8I64(b), DynCell::Str(s)) => {
                    let _ = b.append(s.as_str());
                    Ok(())
                }
                // Dictionary (LargeUtf8 values)
                (Inner::DictLargeUtf8I8(b), DynCell::Str(s)) => {
                    let _ = b.append(s.as_str());
                    Ok(())
                }
                (Inner::DictLargeUtf8I16(b), DynCell::Str(s)) => {
                    let _ = b.append(s.as_str());
                    Ok(())
                }
                (Inner::DictLargeUtf8I32(b), DynCell::Str(s)) => {
                    let _ = b.append(s.as_str());
                    Ok(())
                }
                (Inner::DictLargeUtf8I64(b), DynCell::Str(s)) => {
                    let _ = b.append(s.as_str());
                    Ok(())
                }
                // Dictionary (Binary values)
                (Inner::DictBinaryI8(b), DynCell::Bin(bs)) => {
                    let _ = b.append(bs.as_slice());
                    Ok(())
                }
                (Inner::DictBinaryI16(b), DynCell::Bin(bs)) => {
                    let _ = b.append(bs.as_slice());
                    Ok(())
                }
                (Inner::DictBinaryI32(b), DynCell::Bin(bs)) => {
                    let _ = b.append(bs.as_slice());
                    Ok(())
                }
                (Inner::DictBinaryI64(b), DynCell::Bin(bs)) => {
                    let _ = b.append(bs.as_slice());
                    Ok(())
                }
                (Inner::DictLargeBinaryI8(b), DynCell::Bin(bs)) => {
                    let _ = b.append(bs.as_slice());
                    Ok(())
                }
                (Inner::DictLargeBinaryI16(b), DynCell::Bin(bs)) => {
                    let _ = b.append(bs.as_slice());
                    Ok(())
                }
                (Inner::DictLargeBinaryI32(b), DynCell::Bin(bs)) => {
                    let _ = b.append(bs.as_slice());
                    Ok(())
                }
                (Inner::DictLargeBinaryI64(b), DynCell::Bin(bs)) => {
                    let _ = b.append(bs.as_slice());
                    Ok(())
                }
                (Inner::DictFixedSizeBinaryI8(b), DynCell::Bin(bs)) => {
                    let _ = b.append(bs.as_slice());
                    Ok(())
                }
                (Inner::DictFixedSizeBinaryI16(b), DynCell::Bin(bs)) => {
                    let _ = b.append(bs.as_slice());
                    Ok(())
                }
                (Inner::DictFixedSizeBinaryI32(b), DynCell::Bin(bs)) => {
                    let _ = b.append(bs.as_slice());
                    Ok(())
                }
                (Inner::DictFixedSizeBinaryI64(b), DynCell::Bin(bs)) => {
                    let _ = b.append(bs.as_slice());
                    Ok(())
                }
                // Nested
                (Inner::Struct(b), DynCell::Struct(values)) => b.append_struct(values),
                (Inner::List(b), DynCell::List(values)) => b.append_list(values),
                (Inner::LargeList(b), DynCell::List(values)) => b.append_list(values),
                (Inner::FixedSizeList(b), DynCell::FixedSizeList(values)) => b.append_fixed(values),
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
                Inner::DictLargeUtf8I8(b) => Arc::new(b.finish()),
                Inner::DictLargeUtf8I16(b) => Arc::new(b.finish()),
                Inner::DictLargeUtf8I32(b) => Arc::new(b.finish()),
                Inner::DictLargeUtf8I64(b) => Arc::new(b.finish()),
                Inner::DictBinaryI8(b) => Arc::new(b.finish()),
                Inner::DictBinaryI16(b) => Arc::new(b.finish()),
                Inner::DictBinaryI32(b) => Arc::new(b.finish()),
                Inner::DictBinaryI64(b) => Arc::new(b.finish()),
                Inner::DictLargeBinaryI8(b) => Arc::new(b.finish()),
                Inner::DictLargeBinaryI16(b) => Arc::new(b.finish()),
                Inner::DictLargeBinaryI32(b) => Arc::new(b.finish()),
                Inner::DictLargeBinaryI64(b) => Arc::new(b.finish()),
                Inner::DictFixedSizeBinaryI8(b) => Arc::new(b.finish()),
                Inner::DictFixedSizeBinaryI16(b) => Arc::new(b.finish()),
                Inner::DictFixedSizeBinaryI32(b) => Arc::new(b.finish()),
                Inner::DictFixedSizeBinaryI64(b) => Arc::new(b.finish()),
                Inner::Struct(b) => Arc::new(b.finish()),
                Inner::List(b) => Arc::new(b.finish()),
                Inner::LargeList(b) => Arc::new(b.finish()),
                Inner::FixedSizeList(b) => Arc::new(b.finish()),
            }
        }
    }

    let dt_cloned = dt.clone();
    let inner = match &dt_cloned {
        DataType::Null => Inner::Null(b::NullBuilder::new()),
        DataType::Boolean => Inner::Bool(b::BooleanBuilder::new()),
        DataType::Int8 => Inner::I8(b::PrimitiveBuilder::<t::Int8Type>::new()),
        DataType::Int16 => Inner::I16(b::PrimitiveBuilder::<t::Int16Type>::new()),
        DataType::Int32 => Inner::I32(b::PrimitiveBuilder::<t::Int32Type>::new()),
        DataType::Int64 => Inner::I64(b::PrimitiveBuilder::<t::Int64Type>::new()),
        DataType::UInt8 => Inner::U8(b::PrimitiveBuilder::<t::UInt8Type>::new()),
        DataType::UInt16 => Inner::U16(b::PrimitiveBuilder::<t::UInt16Type>::new()),
        DataType::UInt32 => Inner::U32(b::PrimitiveBuilder::<t::UInt32Type>::new()),
        DataType::UInt64 => Inner::U64(b::PrimitiveBuilder::<t::UInt64Type>::new()),
        DataType::Float32 => Inner::F32(b::PrimitiveBuilder::<t::Float32Type>::new()),
        DataType::Float64 => Inner::F64(b::PrimitiveBuilder::<t::Float64Type>::new()),
        DataType::FixedSizeBinary(w) => {
            Inner::FixedSizeBinary(b::FixedSizeBinaryBuilder::with_capacity(0, *w))
        }
        DataType::Date32 => Inner::Date32(b::PrimitiveBuilder::<t::Date32Type>::new()),
        DataType::Date64 => Inner::Date64(b::PrimitiveBuilder::<t::Date64Type>::new()),
        DataType::Time32(arrow_schema::TimeUnit::Second) => {
            Inner::Time32Second(b::PrimitiveBuilder::<t::Time32SecondType>::new())
        }
        DataType::Time32(arrow_schema::TimeUnit::Millisecond) => {
            Inner::Time32Millisecond(b::PrimitiveBuilder::<t::Time32MillisecondType>::new())
        }
        DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
            Inner::Time64Microsecond(b::PrimitiveBuilder::<t::Time64MicrosecondType>::new())
        }
        DataType::Time64(arrow_schema::TimeUnit::Nanosecond) => {
            Inner::Time64Nanosecond(b::PrimitiveBuilder::<t::Time64NanosecondType>::new())
        }
        DataType::Duration(arrow_schema::TimeUnit::Second) => {
            Inner::DurationSecond(b::PrimitiveBuilder::<t::DurationSecondType>::new())
        }
        DataType::Duration(arrow_schema::TimeUnit::Millisecond) => {
            Inner::DurationMillisecond(b::PrimitiveBuilder::<t::DurationMillisecondType>::new())
        }
        DataType::Duration(arrow_schema::TimeUnit::Microsecond) => {
            Inner::DurationMicrosecond(b::PrimitiveBuilder::<t::DurationMicrosecondType>::new())
        }
        DataType::Duration(arrow_schema::TimeUnit::Nanosecond) => {
            Inner::DurationNanosecond(b::PrimitiveBuilder::<t::DurationNanosecondType>::new())
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Second, _tz) => {
            Inner::TimestampSecond(b::PrimitiveBuilder::<t::TimestampSecondType>::new())
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _tz) => {
            Inner::TimestampMillisecond(b::PrimitiveBuilder::<t::TimestampMillisecondType>::new())
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _tz) => {
            Inner::TimestampMicrosecond(b::PrimitiveBuilder::<t::TimestampMicrosecondType>::new())
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _tz) => {
            Inner::TimestampNanosecond(b::PrimitiveBuilder::<t::TimestampNanosecondType>::new())
        }
        DataType::Utf8 => Inner::Utf8(b::StringBuilder::new()),
        DataType::LargeUtf8 => Inner::LargeUtf8(b::LargeStringBuilder::new()),
        DataType::Binary => Inner::Binary(b::BinaryBuilder::new()),
        DataType::LargeBinary => Inner::LargeBinary(b::LargeBinaryBuilder::new()),
        DataType::Dictionary(key, value) => match (&**key, &**value) {
            // Utf8 dictionaries with signed integer keys
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
            // LargeUtf8 dictionaries with signed integer keys
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
            // Binary dictionaries with signed integer keys
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
            // FixedSizeBinary dictionaries (enforce width at builder init)
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
            _ => Inner::Null(b::NullBuilder::new()),
        },
        DataType::Struct(fields) => {
            let children = fields
                .iter()
                .map(|f| new_dyn_builder(f.data_type(), f.is_nullable()))
                .collect();
            Inner::Struct(StructCol::new_with_children(fields.clone(), children))
        }
        DataType::List(item) => {
            let child = new_dyn_builder(item.data_type(), item.is_nullable());
            Inner::List(ListCol::new_with_child(item.clone(), child))
        }
        DataType::LargeList(item) => {
            let child = new_dyn_builder(item.data_type(), item.is_nullable());
            Inner::LargeList(LargeListCol::new_with_child(item.clone(), child))
        }
        DataType::FixedSizeList(item, len) => {
            let child = new_dyn_builder(item.data_type(), item.is_nullable());
            Inner::FixedSizeList(FixedSizeListCol::new_with_child(item.clone(), *len, child))
        }
        _ => Inner::Null(b::NullBuilder::new()),
    };
    Box::new(Col {
        dt: dt_cloned,
        inner,
        nullable,
    })
}
