//! Dictionary-encoded column bindings and key mapping.

use std::marker::PhantomData;

use arrow_array::{builder::*, types::*};
use arrow_schema::DataType;

use super::{binary::LargeBinary, strings::LargeUtf8, ArrowBinding};

/// Wrapper denoting an Arrow Dictionary column with key type `K` and values of `V`.
pub struct Dictionary<K, V>(pub V, pub PhantomData<K>);

/// Dictionary key mapping from Rust integer to Arrow key type.
pub trait DictKey {
    /// Arrow key type corresponding to this Rust integer key.
    type ArrowKey;

    /// The Arrow `DataType` for the key.
    fn data_type() -> DataType;
}

macro_rules! impl_dict_key {
    ($rust:ty, $arrow:ty, $dt:expr) => {
        impl DictKey for $rust {
            type ArrowKey = $arrow;
            fn data_type() -> DataType {
                $dt
            }
        }
    };
}

impl_dict_key!(i8, Int8Type, DataType::Int8);
impl_dict_key!(i16, Int16Type, DataType::Int16);
impl_dict_key!(i32, Int32Type, DataType::Int32);
impl_dict_key!(i64, Int64Type, DataType::Int64);
impl_dict_key!(u8, UInt8Type, DataType::UInt8);
impl_dict_key!(u16, UInt16Type, DataType::UInt16);
impl_dict_key!(u32, UInt32Type, DataType::UInt32);
impl_dict_key!(u64, UInt64Type, DataType::UInt64);

// Utf8 values
impl<K> ArrowBinding for Dictionary<K, String>
where
    K: DictKey,
    <K as DictKey>::ArrowKey: arrow_array::types::ArrowDictionaryKeyType,
{
    type Builder = StringDictionaryBuilder<<K as DictKey>::ArrowKey>;
    type Array = arrow_array::DictionaryArray<<K as DictKey>::ArrowKey>;
    fn data_type() -> DataType {
        DataType::Dictionary(
            Box::new(<K as DictKey>::data_type()),
            Box::new(DataType::Utf8),
        )
    }
    fn new_builder(_capacity: usize) -> Self::Builder {
        StringDictionaryBuilder::new()
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        let _ = b.append(v.0.as_str());
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// Binary values
impl<K> ArrowBinding for Dictionary<K, Vec<u8>>
where
    K: DictKey,
    <K as DictKey>::ArrowKey: arrow_array::types::ArrowDictionaryKeyType,
{
    type Builder = BinaryDictionaryBuilder<<K as DictKey>::ArrowKey>;
    type Array = arrow_array::DictionaryArray<<K as DictKey>::ArrowKey>;
    fn data_type() -> DataType {
        DataType::Dictionary(
            Box::new(<K as DictKey>::data_type()),
            Box::new(DataType::Binary),
        )
    }
    fn new_builder(_capacity: usize) -> Self::Builder {
        BinaryDictionaryBuilder::new()
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        let _ = b.append(v.0.as_slice());
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// FixedSizeBinary values: [u8; N]
impl<K, const N: usize> ArrowBinding for Dictionary<K, [u8; N]>
where
    K: DictKey,
    <K as DictKey>::ArrowKey: arrow_array::types::ArrowDictionaryKeyType,
{
    type Builder = FixedSizeBinaryDictionaryBuilder<<K as DictKey>::ArrowKey>;
    type Array = arrow_array::DictionaryArray<<K as DictKey>::ArrowKey>;
    fn data_type() -> DataType {
        DataType::Dictionary(
            Box::new(<K as DictKey>::data_type()),
            Box::new(DataType::FixedSizeBinary(N as i32)),
        )
    }
    fn new_builder(_capacity: usize) -> Self::Builder {
        // Builder enforces width on appended values; pass byte width
        FixedSizeBinaryDictionaryBuilder::new(N as i32)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        let _ = b.append(v.0);
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// LargeBinary values
impl<K> ArrowBinding for Dictionary<K, LargeBinary>
where
    K: DictKey,
    <K as DictKey>::ArrowKey: arrow_array::types::ArrowDictionaryKeyType,
{
    type Builder = LargeBinaryDictionaryBuilder<<K as DictKey>::ArrowKey>;
    type Array = arrow_array::DictionaryArray<<K as DictKey>::ArrowKey>;
    fn data_type() -> DataType {
        DataType::Dictionary(
            Box::new(<K as DictKey>::data_type()),
            Box::new(DataType::LargeBinary),
        )
    }
    fn new_builder(_capacity: usize) -> Self::Builder {
        LargeBinaryDictionaryBuilder::new()
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        let _ = b.append(v.0 .0.as_slice());
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// LargeUtf8 values
impl<K> ArrowBinding for Dictionary<K, LargeUtf8>
where
    K: DictKey,
    <K as DictKey>::ArrowKey: arrow_array::types::ArrowDictionaryKeyType,
{
    type Builder = LargeStringDictionaryBuilder<<K as DictKey>::ArrowKey>;
    type Array = arrow_array::DictionaryArray<<K as DictKey>::ArrowKey>;
    fn data_type() -> DataType {
        DataType::Dictionary(
            Box::new(<K as DictKey>::data_type()),
            Box::new(DataType::LargeUtf8),
        )
    }
    fn new_builder(_capacity: usize) -> Self::Builder {
        LargeStringDictionaryBuilder::new()
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        let _ = b.append(v.0 .0.as_str());
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// Primitive values via macro
macro_rules! impl_dict_primitive_value {
    ($rust:ty, $atype:ty, $dt:expr) => {
        impl<K> ArrowBinding for Dictionary<K, $rust>
        where
            K: DictKey,
            <K as DictKey>::ArrowKey: arrow_array::types::ArrowDictionaryKeyType,
        {
            type Builder = PrimitiveDictionaryBuilder<<K as DictKey>::ArrowKey, $atype>;
            type Array = arrow_array::DictionaryArray<<K as DictKey>::ArrowKey>;
            fn data_type() -> DataType {
                DataType::Dictionary(Box::new(<K as DictKey>::data_type()), Box::new($dt))
            }
            fn new_builder(_capacity: usize) -> Self::Builder {
                PrimitiveDictionaryBuilder::<_, $atype>::new()
            }
            fn append_value(b: &mut Self::Builder, v: &Self) {
                let _ = b.append(v.0);
            }
            fn append_null(b: &mut Self::Builder) {
                b.append_null();
            }
            fn finish(mut b: Self::Builder) -> Self::Array {
                b.finish()
            }
        }
    };
}

impl_dict_primitive_value!(i8, Int8Type, DataType::Int8);
impl_dict_primitive_value!(i16, Int16Type, DataType::Int16);
impl_dict_primitive_value!(i32, Int32Type, DataType::Int32);
impl_dict_primitive_value!(i64, Int64Type, DataType::Int64);
impl_dict_primitive_value!(u8, UInt8Type, DataType::UInt8);
impl_dict_primitive_value!(u16, UInt16Type, DataType::UInt16);
impl_dict_primitive_value!(u32, UInt32Type, DataType::UInt32);
impl_dict_primitive_value!(u64, UInt64Type, DataType::UInt64);
impl_dict_primitive_value!(f32, Float32Type, DataType::Float32);
impl_dict_primitive_value!(f64, Float64Type, DataType::Float64);
