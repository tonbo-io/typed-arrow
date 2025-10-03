//! Dictionary-encoded column bindings and key mapping.

use std::marker::PhantomData;

use arrow_array::{
    builder::{
        BinaryDictionaryBuilder, FixedSizeBinaryDictionaryBuilder, LargeBinaryDictionaryBuilder,
        LargeStringDictionaryBuilder, PrimitiveDictionaryBuilder, StringDictionaryBuilder,
    },
    types::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    },
};
use arrow_schema::DataType;

use super::{binary::LargeBinary, strings::LargeUtf8, ArrowBinding};

/// Wrapper denoting an Arrow Dictionary column with key type `K` and values of `V`.
///
/// The inner value is intentionally not exposed. Construct with `Dictionary::new`
/// and access the contained value via `Dictionary::value` or `Dictionary::into_value`.
///
/// This prevents accidental reliance on representation details (e.g., raw keys) and
/// keeps the API focused on appending logical values. The builder handles interning to keys.
#[repr(transparent)]
pub struct Dictionary<K, V>(V, PhantomData<K>);

impl<K, V> Dictionary<K, V> {
    /// Create a new dictionary value wrapper.
    #[inline]
    pub fn new(value: V) -> Self {
        Self(value, PhantomData)
    }

    /// Borrow the contained logical value.
    #[inline]
    pub fn value(&self) -> &V {
        &self.0
    }

    /// Consume and return the contained logical value.
    #[inline]
    pub fn into_value(self) -> V {
        self.0
    }
}

impl<K, V> From<V> for Dictionary<K, V> {
    #[inline]
    fn from(value: V) -> Self {
        Self::new(value)
    }
}

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
        let _ = b.append(v.value().as_str());
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
        let _ = b.append(v.value().as_slice());
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
            Box::new(DataType::FixedSizeBinary(
                i32::try_from(N).expect("width fits i32"),
            )),
        )
    }
    fn new_builder(_capacity: usize) -> Self::Builder {
        // Builder enforces width on appended values; pass byte width
        FixedSizeBinaryDictionaryBuilder::new(i32::try_from(N).expect("width fits i32"))
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        let _ = b.append(*v.value());
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
        let _ = b.append(v.value().as_slice());
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
        let _ = b.append(v.value().as_str());
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
                let _ = b.append(*v.value());
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

// ArrowBindingView implementation for Dictionary types
// Decodes the dictionary value at the given index
#[cfg(feature = "views")]
impl<K, V> super::ArrowBindingView for Dictionary<K, V>
where
    K: DictKey + 'static,
    V: ArrowBinding + super::ArrowBindingView + 'static,
    <K as DictKey>::ArrowKey: arrow_array::types::ArrowDictionaryKeyType,
{
    type Array = arrow_array::DictionaryArray<<K as DictKey>::ArrowKey>;
    type View<'a>
        = V::View<'a>
    where
        Self: 'a;

    fn get_view(
        array: &Self::Array,
        index: usize,
    ) -> Result<Self::View<'_>, crate::schema::ViewAccessError> {
        use arrow_array::Array;
        use arrow_buffer::ArrowNativeType;

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

        // Get the key (dictionary index) for this row
        let keys = array.keys();
        let key_value = keys.value(index);
        let dict_index = key_value.as_usize();

        // Get the values array and downcast to the correct type
        let values_array = array.values();
        let typed_values = values_array
            .as_any()
            .downcast_ref::<<V as super::ArrowBindingView>::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: V::data_type(),
                actual: values_array.data_type().clone(),
                field_name: None,
            })?;

        // Return a view of the decoded value
        V::get_view(typed_values, dict_index)
    }
}
