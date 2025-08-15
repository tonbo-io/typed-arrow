//! Bridge from Rust types to Arrow typed arrays and `DataType`.
//!
//! This module provides a compile-time mapping from Rust value types to
//! arrow-rs typed builders/arrays and their corresponding `arrow_schema::DataType`,
//! avoiding any runtime `DataType` matching.
//!
//! - Core trait: [`ArrowBinding`] (Rust type → typed builder/array + `DataType`).
//! - Primitives: `i{8,16,32,64}`, `u{8,16,32,64}`, `f{32,64}`, `bool`.
//! - Utf8/Binary: `String` → `Utf8`, `Vec<u8>` → `Binary`.
//! - Nested containers:
//!   - [`List<T>`] with non-null items, and [`List<Option<T>>`] for nullable items.
//!   - [`Dictionary<K, String>`] → dictionary-encoded Utf8 values.
//!   - [`Timestamp<U>`] with unit markers ([`Second`], [`Millisecond`], [`Microsecond`],
//!     [`Nanosecond`]) and [`TimestampTz<U, Z>`] for timezone-aware timestamps.
//!   - Any `T: Record + StructMeta` binds to an Arrow `StructArray`.
//!
//! See tests for end-to-end examples and usage patterns.

use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};

use arrow_array::{
    builder::{
        ArrayBuilder, BinaryBuilder, BinaryDictionaryBuilder, BooleanBuilder, Decimal128Builder,
        Decimal256Builder, FixedSizeBinaryBuilder, FixedSizeBinaryDictionaryBuilder,
        LargeBinaryBuilder, LargeBinaryDictionaryBuilder, LargeListBuilder, LargeStringBuilder,
        LargeStringDictionaryBuilder, ListBuilder, MapBuilder, NullBuilder, PrimitiveBuilder,
        PrimitiveDictionaryBuilder, StringBuilder, StringDictionaryBuilder, StructBuilder,
    },
    types::{
        ArrowDictionaryKeyType, ArrowPrimitiveType, ArrowTimestampType, Date32Type, Date64Type,
        DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
        DurationSecondType, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
        Int8Type, IntervalDayTime as IntervalDayTimeNative, IntervalDayTimeType,
        IntervalMonthDayNano as IntervalMonthDayNanoNative, IntervalMonthDayNanoType,
        IntervalYearMonthType, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
        Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType,
        TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type,
        UInt8Type,
    },
    Array, Decimal128Array, Decimal256Array, MapArray, NullArray, PrimitiveArray, StringArray,
};
use arrow_buffer::i256;
use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};
use half::f16;

use crate::schema::{ColAt, Record, StructMeta};

/// Binding from a Rust type to Arrow typed builders/arrays and `DataType`.
///
/// Implementations of this trait provide a zero-cost, monomorphized mapping
/// between a Rust value type and its Arrow representation.
///
/// Associated items:
/// - `Builder`: a concrete `arrow_array::builder::*` type used to build the column.
/// - `Array`: the concrete `arrow_array::*Array` produced by `finish`.
/// - `data_type()`: the `arrow_schema::DataType` for this type.
///
/// Example (primitive)
/// ```no_run
/// use arrow_array::{builder::PrimitiveBuilder, types::Int64Type, Array};
/// use typed_arrow::bridge::ArrowBinding;
/// let mut b = PrimitiveBuilder::<Int64Type>::with_capacity(2);
/// <i64 as ArrowBinding>::append_value(&mut b, &42);
/// <i64 as ArrowBinding>::append_null(&mut b);
/// let a = <i64 as ArrowBinding>::finish(b);
/// assert_eq!(a.len(), 2);
/// ```
pub trait ArrowBinding {
    /// Concrete Arrow builder type used for this Rust type.
    type Builder;

    /// Concrete Arrow array type produced by `finish`.
    type Array: Array;

    /// The Arrow `DataType` corresponding to this Rust type.
    fn data_type() -> DataType;

    /// Create a new builder with an optional capacity hint.
    fn new_builder(capacity: usize) -> Self::Builder;

    /// Append a non-null value to the builder.
    fn append_value(b: &mut Self::Builder, v: &Self);

    /// Append a null to the builder.
    fn append_null(b: &mut Self::Builder);

    /// Finish the builder and produce a typed Arrow array.
    fn finish(b: Self::Builder) -> Self::Array;
}

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

// -------------------------
// Null type marker
// -------------------------

/// Marker type for Arrow `DataType::Null` columns.
///
/// A `Null` column contains only nulls. Appending a value or a null both append
/// a null slot. This maps to `arrow_array::NullArray` and uses `NullBuilder`.
pub struct Null;

impl ArrowBinding for Null {
    type Builder = NullBuilder;
    type Array = NullArray;
    fn data_type() -> DataType {
        DataType::Null
    }
    fn new_builder(_capacity: usize) -> Self::Builder {
        NullBuilder::new()
    }
    fn append_value(b: &mut Self::Builder, _v: &Self) {
        b.append_null();
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

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

// Boolean
impl ArrowBinding for bool {
    type Builder = BooleanBuilder;

    type Array = arrow_array::BooleanArray;

    fn data_type() -> DataType {
        DataType::Boolean
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        BooleanBuilder::with_capacity(capacity)
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

// -------------------------
// LargeUtf8: wrapper around String
// -------------------------

/// Wrapper denoting Arrow `LargeUtf8` values. Use when individual strings can be
/// extremely large or when 64-bit offsets are preferred.
pub struct LargeUtf8(pub String);

impl ArrowBinding for LargeUtf8 {
    type Builder = LargeStringBuilder;

    type Array = arrow_array::LargeStringArray;

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

// -------------------------
// FixedSizeBinary: [u8; N]
// -------------------------

impl<const N: usize> ArrowBinding for [u8; N] {
    type Builder = FixedSizeBinaryBuilder;

    type Array = arrow_array::FixedSizeBinaryArray;

    fn data_type() -> DataType {
        DataType::FixedSizeBinary(N as i32)
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        // Capacity is a hint; builder requires element width (N)
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

// -------------------------
// LargeBinary: wrapper around Vec<u8>
// -------------------------

/// Wrapper denoting Arrow `LargeBinary` values. Use when individual binary values
/// can exceed 2GB or when 64-bit offsets are preferred.
pub struct LargeBinary(pub Vec<u8>);

impl ArrowBinding for LargeBinary {
    type Builder = LargeBinaryBuilder;

    type Array = arrow_array::LargeBinaryArray;

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

// -------------------------
// Decimal128<P, S> and Decimal256<P, S>
// -------------------------

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

// -------------------------
// Nested: List<T>
// -------------------------

/// Wrapper denoting an Arrow `ListArray` column with elements of `T`.
///
/// Notes:
/// - List-level nullability: wrap the column in `Option<List<T>>`.
/// - Item-level nullability: use `List<Option<T>>` when elements can be null.
/// - This avoids conflict with `Vec<u8>` which maps to `Binary`. We may transition to
///   `Vec<T>`/`Vec<Option<T>>` mapping in the future.
///
/// Example
/// ```no_run
/// use arrow_array::Array;
/// use typed_arrow::{bridge::ArrowBinding, List};
/// let mut b = <List<i32> as ArrowBinding>::new_builder(1);
/// <List<i32> as ArrowBinding>::append_value(&mut b, &List(vec![1, 2, 3]));
/// let a = <List<i32> as ArrowBinding>::finish(b);
/// assert_eq!(a.len(), 1);
/// ```
pub struct List<T>(pub Vec<T>);

impl<T> ArrowBinding for List<T>
where
    T: ArrowBinding,
    <T as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = ListBuilder<<T as ArrowBinding>::Builder>;

    type Array = arrow_array::ListArray;

    fn data_type() -> DataType {
        DataType::List(Field::new("item", <T as ArrowBinding>::data_type(), false).into())
    }

    fn new_builder(_capacity: usize) -> Self::Builder {
        let child = <T as ArrowBinding>::new_builder(0);
        ListBuilder::new(child)
    }

    fn append_value(b: &mut Self::Builder, v: &Self) {
        for it in &v.0 {
            <T as ArrowBinding>::append_value(b.values(), it);
        }
        b.append(true);
    }

    fn append_null(b: &mut Self::Builder) {
        b.append(false);
    }

    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

/// Provide ArrowBinding for `List<Option<T>>` so users can express
/// item-nullability via `Option` in the type parameter, avoiding a separate wrapper.
impl<T> ArrowBinding for List<Option<T>>
where
    T: ArrowBinding,
    <T as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = ListBuilder<<T as ArrowBinding>::Builder>;

    type Array = arrow_array::ListArray;

    fn data_type() -> DataType {
        DataType::List(Field::new("item", <T as ArrowBinding>::data_type(), true).into())
    }

    fn new_builder(_capacity: usize) -> Self::Builder {
        let child = <T as ArrowBinding>::new_builder(0);
        ListBuilder::new(child)
    }

    fn append_value(b: &mut Self::Builder, v: &Self) {
        for it in &v.0 {
            match it {
                Some(inner) => <T as ArrowBinding>::append_value(b.values(), inner),
                None => <T as ArrowBinding>::append_null(b.values()),
            }
        }
        b.append(true);
    }

    fn append_null(b: &mut Self::Builder) {
        b.append(false);
    }

    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// -------------------------
// FixedSizeList<T, N>
// -------------------------

/// Wrapper denoting an Arrow `FixedSizeListArray` column with `N` elements of `T`.
///
/// - List-level nullability: wrap the column in `Option<FixedSizeList<_, N>>`.
/// - Item-level non-nullability: child field is non-nullable; use `FixedSizeListNullable` for
///   nullable items.
pub struct FixedSizeList<T, const N: usize>(pub [T; N]);

impl<T, const N: usize> ArrowBinding for FixedSizeList<T, N>
where
    T: ArrowBinding,
    <T as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = arrow_array::builder::FixedSizeListBuilder<<T as ArrowBinding>::Builder>;

    type Array = arrow_array::FixedSizeListArray;

    fn data_type() -> DataType {
        DataType::FixedSizeList(
            Field::new("item", <T as ArrowBinding>::data_type(), false).into(),
            N as i32,
        )
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        let child = <T as ArrowBinding>::new_builder(0);
        arrow_array::builder::FixedSizeListBuilder::with_capacity(child, N as i32, capacity)
            .with_field(Field::new("item", <T as ArrowBinding>::data_type(), false))
    }

    fn append_value(b: &mut Self::Builder, v: &Self) {
        for it in &v.0 {
            <T as ArrowBinding>::append_value(b.values(), it);
        }
        b.append(true);
    }

    fn append_null(b: &mut Self::Builder) {
        for _ in 0..N {
            <T as ArrowBinding>::append_null(b.values());
        }
        b.append(false);
    }

    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

/// Wrapper denoting a `FixedSizeListArray` with `N` elements where items are nullable.
pub struct FixedSizeListNullable<T, const N: usize>(pub [Option<T>; N]);

impl<T, const N: usize> ArrowBinding for FixedSizeListNullable<T, N>
where
    T: ArrowBinding,
    <T as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = arrow_array::builder::FixedSizeListBuilder<<T as ArrowBinding>::Builder>;

    type Array = arrow_array::FixedSizeListArray;

    fn data_type() -> DataType {
        DataType::FixedSizeList(
            Field::new("item", <T as ArrowBinding>::data_type(), true).into(),
            N as i32,
        )
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        let child = <T as ArrowBinding>::new_builder(0);
        arrow_array::builder::FixedSizeListBuilder::with_capacity(child, N as i32, capacity)
            .with_field(Field::new("item", <T as ArrowBinding>::data_type(), true))
    }

    fn append_value(b: &mut Self::Builder, v: &Self) {
        for it in &v.0 {
            match it {
                Some(inner) => <T as ArrowBinding>::append_value(b.values(), inner),
                None => <T as ArrowBinding>::append_null(b.values()),
            }
        }
        b.append(true);
    }

    fn append_null(b: &mut Self::Builder) {
        for _ in 0..N {
            <T as ArrowBinding>::append_null(b.values());
        }
        b.append(false);
    }

    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// -------------------------
// Map<K, V, const SORTED: bool>
// -------------------------

/// Wrapper denoting an Arrow `MapArray` column with entries `(K, V)`.
///
/// - Keys are non-nullable by Arrow spec.
/// - Values are non-nullable for `Map<K, V, SORTED>` and nullable for [`MapNullable<K, V,
///   SORTED>`].
/// - Column-level nullability is expressed with `Option<Map<...>>`.
pub struct Map<K, V, const SORTED: bool = false>(pub Vec<(K, V)>);

impl<K, V, const SORTED: bool> ArrowBinding for Map<K, V, SORTED>
where
    K: ArrowBinding,
    V: ArrowBinding,
    <K as ArrowBinding>::Builder: ArrayBuilder,
    <V as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = MapBuilder<<K as ArrowBinding>::Builder, <V as ArrowBinding>::Builder>;
    type Array = MapArray;

    fn data_type() -> DataType {
        let key_f = Field::new("keys", <K as ArrowBinding>::data_type(), false);
        // MapBuilder uses field name `values` and constructs a nullable value field; align our
        // DataType accordingly
        let val_f = Field::new("values", <V as ArrowBinding>::data_type(), true);
        let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
        DataType::Map(Field::new("entries", entries, false).into(), SORTED)
    }

    fn new_builder(_capacity: usize) -> Self::Builder {
        let kb = <K as ArrowBinding>::new_builder(0);
        let vb = <V as ArrowBinding>::new_builder(0);
        // Use default field names `keys`/`values` matching MapBuilder
        MapBuilder::new(None, kb, vb)
    }

    fn append_value(b: &mut Self::Builder, v: &Self) {
        for (k, val) in &v.0 {
            <K as ArrowBinding>::append_value(b.keys(), k);
            <V as ArrowBinding>::append_value(b.values(), val);
        }
        let _ = b.append(true);
    }

    fn append_null(b: &mut Self::Builder) {
        let _ = b.append(false);
    }

    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// Provide ArrowBinding for Map<K, Option<V>, SORTED> so users can express
// value-nullability via Option in the type parameter, avoiding a separate wrapper.
impl<K, V, const SORTED: bool> ArrowBinding for Map<K, Option<V>, SORTED>
where
    K: ArrowBinding,
    V: ArrowBinding,
    <K as ArrowBinding>::Builder: ArrayBuilder,
    <V as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = MapBuilder<<K as ArrowBinding>::Builder, <V as ArrowBinding>::Builder>;
    type Array = MapArray;

    fn data_type() -> DataType {
        let key_f = Field::new("keys", <K as ArrowBinding>::data_type(), false);
        let val_f = Field::new("values", <V as ArrowBinding>::data_type(), true);
        let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
        DataType::Map(Field::new("entries", entries, false).into(), SORTED)
    }

    fn new_builder(_capacity: usize) -> Self::Builder {
        let kb = <K as ArrowBinding>::new_builder(0);
        let vb = <V as ArrowBinding>::new_builder(0);
        MapBuilder::new(None, kb, vb)
    }

    fn append_value(b: &mut Self::Builder, v: &Self) {
        for (k, val_opt) in &v.0 {
            <K as ArrowBinding>::append_value(b.keys(), k);
            match val_opt {
                Some(val) => <V as ArrowBinding>::append_value(b.values(), val),
                None => <V as ArrowBinding>::append_null(b.values()),
            }
        }
        let _ = b.append(true);
    }

    fn append_null(b: &mut Self::Builder) {
        let _ = b.append(false);
    }

    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// -------------------------
// OrderedMap<K, V> and OrderedMapNullable<K, V>
// -------------------------

/// Sorted-keys Map: entries sourced from `BTreeMap<K, V>`, declaring `keys_sorted = true`.
/// Keys are non-nullable; the value field is nullable per MapBuilder semantics, but this
/// wrapper does not write null values.
pub struct OrderedMap<K, V>(pub BTreeMap<K, V>);

impl<K, V> ArrowBinding for OrderedMap<K, V>
where
    K: ArrowBinding + Ord,
    V: ArrowBinding,
    <K as ArrowBinding>::Builder: ArrayBuilder,
    <V as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = MapBuilder<<K as ArrowBinding>::Builder, <V as ArrowBinding>::Builder>;
    type Array = MapArray;

    fn data_type() -> DataType {
        let key_f = Field::new("keys", <K as ArrowBinding>::data_type(), false);
        let val_f = Field::new("values", <V as ArrowBinding>::data_type(), true);
        let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
        DataType::Map(Field::new("entries", entries, false).into(), true)
    }

    fn new_builder(_capacity: usize) -> Self::Builder {
        let kb = <K as ArrowBinding>::new_builder(0);
        let vb = <V as ArrowBinding>::new_builder(0);
        MapBuilder::new(None, kb, vb)
    }

    fn append_value(b: &mut Self::Builder, v: &Self) {
        for (k, val) in v.0.iter() {
            <K as ArrowBinding>::append_value(b.keys(), k);
            <V as ArrowBinding>::append_value(b.values(), val);
        }
        let _ = b.append(true);
    }

    fn append_null(b: &mut Self::Builder) {
        let _ = b.append(false);
    }

    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// Provide ArrowBinding for OrderedMap<K, Option<V>> mirroring the non-wrapper variant
impl<K, V> ArrowBinding for OrderedMap<K, Option<V>>
where
    K: ArrowBinding + Ord,
    V: ArrowBinding,
    <K as ArrowBinding>::Builder: ArrayBuilder,
    <V as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = MapBuilder<<K as ArrowBinding>::Builder, <V as ArrowBinding>::Builder>;
    type Array = MapArray;

    fn data_type() -> DataType {
        let key_f = Field::new("keys", <K as ArrowBinding>::data_type(), false);
        let val_f = Field::new("values", <V as ArrowBinding>::data_type(), true);
        let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
        DataType::Map(Field::new("entries", entries, false).into(), true)
    }

    fn new_builder(_capacity: usize) -> Self::Builder {
        let kb = <K as ArrowBinding>::new_builder(0);
        let vb = <V as ArrowBinding>::new_builder(0);
        MapBuilder::new(None, kb, vb)
    }

    fn append_value(b: &mut Self::Builder, v: &Self) {
        for (k, val_opt) in v.0.iter() {
            <K as ArrowBinding>::append_value(b.keys(), k);
            match val_opt {
                Some(val) => <V as ArrowBinding>::append_value(b.values(), val),
                None => <V as ArrowBinding>::append_null(b.values()),
            }
        }
        let _ = b.append(true);
    }

    fn append_null(b: &mut Self::Builder) {
        let _ = b.append(false);
    }

    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// Removed ListNullable<T>; use List<Option<T>> instead.

// -------------------------
// LargeList<T>
// -------------------------

/// Wrapper denoting an Arrow `LargeListArray` column with elements of `T`.
pub struct LargeList<T>(pub Vec<T>);

impl<T> ArrowBinding for LargeList<T>
where
    T: ArrowBinding,
    <T as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = LargeListBuilder<<T as ArrowBinding>::Builder>;

    type Array = arrow_array::LargeListArray;

    fn data_type() -> DataType {
        DataType::LargeList(Field::new("item", <T as ArrowBinding>::data_type(), false).into())
    }

    fn new_builder(_capacity: usize) -> Self::Builder {
        let child = <T as ArrowBinding>::new_builder(0);
        LargeListBuilder::new(child)
    }

    fn append_value(b: &mut Self::Builder, v: &Self) {
        for it in &v.0 {
            <T as ArrowBinding>::append_value(b.values(), it);
        }
        b.append(true);
    }

    fn append_null(b: &mut Self::Builder) {
        b.append(false);
    }

    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

/// Provide ArrowBinding for `LargeList<Option<T>>` so users can express
/// item-nullability via `Option` in the type parameter for LargeList.
impl<T> ArrowBinding for LargeList<Option<T>>
where
    T: ArrowBinding,
    <T as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = LargeListBuilder<<T as ArrowBinding>::Builder>;

    type Array = arrow_array::LargeListArray;

    fn data_type() -> DataType {
        DataType::LargeList(Field::new("item", <T as ArrowBinding>::data_type(), true).into())
    }

    fn new_builder(_capacity: usize) -> Self::Builder {
        let child = <T as ArrowBinding>::new_builder(0);
        LargeListBuilder::new(child)
    }

    fn append_value(b: &mut Self::Builder, v: &Self) {
        for it in &v.0 {
            match it {
                Some(inner) => <T as ArrowBinding>::append_value(b.values(), inner),
                None => <T as ArrowBinding>::append_null(b.values()),
            }
        }
        b.append(true);
    }

    fn append_null(b: &mut Self::Builder) {
        b.append(false);
    }

    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// Removed LargeListNullable<T>; use LargeList<Option<T>> instead.

// -------------------------
// Dictionary<K, V>
// -------------------------

/// Wrapper denoting an Arrow Dictionary column with key type `K` and values of `V`.
///
/// Currently value support focuses on `V = String` (Utf8) via
/// `StringDictionaryBuilder`. Column nullability is expressed with
/// `Option<Dictionary<K, V>>`.
///
/// Example
/// ```no_run
/// use arrow_array::Array;
/// use typed_arrow::{bridge::ArrowBinding, Dictionary};
/// let mut b = <Dictionary<i32, String> as ArrowBinding>::new_builder(0);
/// <Dictionary<i32, String> as ArrowBinding>::append_value(
///     &mut b,
///     &Dictionary("hello".to_string(), std::marker::PhantomData),
/// );
/// let a = <Dictionary<i32, String> as ArrowBinding>::finish(b);
/// assert_eq!(a.len(), 1);
/// ```
pub struct Dictionary<K, V>(pub V, pub PhantomData<K>);

/// Dictionary key mapping from Rust integer to Arrow key type.
pub trait DictKey {
    /// Arrow key type corresponding to this Rust integer key.
    type ArrowKey;

    /// The Arrow `DataType` for the key.
    fn data_type() -> DataType;
}

// -------------------------
// Timestamp<U> (unit only, no timezone)
// -------------------------

/// Marker describing a timestamp unit.
pub trait TimeUnitSpec {
    /// Typed Arrow timestamp marker for this unit.
    type Arrow: ArrowTimestampType;

    /// The `arrow_schema::TimeUnit` of this marker.
    fn unit() -> TimeUnit;
}

/// Seconds since epoch.
pub enum Second {}

impl TimeUnitSpec for Second {
    type Arrow = TimestampSecondType;
    fn unit() -> TimeUnit {
        TimeUnit::Second
    }
}

/// Milliseconds since epoch.
pub enum Millisecond {}

impl TimeUnitSpec for Millisecond {
    type Arrow = TimestampMillisecondType;
    fn unit() -> TimeUnit {
        TimeUnit::Millisecond
    }
}

/// Microseconds since epoch.
pub enum Microsecond {}

impl TimeUnitSpec for Microsecond {
    type Arrow = TimestampMicrosecondType;

    fn unit() -> TimeUnit {
        TimeUnit::Microsecond
    }
}

/// Nanoseconds since epoch.
pub enum Nanosecond {}
impl TimeUnitSpec for Nanosecond {
    type Arrow = TimestampNanosecondType;

    fn unit() -> TimeUnit {
        TimeUnit::Nanosecond
    }
}

/// Timestamp value (unit only, timezone = None).
///
/// The unit is encoded at the type level via `U: TimeUnitSpec`, the value is an
/// `i64` epoch count.
///
/// Example
/// ```no_run
/// use arrow_array::Array;
/// use typed_arrow::{bridge::ArrowBinding, Millisecond, Timestamp};
/// let mut b = <Timestamp<Millisecond> as ArrowBinding>::new_builder(2);
/// <Timestamp<Millisecond> as ArrowBinding>::append_value(
///     &mut b,
///     &Timestamp(1_000, std::marker::PhantomData),
/// );
/// <Timestamp<Millisecond> as ArrowBinding>::append_null(&mut b);
/// let a = <Timestamp<Millisecond> as ArrowBinding>::finish(b);
/// assert_eq!(a.len(), 2);
/// ```
pub struct Timestamp<U: TimeUnitSpec>(pub i64, pub PhantomData<U>);

impl<U: TimeUnitSpec> ArrowBinding for Timestamp<U> {
    type Builder = PrimitiveBuilder<U::Arrow>;

    type Array = PrimitiveArray<U::Arrow>;

    fn data_type() -> DataType {
        DataType::Timestamp(U::unit(), None)
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        PrimitiveBuilder::<U::Arrow>::with_capacity(capacity)
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

// -------------------------
// TimestampTz<U, Z> (unit + timezone)
// -------------------------

/// Marker describing a timestamp timezone.
///
/// Implement this for your own unit types to encode a named timezone at the
/// type level (e.g. "UTC" or an IANA name like "Asia/Shanghai").
pub trait TimeZoneSpec {
    /// The optional timezone name for this marker.
    const NAME: Option<&'static str>;
}

/// UTC timezone marker.
pub enum Utc {}
impl TimeZoneSpec for Utc {
    const NAME: Option<&'static str> = Some("UTC");
}

/// Timestamp with time unit `U` and timezone marker `Z`.
///
/// The timezone name is embedded at the type level via `Z: TimeZoneSpec`.
///
/// Example
/// ```no_run
/// use arrow_array::Array;
/// use typed_arrow::{bridge::ArrowBinding, Millisecond, TimestampTz, Utc};
/// let mut b = <TimestampTz<Millisecond, Utc> as ArrowBinding>::new_builder(2);
/// <TimestampTz<Millisecond, Utc> as ArrowBinding>::append_value(
///     &mut b,
///     &TimestampTz::<Millisecond, Utc>(1_000, std::marker::PhantomData),
/// );
/// let a = <TimestampTz<Millisecond, Utc> as ArrowBinding>::finish(b);
/// assert_eq!(a.len(), 1);
/// ```
pub struct TimestampTz<U: TimeUnitSpec, Z: TimeZoneSpec>(pub i64, pub PhantomData<(U, Z)>);

impl<U: TimeUnitSpec, Z: TimeZoneSpec> ArrowBinding for TimestampTz<U, Z> {
    type Builder = PrimitiveBuilder<U::Arrow>;
    type Array = PrimitiveArray<U::Arrow>;

    fn data_type() -> DataType {
        DataType::Timestamp(U::unit(), Z::NAME.map(Arc::<str>::from))
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        PrimitiveBuilder::<U::Arrow>::with_capacity(capacity)
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

// -------------------------
// Date32 / Date64
// -------------------------

/// Days since UNIX epoch.
pub struct Date32(pub i32);

impl ArrowBinding for Date32 {
    type Builder = PrimitiveBuilder<Date32Type>;

    type Array = PrimitiveArray<Date32Type>;

    fn data_type() -> DataType {
        DataType::Date32
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        PrimitiveBuilder::<Date32Type>::with_capacity(capacity)
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

/// Milliseconds since UNIX epoch.
pub struct Date64(pub i64);

impl ArrowBinding for Date64 {
    type Builder = PrimitiveBuilder<Date64Type>;

    type Array = PrimitiveArray<Date64Type>;

    fn data_type() -> DataType {
        DataType::Date64
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        PrimitiveBuilder::<Date64Type>::with_capacity(capacity)
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

// -------------------------
// Time32<U> and Time64<U>
// -------------------------

/// Marker mapping for `Time32` units.
/// Marker trait mapping `Time32` units to Arrow time types.
pub trait Time32UnitSpec {
    /// Arrow type for this time unit (`Time32SecondType` or `Time32MillisecondType`).
    type Arrow;

    /// The `arrow_schema::TimeUnit` variant for this unit.
    fn unit() -> TimeUnit;
}

impl Time32UnitSpec for Second {
    type Arrow = Time32SecondType;

    fn unit() -> TimeUnit {
        TimeUnit::Second
    }
}

impl Time32UnitSpec for Millisecond {
    type Arrow = Time32MillisecondType;

    fn unit() -> TimeUnit {
        TimeUnit::Millisecond
    }
}

/// Number of seconds/milliseconds since midnight.
pub struct Time32<U: Time32UnitSpec>(pub i32, pub PhantomData<U>);

impl<U: Time32UnitSpec> ArrowBinding for Time32<U>
where
    U::Arrow: ArrowPrimitiveType<Native = i32>,
{
    type Builder = PrimitiveBuilder<U::Arrow>;

    type Array = PrimitiveArray<U::Arrow>;

    fn data_type() -> DataType {
        DataType::Time32(U::unit())
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        PrimitiveBuilder::<U::Arrow>::with_capacity(capacity)
    }

    fn append_value(b: &mut Self::Builder, v: &Self) {
        b.append_value(v.0 as <U::Arrow as ArrowPrimitiveType>::Native);
    }

    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }

    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

/// Marker mapping for `Time64` units.
/// Marker trait mapping `Time64` units to Arrow time types.
pub trait Time64UnitSpec {
    /// Arrow type for this time unit (`Time64MicrosecondType` or `Time64NanosecondType`).
    type Arrow;

    /// The `arrow_schema::TimeUnit` variant for this unit.
    fn unit() -> TimeUnit;
}

impl Time64UnitSpec for Microsecond {
    type Arrow = Time64MicrosecondType;

    fn unit() -> TimeUnit {
        TimeUnit::Microsecond
    }
}

impl Time64UnitSpec for Nanosecond {
    type Arrow = Time64NanosecondType;

    fn unit() -> TimeUnit {
        TimeUnit::Nanosecond
    }
}

/// Number of microseconds/nanoseconds since midnight.
pub struct Time64<U: Time64UnitSpec>(pub i64, pub PhantomData<U>);

impl<U: Time64UnitSpec> ArrowBinding for Time64<U>
where
    U::Arrow: ArrowPrimitiveType<Native = i64>,
{
    type Builder = PrimitiveBuilder<U::Arrow>;

    type Array = PrimitiveArray<U::Arrow>;

    fn data_type() -> DataType {
        DataType::Time64(U::unit())
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        PrimitiveBuilder::<U::Arrow>::with_capacity(capacity)
    }

    fn append_value(b: &mut Self::Builder, v: &Self) {
        b.append_value(v.0 as <U::Arrow as ArrowPrimitiveType>::Native);
    }

    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }

    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// -------------------------
// Duration<U>
// -------------------------

/// Marker mapping for `Duration` units.
/// Marker trait mapping `Duration` units to Arrow duration types.
pub trait DurationUnitSpec {
    /// Arrow type for this duration unit (`Duration*Type`).
    type Arrow;

    /// The `arrow_schema::TimeUnit` variant for this unit.
    fn unit() -> TimeUnit;
}

impl DurationUnitSpec for Second {
    type Arrow = DurationSecondType;

    fn unit() -> TimeUnit {
        TimeUnit::Second
    }
}

impl DurationUnitSpec for Millisecond {
    type Arrow = DurationMillisecondType;

    fn unit() -> TimeUnit {
        TimeUnit::Millisecond
    }
}

impl DurationUnitSpec for Microsecond {
    type Arrow = DurationMicrosecondType;

    fn unit() -> TimeUnit {
        TimeUnit::Microsecond
    }
}

impl DurationUnitSpec for Nanosecond {
    type Arrow = DurationNanosecondType;

    fn unit() -> TimeUnit {
        TimeUnit::Nanosecond
    }
}

/// Duration in the given unit.
pub struct Duration<U: DurationUnitSpec>(pub i64, pub PhantomData<U>);

impl<U: DurationUnitSpec> ArrowBinding for Duration<U>
where
    U::Arrow: ArrowPrimitiveType<Native = i64>,
{
    type Builder = PrimitiveBuilder<U::Arrow>;

    type Array = PrimitiveArray<U::Arrow>;

    fn data_type() -> DataType {
        DataType::Duration(U::unit())
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        PrimitiveBuilder::<U::Arrow>::with_capacity(capacity)
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

// -------------------------
// Struct<T: Record + StructMeta>
// -------------------------
// Any `T` implementing `Record + StructMeta` automatically binds to a typed
// Arrow `StructArray`, with a `StructBuilder` produced by `new_builder()`. The
// `DataType::Struct` is assembled from `StructMeta::child_fields()`.

impl<T> ArrowBinding for T
where
    T: Record + StructMeta,
{
    type Builder = StructBuilder;

    type Array = arrow_array::StructArray;

    fn data_type() -> DataType {
        use std::sync::Arc;
        let fields = <T as StructMeta>::child_fields()
            .into_iter()
            .map(Arc::new)
            .collect();
        DataType::Struct(fields)
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        <T as StructMeta>::new_struct_builder(capacity)
    }

    fn append_value(b: &mut Self::Builder, _v: &Self) {
        // The typical pattern is: append child values, then mark presence here.
        b.append(true);
    }

    fn append_null(b: &mut Self::Builder) {
        b.append(false);
    }

    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// -------------------------
// Interval (YearMonth, DayTime, MonthDayNano)
// -------------------------

/// Interval with unit YearMonth (i32 months since epoch).
pub struct IntervalYearMonth(pub i32);

impl ArrowBinding for IntervalYearMonth {
    type Builder = PrimitiveBuilder<IntervalYearMonthType>;

    type Array = PrimitiveArray<IntervalYearMonthType>;

    fn data_type() -> DataType {
        DataType::Interval(IntervalUnit::YearMonth)
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        PrimitiveBuilder::<IntervalYearMonthType>::with_capacity(capacity)
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

/// Interval with unit DayTime (packed days and milliseconds).
pub struct IntervalDayTime(pub IntervalDayTimeNative);

impl ArrowBinding for IntervalDayTime {
    type Builder = PrimitiveBuilder<IntervalDayTimeType>;

    type Array = PrimitiveArray<IntervalDayTimeType>;

    fn data_type() -> DataType {
        DataType::Interval(IntervalUnit::DayTime)
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        PrimitiveBuilder::<IntervalDayTimeType>::with_capacity(capacity)
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

/// Interval with unit MonthDayNano (packed months, days, and nanoseconds).
pub struct IntervalMonthDayNano(pub IntervalMonthDayNanoNative);

impl ArrowBinding for IntervalMonthDayNano {
    type Builder = PrimitiveBuilder<IntervalMonthDayNanoType>;

    type Array = PrimitiveArray<IntervalMonthDayNanoType>;

    fn data_type() -> DataType {
        DataType::Interval(IntervalUnit::MonthDayNano)
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        PrimitiveBuilder::<IntervalMonthDayNanoType>::with_capacity(capacity)
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

// Provide binding for Dictionary<K, String> using StringDictionaryBuilder
impl<K> ArrowBinding for Dictionary<K, String>
where
    K: DictKey,
    <K as DictKey>::ArrowKey: ArrowDictionaryKeyType,
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
        // capacity hint ignored; builder manages its dictionary table.
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

// Provide binding for Dictionary<K, Vec<u8>> (Binary) using BinaryDictionaryBuilder
impl<K> ArrowBinding for Dictionary<K, Vec<u8>>
where
    K: DictKey,
    <K as DictKey>::ArrowKey: ArrowDictionaryKeyType,
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

// Provide binding for Dictionary<K, [u8; N]> (FixedSizeBinary) using
// FixedSizeBinaryDictionaryBuilder
impl<K, const N: usize> ArrowBinding for Dictionary<K, [u8; N]>
where
    K: DictKey,
    <K as DictKey>::ArrowKey: ArrowDictionaryKeyType,
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
        FixedSizeBinaryDictionaryBuilder::new(N as i32)
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

// Provide binding for Dictionary<K, LargeBinary> (LargeBinary) using LargeBinaryDictionaryBuilder
impl<K> ArrowBinding for Dictionary<K, LargeBinary>
where
    K: DictKey,
    <K as DictKey>::ArrowKey: ArrowDictionaryKeyType,
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

// Provide binding for Dictionary<K, LargeUtf8> using LargeStringDictionaryBuilder
impl<K> ArrowBinding for Dictionary<K, LargeUtf8>
where
    K: DictKey,
    <K as DictKey>::ArrowKey: ArrowDictionaryKeyType,
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

macro_rules! impl_dict_primitive_value {
    ($rust:ty, $atype:ty, $dt:expr) => {
        impl<K> ArrowBinding for Dictionary<K, $rust>
        where
            K: DictKey,
            <K as DictKey>::ArrowKey: ArrowDictionaryKeyType,
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

/// Returns the Arrow `DataType` for column `I` of record `R`.
///
/// Convenience helper equivalent to
/// `<<R as ColAt<I>>::Rust as ArrowBinding>::data_type()`.
///
/// Example
/// ```no_run
/// use typed_arrow::{bridge, prelude::*};
/// #[derive(typed_arrow::Record)]
/// struct S {
///     a: i64,
/// }
/// assert_eq!(
///     bridge::data_type_of::<S, 0>(),
///     arrow_schema::DataType::Int64
/// );
/// ```
pub fn data_type_of<R: Record + ColAt<I>, const I: usize>() -> DataType
where
    <R as ColAt<I>>::Rust: ArrowBinding,
{
    <<R as ColAt<I>>::Rust as ArrowBinding>::data_type()
}

/// A typed column builder for column `I` of record `R`.
///
/// This generic builder wraps the concrete builder from `ArrowBinding` and
/// provides a uniform API across columns selected by index `I`.
///
/// Example
/// ```no_run
/// use typed_arrow::{bridge::ColumnBuilder, prelude::*};
/// #[derive(typed_arrow::Record)]
/// struct S {
///     a: i64,
/// }
/// let mut b: ColumnBuilder<S, 0> = ColumnBuilder::with_capacity(2);
/// b.append_value(&1);
/// b.append_option(None);
/// let arr = b.finish();
/// assert_eq!(arr.len(), 2);
/// ```
pub struct ColumnBuilder<R: Record + ColAt<I>, const I: usize>
where
    <R as ColAt<I>>::Rust: ArrowBinding,
{
    inner: <<R as ColAt<I>>::Rust as ArrowBinding>::Builder,
    _pd: PhantomData<R>,
}

impl<R: Record + ColAt<I>, const I: usize> ColumnBuilder<R, I>
where
    <R as ColAt<I>>::Rust: ArrowBinding,
{
    /// Create a builder with `capacity`.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: <<R as ColAt<I>>::Rust as ArrowBinding>::new_builder(capacity),
            _pd: PhantomData,
        }
    }

    /// Append a value.
    pub fn append_value(&mut self, v: &<R as ColAt<I>>::Rust) {
        <<R as ColAt<I>>::Rust as ArrowBinding>::append_value(&mut self.inner, v)
    }

    /// Append an optional value; `None` appends a null.
    pub fn append_option(&mut self, v: Option<&<R as ColAt<I>>::Rust>) {
        match v {
            Some(x) => self.append_value(x),
            None => <<R as ColAt<I>>::Rust as ArrowBinding>::append_null(&mut self.inner),
        }
    }

    /// Finish and produce the typed Arrow array for this column.
    pub fn finish(self) -> <<R as ColAt<I>>::Rust as ArrowBinding>::Array {
        <<R as ColAt<I>>::Rust as ArrowBinding>::finish(self.inner)
    }
}
