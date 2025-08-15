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
//!   - [`List<T>`] / [`ListNullable<T>`] → `ListArray` with non-null/nullable items.
//!   - [`Dictionary<K, String>`] → dictionary-encoded Utf8 values.
//!   - [`Timestamp<U>`] with unit markers ([`Second`], [`Millisecond`], [`Microsecond`],
//!     [`Nanosecond`]).
//!   - Any `T: Record + StructMeta` binds to an Arrow `StructArray`.
//!
//! See tests for end-to-end examples and usage patterns.

use std::marker::PhantomData;

use arrow_array::{
    builder::{BinaryBuilder, BooleanBuilder, PrimitiveBuilder, StringBuilder},
    types::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
        UInt32Type, UInt64Type, UInt8Type,
    },
    Array, PrimitiveArray, StringArray,
};
use arrow_schema::DataType;

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
/// use arrow_native::bridge::ArrowBinding;
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
// Nested: List<T>
// -------------------------

/// Wrapper denoting an Arrow `ListArray` column with elements of `T`.
///
/// Notes:
/// - List-level nullability: wrap the column in `Option<List<T>>`.
/// - Item-level nullability: use [`ListNullable<T>`] when elements can be null.
/// - This avoids conflict with `Vec<u8>` which maps to `Binary`. We may transition to
///   `Vec<T>`/`Vec<Option<T>>` mapping in the future.
///
/// Example
/// ```no_run
/// use arrow_array::Array;
/// use arrow_native::{bridge::ArrowBinding, List};
/// let mut b = <List<i32> as ArrowBinding>::new_builder(1);
/// <List<i32> as ArrowBinding>::append_value(&mut b, &List(vec![1, 2, 3]));
/// let a = <List<i32> as ArrowBinding>::finish(b);
/// assert_eq!(a.len(), 1);
/// ```
pub struct List<T>(pub Vec<T>);

impl<T> ArrowBinding for List<T>
where
    T: ArrowBinding,
    <T as ArrowBinding>::Builder: arrow_array::builder::ArrayBuilder,
{
    type Builder = arrow_array::builder::ListBuilder<<T as ArrowBinding>::Builder>;

    type Array = arrow_array::ListArray;

    fn data_type() -> DataType {
        DataType::List(
            arrow_schema::Field::new("item", <T as ArrowBinding>::data_type(), false).into(),
        )
    }

    fn new_builder(_capacity: usize) -> Self::Builder {
        let child = <T as ArrowBinding>::new_builder(0);
        arrow_array::builder::ListBuilder::new(child)
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

/// Wrapper denoting a `ListArray` whose items are nullable.
///
/// Example
/// ```no_run
/// use arrow_array::Array;
/// use arrow_native::bridge::{ArrowBinding, ListNullable};
/// let mut b = <ListNullable<i32> as ArrowBinding>::new_builder(1);
/// <ListNullable<i32> as ArrowBinding>::append_value(&mut b, &ListNullable(vec![Some(1), None]));
/// let a = <ListNullable<i32> as ArrowBinding>::finish(b);
/// assert_eq!(a.len(), 1);
/// ```
pub struct ListNullable<T>(pub Vec<Option<T>>);

impl<T> ArrowBinding for ListNullable<T>
where
    T: ArrowBinding,
    <T as ArrowBinding>::Builder: arrow_array::builder::ArrayBuilder,
{
    type Builder = arrow_array::builder::ListBuilder<<T as ArrowBinding>::Builder>;

    type Array = arrow_array::ListArray;

    fn data_type() -> DataType {
        DataType::List(
            arrow_schema::Field::new("item", <T as ArrowBinding>::data_type(), true).into(),
        )
    }

    fn new_builder(_capacity: usize) -> Self::Builder {
        let child = <T as ArrowBinding>::new_builder(0);
        arrow_array::builder::ListBuilder::new(child)
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
/// use arrow_native::{bridge::ArrowBinding, Dictionary};
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
    type Arrow: arrow_array::types::ArrowTimestampType;

    /// The `arrow_schema::TimeUnit` of this marker.
    fn unit() -> arrow_schema::TimeUnit;
}

/// Seconds since epoch.
pub enum Second {}

impl TimeUnitSpec for Second {
    type Arrow = arrow_array::types::TimestampSecondType;
    fn unit() -> arrow_schema::TimeUnit {
        arrow_schema::TimeUnit::Second
    }
}

/// Milliseconds since epoch.
pub enum Millisecond {}

impl TimeUnitSpec for Millisecond {
    type Arrow = arrow_array::types::TimestampMillisecondType;
    fn unit() -> arrow_schema::TimeUnit {
        arrow_schema::TimeUnit::Millisecond
    }
}

/// Microseconds since epoch.
pub enum Microsecond {}

impl TimeUnitSpec for Microsecond {
    type Arrow = arrow_array::types::TimestampMicrosecondType;

    fn unit() -> arrow_schema::TimeUnit {
        arrow_schema::TimeUnit::Microsecond
    }
}

/// Nanoseconds since epoch.
pub enum Nanosecond {}
impl TimeUnitSpec for Nanosecond {
    type Arrow = arrow_array::types::TimestampNanosecondType;

    fn unit() -> arrow_schema::TimeUnit {
        arrow_schema::TimeUnit::Nanosecond
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
/// use arrow_native::{bridge::ArrowBinding, Millisecond, Timestamp};
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
// Struct<T: Record + StructMeta>
// -------------------------
// Any `T` implementing `Record + StructMeta` automatically binds to a typed
// Arrow `StructArray`, with a `StructBuilder` produced by `new_builder()`. The
// `DataType::Struct` is assembled from `StructMeta::child_fields()`.

impl<T> ArrowBinding for T
where
    T: Record + StructMeta,
{
    type Builder = arrow_array::builder::StructBuilder;

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
    <K as DictKey>::ArrowKey: arrow_array::types::ArrowDictionaryKeyType,
{
    type Builder = arrow_array::builder::StringDictionaryBuilder<<K as DictKey>::ArrowKey>;

    type Array = arrow_array::DictionaryArray<<K as DictKey>::ArrowKey>;

    fn data_type() -> DataType {
        DataType::Dictionary(
            Box::new(<K as DictKey>::data_type()),
            Box::new(DataType::Utf8),
        )
    }

    fn new_builder(_capacity: usize) -> Self::Builder {
        // capacity hint ignored; builder manages its dictionary table.
        arrow_array::builder::StringDictionaryBuilder::new()
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

/// Returns the Arrow `DataType` for column `I` of record `R`.
///
/// Convenience helper equivalent to
/// `<<R as ColAt<I>>::Rust as ArrowBinding>::data_type()`.
///
/// Example
/// ```no_run
/// use arrow_native::{bridge, prelude::*};
/// #[derive(arrow_native::Record)]
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
/// use arrow_native::{bridge::ColumnBuilder, prelude::*};
/// #[derive(arrow_native::Record)]
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
