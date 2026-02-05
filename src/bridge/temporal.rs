//! Temporal types: Date, Time, Duration, Timestamp (with/without timezone).

use std::{marker::PhantomData, sync::Arc};

#[cfg(feature = "views")]
use arrow_array::Array;
use arrow_array::{
    PrimitiveArray,
    builder::PrimitiveBuilder,
    types::{
        Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType,
        DurationNanosecondType, DurationSecondType, Time32MillisecondType, Time32SecondType,
        Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    },
};
use arrow_schema::{DataType, TimeUnit};

use super::ArrowBinding;
#[cfg(feature = "views")]
use super::ArrowBindingView;

// ---------- Timestamp units and bindings ----------

/// Marker describing a timestamp unit.
pub trait TimeUnitSpec {
    /// Typed Arrow timestamp marker for this unit.
    type Arrow: arrow_array::types::ArrowTimestampType;
    /// The `arrow_schema::TimeUnit` of this marker.
    fn unit() -> TimeUnit;
}

/// Seconds since epoch.
#[derive(Debug)]
pub enum Second {}
impl TimeUnitSpec for Second {
    type Arrow = TimestampSecondType;
    fn unit() -> TimeUnit {
        TimeUnit::Second
    }
}

/// Milliseconds since epoch.
#[derive(Debug)]
pub enum Millisecond {}
impl TimeUnitSpec for Millisecond {
    type Arrow = TimestampMillisecondType;
    fn unit() -> TimeUnit {
        TimeUnit::Millisecond
    }
}

/// Microseconds since epoch.
#[derive(Debug)]
pub enum Microsecond {}
impl TimeUnitSpec for Microsecond {
    type Arrow = TimestampMicrosecondType;
    fn unit() -> TimeUnit {
        TimeUnit::Microsecond
    }
}

/// Nanoseconds since epoch.
#[derive(Debug)]
pub enum Nanosecond {}
impl TimeUnitSpec for Nanosecond {
    type Arrow = TimestampNanosecondType;
    fn unit() -> TimeUnit {
        TimeUnit::Nanosecond
    }
}

/// Timestamp value (unit only, timezone = None).
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp<U: TimeUnitSpec>(i64, PhantomData<U>);
impl<U: TimeUnitSpec> Timestamp<U> {
    /// Construct a new timestamp from an epoch value in the unit `U`.
    #[inline]
    #[must_use]
    pub fn new(value: i64) -> Self {
        Self(value, PhantomData)
    }
    /// Return the inner epoch value.
    #[inline]
    #[must_use]
    pub fn value(&self) -> i64 {
        self.0
    }
    /// Consume and return the inner epoch value.
    #[inline]
    #[must_use]
    pub fn into_value(self) -> i64 {
        self.0
    }
}
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

#[cfg(feature = "views")]
impl<U: TimeUnitSpec + 'static> ArrowBindingView for Timestamp<U> {
    type Array = PrimitiveArray<U::Arrow>;
    type View<'a> = Timestamp<U>;

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
        Ok(Timestamp::new(array.value(index)))
    }
}

/// Marker describing a timestamp timezone.
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
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimestampTz<U: TimeUnitSpec, Z: TimeZoneSpec>(i64, PhantomData<(U, Z)>);
impl<U: TimeUnitSpec, Z: TimeZoneSpec> TimestampTz<U, Z> {
    /// Construct a new timezone-aware timestamp from an epoch value in the unit `U`.
    #[inline]
    #[must_use]
    pub fn new(value: i64) -> Self {
        Self(value, PhantomData)
    }
    /// Return the inner epoch value.
    #[inline]
    #[must_use]
    pub fn value(&self) -> i64 {
        self.0
    }
    /// Consume and return the inner epoch value.
    #[inline]
    #[must_use]
    pub fn into_value(self) -> i64 {
        self.0
    }
}
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

#[cfg(feature = "views")]
impl<U: TimeUnitSpec + 'static, Z: TimeZoneSpec + 'static> ArrowBindingView for TimestampTz<U, Z> {
    type Array = PrimitiveArray<U::Arrow>;
    type View<'a> = TimestampTz<U, Z>;

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
        Ok(TimestampTz::new(array.value(index)))
    }
}

// ---------- Date32 / Date64 ----------

/// Days since UNIX epoch.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date32(i32);
impl Date32 {
    /// Construct a new `Date32` from days since UNIX epoch.
    #[inline]
    #[must_use]
    pub fn new(value: i32) -> Self {
        Self(value)
    }
    /// Return the days since UNIX epoch.
    #[inline]
    #[must_use]
    pub fn value(&self) -> i32 {
        self.0
    }
    /// Consume and return the days since UNIX epoch.
    #[inline]
    #[must_use]
    pub fn into_value(self) -> i32 {
        self.0
    }
}
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

#[cfg(feature = "views")]
impl ArrowBindingView for Date32 {
    type Array = PrimitiveArray<Date32Type>;
    type View<'a> = Date32;

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
        Ok(Date32::new(array.value(index)))
    }
}

/// Milliseconds since UNIX epoch.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date64(i64);
impl Date64 {
    /// Construct a new `Date64` from milliseconds since UNIX epoch.
    #[inline]
    #[must_use]
    pub fn new(value: i64) -> Self {
        Self(value)
    }
    /// Return the milliseconds since UNIX epoch.
    #[inline]
    #[must_use]
    pub fn value(&self) -> i64 {
        self.0
    }
    /// Consume and return the milliseconds since UNIX epoch.
    #[inline]
    #[must_use]
    pub fn into_value(self) -> i64 {
        self.0
    }
}
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

#[cfg(feature = "views")]
impl ArrowBindingView for Date64 {
    type Array = PrimitiveArray<Date64Type>;
    type View<'a> = Date64;

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
        Ok(Date64::new(array.value(index)))
    }
}

// ---------- Time32 / Time64 ----------

/// Marker mapping for `Time32` units to Arrow time types.
pub trait Time32UnitSpec {
    type Arrow;
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
#[derive(Debug, Clone)]
pub struct Time32<U: Time32UnitSpec>(i32, PhantomData<U>);
impl<U: Time32UnitSpec> Time32<U> {
    /// Construct a new `Time32` value from an `i32` count in unit `U`.
    #[inline]
    #[must_use]
    pub fn new(value: i32) -> Self {
        Self(value, PhantomData)
    }
    /// Return the inner value.
    #[inline]
    #[must_use]
    pub fn value(&self) -> i32 {
        self.0
    }
    /// Consume and return the inner value.
    #[inline]
    #[must_use]
    pub fn into_value(self) -> i32 {
        self.0
    }
}
impl<U: Time32UnitSpec> ArrowBinding for Time32<U>
where
    U::Arrow: arrow_array::types::ArrowPrimitiveType<Native = i32>,
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
        b.append_value(v.0 as <U::Arrow as arrow_array::types::ArrowPrimitiveType>::Native);
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

#[cfg(feature = "views")]
impl<U: Time32UnitSpec + 'static> ArrowBindingView for Time32<U>
where
    U::Arrow: arrow_array::types::ArrowPrimitiveType<Native = i32>,
{
    type Array = PrimitiveArray<U::Arrow>;
    type View<'a> = Time32<U>;

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
        Ok(Time32::new(array.value(index)))
    }
}

/// Marker mapping for `Time64` units to Arrow time types.
pub trait Time64UnitSpec {
    type Arrow;
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
#[derive(Debug, Clone)]
pub struct Time64<U: Time64UnitSpec>(i64, PhantomData<U>);
impl<U: Time64UnitSpec> Time64<U> {
    /// Construct a new `Time64` value from an `i64` count in unit `U`.
    #[inline]
    #[must_use]
    pub fn new(value: i64) -> Self {
        Self(value, PhantomData)
    }
    /// Return the inner value.
    #[inline]
    #[must_use]
    pub fn value(&self) -> i64 {
        self.0
    }
    /// Consume and return the inner value.
    #[inline]
    #[must_use]
    pub fn into_value(self) -> i64 {
        self.0
    }
}
impl<U: Time64UnitSpec> ArrowBinding for Time64<U>
where
    U::Arrow: arrow_array::types::ArrowPrimitiveType<Native = i64>,
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
        b.append_value(v.0 as <U::Arrow as arrow_array::types::ArrowPrimitiveType>::Native);
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

#[cfg(feature = "views")]
impl<U: Time64UnitSpec + 'static> ArrowBindingView for Time64<U>
where
    U::Arrow: arrow_array::types::ArrowPrimitiveType<Native = i64>,
{
    type Array = PrimitiveArray<U::Arrow>;
    type View<'a> = Time64<U>;

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
        Ok(Time64::new(array.value(index)))
    }
}

// ---------- Duration ----------

/// Marker mapping for `Duration` units to Arrow duration types.
pub trait DurationUnitSpec {
    type Arrow;
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
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Duration<U: DurationUnitSpec>(i64, PhantomData<U>);
impl<U: DurationUnitSpec> Duration<U> {
    /// Construct a new duration from an `i64` count in unit `U`.
    #[inline]
    #[must_use]
    pub fn new(value: i64) -> Self {
        Self(value, PhantomData)
    }
    /// Return the inner value.
    #[inline]
    #[must_use]
    pub fn value(&self) -> i64 {
        self.0
    }
    /// Consume and return the inner value.
    #[inline]
    #[must_use]
    pub fn into_value(self) -> i64 {
        self.0
    }
}
impl<U: DurationUnitSpec> ArrowBinding for Duration<U>
where
    U::Arrow: arrow_array::types::ArrowPrimitiveType<Native = i64>,
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

#[cfg(feature = "views")]
impl<U: DurationUnitSpec + 'static> ArrowBindingView for Duration<U>
where
    U::Arrow: arrow_array::types::ArrowPrimitiveType<Native = i64>,
{
    type Array = PrimitiveArray<U::Arrow>;
    type View<'a> = Duration<U>;

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
        Ok(Duration::new(array.value(index)))
    }
}
