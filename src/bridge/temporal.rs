//! Temporal types: Date, Time, Duration, Timestamp (with/without timezone).

use std::{marker::PhantomData, sync::Arc};

use arrow_array::{builder::PrimitiveBuilder, types::*, PrimitiveArray};
use arrow_schema::{DataType, TimeUnit};

use super::ArrowBinding;

// ---------- Timestamp units and bindings ----------

/// Marker describing a timestamp unit.
pub trait TimeUnitSpec {
    /// Typed Arrow timestamp marker for this unit.
    type Arrow: arrow_array::types::ArrowTimestampType;
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

// ---------- Date32 / Date64 ----------

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
pub struct Time32<U: Time32UnitSpec>(pub i32, pub PhantomData<U>);
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
pub struct Time64<U: Time64UnitSpec>(pub i64, pub PhantomData<U>);
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
pub struct Duration<U: DurationUnitSpec>(pub i64, pub PhantomData<U>);
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
