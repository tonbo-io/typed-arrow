//! Interval types: YearMonth, DayTime, MonthDayNano.

use arrow_array::{builder::PrimitiveBuilder, types::*, PrimitiveArray};
use arrow_schema::{DataType, IntervalUnit};

use super::ArrowBinding;

/// Interval with unit YearMonth (i32 months since epoch).
pub struct IntervalYearMonth(i32);
impl IntervalYearMonth {
    /// Construct a new YearMonth interval value from months since epoch.
    #[inline]
    pub fn new(value: i32) -> Self {
        Self(value)
    }
    /// Return the months since epoch.
    #[inline]
    pub fn value(&self) -> i32 {
        self.0
    }
    /// Consume and return the months since epoch.
    #[inline]
    pub fn into_value(self) -> i32 {
        self.0
    }
}
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
pub struct IntervalDayTime(arrow_array::types::IntervalDayTime);
impl IntervalDayTime {
    /// Construct a new DayTime interval from the native Arrow struct.
    #[inline]
    pub fn new(value: arrow_array::types::IntervalDayTime) -> Self {
        Self(value)
    }
    /// Return the underlying Arrow DayTime interval value.
    #[inline]
    pub fn value(&self) -> arrow_array::types::IntervalDayTime {
        self.0
    }
    /// Consume and return the underlying Arrow DayTime interval value.
    #[inline]
    pub fn into_value(self) -> arrow_array::types::IntervalDayTime {
        self.0
    }
}
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
pub struct IntervalMonthDayNano(arrow_array::types::IntervalMonthDayNano);
impl IntervalMonthDayNano {
    /// Construct a new MonthDayNano interval from the native Arrow struct.
    #[inline]
    pub fn new(value: arrow_array::types::IntervalMonthDayNano) -> Self {
        Self(value)
    }
    /// Return the underlying Arrow MonthDayNano interval value.
    #[inline]
    pub fn value(&self) -> arrow_array::types::IntervalMonthDayNano {
        self.0
    }
    /// Consume and return the underlying Arrow MonthDayNano interval value.
    #[inline]
    pub fn into_value(self) -> arrow_array::types::IntervalMonthDayNano {
        self.0
    }
}
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
