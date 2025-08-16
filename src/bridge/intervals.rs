//! Interval types: YearMonth, DayTime, MonthDayNano.

use arrow_array::{builder::PrimitiveBuilder, types::*, PrimitiveArray};
use arrow_schema::{DataType, IntervalUnit};

use super::ArrowBinding;

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
pub struct IntervalDayTime(pub arrow_array::types::IntervalDayTime);
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
pub struct IntervalMonthDayNano(pub arrow_array::types::IntervalMonthDayNano);
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
