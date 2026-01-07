//! Interval types: `YearMonth`, `DayTime`, `MonthDayNano`.

#[cfg(feature = "views")]
use arrow_array::Array;
use arrow_array::{
    PrimitiveArray,
    builder::PrimitiveBuilder,
    types::{IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType},
};
use arrow_schema::{DataType, IntervalUnit};

use super::ArrowBinding;
#[cfg(feature = "views")]
use super::ArrowBindingView;

/// Interval with unit `YearMonth` (i32 months since epoch).
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntervalYearMonth(i32);
impl IntervalYearMonth {
    /// Construct a new `YearMonth` interval value from months since epoch.
    #[inline]
    #[must_use]
    pub fn new(value: i32) -> Self {
        Self(value)
    }
    /// Return the months since epoch.
    #[inline]
    #[must_use]
    pub fn value(&self) -> i32 {
        self.0
    }
    /// Consume and return the months since epoch.
    #[inline]
    #[must_use]
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

#[cfg(feature = "views")]
impl ArrowBindingView for IntervalYearMonth {
    type Array = PrimitiveArray<IntervalYearMonthType>;
    type View<'a> = IntervalYearMonth;

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
        Ok(IntervalYearMonth::new(array.value(index)))
    }
}

/// Interval with unit `DayTime` (packed days and milliseconds).
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntervalDayTime(arrow_array::types::IntervalDayTime);
impl IntervalDayTime {
    /// Construct a new `DayTime` interval from the native Arrow struct.
    #[inline]
    #[must_use]
    pub fn new(value: arrow_array::types::IntervalDayTime) -> Self {
        Self(value)
    }
    /// Return the underlying Arrow `DayTime` interval value.
    #[inline]
    #[must_use]
    pub fn value(&self) -> arrow_array::types::IntervalDayTime {
        self.0
    }
    /// Consume and return the underlying Arrow `DayTime` interval value.
    #[inline]
    #[must_use]
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

#[cfg(feature = "views")]
impl ArrowBindingView for IntervalDayTime {
    type Array = PrimitiveArray<IntervalDayTimeType>;
    type View<'a> = IntervalDayTime;

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
        Ok(IntervalDayTime::new(array.value(index)))
    }
}

/// Interval with unit `MonthDayNano` (packed months, days, and nanoseconds).
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntervalMonthDayNano(arrow_array::types::IntervalMonthDayNano);
impl IntervalMonthDayNano {
    /// Construct a new `MonthDayNano` interval from the native Arrow struct.
    #[inline]
    #[must_use]
    pub fn new(value: arrow_array::types::IntervalMonthDayNano) -> Self {
        Self(value)
    }
    /// Return the underlying Arrow `MonthDayNano` interval value.
    #[inline]
    #[must_use]
    pub fn value(&self) -> arrow_array::types::IntervalMonthDayNano {
        self.0
    }
    /// Consume and return the underlying Arrow `MonthDayNano` interval value.
    #[inline]
    #[must_use]
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

#[cfg(feature = "views")]
impl ArrowBindingView for IntervalMonthDayNano {
    type Array = PrimitiveArray<IntervalMonthDayNanoType>;
    type View<'a> = IntervalMonthDayNano;

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
        Ok(IntervalMonthDayNano::new(array.value(index)))
    }
}
