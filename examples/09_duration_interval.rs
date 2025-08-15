//! Showcase: Duration and Interval types in a typed Record.

// Native structs for DayTime and MonthDayNano interval values
use arrow_array::{types as t, Array};
use typed_arrow::{
    bridge::{Duration as Dur, Millisecond, Nanosecond},
    prelude::*,
    IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth,
};

#[derive(typed_arrow::Record)]
struct RowDurInt {
    d_ms: Dur<Millisecond>,
    d_ns_opt: Option<Dur<Nanosecond>>,
    i_ym: IntervalYearMonth,
    i_dt: IntervalDayTime,
    i_mdn_opt: Option<IntervalMonthDayNano>,
}

fn main() {
    use arrow_array::builder::PrimitiveBuilder;
    use arrow_schema::{DataType, IntervalUnit, TimeUnit};

    // Inspect compile-time datatypes
    println!(
        "d_ms={:?}, d_ns_opt={:?}, i_ym={:?}, i_dt={:?}, i_mdn_opt={:?}",
        <RowDurInt as ColAt<0>>::data_type(),
        <RowDurInt as ColAt<1>>::data_type(),
        <RowDurInt as ColAt<2>>::data_type(),
        <RowDurInt as ColAt<3>>::data_type(),
        <RowDurInt as ColAt<4>>::data_type(),
    );

    assert_eq!(
        <RowDurInt as ColAt<0>>::data_type(),
        DataType::Duration(TimeUnit::Millisecond)
    );
    assert_eq!(
        <RowDurInt as ColAt<2>>::data_type(),
        DataType::Interval(IntervalUnit::YearMonth)
    );
    assert_eq!(
        <RowDurInt as ColAt<3>>::data_type(),
        DataType::Interval(IntervalUnit::DayTime)
    );
    assert_eq!(
        <RowDurInt as ColAt<4>>::data_type(),
        DataType::Interval(IntervalUnit::MonthDayNano)
    );

    // Build a couple arrays for demonstration
    // Duration<Millisecond>
    type B0 = <RowDurInt as ColAt<0>>::ColumnBuilder;
    type A0 = <RowDurInt as ColAt<0>>::ColumnArray;
    let mut b0: B0 = PrimitiveBuilder::<t::DurationMillisecondType>::with_capacity(3);
    b0.append_value(1);
    b0.append_value(2);
    b0.append_null();
    let a0: A0 = b0.finish();
    println!("d_ms_len={}, null2={}", a0.len(), a0.is_null(2));

    // IntervalDayTime
    type B3 = <RowDurInt as ColAt<3>>::ColumnBuilder;
    type A3 = <RowDurInt as ColAt<3>>::ColumnArray;
    let mut b3: B3 = PrimitiveBuilder::<t::IntervalDayTimeType>::with_capacity(2);
    b3.append_value(t::IntervalDayTime {
        days: 1,
        milliseconds: 500,
    });
    b3.append_value(t::IntervalDayTime {
        days: 0,
        milliseconds: 0,
    });
    let a3: A3 = b3.finish();
    println!("i_dt_len={}", a3.len());
}
