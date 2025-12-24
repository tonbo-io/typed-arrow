//! Showcase: Duration and Interval types in a typed Record.

// Native structs for DayTime and MonthDayNano interval values
use arrow_array::{Array, types as t};
use typed_arrow::{
    IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth,
    bridge::{Duration as Dur, Millisecond, Nanosecond},
    prelude::*,
};

#[derive(Record)]
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
    let mut b0: <RowDurInt as ColAt<0>>::ColumnBuilder =
        PrimitiveBuilder::<t::DurationMillisecondType>::with_capacity(3);
    b0.append_value(1);
    b0.append_value(2);
    b0.append_null();
    let a0: <RowDurInt as ColAt<0>>::ColumnArray = b0.finish();
    println!("d_ms_len={}, null2={}", a0.len(), a0.is_null(2));

    // IntervalDayTime
    let mut b3: <RowDurInt as ColAt<3>>::ColumnBuilder =
        PrimitiveBuilder::<t::IntervalDayTimeType>::with_capacity(2);
    b3.append_value(t::IntervalDayTime {
        days: 1,
        milliseconds: 500,
    });
    b3.append_value(t::IntervalDayTime {
        days: 0,
        milliseconds: 0,
    });
    let a3: <RowDurInt as ColAt<3>>::ColumnArray = b3.finish();
    println!("i_dt_len={}", a3.len());
}
