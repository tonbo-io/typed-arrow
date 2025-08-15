//! Showcase: TimestampTz<U, Z> with timezone markers.

use arrow_array::Array;
use arrow_native::{prelude::*, Millisecond, Nanosecond, Second, TimestampTz, Utc};

// Custom timezone marker example
enum AsiaShanghai {}
impl arrow_native::TimeZoneSpec for AsiaShanghai {
    const NAME: Option<&'static str> = Some("Asia/Shanghai");
}

#[derive(arrow_native::Record)]
struct RowTz {
    s_utc: TimestampTz<Second, Utc>,
    ms_utc: Option<TimestampTz<Millisecond, Utc>>,
    ns_sh: TimestampTz<Nanosecond, AsiaShanghai>,
}

fn main() {
    use arrow_array::{builder::PrimitiveBuilder, types as t};
    use arrow_schema::{DataType, TimeUnit};

    println!(
        "s_utc={:?}, ms_utc={:?}, ns_sh={:?}",
        <RowTz as ColAt<0>>::data_type(),
        <RowTz as ColAt<1>>::data_type(),
        <RowTz as ColAt<2>>::data_type()
    );

    // Build seconds-with-UTC array
    type B0 = <RowTz as ColAt<0>>::ColumnBuilder;
    type A0 = <RowTz as ColAt<0>>::ColumnArray;
    let mut b0: B0 = PrimitiveBuilder::<t::TimestampSecondType>::with_capacity(2);
    b0.append_value(1);
    b0.append_null();
    let a0: A0 = b0.finish();
    println!("len={}, is_null1={}", a0.len(), a0.is_null(1));

    // Sanity check a DataType
    assert_eq!(
        <RowTz as ColAt<0>>::data_type(),
        DataType::Timestamp(TimeUnit::Second, Some(std::sync::Arc::<str>::from("UTC")))
    );
}
