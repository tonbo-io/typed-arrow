//! Showcase: `TimestampTz`<U, Z> with timezone markers.

use arrow_array::Array;
use typed_arrow::{prelude::*, Millisecond, Nanosecond, Second, TimestampTz, Utc};

// Custom timezone marker example
enum AsiaShanghai {}
impl typed_arrow::TimeZoneSpec for AsiaShanghai {
    const NAME: Option<&'static str> = Some("Asia/Shanghai");
}

#[derive(typed_arrow::Record)]
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
    let mut b0: <RowTz as ColAt<0>>::ColumnBuilder =
        PrimitiveBuilder::<t::TimestampSecondType>::with_capacity(2);
    b0.append_value(1);
    b0.append_null();
    let a0: <RowTz as ColAt<0>>::ColumnArray = b0.finish();
    println!("len={}, is_null1={}", a0.len(), a0.is_null(1));

    // Sanity check a DataType
    assert_eq!(
        <RowTz as ColAt<0>>::data_type(),
        DataType::Timestamp(TimeUnit::Second, Some(std::sync::Arc::<str>::from("UTC")))
    );
}
