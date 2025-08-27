use typed_arrow::{prelude::*, Millisecond, Nanosecond, Second, TimestampTz, Utc};

#[derive(typed_arrow::Record)]
struct RowTz {
    s_utc: TimestampTz<Second, Utc>,
    ms_utc: Option<TimestampTz<Millisecond, Utc>>,
}

#[test]
fn timestamp_tz_schema_and_types() {
    use arrow_array::{builder::PrimitiveBuilder, types as t};
    use arrow_schema::{DataType, TimeUnit};

    assert_eq!(<RowTz as Record>::LEN, 2);

    // DataTypes include timezone name
    assert_eq!(
        <RowTz as ColAt<0>>::data_type(),
        DataType::Timestamp(TimeUnit::Second, Some(std::sync::Arc::<str>::from("UTC")))
    );
    assert_eq!(
        <RowTz as ColAt<1>>::data_type(),
        DataType::Timestamp(
            TimeUnit::Millisecond,
            Some(std::sync::Arc::<str>::from("UTC"))
        )
    );

    // Associated builder/array types are identical to non-tz counterparts
    let mut b0: <RowTz as ColAt<0>>::ColumnBuilder =
        PrimitiveBuilder::<t::TimestampSecondType>::with_capacity(1);
    b0.append_value(42);
    let a0: <RowTz as ColAt<0>>::ColumnArray = b0.finish();
    assert_eq!(a0.len(), 1);
}

// Demonstrate a custom timezone marker
enum AsiaShanghai {}
impl typed_arrow::TimeZoneSpec for AsiaShanghai {
    const NAME: Option<&'static str> = Some("Asia/Shanghai");
}

#[derive(typed_arrow::Record)]
struct RowCustomTz {
    ns_sh: TimestampTz<Nanosecond, AsiaShanghai>,
}

#[test]
fn custom_tz_marker_datatype() {
    use arrow_schema::{DataType, TimeUnit};
    assert_eq!(
        <RowCustomTz as ColAt<0>>::data_type(),
        DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some(std::sync::Arc::<str>::from("Asia/Shanghai"))
        )
    );
}
