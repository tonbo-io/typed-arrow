use arrow_array::Array;
use typed_arrow::{prelude::*, Microsecond, Millisecond, Nanosecond, Second, Timestamp};

#[derive(typed_arrow::Record)]
pub struct RowTs {
    pub s: Timestamp<Second>,
    pub ms: Option<Timestamp<Millisecond>>,
    pub us: Timestamp<Microsecond>,
    pub ns: Option<Timestamp<Nanosecond>>,
}

#[test]
fn timestamp_schema_and_types() {
    use arrow_array::{builder::PrimitiveBuilder, types as t};
    use arrow_schema::{DataType, TimeUnit};

    assert_eq!(<RowTs as Record>::LEN, 4);

    // DataTypes
    assert_eq!(
        <RowTs as ColAt<0>>::data_type(),
        DataType::Timestamp(TimeUnit::Second, None)
    );
    assert_eq!(
        <RowTs as ColAt<1>>::data_type(),
        DataType::Timestamp(TimeUnit::Millisecond, None)
    );
    assert_eq!(
        <RowTs as ColAt<2>>::data_type(),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        <RowTs as ColAt<3>>::data_type(),
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    );

    // Associated builders/arrays types
    type B0 = <RowTs as ColAt<0>>::ColumnBuilder; // PrimitiveBuilder<TimestampSecondType>
    type A0 = <RowTs as ColAt<0>>::ColumnArray; // PrimitiveArray<TimestampSecondType>
    type B1 = <RowTs as ColAt<1>>::ColumnBuilder; // PrimitiveBuilder<TimestampMillisecondType>
    type A1 = <RowTs as ColAt<1>>::ColumnArray; // PrimitiveArray<TimestampMillisecondType>

    // Compile-time checks
    trait Same<T> {}
    impl<T> Same<T> for T {}
    fn _b0<T: Same<PrimitiveBuilder<t::TimestampSecondType>>>() {}
    fn _a0<T: Same<arrow_array::PrimitiveArray<t::TimestampSecondType>>>() {}
    fn _b1<T: Same<PrimitiveBuilder<t::TimestampMillisecondType>>>() {}
    fn _a1<T: Same<arrow_array::PrimitiveArray<t::TimestampMillisecondType>>>() {}
    _b0::<B0>();
    _a0::<A0>();
    _b1::<B1>();
    _a1::<A1>();
}

#[test]
fn build_timestamp_arrays() {
    use arrow_array::{builder::PrimitiveBuilder, types as t};
    // Build seconds array
    type B0 = <RowTs as ColAt<0>>::ColumnBuilder;
    type A0 = <RowTs as ColAt<0>>::ColumnArray;
    let mut b0: B0 = PrimitiveBuilder::<t::TimestampSecondType>::with_capacity(3);
    b0.append_value(1);
    b0.append_null();
    b0.append_value(3);
    let a0: A0 = b0.finish();
    assert_eq!(a0.len(), 3);
    assert!(a0.is_null(1));
}
