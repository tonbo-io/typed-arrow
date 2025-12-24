//! Showcase: Timestamp units (Second/Millisecond/Microsecond/Nanosecond).

use arrow_array::Array;
use typed_arrow::{Microsecond, Millisecond, Nanosecond, Second, Timestamp, prelude::*};

#[derive(Record)]
struct RowTs {
    s: Timestamp<Second>,
    ms: Option<Timestamp<Millisecond>>,
    us: Timestamp<Microsecond>,
    ns: Option<Timestamp<Nanosecond>>,
}

fn main() {
    use arrow_array::{builder::PrimitiveBuilder, types as t};
    use arrow_schema::{DataType, TimeUnit};

    println!(
        "s={:?}, ms={:?}, us={:?}, ns={:?}",
        <RowTs as ColAt<0>>::data_type(),
        <RowTs as ColAt<1>>::data_type(),
        <RowTs as ColAt<2>>::data_type(),
        <RowTs as ColAt<3>>::data_type()
    );

    // Build seconds array
    let mut b0: <RowTs as ColAt<0>>::ColumnBuilder =
        PrimitiveBuilder::<t::TimestampSecondType>::with_capacity(3);
    b0.append_value(1);
    b0.append_null();
    b0.append_value(3);
    let a0: <RowTs as ColAt<0>>::ColumnArray = b0.finish();
    println!("ts_second len={}, is_null[1]={}", a0.len(), a0.is_null(1));

    // Sanity check the data types
    assert_eq!(
        <RowTs as ColAt<0>>::data_type(),
        DataType::Timestamp(TimeUnit::Second, None)
    );
}
