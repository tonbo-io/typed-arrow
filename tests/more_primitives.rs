use arrow_schema::{DataType, TimeUnit};
use half::f16;
use typed_arrow::{
    Date32, Date64, Duration, Microsecond, Millisecond, Nanosecond, Second, Time32, Time64,
    bridge::ArrowBinding,
};

#[test]
fn float16_datatype() {
    assert_eq!(<f16 as ArrowBinding>::data_type(), DataType::Float16);
    let mut b = <f16 as ArrowBinding>::new_builder(2);
    <f16 as ArrowBinding>::append_value(&mut b, &f16::from_f32(1.5));
    <f16 as ArrowBinding>::append_null(&mut b);
    let a = <f16 as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 2);
}

#[test]
fn date32_date64_datatype() {
    assert_eq!(<Date32 as ArrowBinding>::data_type(), DataType::Date32);
    let mut b32 = <Date32 as ArrowBinding>::new_builder(1);
    <Date32 as ArrowBinding>::append_value(&mut b32, &Date32::new(0));
    let _ = <Date32 as ArrowBinding>::finish(b32);

    assert_eq!(<Date64 as ArrowBinding>::data_type(), DataType::Date64);
    let mut b64 = <Date64 as ArrowBinding>::new_builder(1);
    <Date64 as ArrowBinding>::append_value(&mut b64, &Date64::new(0));
    let _ = <Date64 as ArrowBinding>::finish(b64);
}

#[test]
fn time32_time64_datatype() {
    type T32S = Time32<Second>;
    type T32Ms = Time32<Millisecond>;
    type T64Us = Time64<Microsecond>;
    type T64Ns = Time64<Nanosecond>;

    assert_eq!(
        <T32S as ArrowBinding>::data_type(),
        DataType::Time32(TimeUnit::Second)
    );
    assert_eq!(
        <T32Ms as ArrowBinding>::data_type(),
        DataType::Time32(TimeUnit::Millisecond)
    );
    assert_eq!(
        <T64Us as ArrowBinding>::data_type(),
        DataType::Time64(TimeUnit::Microsecond)
    );
    assert_eq!(
        <T64Ns as ArrowBinding>::data_type(),
        DataType::Time64(TimeUnit::Nanosecond)
    );

    let mut b = <T32S as ArrowBinding>::new_builder(1);
    let v = Time32::<Second>::new(0);
    <T32S as ArrowBinding>::append_value(&mut b, &v);
    let _ = <T32S as ArrowBinding>::finish(b);
}

#[test]
fn duration_datatype() {
    type Dms = Duration<Millisecond>;
    assert_eq!(
        <Dms as ArrowBinding>::data_type(),
        DataType::Duration(TimeUnit::Millisecond)
    );
    let mut b = <Dms as ArrowBinding>::new_builder(1);
    <Dms as ArrowBinding>::append_value(&mut b, &Duration::<Millisecond>::new(1));
    let _ = <Dms as ArrowBinding>::finish(b);
}
