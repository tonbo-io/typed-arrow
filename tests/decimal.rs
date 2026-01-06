use typed_arrow::arrow_array::{Array, Decimal128Array, Decimal256Array};
use typed_arrow::arrow_buffer::i256;
use typed_arrow::bridge::ArrowBinding;

#[test]
fn decimal128_bindings() {
    type D = typed_arrow::Decimal128<38, 4>;
    assert_eq!(
        <D as ArrowBinding>::data_type(),
        typed_arrow::arrow_schema::DataType::Decimal128(38, 4)
    );

    let mut b = <D as ArrowBinding>::new_builder(3);
    <D as ArrowBinding>::append_value(&mut b, &typed_arrow::Decimal128::<38, 4>::new(12_345)); // 1.2345
    <D as ArrowBinding>::append_null(&mut b);
    <D as ArrowBinding>::append_value(&mut b, &typed_arrow::Decimal128::<38, 4>::new(-9)); // -0.0009
    let a: Decimal128Array = <D as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 3);
    assert_eq!(a.value(0), 12_345i128);
    assert!(a.is_null(1));
    assert_eq!(a.value(2), -9i128);
}

#[test]
fn decimal256_bindings() {
    type D = typed_arrow::Decimal256<76, 10>;
    assert_eq!(
        <D as ArrowBinding>::data_type(),
        typed_arrow::arrow_schema::DataType::Decimal256(76, 10)
    );

    let mut b = <D as ArrowBinding>::new_builder(2);
    <D as ArrowBinding>::append_value(
        &mut b,
        &typed_arrow::Decimal256::<76, 10>::new(i256::from(12345i64)),
    );
    <D as ArrowBinding>::append_value(
        &mut b,
        &typed_arrow::Decimal256::<76, 10>::new(i256::from(-7i64)),
    );
    let a: Decimal256Array = <D as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 2);
    assert_eq!(a.value(0), i256::from(12345i64));
    assert_eq!(a.value(1), i256::from(-7i64));
}
