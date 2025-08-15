use arrow_native::{bridge::ArrowBinding, Dictionary, LargeBinary, LargeUtf8};

#[test]
fn dict_utf8_value() {
    type D = Dictionary<i32, String>;
    let mut b = <D as ArrowBinding>::new_builder(0);
    <D as ArrowBinding>::append_value(
        &mut b,
        &Dictionary("a".to_string(), std::marker::PhantomData),
    );
    <D as ArrowBinding>::append_null(&mut b);
    let arr = <D as ArrowBinding>::finish(b);
    assert_eq!(arr.len(), 2);
}

#[test]
fn dict_binary_value() {
    type D = Dictionary<i32, Vec<u8>>;
    let mut b = <D as ArrowBinding>::new_builder(0);
    <D as ArrowBinding>::append_value(&mut b, &Dictionary(vec![1, 2], std::marker::PhantomData));
    <D as ArrowBinding>::append_null(&mut b);
    let arr = <D as ArrowBinding>::finish(b);
    assert_eq!(arr.len(), 2);
}

#[test]
fn dict_primitive_values() {
    macro_rules! check {
        ($t:ty, $v:expr) => {{
            type D = Dictionary<i32, $t>;
            let mut b = <D as ArrowBinding>::new_builder(0);
            <D as ArrowBinding>::append_value(&mut b, &Dictionary($v, std::marker::PhantomData));
            <D as ArrowBinding>::append_null(&mut b);
            let arr = <D as ArrowBinding>::finish(b);
            assert_eq!(arr.len(), 2);
        }};
    }

    check!(i8, 1i8);
    check!(i16, 1i16);
    check!(i32, 1i32);
    check!(i64, 1i64);
    check!(u8, 1u8);
    check!(u16, 1u16);
    check!(u32, 1u32);
    check!(u64, 1u64);
    check!(f32, 1.0f32);
    check!(f64, 1.0f64);
}

#[test]
fn dict_large_binary_value() {
    use arrow_schema::DataType;

    type D = Dictionary<i32, LargeBinary>;
    let mut b = <D as ArrowBinding>::new_builder(0);
    <D as ArrowBinding>::append_value(
        &mut b,
        &Dictionary(LargeBinary(vec![1, 2]), std::marker::PhantomData),
    );
    <D as ArrowBinding>::append_null(&mut b);
    let arr = <D as ArrowBinding>::finish(b);
    assert_eq!(arr.len(), 2);
    assert_eq!(
        <D as ArrowBinding>::data_type(),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeBinary))
    );
}

#[test]
fn dict_large_utf8_value() {
    use arrow_schema::DataType;

    type D = Dictionary<i32, LargeUtf8>;
    let mut b = <D as ArrowBinding>::new_builder(0);
    <D as ArrowBinding>::append_value(
        &mut b,
        &Dictionary(LargeUtf8("a".into()), std::marker::PhantomData),
    );
    <D as ArrowBinding>::append_null(&mut b);
    let arr = <D as ArrowBinding>::finish(b);
    assert_eq!(arr.len(), 2);
    assert_eq!(
        <D as ArrowBinding>::data_type(),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8))
    );
}

#[test]
fn dict_fixed_size_binary_value() {
    use arrow_schema::DataType;

    type D = Dictionary<i32, [u8; 4]>;
    assert_eq!(
        <D as ArrowBinding>::data_type(),
        DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::FixedSizeBinary(4))
        )
    );

    let mut b = <D as ArrowBinding>::new_builder(0);
    <D as ArrowBinding>::append_value(&mut b, &Dictionary([1, 2, 3, 4], std::marker::PhantomData));
    <D as ArrowBinding>::append_value(&mut b, &Dictionary([1, 2, 3, 4], std::marker::PhantomData));
    <D as ArrowBinding>::append_value(&mut b, &Dictionary([9, 9, 9, 9], std::marker::PhantomData));
    <D as ArrowBinding>::append_null(&mut b);
    let arr = <D as ArrowBinding>::finish(b);
    assert_eq!(arr.len(), 4);
}
