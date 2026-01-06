use typed_arrow::arrow_array as arrow_array;
use typed_arrow::arrow_schema as arrow_schema;

use arrow_array::Array;
use typed_arrow::{Dictionary, LargeBinary, LargeUtf8, bridge::ArrowBinding};

#[test]
fn dict_utf8_value() {
    type D = Dictionary<i32, String>;
    let mut b = <D as ArrowBinding>::new_builder(0);
    <D as ArrowBinding>::append_value(&mut b, &Dictionary::new("a".to_string()));
    <D as ArrowBinding>::append_null(&mut b);
    let arr = <D as ArrowBinding>::finish(b);
    assert_eq!(arr.len(), 2);
}

#[test]
fn dict_utf8_roundtrip_values() {
    use arrow_array::cast;
    type D = Dictionary<i32, String>;
    let mut b = <D as ArrowBinding>::new_builder(0);
    // Build values: ["apple", "banana", "apple", null, "banana"]
    <D as ArrowBinding>::append_value(&mut b, &Dictionary::new("apple".to_string()));
    <D as ArrowBinding>::append_value(&mut b, &Dictionary::new("banana".to_string()));
    <D as ArrowBinding>::append_value(&mut b, &Dictionary::new("apple".to_string()));
    <D as ArrowBinding>::append_null(&mut b);
    <D as ArrowBinding>::append_value(&mut b, &Dictionary::new("banana".to_string()));
    let arr = <D as ArrowBinding>::finish(b);

    let dict_arr = &arr;
    let keys = dict_arr.keys();
    let values = cast::as_string_array(dict_arr.values().as_ref());
    let expected = [
        Some("apple"),
        Some("banana"),
        Some("apple"),
        None,
        Some("banana"),
    ];
    #[allow(clippy::needless_range_loop)]
    for i in 0..dict_arr.len() {
        match expected[i] {
            None => assert!(dict_arr.is_null(i)),
            Some(s) => {
                assert!(dict_arr.is_valid(i));
                let k = usize::try_from(keys.value(i)).expect("non-negative dictionary key");
                assert_eq!(values.value(k), s);
            }
        }
    }
}

#[test]
fn dict_binary_value() {
    type D = Dictionary<i32, Vec<u8>>;
    let mut b = <D as ArrowBinding>::new_builder(0);
    <D as ArrowBinding>::append_value(&mut b, &Dictionary::new(vec![1, 2]));
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
            <D as ArrowBinding>::append_value(&mut b, &Dictionary::new($v));
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
    <D as ArrowBinding>::append_value(&mut b, &Dictionary::new(LargeBinary::new(vec![1, 2])));
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
    <D as ArrowBinding>::append_value(&mut b, &Dictionary::new(LargeUtf8::new("a".into())));
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
    <D as ArrowBinding>::append_value(&mut b, &Dictionary::new([1, 2, 3, 4]));
    <D as ArrowBinding>::append_value(&mut b, &Dictionary::new([1, 2, 3, 4]));
    <D as ArrowBinding>::append_value(&mut b, &Dictionary::new([9, 9, 9, 9]));
    <D as ArrowBinding>::append_null(&mut b);
    let arr = <D as ArrowBinding>::finish(b);
    assert_eq!(arr.len(), 4);
}

#[test]
fn dict_fixed_size_binary_roundtrip() {
    type D = Dictionary<i16, [u8; 4]>;
    let mut b = <D as ArrowBinding>::new_builder(0);
    <D as ArrowBinding>::append_value(&mut b, &Dictionary::new([0xAA, 0xBB, 0xCC, 0xDD]));
    <D as ArrowBinding>::append_null(&mut b);
    <D as ArrowBinding>::append_value(&mut b, &Dictionary::new([0xAA, 0xBB, 0xCC, 0xDD]));
    let arr = <D as ArrowBinding>::finish(b);
    let values = arr
        .values()
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
        .unwrap();
    let keys = arr.keys();
    for &i in &[0usize, 2usize] {
        let k = usize::try_from(keys.value(i)).expect("non-negative dictionary key");
        assert_eq!(values.value(k), &[0xAA, 0xBB, 0xCC, 0xDD]);
    }
}
