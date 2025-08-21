use arrow_array::Array;
use arrow_schema::DataType;
use typed_arrow::{bridge::ArrowBinding, LargeUtf8};

#[test]
fn large_utf8_datatype_and_build() {
    assert_eq!(
        <LargeUtf8 as ArrowBinding>::data_type(),
        DataType::LargeUtf8
    );
    let mut b = <LargeUtf8 as ArrowBinding>::new_builder(3);
    <LargeUtf8 as ArrowBinding>::append_value(&mut b, &LargeUtf8::new("hello".into()));
    <LargeUtf8 as ArrowBinding>::append_null(&mut b);
    <LargeUtf8 as ArrowBinding>::append_value(&mut b, &LargeUtf8::new(String::new()));
    let a = <LargeUtf8 as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 3);
}
