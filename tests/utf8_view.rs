use arrow_array::Array;
use arrow_schema::DataType;
use typed_arrow::{Utf8View, bridge::ArrowBinding};

#[test]
fn utf8_view_datatype_and_build() {
    assert_eq!(<Utf8View as ArrowBinding>::data_type(), DataType::Utf8View);
    let mut b = <Utf8View as ArrowBinding>::new_builder(3);
    <Utf8View as ArrowBinding>::append_value(&mut b, &Utf8View::new("hello".into()));
    <Utf8View as ArrowBinding>::append_null(&mut b);
    <Utf8View as ArrowBinding>::append_value(&mut b, &Utf8View::new(String::new()));
    let a = <Utf8View as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 3);
}
