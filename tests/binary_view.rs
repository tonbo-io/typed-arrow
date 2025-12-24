use arrow_array::Array;
use arrow_schema::DataType;
use typed_arrow::{BinaryView, bridge::ArrowBinding};

#[test]
fn binary_view_datatype_and_build() {
    assert_eq!(
        <BinaryView as ArrowBinding>::data_type(),
        DataType::BinaryView
    );
    let mut b = <BinaryView as ArrowBinding>::new_builder(3);
    <BinaryView as ArrowBinding>::append_value(&mut b, &BinaryView::new(vec![1, 2, 3]));
    <BinaryView as ArrowBinding>::append_null(&mut b);
    <BinaryView as ArrowBinding>::append_value(&mut b, &BinaryView::new(vec![]));
    let a = <BinaryView as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 3);
}
