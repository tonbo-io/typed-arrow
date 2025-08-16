use arrow_array::Array;
use arrow_schema::DataType;
use typed_arrow::{bridge::ArrowBinding, LargeBinary};

#[test]
fn large_binary_datatype_and_build() {
    assert_eq!(
        <LargeBinary as ArrowBinding>::data_type(),
        DataType::LargeBinary
    );
    let mut b = <LargeBinary as ArrowBinding>::new_builder(3);
    <LargeBinary as ArrowBinding>::append_value(&mut b, &LargeBinary(vec![1, 2, 3]));
    <LargeBinary as ArrowBinding>::append_null(&mut b);
    <LargeBinary as ArrowBinding>::append_value(&mut b, &LargeBinary(vec![]));
    let a = <LargeBinary as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 3);
}
