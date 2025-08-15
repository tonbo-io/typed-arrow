use arrow_array::Array;
use typed_arrow::{bridge::ArrowBinding, Null};
use arrow_schema::DataType;

#[test]
fn null_datatype_and_builder() {
    assert_eq!(<Null as ArrowBinding>::data_type(), DataType::Null);
    let mut b = <Null as ArrowBinding>::new_builder(10);
    <Null as ArrowBinding>::append_value(&mut b, &Null);
    <Null as ArrowBinding>::append_null(&mut b);
    let a = <Null as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 2);
    assert_eq!(a.logical_null_count(), 2);
}
