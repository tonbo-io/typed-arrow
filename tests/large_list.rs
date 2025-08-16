use arrow_array::Array;
use arrow_schema::{DataType, Field};
use typed_arrow::{bridge::ArrowBinding, LargeList};

#[test]
fn large_list_datatype() {
    type L = LargeList<i32>;
    assert_eq!(
        <L as ArrowBinding>::data_type(),
        DataType::LargeList(Field::new("item", DataType::Int32, false).into())
    );

    type LN = LargeList<Option<i32>>;
    assert_eq!(
        <LN as ArrowBinding>::data_type(),
        DataType::LargeList(Field::new("item", DataType::Int32, true).into())
    );
}

#[test]
fn large_list_build() {
    type L = LargeList<i32>;
    let mut b = <L as ArrowBinding>::new_builder(3);
    <L as ArrowBinding>::append_value(&mut b, &LargeList(vec![1, 2, 3]));
    <L as ArrowBinding>::append_null(&mut b);
    <L as ArrowBinding>::append_value(&mut b, &LargeList(vec![]));
    let a = <L as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 3);
}

#[test]
fn large_list_nullable_items_build() {
    type LN = LargeList<Option<i32>>;
    let mut b = <LN as ArrowBinding>::new_builder(2);
    <LN as ArrowBinding>::append_value(&mut b, &LargeList(vec![Some(1), None]));
    <LN as ArrowBinding>::append_null(&mut b);
    let a = <LN as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 2);
}
