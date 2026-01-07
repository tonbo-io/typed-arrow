use arrow_array::Array;
use arrow_schema::{DataType, Field};
use typed_arrow::{LargeList, arrow_array, arrow_schema, bridge::ArrowBinding};

#[test]
fn large_list_datatype() {
    type L = LargeList<i32>;
    assert_eq!(
        <L as ArrowBinding>::data_type(),
        DataType::LargeList(Field::new("item", DataType::Int32, false).into())
    );

    assert_eq!(
        <LargeList<Option<i32>> as ArrowBinding>::data_type(),
        DataType::LargeList(Field::new("item", DataType::Int32, true).into())
    );
}

#[test]
fn large_list_build() {
    type L = LargeList<i32>;
    let mut b = <L as ArrowBinding>::new_builder(3);
    <L as ArrowBinding>::append_value(&mut b, &LargeList::new(vec![1, 2, 3]));
    <L as ArrowBinding>::append_null(&mut b);
    <L as ArrowBinding>::append_value(&mut b, &LargeList::new(vec![]));
    let a = <L as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 3);
}

#[test]
fn large_list_nullable_items_build() {
    type LN = LargeList<Option<i32>>;
    let mut b = <LN as ArrowBinding>::new_builder(2);
    <LN as ArrowBinding>::append_value(&mut b, &LargeList::new(vec![Some(1), None]));
    <LN as ArrowBinding>::append_null(&mut b);
    let a = <LN as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 2);
}

#[test]
fn large_list_offsets_and_values() {
    use arrow_array::{Array, cast};
    type L = LargeList<Option<i32>>;
    let mut b = <L as ArrowBinding>::new_builder(4);
    // row0: []
    <L as ArrowBinding>::append_value(&mut b, &LargeList::new(vec![]));
    // row1: [1, null]
    <L as ArrowBinding>::append_value(&mut b, &LargeList::new(vec![Some(1), None]));
    // row2: null list simulated by appending null row via builder API is not available for
    // value-level; here we append another non-empty list to verify offsets
    <L as ArrowBinding>::append_value(&mut b, &LargeList::new(vec![Some(2)]));
    // row3: [3,4]
    <L as ArrowBinding>::append_value(&mut b, &LargeList::new(vec![Some(3), Some(4)]));
    let a = <L as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 4);
    let offs = a.value_offsets();
    assert_eq!(offs, &[0, 0, 2, 3, 5]);
    let child = cast::as_primitive_array::<arrow_array::types::Int32Type>(a.values());
    assert_eq!(child.len(), 5);
    assert_eq!(child.value(0), 1);
    assert!(child.is_null(1));
    assert_eq!(child.value(2), 2);
    assert_eq!(child.value(3), 3);
    assert_eq!(child.value(4), 4);
}
