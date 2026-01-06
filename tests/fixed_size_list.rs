use typed_arrow::arrow_array::Array;
use typed_arrow::arrow_schema::{DataType, Field};
use typed_arrow::{FixedSizeList, FixedSizeListNullable, bridge::ArrowBinding};

#[test]
fn fixed_size_list_datatype() {
    type L = FixedSizeList<i32, 3>;
    let dt = <L as ArrowBinding>::data_type();
    assert_eq!(
        dt,
        DataType::FixedSizeList(Field::new("item", DataType::Int32, false).into(), 3)
    );

    let dtn = <FixedSizeListNullable<i32, 2> as ArrowBinding>::data_type();
    assert_eq!(
        dtn,
        DataType::FixedSizeList(Field::new("item", DataType::Int32, true).into(), 2)
    );
}

#[test]
fn fixed_size_list_build_and_nulls() {
    type L = FixedSizeList<i32, 3>;
    let mut b = <L as ArrowBinding>::new_builder(3);
    <L as ArrowBinding>::append_value(&mut b, &FixedSizeList::new([1, 2, 3]));
    <L as ArrowBinding>::append_null(&mut b);
    <L as ArrowBinding>::append_value(&mut b, &FixedSizeList::new([10, 11, 12]));
    let a = <L as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 3);
    assert_eq!(a.value_length(), 3);
}

#[test]
fn fixed_size_list_nullable_items_build() {
    type LN = FixedSizeListNullable<i32, 2>;
    let mut b = <LN as ArrowBinding>::new_builder(2);
    <LN as ArrowBinding>::append_value(&mut b, &FixedSizeListNullable::new([Some(1), None]));
    <LN as ArrowBinding>::append_value(&mut b, &FixedSizeListNullable::new([None, Some(2)]));
    let a = <LN as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 2);
    assert_eq!(a.value_length(), 2);
}

#[test]
fn fixed_size_list_child_values() {
    use typed_arrow::arrow_array::Array;
    type L = FixedSizeList<i32, 3>;
    let mut b = <L as ArrowBinding>::new_builder(3);
    <L as ArrowBinding>::append_value(&mut b, &FixedSizeList::new([7, 8, 9]));
    <L as ArrowBinding>::append_null(&mut b);
    <L as ArrowBinding>::append_value(&mut b, &FixedSizeList::new([1, 2, 3]));
    let a = <L as ArrowBinding>::finish(b);
    let child = a
        .values()
        .as_any()
        .downcast_ref::<typed_arrow::arrow_array::PrimitiveArray<typed_arrow::arrow_array::types::Int32Type>>()
        .unwrap();
    assert_eq!(child.len(), 9);
    assert_eq!(child.value(0), 7);
    assert_eq!(child.value(1), 8);
    assert_eq!(child.value(2), 9);
    // middle row is null, so 3 child nulls
    assert!(child.is_null(3));
    assert!(child.is_null(4));
    assert!(child.is_null(5));
    assert_eq!(child.value(6), 1);
    assert_eq!(child.value(7), 2);
    assert_eq!(child.value(8), 3);
}
