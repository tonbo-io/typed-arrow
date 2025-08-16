use arrow_array::Array;
use arrow_schema::{DataType, Field};
use typed_arrow::{bridge::ArrowBinding, FixedSizeList, FixedSizeListNullable};

#[test]
fn fixed_size_list_datatype() {
    type L = FixedSizeList<i32, 3>;
    let dt = <L as ArrowBinding>::data_type();
    assert_eq!(
        dt,
        DataType::FixedSizeList(Field::new("item", DataType::Int32, false).into(), 3)
    );

    type LN = FixedSizeListNullable<i32, 2>;
    let dtn = <LN as ArrowBinding>::data_type();
    assert_eq!(
        dtn,
        DataType::FixedSizeList(Field::new("item", DataType::Int32, true).into(), 2)
    );
}

#[test]
fn fixed_size_list_build_and_nulls() {
    type L = FixedSizeList<i32, 3>;
    let mut b = <L as ArrowBinding>::new_builder(3);
    <L as ArrowBinding>::append_value(&mut b, &FixedSizeList([1, 2, 3]));
    <L as ArrowBinding>::append_null(&mut b);
    <L as ArrowBinding>::append_value(&mut b, &FixedSizeList([10, 11, 12]));
    let a = <L as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 3);
    assert_eq!(a.value_length(), 3);
}

#[test]
fn fixed_size_list_nullable_items_build() {
    type LN = FixedSizeListNullable<i32, 2>;
    let mut b = <LN as ArrowBinding>::new_builder(2);
    <LN as ArrowBinding>::append_value(&mut b, &FixedSizeListNullable([Some(1), None]));
    <LN as ArrowBinding>::append_value(&mut b, &FixedSizeListNullable([None, Some(2)]));
    let a = <LN as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 2);
    assert_eq!(a.value_length(), 2);
}
