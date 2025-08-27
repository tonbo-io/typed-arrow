#![allow(clippy::assertions_on_constants, clippy::bool_assert_comparison)]
// Import List wrapper; use List<Option<T>> for item-nullability
use typed_arrow::{prelude::*, List};

#[derive(typed_arrow::Record)]
pub struct Row {
    pub tags: List<String>,                // List<Utf8>, items non-null
    pub scores: Option<List<Option<i32>>>, // Nullable list of nullable i32 items
}

// Helper trait to assert type equality at compile time
trait Same<T> {}
impl<T> Same<T> for T {}

#[test]
fn list_datatypes_and_associated_types() {
    use arrow_array::{
        builder::{ListBuilder, PrimitiveBuilder, StringBuilder},
        types::Int32Type,
        ListArray,
    };
    use arrow_schema::{DataType, Field};

    // Record basics
    assert_eq!(<Row as Record>::LEN, 2);
    assert_eq!(<Row as ColAt<0>>::NAME, "tags");
    assert_eq!(<Row as ColAt<1>>::NAME, "scores");

    // Column-level nullability
    assert_eq!(<Row as ColAt<0>>::NULLABLE, false); // tags required
    assert_eq!(<Row as ColAt<1>>::NULLABLE, true); // scores optional (list-level)

    // DataType mapping
    let expected_tags = DataType::List(Field::new("item", DataType::Utf8, false).into());
    assert_eq!(<Row as ColAt<0>>::data_type(), expected_tags);

    let expected_scores = DataType::List(Field::new("item", DataType::Int32, true).into());
    assert_eq!(<Row as ColAt<1>>::data_type(), expected_scores);

    // Associated ColumnArray is ListArray
    {
        type A0 = <Row as ColAt<0>>::ColumnArray;
        type A1 = <Row as ColAt<1>>::ColumnArray;
        #[allow(clippy::used_underscore_items)]
        fn _arr0<T: Same<ListArray>>() {}
        #[allow(clippy::used_underscore_items)]
        fn _arr1<T: Same<ListArray>>() {}
        #[allow(clippy::used_underscore_items)]
        {
            _arr0::<A0>();
            _arr1::<A1>();
        }
    }

    // Associated ColumnBuilder is ListBuilder<ChildBuilder>
    {
        type B0 = <Row as ColAt<0>>::ColumnBuilder;
        type B1 = <Row as ColAt<1>>::ColumnBuilder;
        #[allow(clippy::used_underscore_items)]
        fn _b0<T: Same<ListBuilder<StringBuilder>>>() {}
        #[allow(clippy::used_underscore_items)]
        fn _b1<T: Same<ListBuilder<PrimitiveBuilder<Int32Type>>>>() {}
        #[allow(clippy::used_underscore_items)]
        {
            _b0::<B0>();
            _b1::<B1>();
        }
    }
}

#[test]
fn list_build_and_values() {
    use arrow_array::{cast, Array};

    // Non-null item list: List<String>
    type L = List<String>;
    let mut b = <L as typed_arrow::bridge::ArrowBinding>::new_builder(3);
    <L as typed_arrow::bridge::ArrowBinding>::append_value(
        &mut b,
        &List::new(vec!["a".to_string(), "b".to_string()]),
    );
    <L as typed_arrow::bridge::ArrowBinding>::append_null(&mut b);
    <L as typed_arrow::bridge::ArrowBinding>::append_value(
        &mut b,
        &List::new(vec!["c".to_string()]),
    );
    let arr = <L as typed_arrow::bridge::ArrowBinding>::finish(b);
    assert_eq!(arr.len(), 3);
    let offsets = arr.value_offsets();
    assert_eq!(offsets, &[0, 2, 2, 3]);
    let values = cast::as_string_array(arr.values());
    assert_eq!(values.len(), 3);
    assert_eq!(values.value(0), "a");
    assert_eq!(values.value(1), "b");
    assert_eq!(values.value(2), "c");

    // Nullable item list nested in Option at column-level: Option<List<Option<i32>>>
    let mut b = <List<Option<i32>> as typed_arrow::bridge::ArrowBinding>::new_builder(0);
    <List<Option<i32>> as typed_arrow::bridge::ArrowBinding>::append_value(
        &mut b,
        &List::new(vec![Some(1), None]),
    );
    <List<Option<i32>> as typed_arrow::bridge::ArrowBinding>::append_value(
        &mut b,
        &List::<Option<i32>>::new(vec![]),
    );
    let a = <List<Option<i32>> as typed_arrow::bridge::ArrowBinding>::finish(b);
    assert_eq!(a.len(), 2);
    let offs = a.value_offsets();
    assert_eq!(offs, &[0, 2, 2]);
    let child = a
        .values()
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<arrow_array::types::Int32Type>>()
        .unwrap();
    assert_eq!(child.len(), 2);
    assert_eq!(child.value(0), 1);
    assert!(child.is_null(1));
}
