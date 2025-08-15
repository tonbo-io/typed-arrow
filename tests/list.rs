#![allow(clippy::assertions_on_constants, clippy::bool_assert_comparison)]
// Import List wrapper; use List<Option<T>> for item-nullability
use arrow_native::{prelude::*, List};

#[derive(arrow_native::Record)]
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
    type A0 = <Row as ColAt<0>>::ColumnArray;
    type A1 = <Row as ColAt<1>>::ColumnArray;
    fn _arr0<T: Same<ListArray>>() {}
    fn _arr1<T: Same<ListArray>>() {}
    _arr0::<A0>();
    _arr1::<A1>();

    // Associated ColumnBuilder is ListBuilder<ChildBuilder>
    type B0 = <Row as ColAt<0>>::ColumnBuilder;
    type B1 = <Row as ColAt<1>>::ColumnBuilder;
    fn _b0<T: Same<ListBuilder<StringBuilder>>>() {}
    fn _b1<T: Same<ListBuilder<PrimitiveBuilder<Int32Type>>>>() {}
    _b0::<B0>();
    _b1::<B1>();
}
