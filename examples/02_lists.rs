//! Showcase: List and item-nullable lists via List<Option<T>> for Arrow `ListArray`.

use arrow_array::Array;
use typed_arrow::{List, bridge::ArrowBinding, prelude::*};

#[derive(Record)]
struct Row {
    tags: List<String>,                // List<Utf8>, items non-null
    scores: Option<List<Option<i32>>>, // Nullable list whose items are nullable i32
}

fn main() {
    // DataTypes from compile-time mapping
    println!(
        "tags={:?}, scores={:?}",
        <Row as ColAt<0>>::data_type(),
        <Row as ColAt<1>>::data_type()
    );

    // Build a List<Utf8> column manually via ArrowBinding
    let mut lb = <List<String> as ArrowBinding>::new_builder(2);
    <List<String> as ArrowBinding>::append_value(&mut lb, &List::new(vec!["a".into(), "b".into()]));
    <List<String> as ArrowBinding>::append_value(&mut lb, &List::new(vec!["x".into()]));
    let la = <List<String> as ArrowBinding>::finish(lb);
    println!(
        "List<Utf8> len={}, first_list_len={}",
        la.len(),
        arrow_array::cast::as_list_array(&la).value_length(0)
    );

    // Build a nullable-list of nullable i32 items
    let mut nullable_builder = <List<Option<i32>> as ArrowBinding>::new_builder(2);
    <List<Option<i32>> as ArrowBinding>::append_value(
        &mut nullable_builder,
        &List::new(vec![Some(1), None]),
    );
    <List<Option<i32>> as ArrowBinding>::append_null(&mut nullable_builder);
    let nullable_arr = <List<Option<i32>> as ArrowBinding>::finish(nullable_builder);
    println!(
        "List<Nullable<i32>> len={}, second_is_null={}",
        nullable_arr.len(),
        nullable_arr.is_null(1)
    );
}
