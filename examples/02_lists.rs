//! Showcase: List and item-nullable lists via List<Option<T>> for Arrow ListArray.

use arrow_array::Array;
use arrow_native::{bridge::ArrowBinding, prelude::*, List};

#[derive(arrow_native::Record)]
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
    <List<String> as ArrowBinding>::append_value(&mut lb, &List(vec!["a".into(), "b".into()]));
    <List<String> as ArrowBinding>::append_value(&mut lb, &List(vec!["x".into()]));
    let la = <List<String> as ArrowBinding>::finish(lb);
    println!(
        "List<Utf8> len={}, first_list_len={}",
        la.len(),
        arrow_array::cast::as_list_array(&la).value_length(0)
    );

    // Build a nullable-list of nullable i32 items
    let mut nlb = <List<Option<i32>> as ArrowBinding>::new_builder(2);
    <List<Option<i32>> as ArrowBinding>::append_value(&mut nlb, &List(vec![Some(1), None]));
    <List<Option<i32>> as ArrowBinding>::append_null(&mut nlb);
    let nla = <List<Option<i32>> as ArrowBinding>::finish(nlb);
    println!(
        "List<Nullable<i32>> len={}, second_is_null={}",
        nla.len(),
        nla.is_null(1)
    );
}
