//! Showcase: derive `Record`, inspect schema, build primitive columns.

use arrow_array::{cast, Array};
use typed_arrow::prelude::*;

#[derive(typed_arrow::Record)]
struct Person {
    id: i64,
    name: Option<String>,
    score: f32,
}

fn main() {
    // Compile-time schema info
    println!(
        "columns={}, id_type={:?}, name_type={:?}, score_type={:?}",
        <Person as Record>::LEN,
        <Person as ColAt<0>>::data_type(),
        <Person as ColAt<1>>::data_type(),
        <Person as ColAt<2>>::data_type(),
    );

    // Build arrays via associated ColumnBuilder/ColumnArray types
    type IdB = <Person as ColAt<0>>::ColumnBuilder;
    type IdA = <Person as ColAt<0>>::ColumnArray;
    let mut idb: IdB =
        arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Int64Type>::with_capacity(3);
    idb.append_value(10);
    idb.append_value(20);
    idb.append_value(30);
    let ida: IdA = idb.finish();

    type NameB = <Person as ColAt<1>>::ColumnBuilder;
    type NameA = <Person as ColAt<1>>::ColumnArray;
    let mut nb: NameB = arrow_array::builder::StringBuilder::with_capacity(3, 0);
    nb.append_value("alice");
    nb.append_null();
    nb.append_value("carol");
    let na: NameA = nb.finish();

    type ScoreB = <Person as ColAt<2>>::ColumnBuilder;
    type ScoreA = <Person as ColAt<2>>::ColumnArray;
    let mut sb: ScoreB =
        arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Float32Type>::with_capacity(3);
    sb.append_value(10.5);
    sb.append_value(20.0);
    sb.append_value(30.25);
    let sa: ScoreA = sb.finish();

    // Inspect values
    let id = cast::as_primitive_array::<arrow_array::types::Int64Type>(&ida);
    let name = arrow_array::cast::as_string_array(&na);
    let score = cast::as_primitive_array::<arrow_array::types::Float32Type>(&sa);
    println!(
        "rows={}, id[0]={}, name[1]_is_null={}, score[2]={}",
        ida.len(),
        id.value(0),
        name.is_null(1),
        score.value(2)
    );
}
