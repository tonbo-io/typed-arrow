//! Showcase: derive `Record`, inspect schema, build primitive columns.

use arrow_array::{Array, cast};
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
    let mut id_builder: <Person as ColAt<0>>::ColumnBuilder =
        arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Int64Type>::with_capacity(3);
    id_builder.append_value(10);
    id_builder.append_value(20);
    id_builder.append_value(30);
    let id_array: <Person as ColAt<0>>::ColumnArray = id_builder.finish();

    let mut nb: <Person as ColAt<1>>::ColumnBuilder =
        arrow_array::builder::StringBuilder::with_capacity(3, 0);
    nb.append_value("alice");
    nb.append_null();
    nb.append_value("carol");
    let na: <Person as ColAt<1>>::ColumnArray = nb.finish();
    let mut sb: <Person as ColAt<2>>::ColumnBuilder =
        arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Float32Type>::with_capacity(3);
    sb.append_value(10.5);
    sb.append_value(20.0);
    sb.append_value(30.25);
    let sa: <Person as ColAt<2>>::ColumnArray = sb.finish();

    // Inspect values
    let id = cast::as_primitive_array::<arrow_array::types::Int64Type>(&id_array);
    let name = arrow_array::cast::as_string_array(&na);
    let score = cast::as_primitive_array::<arrow_array::types::Float32Type>(&sa);
    println!(
        "rows={}, id[0]={}, name[1]_is_null={}, score[2]={}",
        id_array.len(),
        id.value(0),
        name.is_null(1),
        score.value(2)
    );
}
