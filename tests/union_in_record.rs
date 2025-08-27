use arrow_array::Array;
use typed_arrow::{bridge::ArrowBinding, prelude::*};

#[derive(typed_arrow::Union)]
enum Value {
    I(i32),
    S(String),
}

#[derive(typed_arrow::Record)]
struct Test {
    value: Value,
}

#[test]
fn union_as_record_field_builders() {
    // Column types
    assert_eq!(<Test as Record>::LEN, 1);
    let dt = <Test as ColAt<0>>::data_type();
    // Ensure it is a Union(DataType::Int32 | Utf8, Dense)
    match dt {
        arrow_schema::DataType::Union(_, arrow_schema::UnionMode::Dense) => {}
        _ => panic!("unexpected datatype: {dt:?}"),
    }

    // Build via typed builder
    let mut b: <Test as ColAt<0>>::ColumnBuilder = <Value as ArrowBinding>::new_builder(3);
    <Value as ArrowBinding>::append_value(&mut b, &Value::I(1));
    <Value as ArrowBinding>::append_value(&mut b, &Value::S("x".into()));
    <Value as ArrowBinding>::append_null(&mut b);
    let a: <Test as ColAt<0>>::ColumnArray = <Value as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 3);
}
