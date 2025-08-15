use arrow_array::Array;
use arrow_native::{bridge::ArrowBinding, prelude::*};

#[derive(arrow_native::Union)]
enum Value {
    I(i32),
    S(String),
}

#[derive(arrow_native::Record)]
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
    type B0 = <Test as ColAt<0>>::ColumnBuilder; // == <Value as ArrowBinding>::Builder
    type A0 = <Test as ColAt<0>>::ColumnArray; // == UnionArray
    let mut b: B0 = <Value as ArrowBinding>::new_builder(3);
    <Value as ArrowBinding>::append_value(&mut b, &Value::I(1));
    <Value as ArrowBinding>::append_value(&mut b, &Value::S("x".into()));
    <Value as ArrowBinding>::append_null(&mut b);
    let a: A0 = <Value as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 3);
}
