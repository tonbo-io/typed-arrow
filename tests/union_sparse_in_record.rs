use arrow_array::Array;
use typed_arrow::{bridge::ArrowBinding, prelude::*};

#[derive(typed_arrow::Union)]
#[union(mode = "sparse")]
enum Value {
    I(i32),
    S(String),
}

#[derive(typed_arrow::Record)]
struct Row {
    id: i32,
    value: Value,
}

#[test]
fn union_sparse_as_record_field() {
    // Column types
    assert_eq!(<Row as Record>::LEN, 2);
    let dt = <Row as ColAt<1>>::data_type();
    match dt {
        arrow_schema::DataType::Union(_, arrow_schema::UnionMode::Sparse) => {}
        _ => panic!("unexpected datatype: {dt:?}"),
    }

    // Build via typed builder
    let mut b: <Row as ColAt<1>>::ColumnBuilder = <Value as ArrowBinding>::new_builder(3);
    <Value as ArrowBinding>::append_value(&mut b, &Value::I(1));
    <Value as ArrowBinding>::append_value(&mut b, &Value::S("x".into()));
    <Value as ArrowBinding>::append_null(&mut b);
    let a: <Row as ColAt<1>>::ColumnArray = <Value as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 3);
}
