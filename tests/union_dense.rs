use arrow_array::{Array, Int32Array, StringArray, UnionArray};
use typed_arrow::bridge::ArrowBinding;

#[derive(typed_arrow::Union)]
enum U {
    I(i32),
    S(String),
}

#[test]
fn union_dense_datatype_and_build() {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, UnionFields, UnionMode};

    // DataType check
    let fields: UnionFields = [
        (0_i8, Arc::new(Field::new("I", DataType::Int32, true))),
        (1_i8, Arc::new(Field::new("S", DataType::Utf8, true))),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        <U as ArrowBinding>::data_type(),
        DataType::Union(fields, UnionMode::Dense)
    );

    let mut b = <U as ArrowBinding>::new_builder(4);
    <U as ArrowBinding>::append_value(&mut b, &U::I(1));
    <U as ArrowBinding>::append_value(&mut b, &U::S("x".into()));
    <U as ArrowBinding>::append_null(&mut b);
    <U as ArrowBinding>::append_value(&mut b, &U::I(7));
    let arr: UnionArray = <U as ArrowBinding>::finish(b);
    assert_eq!(arr.len(), 4);

    // type_id and offset checks
    assert_eq!(arr.type_id(0), 0);
    assert_eq!(arr.value_offset(0), 0);
    assert_eq!(arr.type_id(1), 1);
    assert_eq!(arr.value_offset(1), 0);
    assert_eq!(arr.type_id(2), 0);
    assert_eq!(arr.value_offset(2), 1);
    assert_eq!(arr.type_id(3), 0);
    assert_eq!(arr.value_offset(3), 2);

    // value checks via direct child access
    let v0 = arr.value(0);
    assert_eq!(
        v0.as_any().downcast_ref::<Int32Array>().unwrap().value(0),
        1
    );
    let v1 = arr.value(1);
    assert_eq!(
        v1.as_any().downcast_ref::<StringArray>().unwrap().value(0),
        "x"
    );
    // Third row encodes null in first variant; check child array directly
    let ints = arr.child(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert!(ints.is_null(1));
    let v3 = arr.value(3);
    assert_eq!(
        v3.as_any().downcast_ref::<Int32Array>().unwrap().value(0),
        7
    );
}
