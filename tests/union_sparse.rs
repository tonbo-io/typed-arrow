use arrow_array::{Array, Int32Array, StringArray, UnionArray};
use typed_arrow::bridge::ArrowBinding;

#[derive(typed_arrow::Union)]
#[union(mode = "sparse")] // default tags: I=0, S=1
enum U {
    I(i32),
    S(String),
}

#[test]
fn union_sparse_datatype_and_build() {
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
        DataType::Union(fields, UnionMode::Sparse)
    );

    let mut b = <U as ArrowBinding>::new_builder(4);
    <U as ArrowBinding>::append_value(&mut b, &U::I(1));
    <U as ArrowBinding>::append_value(&mut b, &U::S("x".into()));
    <U as ArrowBinding>::append_null(&mut b);
    <U as ArrowBinding>::append_value(&mut b, &U::I(7));
    let arr: UnionArray = <U as ArrowBinding>::finish(b);
    assert_eq!(arr.len(), 4);

    // type_id checks
    assert_eq!(arr.type_id(0), 0);
    assert_eq!(arr.type_id(1), 1);
    assert_eq!(arr.type_id(2), 0); // null encoded via first variant by default
    assert_eq!(arr.type_id(3), 0);

    // In sparse mode, children length == union length; index by row index
    let ints = arr.child(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ints.value(0), 1);
    assert!(ints.is_null(1));
    assert!(ints.is_null(2));
    assert_eq!(ints.value(3), 7);
    let strs = arr.child(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(strs.is_null(0));
    assert_eq!(strs.value(1), "x");
    assert!(strs.is_null(2));
    assert!(strs.is_null(3));
}

#[derive(typed_arrow::Union)]
#[union(mode = "sparse", tags(I = 10, S = 7))]
enum V {
    #[union(field = "int_value", null)]
    I(i32),
    #[union(field = "str_value")]
    S(String),
}

#[test]
fn union_sparse_custom_tags_and_null_carrier() {
    let mut b = <V as ArrowBinding>::new_builder(3);
    <V as ArrowBinding>::append_value(&mut b, &V::I(5));
    <V as ArrowBinding>::append_value(&mut b, &V::S("a".into()));
    <V as ArrowBinding>::append_null(&mut b);
    let arr: UnionArray = <V as ArrowBinding>::finish(b);
    assert_eq!(arr.type_id(0), 10);
    assert_eq!(arr.type_id(1), 7);
    assert_eq!(arr.type_id(2), 10); // null encoded via I

    let ints = arr.child(10).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ints.value(0), 5);
    assert!(ints.is_null(1));
    assert!(ints.is_null(2));
    let strs = arr.child(7).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(strs.is_null(0));
    assert_eq!(strs.value(1), "a");
    assert!(strs.is_null(2));
}
