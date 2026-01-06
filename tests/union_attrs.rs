use typed_arrow::arrow_array::{Array, Int32Array, StringArray, UnionArray};
use typed_arrow::{bridge::ArrowBinding, prelude::*};

#[derive(Union)]
#[union(tags(I = 10, S = 7))]
enum U {
    #[union(field = "int_value")]
    I(i32),
    #[union(field = "str_value")]
    S(String),
}

#[test]
fn union_attrs_datatype_and_tags_fields() {
    use std::sync::Arc;

    use typed_arrow::arrow_schema::{DataType, Field, UnionFields, UnionMode};

    let fields: UnionFields = [
        (
            10_i8,
            Arc::new(Field::new("int_value", DataType::Int32, true)),
        ),
        (
            7_i8,
            Arc::new(Field::new("str_value", DataType::Utf8, true)),
        ),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        <U as ArrowBinding>::data_type(),
        DataType::Union(fields, UnionMode::Dense)
    );

    let mut b = <U as ArrowBinding>::new_builder(3);
    <U as ArrowBinding>::append_value(&mut b, &U::S("x".into())); // tag 7
    <U as ArrowBinding>::append_value(&mut b, &U::I(1)); // tag 10
    <U as ArrowBinding>::append_null(&mut b); // default null carrier: first variant (I, tag 10)
    let arr: UnionArray = <U as ArrowBinding>::finish(b);

    assert_eq!(arr.type_id(0), 7);
    assert_eq!(arr.type_id(1), 10);
    assert_eq!(arr.type_id(2), 10);
    // Verify child field names via DataType
    match <U as ArrowBinding>::data_type() {
        DataType::Union(flds, UnionMode::Dense) => {
            let v: Vec<(i8, String)> = flds.iter().map(|(t, f)| (t, f.name().clone())).collect();
            assert_eq!(v, vec![(10, "int_value".into()), (7, "str_value".into())]);
        }
        dt => panic!("unexpected datatype: {dt:?}"),
    }
}

#[derive(Union)]
enum V {
    #[union(tag = 42, field = "num", null)]
    I(i32),
    S(String),
}

#[test]
fn union_variant_level_attrs_and_null_carrier() {
    let mut b = <V as ArrowBinding>::new_builder(3);
    <V as ArrowBinding>::append_value(&mut b, &V::I(5));
    <V as ArrowBinding>::append_value(&mut b, &V::S("a".into()));
    <V as ArrowBinding>::append_null(&mut b); // should null in variant I (tag 42)
    let arr: UnionArray = <V as ArrowBinding>::finish(b);
    assert_eq!(arr.type_id(0), 42);
    // auto tag for S: any i8 other than 42 is acceptable
    assert_ne!(arr.type_id(1), 42);
    assert_eq!(arr.type_id(2), 42); // null encoded into I
    // Check offsets increment for I
    assert_eq!(arr.value_offset(0), 0);
    assert_eq!(arr.value_offset(2), 1);
    // Check child arrays directly by looking up field order
    let (num_tid, str_tid) = match <V as ArrowBinding>::data_type() {
        typed_arrow::arrow_schema::DataType::Union(fields, _) => {
            let pairs: Vec<(i8, String)> =
                fields.iter().map(|(t, f)| (t, f.name().clone())).collect();
            let n = pairs
                .iter()
                .find(|(_, n)| n == "num")
                .map(|(t, _)| *t)
                .unwrap();
            let s = pairs
                .iter()
                .find(|(_, n)| n == "S")
                .map(|(t, _)| *t)
                .unwrap();
            (n, s)
        }
        _ => unreachable!(),
    };
    let ints = arr
        .child(num_tid)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ints.value(0), 5);
    assert!(ints.is_null(1));
    let strs = arr
        .child(str_tid)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(strs.value(0), "a");
}
