use std::{collections::BTreeMap, sync::Arc};

use typed_arrow::arrow_array::Array;
use typed_arrow::arrow_schema::{DataType, Field};
use typed_arrow::{OrderedMap, bridge::ArrowBinding};

#[test]
fn ordered_map_datatype() {
    let dt = <OrderedMap<String, i32> as ArrowBinding>::data_type();
    let key_f = Field::new("keys", DataType::Utf8, false);
    let val_f = Field::new("values", DataType::Int32, true);
    let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
    let expected = DataType::Map(Field::new("entries", entries, false).into(), true);
    assert_eq!(dt, expected);
}

#[test]
fn ordered_map_build() {
    let mut b = <OrderedMap<String, i32> as ArrowBinding>::new_builder(0);
    let mut m = BTreeMap::new();
    m.insert("b".to_string(), 2);
    m.insert("a".to_string(), 1);
    <OrderedMap<String, i32> as ArrowBinding>::append_value(&mut b, &OrderedMap::new(m));
    let a = <OrderedMap<String, i32> as ArrowBinding>::finish(b);
    let entries = a.entries();
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<typed_arrow::arrow_array::StringArray>()
        .unwrap();
    let vals = entries
        .column(1)
        .as_any()
        .downcast_ref::<typed_arrow::arrow_array::PrimitiveArray<typed_arrow::arrow_array::types::Int32Type>>()
        .unwrap();
    // BTreeMap order: a, b
    assert_eq!(keys.value(0), "a");
    assert_eq!(vals.value(0), 1);
    assert_eq!(keys.value(1), "b");
    assert_eq!(vals.value(1), 2);
}

#[test]
fn ordered_map_nullable_build() {
    let mut b = <OrderedMap<String, Option<i32>> as ArrowBinding>::new_builder(0);
    let mut m = BTreeMap::new();
    m.insert("x".to_string(), Some(1));
    m.insert("y".to_string(), None);
    <OrderedMap<String, Option<i32>> as ArrowBinding>::append_value(&mut b, &OrderedMap::new(m));
    let a = <OrderedMap<String, Option<i32>> as ArrowBinding>::finish(b);
    let entries = a.entries();
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<typed_arrow::arrow_array::StringArray>()
        .unwrap();
    let vals = entries
        .column(1)
        .as_any()
        .downcast_ref::<typed_arrow::arrow_array::PrimitiveArray<typed_arrow::arrow_array::types::Int32Type>>()
        .unwrap();
    assert_eq!(keys.value(0), "x");
    assert!(vals.is_valid(0));
    assert_eq!(vals.value(0), 1);
    assert_eq!(keys.value(1), "y");
    assert!(!vals.is_valid(1));
}
