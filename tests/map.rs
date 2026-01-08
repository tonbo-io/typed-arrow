use std::sync::Arc;

use arrow_array::Array;
use arrow_schema::{DataType, Field};
use typed_arrow::{Map, arrow_array, arrow_schema, bridge::ArrowBinding};

#[test]
fn map_datatype_shapes_and_sorted_flag() {
    // Non-nullable values
    let dt_nn = <Map<String, i32> as ArrowBinding>::data_type();
    let key_f = Field::new("keys", DataType::Utf8, false);
    // Value is declared nullable by MapBuilder; align expectations
    let val_f = Field::new("values", DataType::Int32, true);
    let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
    let expected = DataType::Map(Field::new("entries", entries, false).into(), false);
    assert_eq!(dt_nn, expected);

    // Nullable values via Option
    let dt_n = <Map<String, Option<i64>> as ArrowBinding>::data_type();
    let key_f = Field::new("keys", DataType::Utf8, false);
    let val_f = Field::new("values", DataType::Int64, true);
    let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
    let expected = DataType::Map(Field::new("entries", entries, false).into(), false);
    assert_eq!(dt_n, expected);

    // Sorted keys flag
    let dt_sorted = <Map<String, i32, true> as ArrowBinding>::data_type();
    let key_f = Field::new("keys", DataType::Utf8, false);
    let val_f = Field::new("values", DataType::Int32, true);
    let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
    let expected_sorted = DataType::Map(Field::new("entries", entries, false).into(), true);
    assert_eq!(dt_sorted, expected_sorted);
}

#[test]
fn map_append_and_lengths() {
    // Build a map column with two rows: one with two entries and one null
    let mut b = <Map<String, i32> as ArrowBinding>::new_builder(0);
    typed_arrow::bridge::ArrowBinding::append_value(
        &mut b,
        &Map::<String, i32, false>::new(vec![("a".to_string(), 1), ("b".to_string(), 2)]),
    );
    <Map<String, i32, false> as typed_arrow::bridge::ArrowBinding>::append_null(&mut b);
    let a = <Map<String, i32> as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 2);

    // Entries should total 2
    let entries = a.entries();
    assert_eq!(entries.len(), 2);

    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let vals = entries
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<arrow_array::types::Int32Type>>()
        .unwrap();
    assert_eq!(keys.value(0), "a");
    assert_eq!(keys.value(1), "b");
    assert_eq!(vals.value(0), 1);
    assert_eq!(vals.value(1), 2);
}

#[test]
fn map_option_values_append() {
    let mut b = <Map<String, Option<i32>> as ArrowBinding>::new_builder(0);
    typed_arrow::bridge::ArrowBinding::append_value(
        &mut b,
        &Map::<String, Option<i32>, false>::new(vec![
            ("a".to_string(), Some(1)),
            ("b".to_string(), None),
        ]),
    );
    let a = <Map<String, Option<i32>> as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 1);
    let entries = a.entries();
    assert_eq!(entries.len(), 2);
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let vals = entries
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<arrow_array::types::Int32Type>>()
        .unwrap();
    assert_eq!(keys.value(0), "a");
    assert!(vals.is_valid(0));
    assert_eq!(vals.value(0), 1);
    assert_eq!(keys.value(1), "b");
    assert!(!vals.is_valid(1));
}
