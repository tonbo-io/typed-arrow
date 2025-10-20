use std::sync::Arc;

use arrow_array::{cast, Array, MapArray};
use arrow_schema::{DataType, Field, Fields, Schema};
use typed_arrow_dyn::{DynBuilders, DynCell, DynError, DynRow};

fn map_field(value_nullable: bool, ordered: bool) -> Field {
    let entry_fields = Fields::from(vec![
        Arc::new(Field::new("keys", DataType::Utf8, false)),
        Arc::new(Field::new("values", DataType::Int32, value_nullable)),
    ]);
    let entry = Arc::new(Field::new("entries", DataType::Struct(entry_fields), false));
    Field::new("metadata", DataType::Map(entry, ordered), true)
}

#[test]
fn build_map_arrays() {
    let schema = Arc::new(Schema::new(vec![map_field(true, false)]));

    let mut builders = DynBuilders::new(Arc::clone(&schema), 0);
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::Map(vec![
            (DynCell::Str("author".into()), Some(DynCell::I32(1))),
            (DynCell::Str("editor".into()), None),
        ]))])))
        .unwrap();
    builders
        .append_option_row(Some(DynRow(vec![None])))
        .unwrap();

    let batch = builders.try_finish_into_batch().unwrap();
    assert_eq!(batch.num_rows(), 2);
    let map = batch
        .column(0)
        .as_any()
        .downcast_ref::<MapArray>()
        .unwrap();

    assert_eq!(map.len(), 2);
    assert!(!map.is_null(0));
    assert!(map.is_null(1));
    assert_eq!(map.value_offsets(), &[0, 2, 2]);
    assert_eq!(map.data_type(), schema.field(0).data_type());

    let keys = cast::as_string_array(map.keys());
    assert_eq!(keys.value(0), "author");
    assert_eq!(keys.value(1), "editor");

    let values = cast::as_primitive_array::<arrow_array::types::Int32Type>(map.values());
    assert_eq!(values.len(), 2);
    assert_eq!(values.value(0), 1);
    assert!(values.is_null(1));
}

#[test]
fn build_ordered_map_arrays() {
    let schema = Arc::new(Schema::new(vec![map_field(true, true)]));
    let mut builders = DynBuilders::new(Arc::clone(&schema), 0);
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::Map(vec![
            (DynCell::Str("a".into()), Some(DynCell::I32(10))),
            (DynCell::Str("b".into()), Some(DynCell::I32(20))),
        ]))])))
        .unwrap();

    let batch = builders.try_finish_into_batch().unwrap();
    let map = batch
        .column(0)
        .as_any()
        .downcast_ref::<MapArray>()
        .unwrap();
    assert_eq!(map.data_type(), schema.field(0).data_type());
}

#[test]
fn reject_null_map_key() {
    let schema = Arc::new(Schema::new(vec![map_field(true, false)]));
    let mut builders = DynBuilders::new(schema, 0);
    let err = builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::Map(vec![
            (DynCell::Null, Some(DynCell::I32(5))),
        ]))])))
        .unwrap_err();
    assert!(matches!(err, DynError::Append { .. }));
}

#[test]
fn map_value_nullability_enforced() {
    let schema = Arc::new(Schema::new(vec![map_field(false, false)]));
    let mut builders = DynBuilders::new(Arc::clone(&schema), 0);
    let err = builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::Map(vec![
            (DynCell::Str("missing".into()), None),
        ]))])))
        .unwrap_err();
    assert!(matches!(err, DynError::Append { .. }));
}
