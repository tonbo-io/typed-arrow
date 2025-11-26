use std::sync::Arc;

use arrow_array::{Array, MapArray, cast};
use arrow_schema::{DataType, Field, Schema};
use typed_arrow_dyn::{DynBuilders, DynCell, DynError, DynRow};

fn map_field(
    column_name: &str,
    key_type: DataType,
    value_type: DataType,
    value_nullable: bool,
    ordered: bool,
    column_nullable: bool,
) -> Field {
    let entries_struct = DataType::Struct(
        vec![
            Arc::new(Field::new("keys", key_type, false)),
            Arc::new(Field::new("values", value_type, value_nullable)),
        ]
        .into(),
    );
    Field::new(
        column_name,
        DataType::Map(Field::new("entries", entries_struct, false).into(), ordered),
        column_nullable,
    )
}

#[test]
fn append_map_rows() {
    let map_field = map_field("data", DataType::Utf8, DataType::Int64, true, false, true);
    let schema = Arc::new(Schema::new(vec![map_field]));

    let mut builders = DynBuilders::new(Arc::clone(&schema), 3);

    let row0 = DynRow(vec![Some(DynCell::Map(vec![
        (DynCell::Str("a".into()), Some(DynCell::I64(1))),
        (DynCell::Str("b".into()), None),
    ]))]);
    builders.append_option_row(Some(row0)).unwrap();

    let row1 = DynRow(vec![Some(DynCell::Map(vec![(
        DynCell::Str("c".into()),
        Some(DynCell::I64(3)),
    )]))]);
    builders.append_option_row(Some(row1)).unwrap();
    builders.append_option_row(None).unwrap();

    let batch = builders.try_finish_into_batch().unwrap();
    assert_eq!(batch.num_rows(), 3);

    let map = batch
        .column(0)
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("map column");

    assert_eq!(map.value_offsets(), &[0, 2, 3, 3]);

    let keys = cast::as_string_array(map.keys());
    let values = cast::as_primitive_array::<arrow_array::types::Int64Type>(map.values());

    assert_eq!(keys.value(0), "a");
    assert_eq!(keys.value(1), "b");
    assert_eq!(keys.value(2), "c");

    assert_eq!(values.value(0), 1);
    assert!(values.is_null(1));
    assert_eq!(values.value(2), 3);
}

#[test]
fn reject_null_map_key() {
    let map_field = map_field("data", DataType::Utf8, DataType::Int64, true, false, true);
    let schema = Arc::new(Schema::new(vec![map_field]));
    let mut builders = DynBuilders::new(Arc::clone(&schema), 1);

    let row = DynRow(vec![Some(DynCell::Map(vec![(
        DynCell::Null,
        Some(DynCell::I64(1)),
    )]))]);
    let err = builders.append_option_row(Some(row)).unwrap_err();

    match err {
        DynError::Append { message, .. } => {
            assert!(message.contains("map key"), "unexpected message: {message}");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn reject_null_value_when_not_nullable() {
    let map_field = map_field("data", DataType::Utf8, DataType::Int64, false, false, true);
    let schema = Arc::new(Schema::new(vec![map_field]));
    let mut builders = DynBuilders::new(Arc::clone(&schema), 1);

    let row = DynRow(vec![Some(DynCell::Map(vec![(
        DynCell::Str("k".into()),
        None,
    )]))]);
    let err = builders.append_option_row(Some(row)).unwrap_err();

    match err {
        DynError::Append { message, .. } => {
            assert!(
                message.contains("not nullable"),
                "unexpected message: {message}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn ordered_map_preserves_flag() {
    let map_field = map_field("data", DataType::Utf8, DataType::Int64, true, true, true);
    let schema = Arc::new(Schema::new(vec![map_field]));
    let mut builders = DynBuilders::new(Arc::clone(&schema), 1);

    let row = DynRow(vec![Some(DynCell::Map(vec![
        (DynCell::Str("a".into()), Some(DynCell::I64(1))),
        (DynCell::Str("b".into()), Some(DynCell::I64(2))),
    ]))]);
    builders.append_option_row(Some(row)).unwrap();

    let batch = builders.try_finish_into_batch().unwrap();
    let map = batch
        .column(0)
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("map column");

    match map.data_type() {
        DataType::Map(_, ordered) => assert!(ordered),
        other => panic!("unexpected data type: {other:?}"),
    }
}
