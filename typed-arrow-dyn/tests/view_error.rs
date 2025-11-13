use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema};
use typed_arrow_dyn::{DynProjection, DynSchema, DynViewError};

#[test]
fn map_projection_rejects_reordered_entry_fields() {
    let source = canonical_map_schema();
    let value_first = Fields::from(vec![
        Arc::new(Field::new("value", DataType::Utf8, true)),
        Arc::new(Field::new("key", DataType::Utf8, false)),
    ]);
    let projection = map_projection_schema(value_first);
    match DynProjection::from_schema(source.as_ref(), &projection) {
        Err(DynViewError::Invalid { message, .. }) => {
            assert!(
                message.contains("key field before the value"),
                "unexpected message: {message}"
            );
        }
        Err(err) => panic!("unexpected error: {err}"),
        Ok(_) => panic!("expected invalid projection error, got success"),
    }
}

#[test]
fn map_projection_rejects_duplicate_key_field() {
    let source = canonical_map_schema();
    let duplicate_key = Fields::from(vec![
        Arc::new(Field::new("key", DataType::Utf8, false)),
        Arc::new(Field::new("key", DataType::Utf8, false)),
    ]);
    let projection = map_projection_schema(duplicate_key);
    match DynProjection::from_schema(source.as_ref(), &projection) {
        Err(DynViewError::Invalid { message, .. }) => {
            assert!(
                message.contains("key field before the value"),
                "unexpected message: {message}"
            );
        }
        Err(err) => panic!("unexpected error: {err}"),
        Ok(_) => panic!("expected invalid projection error, got success"),
    }
}

#[test]
fn map_projection_rejects_missing_value_field() {
    let source = canonical_map_schema();
    let missing_value = Fields::from(vec![Arc::new(Field::new("key", DataType::Utf8, false))]);
    let projection = map_projection_schema(missing_value);
    match DynProjection::from_schema(source.as_ref(), &projection) {
        Err(DynViewError::Invalid { message, .. }) => {
            assert!(
                message.contains("exactly two fields"),
                "unexpected message: {message}"
            );
        }
        Err(err) => panic!("unexpected error: {err}"),
        Ok(_) => panic!("expected invalid projection error, got success"),
    }
}

#[test]
fn map_projection_rejects_non_struct_entry() {
    let source = canonical_map_schema();
    let projection = Schema::new(vec![Field::new(
        "map",
        DataType::Map(
            Arc::new(Field::new("entries", DataType::Utf8, false)),
            false,
        ),
        true,
    )]);
    match DynProjection::from_schema(source.as_ref(), &projection) {
        Err(DynViewError::Invalid { message, .. }) => {
            assert!(
                message.contains("must be a struct"),
                "unexpected message: {message}"
            );
        }
        Err(err) => panic!("unexpected error: {err}"),
        Ok(_) => panic!("expected invalid projection error, got success"),
    }
}

#[test]
fn row_view_reports_column_out_of_bounds() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
    )
    .unwrap();
    let dyn_schema = DynSchema::from_ref(schema);
    let mut rows = dyn_schema.iter_views(&batch).unwrap();
    let view = rows.next().unwrap().unwrap();
    match view.get(1) {
        Err(DynViewError::ColumnOutOfBounds { column, .. }) => assert_eq!(column, 1),
        Ok(_) => panic!("expected column out of bounds"),
        Err(err) => panic!("unexpected error: {err}"),
    }
}

#[test]
fn struct_view_reports_column_out_of_bounds() {
    let inner_fields = Fields::from(vec![Arc::new(Field::new("name", DataType::Utf8, false))]);
    let struct_array = StructArray::new(
        inner_fields.clone(),
        vec![Arc::new(StringArray::from(vec!["alice"])) as ArrayRef],
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "user",
        DataType::Struct(inner_fields),
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(struct_array) as ArrayRef],
    )
    .unwrap();
    let dyn_schema = DynSchema::from_ref(schema);
    let mut rows = dyn_schema.iter_views(&batch).unwrap();
    let view = rows.next().unwrap().unwrap();
    let struct_view = view.get(0).unwrap().unwrap().into_struct().unwrap();
    match struct_view.get(1) {
        Err(DynViewError::ColumnOutOfBounds { column, .. }) => assert_eq!(column, 1),
        Ok(_) => panic!("expected column out of bounds"),
        Err(err) => panic!("unexpected error: {err}"),
    }
}

#[test]
fn nested_struct_projection_missing_child_errors() {
    let source_struct = Fields::from(vec![Arc::new(Field::new("a", DataType::Int32, false))]);
    let source = Arc::new(Schema::new(vec![Field::new(
        "root",
        DataType::Struct(source_struct),
        false,
    )]));
    let projection_struct = Fields::from(vec![Arc::new(Field::new(
        "missing",
        DataType::Int32,
        false,
    ))]);
    let projection = Schema::new(vec![Field::new(
        "root",
        DataType::Struct(projection_struct),
        false,
    )]);
    match DynProjection::from_schema(source.as_ref(), &projection) {
        Err(DynViewError::Invalid { message, .. }) => {
            assert!(
                message.contains("field not found"),
                "unexpected message: {message}"
            );
        }
        Err(err) => panic!("unexpected error: {err}"),
        Ok(_) => panic!("expected invalid projection error, got success"),
    }
}

fn canonical_map_schema() -> Arc<Schema> {
    let key = Arc::new(Field::new("key", DataType::Utf8, false));
    let value = Arc::new(Field::new("value", DataType::Utf8, true));
    let entry_struct = Fields::from(vec![Arc::clone(&key), Arc::clone(&value)]);
    let entry_field = Arc::new(Field::new("entries", DataType::Struct(entry_struct), false));
    let map_field = Field::new("map", DataType::Map(entry_field, false), true);
    Arc::new(Schema::new(vec![map_field]))
}

fn map_projection_schema(entry_fields: Fields) -> Schema {
    Schema::new(vec![Field::new(
        "map",
        DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(entry_fields), false)),
            false,
        ),
        true,
    )])
}
