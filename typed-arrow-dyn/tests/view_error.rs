use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema};
use typed_arrow_dyn::{DynProjection, DynViewError};

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
        Ok(_) => panic!("expected invalid projection error, got success"),
        Err(err) => panic!("unexpected error: {err:?}"),
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
        Ok(_) => panic!("expected invalid projection error, got success"),
        Err(err) => panic!("unexpected error: {err:?}"),
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
        Ok(_) => panic!("expected invalid projection error, got success"),
        Err(err) => panic!("unexpected error: {err:?}"),
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
        Ok(_) => panic!("expected invalid projection error, got success"),
        Err(err) => panic!("unexpected error: {err:?}"),
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
