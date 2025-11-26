use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, MapArray, RecordBatch, StringArray, StructArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Fields, Schema};

use super::{
    DynCellRaw, DynMapView, DynRowOwned, DynRowView, DynStructView, path::Path,
    projection::StructProjection,
};
use crate::{DynViewError, cell::DynCell};

#[test]
fn dyn_row_owned_round_trip_utf8() {
    let fields = Fields::from(vec![Arc::new(Field::new("id", DataType::Utf8, false))]);
    let row =
        DynRowOwned::try_new(fields.clone(), vec![Some(DynCell::Str("hello".into()))]).unwrap();
    let raw = row.as_raw().unwrap();
    assert!(matches!(raw.cells()[0], Some(DynCellRaw::Str { .. })));

    let rebuilt = DynRowOwned::from_raw(&raw).unwrap();
    assert_eq!(rebuilt.len(), 1);
    assert!(matches!(rebuilt.cells()[0], Some(DynCell::Str(_))));
}

#[test]
fn dyn_cell_as_ref_scalars() {
    let c_bool = DynCell::Bool(true);
    let c_str = DynCell::Str("hi".into());
    let c_bin = DynCell::Bin(vec![1, 2, 3]);

    let r_bool = c_bool.as_ref().unwrap();
    assert_eq!(r_bool.as_bool(), Some(true));

    let r_str = c_str.as_ref().unwrap();
    assert_eq!(r_str.as_str(), Some("hi"));

    let r_bin = c_bin.as_ref().unwrap();
    assert_eq!(r_bin.as_bin(), Some(&[1, 2, 3][..]));
}

#[test]
fn dyn_cell_as_ref_rejects_nested() {
    assert!(DynCell::List(Vec::new()).as_ref().is_none());
    assert!(DynCell::Struct(Vec::new()).as_ref().is_none());
    assert!(
        DynCell::Union {
            type_id: 0,
            value: None
        }
        .as_ref()
        .is_none()
    );
}

#[test]
fn dyn_row_owned_rejects_nested() {
    let fields = Fields::from(vec![Arc::new(Field::new("map", DataType::Binary, false))]);
    let row = DynRowOwned::try_new(fields, vec![Some(DynCell::Map(Vec::new()))]).unwrap();
    assert!(row.as_raw().is_err());
}

#[test]
fn dyn_map_view_errors_when_schema_missing_key_field() {
    let map = sample_map_array();
    let entry_fields = Fields::from(Vec::<Arc<Field>>::new());
    let view =
        DynMapView::with_projection(&map, entry_fields, Path::new(0, "map"), 0, None).unwrap();
    match view.get(0) {
        Err(DynViewError::Invalid { message, .. }) => {
            assert!(
                message.contains("map schema missing key field"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected invalid error, got {other:?}"),
    }
}

#[test]
fn dyn_map_view_errors_when_schema_missing_value_field() {
    let map = sample_map_array();
    let entry_fields = Fields::from(vec![Arc::new(Field::new("key", DataType::Utf8, false))]);
    let view =
        DynMapView::with_projection(&map, entry_fields, Path::new(0, "map"), 0, None).unwrap();
    match view.get(0) {
        Err(DynViewError::Invalid { message, .. }) => {
            assert!(
                message.contains("map schema missing value field"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected invalid error, got {other:?}"),
    }
}

#[test]
fn dyn_map_view_errors_when_projection_missing_children() {
    let map = sample_map_array();
    let entry_fields = Fields::from(vec![
        Arc::new(Field::new("keys", DataType::Utf8, false)),
        Arc::new(Field::new("values", DataType::Int32, true)),
    ]);
    let projection = Arc::new(StructProjection {
        children: Arc::from(Vec::new()),
    });
    let view =
        DynMapView::with_projection(&map, entry_fields, Path::new(0, "map"), 0, Some(projection))
            .unwrap();
    match view.get(0) {
        Err(DynViewError::Invalid { message, .. }) => {
            assert!(
                message.contains("map projection missing key child"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected invalid error, got {other:?}"),
    }
}

fn sample_map_array() -> MapArray {
    let entry_fields = Fields::from(vec![
        Arc::new(Field::new("keys", DataType::Utf8, false)),
        Arc::new(Field::new("values", DataType::Int32, true)),
    ]);
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
    let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(1)]));
    let entries = StructArray::new(entry_fields.clone(), vec![keys, values], None);
    let entry_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(entry_fields.clone()),
        false,
    ));
    let offsets = OffsetBuffer::new(vec![0i32, 1].into());
    MapArray::new(entry_field, offsets, entries, None, false)
}

#[test]
fn row_view_errors_when_projector_width_mismatch() {
    let field = Arc::new(Field::new("id", DataType::Int32, false));
    let schema = Arc::new(Schema::new(vec![field.as_ref().clone()]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
    )
    .unwrap();
    let view = DynRowView::new_for_testing(
        &batch,
        Fields::from(vec![Arc::clone(&field)]),
        Some(Arc::from(vec![0])),
        Some(Arc::from(Vec::new())),
        0,
    );
    match view.get(0) {
        Err(DynViewError::Invalid { message, .. }) => {
            assert!(message.contains("projection width mismatch"));
        }
        other => panic!("expected mismatch error, got {other:?}"),
    }
}

#[test]
fn struct_view_errors_when_index_missing() {
    let fields = Fields::from(vec![Arc::new(Field::new("a", DataType::Int32, false))]);
    let struct_array = StructArray::new(
        fields.clone(),
        vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        None,
    );
    let view = DynStructView {
        array: &struct_array,
        fields,
        row: 0,
        base_path: Path::new(0, "root"),
        projection: None,
    };
    match view.get(1) {
        Err(DynViewError::ColumnOutOfBounds { column, .. }) => assert_eq!(column, 1),
        other => panic!("expected column out of bounds, got {other:?}"),
    }
}
