use std::sync::Arc;

use arrow_array::{Array, UnionArray, cast};
use arrow_schema::{DataType, Field, Schema, UnionFields, UnionMode};
use typed_arrow_dyn::{DynBuilders, DynCell, DynError, DynRow};

#[test]
fn build_dense_union_arrays() {
    let union_fields: UnionFields = [
        (0_i8, Arc::new(Field::new("int_val", DataType::Int32, true))),
        (1_i8, Arc::new(Field::new("text", DataType::Utf8, true))),
    ]
    .into_iter()
    .collect();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "u",
        DataType::Union(union_fields, UnionMode::Dense),
        true,
    )]));

    let mut builders = DynBuilders::new(Arc::clone(&schema), 0);
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_value(
            0,
            DynCell::I32(42),
        ))])))
        .unwrap();
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_value(
            1,
            DynCell::Str("cats".into()),
        ))])))
        .unwrap();
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_value(
            1,
            DynCell::Str("dogs".into()),
        ))])))
        .unwrap();
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_value(
            0,
            DynCell::I32(-7),
        ))])))
        .unwrap();

    let batch = builders.try_finish_into_batch().unwrap();
    assert_eq!(batch.num_rows(), 4);
    let union = batch
        .column(0)
        .as_any()
        .downcast_ref::<UnionArray>()
        .unwrap();

    assert_eq!(union.len(), 4);
    assert_eq!(&union.type_ids()[..], &[0, 1, 1, 0]);
    assert_eq!(&union.offsets().unwrap()[..], &[0, 0, 1, 1]);

    let int_child = cast::as_primitive_array::<arrow_array::types::Int32Type>(union.child(0));
    assert_eq!(int_child.len(), 2);
    assert_eq!(int_child.value(0), 42);
    assert_eq!(int_child.value(1), -7);

    let str_child = cast::as_string_array(union.child(1));
    assert_eq!(str_child.value(0), "cats");
    assert_eq!(str_child.value(1), "dogs");
}

#[test]
fn dense_union_nullability_violation() {
    let union_fields: UnionFields = [
        (
            0_i8,
            Arc::new(Field::new("int_val", DataType::Int32, false)),
        ),
        (1_i8, Arc::new(Field::new("text", DataType::Utf8, true))),
    ]
    .into_iter()
    .collect();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "u",
        DataType::Union(union_fields, UnionMode::Dense),
        true,
    )]));

    let mut builders = DynBuilders::new(Arc::clone(&schema), 0);
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_value(
            0,
            DynCell::I32(10),
        ))])))
        .unwrap();
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_null(0))])))
        .unwrap();

    let err = builders.try_finish_into_batch().unwrap_err();
    match err {
        DynError::Nullability { path, .. } => {
            assert_eq!(path, "u.int_val");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn dense_union_top_level_null_rejected() {
    let union_fields: UnionFields = [
        (0_i8, Arc::new(Field::new("int_val", DataType::Int32, true))),
        (1_i8, Arc::new(Field::new("text", DataType::Utf8, true))),
    ]
    .into_iter()
    .collect();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "u",
        DataType::Union(union_fields, UnionMode::Dense),
        false,
    )]));

    let mut builders = DynBuilders::new(Arc::clone(&schema), 0);
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_value(
            0,
            DynCell::I32(1),
        ))])))
        .unwrap();
    builders
        .append_option_row(Some(DynRow(vec![None])))
        .unwrap();

    let err = builders.try_finish_into_batch().unwrap_err();
    match err {
        DynError::Nullability { path, index, .. } => {
            assert_eq!(path, "u");
            assert_eq!(index, 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn dense_union_unknown_tag_rejected() {
    let union_fields: UnionFields = [
        (0_i8, Arc::new(Field::new("int_val", DataType::Int32, true))),
        (1_i8, Arc::new(Field::new("text", DataType::Utf8, true))),
    ]
    .into_iter()
    .collect();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "u",
        DataType::Union(union_fields, UnionMode::Dense),
        true,
    )]));

    let mut builders = DynBuilders::new(Arc::clone(&schema), 0);
    let err = builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_value(
            5,
            DynCell::I32(1),
        ))])))
        .unwrap_err();
    assert!(
        matches!(err, DynError::Append { .. }),
        "expected append error, got {err:?}"
    );
}
