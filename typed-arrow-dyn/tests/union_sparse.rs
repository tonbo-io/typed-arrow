use std::sync::Arc;

use typed_arrow_dyn::arrow_array::{Array, UnionArray, cast};
use typed_arrow_dyn::arrow_schema::{DataType, Field, Schema, UnionFields, UnionMode};
use typed_arrow_dyn::{DynBuilders, DynCell, DynError, DynRow};

#[test]
fn build_sparse_union_arrays() {
    let union_fields: UnionFields = [
        (0_i8, Arc::new(Field::new("int_val", DataType::Int32, true))),
        (1_i8, Arc::new(Field::new("text", DataType::Utf8, true))),
    ]
    .into_iter()
    .collect();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "u",
        DataType::Union(union_fields, UnionMode::Sparse),
        true,
    )]));

    let mut builders = DynBuilders::new(Arc::clone(&schema), 0);
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_value(
            0,
            DynCell::I32(7),
        ))])))
        .unwrap();
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_value(
            1,
            DynCell::Str("a".into()),
        ))])))
        .unwrap();
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_value(
            1,
            DynCell::Str("b".into()),
        ))])))
        .unwrap();
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_null(0))])))
        .unwrap();
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_null(1))])))
        .unwrap();

    let batch = builders.try_finish_into_batch().unwrap();
    let union = batch
        .column(0)
        .as_any()
        .downcast_ref::<UnionArray>()
        .unwrap();

    assert_eq!(&union.type_ids()[..], &[0, 1, 1, 0, 1]);
    assert!(union.offsets().is_none());

    let int_child = cast::as_primitive_array::<typed_arrow_dyn::arrow_array::types::Int32Type>(union.child(0));
    assert_eq!(int_child.len(), 5);
    assert_eq!(int_child.value(0), 7);
    assert!(int_child.is_null(1));
    assert!(int_child.is_null(2));
    assert!(int_child.is_null(3));
    assert!(int_child.is_null(4));

    let str_child = cast::as_string_array(union.child(1));
    assert!(str_child.is_null(0));
    assert_eq!(str_child.value(1), "a");
    assert_eq!(str_child.value(2), "b");
    assert!(str_child.is_null(3));
    assert!(str_child.is_null(4));
}

#[test]
fn sparse_union_nullability_violation() {
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
        DataType::Union(union_fields, UnionMode::Sparse),
        true,
    )]));

    let mut builders = DynBuilders::new(Arc::clone(&schema), 0);
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_value(
            0,
            DynCell::I32(1),
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
fn sparse_union_top_level_null_rejected() {
    let union_fields: UnionFields = [
        (0_i8, Arc::new(Field::new("int_val", DataType::Int32, true))),
        (1_i8, Arc::new(Field::new("text", DataType::Utf8, true))),
    ]
    .into_iter()
    .collect();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "u",
        DataType::Union(union_fields, UnionMode::Sparse),
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
fn sparse_union_unknown_tag_rejected() {
    let union_fields: UnionFields = [
        (0_i8, Arc::new(Field::new("int_val", DataType::Int32, true))),
        (1_i8, Arc::new(Field::new("text", DataType::Utf8, true))),
    ]
    .into_iter()
    .collect();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "u",
        DataType::Union(union_fields, UnionMode::Sparse),
        true,
    )]));

    let mut builders = DynBuilders::new(Arc::clone(&schema), 0);
    let err = builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_value(
            9,
            DynCell::I32(1),
        ))])))
        .unwrap_err();
    assert!(
        matches!(err, DynError::Append { .. }),
        "expected append error, got {err:?}"
    );
}
