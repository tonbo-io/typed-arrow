use std::sync::Arc;

use arrow_array::{Array, UnionArray, cast};
use arrow_schema::{DataType, Field, Schema, UnionFields, UnionMode};
use typed_arrow_dyn::{DynBuilders, DynCell, DynError, DynRow};

#[test]
fn union_payload_type_mismatch() {
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
    let err = builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_value(
            0,
            DynCell::Str("wrong".into()),
        ))])))
        .unwrap_err();
    match err {
        DynError::Append { col, message } => {
            assert_eq!(col, 0);
            assert!(
                message.contains("type mismatch"),
                "unexpected message: {message}"
            );
        }
        other => panic!("unexpected error kind: {other:?}"),
    }
}

#[test]
fn union_nullable_variant_type_mismatch() {
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
            0,
            DynCell::Str("wrong".into()),
        ))])))
        .unwrap_err();
    match err {
        DynError::Append { col, message } => {
            assert_eq!(col, 0);
            assert!(
                message.contains("type mismatch"),
                "unexpected message: {message}"
            );
        }
        other => panic!("unexpected error kind: {other:?}"),
    }
}

#[test]
fn build_nested_dense_unions() {
    let inner_fields: UnionFields = [
        (
            2_i8,
            Arc::new(Field::new("inner_text", DataType::Utf8, true)),
        ),
        (
            3_i8,
            Arc::new(Field::new("inner_num", DataType::Int16, false)),
        ),
    ]
    .into_iter()
    .collect();
    let outer_fields: UnionFields = [
        (
            0_i8,
            Arc::new(Field::new("outer_int", DataType::Int32, false)),
        ),
        (
            1_i8,
            Arc::new(Field::new(
                "nested",
                DataType::Union(inner_fields, UnionMode::Dense),
                true,
            )),
        ),
    ]
    .into_iter()
    .collect();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "outer",
        DataType::Union(outer_fields, UnionMode::Dense),
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
            DynCell::union_value(2, DynCell::Str("hi".into())),
        ))])))
        .unwrap();
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_value(
            1,
            DynCell::union_value(3, DynCell::I16(5)),
        ))])))
        .unwrap();
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_value(
            1,
            DynCell::union_null(2),
        ))])))
        .unwrap();

    let batch = builders
        .try_finish_into_batch()
        .expect("valid nested union");
    assert_eq!(batch.num_rows(), 4);
    let outer_union = batch
        .column(0)
        .as_any()
        .downcast_ref::<UnionArray>()
        .unwrap();
    assert_eq!(&outer_union.type_ids()[..], &[0, 1, 1, 1]);
    assert_eq!(&outer_union.offsets().unwrap()[..], &[0, 0, 1, 2]);

    let outer_int = cast::as_primitive_array::<arrow_array::types::Int32Type>(outer_union.child(0));
    assert_eq!(outer_int.len(), 1);
    assert_eq!(outer_int.value(0), 42);
    assert!(outer_int.is_valid(0));

    let nested = outer_union
        .child(1)
        .as_any()
        .downcast_ref::<UnionArray>()
        .unwrap();
    assert_eq!(&nested.type_ids()[..], &[2, 3, 2]);
    assert_eq!(&nested.offsets().unwrap()[..], &[0, 0, 1]);

    let nested_text = cast::as_string_array(nested.child(2));
    assert_eq!(nested_text.len(), 2);
    assert_eq!(nested_text.value(0), "hi");
    assert!(nested_text.is_null(1));

    let nested_num = cast::as_primitive_array::<arrow_array::types::Int16Type>(nested.child(3));
    assert_eq!(nested_num.value(0), 5);
}

#[test]
fn union_column_level_null_skips_variant_check() {
    let union_fields: UnionFields = [
        (5_i8, Arc::new(Field::new("text", DataType::Utf8, false))),
        (6_i8, Arc::new(Field::new("num", DataType::Int32, false))),
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
            5,
            DynCell::Str("ok".into()),
        ))])))
        .unwrap();
    builders
        .append_option_row(Some(DynRow(vec![None])))
        .unwrap();
    builders
        .append_option_row(Some(DynRow(vec![Some(DynCell::union_value(
            6,
            DynCell::I32(7),
        ))])))
        .unwrap();

    let batch = builders
        .try_finish_into_batch()
        .expect("column-level union null allowed");
    let union = batch
        .column(0)
        .as_any()
        .downcast_ref::<UnionArray>()
        .unwrap();
    assert_eq!(&union.type_ids()[..], &[5, 5, 6]);
    assert_eq!(&union.offsets().unwrap()[..], &[0, 1, 0]);

    let text_child = cast::as_string_array(union.child(5));
    assert_eq!(text_child.value(0), "ok");
    assert!(text_child.is_null(1));

    let num_child = cast::as_primitive_array::<arrow_array::types::Int32Type>(union.child(6));
    assert_eq!(num_child.value(0), 7);
}
