use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, ArrayRef, Int64Array, MapArray, StringArray, StructArray};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, Fields, Schema};
use typed_arrow_dyn::{validate_nullability, DynBuilders, DynCell, DynError, DynRow};

#[test]
fn rejects_none_for_non_nullable_primitive() {
    // Schema: { a: Int64 (required) }
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let mut b = DynBuilders::new(Arc::clone(&schema), 0);

    b.append_option_row(Some(DynRow(vec![None]))).unwrap();
    // Expect error due to nullability violation at try-finish
    let err = b.try_finish_into_batch().unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("nullability"));
}

#[test]
fn rejects_top_level_none_row_when_any_column_required() {
    // Schema: { a: Int64 (required), b: Utf8 (nullable) }
    let fields = vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
    ];
    let schema = Arc::new(Schema::new(fields));
    let mut b = DynBuilders::new(Arc::clone(&schema), 0);

    b.append_option_row(None).unwrap();
    let err = b.try_finish_into_batch().unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("nullability"));
}

#[test]
fn struct_child_non_nullable_rejects_none() {
    // person: Struct{name: Utf8 (req), age: Int32 (opt)} (person itself nullable)
    let person_fields = vec![
        Arc::new(Field::new("name", DataType::Utf8, false)),
        Arc::new(Field::new("age", DataType::Int32, true)),
    ];
    let person = Field::new("person", DataType::Struct(person_fields.into()), true);
    let schema = Arc::new(Schema::new(vec![person]));

    let mut b = DynBuilders::new(Arc::clone(&schema), 0);

    // Entire struct null is allowed (masked by parent validity)
    b.append_option_row(Some(DynRow(vec![None]))).unwrap();

    let row = DynRow(vec![Some(DynCell::Struct(vec![
        None,
        Some(DynCell::I32(10)),
    ]))]);
    b.append_option_row(Some(row)).unwrap();
    let err = b.try_finish_into_batch().unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("StructArray") || msg.contains("struct field"));
}

#[test]
fn list_item_non_nullable_rejects_none() {
    // tags: List<Utf8 (required)>
    let item = Arc::new(Field::new("item", DataType::Utf8, false));
    let tags = Field::new("tags", DataType::List(item), true);
    let schema = Arc::new(Schema::new(vec![tags]));
    let mut b = DynBuilders::new(Arc::clone(&schema), 0);

    let row = DynRow(vec![Some(DynCell::List(vec![None]))]);
    b.append_option_row(Some(row)).unwrap();
    let err = b.try_finish_into_batch().unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("ListArray") || msg.contains("list item"));
}

#[test]
fn list_nullable_parent_allows_none_even_if_items_required() {
    // tags: List<Utf8 (required)> and field is nullable
    let item = Arc::new(Field::new("item", DataType::Utf8, false));
    let tags = Field::new("tags", DataType::List(item), true);
    let schema = Arc::new(Schema::new(vec![tags]));
    let mut b = DynBuilders::new(Arc::clone(&schema), 0);

    // Entire list None is allowed since the list field is nullable
    b.append_option_row(Some(DynRow(vec![None]))).unwrap();
}

#[test]
fn large_list_item_non_nullable_rejects_none() {
    // big: LargeList<Utf8 (required)>
    let item = Arc::new(Field::new("item", DataType::Utf8, false));
    let field = Field::new("big", DataType::LargeList(item), true);
    let schema = Arc::new(Schema::new(vec![field]));
    let mut b = DynBuilders::new(Arc::clone(&schema), 0);

    let row = DynRow(vec![Some(DynCell::List(vec![None]))]);
    b.append_option_row(Some(row)).unwrap();
    let err = b.try_finish_into_batch().unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("LargeListArray") || msg.contains("large-list"));
}

#[test]
fn fixed_size_list_item_non_nullable_rejects_none() {
    // nums3: FixedSizeList<Int32 (required), 3>
    let item = Arc::new(Field::new("item", DataType::Int32, false));
    let field = Field::new("nums3", DataType::FixedSizeList(item, 3), true);
    let schema = Arc::new(Schema::new(vec![field]));
    let mut b = DynBuilders::new(Arc::clone(&schema), 0);

    let row = DynRow(vec![Some(DynCell::FixedSizeList(vec![
        Some(DynCell::I32(1)),
        None,
        Some(DynCell::I32(3)),
    ]))]);
    b.append_option_row(Some(row)).unwrap();
    let err = b.try_finish_into_batch().unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("FixedSizeListArray") || msg.contains("fixed-size"));
}

#[test]
fn deferred_allows_appends_but_fails_at_finish_primitive() {
    // Schema: { a: Int64 (required) }
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let mut b = DynBuilders::new(Arc::clone(&schema), 0);

    // Appending null should be allowed in deferred mode
    b.append_option_row(Some(DynRow(vec![None]))).unwrap();

    // Expect error from validator at try-finish
    assert!(b.try_finish_into_batch().is_err());
}

#[test]
fn deferred_struct_child_violation_detected_at_finish() {
    // person: Struct{name: Utf8 (req), age: Int32 (opt)} (person itself nullable)
    let person_fields = vec![
        Arc::new(Field::new("name", DataType::Utf8, false)),
        Arc::new(Field::new("age", DataType::Int32, true)),
    ];
    let person = Field::new("person", DataType::Struct(person_fields.into()), true);
    let schema = Arc::new(Schema::new(vec![person]));

    let mut b = DynBuilders::new(Arc::clone(&schema), 0);
    // Child 'name' is non-nullable; providing None should be caught at finish
    b.append_option_row(Some(DynRow(vec![Some(DynCell::Struct(vec![
        None,
        Some(DynCell::I32(10)),
    ]))])))
    .unwrap();

    assert!(b.try_finish_into_batch().is_err());
}

#[test]
fn deferred_list_item_violation_detected_at_finish() {
    // tags: List<Utf8 (required)>
    let item = Arc::new(Field::new("item", DataType::Utf8, false));
    let tags = Field::new("tags", DataType::List(item), true);
    let schema = Arc::new(Schema::new(vec![tags]));
    let mut b = DynBuilders::new(Arc::clone(&schema), 0);

    let row = DynRow(vec![Some(DynCell::List(vec![None]))]);
    b.append_option_row(Some(row)).unwrap();

    assert!(b.try_finish_into_batch().is_err());
}

#[test]
fn map_value_nullability_violation_detected() {
    let entry_fields: Fields = vec![
        Arc::new(Field::new("keys", DataType::Utf8, false)),
        Arc::new(Field::new("values", DataType::Int64, false)),
    ]
    .into();
    let entry_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(entry_fields.clone()),
        false,
    ));
    let schema = Schema::new(vec![Field::new(
        "data",
        DataType::Map(entry_field.clone(), false),
        false,
    )]);

    let keys_array = StringArray::from(vec!["a", "b"]);
    let values_array = Int64Array::from(vec![Some(1), None]);
    let num_entries = keys_array.len();
    // Intentionally build an invalid entries struct (null values despite non-nullable field).
    let struct_data = unsafe {
        ArrayData::builder(DataType::Struct(entry_fields.clone()))
            .len(num_entries)
            .child_data(vec![keys_array.into_data(), values_array.into_data()])
            .build_unchecked()
    };
    let entries = StructArray::from(struct_data);

    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2]));
    let mut validity = BooleanBufferBuilder::new(1);
    validity.append(true);
    let validity = Some(NullBuffer::new(validity.finish()));

    let map = MapArray::try_new(entry_field, offsets, entries, validity, false).unwrap();
    let arrays: Vec<ArrayRef> = vec![Arc::new(map) as ArrayRef];

    let union_null_rows = HashMap::new();
    match validate_nullability(&schema, &arrays, &union_null_rows) {
        Err(DynError::Nullability { col, path, .. }) => {
            assert_eq!(col, 0);
            assert_eq!(path, "data.values");
        }
        other => panic!("expected nullability error, got {other:?}"),
    }
}
