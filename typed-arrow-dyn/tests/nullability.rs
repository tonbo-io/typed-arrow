use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use typed_arrow_dyn::{DynBuilders, DynCell, DynError, DynRow};

#[test]
fn rejects_none_for_non_nullable_primitive() {
    // Schema: { a: Int64 (required) }
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let mut b = DynBuilders::new(Arc::clone(&schema), 0);

    b.append_option_row(Some(DynRow(vec![None]))).unwrap();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _ = b.finish_into_batch();
    }));
    assert!(res.is_err(), "expected panic due to nullability violation");
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
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _ = b.finish_into_batch();
    }));
    assert!(res.is_err(), "expected panic due to nullability violation");
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
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _ = b.finish_into_batch();
    }));
    assert!(res.is_err(), "expected panic due to nullability violation");
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
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _ = b.finish_into_batch();
    }));
    assert!(res.is_err(), "expected panic due to nullability violation");
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
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _ = b.finish_into_batch();
    }));
    assert!(res.is_err(), "expected panic due to nullability violation");
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
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _ = b.finish_into_batch();
    }));
    assert!(res.is_err(), "expected panic due to nullability violation");
}

#[test]
fn deferred_allows_appends_but_fails_at_finish_primitive() {
    // Schema: { a: Int64 (required) }
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let mut b = DynBuilders::new(Arc::clone(&schema), 0);

    // Appending null should be allowed in deferred mode
    b.append_option_row(Some(DynRow(vec![None]))).unwrap();

    // Finish with checked validation should error
    // Expect panic from Arrow validation on finish
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _batch = b.finish_into_batch();
    }));
    assert!(res.is_err(), "expected panic due to nullability violation");
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

    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _batch = b.finish_into_batch();
    }));
    assert!(res.is_err(), "expected panic due to nullability violation");
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

    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _batch = b.finish_into_batch();
    }));
    assert!(res.is_err(), "expected panic due to nullability violation");
}
