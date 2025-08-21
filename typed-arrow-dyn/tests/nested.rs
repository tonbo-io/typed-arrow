use std::sync::Arc;

use arrow_array::{cast, Array, Int32Array, LargeListArray, ListArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use typed_arrow_dyn::{DynBuilders, DynCell, DynRow};

fn build_nested_batch() -> RecordBatch {
    // schema: { person: Struct{name: Utf8 (req), age: Int32 (opt)}, tags: List<Utf8>, nums3:
    // FixedSizeList<Int32,3> }
    let person_fields = vec![
        Arc::new(Field::new("name", DataType::Utf8, false)),
        Arc::new(Field::new("age", DataType::Int32, true)),
    ];
    let person = Field::new("person", DataType::Struct(person_fields.into()), true);
    let tags_item = Arc::new(Field::new("item", DataType::Utf8, false));
    let tags = Field::new("tags", DataType::List(tags_item), true);
    let nums_item = Arc::new(Field::new("item", DataType::Int32, false));
    let nums3 = Field::new("nums3", DataType::FixedSizeList(nums_item, 3), true);
    let schema = Arc::new(Schema::new(vec![person, tags, nums3]));

    let mut b = DynBuilders::new(Arc::clone(&schema), 0);

    // row0: person null, tags null, nums3 [1,2,3]
    b.append_option_row(Some(DynRow(vec![
        None,
        None,
        Some(DynCell::FixedSizeList(vec![
            Some(DynCell::I32(1)),
            Some(DynCell::I32(2)),
            Some(DynCell::I32(3)),
        ])),
    ])))
    .unwrap();

    // row1: person {name: "a", age: null}, tags ["x","y"], nums3 null
    b.append_option_row(Some(DynRow(vec![
        Some(DynCell::Struct(vec![Some(DynCell::Str("a".into())), None])),
        Some(DynCell::List(vec![
            Some(DynCell::Str("x".into())),
            Some(DynCell::Str("y".into())),
        ])),
        None,
    ])))
    .unwrap();

    b.finish_into_batch()
}

#[test]
fn struct_and_lists_build() {
    let batch = build_nested_batch();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 3);

    // person: Struct{name: Utf8 (req), age: Int32 (opt)} with validity [false, true]
    let person = cast::as_struct_array(batch.column(0));
    assert!(person.is_null(0));
    assert!(person.is_valid(1));
    // child 0: name
    let name = cast::as_string_array(person.column(0));
    assert!(name.is_null(0));
    assert_eq!(name.value(1), "a");
    // child 1: age
    let age = cast::as_primitive_array::<arrow_array::types::Int32Type>(person.column(1));
    assert!(age.is_null(0));
    assert!(age.is_null(1));

    // tags: List<Utf8> with rows [null, ["x","y"]]
    let tags = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(tags.is_null(0));
    assert!(tags.is_valid(1));
    let offsets = tags.value_offsets();
    assert_eq!(offsets, &[0, 0, 2]);
    let values = cast::as_string_array(tags.values());
    assert_eq!(values.len(), 2);
    assert_eq!(values.value(0), "x");
    assert_eq!(values.value(1), "y");

    // nums3: FixedSizeList<Int32,3> with rows [[1,2,3], null]
    let nums3 = batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeListArray>()
        .unwrap();
    assert!(nums3.is_valid(0));
    assert!(nums3.is_null(1));
    let child = nums3
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(child.len(), 6);
    assert_eq!(child.value(0), 1);
    assert_eq!(child.value(1), 2);
    assert_eq!(child.value(2), 3);
    assert!(child.is_null(3));
    assert!(child.is_null(4));
    assert!(child.is_null(5));
}

#[test]
fn large_list_offsets_and_validity() {
    // Schema: { big: LargeList<Utf8> }
    let item = Arc::new(Field::new("item", DataType::Utf8, true));
    let field = Field::new("big", DataType::LargeList(item), true);
    let schema = Arc::new(Schema::new(vec![field]));

    let mut b = DynBuilders::new(Arc::clone(&schema), 0);
    // row0: []
    b.append_option_row(Some(DynRow(vec![Some(DynCell::List(vec![]))])))
        .unwrap();
    // row1: null
    b.append_option_row(Some(DynRow(vec![None]))).unwrap();
    // row2: ["a", null]
    b.append_option_row(Some(DynRow(vec![Some(DynCell::List(vec![
        Some(DynCell::Str("a".into())),
        None,
    ]))])))
    .unwrap();
    // row3: ["b","c"]
    b.append_option_row(Some(DynRow(vec![Some(DynCell::List(vec![
        Some(DynCell::Str("b".into())),
        Some(DynCell::Str("c".into())),
    ]))])))
    .unwrap();

    let batch = b.finish_into_batch();
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<LargeListArray>()
        .unwrap();
    assert_eq!(batch.num_rows(), 4);
    assert!(arr.is_valid(0));
    assert!(arr.is_null(1));
    assert!(arr.is_valid(2));
    assert!(arr.is_valid(3));
    let offsets = arr.value_offsets();
    // Expect [0,0,0,2,4]
    assert_eq!(offsets, &[0, 0, 0, 2, 4]);
    let values = cast::as_string_array(arr.values());
    // Values: ["a", null, "b", "c"]
    assert_eq!(values.len(), 4);
    assert_eq!(values.value(0), "a");
    assert!(values.is_null(1));
    assert_eq!(values.value(2), "b");
    assert_eq!(values.value(3), "c");
}
