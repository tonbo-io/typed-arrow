use std::sync::Arc;

use typed_arrow_dyn::{
    DynBuilders, DynCell, DynRow,
    arrow_array::RecordBatch,
    arrow_schema::{DataType, Field, Schema},
};

fn main() {
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

    // Finish to RecordBatch
    let batch: RecordBatch = b.finish_into_batch();
    println!(
        "rows={} cols={} schema={}",
        batch.num_rows(),
        batch.num_columns(),
        batch.schema()
    );
}
