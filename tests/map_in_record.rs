use std::sync::Arc;

use arrow_schema::{DataType, Field};
use typed_arrow::{Map, Record, prelude::*};

#[derive(Record)]
struct Row {
    id: i32,
    tags: Map<String, i32>,
    attrs: Option<Map<String, Option<String>>>,
}

#[test]
fn map_inside_record_build_rows() {
    let mut b = <Row as BuildRows>::new_builders(0);
    b.append_row(Row {
        id: 1,
        tags: Map::new(vec![("a".to_string(), 10), ("b".to_string(), 20)]),
        attrs: Some(Map::new(vec![
            ("x".to_string(), Some("foo".to_string())),
            ("y".to_string(), None),
        ])),
    });
    b.append_row(Row {
        id: 2,
        tags: Map::new(vec![]),
        attrs: None,
    });
    let arrays = b.finish();
    let rb = arrays.into_record_batch();
    assert_eq!(rb.num_rows(), 2);

    // Validate schema datatypes
    let schema = rb.schema();
    let fields = schema.fields();
    assert_eq!(fields[0].data_type(), &DataType::Int32);

    // tags: Map<String, i32>
    let key_f = Field::new("keys", DataType::Utf8, false);
    let val_f = Field::new("values", DataType::Int32, true);
    let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
    let expected_map = DataType::Map(Field::new("entries", entries, false).into(), false);
    assert_eq!(fields[1].data_type(), &expected_map);

    // attrs: Option<Map<String, Option<String>>>
    let key_f = Field::new("keys", DataType::Utf8, false);
    let val_f = Field::new("values", DataType::Utf8, true);
    let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
    let expected_map_nullable = DataType::Map(Field::new("entries", entries, false).into(), false);
    assert_eq!(fields[2].data_type(), &expected_map_nullable);
}
