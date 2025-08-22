use std::sync::Arc;

use arrow_schema::{DataType, Field};
use typed_arrow::{arrow_array::Array, prelude::*};

#[derive(typed_arrow::Record)]
struct Sub {
    x: i32,
}

#[derive(typed_arrow::Record)]
struct RowListOfMapStruct {
    id: i32,
    groups: typed_arrow::List<typed_arrow::Map<String, Option<Sub>>>,
}

#[test]
fn list_of_map_of_struct_schema_and_build() {
    // Build two rows: one with two maps, one empty list
    let rows = vec![
        RowListOfMapStruct {
            id: 1,
            groups: typed_arrow::List::new(vec![
                typed_arrow::Map::new(vec![
                    ("a".to_string(), Some(Sub { x: 1 })),
                    ("b".to_string(), None),
                ]),
                typed_arrow::Map::new(vec![("c".to_string(), Some(Sub { x: 2 }))]),
            ]),
        },
        RowListOfMapStruct {
            id: 2,
            groups: typed_arrow::List::new(vec![]),
        },
    ];
    let mut b = <RowListOfMapStruct as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    // Validate nested DataType shape: List<Map<entries: Struct<keys: Utf8, values: Struct<...>>>>
    let key_f = Field::new("keys", DataType::Utf8, false);
    let sub_fields = vec![Arc::new(Field::new("x", DataType::Int32, false))];
    let value_dt = DataType::Struct(sub_fields.into());
    let val_f = Field::new("values", value_dt, true);
    let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
    let map_dt = DataType::Map(Field::new("entries", entries, false).into(), false);
    let expected = DataType::List(Field::new("item", map_dt, false).into());
    assert_eq!(<RowListOfMapStruct as ColAt<1>>::data_type(), expected);
    assert_eq!(arrays.groups.len(), 2);
}

#[derive(typed_arrow::Record)]
struct RowMapOfList {
    id: i32,
    buckets: typed_arrow::Map<String, typed_arrow::List<Option<i32>>>,
}

#[test]
fn map_of_list_schema_and_build() {
    let rows = vec![
        RowMapOfList {
            id: 1,
            buckets: typed_arrow::Map::new(vec![
                (
                    "p1".to_string(),
                    typed_arrow::List::new(vec![Some(1), None, Some(2)]),
                ),
                ("p2".to_string(), typed_arrow::List::new(vec![])),
            ]),
        },
        RowMapOfList {
            id: 2,
            buckets: typed_arrow::Map::new(vec![]),
        },
    ];
    let mut b = <RowMapOfList as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    // keys: Utf8, values: List<Int32 nullable>
    let key_f = Field::new("keys", DataType::Utf8, false);
    let list_v = DataType::List(Field::new("item", DataType::Int32, true).into());
    let val_f = Field::new("values", list_v, true);
    let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
    let expected = DataType::Map(Field::new("entries", entries, false).into(), false);
    assert_eq!(<RowMapOfList as ColAt<1>>::data_type(), expected);
    assert_eq!(arrays.buckets.len(), 2);
}

#[derive(typed_arrow::Record)]
struct RowListOfList {
    id: i32,
    mats: typed_arrow::List<typed_arrow::List<Option<i32>>>,
}

#[test]
fn list_of_list_schema_and_build() {
    let rows = vec![
        RowListOfList {
            id: 1,
            mats: typed_arrow::List::new(vec![
                typed_arrow::List::new(vec![Some(1), None]),
                typed_arrow::List::new(vec![Some(2)]),
            ]),
        },
        RowListOfList {
            id: 2,
            mats: typed_arrow::List::new(vec![]),
        },
    ];
    let mut b = <RowListOfList as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    let inner = DataType::List(Field::new("item", DataType::Int32, true).into());
    let expected = DataType::List(Field::new("item", inner, false).into());
    assert_eq!(<RowListOfList as ColAt<1>>::data_type(), expected);
    assert_eq!(arrays.mats.len(), 2);
}

#[derive(typed_arrow::Record)]
struct RowMapOfStruct {
    id: i32,
    attrs: typed_arrow::Map<String, Sub>,
}

#[test]
fn map_of_struct_schema_and_build() {
    let rows = vec![
        RowMapOfStruct {
            id: 1,
            attrs: typed_arrow::Map::new(vec![
                ("u".to_string(), Sub { x: 10 }),
                ("v".to_string(), Sub { x: 20 }),
            ]),
        },
        RowMapOfStruct {
            id: 2,
            attrs: typed_arrow::Map::new(vec![]),
        },
    ];
    let mut b = <RowMapOfStruct as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    let key_f = Field::new("keys", DataType::Utf8, false);
    let sub_fields = vec![Arc::new(Field::new("x", DataType::Int32, false))];
    let val_f = Field::new("values", DataType::Struct(sub_fields.into()), true);
    let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
    let expected = DataType::Map(Field::new("entries", entries, false).into(), false);
    assert_eq!(<RowMapOfStruct as ColAt<1>>::data_type(), expected);
    assert_eq!(arrays.attrs.len(), 2);
}
