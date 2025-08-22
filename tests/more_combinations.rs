use std::sync::Arc;

use arrow_array::cast::as_list_array;
use arrow_schema::{DataType, Field};
use typed_arrow::{arrow_array::Array, prelude::*};

#[derive(typed_arrow::Record)]
struct PartA {
    a: i32,
    b: i32,
}

#[derive(typed_arrow::Record)]
struct RowMapLargeListStruct {
    id: i32,
    buckets: typed_arrow::Map<String, typed_arrow::LargeList<PartA>>,
}

#[test]
fn map_of_large_list_struct_schema_and_build() {
    let rows = vec![
        RowMapLargeListStruct {
            id: 1,
            buckets: typed_arrow::Map::new(vec![
                (
                    "k1".to_string(),
                    typed_arrow::LargeList::new(vec![PartA { a: 1, b: 2 }, PartA { a: 3, b: 4 }]),
                ),
                (
                    "k2".to_string(),
                    typed_arrow::LargeList::new(vec![PartA { a: 10, b: 20 }]),
                ),
            ]),
        },
        RowMapLargeListStruct {
            id: 2,
            buckets: typed_arrow::Map::new(vec![]),
        },
    ];

    let mut b = <RowMapLargeListStruct as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    // Expected: Map<entries: Struct<keys: Utf8, values: LargeList<Struct<PartA>>>>
    let sub_fields = vec![
        Arc::new(Field::new("a", DataType::Int32, false)),
        Arc::new(Field::new("b", DataType::Int32, false)),
    ];
    let ll =
        DataType::LargeList(Field::new("item", DataType::Struct(sub_fields.into()), false).into());
    let key_f = Field::new("keys", DataType::Utf8, false);
    let val_f = Field::new("values", ll, true);
    let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
    let expected = DataType::Map(Field::new("entries", entries, false).into(), false);
    assert_eq!(<RowMapLargeListStruct as ColAt<1>>::data_type(), expected);
    assert_eq!(arrays.buckets.len(), 2);
}

#[derive(typed_arrow::Record)]
struct PartB {
    x: i32,
}

#[derive(typed_arrow::Record)]
struct RowMapFixedListStruct {
    id: i32,
    groups: typed_arrow::Map<String, typed_arrow::FixedSizeList<PartB, 2>>,
}

#[test]
fn map_of_fixed_size_list_struct_schema_and_build() {
    let rows = vec![
        RowMapFixedListStruct {
            id: 1,
            groups: typed_arrow::Map::new(vec![
                (
                    "g1".to_string(),
                    typed_arrow::FixedSizeList::new([PartB { x: 1 }, PartB { x: 2 }]),
                ),
                (
                    "g2".to_string(),
                    typed_arrow::FixedSizeList::new([PartB { x: 10 }, PartB { x: 20 }]),
                ),
            ]),
        },
        RowMapFixedListStruct {
            id: 2,
            groups: typed_arrow::Map::new(vec![]),
        },
    ];

    let mut b = <RowMapFixedListStruct as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    let sub_fields = vec![Arc::new(Field::new("x", DataType::Int32, false))];
    let fsl = DataType::FixedSizeList(
        Field::new("item", DataType::Struct(sub_fields.into()), false).into(),
        2,
    );
    let key_f = Field::new("keys", DataType::Utf8, false);
    let val_f = Field::new("values", fsl, true);
    let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
    let expected = DataType::Map(Field::new("entries", entries, false).into(), false);
    assert_eq!(<RowMapFixedListStruct as ColAt<1>>::data_type(), expected);
    assert_eq!(arrays.groups.len(), 2);
}

#[derive(typed_arrow::Record)]
struct PartC {
    y: i32,
}

#[derive(typed_arrow::Record)]
struct RowListMapListStruct {
    id: i32,
    mats: typed_arrow::List<typed_arrow::Map<String, typed_arrow::List<Option<PartC>>>>,
}

#[test]
fn list_of_map_of_list_of_struct_schema_and_build() {
    let rows = vec![
        RowListMapListStruct {
            id: 1,
            mats: typed_arrow::List::new(vec![
                typed_arrow::Map::new(vec![(
                    "a".to_string(),
                    typed_arrow::List::new(vec![Some(PartC { y: 1 }), None]),
                )]),
                typed_arrow::Map::new(vec![(
                    "b".to_string(),
                    typed_arrow::List::new(vec![Some(PartC { y: 2 })]),
                )]),
            ]),
        },
        RowListMapListStruct {
            id: 2,
            mats: typed_arrow::List::new(vec![]),
        },
    ];

    let mut b = <RowListMapListStruct as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    // Type: List<Map<entries: Struct<keys: Utf8, values: List<Option<Struct<PartC>>>>>>
    let sub_fields = vec![Arc::new(Field::new("y", DataType::Int32, false))];
    let inner_list =
        DataType::List(Field::new("item", DataType::Struct(sub_fields.into()), true).into());
    let key_f = Field::new("keys", DataType::Utf8, false);
    let val_f = Field::new("values", inner_list, true);
    let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
    let map_dt = DataType::Map(Field::new("entries", entries, false).into(), false);
    let expected = DataType::List(Field::new("item", map_dt, false).into());
    assert_eq!(<RowListMapListStruct as ColAt<1>>::data_type(), expected);

    // Basic lengths
    let la = as_list_array(&arrays.mats);
    assert_eq!(la.len(), 2);
    assert_eq!(la.value_length(0), 2);
    assert_eq!(la.value_length(1), 0);
}
