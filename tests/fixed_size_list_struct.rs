use typed_arrow::arrow_array::{Array, cast::as_primitive_array, types::Int32Type};
use typed_arrow::prelude::*;

#[derive(Record)]
pub struct Item {
    pub a: i32,
}

#[derive(Record)]
pub struct RowFSL {
    pub id: i64,
    pub pair: typed_arrow::FixedSizeList<Item, 2>,
}

#[test]
fn fixed_size_list_of_struct_builds_and_values() {
    let rows = vec![
        RowFSL {
            id: 1,
            pair: typed_arrow::FixedSizeList::new([Item { a: 1 }, Item { a: 2 }]),
        },
        RowFSL {
            id: 2,
            pair: typed_arrow::FixedSizeList::new([Item { a: 10 }, Item { a: 20 }]),
        },
    ];

    let mut b = <RowFSL as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    // Downcast values to StructArray and inspect `a` field
    let a = arrays
        .pair
        .values()
        .as_any()
        .downcast_ref::<typed_arrow::arrow_array::StructArray>()
        .unwrap();
    let a_field = as_primitive_array::<Int32Type>(a.column(0));
    assert_eq!(a_field.len(), 4);
    assert_eq!(a_field.value(0), 1);
    assert_eq!(a_field.value(1), 2);
    assert_eq!(a_field.value(2), 10);
    assert_eq!(a_field.value(3), 20);
}

#[derive(Record)]
pub struct RowFSLN {
    pub id: i64,
    pub pair: typed_arrow::FixedSizeListNullable<Item, 2>,
}

#[test]
fn fixed_size_list_nullable_items_of_struct() {
    let rows = vec![
        RowFSLN {
            id: 1,
            pair: typed_arrow::FixedSizeListNullable::new([Some(Item { a: 1 }), None]),
        },
        RowFSLN {
            id: 2,
            pair: typed_arrow::FixedSizeListNullable::new([None, Some(Item { a: 2 })]),
        },
    ];

    let mut b = <RowFSLN as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    let a = arrays
        .pair
        .values()
        .as_any()
        .downcast_ref::<typed_arrow::arrow_array::StructArray>()
        .unwrap();
    let a_field = as_primitive_array::<Int32Type>(a.column(0));
    assert_eq!(a_field.len(), 4);
    assert_eq!(a_field.value(0), 1);
    assert!(a.is_null(1));
    assert!(a.is_null(2));
    assert_eq!(a_field.value(3), 2);
}
