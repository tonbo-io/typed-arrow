use arrow_array::{
    Array, LargeListArray,
    cast::{as_primitive_array, as_struct_array},
    types::Int32Type,
};
use typed_arrow::prelude::*;

#[derive(Record)]
pub struct PartLL {
    pub a: i32,
    pub b: i32,
}

#[derive(Record)]
pub struct RowLLS {
    pub id: i64,
    pub parts: typed_arrow::LargeList<PartLL>,
}

#[test]
fn large_list_of_struct_builds_and_values() {
    let rows = vec![
        RowLLS {
            id: 1,
            parts: typed_arrow::LargeList::new(vec![PartLL { a: 1, b: 2 }, PartLL { a: 3, b: 4 }]),
        },
        RowLLS {
            id: 2,
            parts: typed_arrow::LargeList::new(vec![PartLL { a: 10, b: 20 }]),
        },
    ];

    let mut b = <RowLLS as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    let parts: LargeListArray = arrays.parts; // concrete type
    assert_eq!(parts.len(), 2);
    assert_eq!(parts.value_length(0), 2);
    assert_eq!(parts.value_length(1), 1);

    let child = as_struct_array(parts.values());
    let a = as_primitive_array::<Int32Type>(child.column(0));
    let b = as_primitive_array::<Int32Type>(child.column(1));
    assert_eq!(a.value(0), 1);
    assert_eq!(b.value(0), 2);
    assert_eq!(a.value(1), 3);
    assert_eq!(b.value(1), 4);
    assert_eq!(a.value(2), 10);
    assert_eq!(b.value(2), 20);
}
