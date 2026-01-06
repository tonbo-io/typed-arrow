use typed_arrow::arrow_array::{
    Array,
    cast::{as_list_array, as_primitive_array, as_struct_array},
    types::Int32Type,
};
use typed_arrow::prelude::*;

#[derive(Record)]
pub struct Part {
    pub a: i32,
    pub b: i32,
}

#[derive(Record)]
pub struct RowLOS {
    pub id: i64,
    pub parts: typed_arrow::List<Part>,
}

#[test]
fn list_of_struct_builds_and_values() {
    let rows = vec![
        RowLOS {
            id: 1,
            parts: typed_arrow::List::new(vec![Part { a: 1, b: 2 }, Part { a: 3, b: 4 }]),
        },
        RowLOS {
            id: 2,
            parts: typed_arrow::List::new(vec![Part { a: 10, b: 20 }]),
        },
    ];

    let mut b = <RowLOS as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    // Downcast parts: List<Struct<a,b>>
    let parts = arrays.parts;
    let la = as_list_array(&parts);
    assert_eq!(la.len(), 2);
    assert_eq!(la.value_length(0), 2);
    assert_eq!(la.value_length(1), 1);

    let child = as_struct_array(la.values());
    let a = as_primitive_array::<Int32Type>(child.column(0));
    let b = as_primitive_array::<Int32Type>(child.column(1));
    assert_eq!(a.value(0), 1);
    assert_eq!(b.value(0), 2);
    assert_eq!(a.value(1), 3);
    assert_eq!(b.value(1), 4);
    assert_eq!(a.value(2), 10);
    assert_eq!(b.value(2), 20);
}
