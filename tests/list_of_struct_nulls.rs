use arrow_array::{
    Array,
    cast::{as_list_array, as_primitive_array, as_struct_array},
    types::Int32Type,
};
use typed_arrow::prelude::*;

#[derive(typed_arrow::Record)]
pub struct PartN {
    pub a: i32,
}

#[derive(typed_arrow::Record)]
pub struct RowLON {
    pub id: i64,
    pub parts: typed_arrow::List<Option<PartN>>, // item-nullable list of struct
    pub opt_parts: Option<typed_arrow::List<PartN>>, // nullable list of struct
}

#[test]
fn list_of_struct_with_nulls_builds() {
    let rows = vec![
        RowLON {
            id: 1,
            parts: typed_arrow::List::new(vec![Some(PartN { a: 1 }), None, Some(PartN { a: 2 })]),
            opt_parts: Some(typed_arrow::List::new(vec![PartN { a: 10 }])),
        },
        RowLON {
            id: 2,
            parts: typed_arrow::List::new(vec![]),
            opt_parts: None,
        },
    ];

    let mut b = <RowLON as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    // parts: List<Option<Struct<a>>> → values child is Struct
    let la = as_list_array(&arrays.parts);
    assert_eq!(la.len(), 2);
    assert_eq!(la.value_length(0), 3);
    assert_eq!(la.value_length(1), 0);
    let child = as_struct_array(la.values());
    let a = as_primitive_array::<Int32Type>(child.column(0));
    assert_eq!(a.value(0), 1);
    assert!(child.is_null(1));
    assert_eq!(a.value(2), 2);

    // opt_parts: Option<List<Struct<a>>>
    let opt = as_list_array(&arrays.opt_parts);
    assert_eq!(opt.len(), 2);
    assert!(opt.is_valid(0));
    assert!(opt.is_null(1));
}
