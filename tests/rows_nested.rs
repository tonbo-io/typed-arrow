use typed_arrow::arrow_array::{Array, cast::as_string_array};
use typed_arrow::prelude::*;

#[derive(Record)]
pub struct AddressN {
    pub city: String,
    pub zip: Option<i32>,
}

#[derive(Record)]
pub struct PersonN {
    pub id: i64,
    pub address: Option<AddressN>,
}

#[test]
fn build_rows_with_nested_struct() {
    let rows = vec![
        PersonN {
            id: 1,
            address: Some(AddressN {
                city: "NYC".into(),
                zip: None,
            }),
        },
        PersonN {
            id: 2,
            address: None,
        },
        PersonN {
            id: 3,
            address: Some(AddressN {
                city: "SF".into(),
                zip: Some(94111),
            }),
        },
    ];

    let mut b = <PersonN as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    // Validate nested struct array
    let addr = arrays.address;
    assert_eq!(addr.len(), 3);
    assert!(addr.is_null(1));
    let city = as_string_array(addr.column(0));
    assert_eq!(city.value(0), "NYC");
    assert_eq!(city.value(2), "SF");
}
