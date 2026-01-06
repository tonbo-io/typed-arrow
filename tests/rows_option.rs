use typed_arrow::arrow_array::Array;
use typed_arrow::prelude::*;

#[derive(Record)]
pub struct PersonO {
    pub id: i64,
    pub name: Option<String>,
}

#[test]
fn build_from_option_rows_flat() {
    let rows: Vec<Option<PersonO>> = vec![
        Some(PersonO {
            id: 1,
            name: Some("a".into()),
        }),
        None,
        Some(PersonO { id: 3, name: None }),
    ];
    let mut b = <PersonO as BuildRows>::new_builders(rows.len());
    b.append_option_rows(rows);
    let arrays = b.finish();

    assert_eq!(arrays.id.len(), 3);
    assert!(arrays.id.is_null(1));
    assert_eq!(arrays.id.value(0), 1);
}

#[derive(Record)]
pub struct AddressO {
    pub city: String,
}

#[derive(Record)]
pub struct PersonNO {
    pub id: i64,
    pub address: Option<AddressO>,
}

#[test]
fn build_from_option_rows_nested() {
    let rows: Vec<Option<PersonNO>> = vec![
        Some(PersonNO {
            id: 1,
            address: Some(AddressO { city: "NYC".into() }),
        }),
        None,
        Some(PersonNO {
            id: 3,
            address: None,
        }),
    ];
    let mut b = <PersonNO as BuildRows>::new_builders(rows.len());
    b.append_option_rows(rows);
    let arrays = b.finish();
    assert_eq!(arrays.id.len(), 3);
    assert!(arrays.id.is_null(1));
    assert!(arrays.address.is_null(1));
}
