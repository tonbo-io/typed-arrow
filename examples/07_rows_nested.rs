//! Showcase: Row-based building with nested struct fields.

use arrow_array::{cast::as_string_array, Array};
use arrow_native::prelude::*;

#[derive(arrow_native::Record)]
struct AddressN {
    city: String,
    zip: Option<i32>,
}

#[derive(arrow_native::Record)]
struct PersonN {
    id: i64,
    #[record(nested)]
    address: Option<AddressN>,
}

fn main() {
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

    let addr = arrays.address;
    let city = as_string_array(addr.column(0));
    println!(
        "rows={}, addr1_null={}, city0={}, city2={}",
        addr.len(),
        addr.is_null(1),
        city.value(0),
        city.value(2)
    );
}
