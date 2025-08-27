#![allow(clippy::float_cmp)]

use arrow_array::{
    cast::{as_primitive_array, as_string_array, as_struct_array},
    types::{Float64Type, Int32Type},
    Array,
};
use typed_arrow::prelude::*;

// Deeply nested structs (3+ levels) with a mix of
// required/optional fields to exercise derive and builders.

#[derive(typed_arrow::Record)]
pub struct Geo {
    pub lat: f64,
    pub lon: f64,
}

#[derive(typed_arrow::Record)]
pub struct Address {
    pub city: String,
    pub zip: Option<i32>,
    #[nested]
    pub geo: Option<Geo>,
}

#[derive(typed_arrow::Record)]
pub struct Company {
    pub name: String,
    #[nested]
    pub hq: Option<Address>,
}

#[derive(typed_arrow::Record)]
pub struct PersonDeep {
    pub id: i64,
    #[nested]
    pub company: Option<Company>,
    #[nested]
    pub address: Option<Address>,
    // Also include a nested container field to ensure non-struct siblings behave
    pub scores: Option<typed_arrow::List<Option<i32>>>,
}

#[test]
fn deep_nested_schema_and_usage() {
    // Prepare a few rows with varying nullability along the nested chain
    let rows = vec![
        PersonDeep {
            id: 1,
            company: Some(Company {
                name: "Acme".into(),
                hq: Some(Address {
                    city: "NYC".into(),
                    zip: None,
                    geo: Some(Geo {
                        lat: 40.7128,
                        lon: -74.0060,
                    }),
                }),
            }),
            address: None,
            scores: Some(typed_arrow::List::new(vec![Some(10), None, Some(20)])),
        },
        PersonDeep {
            id: 2,
            company: None,
            address: Some(Address {
                city: "SF".into(),
                zip: Some(94107),
                geo: None,
            }),
            scores: None,
        },
        PersonDeep {
            id: 3,
            company: Some(Company {
                name: "Globex".into(),
                hq: None,
            }),
            address: Some(Address {
                city: "LA".into(),
                zip: None,
                geo: Some(Geo {
                    lat: 34.0522,
                    lon: -118.2437,
                }),
            }),
            scores: Some(typed_arrow::List::new(vec![])),
        },
    ];

    // Build arrays via the row-based builders
    let mut b = <PersonDeep as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    // Top-level checks
    assert_eq!(<PersonDeep as Record>::LEN, 4);

    // Validate deep nested values under company -> hq -> geo
    let company = arrays.company; // StructArray with children [name: Utf8, hq: Struct]
    assert_eq!(company.len(), 3);
    assert!(company.is_valid(0));
    assert!(company.is_null(1));
    assert!(company.is_valid(2));

    // name child
    let c_name = as_string_array(company.column(0));
    assert_eq!(c_name.value(0), "Acme");
    assert_eq!(c_name.value(2), "Globex");

    // hq child (nested Address struct)
    let hq = as_struct_array(company.column(1));
    assert!(hq.is_valid(0)); // row0 has Some(Address)
    assert!(hq.is_null(1)); // row1 company is None → children aligned with nulls
    assert!(hq.is_null(2)); // row2 company.hq is None

    // Address children: [city, zip, geo]
    let hq_city = as_string_array(hq.column(0));
    assert_eq!(hq_city.value(0), "NYC");

    let hq_zip = as_primitive_array::<Int32Type>(hq.column(1));
    assert!(hq_zip.is_null(0));

    let hq_geo = as_struct_array(hq.column(2));
    assert!(hq_geo.is_valid(0));
    let lat = as_primitive_array::<Float64Type>(hq_geo.column(0));
    let lon = as_primitive_array::<Float64Type>(hq_geo.column(1));
    assert_eq!(lat.value(0), 40.7128);
    assert_eq!(lon.value(0), -74.0060);

    // Validate sibling deep path: address -> geo on rows 1 and 2
    let addr = arrays.address; // StructArray [city, zip, geo]
    assert!(addr.is_null(0));
    assert!(addr.is_valid(1));
    assert!(addr.is_valid(2));

    let a_city = as_string_array(addr.column(0));
    assert_eq!(a_city.value(1), "SF");
    assert_eq!(a_city.value(2), "LA");

    let a_geo = as_struct_array(addr.column(2));
    assert!(a_geo.is_null(1));
    assert!(a_geo.is_valid(2));
    let a_lat = as_primitive_array::<Float64Type>(a_geo.column(0));
    let a_lon = as_primitive_array::<Float64Type>(a_geo.column(1));
    assert_eq!(a_lat.value(2), 34.0522);
    assert_eq!(a_lon.value(2), -118.2437);

    // Validate the list sibling behaves independently
    let scores = arrays.scores; // ListArray
    assert_eq!(scores.len(), 3);
    // row0: 3 items
    assert_eq!(scores.value_length(0), 3);
    // row1: null list
    assert!(scores.is_null(1));
    // row2: empty list
    assert_eq!(scores.value_length(2), 0);
}
