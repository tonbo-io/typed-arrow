//! Showcase: Compile-time Schema + `RecordBatch` from typed arrays.

use arrow_array::{RecordBatch, cast::as_string_array};
use typed_arrow::{prelude::*, schema::SchemaMeta};

#[derive(typed_arrow::Record)]
struct AddressRB {
    city: String,
    zip: Option<i32>,
}

#[derive(typed_arrow::Record)]
struct PersonRB {
    id: i64,
    address: Option<AddressRB>,
    email: Option<String>,
}

fn main() {
    let rows = vec![
        PersonRB {
            id: 1,
            address: Some(AddressRB {
                city: "NYC".into(),
                zip: None,
            }),
            email: Some("a@example.com".into()),
        },
        PersonRB {
            id: 2,
            address: None,
            email: None,
        },
        PersonRB {
            id: 3,
            address: Some(AddressRB {
                city: "SF".into(),
                zip: Some(94111),
            }),
            email: Some("c@example.com".into()),
        },
    ];

    // Schema from type
    let schema = <PersonRB as SchemaMeta>::schema();
    println!("fields={}", schema.fields().len());

    // Build arrays from rows and into RecordBatch
    let mut b = <PersonRB as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();
    println!(
        "batch_rows={}, field0={}, field1={}, field2={}",
        batch.num_rows(),
        batch.schema().field(0).name(),
        batch.schema().field(1).name(),
        batch.schema().field(2).name()
    );

    // A quick peek at nested values
    let city = as_string_array(arrow_array::cast::as_struct_array(batch.column(1)).column(0));
    println!("first_city={}, third_city={}", city.value(0), city.value(2));
}
