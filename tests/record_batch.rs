use typed_arrow::arrow_array::{Array, RecordBatch, cast::as_string_array};
use typed_arrow::{prelude::*, schema::SchemaMeta};

#[derive(Record)]
struct AddressRB {
    city: String,
    zip: Option<i32>,
}

#[derive(Record)]
struct PersonRB {
    id: i64,
    address: Option<AddressRB>,
    email: Option<String>,
}

#[test]
fn schema_and_record_batch_from_rows() {
    // Build rows (no null rows to keep required columns satisfied)
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

    // Generate schema from type
    let schema = <PersonRB as SchemaMeta>::schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "address");
    assert_eq!(schema.field(2).name(), "email");

    // Build arrays from rows
    let mut b = <PersonRB as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    // Into RecordBatch
    let batch: RecordBatch = arrays.into_record_batch();
    assert_eq!(batch.schema().fields().len(), 3);
    assert_eq!(batch.num_rows(), 3);

    // Validate id column
    let id =
        typed_arrow::arrow_array::cast::as_primitive_array::<typed_arrow::arrow_array::types::Int64Type>(batch.column(0));
    assert_eq!(id.value(0), 1);
    assert_eq!(id.value(2), 3);

    // Validate nested address struct
    let address = typed_arrow::arrow_array::cast::as_struct_array(batch.column(1));
    assert!(address.is_null(1));
    let city = as_string_array(address.column(0));
    assert_eq!(city.value(0), "NYC");
    assert_eq!(city.value(2), "SF");

    // Email column
    let email = as_string_array(batch.column(2));
    assert_eq!(email.value(0), "a@example.com");
    assert!(email.is_null(1));
}
