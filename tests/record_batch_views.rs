use arrow_array::RecordBatch;
use typed_arrow::prelude::*;

#[derive(typed_arrow::Record)]
struct SimpleRecord {
    id: i64,
    name: String,
    score: f32,
    active: Option<bool>,
}

#[test]
fn test_simple_record_batch_views() -> Result<(), SchemaError> {
    // Build rows
    let rows = vec![
        SimpleRecord {
            id: 1,
            name: "alice".into(),
            score: 10.5,
            active: Some(true),
        },
        SimpleRecord {
            id: 2,
            name: "bob".into(),
            score: 20.0,
            active: None,
        },
        SimpleRecord {
            id: 3,
            name: "carol".into(),
            score: 30.25,
            active: Some(false),
        },
    ];

    // Build RecordBatch
    let mut b = <SimpleRecord as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    // Create views iterator using convenient API
    let views = batch.iter_views::<SimpleRecord>()?;

    // Collect all views using the convenience method
    let collected = views.try_flatten()?;
    assert_eq!(collected.len(), 3);

    // Check first row
    assert_eq!(collected[0].id, 1);
    assert_eq!(collected[0].name, "alice");
    assert_eq!(collected[0].score, 10.5);
    assert_eq!(collected[0].active, Some(true));

    // Check second row
    assert_eq!(collected[1].id, 2);
    assert_eq!(collected[1].name, "bob");
    assert_eq!(collected[1].score, 20.0);
    assert_eq!(collected[1].active, None);

    // Check third row
    assert_eq!(collected[2].id, 3);
    assert_eq!(collected[2].name, "carol");
    assert_eq!(collected[2].score, 30.25);
    assert_eq!(collected[2].active, Some(false));

    Ok(())
}

#[derive(typed_arrow::Record)]
struct Address {
    city: String,
    zip: Option<i32>,
}

#[derive(typed_arrow::Record)]
struct Person {
    id: i64,
    address: Option<Address>,
    email: Option<String>,
}

#[test]
fn test_nested_record_batch_views() -> Result<(), SchemaError> {
    let rows = vec![
        Person {
            id: 1,
            address: Some(Address {
                city: "NYC".into(),
                zip: None,
            }),
            email: Some("a@example.com".into()),
        },
        Person {
            id: 2,
            address: None,
            email: None,
        },
        Person {
            id: 3,
            address: Some(Address {
                city: "SF".into(),
                zip: Some(94111),
            }),
            email: Some("c@example.com".into()),
        },
    ];

    // Build RecordBatch
    let mut b = <Person as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    // Create views iterator
    let views = batch.iter_views::<Person>()?;

    let collected = views.try_flatten()?;
    assert_eq!(collected.len(), 3);

    // Check first row with nested struct
    assert_eq!(collected[0].id, 1);
    assert!(collected[0].address.is_some());
    let addr = collected[0].address.as_ref().unwrap();
    assert_eq!(addr.city, "NYC");
    assert_eq!(addr.zip, None);
    assert_eq!(collected[0].email, Some("a@example.com"));

    // Check second row with null nested struct
    assert_eq!(collected[1].id, 2);
    assert!(collected[1].address.is_none());
    assert_eq!(collected[1].email, None);

    // Check third row
    assert_eq!(collected[2].id, 3);
    assert!(collected[2].address.is_some());
    let addr = collected[2].address.as_ref().unwrap();
    assert_eq!(addr.city, "SF");
    assert_eq!(addr.zip, Some(94111));
    assert_eq!(collected[2].email, Some("c@example.com"));

    Ok(())
}

#[test]
fn test_iterator_properties() -> Result<(), SchemaError> {
    let rows = vec![
        SimpleRecord {
            id: 1,
            name: "test".into(),
            score: 1.0,
            active: Some(true),
        },
        SimpleRecord {
            id: 2,
            name: "test2".into(),
            score: 2.0,
            active: None,
        },
    ];

    let mut b = <SimpleRecord as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let views = batch.iter_views::<SimpleRecord>()?;

    // Test size_hint
    assert_eq!(views.size_hint(), (2, Some(2)));

    // Test ExactSizeIterator
    assert_eq!(views.len(), 2);

    Ok(())
}
