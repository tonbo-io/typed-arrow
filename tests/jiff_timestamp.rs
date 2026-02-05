use arrow_array::RecordBatch;
use typed_arrow::prelude::*;

#[derive(Record)]
struct Event {
    id: i64,
    timestamp: jiff::Timestamp,
    description: String,
}

#[test]
fn test_jiff_timestamp_roundtrip() -> Result<(), SchemaError> {
    let ts1 = jiff::Timestamp::from_microsecond(1_700_000_000_000_000).unwrap();
    let ts2 = jiff::Timestamp::from_microsecond(1_700_000_100_000_000).unwrap();

    let rows = vec![
        Event {
            id: 1,
            timestamp: ts1,
            description: "First event".to_string(),
        },
        Event {
            id: 2,
            timestamp: ts2,
            description: "Second event".to_string(),
        },
    ];

    let mut b = <Event as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let views = <Event as FromRecordBatch>::from_record_batch(&batch)?;
    let collected: Vec<_> = views.map(|r| r.unwrap()).collect();

    assert_eq!(collected.len(), 2);

    assert_eq!(collected[0].id, 1);
    assert_eq!(
        collected[0].timestamp.as_microsecond(),
        1_700_000_000_000_000
    );
    assert_eq!(collected[0].description, "First event");

    assert_eq!(collected[1].id, 2);
    assert_eq!(
        collected[1].timestamp.as_microsecond(),
        1_700_000_100_000_000
    );
    assert_eq!(collected[1].description, "Second event");

    Ok(())
}

#[test]
fn test_jiff_timestamp_optional() -> Result<(), SchemaError> {
    #[derive(Record)]
    struct EventOptional {
        id: i64,
        timestamp: Option<jiff::Timestamp>,
    }

    let ts = jiff::Timestamp::from_microsecond(1_700_000_000_000_000).unwrap();

    let rows = vec![
        EventOptional {
            id: 1,
            timestamp: Some(ts),
        },
        EventOptional {
            id: 2,
            timestamp: None,
        },
    ];

    let mut b = <EventOptional as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let views = <EventOptional as FromRecordBatch>::from_record_batch(&batch)?;
    let collected: Vec<_> = views.map(|r| r.unwrap()).collect();

    assert_eq!(collected.len(), 2);

    assert_eq!(collected[0].id, 1);
    assert!(collected[0].timestamp.is_some());
    assert_eq!(
        collected[0].timestamp.unwrap().as_microsecond(),
        1_700_000_000_000_000
    );

    assert_eq!(collected[1].id, 2);
    assert!(collected[1].timestamp.is_none());

    Ok(())
}
