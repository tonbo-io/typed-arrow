use arrow_array::RecordBatch;
use arrow_schema::DataType;
use typed_arrow::prelude::*;

#[derive(Record)]
struct Event {
    id: i64,
    timestamp: jiff::Timestamp,
    date: jiff::civil::Date,
    description: String,
}

#[test]
fn test_jiff_roundtrip() -> Result<(), SchemaError> {
    let ts1 = jiff::Timestamp::from_microsecond(1_700_000_000_000_000).unwrap();
    let ts2 = jiff::Timestamp::from_microsecond(1_700_000_100_000_000).unwrap();

    let rows = vec![
        Event {
            id: 1,
            timestamp: ts1,
            date: jiff::civil::date(2024, 6, 15),
            description: "First event".to_string(),
        },
        Event {
            id: 2,
            timestamp: ts2,
            date: jiff::civil::date(1969, 12, 31),
            description: "Second event".to_string(),
        },
    ];

    assert_eq!(<Event as ColAt<2>>::data_type(), DataType::Date32);

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
    assert_eq!(collected[0].date, jiff::civil::date(2024, 6, 15));
    assert_eq!(collected[0].description, "First event");

    assert_eq!(collected[1].id, 2);
    assert_eq!(
        collected[1].timestamp.as_microsecond(),
        1_700_000_100_000_000
    );
    assert_eq!(collected[1].date, jiff::civil::date(1969, 12, 31));
    assert_eq!(collected[1].description, "Second event");

    Ok(())
}

#[test]
fn test_jiff_optional() -> Result<(), SchemaError> {
    #[derive(Record)]
    struct EventOptional {
        id: i64,
        timestamp: Option<jiff::Timestamp>,
        date: Option<jiff::civil::Date>,
    }

    let ts = jiff::Timestamp::from_microsecond(1_700_000_000_000_000).unwrap();

    let rows = vec![
        EventOptional {
            id: 1,
            timestamp: Some(ts),
            date: Some(jiff::civil::date(2024, 6, 15)),
        },
        EventOptional {
            id: 2,
            timestamp: None,
            date: None,
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
    assert_eq!(
        collected[0].timestamp.unwrap().as_microsecond(),
        1_700_000_000_000_000
    );
    assert_eq!(collected[0].date, Some(jiff::civil::date(2024, 6, 15)));

    assert_eq!(collected[1].id, 2);
    assert!(collected[1].timestamp.is_none());
    assert!(collected[1].date.is_none());

    Ok(())
}
