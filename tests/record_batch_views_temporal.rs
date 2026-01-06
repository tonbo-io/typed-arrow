use arrow_array::RecordBatch;
use typed_arrow::{Date32, Duration, Millisecond, Second, Timestamp, prelude::*};

#[derive(Record)]
struct Event {
    id: i64,
    timestamp: Timestamp<Millisecond>,
    date: Date32,
    duration: Option<Duration<Second>>,
}

#[test]
fn test_temporal_views() -> Result<(), SchemaError> {
    let rows = vec![
        Event {
            id: 1,
            timestamp: Timestamp::new(1700000000000),
            date: Date32::new(19000),
            duration: Some(Duration::new(3600)),
        },
        Event {
            id: 2,
            timestamp: Timestamp::new(1700000100000),
            date: Date32::new(19001),
            duration: None,
        },
    ];

    let mut b = <Event as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let views = <Event as FromRecordBatch>::from_record_batch(&batch)?;
    let collected: Vec<_> = views.map(|r| r.unwrap()).collect();

    assert_eq!(collected.len(), 2);

    // Check first row
    assert_eq!(collected[0].id, 1);
    assert_eq!(collected[0].timestamp.value(), 1700000000000);
    assert_eq!(collected[0].date.value(), 19000);
    assert!(collected[0].duration.is_some());
    if let Some(ref d) = collected[0].duration {
        assert_eq!(d.value(), 3600);
    }

    // Check second row
    assert_eq!(collected[1].id, 2);
    assert_eq!(collected[1].timestamp.value(), 1700000100000);
    assert_eq!(collected[1].date.value(), 19001);
    assert!(collected[1].duration.is_none());
    Ok(())
}
