use arrow_array::RecordBatch;
use typed_arrow::{bridge::Map, prelude::*};

#[test]
fn test_map_views() -> Result<(), SchemaError> {
    #[derive(typed_arrow::Record)]
    struct TestRow {
        id: i32,
        tags: Map<String, i32>,
    }

    let rows = vec![
        TestRow {
            id: 1,
            tags: Map::new(vec![("foo".to_string(), 10), ("bar".to_string(), 20)]),
        },
        TestRow {
            id: 2,
            tags: Map::new(vec![("baz".to_string(), 30)]),
        },
        TestRow {
            id: 3,
            tags: Map::new(vec![]),
        },
    ];

    let expected_data = [
        (1, vec![("foo", 10), ("bar", 20)]),
        (2, vec![("baz", 30)]),
        (3, vec![]),
    ];

    let mut b = <TestRow as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    // Test iteration over batch views
    let views = <TestRow as FromRecordBatch>::from_record_batch(&batch)?;
    for (idx, row_view) in views.enumerate() {
        let row_view = row_view.unwrap();
        assert_eq!(row_view.id, expected_data[idx].0);

        // Collect map entries into a Vec (unwrap Result items)
        let entries: Vec<_> = row_view.tags.map(|r| r.unwrap()).collect();

        assert_eq!(entries.len(), expected_data[idx].1.len());
        for (i, (key, val)) in entries.iter().enumerate() {
            assert_eq!(*key, expected_data[idx].1[i].0);
            assert_eq!(*val, expected_data[idx].1[i].1);
        }
    }

    Ok(())
}

#[test]
fn test_map_nullable_values_views() -> Result<(), SchemaError> {
    #[derive(typed_arrow::Record)]
    struct TestRow {
        id: i32,
        tags: Map<String, Option<i32>>,
    }

    let rows = vec![
        TestRow {
            id: 1,
            tags: Map::new(vec![
                ("foo".to_string(), Some(10)),
                ("bar".to_string(), None),
                ("baz".to_string(), Some(30)),
            ]),
        },
        TestRow {
            id: 2,
            tags: Map::new(vec![("null_val".to_string(), None)]),
        },
    ];

    let expected_data = [
        (1, vec![("foo", Some(10)), ("bar", None), ("baz", Some(30))]),
        (2, vec![("null_val", None)]),
    ];

    let mut b = <TestRow as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let views = <TestRow as FromRecordBatch>::from_record_batch(&batch)?;
    for (idx, row_view) in views.enumerate() {
        let row_view = row_view.unwrap();
        assert_eq!(row_view.id, expected_data[idx].0);

        let entries: Vec<_> = row_view.tags.map(|r| r.unwrap()).collect();

        assert_eq!(entries.len(), expected_data[idx].1.len());
        for (i, (key, val)) in entries.iter().enumerate() {
            assert_eq!(*key, expected_data[idx].1[i].0);
            assert_eq!(val, &expected_data[idx].1[i].1);
        }
    }

    Ok(())
}

#[test]
fn test_ordered_map_views() -> Result<(), SchemaError> {
    use std::collections::BTreeMap;

    use typed_arrow::bridge::OrderedMap;

    #[derive(typed_arrow::Record)]
    struct TestRow {
        id: i32,
        metadata: OrderedMap<String, String>,
    }

    let rows = vec![
        TestRow {
            id: 1,
            metadata: OrderedMap::new(BTreeMap::from([
                ("author".to_string(), "Alice".to_string()),
                ("version".to_string(), "1.0".to_string()),
            ])),
        },
        TestRow {
            id: 2,
            metadata: OrderedMap::new(BTreeMap::from([("author".to_string(), "Bob".to_string())])),
        },
    ];

    let expected_data = [
        (1, vec![("author", "Alice"), ("version", "1.0")]),
        (2, vec![("author", "Bob")]),
    ];

    let mut b = <TestRow as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let views = <TestRow as FromRecordBatch>::from_record_batch(&batch)?;
    for (idx, row_view) in views.enumerate() {
        let row_view = row_view.unwrap();
        assert_eq!(row_view.id, expected_data[idx].0);

        let entries: Vec<_> = row_view.metadata.map(|r| r.unwrap()).collect();

        assert_eq!(entries.len(), expected_data[idx].1.len());
        for (i, (key, val)) in entries.iter().enumerate() {
            assert_eq!(*key, expected_data[idx].1[i].0);
            assert_eq!(*val, expected_data[idx].1[i].1);
        }
    }

    Ok(())
}

#[test]
fn test_map_len_and_is_empty() -> Result<(), SchemaError> {
    #[derive(typed_arrow::Record)]
    struct TestRow {
        tags: Map<String, i32>,
    }

    let rows = vec![
        TestRow {
            tags: Map::new(vec![
                ("a".to_string(), 1),
                ("b".to_string(), 2),
                ("c".to_string(), 3),
            ]),
        },
        TestRow {
            tags: Map::new(vec![]),
        },
    ];

    let mut b = <TestRow as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let mut views = <TestRow as FromRecordBatch>::from_record_batch(&batch)?;

    let row0 = views.next().unwrap().unwrap();
    assert_eq!(row0.tags.len(), 3);
    assert!(!row0.tags.is_empty());

    let row1 = views.next().unwrap().unwrap();
    assert_eq!(row1.tags.len(), 0);
    assert!(row1.tags.is_empty());
    Ok(())
}
