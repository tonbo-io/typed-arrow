use arrow_array::RecordBatch;
use typed_arrow::{bridge::Dictionary, prelude::*};

#[test]
fn test_dictionary_string_views() -> Result<(), SchemaError> {
    #[derive(Record)]
    struct TestRow {
        id: i32,
        category: Dictionary<i32, String>,
    }

    let rows = vec![
        TestRow {
            id: 1,
            category: Dictionary::new("apple".to_string()),
        },
        TestRow {
            id: 2,
            category: Dictionary::new("banana".to_string()),
        },
        TestRow {
            id: 3,
            category: Dictionary::new("apple".to_string()), // Repeated value
        },
    ];

    let mut b = <TestRow as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let expected = [(1, "apple"), (2, "banana"), (3, "apple")];

    let views = <TestRow as FromRecordBatch>::from_record_batch(&batch)?;
    for (idx, row_view) in views.enumerate() {
        let row_view = row_view.unwrap();
        assert_eq!(row_view.id, expected[idx].0);
        assert_eq!(row_view.category, expected[idx].1);
    }

    Ok(())
}

#[test]
fn test_dictionary_primitive_views() -> Result<(), SchemaError> {
    #[derive(Record)]
    struct TestRow {
        id: i32,
        code: Dictionary<i16, i64>,
    }

    let rows = vec![
        TestRow {
            id: 1,
            code: Dictionary::new(100),
        },
        TestRow {
            id: 2,
            code: Dictionary::new(200),
        },
        TestRow {
            id: 3,
            code: Dictionary::new(100), // Repeated
        },
    ];

    let mut b = <TestRow as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let expected = [(1, 100), (2, 200), (3, 100)];

    let views = <TestRow as FromRecordBatch>::from_record_batch(&batch)?;
    for (idx, row_view) in views.enumerate() {
        let row_view = row_view.unwrap();
        assert_eq!(row_view.id, expected[idx].0);
        assert_eq!(row_view.code, expected[idx].1);
    }
    Ok(())
}
