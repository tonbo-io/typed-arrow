// Comprehensive test showing all view types working together

use typed_arrow::arrow_array::RecordBatch;
use typed_arrow::{
    bridge::{Dictionary, List, Map},
    prelude::*,
};

#[derive(Union)]
enum Status {
    Active(i32),
    Inactive(String),
}

#[test]
fn test_all_view_types_together() -> Result<(), typed_arrow::schema::SchemaError> {
    #[derive(Record)]
    struct ComplexRow {
        // Primitives
        id: i64,
        score: f64,

        // Strings
        name: String,

        // Dictionary
        category: Dictionary<i32, String>,

        // List
        tags: List<String>,

        // Map
        metadata: Map<String, i32>,

        // Union
        status: Status,
    }

    let rows = vec![
        ComplexRow {
            id: 1,
            score: 95.5,
            name: "Alice".to_string(),
            category: Dictionary::new("premium".to_string()),
            tags: List::new(vec!["rust".to_string(), "arrow".to_string()]),
            metadata: Map::new(vec![("views".to_string(), 100), ("likes".to_string(), 50)]),
            status: Status::Active(42),
        },
        ComplexRow {
            id: 2,
            score: 87.3,
            name: "Bob".to_string(),
            category: Dictionary::new("standard".to_string()),
            tags: List::new(vec!["python".to_string()]),
            metadata: Map::new(vec![("views".to_string(), 200)]),
            status: Status::Inactive("on vacation".to_string()),
        },
    ];

    // Build RecordBatch
    let mut b = <ComplexRow as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    // Iterate over views using convenient API
    let views = batch.iter_views::<ComplexRow>()?;

    for (idx, row) in views.enumerate() {
        let row = row.unwrap();
        if idx == 0 {
            // Verify row 1
            assert_eq!(row.id, 1);
            assert!((row.score - 95.5).abs() < 0.001);
            assert_eq!(row.name, "Alice");
            assert_eq!(row.category, "premium");

            let tags0: Vec<_> = row.tags.map(|r| r.unwrap()).collect();
            assert_eq!(tags0, vec!["rust", "arrow"]);

            let metadata0: Vec<_> = row.metadata.map(|r| r.unwrap()).collect();
            assert_eq!(metadata0.len(), 2);

            match row.status {
                StatusView::Active(v) => assert_eq!(v, 42),
                _ => panic!("expected Active"),
            }
        } else if idx == 1 {
            // Verify row 2
            assert_eq!(row.id, 2);
            assert_eq!(row.name, "Bob");
            assert_eq!(row.category, "standard");

            let tags1: Vec<_> = row.tags.map(|r| r.unwrap()).collect();
            assert_eq!(tags1, vec!["python"]);

            match row.status {
                StatusView::Inactive(v) => assert_eq!(v, "on vacation"),
                _ => panic!("expected Inactive"),
            }
        }
    }

    Ok(())
}
