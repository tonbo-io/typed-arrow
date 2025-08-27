use arrow_array::Array;
use typed_arrow::prelude::*;

#[derive(typed_arrow::Record)]
pub struct PersonR {
    pub id: i64,
    pub name: Option<String>,
    pub score: f32,
}

#[test]
#[allow(clippy::float_cmp)]
fn build_from_rows_flat() {
    // Prepare rows
    let rows = vec![
        PersonR {
            id: 1,
            name: Some("alice".to_string()),
            score: 10.5,
        },
        PersonR {
            id: 2,
            name: None,
            score: 20.0,
        },
        PersonR {
            id: 3,
            name: Some("carol".to_string()),
            score: 30.25,
        },
    ];

    // Build
    let mut b = <PersonR as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    // Validate typed arrays
    assert_eq!(arrays.id.len(), 3);
    assert_eq!(arrays.id.value(0), 1);
    assert_eq!(arrays.id.value(2), 3);

    assert_eq!(arrays.score.len(), 3);
    // float value check (approx not needed for simple values)
    assert_eq!(arrays.score.value(1), 20.0);

    assert_eq!(arrays.name.len(), 3);
    let name = arrays.name;
    assert_eq!(name.value(0), "alice");
    assert!(name.is_null(1));
    assert_eq!(name.value(2), "carol");
}
