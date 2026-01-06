//! Showcase: Row-based building for flat records.

use typed_arrow::arrow_array::Array;
use typed_arrow::prelude::*;

#[derive(Record)]
struct PersonR {
    id: i64,
    name: Option<String>,
    score: f32,
}

fn main() {
    let rows = vec![
        PersonR {
            id: 1,
            name: Some("alice".into()),
            score: 10.5,
        },
        PersonR {
            id: 2,
            name: None,
            score: 20.0,
        },
        PersonR {
            id: 3,
            name: Some("carol".into()),
            score: 30.25,
        },
    ];

    let mut b = <PersonR as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    println!(
        "rows={}, id0={}, name1_null={}, score2={}",
        arrays.id.len(),
        arrays.id.value(0),
        arrays.name.is_null(1),
        arrays.score.value(2)
    );
}
