//! Showcase: Extend `#[derive(Record)]` via container/field hooks.
//!
//! Demonstrates:
//! - per-field macro callback: `#[record(field_macro = per_field)]`
//! - per-record macro callback: `#[record(record_macro = per_record)]`
//! - field-level markers forwarded as tokens: `#[record(ext(key))]`
//! - compile-time visitor traversal via `ForEachCol`

use typed_arrow::prelude::*;

// A simple trait we implement via the per-field macro when `ext(key)` is present
trait PrimaryKey {
    type Key;
    const FIELD_INDEX: usize;
    const FIELD_NAME: &'static str;
}

// Per-field macro: generates a PrimaryKey impl only when `ext(key)` appears
macro_rules! per_field {
    (owner = $owner:ty, index = $idx:tt, field = $fname:ident, ty = $ty:ty, nullable = $nul:expr, ext = (key $($rest:tt)*)) => {
        impl PrimaryKey for $owner {
            type Key = $ty;
            const FIELD_INDEX: usize = $idx;
            const FIELD_NAME: &'static str = stringify!($fname);
        }
    };
    // default: no-op for other fields
    (owner = $owner:ty, index = $idx:tt, field = $fname:ident, ty = $ty:ty, nullable = $nul:expr, ext = ($($rest:tt)*)) => {};
}

// Per-record macro: just define a const with the number of columns
macro_rules! per_record {
    (owner = $owner:ty, len = $len:expr, ext = ($($rest:tt)*)) => {
        impl $owner {
            const __NUM_COLS: usize = $len;
        }
    };
}

// Visitor used to demonstrate compile-time column traversal. It doesn’t have
// side-effects — it just forces monomorphization for each column/base type.
struct DebugVisitor;
impl ColumnVisitor for DebugVisitor {
    fn visit<const I: usize, R>(_m: FieldMeta<R>) {
        let _ = I;
    }
}

#[derive(typed_arrow::Record)]
#[record(field_macro = "per_field", record_macro = "per_record")]
struct Person {
    #[record(ext(key))]
    id: i64,
    name: Option<String>,
    score: f32,
}

fn main() {
    // Demonstrate visitor traversal
    <Person as ForEachCol>::for_each_col::<DebugVisitor>();

    // Normal typed builders still work
    let mut b = <Person as BuildRows>::new_builders(2);
    b.append_row(Person {
        id: 1,
        name: Some("a".into()),
        score: 3.5,
    });
    b.append_row(Person {
        id: 2,
        name: None,
        score: 4.0,
    });
    let arrays = b.finish();
    let _batch = arrays.into_record_batch();

    // Show generated data from macros
    println!("columns: {}", Person::__NUM_COLS);
    println!(
        "pk field: {}@{}",
        <Person as PrimaryKey>::FIELD_NAME,
        <Person as PrimaryKey>::FIELD_INDEX
    );
}
