use arrow_array::Array;
use typed_arrow::{bridge::ArrowBinding, Map, OrderedMap, Record};

#[derive(Record)]
struct Row {
    id: i32,
    tags: Map<String, i32>,
    notes: Option<Map<String, Option<String>>>, // nullable map with nullable values
}

fn main() {
    // Standalone Map column
    let mut b = <Map<String, i32> as ArrowBinding>::new_builder(0);
    <Map<String, i32> as ArrowBinding>::append_value(
        &mut b,
        &Map(vec![("a".to_string(), 1), ("b".to_string(), 2)]),
    );
    <Map<String, i32> as ArrowBinding>::append_null(&mut b);
    let a = <Map<String, i32> as ArrowBinding>::finish(b);
    println!(
        "standalone map rows={}, entries={}",
        a.len(),
        a.entries().len()
    );

    // Sorted keys example (keys_sorted = true)
    let dt_sorted = <Map<String, i32, true> as ArrowBinding>::data_type();
    println!("sorted map datatype = {dt_sorted:?}");

    // OrderedMap using BTreeMap row layout
    use std::collections::BTreeMap;
    let mut ord = BTreeMap::new();
    ord.insert("b".to_string(), 2);
    ord.insert("a".to_string(), 1);
    let mut ob = <OrderedMap<String, i32> as ArrowBinding>::new_builder(0);
    <OrderedMap<String, i32> as ArrowBinding>::append_value(&mut ob, &OrderedMap(ord));
    let oa = <OrderedMap<String, i32> as ArrowBinding>::finish(ob);
    println!("ordered map entries={}", oa.entries().len());

    // In a Record
    let rows = vec![
        Row {
            id: 1,
            tags: Map(vec![("x".to_string(), 10)]),
            notes: Some(Map(vec![
                ("hello".to_string(), Some("world".to_string())),
                ("empty".to_string(), None),
            ])),
        },
        Row {
            id: 2,
            tags: Map(vec![]),
            notes: None,
        },
    ];
    let mut rb = <Row as typed_arrow::schema::BuildRows>::new_builders(rows.len());
    rb.append_rows(rows);
    let arrays = rb.finish();
    let batch = arrays.into_record_batch();
    println!(
        "record rows={}, fields={}",
        batch.num_rows(),
        batch.schema().fields().len()
    );
}
