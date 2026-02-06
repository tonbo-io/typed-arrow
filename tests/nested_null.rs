use typed_arrow::{Null, Record, schema::BuildRows};

#[derive(Debug, Record)]
struct Inner {
    always_null: Null,
}

#[derive(Debug, Record)]
struct MyStruct {
    inner: Inner,
}

#[test]
fn nested_null() {
    // Deserialize JSON into structs
    let events: Vec<MyStruct> = vec![MyStruct {
        inner: Inner { always_null: Null },
    }];

    // Convert to Arrow RecordBatch using typed-arrow
    let mut builders = <MyStruct as BuildRows>::new_builders(events.len());
    builders.append_rows(events);
    let _ = builders.finish().into_record_batch();
}
