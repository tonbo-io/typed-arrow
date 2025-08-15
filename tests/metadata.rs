use std::collections::HashMap;

use typed_arrow::{prelude::*, schema::SchemaMeta};

#[derive(typed_arrow::Record)]
#[schema_metadata(k = "owner", v = "team")]
#[record(schema_metadata(k = "env", v = "dev"))]
struct MetaDemo {
    #[metadata(k = "pii", v = "no")]
    a: i32,
    b: Option<String>,
}

#[test]
fn schema_and_field_metadata_are_applied() {
    // Top-level schema metadata
    let schema = <MetaDemo as SchemaMeta>::schema();
    let m: &HashMap<String, String> = schema.metadata();
    assert_eq!(m.get("owner"), Some(&"team".to_string()));
    assert_eq!(m.get("env"), Some(&"dev".to_string()));

    // Field-level metadata on column 'a'
    let fa = schema.field_with_name("a").unwrap();
    let fam: &HashMap<String, String> = fa.metadata();
    assert_eq!(fam.get("pii"), Some(&"no".to_string()));

    // Column 'b' has no field metadata
    let fb = schema.field_with_name("b").unwrap();
    assert!(fb.metadata().is_empty());

    // Also ensure the RecordBatch built from arrays preserves schema metadata
    let rows = vec![MetaDemo {
        a: 1,
        b: Some("x".into()),
    }];
    let mut builders = <MetaDemo as BuildRows>::new_builders(rows.len());
    builders.append_rows(rows);
    let arrays = builders.finish();
    let batch = arrays.into_record_batch();
    let schema2 = batch.schema();
    let bm = schema2.metadata();
    assert_eq!(bm.get("owner"), Some(&"team".to_string()));
    assert_eq!(bm.get("env"), Some(&"dev".to_string()));
}
