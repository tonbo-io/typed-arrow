use typed_arrow::{prelude::*, schema::SchemaMeta};

#[derive(typed_arrow::Record)]
struct FieldNameOverride {
    #[record(name = "custom_name")]
    rust_field: i32,
    normal_field: String,
}

#[test]
fn field_name_override_in_schema() {
    let schema = <FieldNameOverride as SchemaMeta>::schema();

    // The field with the override should use the custom name
    assert!(schema.field_with_name("custom_name").is_ok());
    assert!(schema.field_with_name("rust_field").is_err());

    // The field without the override should use the Rust field name
    assert!(schema.field_with_name("normal_field").is_ok());
}

#[test]
fn field_name_override_in_record_batch() {
    let rows = vec![FieldNameOverride {
        rust_field: 42,
        normal_field: "hello".into(),
    }];
    let mut builders = <FieldNameOverride as BuildRows>::new_builders(rows.len());
    builders.append_rows(rows);
    let arrays = builders.finish();
    let batch = arrays.into_record_batch();

    // Verify the column names in the RecordBatch
    let schema = batch.schema();
    assert!(schema.field_with_name("custom_name").is_ok());
    assert!(schema.field_with_name("rust_field").is_err());
    assert!(schema.field_with_name("normal_field").is_ok());
}

#[derive(typed_arrow::Record)]
struct MultipleOverrides {
    #[record(name = "ID")]
    id: i64,
    #[record(name = "UserName")]
    user_name: String,
    #[record(name = "isActive")]
    is_active: bool,
}

#[test]
fn multiple_field_name_overrides() {
    let schema = <MultipleOverrides as SchemaMeta>::schema();

    assert!(schema.field_with_name("ID").is_ok());
    assert!(schema.field_with_name("id").is_err());

    assert!(schema.field_with_name("UserName").is_ok());
    assert!(schema.field_with_name("user_name").is_err());

    assert!(schema.field_with_name("isActive").is_ok());
    assert!(schema.field_with_name("is_active").is_err());
}

#[derive(typed_arrow::Record)]
struct OverrideWithMetadata {
    #[record(name = "renamed")]
    #[metadata(k = "description", v = "A renamed field")]
    field: i32,
}

#[test]
fn field_name_override_with_metadata() {
    let schema = <OverrideWithMetadata as SchemaMeta>::schema();

    let field = schema.field_with_name("renamed").unwrap();
    assert_eq!(
        field.metadata().get("description"),
        Some(&"A renamed field".to_string())
    );
}
