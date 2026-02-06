use arrow_array::{Array, StringArray, StructArray};
use arrow_schema::DataType;
use typed_arrow::{prelude::*, schema::SchemaMeta};

#[derive(Record)]
struct C<T> {
    data: T,
}

#[derive(Record)]
struct S<T> {
    d: u32,
    c: C<T>,
}

#[test]
fn generic_schema_and_rows_u32() {
    let fields = <C<u32> as SchemaMeta>::fields();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].data_type(), &DataType::UInt32);

    let rows = vec![C { data: 1u32 }, C { data: 2u32 }];
    let mut b = <C<u32> as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    assert_eq!(arrays.data.len(), 2);
    assert_eq!(arrays.data.value(0), 1);
    assert_eq!(arrays.data.value(1), 2);
}

#[test]
fn generic_nested_struct_string() {
    let fields = <S<String> as SchemaMeta>::fields();
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].data_type(), &DataType::UInt32);
    match fields[1].data_type() {
        DataType::Struct(children) => {
            assert_eq!(children.len(), 1);
            assert_eq!(children[0].name(), "data");
            assert_eq!(children[0].data_type(), &DataType::Utf8);
        }
        other => panic!("expected Struct, got {other:?}"),
    }

    let rows = vec![
        S {
            d: 1,
            c: C {
                data: "a".to_string(),
            },
        },
        S {
            d: 2,
            c: C {
                data: "b".to_string(),
            },
        },
    ];
    let mut b = <S<String> as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    assert_eq!(arrays.d.len(), 2);
    assert_eq!(arrays.d.value(1), 2);

    let c: StructArray = arrays.c;
    assert_eq!(c.len(), 2);
    let data = c
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("data column is StringArray");
    assert_eq!(data.value(0), "a");
    assert_eq!(data.value(1), "b");
}

#[cfg(feature = "views")]
#[test]
fn generic_views_roundtrip() -> Result<(), typed_arrow::error::SchemaError> {
    let rows = vec![C { data: 10u32 }, C { data: 20u32 }];
    let mut b = <C<u32> as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let batch = b.finish().into_record_batch();

    let values: Vec<u32> = batch
        .iter_views::<C<u32>>()?
        .try_flatten()?
        .into_iter()
        .map(|v| v.data)
        .collect();
    assert_eq!(values, vec![10, 20]);
    Ok(())
}
