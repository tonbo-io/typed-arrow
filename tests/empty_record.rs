use arrow_array::{Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Fields};
use typed_arrow::{
    bridge::{ArrowBinding, ArrowBindingView},
    prelude::*,
};

#[derive(Record, Debug, Clone, PartialEq)]
struct Empty {}

#[derive(Record, Debug, Clone, PartialEq)]
struct Outer {
    id: u32,
    marker: Empty,
    maybe_marker: Option<Empty>,
}

#[test]
fn empty_record_is_a_struct_without_fields() {
    assert_eq!(
        <Empty as ArrowBinding>::data_type(),
        DataType::Struct(Fields::empty())
    );
}

#[test]
fn empty_record_array_keeps_rows_and_nulls() -> Result<(), SchemaError> {
    let mut builder = <Empty as ArrowBinding>::new_builder(3);
    <Empty as ArrowBinding>::append_value(&mut builder, &Empty {});
    <Empty as ArrowBinding>::append_null(&mut builder);
    <Empty as ArrowBinding>::append_value(&mut builder, &Empty {});
    let array: StructArray = <Empty as ArrowBinding>::finish(builder);
    assert_eq!(array.len(), 3);
    assert_eq!(array.num_columns(), 0);
    assert!(array.is_null(1));

    let view = <Empty as ArrowBindingView>::get_view(&array, 0)?;
    assert_eq!(Empty::try_from(view)?, Empty {});
    let null_view = <Option<Empty> as ArrowBindingView>::get_view(&array, 1)?;
    assert!(null_view.is_none());
    Ok(())
}

#[test]
fn nested_empty_record_round_trips_through_record_batch() -> Result<(), SchemaError> {
    let rows = vec![
        Outer {
            id: 1,
            marker: Empty {},
            maybe_marker: Some(Empty {}),
        },
        Outer {
            id: 2,
            marker: Empty {},
            maybe_marker: None,
        },
    ];
    let mut builders = <Outer as BuildRows>::new_builders(rows.len());
    builders.append_rows(rows.clone());
    let batch: RecordBatch = builders.finish().into_record_batch();

    let mut decoded = Vec::new();
    for view in batch.iter_views::<Outer>()? {
        decoded.push(Outer::try_from(view?)?);
    }
    assert_eq!(decoded, rows);
    Ok(())
}

#[test]
fn empty_record_batch_keeps_its_row_count() -> Result<(), SchemaError> {
    let mut builders = <Empty as BuildRows>::new_builders(3);
    builders.append_rows(vec![Empty {}, Empty {}]);
    builders.append_row_ref(&Empty {});
    // Null rows count too: a null struct row is still a row of the batch.
    builders.append_null_row();
    builders.append_option_row(None);
    let batch: RecordBatch = builders.finish().into_record_batch();
    assert_eq!(batch.num_columns(), 0);
    assert_eq!(batch.num_rows(), 5);

    // The views iterator is the generated item that failed to compile before the fix.
    let mut decoded = Vec::new();
    for view in batch.iter_views::<Empty>()? {
        decoded.push(Empty::try_from(view?)?);
    }
    assert_eq!(decoded, vec![Empty {}; 5]);
    Ok(())
}

#[test]
fn empty_record_batch_without_rows_is_empty() {
    let builders = <Empty as BuildRows>::new_builders(0);
    let batch: RecordBatch = builders.finish().into_record_batch();
    assert_eq!(batch.num_rows(), 0);
}
