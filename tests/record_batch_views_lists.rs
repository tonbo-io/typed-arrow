use typed_arrow::arrow_array::RecordBatch;
use typed_arrow::{FixedSizeList, LargeList, List, prelude::*};

#[derive(Record)]
struct ListRecord {
    id: i64,
    values: List<i32>,
}

#[test]
fn test_list_views() -> Result<(), SchemaError> {
    let rows = vec![
        ListRecord {
            id: 1,
            values: List::new(vec![1, 2, 3]),
        },
        ListRecord {
            id: 2,
            values: List::new(vec![4, 5]),
        },
    ];

    let mut b = <ListRecord as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let mut views = <ListRecord as FromRecordBatch>::from_record_batch(&batch)?;

    // Check first row
    let first = views.next().unwrap().unwrap();
    assert_eq!(first.id, 1);

    let first_values: Vec<i32> = first.values.map(|r| r.unwrap()).collect();
    assert_eq!(first_values, vec![1, 2, 3]);

    // Check second row
    let second = views.next().unwrap().unwrap();
    assert_eq!(second.id, 2);

    let second_values: Vec<i32> = second.values.map(|r| r.unwrap()).collect();
    assert_eq!(second_values, vec![4, 5]);

    assert!(views.next().is_none());

    Ok(())
}

#[derive(Record)]
struct ListNullableRecord {
    id: i64,
    nullable_values: List<Option<i32>>,
}

#[test]
fn test_list_nullable_views() -> Result<(), SchemaError> {
    let rows = vec![
        ListNullableRecord {
            id: 1,
            nullable_values: List::new(vec![Some(10), None, Some(30)]),
        },
        ListNullableRecord {
            id: 2,
            nullable_values: List::new(vec![Some(40), Some(50)]),
        },
    ];

    let mut b = <ListNullableRecord as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let mut views = <ListNullableRecord as FromRecordBatch>::from_record_batch(&batch)?;

    // Check first row
    let first = views.next().unwrap().unwrap();
    assert_eq!(first.id, 1);

    let first_nullable: Vec<Option<i32>> = first.nullable_values.map(|r| r.unwrap()).collect();
    assert_eq!(first_nullable, vec![Some(10), None, Some(30)]);

    // Check second row
    let second = views.next().unwrap().unwrap();
    assert_eq!(second.id, 2);

    let second_nullable: Vec<Option<i32>> = second.nullable_values.map(|r| r.unwrap()).collect();
    assert_eq!(second_nullable, vec![Some(40), Some(50)]);

    assert!(views.next().is_none());

    Ok(())
}

#[derive(Record)]
struct FixedSizeListRecord {
    id: i64,
    coordinates: FixedSizeList<f64, 3>,
}

#[test]
fn test_fixed_size_list_views() -> Result<(), SchemaError> {
    let rows = vec![
        FixedSizeListRecord {
            id: 1,
            coordinates: FixedSizeList::new([1.0, 2.0, 3.0]),
        },
        FixedSizeListRecord {
            id: 2,
            coordinates: FixedSizeList::new([4.0, 5.0, 6.0]),
        },
    ];

    let mut b = <FixedSizeListRecord as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let mut views = <FixedSizeListRecord as FromRecordBatch>::from_record_batch(&batch)?;

    // Check first row
    let first = views.next().unwrap().unwrap();
    assert_eq!(first.id, 1);
    let first_coords: Vec<f64> = first.coordinates.map(|r| r.unwrap()).collect();
    assert_eq!(first_coords, vec![1.0, 2.0, 3.0]);

    // Check second row
    let second = views.next().unwrap().unwrap();
    assert_eq!(second.id, 2);
    let second_coords: Vec<f64> = second.coordinates.map(|r| r.unwrap()).collect();
    assert_eq!(second_coords, vec![4.0, 5.0, 6.0]);

    assert!(views.next().is_none());

    Ok(())
}

#[derive(Record)]
struct LargeListRecord {
    id: i64,
    large_values: LargeList<String>,
}

#[test]
fn test_large_list_views() -> Result<(), SchemaError> {
    let rows = vec![
        LargeListRecord {
            id: 1,
            large_values: LargeList::new(vec!["hello".to_string(), "world".to_string()]),
        },
        LargeListRecord {
            id: 2,
            large_values: LargeList::new(vec!["foo".to_string()]),
        },
    ];

    let mut b = <LargeListRecord as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let mut views = <LargeListRecord as FromRecordBatch>::from_record_batch(&batch)?;

    // Check first row
    let first = views.next().unwrap().unwrap();
    assert_eq!(first.id, 1);
    let first_values: Vec<&str> = first.large_values.map(|r| r.unwrap()).collect();
    assert_eq!(first_values, vec!["hello", "world"]);

    // Check second row
    let second = views.next().unwrap().unwrap();
    assert_eq!(second.id, 2);
    let second_values: Vec<&str> = second.large_values.map(|r| r.unwrap()).collect();
    assert_eq!(second_values, vec!["foo"]);

    assert!(views.next().is_none());

    Ok(())
}

#[derive(Record)]
struct NestedListRecord {
    id: i64,
    nested: List<List<i32>>,
}

#[test]
fn test_nested_list_views() -> Result<(), SchemaError> {
    let rows = vec![
        NestedListRecord {
            id: 1,
            nested: List::new(vec![List::new(vec![1, 2]), List::new(vec![3, 4, 5])]),
        },
        NestedListRecord {
            id: 2,
            nested: List::new(vec![List::new(vec![6])]),
        },
    ];

    let mut b = <NestedListRecord as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let mut views = <NestedListRecord as FromRecordBatch>::from_record_batch(&batch)?;

    // Check first row
    let first = views.next().unwrap().unwrap();
    assert_eq!(first.id, 1);
    let first_nested: Vec<Vec<i32>> = first
        .nested
        .map(|inner_list| inner_list.unwrap().map(|r| r.unwrap()).collect::<Vec<_>>())
        .collect();
    assert_eq!(first_nested, vec![vec![1, 2], vec![3, 4, 5]]);

    // Check second row
    let second = views.next().unwrap().unwrap();
    assert_eq!(second.id, 2);
    let second_nested: Vec<Vec<i32>> = second
        .nested
        .map(|inner_list| inner_list.unwrap().map(|r| r.unwrap()).collect::<Vec<_>>())
        .collect();
    assert_eq!(second_nested, vec![vec![6]]);

    assert!(views.next().is_none());
    Ok(())
}
