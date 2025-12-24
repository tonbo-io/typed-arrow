use arrow_array::RecordBatch;
use typed_arrow::prelude::*;

#[derive(Union)]
enum Value {
    I(i32),
    S(String),
}

#[test]
fn test_union_dense_views() -> Result<(), SchemaError> {
    #[derive(Record)]
    struct TestRow {
        id: i32,
        value: Value,
    }

    let rows = vec![
        TestRow {
            id: 1,
            value: Value::I(42),
        },
        TestRow {
            id: 2,
            value: Value::S("hello".to_string()),
        },
        TestRow {
            id: 3,
            value: Value::I(99),
        },
    ];

    let mut b = <TestRow as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let views = <TestRow as FromRecordBatch>::from_record_batch(&batch)?;
    let collected: Vec<_> = views.map(|r| r.unwrap()).collect();

    assert_eq!(collected.len(), 3);
    assert_eq!(collected[0].id, 1);
    assert_eq!(collected[1].id, 2);
    assert_eq!(collected[2].id, 3);

    // Check union values
    match collected[0].value {
        ValueView::I(v) => assert_eq!(v, 42),
        _ => panic!("expected I variant"),
    }

    match collected[1].value {
        ValueView::S(v) => assert_eq!(v, "hello"),
        _ => panic!("expected S variant"),
    }

    match collected[2].value {
        ValueView::I(v) => assert_eq!(v, 99),
        _ => panic!("expected I variant"),
    }

    Ok(())
}

#[derive(Union)]
#[union(mode = "sparse")]
enum SparseValue {
    A(i64),
    B(f64),
}

#[test]
fn test_union_sparse_views() -> Result<(), SchemaError> {
    #[derive(Record)]
    struct TestRow {
        id: i32,
        value: SparseValue,
    }

    let rows = vec![
        TestRow {
            id: 1,
            value: SparseValue::A(100),
        },
        TestRow {
            id: 2,
            value: SparseValue::B(3.125),
        },
    ];

    let mut b = <TestRow as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let views = <TestRow as FromRecordBatch>::from_record_batch(&batch)?;
    let collected: Vec<_> = views.map(|r| r.unwrap()).collect();

    assert_eq!(collected.len(), 2);

    match collected[0].value {
        SparseValueView::A(v) => assert_eq!(v, 100),
        _ => panic!("expected A variant"),
    }

    match collected[1].value {
        SparseValueView::B(v) => assert!((v - 3.125).abs() < 0.001),
        _ => panic!("expected B variant"),
    }
    Ok(())
}
