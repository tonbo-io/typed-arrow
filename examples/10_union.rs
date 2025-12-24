use arrow_array::{Array, Int32Array, StringArray, UnionArray};
use typed_arrow::{bridge::ArrowBinding, prelude::*};

// Dense Union with attributes: explicit tags, per-variant field names, and null carrier
#[derive(Union)]
#[union(tags(Num = 10, Str = 7))]
enum Value {
    #[union(null, field = "int_value")]
    Num(i32),
    #[union(field = "str_value")]
    Str(String),
}

// Sparse Union: children align in length with union
#[derive(Union)]
#[union(mode = "sparse")]
enum ValueSparse {
    I(i32),
    S(String),
}

// Use the union as a column in a Record
#[derive(Record)]
struct Row {
    id: i32,
    value: Value,
}

fn main() {
    // Inspect DataType for the union and the record column
    let udt = <Value as ArrowBinding>::data_type();
    println!("Union DataType: {udt:?}");
    let col_dt = <Row as ColAt<1>>::data_type();
    println!("Record column[1] DataType: {col_dt:?}");

    // Standalone Union builder: [Str("x"), Num(1), null, Num(7)]
    let mut b = <Value as ArrowBinding>::new_builder(4);
    <Value as ArrowBinding>::append_value(&mut b, &Value::Str("x".into())); // tag 7
    <Value as ArrowBinding>::append_value(&mut b, &Value::Num(1)); // tag 10
    <Value as ArrowBinding>::append_null(&mut b); // null carried by Num (tag 10)
    <Value as ArrowBinding>::append_value(&mut b, &Value::Num(7)); // tag 10
    let arr: UnionArray = <Value as ArrowBinding>::finish(b);

    // Read using type_ids + per-variant offsets; fetch children by tag
    let ints = arr.child(10).as_any().downcast_ref::<Int32Array>().unwrap();
    let strs = arr.child(7).as_any().downcast_ref::<StringArray>().unwrap();
    for i in 0..arr.len() {
        let tid = arr.type_id(i);
        let off = arr.value_offset(i);
        match tid {
            10 => {
                if ints.is_null(off) {
                    println!("row {i} => Num = null");
                } else {
                    println!("row {} => Num = {}", i, ints.value(off));
                }
            }
            7 => {
                println!("row {} => Str = {}", i, strs.value(off));
            }
            _ => unreachable!(),
        }
    }

    // Use union as a Record column: build a RecordBatch
    let rows = vec![
        Row {
            id: 1,
            value: Value::Num(5),
        },
        Row {
            id: 2,
            value: Value::Str("a".into()),
        },
        Row {
            id: 3,
            value: Value::Num(8),
        },
    ];
    let mut rb = <Row as BuildRows>::new_builders(rows.len());
    rb.append_rows(rows);
    let arrays = rb.finish();
    let batch = arrays.into_record_batch();
    println!(
        "RecordBatch rows={}, fields={}",
        batch.num_rows(),
        batch.schema().fields().len()
    );

    // Sparse union demo: [I(2), S("y"), null, I(8)]
    let mut bs = <ValueSparse as ArrowBinding>::new_builder(4);
    <ValueSparse as ArrowBinding>::append_value(&mut bs, &ValueSparse::I(2));
    <ValueSparse as ArrowBinding>::append_value(&mut bs, &ValueSparse::S("y".into()));
    <ValueSparse as ArrowBinding>::append_null(&mut bs); // null encoded via first variant by default
    <ValueSparse as ArrowBinding>::append_value(&mut bs, &ValueSparse::I(8));
    let arr_s: UnionArray = <ValueSparse as ArrowBinding>::finish(bs);
    println!(
        "Sparse len={}, mode_is_sparse={}",
        arr_s.len(),
        matches!(
            <ValueSparse as ArrowBinding>::data_type(),
            arrow_schema::DataType::Union(_, arrow_schema::UnionMode::Sparse)
        )
    );
    let ints_s = arr_s
        .child(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let strs_s = arr_s
        .child(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..arr_s.len() {
        match arr_s.type_id(i) {
            0 => {
                if ints_s.is_null(i) {
                    println!("sparse row {i} => I = null");
                } else {
                    println!("sparse row {i} => I = {}", ints_s.value(i));
                }
            }
            1 => println!("sparse row {i} => S = {}", strs_s.value(i)),
            _ => unreachable!(),
        }
    }
}
