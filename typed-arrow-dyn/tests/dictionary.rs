use std::sync::Arc;

use arrow_array::{cast, Array, DictionaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use typed_arrow_dyn::{DynBuilders, DynCell, DynRow};

fn build_dict_batch() -> RecordBatch {
    // Schema: { d: Dictionary(Int32, Utf8) }
    let key = DataType::Int32;
    let value = DataType::Utf8;
    let dict = Field::new(
        "d",
        DataType::Dictionary(Box::new(key), Box::new(value)),
        true,
    );
    let schema = Arc::new(Schema::new(vec![dict]));

    let mut b = DynBuilders::new(Arc::clone(&schema), 0);
    // Values: ["apple", "banana", "apple", null, "banana"]
    b.append_option_row(Some(DynRow(vec![Some(DynCell::Str("apple".into()))])))
        .unwrap();
    b.append_option_row(Some(DynRow(vec![Some(DynCell::Str("banana".into()))])))
        .unwrap();
    b.append_option_row(Some(DynRow(vec![Some(DynCell::Str("apple".into()))])))
        .unwrap();
    b.append_option_row(Some(DynRow(vec![None]))).unwrap();
    b.append_option_row(Some(DynRow(vec![Some(DynCell::Str("banana".into()))])))
        .unwrap();

    b.finish_into_batch()
}

#[test]
fn dictionary_utf8_roundtrip() {
    let batch = build_dict_batch();
    assert_eq!(batch.num_rows(), 5);
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<arrow_array::types::Int32Type>>()
        .expect("dictionary array");

    // Decode each row to its string value and compare to expected sequence
    let keys = arr.keys();
    let dict_values = cast::as_string_array(arr.values().as_ref());
    let expected = [
        Some("apple"),
        Some("banana"),
        Some("apple"),
        None,
        Some("banana"),
    ];
    #[allow(clippy::needless_range_loop)]
    for i in 0..arr.len() {
        match expected[i] {
            None => assert!(arr.is_null(i)),
            Some(exp) => {
                assert!(arr.is_valid(i));
                let k = usize::try_from(keys.value(i)).expect("non-negative dictionary key");
                assert_eq!(dict_values.value(k), exp);
            }
        }
    }
}

#[test]
fn dictionary_binary_and_fixed_size() {
    // Schema: { bin: Dictionary(Int8, Binary), f4: Dictionary(Int16, FixedSizeBinary(4)) }
    let bin_field = Field::new(
        "bin",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Binary)),
        true,
    );
    let f4_field = Field::new(
        "f4",
        DataType::Dictionary(
            Box::new(DataType::Int16),
            Box::new(DataType::FixedSizeBinary(4)),
        ),
        true,
    );
    let schema = Arc::new(Schema::new(vec![bin_field, f4_field]));

    let mut b = DynBuilders::new(Arc::clone(&schema), 0);
    // Row0: bin=[0x01,0x02], f4=[0xAA,0xBB,0xCC,0xDD]
    b.append_option_row(Some(DynRow(vec![
        Some(DynCell::Bin(vec![1, 2])),
        Some(DynCell::Bin(vec![0xAA, 0xBB, 0xCC, 0xDD])),
    ])))
    .unwrap();
    // Row1: bin=null, f4=null
    b.append_option_row(Some(DynRow(vec![None, None]))).unwrap();
    // Row2: bin=[0x01,0x02], f4=[0xAA,0xBB,0xCC,0xDD] (repeat to test dictionary reuse)
    b.append_option_row(Some(DynRow(vec![
        Some(DynCell::Bin(vec![1, 2])),
        Some(DynCell::Bin(vec![0xAA, 0xBB, 0xCC, 0xDD])),
    ])))
    .unwrap();

    let batch = b.finish_into_batch();

    // bin column: Dictionary<Int8, Binary>
    let bin = batch
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<arrow_array::types::Int8Type>>()
        .unwrap();
    assert_eq!(bin.len(), 3);
    assert!(bin.is_valid(0));
    assert!(bin.is_null(1));
    assert!(bin.is_valid(2));
    let bin_keys = bin.keys();
    let bin_values = bin
        .values()
        .as_any()
        .downcast_ref::<arrow_array::BinaryArray>()
        .unwrap();
    // Decode row0 and row2 back to the byte sequences
    for &i in &[0usize, 2usize] {
        let k = usize::try_from(bin_keys.value(i)).expect("non-negative dictionary key");
        assert_eq!(bin_values.value(k), &[1u8, 2u8]);
    }

    // f4 column: Dictionary<Int16, FixedSizeBinary(4)>
    let f4 = batch
        .column(1)
        .as_any()
        .downcast_ref::<DictionaryArray<arrow_array::types::Int16Type>>()
        .unwrap();
    assert_eq!(f4.len(), 3);
    assert!(f4.is_valid(0));
    assert!(f4.is_null(1));
    assert!(f4.is_valid(2));
    let f4_keys = f4.keys();
    let f4_values = f4
        .values()
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
        .unwrap();
    for &i in &[0usize, 2usize] {
        let k = usize::try_from(f4_keys.value(i)).expect("non-negative dictionary key");
        assert_eq!(f4_values.value(k), &[0xAA, 0xBB, 0xCC, 0xDD]);
    }
}
