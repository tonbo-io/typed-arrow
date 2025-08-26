use std::sync::Arc;

use arrow_array::{cast, Array, DictionaryArray};
use arrow_schema::{DataType, Field, Schema};
use typed_arrow_dyn::{DynBuilders, DynCell, DynRow};

#[test]
fn dictionary_unsigned_key_utf8_values() {
    // Schema: { d: Dictionary(UInt8, Utf8) }
    let dict_field = Field::new(
        "d",
        DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
        true,
    );
    let schema = Arc::new(Schema::new(vec![dict_field]));
    let mut b = DynBuilders::new(Arc::clone(&schema), 0);

    b.append_option_row(Some(DynRow(vec![Some(DynCell::Str("a".into()))])))
        .unwrap();
    b.append_option_row(Some(DynRow(vec![Some(DynCell::Str("b".into()))])))
        .unwrap();
    b.append_option_row(Some(DynRow(vec![Some(DynCell::Str("a".into()))])))
        .unwrap();
    b.append_option_row(Some(DynRow(vec![None]))).unwrap();

    let batch = b.finish_into_batch();
    assert_eq!(batch.num_rows(), 4);
    // Downcast to DictionaryArray<UInt8Type>
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<arrow_array::types::UInt8Type>>()
        .expect("dictionary array with UInt8 keys");
    assert!(arr.is_valid(0));
    assert!(arr.is_valid(1));
    assert!(arr.is_valid(2));
    assert!(arr.is_null(3));
    let dict_values = cast::as_string_array(arr.values().as_ref());
    // Check that the dictionary contains both values
    assert!(dict_values.iter().any(|v| v == Some("a")));
    assert!(dict_values.iter().any(|v| v == Some("b")));
}

#[test]
fn dictionary_primitive_values_roundtrip() {
    // Schema: { d: Dictionary(Int16, UInt32) }
    let field = Field::new(
        "d",
        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::UInt32)),
        true,
    );
    let schema = Arc::new(Schema::new(vec![field]));
    let mut b = DynBuilders::new(Arc::clone(&schema), 0);

    // Values: [1, 2, 1, null, 3]
    b.append_option_row(Some(DynRow(vec![Some(DynCell::U32(1))])))
        .unwrap();
    b.append_option_row(Some(DynRow(vec![Some(DynCell::U32(2))])))
        .unwrap();
    b.append_option_row(Some(DynRow(vec![Some(DynCell::U32(1))])))
        .unwrap();
    b.append_option_row(Some(DynRow(vec![None]))).unwrap();
    b.append_option_row(Some(DynRow(vec![Some(DynCell::U32(3))])))
        .unwrap();

    let batch = b.finish_into_batch();
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<arrow_array::types::Int16Type>>()
        .expect("dict array");
    assert_eq!(arr.len(), 5);
    // Verify keys resolve to expected values
    let keys = arr.keys();
    let values = arr
        .values()
        .as_any()
        .downcast_ref::<arrow_array::UInt32Array>()
        .unwrap();
    let expected = [Some(1u32), Some(2), Some(1), None, Some(3)];
    for (i, exp_opt) in expected.iter().copied().enumerate() {
        match exp_opt {
            None => assert!(arr.is_null(i)),
            Some(exp) => {
                let k = keys.value(i) as usize;
                assert_eq!(values.value(k), exp);
            }
        }
    }
}
