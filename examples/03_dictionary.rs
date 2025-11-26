//! Showcase: Dictionary<K, String> columns using typed builder.

use arrow_array::{Array, DictionaryArray};
use typed_arrow::{Dictionary, prelude::*};

#[derive(typed_arrow::Record)]
struct Row {
    code: Dictionary<i32, String>,
    opt_code: Option<Dictionary<i8, String>>,
}

fn main() {
    // Compile-time DataTypes
    println!(
        "code={:?}, opt_code={:?}",
        <Row as ColAt<0>>::data_type(),
        <Row as ColAt<1>>::data_type()
    );

    // Build a dictionary<i32, Utf8> via StringDictionaryBuilder
    let mut b: <Row as ColAt<0>>::ColumnBuilder =
        arrow_array::builder::StringDictionaryBuilder::new();
    let _ = b.append("foo");
    let _ = b.append("bar");
    let _ = b.append("foo");
    let a: <Row as ColAt<0>>::ColumnArray = b.finish();
    let dict: &DictionaryArray<arrow_array::types::Int32Type> = &a;
    println!("dict_len={}, keys_type={:?}", dict.len(), dict.data_type());
}
