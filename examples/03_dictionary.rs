//! Showcase: Dictionary<K, String> columns using typed builder.

use arrow_array::{Array, DictionaryArray};
use typed_arrow::{prelude::*, Dictionary};

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
    type A0 = <Row as ColAt<0>>::ColumnArray;
    type B0 = <Row as ColAt<0>>::ColumnBuilder;
    let mut b: B0 = arrow_array::builder::StringDictionaryBuilder::new();
    let _ = b.append("foo");
    let _ = b.append("bar");
    let _ = b.append("foo");
    let a: A0 = b.finish();
    let dict: &DictionaryArray<arrow_array::types::Int32Type> = &a;
    println!("dict_len={}, keys_type={:?}", dict.len(), dict.data_type());
}
