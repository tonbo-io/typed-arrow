#![allow(clippy::assertions_on_constants, clippy::bool_assert_comparison)]
use arrow_array::types::Int32Type;
use typed_arrow::{Dictionary, prelude::*};

#[derive(typed_arrow::Record)]
pub struct Row {
    pub code: Dictionary<i32, i32>,               // dict<i32, Utf8>
    pub opt_code: Option<Dictionary<i8, String>>, // nullable dict<i8, Utf8>
}

#[test]
fn build_dictionary_arrays() {
    type B0 = <Row as ColAt<0>>::ColumnBuilder;
    type A0 = <Row as ColAt<0>>::ColumnArray;
    let mut b: B0 = arrow_array::builder::PrimitiveDictionaryBuilder::<Int32Type, Int32Type>::new();
    let _ = b.append(0);
    let _ = b.append(1);
    let _ = b.append(0);
    let a: A0 = b.finish();
    assert_eq!(a.len(), 3);
}

#[test]
fn dictionary_schema_and_types() {
    use arrow_schema::DataType;

    assert_eq!(<Row as Record>::LEN, 2);
    assert_eq!(<Row as ColAt<0>>::NAME, "code");
    assert_eq!(<Row as ColAt<1>>::NAME, "opt_code");
    assert_eq!(<Row as ColAt<0>>::NULLABLE, false);
    assert_eq!(<Row as ColAt<1>>::NULLABLE, true);

    // DataType
    let dt0 = <Row as ColAt<0>>::data_type();
    assert_eq!(
        dt0,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32))
    );
    let dt1 = <Row as ColAt<1>>::data_type();
    assert_eq!(
        dt1,
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8))
    );

    // Associated types: compile-time checks in an inner block to avoid clippy
    // items-after-statements
    {
        type A0 = <Row as ColAt<0>>::ColumnArray;
        type B0 = <Row as ColAt<0>>::ColumnBuilder;
        trait Same<T> {}
        impl<T> Same<T> for T {}
        #[allow(clippy::used_underscore_items)]
        fn _a0<T: Same<arrow_array::DictionaryArray<arrow_array::types::Int32Type>>>() {}
        #[allow(clippy::used_underscore_items)]
        fn _b0<T: Same<arrow_array::builder::PrimitiveDictionaryBuilder<Int32Type, Int32Type>>>() {}
        #[allow(clippy::used_underscore_items)]
        {
            _a0::<A0>();
            _b0::<B0>();
        }
    }
}
