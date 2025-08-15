use arrow_array::{builder::StructBuilder, cast::as_string_array, Array};
use typed_arrow::prelude::*;

#[derive(typed_arrow::Record)]
pub struct Address {
    pub city: String,
    pub zip: Option<i32>,
}

#[derive(typed_arrow::Record)]
pub struct PersonS {
    pub id: i64,
    pub address: Option<Address>,
}

#[test]
fn struct_datatype_and_associated_types() {
    use arrow_schema::{DataType, Field};
    // DataType for nested struct field
    let expected = DataType::Struct(
        vec![
            ::std::sync::Arc::new(Field::new("city", DataType::Utf8, false)),
            ::std::sync::Arc::new(Field::new("zip", DataType::Int32, true)),
        ]
        .into(),
    );
    assert_eq!(<PersonS as ColAt<1>>::data_type(), expected);

    // Associated types are StructBuilder/StructArray
    type AB = <PersonS as ColAt<1>>::ColumnBuilder;
    type AA = <PersonS as ColAt<1>>::ColumnArray;

    trait Same<T> {}
    impl<T> Same<T> for T {}
    fn _b<T: Same<StructBuilder>>() {}
    fn _a<T: Same<arrow_array::StructArray>>() {}
    _b::<AB>();
    _a::<AA>();
}

#[test]
fn build_struct_array_values() {
    use arrow_array::{
        builder::{PrimitiveBuilder, StringBuilder},
        types::Int32Type,
    };

    // Actually create via ArrowBinding for Address
    let mut b: <Address as typed_arrow::bridge::ArrowBinding>::Builder =
        <Address as typed_arrow::bridge::ArrowBinding>::new_builder(2);

    // Row 0: { city: "NYC", zip: null }
    {
        let city_b: &mut StringBuilder = b
            .field_builder::<StringBuilder>(0)
            .expect("child 0 is StringBuilder");
        city_b.append_value("NYC");
        let zip_b: &mut PrimitiveBuilder<Int32Type> = b
            .field_builder::<PrimitiveBuilder<Int32Type>>(1)
            .expect("child 1 is PrimitiveBuilder<Int32Type>");
        zip_b.append_null();
        b.append(true);
    }

    // Row 1: null struct — also append null to each child to keep lengths equal
    {
        let city_b: &mut StringBuilder = b
            .field_builder::<StringBuilder>(0)
            .expect("child 0 is StringBuilder");
        city_b.append_null();
        let zip_b: &mut PrimitiveBuilder<Int32Type> = b
            .field_builder::<PrimitiveBuilder<Int32Type>>(1)
            .expect("child 1 is PrimitiveBuilder<Int32Type>");
        zip_b.append_null();
        b.append(false);
    }

    let arr: arrow_array::StructArray = b.finish();
    assert_eq!(arr.len(), 2);
    assert!(arr.is_null(1));
    let city = as_string_array(arr.column(0));
    assert_eq!(city.value(0), "NYC");
}
