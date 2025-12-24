//! Showcase: Struct arrays from nested `Record` types.

use arrow_array::{Array, cast::as_string_array};
use typed_arrow::prelude::*;

#[derive(Record)]
struct Address {
    city: String,
    zip: Option<i32>,
}

#[derive(Record)]
struct PersonS {
    id: i64,
    address: Option<Address>,
}

fn main() {
    use arrow_array::{
        builder::{PrimitiveBuilder, StringBuilder},
        types::Int32Type,
    };

    // DataType for nested struct field
    println!("address_dtype={:?}", <PersonS as ColAt<1>>::data_type());

    // Build a StructArray for Address using the ArrowBinding of Address
    let mut b: <Address as typed_arrow::bridge::ArrowBinding>::Builder =
        <Address as typed_arrow::bridge::ArrowBinding>::new_builder(2);

    // Row 0: { city: "NYC", zip: null }
    {
        let city_b: &mut StringBuilder = b.field_builder::<StringBuilder>(0).unwrap();
        city_b.append_value("NYC");
        let zip_b: &mut PrimitiveBuilder<Int32Type> =
            b.field_builder::<PrimitiveBuilder<Int32Type>>(1).unwrap();
        zip_b.append_null();
        b.append(true);
    }

    // Row 1: null struct — append nulls to children too
    {
        let city_b: &mut StringBuilder = b.field_builder::<StringBuilder>(0).unwrap();
        city_b.append_null();
        let zip_b: &mut PrimitiveBuilder<Int32Type> =
            b.field_builder::<PrimitiveBuilder<Int32Type>>(1).unwrap();
        zip_b.append_null();
        b.append(false);
    }

    let arr: arrow_array::StructArray = b.finish();
    println!("struct_len={}, is_null[1]={}", arr.len(), arr.is_null(1));
    let city = as_string_array(arr.column(0));
    println!("first_city={}", city.value(0));
}
