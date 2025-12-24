use typed_arrow::prelude::*;

#[derive(Record)]
pub struct User {
    name: String,
    email: Option<String>,
    age: u8,
}


fn main() {}