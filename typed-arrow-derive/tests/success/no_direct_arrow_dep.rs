#[derive(typed_arrow::Record)]
pub struct User {
    name: String,
    email: Option<String>,
    age: u8,
}


fn main() {}