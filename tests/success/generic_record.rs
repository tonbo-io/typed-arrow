use typed_arrow::prelude::*;

#[derive(Record)]
struct C<T> {
    data: T,
}

#[derive(Record)]
struct S<T> {
    d: u32,
    c: C<T>,
}

fn main() {
    let _ = C { data: 1u32 };
    let _ = S { d: 1, c: C { data: 2u32 } };
}
