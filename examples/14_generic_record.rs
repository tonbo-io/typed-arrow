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
    let rows = vec![
        S {
            d: 1,
            c: C {
                data: "a".to_string(),
            },
        },
        S {
            d: 2,
            c: C {
                data: "b".to_string(),
            },
        },
    ];

    let mut b = <S<String> as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let batch = b.finish().into_record_batch();
    let _ = batch;
}
