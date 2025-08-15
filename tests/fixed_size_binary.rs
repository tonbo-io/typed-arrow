use arrow_array::Array;
use arrow_schema::DataType;
use typed_arrow::bridge::ArrowBinding;

#[test]
fn fixed_size_binary_datatype_and_build() {
    const N: usize = 16;
    assert_eq!(
        <[u8; N] as ArrowBinding>::data_type(),
        DataType::FixedSizeBinary(N as i32)
    );

    let mut b = <[u8; N] as ArrowBinding>::new_builder(2);
    <[u8; N] as ArrowBinding>::append_value(&mut b, &[1u8; N]);
    <[u8; N] as ArrowBinding>::append_null(&mut b);
    let a = <[u8; N] as ArrowBinding>::finish(b);
    assert_eq!(a.len(), 2);
    assert_eq!(a.value_length(), N as i32);
}

#[derive(typed_arrow::Record)]
struct Row {
    tag4: [u8; 4],
    opt_tag8: Option<[u8; 8]>,
}

#[test]
fn fixed_size_binary_in_record() {
    use typed_arrow::schema::{ColAt, SchemaMeta};

    assert_eq!(<Row as ColAt<0>>::data_type(), DataType::FixedSizeBinary(4));
    assert_eq!(<Row as ColAt<1>>::data_type(), DataType::FixedSizeBinary(8));

    // Build column 0 directly via the typed builder
    type B0 = <Row as ColAt<0>>::ColumnBuilder;
    type A0 = <Row as ColAt<0>>::ColumnArray;
    let mut b0: B0 = <[u8; 4] as ArrowBinding>::new_builder(2);
    <[u8; 4] as ArrowBinding>::append_value(&mut b0, &[9, 9, 9, 9]);
    <[u8; 4] as ArrowBinding>::append_value(&mut b0, &[1, 2, 3, 4]);
    let a0: A0 = <[u8; 4] as ArrowBinding>::finish(b0);
    assert_eq!(a0.len(), 2);

    // Schema contains both fields
    let schema = <Row as SchemaMeta>::schema();
    assert_eq!(schema.fields().len(), 2);
}
