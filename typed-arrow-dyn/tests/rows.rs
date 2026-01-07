use std::sync::Arc;

use typed_arrow_dyn::{
    DynBuilders,
    arrow_array::{Array, cast},
    arrow_schema::{DataType, Field, Schema},
};

#[test]
fn append_none_row_sets_all_nulls() {
    // Schema: { a: Int64, b: Utf8 }
    let a = Field::new("a", DataType::Int64, true);
    let b = Field::new("b", DataType::Utf8, true);
    let schema = Arc::new(Schema::new(vec![a, b]));

    let mut builders = DynBuilders::new(Arc::clone(&schema), 0);
    builders.append_option_row(None).unwrap();
    let batch = builders.finish_into_batch();

    assert_eq!(batch.num_rows(), 1);
    let a =
        cast::as_primitive_array::<typed_arrow_dyn::arrow_array::types::Int64Type>(batch.column(0));
    let b = cast::as_string_array(batch.column(1));
    assert!(a.is_null(0));
    assert!(b.is_null(0));
}
