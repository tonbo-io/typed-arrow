//! Blanket binding for `T: Record + StructMeta` to Arrow `StructArray`.

use arrow_array::builder::StructBuilder;
use arrow_schema::DataType;

use super::ArrowBinding;
use crate::schema::{Record, StructMeta};

// Any `T` implementing `Record + StructMeta` automatically binds to a typed
// Arrow `StructArray`, with a `StructBuilder` produced by `new_builder()`. The
// `DataType::Struct` is assembled from `StructMeta::child_fields()`.
impl<T> ArrowBinding for T
where
    T: Record + StructMeta,
{
    type Builder = StructBuilder;
    type Array = arrow_array::StructArray;
    fn data_type() -> DataType {
        use std::sync::Arc;
        let fields = <T as StructMeta>::child_fields()
            .into_iter()
            .map(Arc::new)
            .collect();
        DataType::Struct(fields)
    }
    fn new_builder(capacity: usize) -> Self::Builder {
        <T as StructMeta>::new_struct_builder(capacity)
    }
    fn append_value(b: &mut Self::Builder, _v: &Self) {
        // The typical pattern is: append child values, then mark presence here.
        b.append(true);
    }
    fn append_null(b: &mut Self::Builder) {
        b.append(false);
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}
