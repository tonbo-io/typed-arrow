//! Blanket binding for `T: Record + StructMeta` to Arrow `StructArray`.

use arrow_array::builder::StructBuilder;
use arrow_schema::DataType;

use super::ArrowBinding;
#[cfg(feature = "views")]
use super::ArrowBindingView;
#[cfg(feature = "views")]
use crate::schema::StructView;
use crate::schema::{AppendStruct, AppendStructRef, Record, StructMeta};

// Any `T` implementing `Record + StructMeta` automatically binds to a typed
// Arrow `StructArray`, with a `StructBuilder` produced by `new_builder()`. The
// `DataType::Struct` is assembled from `StructMeta::child_fields()`.
impl<T> ArrowBinding for T
where
    T: Record + StructMeta + AppendStruct + AppendStructRef,
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
    fn append_value(b: &mut Self::Builder, v: &Self) {
        // Append child values first, then mark presence
        <T as AppendStructRef>::append_borrowed_into(v, b);
        b.append(true);
    }
    fn append_null(b: &mut Self::Builder) {
        // Append nulls to children to keep lengths aligned, then mark null
        <T as AppendStruct>::append_null_into(b);
        b.append(false);
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// Blanket impl of ArrowBindingView for structs that implement StructView
// Note: StructView itself has where clauses that enforce ArrowBindingView on fields
#[cfg(feature = "views")]
impl<T> ArrowBindingView for T
where
    T: Record + StructView + 'static,
{
    type Array = arrow_array::StructArray;
    type View<'a>
        = <T as StructView>::View<'a>
    where
        T: 'a;

    fn get_view(
        array: &Self::Array,
        index: usize,
    ) -> Result<Self::View<'_>, crate::schema::ViewAccessError> {
        use arrow_array::Array;
        if index >= array.len() {
            return Err(crate::schema::ViewAccessError::OutOfBounds {
                index,
                len: array.len(),
                field_name: None,
            });
        }
        if <T as StructView>::is_null_at(array, index) {
            return Err(crate::schema::ViewAccessError::UnexpectedNull {
                index,
                field_name: None,
            });
        }
        <T as StructView>::view_at(array, index)
    }
}
