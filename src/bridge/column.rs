//! Column-level helpers: `data_type_of<R, I>()` and `ColumnBuilder<R, I>`.

use std::marker::PhantomData;

use arrow_schema::DataType;

use super::ArrowBinding;
use crate::schema::{ColAt, Record};

/// Returns the Arrow `DataType` for column `I` of record `R`.
pub fn data_type_of<R: Record + ColAt<I>, const I: usize>() -> DataType
where
    <R as ColAt<I>>::Rust: ArrowBinding,
{
    <<R as ColAt<I>>::Rust as ArrowBinding>::data_type()
}

/// A typed column builder for column `I` of record `R`.
pub struct ColumnBuilder<R: Record + ColAt<I>, const I: usize>
where
    <R as ColAt<I>>::Rust: ArrowBinding,
{
    inner: <<R as ColAt<I>>::Rust as ArrowBinding>::Builder,
    _pd: PhantomData<R>,
}

impl<R: Record + ColAt<I>, const I: usize> ColumnBuilder<R, I>
where
    <R as ColAt<I>>::Rust: ArrowBinding,
{
    /// Create a builder with `capacity`.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: <<R as ColAt<I>>::Rust as ArrowBinding>::new_builder(capacity),
            _pd: PhantomData,
        }
    }

    /// Append a value.
    pub fn append_value(&mut self, v: &<R as ColAt<I>>::Rust) {
        <<R as ColAt<I>>::Rust as ArrowBinding>::append_value(&mut self.inner, v)
    }

    /// Append an optional value; `None` appends a null.
    pub fn append_option(&mut self, v: Option<&<R as ColAt<I>>::Rust>) {
        match v {
            Some(x) => self.append_value(x),
            None => <<R as ColAt<I>>::Rust as ArrowBinding>::append_null(&mut self.inner),
        }
    }

    /// Finish and produce the typed Arrow array for this column.
    pub fn finish(self) -> <<R as ColAt<I>>::Rust as ArrowBinding>::Array {
        <<R as ColAt<I>>::Rust as ArrowBinding>::finish(self.inner)
    }
}
