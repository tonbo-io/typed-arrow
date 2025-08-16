//! List, LargeList, and FixedSizeList bindings.

use arrow_array::builder::*;
use arrow_schema::{DataType, Field};

use super::ArrowBinding;

/// Wrapper denoting an Arrow `ListArray` column with elements of `T`.
///
/// Notes:
/// - List-level nullability: wrap the column in `Option<List<T>>`.
/// - Item-level nullability: use `List<Option<T>>` when elements can be null.
pub struct List<T>(pub Vec<T>);

impl<T> ArrowBinding for List<T>
where
    T: ArrowBinding,
    <T as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = ListBuilder<<T as ArrowBinding>::Builder>;
    type Array = arrow_array::ListArray;
    fn data_type() -> DataType {
        DataType::List(Field::new("item", <T as ArrowBinding>::data_type(), false).into())
    }
    fn new_builder(_capacity: usize) -> Self::Builder {
        let child = <T as ArrowBinding>::new_builder(0);
        ListBuilder::new(child)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        for it in &v.0 {
            <T as ArrowBinding>::append_value(b.values(), it);
        }
        b.append(true);
    }
    fn append_null(b: &mut Self::Builder) {
        b.append(false);
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

/// Provide ArrowBinding for `List<Option<T>>` so users can express
/// item-nullability via `Option` in the type parameter.
impl<T> ArrowBinding for List<Option<T>>
where
    T: ArrowBinding,
    <T as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = ListBuilder<<T as ArrowBinding>::Builder>;
    type Array = arrow_array::ListArray;
    fn data_type() -> DataType {
        DataType::List(Field::new("item", <T as ArrowBinding>::data_type(), true).into())
    }
    fn new_builder(_capacity: usize) -> Self::Builder {
        let child = <T as ArrowBinding>::new_builder(0);
        ListBuilder::new(child)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        for it in &v.0 {
            match it {
                Some(inner) => <T as ArrowBinding>::append_value(b.values(), inner),
                None => <T as ArrowBinding>::append_null(b.values()),
            }
        }
        b.append(true);
    }
    fn append_null(b: &mut Self::Builder) {
        b.append(false);
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

/// Wrapper denoting an Arrow `FixedSizeListArray` column with `N` elements of `T`.
pub struct FixedSizeList<T, const N: usize>(pub [T; N]);

impl<T, const N: usize> ArrowBinding for FixedSizeList<T, N>
where
    T: ArrowBinding,
    <T as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = arrow_array::builder::FixedSizeListBuilder<<T as ArrowBinding>::Builder>;
    type Array = arrow_array::FixedSizeListArray;
    fn data_type() -> DataType {
        DataType::FixedSizeList(
            Field::new("item", <T as ArrowBinding>::data_type(), false).into(),
            N as i32,
        )
    }
    fn new_builder(capacity: usize) -> Self::Builder {
        let child = <T as ArrowBinding>::new_builder(0);
        arrow_array::builder::FixedSizeListBuilder::with_capacity(child, N as i32, capacity)
            .with_field(Field::new("item", <T as ArrowBinding>::data_type(), false))
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        for it in &v.0 {
            <T as ArrowBinding>::append_value(b.values(), it);
        }
        b.append(true);
    }
    fn append_null(b: &mut Self::Builder) {
        for _ in 0..N {
            <T as ArrowBinding>::append_null(b.values());
        }
        b.append(false);
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

/// Wrapper denoting a `FixedSizeListArray` with `N` elements where items are nullable.
pub struct FixedSizeListNullable<T, const N: usize>(pub [Option<T>; N]);

impl<T, const N: usize> ArrowBinding for FixedSizeListNullable<T, N>
where
    T: ArrowBinding,
    <T as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = arrow_array::builder::FixedSizeListBuilder<<T as ArrowBinding>::Builder>;
    type Array = arrow_array::FixedSizeListArray;
    fn data_type() -> DataType {
        DataType::FixedSizeList(
            Field::new("item", <T as ArrowBinding>::data_type(), true).into(),
            N as i32,
        )
    }
    fn new_builder(capacity: usize) -> Self::Builder {
        let child = <T as ArrowBinding>::new_builder(0);
        arrow_array::builder::FixedSizeListBuilder::with_capacity(child, N as i32, capacity)
            .with_field(Field::new("item", <T as ArrowBinding>::data_type(), true))
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        for it in &v.0 {
            match it {
                Some(inner) => <T as ArrowBinding>::append_value(b.values(), inner),
                None => <T as ArrowBinding>::append_null(b.values()),
            }
        }
        b.append(true);
    }
    fn append_null(b: &mut Self::Builder) {
        for _ in 0..N {
            <T as ArrowBinding>::append_null(b.values());
        }
        b.append(false);
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

/// Wrapper denoting an Arrow `LargeListArray` column with elements of `T`.
pub struct LargeList<T>(pub Vec<T>);

impl<T> ArrowBinding for LargeList<T>
where
    T: ArrowBinding,
    <T as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = LargeListBuilder<<T as ArrowBinding>::Builder>;
    type Array = arrow_array::LargeListArray;
    fn data_type() -> DataType {
        DataType::LargeList(Field::new("item", <T as ArrowBinding>::data_type(), false).into())
    }
    fn new_builder(_capacity: usize) -> Self::Builder {
        let child = <T as ArrowBinding>::new_builder(0);
        LargeListBuilder::new(child)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        for it in &v.0 {
            <T as ArrowBinding>::append_value(b.values(), it);
        }
        b.append(true);
    }
    fn append_null(b: &mut Self::Builder) {
        b.append(false);
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

/// Provide ArrowBinding for `LargeList<Option<T>>` so users can express
/// item-nullability via `Option` in the type parameter for LargeList.
impl<T> ArrowBinding for LargeList<Option<T>>
where
    T: ArrowBinding,
    <T as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = LargeListBuilder<<T as ArrowBinding>::Builder>;
    type Array = arrow_array::LargeListArray;
    fn data_type() -> DataType {
        DataType::LargeList(Field::new("item", <T as ArrowBinding>::data_type(), true).into())
    }
    fn new_builder(_capacity: usize) -> Self::Builder {
        let child = <T as ArrowBinding>::new_builder(0);
        LargeListBuilder::new(child)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        for it in &v.0 {
            match it {
                Some(inner) => <T as ArrowBinding>::append_value(b.values(), inner),
                None => <T as ArrowBinding>::append_null(b.values()),
            }
        }
        b.append(true);
    }
    fn append_null(b: &mut Self::Builder) {
        b.append(false);
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}
