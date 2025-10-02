//! `List`, `LargeList`, and `FixedSizeList` bindings.

use arrow_array::{
    builder::{ArrayBuilder, FixedSizeListBuilder, LargeListBuilder, ListBuilder},
    Array,
};
use arrow_schema::{DataType, Field};

use super::ArrowBinding;
#[cfg(feature = "views")]
use super::ArrowBindingView;

/// Wrapper denoting an Arrow `ListArray` column with elements of `T`.
///
/// Notes:
/// - List-level nullability: wrap the column in `Option<List<T>>`.
/// - Item-level nullability: use `List<Option<T>>` when elements can be null.
pub struct List<T>(Vec<T>);
impl<T> List<T> {
    /// Construct a new list from a vector of values.
    #[inline]
    #[must_use]
    pub fn new(values: Vec<T>) -> Self {
        Self(values)
    }
    /// Borrow the underlying values.
    #[inline]
    #[must_use]
    pub fn values(&self) -> &Vec<T> {
        &self.0
    }
    /// Consume and return the underlying vector of values.
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> Vec<T> {
        self.0
    }
}

impl<T> From<Vec<T>> for List<T> {
    /// Convert a vector into a `List<T>`.
    #[inline]
    fn from(values: Vec<T>) -> Self {
        Self::new(values)
    }
}

impl<T> std::iter::FromIterator<T> for List<T> {
    /// Collect an iterator into a `List<T>`.
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

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
        ListBuilder::new(child).with_field(Field::new(
            "item",
            <T as ArrowBinding>::data_type(),
            false,
        ))
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

/// Iterator over views of list elements.
#[cfg(feature = "views")]
pub struct ListView<'a, T>
where
    T: ArrowBindingView + 'static,
{
    values_array: &'a T::Array,
    start: usize,
    end: usize,
}

#[cfg(feature = "views")]
impl<'a, T> ListView<'a, T>
where
    T: ArrowBindingView + 'static,
{
    /// Create a new list view from a values array and offset range.
    #[inline]
    fn new(values_array: &'a T::Array, start: usize, end: usize) -> Self {
        Self {
            values_array,
            start,
            end,
        }
    }

    /// Get the length of the list.
    #[inline]
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Check if the list is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
}

#[cfg(feature = "views")]
impl<'a, T> Iterator for ListView<'a, T>
where
    T: ArrowBindingView + 'static,
{
    type Item = Result<T::View<'a>, crate::schema::ViewAccessError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start < self.end {
            let value = T::get_view(self.values_array, self.start);
            self.start += 1;
            Some(value)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.end - self.start;
        (remaining, Some(remaining))
    }
}

#[cfg(feature = "views")]
impl<'a, T> ExactSizeIterator for ListView<'a, T>
where
    T: ArrowBindingView + 'static,
{
    fn len(&self) -> usize {
        self.end - self.start
    }
}

#[cfg(feature = "views")]
impl<'a, T> DoubleEndedIterator for ListView<'a, T>
where
    T: ArrowBindingView + 'static,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.start < self.end {
            self.end -= 1;
            Some(T::get_view(self.values_array, self.end))
        } else {
            None
        }
    }
}

#[cfg(feature = "views")]
impl<T> ArrowBindingView for List<T>
where
    T: ArrowBindingView + 'static,
{
    type Array = arrow_array::ListArray;
    type View<'a> = ListView<'a, T>;

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
        if array.is_null(index) {
            return Err(crate::schema::ViewAccessError::UnexpectedNull {
                index,
                field_name: None,
            });
        }
        let offsets = array.value_offsets();
        let start = offsets[index] as usize;
        let end = offsets[index + 1] as usize;
        let values_array = array
            .values()
            .as_any()
            .downcast_ref::<T::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: std::any::type_name::<T::Array>().to_string(),
                actual: format!("{:?}", array.values().data_type()),
                field_name: None,
            })?;
        Ok(ListView::new(values_array, start, end))
    }

    fn is_null(array: &Self::Array, index: usize) -> bool {
        array.is_null(index)
    }
}

/// Provide `ArrowBinding` for `List<Option<T>>` so users can express
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
        ListBuilder::new(child).with_field(Field::new(
            "item",
            <T as ArrowBinding>::data_type(),
            true,
        ))
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

/// Iterator over views of list elements with nullable items.
#[cfg(feature = "views")]
pub struct ListViewNullable<'a, T>
where
    T: ArrowBindingView + 'static,
{
    values_array: &'a T::Array,
    start: usize,
    end: usize,
}

#[cfg(feature = "views")]
impl<'a, T> ListViewNullable<'a, T>
where
    T: ArrowBindingView + 'static,
{
    /// Create a new nullable list view from a values array and offset range.
    #[inline]
    fn new(values_array: &'a T::Array, start: usize, end: usize) -> Self {
        Self {
            values_array,
            start,
            end,
        }
    }

    /// Get the length of the list.
    #[inline]
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Check if the list is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
}

#[cfg(feature = "views")]
impl<'a, T> Iterator for ListViewNullable<'a, T>
where
    T: ArrowBindingView + 'static,
{
    type Item = Result<Option<T::View<'a>>, crate::schema::ViewAccessError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start < self.end {
            let result = if T::is_null(self.values_array, self.start) {
                Ok(None)
            } else {
                T::get_view(self.values_array, self.start).map(Some)
            };
            self.start += 1;
            Some(result)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.end - self.start;
        (remaining, Some(remaining))
    }
}

#[cfg(feature = "views")]
impl<'a, T> ExactSizeIterator for ListViewNullable<'a, T>
where
    T: ArrowBindingView + 'static,
{
    fn len(&self) -> usize {
        self.end - self.start
    }
}

#[cfg(feature = "views")]
impl<'a, T> DoubleEndedIterator for ListViewNullable<'a, T>
where
    T: ArrowBindingView + 'static,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.start < self.end {
            self.end -= 1;
            let result = if T::is_null(self.values_array, self.end) {
                Ok(None)
            } else {
                T::get_view(self.values_array, self.end).map(Some)
            };
            Some(result)
        } else {
            None
        }
    }
}

#[cfg(feature = "views")]
impl<T> ArrowBindingView for List<Option<T>>
where
    T: ArrowBindingView + 'static,
{
    type Array = arrow_array::ListArray;
    type View<'a> = ListViewNullable<'a, T>;

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
        if array.is_null(index) {
            return Err(crate::schema::ViewAccessError::UnexpectedNull {
                index,
                field_name: None,
            });
        }
        let offsets = array.value_offsets();
        let start = offsets[index] as usize;
        let end = offsets[index + 1] as usize;
        let values_array = array
            .values()
            .as_any()
            .downcast_ref::<T::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: std::any::type_name::<T::Array>().to_string(),
                actual: format!("{:?}", array.values().data_type()),
                field_name: None,
            })?;
        Ok(ListViewNullable::new(values_array, start, end))
    }

    fn is_null(array: &Self::Array, index: usize) -> bool {
        array.is_null(index)
    }
}

/// Wrapper denoting an Arrow `FixedSizeListArray` column with `N` elements of `T`.
pub struct FixedSizeList<T, const N: usize>([T; N]);
impl<T, const N: usize> FixedSizeList<T, N> {
    /// Construct a new fixed-size list from an array of length `N`.
    #[inline]
    #[must_use]
    pub fn new(values: [T; N]) -> Self {
        Self(values)
    }
    /// Borrow the underlying fixed-size array of values.
    #[inline]
    #[must_use]
    pub fn values(&self) -> &[T; N] {
        &self.0
    }
    /// Consume and return the underlying fixed-size array of values.
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> [T; N] {
        self.0
    }
}

impl<T, const N: usize> ArrowBinding for FixedSizeList<T, N>
where
    T: ArrowBinding,
    <T as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = arrow_array::builder::FixedSizeListBuilder<<T as ArrowBinding>::Builder>;
    type Array = arrow_array::FixedSizeListArray;
    fn data_type() -> DataType {
        let n_i32 = i32::try_from(N).expect("FixedSizeList N fits in i32");
        DataType::FixedSizeList(
            Field::new("item", <T as ArrowBinding>::data_type(), false).into(),
            n_i32,
        )
    }
    fn new_builder(capacity: usize) -> Self::Builder {
        let child = <T as ArrowBinding>::new_builder(0);
        let n_i32 = i32::try_from(N).expect("FixedSizeList N fits in i32");
        FixedSizeListBuilder::with_capacity(child, n_i32, capacity).with_field(Field::new(
            "item",
            <T as ArrowBinding>::data_type(),
            false,
        ))
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

/// Iterator over views of fixed-size list elements.
#[cfg(feature = "views")]
pub struct FixedSizeListView<'a, T, const N: usize>
where
    T: ArrowBindingView + 'static,
{
    values_array: &'a T::Array,
    start: usize,
    current: usize,
}

#[cfg(feature = "views")]
impl<'a, T, const N: usize> FixedSizeListView<'a, T, N>
where
    T: ArrowBindingView + 'static,
{
    /// Create a new fixed-size list view from a values array and start offset.
    #[inline]
    fn new(values_array: &'a T::Array, start: usize) -> Self {
        Self {
            values_array,
            start,
            current: 0,
        }
    }

    /// Get the length of the list (always N).
    #[inline]
    pub const fn len(&self) -> usize {
        N
    }

    /// Check if the list is empty (true only when N = 0).
    #[inline]
    pub const fn is_empty(&self) -> bool {
        N == 0
    }

    /// Get the value at a specific index.
    ///
    /// Returns Ok(Some(view)) if the index is valid, Ok(None) if out of bounds,
    /// or Err if there's an error accessing the view.
    #[inline]
    pub fn get(&self, index: usize) -> Result<Option<T::View<'a>>, crate::schema::ViewAccessError> {
        if index < N {
            T::get_view(self.values_array, self.start + index).map(Some)
        } else {
            Ok(None)
        }
    }
}

#[cfg(feature = "views")]
impl<'a, T, const N: usize> Iterator for FixedSizeListView<'a, T, N>
where
    T: ArrowBindingView + 'static,
{
    type Item = Result<T::View<'a>, crate::schema::ViewAccessError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current < N {
            let value = T::get_view(self.values_array, self.start + self.current);
            self.current += 1;
            Some(value)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = N.saturating_sub(self.current);
        (remaining, Some(remaining))
    }
}

#[cfg(feature = "views")]
impl<'a, T, const N: usize> ExactSizeIterator for FixedSizeListView<'a, T, N>
where
    T: ArrowBindingView + 'static,
{
    fn len(&self) -> usize {
        N.saturating_sub(self.current)
    }
}

#[cfg(feature = "views")]
impl<T, const N: usize> ArrowBindingView for FixedSizeList<T, N>
where
    T: ArrowBindingView + 'static,
{
    type Array = arrow_array::FixedSizeListArray;
    type View<'a> = FixedSizeListView<'a, T, N>;

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
        if array.is_null(index) {
            return Err(crate::schema::ViewAccessError::UnexpectedNull {
                index,
                field_name: None,
            });
        }
        let start = index * N;
        let values_array = array
            .values()
            .as_any()
            .downcast_ref::<T::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: std::any::type_name::<T::Array>().to_string(),
                actual: format!("{:?}", array.values().data_type()),
                field_name: None,
            })?;
        Ok(FixedSizeListView::new(values_array, start))
    }

    fn is_null(array: &Self::Array, index: usize) -> bool {
        array.is_null(index)
    }
}

/// Wrapper denoting a `FixedSizeListArray` with `N` elements where items are nullable.
pub struct FixedSizeListNullable<T, const N: usize>([Option<T>; N]);
impl<T, const N: usize> FixedSizeListNullable<T, N> {
    /// Construct a new fixed-size list with nullable items from an array of length `N`.
    #[inline]
    #[must_use]
    pub fn new(values: [Option<T>; N]) -> Self {
        Self(values)
    }
    /// Borrow the underlying fixed-size array of optional values.
    #[inline]
    #[must_use]
    pub fn values(&self) -> &[Option<T>; N] {
        &self.0
    }
    /// Consume and return the underlying fixed-size array of optional values.
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> [Option<T>; N] {
        self.0
    }
}

impl<T, const N: usize> ArrowBinding for FixedSizeListNullable<T, N>
where
    T: ArrowBinding,
    <T as ArrowBinding>::Builder: ArrayBuilder,
{
    type Builder = arrow_array::builder::FixedSizeListBuilder<<T as ArrowBinding>::Builder>;
    type Array = arrow_array::FixedSizeListArray;
    fn data_type() -> DataType {
        let n_i32 = i32::try_from(N).expect("FixedSizeList N fits in i32");
        DataType::FixedSizeList(
            Field::new("item", <T as ArrowBinding>::data_type(), true).into(),
            n_i32,
        )
    }
    fn new_builder(capacity: usize) -> Self::Builder {
        let child = <T as ArrowBinding>::new_builder(0);
        let n_i32 = i32::try_from(N).expect("FixedSizeList N fits in i32");
        FixedSizeListBuilder::with_capacity(child, n_i32, capacity).with_field(Field::new(
            "item",
            <T as ArrowBinding>::data_type(),
            true,
        ))
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

/// Iterator over views of fixed-size list elements with nullable items.
#[cfg(feature = "views")]
pub struct FixedSizeListViewNullable<'a, T, const N: usize>
where
    T: ArrowBindingView + 'static,
{
    values_array: &'a T::Array,
    start: usize,
    current: usize,
}

#[cfg(feature = "views")]
impl<'a, T, const N: usize> FixedSizeListViewNullable<'a, T, N>
where
    T: ArrowBindingView + 'static,
{
    /// Create a new nullable fixed-size list view from a values array and start offset.
    #[inline]
    fn new(values_array: &'a T::Array, start: usize) -> Self {
        Self {
            values_array,
            start,
            current: 0,
        }
    }

    /// Get the length of the list (always N).
    #[inline]
    pub const fn len(&self) -> usize {
        N
    }

    /// Check if the list is empty (true only when N = 0).
    #[inline]
    pub const fn is_empty(&self) -> bool {
        N == 0
    }

    /// Get the value at a specific index, returning None if null or out of bounds.
    ///
    /// Returns Ok(Some(Some(view))) if valid and non-null, Ok(Some(None)) if null,
    /// Ok(None) if out of bounds, or Err if there's an error accessing the view.
    #[inline]
    pub fn get(
        &self,
        index: usize,
    ) -> Result<Option<Option<T::View<'a>>>, crate::schema::ViewAccessError> {
        if index < N {
            let idx = self.start + index;
            if T::is_null(self.values_array, idx) {
                Ok(Some(None))
            } else {
                T::get_view(self.values_array, idx).map(|v| Some(Some(v)))
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(feature = "views")]
impl<'a, T, const N: usize> Iterator for FixedSizeListViewNullable<'a, T, N>
where
    T: ArrowBindingView + 'static,
{
    type Item = Result<Option<T::View<'a>>, crate::schema::ViewAccessError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current < N {
            let idx = self.start + self.current;
            let result = if T::is_null(self.values_array, idx) {
                Ok(None)
            } else {
                T::get_view(self.values_array, idx).map(Some)
            };
            self.current += 1;
            Some(result)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = N.saturating_sub(self.current);
        (remaining, Some(remaining))
    }
}

#[cfg(feature = "views")]
impl<'a, T, const N: usize> ExactSizeIterator for FixedSizeListViewNullable<'a, T, N>
where
    T: ArrowBindingView + 'static,
{
    fn len(&self) -> usize {
        N.saturating_sub(self.current)
    }
}

#[cfg(feature = "views")]
impl<T, const N: usize> ArrowBindingView for FixedSizeListNullable<T, N>
where
    T: ArrowBindingView + 'static,
{
    type Array = arrow_array::FixedSizeListArray;
    type View<'a> = FixedSizeListViewNullable<'a, T, N>;

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
        if array.is_null(index) {
            return Err(crate::schema::ViewAccessError::UnexpectedNull {
                index,
                field_name: None,
            });
        }
        let start = index * N;
        let values_array = array
            .values()
            .as_any()
            .downcast_ref::<T::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: std::any::type_name::<T::Array>().to_string(),
                actual: format!("{:?}", array.values().data_type()),
                field_name: None,
            })?;
        Ok(FixedSizeListViewNullable::new(values_array, start))
    }

    fn is_null(array: &Self::Array, index: usize) -> bool {
        array.is_null(index)
    }
}

/// Wrapper denoting an Arrow `LargeListArray` column with elements of `T`.
pub struct LargeList<T>(Vec<T>);
impl<T> LargeList<T> {
    /// Construct a new large-list from a vector of values.
    #[inline]
    #[must_use]
    pub fn new(values: Vec<T>) -> Self {
        Self(values)
    }
    /// Borrow the underlying values.
    #[inline]
    #[must_use]
    pub fn values(&self) -> &Vec<T> {
        &self.0
    }
    /// Consume and return the underlying vector of values.
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> Vec<T> {
        self.0
    }
}

impl<T> From<Vec<T>> for LargeList<T> {
    /// Convert a vector into a `LargeList<T>`.
    #[inline]
    fn from(values: Vec<T>) -> Self {
        Self::new(values)
    }
}

impl<T> std::iter::FromIterator<T> for LargeList<T> {
    /// Collect an iterator into a `LargeList<T>`.
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

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
        LargeListBuilder::new(child).with_field(Field::new(
            "item",
            <T as ArrowBinding>::data_type(),
            false,
        ))
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

/// Iterator over views of large list elements.
#[cfg(feature = "views")]
pub struct LargeListView<'a, T>
where
    T: ArrowBindingView + 'static,
{
    values_array: &'a T::Array,
    start: usize,
    end: usize,
}

#[cfg(feature = "views")]
impl<'a, T> LargeListView<'a, T>
where
    T: ArrowBindingView + 'static,
{
    /// Create a new large list view from a values array and offset range.
    #[inline]
    fn new(values_array: &'a T::Array, start: usize, end: usize) -> Self {
        Self {
            values_array,
            start,
            end,
        }
    }

    /// Get the length of the list.
    #[inline]
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Check if the list is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
}

#[cfg(feature = "views")]
impl<'a, T> Iterator for LargeListView<'a, T>
where
    T: ArrowBindingView + 'static,
{
    type Item = Result<T::View<'a>, crate::schema::ViewAccessError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start < self.end {
            let value = T::get_view(self.values_array, self.start);
            self.start += 1;
            Some(value)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.end - self.start;
        (remaining, Some(remaining))
    }
}

#[cfg(feature = "views")]
impl<'a, T> ExactSizeIterator for LargeListView<'a, T>
where
    T: ArrowBindingView + 'static,
{
    fn len(&self) -> usize {
        self.end - self.start
    }
}

#[cfg(feature = "views")]
impl<'a, T> DoubleEndedIterator for LargeListView<'a, T>
where
    T: ArrowBindingView + 'static,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.start < self.end {
            self.end -= 1;
            Some(T::get_view(self.values_array, self.end))
        } else {
            None
        }
    }
}

#[cfg(feature = "views")]
impl<T> ArrowBindingView for LargeList<T>
where
    T: ArrowBindingView + 'static,
{
    type Array = arrow_array::LargeListArray;
    type View<'a> = LargeListView<'a, T>;

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
        if array.is_null(index) {
            return Err(crate::schema::ViewAccessError::UnexpectedNull {
                index,
                field_name: None,
            });
        }
        let offsets = array.value_offsets();
        let start = offsets[index] as usize;
        let end = offsets[index + 1] as usize;
        let values_array = array
            .values()
            .as_any()
            .downcast_ref::<T::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: std::any::type_name::<T::Array>().to_string(),
                actual: format!("{:?}", array.values().data_type()),
                field_name: None,
            })?;
        Ok(LargeListView::new(values_array, start, end))
    }

    fn is_null(array: &Self::Array, index: usize) -> bool {
        array.is_null(index)
    }
}

/// Provide `ArrowBinding` for `LargeList<Option<T>>` so users can express
/// item-nullability via `Option` in the type parameter for `LargeList`.
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
        LargeListBuilder::new(child).with_field(Field::new(
            "item",
            <T as ArrowBinding>::data_type(),
            true,
        ))
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

/// Iterator over views of large list elements with nullable items.
#[cfg(feature = "views")]
pub struct LargeListViewNullable<'a, T>
where
    T: ArrowBindingView + 'static,
{
    values_array: &'a T::Array,
    start: usize,
    end: usize,
}

#[cfg(feature = "views")]
impl<'a, T> LargeListViewNullable<'a, T>
where
    T: ArrowBindingView + 'static,
{
    /// Create a new nullable large list view from a values array and offset range.
    #[inline]
    fn new(values_array: &'a T::Array, start: usize, end: usize) -> Self {
        Self {
            values_array,
            start,
            end,
        }
    }

    /// Get the length of the list.
    #[inline]
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Check if the list is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
}

#[cfg(feature = "views")]
impl<'a, T> Iterator for LargeListViewNullable<'a, T>
where
    T: ArrowBindingView + 'static,
{
    type Item = Result<Option<T::View<'a>>, crate::schema::ViewAccessError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start < self.end {
            let result = if T::is_null(self.values_array, self.start) {
                Ok(None)
            } else {
                T::get_view(self.values_array, self.start).map(Some)
            };
            self.start += 1;
            Some(result)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.end - self.start;
        (remaining, Some(remaining))
    }
}

#[cfg(feature = "views")]
impl<'a, T> ExactSizeIterator for LargeListViewNullable<'a, T>
where
    T: ArrowBindingView + 'static,
{
    fn len(&self) -> usize {
        self.end - self.start
    }
}

#[cfg(feature = "views")]
impl<'a, T> DoubleEndedIterator for LargeListViewNullable<'a, T>
where
    T: ArrowBindingView + 'static,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.start < self.end {
            self.end -= 1;
            let result = if T::is_null(self.values_array, self.end) {
                Ok(None)
            } else {
                T::get_view(self.values_array, self.end).map(Some)
            };
            Some(result)
        } else {
            None
        }
    }
}

#[cfg(feature = "views")]
impl<T> ArrowBindingView for LargeList<Option<T>>
where
    T: ArrowBindingView + 'static,
{
    type Array = arrow_array::LargeListArray;
    type View<'a> = LargeListViewNullable<'a, T>;

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
        if array.is_null(index) {
            return Err(crate::schema::ViewAccessError::UnexpectedNull {
                index,
                field_name: None,
            });
        }
        let offsets = array.value_offsets();
        let start = offsets[index] as usize;
        let end = offsets[index + 1] as usize;
        let values_array = array
            .values()
            .as_any()
            .downcast_ref::<T::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: std::any::type_name::<T::Array>().to_string(),
                actual: format!("{:?}", array.values().data_type()),
                field_name: None,
            })?;
        Ok(LargeListViewNullable::new(values_array, start, end))
    }

    fn is_null(array: &Self::Array, index: usize) -> bool {
        array.is_null(index)
    }
}
