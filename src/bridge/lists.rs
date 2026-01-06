//! `List`, `LargeList`, and `FixedSizeList` bindings.

use crate::arrow_array::builder::{ArrayBuilder, FixedSizeListBuilder, LargeListBuilder, ListBuilder};
use crate::arrow_schema::{DataType, Field};

use super::ArrowBinding;
#[cfg(feature = "views")]
use super::ArrowBindingView;

/// Wrapper denoting an Arrow `ListArray` column with elements of `T`.
///
/// Notes:
/// - List-level nullability: wrap the column in `Option<List<T>>`.
/// - Item-level nullability: use `List<Option<T>>` when elements can be null.
pub struct List<T>(Vec<T>);

impl<T: Clone> Clone for List<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for List<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("List").field(&self.0).finish()
    }
}

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
    type Array = crate::arrow_array::ListArray;
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
impl<'a, T, E> TryFrom<ListView<'a, T>> for List<T>
where
    T: ArrowBindingView + 'static,
    T::View<'a>: TryInto<T, Error = E>,
    E: Into<crate::schema::ViewAccessError>,
{
    type Error = crate::schema::ViewAccessError;

    fn try_from(view: ListView<'a, T>) -> Result<Self, Self::Error> {
        let mut values = Vec::with_capacity(view.len());
        for i in view.start..view.end {
            let v = T::get_view(view.values_array, i)?;
            values.push(v.try_into().map_err(|e| e.into())?);
        }
        Ok(List::new(values))
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
    T: ArrowBinding + ArrowBindingView + 'static,
{
    type Array = crate::arrow_array::ListArray;
    type View<'a> = ListView<'a, T>;

    fn get_view(
        array: &Self::Array,
        index: usize,
    ) -> Result<Self::View<'_>, crate::schema::ViewAccessError> {
        use crate::arrow_array::Array;
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
            .downcast_ref::<<T as ArrowBindingView>::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: T::data_type(),
                actual: array.values().data_type().clone(),
                field_name: None,
            })?;
        Ok(ListView::new(values_array, start, end))
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
    type Array = crate::arrow_array::ListArray;
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
impl<'a, T, E> TryFrom<ListViewNullable<'a, T>> for List<Option<T>>
where
    T: ArrowBindingView + 'static,
    T::View<'a>: TryInto<T, Error = E>,
    E: Into<crate::schema::ViewAccessError>,
{
    type Error = crate::schema::ViewAccessError;

    fn try_from(view: ListViewNullable<'a, T>) -> Result<Self, Self::Error> {
        let mut values = Vec::with_capacity(view.len());
        for i in view.start..view.end {
            let opt_view = <Option<T> as ArrowBindingView>::get_view(view.values_array, i)?;
            let opt_owned = match opt_view {
                Some(v) => Some(v.try_into().map_err(|e| e.into())?),
                None => None,
            };
            values.push(opt_owned);
        }
        Ok(List::new(values))
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
            let result = <Option<T> as ArrowBindingView>::get_view(self.values_array, self.start);
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
            Some(<Option<T> as ArrowBindingView>::get_view(
                self.values_array,
                self.end,
            ))
        } else {
            None
        }
    }
}

#[cfg(feature = "views")]
impl<T> ArrowBindingView for List<Option<T>>
where
    T: ArrowBinding + ArrowBindingView + 'static,
{
    type Array = crate::arrow_array::ListArray;
    type View<'a> = ListViewNullable<'a, T>;

    fn get_view(
        array: &Self::Array,
        index: usize,
    ) -> Result<Self::View<'_>, crate::schema::ViewAccessError> {
        use crate::arrow_array::Array;
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
            .downcast_ref::<<T as ArrowBindingView>::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: T::data_type(),
                actual: array.values().data_type().clone(),
                field_name: None,
            })?;
        Ok(ListViewNullable::new(values_array, start, end))
    }
}

/// Wrapper denoting an Arrow `FixedSizeListArray` column with `N` elements of `T`.
pub struct FixedSizeList<T, const N: usize>([T; N]);

impl<T: Clone, const N: usize> Clone for FixedSizeList<T, N> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: std::fmt::Debug, const N: usize> std::fmt::Debug for FixedSizeList<T, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("FixedSizeList").field(&self.0).finish()
    }
}

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
    type Builder = crate::arrow_array::builder::FixedSizeListBuilder<<T as ArrowBinding>::Builder>;
    type Array = crate::arrow_array::FixedSizeListArray;
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
impl<'a, T, E, const N: usize> TryFrom<FixedSizeListView<'a, T, N>> for FixedSizeList<T, N>
where
    T: ArrowBindingView + 'static,
    T::View<'a>: TryInto<T, Error = E>,
    E: Into<crate::schema::ViewAccessError>,
{
    type Error = crate::schema::ViewAccessError;

    fn try_from(view: FixedSizeListView<'a, T, N>) -> Result<Self, Self::Error> {
        let mut values = Vec::with_capacity(N);
        for i in 0..N {
            let v = T::get_view(view.values_array, view.start + i)?;
            values.push(v.try_into().map_err(|e| e.into())?);
        }
        // SAFETY: We pushed exactly N elements, so conversion to [T; N] cannot fail
        let arr: [T; N] = values
            .try_into()
            .unwrap_or_else(|_| unreachable!("Vec has exactly N elements"));
        Ok(FixedSizeList::new(arr))
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
    T: ArrowBinding + ArrowBindingView + 'static,
{
    type Array = crate::arrow_array::FixedSizeListArray;
    type View<'a> = FixedSizeListView<'a, T, N>;

    fn get_view(
        array: &Self::Array,
        index: usize,
    ) -> Result<Self::View<'_>, crate::schema::ViewAccessError> {
        use crate::arrow_array::Array;
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
            .downcast_ref::<<T as ArrowBindingView>::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: T::data_type(),
                actual: array.values().data_type().clone(),
                field_name: None,
            })?;
        Ok(FixedSizeListView::new(values_array, start))
    }
}

/// Wrapper denoting a `FixedSizeListArray` with `N` elements where items are nullable.
pub struct FixedSizeListNullable<T, const N: usize>([Option<T>; N]);

impl<T: Clone, const N: usize> Clone for FixedSizeListNullable<T, N> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: std::fmt::Debug, const N: usize> std::fmt::Debug for FixedSizeListNullable<T, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("FixedSizeListNullable")
            .field(&self.0)
            .finish()
    }
}

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
    type Builder = crate::arrow_array::builder::FixedSizeListBuilder<<T as ArrowBinding>::Builder>;
    type Array = crate::arrow_array::FixedSizeListArray;
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

    /// Get the value at a specific index.
    ///
    /// Returns Ok(Some(view)) if valid and non-null, Ok(None) if null,
    /// or Err if out of bounds or there's an error accessing the view.
    #[inline]
    pub fn get(&self, index: usize) -> Result<Option<T::View<'a>>, crate::schema::ViewAccessError> {
        if index >= N {
            return Err(crate::schema::ViewAccessError::OutOfBounds {
                index,
                len: N,
                field_name: None,
            });
        }
        let idx = self.start + index;
        <Option<T> as ArrowBindingView>::get_view(self.values_array, idx)
    }
}

#[cfg(feature = "views")]
impl<'a, T, E, const N: usize> TryFrom<FixedSizeListViewNullable<'a, T, N>>
    for FixedSizeListNullable<T, N>
where
    T: ArrowBindingView + 'static,
    T::View<'a>: TryInto<T, Error = E>,
    E: Into<crate::schema::ViewAccessError>,
{
    type Error = crate::schema::ViewAccessError;

    fn try_from(view: FixedSizeListViewNullable<'a, T, N>) -> Result<Self, Self::Error> {
        let mut values = Vec::with_capacity(N);
        for i in 0..N {
            let opt_view =
                <Option<T> as ArrowBindingView>::get_view(view.values_array, view.start + i)?;
            match opt_view {
                Some(v) => values.push(Some(v.try_into().map_err(|e| e.into())?)),
                None => values.push(None),
            }
        }
        // SAFETY: We pushed exactly N elements, so conversion to [Option<T>; N] cannot fail
        let arr: [Option<T>; N] = values
            .try_into()
            .unwrap_or_else(|_| unreachable!("Vec has exactly N elements"));
        Ok(FixedSizeListNullable::new(arr))
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
            let result = <Option<T> as ArrowBindingView>::get_view(self.values_array, idx);
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
    T: ArrowBinding + ArrowBindingView + 'static,
{
    type Array = crate::arrow_array::FixedSizeListArray;
    type View<'a> = FixedSizeListViewNullable<'a, T, N>;

    fn get_view(
        array: &Self::Array,
        index: usize,
    ) -> Result<Self::View<'_>, crate::schema::ViewAccessError> {
        use crate::arrow_array::Array;
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
            .downcast_ref::<<T as ArrowBindingView>::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: T::data_type(),
                actual: array.values().data_type().clone(),
                field_name: None,
            })?;
        Ok(FixedSizeListViewNullable::new(values_array, start))
    }
}

/// Wrapper denoting an Arrow `LargeListArray` column with elements of `T`.
pub struct LargeList<T>(Vec<T>);

impl<T: Clone> Clone for LargeList<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for LargeList<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("LargeList").field(&self.0).finish()
    }
}

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
    type Array = crate::arrow_array::LargeListArray;
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
impl<'a, T, E> TryFrom<LargeListView<'a, T>> for LargeList<T>
where
    T: ArrowBindingView + 'static,
    T::View<'a>: TryInto<T, Error = E>,
    E: Into<crate::schema::ViewAccessError>,
{
    type Error = crate::schema::ViewAccessError;

    fn try_from(view: LargeListView<'a, T>) -> Result<Self, Self::Error> {
        let mut values = Vec::with_capacity(view.len());
        for i in view.start..view.end {
            let v = T::get_view(view.values_array, i)?;
            values.push(v.try_into().map_err(|e| e.into())?);
        }
        Ok(LargeList::new(values))
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
    T: ArrowBinding + ArrowBindingView + 'static,
{
    type Array = crate::arrow_array::LargeListArray;
    type View<'a> = LargeListView<'a, T>;

    fn get_view(
        array: &Self::Array,
        index: usize,
    ) -> Result<Self::View<'_>, crate::schema::ViewAccessError> {
        use crate::arrow_array::Array;
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
            .downcast_ref::<<T as ArrowBindingView>::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: T::data_type(),
                actual: array.values().data_type().clone(),
                field_name: None,
            })?;
        Ok(LargeListView::new(values_array, start, end))
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
    type Array = crate::arrow_array::LargeListArray;
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
impl<'a, T, E> TryFrom<LargeListViewNullable<'a, T>> for LargeList<Option<T>>
where
    T: ArrowBindingView + 'static,
    T::View<'a>: TryInto<T, Error = E>,
    E: Into<crate::schema::ViewAccessError>,
{
    type Error = crate::schema::ViewAccessError;

    fn try_from(view: LargeListViewNullable<'a, T>) -> Result<Self, Self::Error> {
        let mut values = Vec::with_capacity(view.len());
        for i in view.start..view.end {
            let opt_view = <Option<T> as ArrowBindingView>::get_view(view.values_array, i)?;
            let opt_owned = match opt_view {
                Some(v) => Some(v.try_into().map_err(|e| e.into())?),
                None => None,
            };
            values.push(opt_owned);
        }
        Ok(LargeList::new(values))
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
            let result = <Option<T> as ArrowBindingView>::get_view(self.values_array, self.start);
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
            Some(<Option<T> as ArrowBindingView>::get_view(
                self.values_array,
                self.end,
            ))
        } else {
            None
        }
    }
}

#[cfg(feature = "views")]
impl<T> ArrowBindingView for LargeList<Option<T>>
where
    T: ArrowBinding + ArrowBindingView + 'static,
{
    type Array = crate::arrow_array::LargeListArray;
    type View<'a> = LargeListViewNullable<'a, T>;

    fn get_view(
        array: &Self::Array,
        index: usize,
    ) -> Result<Self::View<'_>, crate::schema::ViewAccessError> {
        use crate::arrow_array::Array;
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
            .downcast_ref::<<T as ArrowBindingView>::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: T::data_type(),
                actual: array.values().data_type().clone(),
                field_name: None,
            })?;
        Ok(LargeListViewNullable::new(values_array, start, end))
    }
}
