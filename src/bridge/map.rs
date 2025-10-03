//! `Map` and `OrderedMap` bindings.

use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{builder::MapBuilder, MapArray};
use arrow_schema::{DataType, Field};

use super::ArrowBinding;

/// Wrapper denoting an Arrow `MapArray` column with entries `(K, V)`.
///
/// - Keys are non-nullable by Arrow spec.
/// - Values are non-nullable for `Map<K, V, SORTED>` and nullable for `Map<K, Option<V>, SORTED>`.
/// - Column-level nullability is expressed with `Option<Map<...>>`.
pub struct Map<K, V, const SORTED: bool = false>(Vec<(K, V)>);
impl<K, V, const SORTED: bool> Map<K, V, SORTED> {
    /// Construct a new map from a vector of `(key, value)` pairs.
    #[inline]
    #[must_use]
    pub fn new(entries: Vec<(K, V)>) -> Self {
        Self(entries)
    }
    /// Borrow the underlying `(key, value)` entries.
    #[inline]
    #[must_use]
    pub fn entries(&self) -> &Vec<(K, V)> {
        &self.0
    }
    /// Consume and return the underlying `(key, value)` entries.
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> Vec<(K, V)> {
        self.0
    }
}

impl<K, V, const SORTED: bool> From<Vec<(K, V)>> for Map<K, V, SORTED> {
    /// Convert a vector of `(key, value)` pairs into a `Map`.
    #[inline]
    fn from(entries: Vec<(K, V)>) -> Self {
        Self::new(entries)
    }
}

impl<K, V, const SORTED: bool> std::iter::FromIterator<(K, V)> for Map<K, V, SORTED> {
    /// Collect an iterator of `(key, value)` pairs into a `Map`.
    #[inline]
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

impl<K, V, const SORTED: bool> ArrowBinding for Map<K, V, SORTED>
where
    K: ArrowBinding,
    V: ArrowBinding,
    <K as ArrowBinding>::Builder: arrow_array::builder::ArrayBuilder,
    <V as ArrowBinding>::Builder: arrow_array::builder::ArrayBuilder,
{
    type Builder = MapBuilder<<K as ArrowBinding>::Builder, <V as ArrowBinding>::Builder>;
    type Array = MapArray;
    fn data_type() -> DataType {
        let key_f = Field::new("keys", <K as ArrowBinding>::data_type(), false);
        // MapBuilder names children `keys` and `values`; value field is nullable
        let val_f = Field::new("values", <V as ArrowBinding>::data_type(), true);
        let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
        DataType::Map(Field::new("entries", entries, false).into(), SORTED)
    }
    fn new_builder(_capacity: usize) -> Self::Builder {
        let kb = <K as ArrowBinding>::new_builder(0);
        let vb = <V as ArrowBinding>::new_builder(0);
        MapBuilder::new(None, kb, vb)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        for (k, val) in &v.0 {
            <K as ArrowBinding>::append_value(b.keys(), k);
            <V as ArrowBinding>::append_value(b.values(), val);
        }
        let _ = b.append(true);
    }
    fn append_null(b: &mut Self::Builder) {
        let _ = b.append(false);
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// Provide ArrowBinding for value-nullable variant via Option<V>
impl<K, V, const SORTED: bool> ArrowBinding for Map<K, Option<V>, SORTED>
where
    K: ArrowBinding,
    V: ArrowBinding,
    <K as ArrowBinding>::Builder: arrow_array::builder::ArrayBuilder,
    <V as ArrowBinding>::Builder: arrow_array::builder::ArrayBuilder,
{
    type Builder = MapBuilder<<K as ArrowBinding>::Builder, <V as ArrowBinding>::Builder>;
    type Array = MapArray;
    fn data_type() -> DataType {
        let key_f = Field::new("keys", <K as ArrowBinding>::data_type(), false);
        let val_f = Field::new("values", <V as ArrowBinding>::data_type(), true);
        let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
        DataType::Map(Field::new("entries", entries, false).into(), SORTED)
    }
    fn new_builder(_capacity: usize) -> Self::Builder {
        let kb = <K as ArrowBinding>::new_builder(0);
        let vb = <V as ArrowBinding>::new_builder(0);
        MapBuilder::new(None, kb, vb)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        for (k, val_opt) in &v.0 {
            <K as ArrowBinding>::append_value(b.keys(), k);
            match val_opt {
                Some(val) => <V as ArrowBinding>::append_value(b.values(), val),
                None => <V as ArrowBinding>::append_null(b.values()),
            }
        }
        let _ = b.append(true);
    }
    fn append_null(b: &mut Self::Builder) {
        let _ = b.append(false);
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

/// Sorted-keys `Map`: entries sourced from `BTreeMap<K, V>`, declaring `keys_sorted = true`.
/// Keys are non-nullable; the value field is nullable per `MapBuilder` semantics, but this
/// wrapper does not write null values.
pub struct OrderedMap<K, V>(BTreeMap<K, V>);
impl<K, V> OrderedMap<K, V> {
    /// Construct a new ordered-map from a `BTreeMap` (keys sorted).
    #[inline]
    #[must_use]
    pub fn new(map: BTreeMap<K, V>) -> Self {
        Self(map)
    }
    /// Borrow the underlying `BTreeMap`.
    #[inline]
    #[must_use]
    pub fn map(&self) -> &BTreeMap<K, V> {
        &self.0
    }
    /// Consume and return the underlying `BTreeMap`.
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> BTreeMap<K, V> {
        self.0
    }
}

impl<K, V> ArrowBinding for OrderedMap<K, V>
where
    K: ArrowBinding + Ord,
    V: ArrowBinding,
    <K as ArrowBinding>::Builder: arrow_array::builder::ArrayBuilder,
    <V as ArrowBinding>::Builder: arrow_array::builder::ArrayBuilder,
{
    type Builder = MapBuilder<<K as ArrowBinding>::Builder, <V as ArrowBinding>::Builder>;
    type Array = MapArray;
    fn data_type() -> DataType {
        let key_f = Field::new("keys", <K as ArrowBinding>::data_type(), false);
        let val_f = Field::new("values", <V as ArrowBinding>::data_type(), true);
        let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
        DataType::Map(Field::new("entries", entries, false).into(), true)
    }
    fn new_builder(_capacity: usize) -> Self::Builder {
        let kb = <K as ArrowBinding>::new_builder(0);
        let vb = <V as ArrowBinding>::new_builder(0);
        MapBuilder::new(None, kb, vb)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        for (k, val) in &v.0 {
            <K as ArrowBinding>::append_value(b.keys(), k);
            <V as ArrowBinding>::append_value(b.values(), val);
        }
        let _ = b.append(true);
    }
    fn append_null(b: &mut Self::Builder) {
        let _ = b.append(false);
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        use arrow_array::Array;
        use arrow_data::ArrayData;

        let map_array = b.finish();

        // MapBuilder always creates maps with sorted=false, so we need to manually update it
        // Get the existing field and create a new DataType with sorted=true
        let data = map_array.into_data();
        let (field, _sorted) = match data.data_type() {
            DataType::Map(f, _) => (f.clone(), true),
            _ => unreachable!(),
        };

        // Reconstruct the MapArray with sorted=true flag
        // All data is copied from the valid MapArray produced by MapBuilder
        let new_data = ArrayData::builder(DataType::Map(field, true))
            .len(data.len())
            .buffers(data.buffers().to_vec())
            .child_data(data.child_data().to_vec())
            .nulls(data.nulls().cloned())
            .build()
            .expect("MapArray reconstruction should succeed - all data copied from valid array");

        MapArray::from(new_data)
    }
}

// Provide ArrowBinding for OrderedMap<K, Option<V>> mirroring the non-wrapper variant
impl<K, V> ArrowBinding for OrderedMap<K, Option<V>>
where
    K: ArrowBinding + Ord,
    V: ArrowBinding,
    <K as ArrowBinding>::Builder: arrow_array::builder::ArrayBuilder,
    <V as ArrowBinding>::Builder: arrow_array::builder::ArrayBuilder,
{
    type Builder = MapBuilder<<K as ArrowBinding>::Builder, <V as ArrowBinding>::Builder>;
    type Array = MapArray;
    fn data_type() -> DataType {
        let key_f = Field::new("keys", <K as ArrowBinding>::data_type(), false);
        let val_f = Field::new("values", <V as ArrowBinding>::data_type(), true);
        let entries = DataType::Struct(vec![Arc::new(key_f), Arc::new(val_f)].into());
        DataType::Map(Field::new("entries", entries, false).into(), true)
    }
    fn new_builder(_capacity: usize) -> Self::Builder {
        let kb = <K as ArrowBinding>::new_builder(0);
        let vb = <V as ArrowBinding>::new_builder(0);
        MapBuilder::new(None, kb, vb)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        for (k, val_opt) in &v.0 {
            <K as ArrowBinding>::append_value(b.keys(), k);
            match val_opt {
                Some(val) => <V as ArrowBinding>::append_value(b.values(), val),
                None => <V as ArrowBinding>::append_null(b.values()),
            }
        }
        let _ = b.append(true);
    }
    fn append_null(b: &mut Self::Builder) {
        let _ = b.append(false);
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        use arrow_array::Array;
        use arrow_data::ArrayData;

        let map_array = b.finish();

        // MapBuilder always creates maps with sorted=false, so we need to manually update it
        // Get the existing field and create a new DataType with sorted=true
        let data = map_array.into_data();
        let (field, _sorted) = match data.data_type() {
            DataType::Map(f, _) => (f.clone(), true),
            _ => unreachable!(),
        };

        // Reconstruct the MapArray with sorted=true flag
        // All data is copied from the valid MapArray produced by MapBuilder
        let new_data = ArrayData::builder(DataType::Map(field, true))
            .len(data.len())
            .buffers(data.buffers().to_vec())
            .child_data(data.child_data().to_vec())
            .nulls(data.nulls().cloned())
            .build()
            .expect("MapArray reconstruction should succeed - all data copied from valid array");

        MapArray::from(new_data)
    }
}

/// Iterator over views of map entries (key-value pairs).
#[cfg(feature = "views")]
pub struct MapView<'a, K, V, const SORTED: bool = false>
where
    K: super::ArrowBindingView + 'static,
    V: super::ArrowBindingView + 'static,
{
    keys_array: &'a <K as super::ArrowBindingView>::Array,
    values_array: &'a <V as super::ArrowBindingView>::Array,
    start: usize,
    end: usize,
}

#[cfg(feature = "views")]
impl<'a, K, V, const SORTED: bool> MapView<'a, K, V, SORTED>
where
    K: super::ArrowBindingView + 'static,
    V: super::ArrowBindingView + 'static,
{
    fn new(
        keys_array: &'a <K as super::ArrowBindingView>::Array,
        values_array: &'a <V as super::ArrowBindingView>::Array,
        start: usize,
        end: usize,
    ) -> Self {
        Self {
            keys_array,
            values_array,
            start,
            end,
        }
    }

    /// Get the number of entries in the map.
    #[inline]
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Check if the map is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
}

#[cfg(feature = "views")]
impl<'a, K, V, const SORTED: bool> Iterator for MapView<'a, K, V, SORTED>
where
    K: super::ArrowBindingView + 'static,
    V: super::ArrowBindingView + 'static,
{
    type Item = Result<(K::View<'a>, V::View<'a>), crate::schema::ViewAccessError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start < self.end {
            let result = K::get_view(self.keys_array, self.start).and_then(|key| {
                V::get_view(self.values_array, self.start).map(|value| (key, value))
            });
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
impl<'a, K, V, const SORTED: bool> ExactSizeIterator for MapView<'a, K, V, SORTED>
where
    K: super::ArrowBindingView + 'static,
    V: super::ArrowBindingView + 'static,
{
    fn len(&self) -> usize {
        self.end - self.start
    }
}

#[cfg(feature = "views")]
impl<K, V, const SORTED: bool> super::ArrowBindingView for Map<K, V, SORTED>
where
    K: ArrowBinding + super::ArrowBindingView + 'static,
    V: ArrowBinding + super::ArrowBindingView + 'static,
{
    type Array = arrow_array::MapArray;
    type View<'a> = MapView<'a, K, V, SORTED>;

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

        // MapArray entries are stored as a StructArray with "keys" and "values" fields
        let entries = array.entries();
        let keys_array = entries
            .column(0)
            .as_any()
            .downcast_ref::<<K as super::ArrowBindingView>::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: K::data_type(),
                actual: entries.column(0).data_type().clone(),
                field_name: Some("keys"),
            })?;
        let values_array = entries
            .column(1)
            .as_any()
            .downcast_ref::<<V as super::ArrowBindingView>::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: V::data_type(),
                actual: entries.column(1).data_type().clone(),
                field_name: Some("values"),
            })?;

        Ok(MapView::new(keys_array, values_array, start, end))
    }
}

#[cfg(feature = "views")]
impl<K, V> super::ArrowBindingView for OrderedMap<K, V>
where
    K: ArrowBinding + Ord + super::ArrowBindingView + 'static,
    V: ArrowBinding + super::ArrowBindingView + 'static,
{
    type Array = arrow_array::MapArray;
    type View<'a> = MapView<'a, K, V, true>;

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

        let entries = array.entries();
        let keys_array = entries
            .column(0)
            .as_any()
            .downcast_ref::<<K as super::ArrowBindingView>::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: K::data_type(),
                actual: entries.column(0).data_type().clone(),
                field_name: Some("keys"),
            })?;
        let values_array = entries
            .column(1)
            .as_any()
            .downcast_ref::<<V as super::ArrowBindingView>::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: V::data_type(),
                actual: entries.column(1).data_type().clone(),
                field_name: Some("values"),
            })?;

        Ok(MapView::new(keys_array, values_array, start, end))
    }
}

/// Iterator over views of map entries with nullable values.
#[cfg(feature = "views")]
pub struct MapViewNullable<'a, K, V, const SORTED: bool = false>
where
    K: super::ArrowBindingView + 'static,
    V: super::ArrowBindingView + 'static,
{
    keys_array: &'a <K as super::ArrowBindingView>::Array,
    values_array: &'a <V as super::ArrowBindingView>::Array,
    start: usize,
    end: usize,
}

#[cfg(feature = "views")]
impl<'a, K, V, const SORTED: bool> MapViewNullable<'a, K, V, SORTED>
where
    K: super::ArrowBindingView + 'static,
    V: super::ArrowBindingView + 'static,
{
    fn new(
        keys_array: &'a <K as super::ArrowBindingView>::Array,
        values_array: &'a <V as super::ArrowBindingView>::Array,
        start: usize,
        end: usize,
    ) -> Self {
        Self {
            keys_array,
            values_array,
            start,
            end,
        }
    }

    /// Get the number of entries in the map.
    #[inline]
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Check if the map is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
}

#[cfg(feature = "views")]
impl<'a, K, V, const SORTED: bool> Iterator for MapViewNullable<'a, K, V, SORTED>
where
    K: super::ArrowBindingView + 'static,
    V: super::ArrowBindingView + 'static,
{
    type Item = Result<(K::View<'a>, Option<V::View<'a>>), crate::schema::ViewAccessError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start < self.end {
            let result = K::get_view(self.keys_array, self.start).and_then(|key| {
                use arrow_array::Array;
                let value = if self.values_array.is_null(self.start) {
                    Ok(None)
                } else {
                    V::get_view(self.values_array, self.start).map(Some)
                };
                value.map(|v| (key, v))
            });
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
impl<'a, K, V, const SORTED: bool> ExactSizeIterator for MapViewNullable<'a, K, V, SORTED>
where
    K: super::ArrowBindingView + 'static,
    V: super::ArrowBindingView + 'static,
{
    fn len(&self) -> usize {
        self.end - self.start
    }
}

#[cfg(feature = "views")]
impl<K, V, const SORTED: bool> super::ArrowBindingView for Map<K, Option<V>, SORTED>
where
    K: ArrowBinding + super::ArrowBindingView + 'static,
    V: ArrowBinding + super::ArrowBindingView + 'static,
{
    type Array = arrow_array::MapArray;
    type View<'a> = MapViewNullable<'a, K, V, SORTED>;

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

        let entries = array.entries();
        let keys_array = entries
            .column(0)
            .as_any()
            .downcast_ref::<<K as super::ArrowBindingView>::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: K::data_type(),
                actual: entries.column(0).data_type().clone(),
                field_name: Some("keys"),
            })?;
        let values_array = entries
            .column(1)
            .as_any()
            .downcast_ref::<<V as super::ArrowBindingView>::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: V::data_type(),
                actual: entries.column(1).data_type().clone(),
                field_name: Some("values"),
            })?;

        Ok(MapViewNullable::new(keys_array, values_array, start, end))
    }
}

#[cfg(feature = "views")]
impl<K, V> super::ArrowBindingView for OrderedMap<K, Option<V>>
where
    K: ArrowBinding + Ord + super::ArrowBindingView + 'static,
    V: ArrowBinding + super::ArrowBindingView + 'static,
{
    type Array = arrow_array::MapArray;
    type View<'a> = MapViewNullable<'a, K, V, true>;

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

        let entries = array.entries();
        let keys_array = entries
            .column(0)
            .as_any()
            .downcast_ref::<<K as super::ArrowBindingView>::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: K::data_type(),
                actual: entries.column(0).data_type().clone(),
                field_name: Some("keys"),
            })?;
        let values_array = entries
            .column(1)
            .as_any()
            .downcast_ref::<<V as super::ArrowBindingView>::Array>()
            .ok_or_else(|| crate::schema::ViewAccessError::TypeMismatch {
                expected: V::data_type(),
                actual: entries.column(1).data_type().clone(),
                field_name: Some("values"),
            })?;

        Ok(MapViewNullable::new(keys_array, values_array, start, end))
    }
}
