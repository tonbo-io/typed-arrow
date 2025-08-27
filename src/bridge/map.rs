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
        b.finish()
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
        b.finish()
    }
}
