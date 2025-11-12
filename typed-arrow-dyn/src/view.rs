//! Dynamic zero-copy views over Arrow data.
//!
//! This module provides runtime equivalents to the typed `#[derive(Record)]`
//! view APIs. It allows callers to iterate rows of an `arrow_array::RecordBatch`
//! using a runtime schema (`DynSchema`) while retrieving borrowed values
//! (`DynCellRef<'_>`). The implementation mirrors the owned dynamic builders
//! (`DynCell`) so consumers can switch between owned and borrowed access paths.

use std::{marker::PhantomData, ptr::NonNull, slice, str, sync::Arc};

use arrow_array::{
    types::{
        Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType,
        DurationNanosecondType, DurationSecondType, Int16Type, Int32Type, Int64Type, Int8Type,
        Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
    Array, ArrayRef, BinaryArray, BooleanArray, DictionaryArray, FixedSizeBinaryArray,
    FixedSizeListArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray, PrimitiveArray,
    RecordBatch, StringArray, StructArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    UnionArray,
};
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, UnionFields, UnionMode};

use crate::{cell::DynCell, rows::DynRow, schema::DynSchema, DynViewError};

macro_rules! dyn_cell_primitive_methods {
    ($(($variant:ident, $ctor:ident, $getter:ident, $into:ident, $ty:ty, $arrow:literal, $desc:literal)),* $(,)?) => {
        $(
            #[doc = concat!("Constructs a dynamic cell wrapping an ", $arrow, " value.")]
            pub(crate) fn $ctor(value: $ty) -> Self {
                Self::from_raw(DynCellRaw::$variant(value))
            }

            #[doc = concat!("Returns the ", $desc, " value if this cell stores an ", $arrow, ".")]
            pub fn $getter(&self) -> Option<$ty> {
                match self.raw {
                    DynCellRaw::$variant(value) => Some(value),
                    _ => None,
                }
            }

            #[doc = concat!("Consumes the cell and returns the ", $desc, " value if it stores an ", $arrow, ".")]
            pub fn $into(self) -> Option<$ty> {
                match self.raw {
                    DynCellRaw::$variant(value) => Some(value),
                    _ => None,
                }
            }
        )*
    };
}

/// Borrowed representation of a single value backed by a raw pointer payload.
#[derive(Clone)]
pub struct DynCellRef<'a> {
    raw: DynCellRaw,
    _marker: PhantomData<&'a ()>,
}

impl<'a> DynCellRef<'a> {
    /// Create a new borrowed cell from its raw lifetime-erased payload.
    pub fn from_raw(raw: DynCellRaw) -> Self {
        Self {
            raw,
            _marker: PhantomData,
        }
    }

    /// Access the underlying raw representation.
    pub fn as_raw(&self) -> &DynCellRaw {
        &self.raw
    }

    /// Consume this reference, yielding the raw payload.
    pub fn into_raw(self) -> DynCellRaw {
        self.raw
    }

    /// Convert this borrowed cell into an owned [`DynCell`], cloning any backing data as needed.
    pub fn into_owned(self) -> Result<DynCell, DynViewError> {
        self.raw.into_owned()
    }

    /// Clone this borrowed cell into an owned [`DynCell`] without consuming the reference.
    pub fn to_owned(&self) -> Result<DynCell, DynViewError> {
        self.clone().into_owned()
    }

    /// Returns true if this cell represents Arrow `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self.raw, DynCellRaw::Null)
    }

    /// Constructs a dynamic cell representing Arrow `Null`.
    pub(crate) fn null() -> Self {
        Self::from_raw(DynCellRaw::Null)
    }

    dyn_cell_primitive_methods! {
        (Bool, bool, as_bool, into_bool, bool, "Arrow boolean", "boolean"),
        (I8, i8, as_i8, into_i8, i8, "Arrow Int8", "`i8`"),
        (I16, i16, as_i16, into_i16, i16, "Arrow Int16", "`i16`"),
        (I32, i32, as_i32, into_i32, i32, "Arrow Int32", "`i32`"),
        (I64, i64, as_i64, into_i64, i64, "Arrow Int64", "`i64`"),
        (U8, u8, as_u8, into_u8, u8, "Arrow UInt8", "`u8`"),
        (U16, u16, as_u16, into_u16, u16, "Arrow UInt16", "`u16`"),
        (U32, u32, as_u32, into_u32, u32, "Arrow UInt32", "`u32`"),
        (U64, u64, as_u64, into_u64, u64, "Arrow UInt64", "`u64`"),
        (F32, f32, as_f32, into_f32, f32, "Arrow Float32", "`f32`"),
        (F64, f64, as_f64, into_f64, f64, "Arrow Float64", "`f64`")
    }

    /// Constructs a dynamic cell wrapping an Arrow UTF-8 string slice.
    pub(crate) fn string(value: &'a str) -> Self {
        Self::from_raw(DynCellRaw::from_str(value))
    }

    /// Constructs a dynamic cell wrapping an Arrow binary slice.
    pub(crate) fn binary(value: &'a [u8]) -> Self {
        Self::from_raw(DynCellRaw::from_bin(value))
    }

    /// Constructs a dynamic cell wrapping a struct view.
    pub(crate) fn structure(view: DynStructView<'a>) -> Self {
        Self::from_raw(DynCellRaw::from_struct(view))
    }

    /// Constructs a dynamic cell wrapping a list view.
    pub(crate) fn list(view: DynListView<'a>) -> Self {
        Self::from_raw(DynCellRaw::from_list(view))
    }

    /// Constructs a dynamic cell wrapping a fixed-size list view.
    pub(crate) fn fixed_size_list(view: DynFixedSizeListView<'a>) -> Self {
        Self::from_raw(DynCellRaw::from_fixed_size_list(view))
    }

    /// Constructs a dynamic cell wrapping a map view.
    pub(crate) fn map(view: DynMapView<'a>) -> Self {
        Self::from_raw(DynCellRaw::from_map(view))
    }

    /// Constructs a dynamic cell wrapping a union view.
    pub(crate) fn union(view: DynUnionView<'a>) -> Self {
        Self::from_raw(DynCellRaw::from_union(view))
    }

    /// Returns the UTF-8 string slice if this cell stores Arrow `Utf8` or `LargeUtf8`.
    pub fn as_str(&self) -> Option<&'a str> {
        match &self.raw {
            DynCellRaw::Str { ptr, len } => unsafe {
                let bytes = slice::from_raw_parts(ptr.as_ptr() as *const u8, *len);
                Some(str::from_utf8_unchecked(bytes))
            },
            _ => None,
        }
    }

    /// Returns the binary slice if this cell stores Arrow `Binary`, `LargeBinary`, or
    /// `FixedSizeBinary`.
    pub fn as_bin(&self) -> Option<&'a [u8]> {
        match &self.raw {
            DynCellRaw::Bin { ptr, len } => unsafe {
                Some(slice::from_raw_parts(ptr.as_ptr() as *const u8, *len))
            },
            _ => None,
        }
    }

    /// Returns a struct view if this cell stores Arrow `Struct`.
    pub fn as_struct(&self) -> Option<DynStructView<'a>> {
        match &self.raw {
            DynCellRaw::Struct(raw) => unsafe { Some(raw.as_view()) },
            _ => None,
        }
    }

    /// Returns a list view if this cell stores Arrow `List` or `LargeList`.
    pub fn as_list(&self) -> Option<DynListView<'a>> {
        match &self.raw {
            DynCellRaw::List(raw) => unsafe { Some(raw.as_view()) },
            _ => None,
        }
    }

    /// Returns a fixed-size list view if this cell stores Arrow `FixedSizeList`.
    pub fn as_fixed_size_list(&self) -> Option<DynFixedSizeListView<'a>> {
        match &self.raw {
            DynCellRaw::FixedSizeList(raw) => unsafe { Some(raw.as_view()) },
            _ => None,
        }
    }

    /// Returns a map view if this cell stores Arrow `Map`.
    pub fn as_map(&self) -> Option<DynMapView<'a>> {
        match &self.raw {
            DynCellRaw::Map(raw) => unsafe { Some(raw.as_view()) },
            _ => None,
        }
    }

    /// Returns a union view if this cell stores Arrow `Union`.
    pub fn as_union(&self) -> Option<DynUnionView<'a>> {
        match &self.raw {
            DynCellRaw::Union(raw) => unsafe { Some(raw.as_view()) },
            _ => None,
        }
    }

    /// Consumes the cell and returns the UTF-8 string slice if it stores Arrow `Utf8` or
    /// `LargeUtf8`.
    pub fn into_str(self) -> Option<&'a str> {
        match self.raw {
            DynCellRaw::Str { ptr, len } => unsafe {
                let bytes = slice::from_raw_parts(ptr.as_ptr() as *const u8, len);
                Some(str::from_utf8_unchecked(bytes))
            },
            _ => None,
        }
    }

    /// Consumes the cell and returns the binary slice if it stores Arrow `Binary`, `LargeBinary`,
    /// or `FixedSizeBinary`.
    pub fn into_bin(self) -> Option<&'a [u8]> {
        match self.raw {
            DynCellRaw::Bin { ptr, len } => unsafe {
                Some(slice::from_raw_parts(ptr.as_ptr() as *const u8, len))
            },
            _ => None,
        }
    }

    /// Consumes the cell and returns a struct view if it stores Arrow `Struct`.
    pub fn into_struct(self) -> Option<DynStructView<'a>> {
        match self.raw {
            DynCellRaw::Struct(raw) => unsafe { Some(raw.into_view()) },
            _ => None,
        }
    }

    /// Consumes the cell and returns a list view if it stores Arrow `List` or `LargeList`.
    pub fn into_list(self) -> Option<DynListView<'a>> {
        match self.raw {
            DynCellRaw::List(raw) => unsafe { Some(raw.into_view()) },
            _ => None,
        }
    }

    /// Consumes the cell and returns a fixed-size list view if it stores Arrow `FixedSizeList`.
    pub fn into_fixed_size_list(self) -> Option<DynFixedSizeListView<'a>> {
        match self.raw {
            DynCellRaw::FixedSizeList(raw) => unsafe { Some(raw.into_view()) },
            _ => None,
        }
    }

    /// Consumes the cell and returns a map view if it stores Arrow `Map`.
    pub fn into_map(self) -> Option<DynMapView<'a>> {
        match self.raw {
            DynCellRaw::Map(raw) => unsafe { Some(raw.into_view()) },
            _ => None,
        }
    }

    /// Consumes the cell and returns a union view if it stores Arrow `Union`.
    pub fn into_union(self) -> Option<DynUnionView<'a>> {
        match self.raw {
            DynCellRaw::Union(raw) => unsafe { Some(raw.into_view()) },
            _ => None,
        }
    }
}

impl<'a> From<DynCellRaw> for DynCellRef<'a> {
    fn from(raw: DynCellRaw) -> Self {
        Self::from_raw(raw)
    }
}

impl<'a> std::fmt::Debug for DynCellRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_raw().fmt(f)
    }
}

/// Lifetime-erased counterpart to [`DynCellRef`].
///
/// This representation stores raw pointers in place of borrowed references. Callers must ensure the
/// backing Arrow arrays and batches remain alive while the raw cell (and any derived views) are in
/// use.
#[derive(Clone)]
pub enum DynCellRaw {
    /// Arrow `Null` value.
    Null,
    /// Boolean scalar.
    Bool(bool),
    /// 8-bit signed integer.
    I8(i8),
    /// 16-bit signed integer.
    I16(i16),
    /// 32-bit signed integer.
    I32(i32),
    /// 64-bit signed integer.
    I64(i64),
    /// 8-bit unsigned integer.
    U8(u8),
    /// 16-bit unsigned integer.
    U16(u16),
    /// 32-bit unsigned integer.
    U32(u32),
    /// 64-bit unsigned integer.
    U64(u64),
    /// 32-bit floating-point number.
    F32(f32),
    /// 64-bit floating-point number.
    F64(f64),
    /// Borrowed UTF-8 string slice.
    Str {
        /// Pointer to the first byte of the UTF-8 value.
        ptr: NonNull<u8>,
        /// Length in bytes of the UTF-8 value.
        len: usize,
    },
    /// Borrowed binary slice.
    Bin {
        /// Pointer to the first byte of the binary value.
        ptr: NonNull<u8>,
        /// Length in bytes of the binary value.
        len: usize,
    },
    /// Borrowed struct view.
    Struct(DynStructViewRaw),
    /// Borrowed variable-sized list view.
    List(DynListViewRaw),
    /// Borrowed fixed-size list view.
    FixedSizeList(DynFixedSizeListViewRaw),
    /// Borrowed map view.
    Map(DynMapViewRaw),
    /// Borrowed union view.
    Union(DynUnionViewRaw),
}

impl DynCellRaw {
    /// Convert a borrowed dynamic cell into its lifetime-erased form.
    pub fn from_ref(cell: DynCellRef<'_>) -> Self {
        cell.into_raw()
    }

    /// Convert this raw cell into an owned [`DynCell`] by cloning any referenced data.
    pub fn into_owned(self) -> Result<DynCell, DynViewError> {
        match self {
            DynCellRaw::Null => Ok(DynCell::Null),
            DynCellRaw::Bool(value) => Ok(DynCell::Bool(value)),
            DynCellRaw::I8(value) => Ok(DynCell::I8(value)),
            DynCellRaw::I16(value) => Ok(DynCell::I16(value)),
            DynCellRaw::I32(value) => Ok(DynCell::I32(value)),
            DynCellRaw::I64(value) => Ok(DynCell::I64(value)),
            DynCellRaw::U8(value) => Ok(DynCell::U8(value)),
            DynCellRaw::U16(value) => Ok(DynCell::U16(value)),
            DynCellRaw::U32(value) => Ok(DynCell::U32(value)),
            DynCellRaw::U64(value) => Ok(DynCell::U64(value)),
            DynCellRaw::F32(value) => Ok(DynCell::F32(value)),
            DynCellRaw::F64(value) => Ok(DynCell::F64(value)),
            DynCellRaw::Str { ptr, len } => {
                let bytes = unsafe { slice::from_raw_parts(ptr.as_ptr(), len) };
                let owned = unsafe { String::from_utf8_unchecked(bytes.to_vec()) };
                Ok(DynCell::Str(owned))
            }
            DynCellRaw::Bin { ptr, len } => {
                let bytes = unsafe { slice::from_raw_parts(ptr.as_ptr(), len) };
                Ok(DynCell::Bin(bytes.to_vec()))
            }
            DynCellRaw::Struct(raw) => {
                let values = Self::collect_struct(raw)?;
                Ok(DynCell::Struct(values))
            }
            DynCellRaw::List(raw) => {
                let items = Self::collect_list(raw)?;
                Ok(DynCell::List(items))
            }
            DynCellRaw::FixedSizeList(raw) => {
                let items = Self::collect_fixed_size_list(raw)?;
                Ok(DynCell::FixedSizeList(items))
            }
            DynCellRaw::Map(raw) => {
                let entries = Self::collect_map(raw)?;
                Ok(DynCell::Map(entries))
            }
            DynCellRaw::Union(raw) => Self::collect_union(raw),
        }
    }

    fn from_str(value: &str) -> Self {
        Self::Str {
            ptr: non_null_from_bytes(value.as_bytes()),
            len: value.len(),
        }
    }

    fn from_bin(value: &[u8]) -> Self {
        Self::Bin {
            ptr: non_null_from_bytes(value),
            len: value.len(),
        }
    }

    fn from_struct(view: DynStructView<'_>) -> Self {
        Self::Struct(DynStructViewRaw::from_view(view))
    }

    fn from_list(view: DynListView<'_>) -> Self {
        Self::List(DynListViewRaw::from_view(view))
    }

    fn from_fixed_size_list(view: DynFixedSizeListView<'_>) -> Self {
        Self::FixedSizeList(DynFixedSizeListViewRaw::from_view(view))
    }

    fn from_map(view: DynMapView<'_>) -> Self {
        Self::Map(DynMapViewRaw::from_view(view))
    }

    fn from_union(view: DynUnionView<'_>) -> Self {
        Self::Union(DynUnionViewRaw::from_view(view))
    }

    /// Reborrow this raw cell as a scoped [`DynCellRef`].
    ///
    /// # Safety
    /// The caller must guarantee that all underlying Arrow data structures outlive the returned
    /// reference.
    pub unsafe fn as_ref<'a>(&self) -> DynCellRef<'a> {
        DynCellRef::from_raw(self.clone())
    }

    fn cell_opt_into_owned(cell: Option<DynCellRef<'_>>) -> Result<Option<DynCell>, DynViewError> {
        cell.map(DynCellRef::into_owned).transpose()
    }

    fn collect_struct(raw: DynStructViewRaw) -> Result<Vec<Option<DynCell>>, DynViewError> {
        let view = unsafe { raw.into_view() };
        let mut values = Vec::with_capacity(view.len());
        for idx in 0..view.len() {
            let value = view.get(idx)?;
            values.push(Self::cell_opt_into_owned(value)?);
        }
        Ok(values)
    }

    fn collect_list(raw: DynListViewRaw) -> Result<Vec<Option<DynCell>>, DynViewError> {
        let view = unsafe { raw.into_view() };
        let mut items = Vec::with_capacity(view.len());
        for idx in 0..view.len() {
            let item = view.get(idx)?;
            items.push(Self::cell_opt_into_owned(item)?);
        }
        Ok(items)
    }

    fn collect_fixed_size_list(
        raw: DynFixedSizeListViewRaw,
    ) -> Result<Vec<Option<DynCell>>, DynViewError> {
        let view = unsafe { raw.into_view() };
        let mut items = Vec::with_capacity(view.len());
        for idx in 0..view.len() {
            let item = view.get(idx)?;
            items.push(Self::cell_opt_into_owned(item)?);
        }
        Ok(items)
    }

    fn collect_map(raw: DynMapViewRaw) -> Result<Vec<(DynCell, Option<DynCell>)>, DynViewError> {
        let view = unsafe { raw.into_view() };
        let mut entries = Vec::with_capacity(view.len());
        for idx in 0..view.len() {
            let (key, value) = view.get(idx)?;
            let owned_key = key.into_owned()?;
            let owned_value = Self::cell_opt_into_owned(value)?;
            entries.push((owned_key, owned_value));
        }
        Ok(entries)
    }

    fn collect_union(raw: DynUnionViewRaw) -> Result<DynCell, DynViewError> {
        let view = unsafe { raw.into_view() };
        let type_id = view.type_id();
        let payload = view
            .value()?
            .map(|cell| cell.into_owned().map(Box::new))
            .transpose()?;
        Ok(DynCell::Union {
            type_id,
            value: payload,
        })
    }
}

impl std::fmt::Debug for DynCellRaw {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe { self.as_ref() }.fmt(f)
    }
}

/// Lifetime-erased struct view backing a [`DynCellRaw::Struct`] cell.
#[derive(Clone)]
pub struct DynStructViewRaw {
    array: NonNull<StructArray>,
    fields: Fields,
    row: usize,
    base_path: Path,
}

impl DynStructViewRaw {
    fn from_view(view: DynStructView<'_>) -> Self {
        Self {
            array: NonNull::from(view.array),
            fields: view.fields.clone(),
            row: view.row,
            base_path: view.base_path.clone(),
        }
    }

    /// Reborrow the struct view with an explicit lifetime.
    ///
    /// # Safety
    /// The caller must ensure the underlying `StructArray` outlives `'a`.
    pub unsafe fn as_view<'a>(&self) -> DynStructView<'a> {
        DynStructView {
            array: self.array.as_ref(),
            fields: self.fields.clone(),
            row: self.row,
            base_path: self.base_path.clone(),
        }
    }

    /// Consume the raw view, yielding a scoped [`DynStructView`].
    ///
    /// # Safety
    /// The caller must ensure the underlying `StructArray` outlives `'a`.
    pub unsafe fn into_view<'a>(self) -> DynStructView<'a> {
        let array = self.array.as_ref();
        DynStructView {
            array,
            fields: self.fields,
            row: self.row,
            base_path: self.base_path,
        }
    }
}

/// Lifetime-erased list view backing a [`DynCellRaw::List`] cell.
#[derive(Clone)]
pub struct DynListViewRaw {
    values: ArrayRef,
    item_field: FieldRef,
    start: usize,
    end: usize,
    base_path: Path,
}

impl DynListViewRaw {
    fn from_view(view: DynListView<'_>) -> Self {
        Self {
            values: view.values.clone(),
            item_field: Arc::clone(&view.item_field),
            start: view.start,
            end: view.end,
            base_path: view.base_path.clone(),
        }
    }

    /// Reborrow the list view with an explicit lifetime.
    ///
    /// # Safety
    /// The caller must ensure the arrays referenced by this view outlive `'a`.
    pub unsafe fn as_view<'a>(&self) -> DynListView<'a> {
        DynListView {
            values: self.values.clone(),
            item_field: Arc::clone(&self.item_field),
            start: self.start,
            end: self.end,
            base_path: self.base_path.clone(),
            _marker: PhantomData,
        }
    }

    /// Consume the raw list view, yielding a scoped [`DynListView`].
    ///
    /// # Safety
    /// The caller must ensure the arrays referenced by this view outlive `'a`.
    pub unsafe fn into_view<'a>(self) -> DynListView<'a> {
        DynListView {
            values: self.values,
            item_field: self.item_field,
            start: self.start,
            end: self.end,
            base_path: self.base_path,
            _marker: PhantomData,
        }
    }
}

/// Lifetime-erased fixed-size list view backing a [`DynCellRaw::FixedSizeList`] cell.

#[derive(Clone)]
pub struct DynFixedSizeListViewRaw {
    values: ArrayRef,
    item_field: FieldRef,
    start: usize,
    len: usize,
    base_path: Path,
}

impl DynFixedSizeListViewRaw {
    fn from_view(view: DynFixedSizeListView<'_>) -> Self {
        Self {
            values: view.values.clone(),
            item_field: Arc::clone(&view.item_field),
            start: view.start,
            len: view.len,
            base_path: view.base_path.clone(),
        }
    }

    /// Reborrow the fixed-size list view with an explicit lifetime.
    ///
    /// # Safety
    /// The caller must ensure the arrays referenced by this view outlive `'a`.
    pub unsafe fn as_view<'a>(&self) -> DynFixedSizeListView<'a> {
        DynFixedSizeListView {
            values: self.values.clone(),
            item_field: Arc::clone(&self.item_field),
            start: self.start,
            len: self.len,
            base_path: self.base_path.clone(),
            _marker: PhantomData,
        }
    }

    /// Consume the raw fixed-size list view, yielding a scoped [`DynFixedSizeListView`].
    ///
    /// # Safety
    /// The caller must ensure the arrays referenced by this view outlive `'a`.
    pub unsafe fn into_view<'a>(self) -> DynFixedSizeListView<'a> {
        DynFixedSizeListView {
            values: self.values,
            item_field: self.item_field,
            start: self.start,
            len: self.len,
            base_path: self.base_path,
            _marker: PhantomData,
        }
    }
}

/// Lifetime-erased map view backing a [`DynCellRaw::Map`] cell.
#[derive(Clone)]
pub struct DynMapViewRaw {
    array: NonNull<MapArray>,
    start: usize,
    end: usize,
    base_path: Path,
}

impl DynMapViewRaw {
    fn from_view(view: DynMapView<'_>) -> Self {
        Self {
            array: NonNull::from(view.array),
            start: view.start,
            end: view.end,
            base_path: view.base_path.clone(),
        }
    }

    /// Reborrow the map view with an explicit lifetime.
    ///
    /// # Safety
    /// The caller must ensure the underlying `MapArray` outlives `'a`.
    pub unsafe fn as_view<'a>(&self) -> DynMapView<'a> {
        DynMapView {
            array: self.array.as_ref(),
            start: self.start,
            end: self.end,
            base_path: self.base_path.clone(),
        }
    }

    /// Consume the raw map view, yielding a scoped [`DynMapView`].
    ///
    /// # Safety
    /// The caller must ensure the underlying `MapArray` outlives `'a`.
    pub unsafe fn into_view<'a>(self) -> DynMapView<'a> {
        DynMapView {
            array: self.array.as_ref(),
            start: self.start,
            end: self.end,
            base_path: self.base_path,
        }
    }
}

/// Lifetime-erased union view backing a [`DynCellRaw::Union`] cell.
#[derive(Clone)]
pub struct DynUnionViewRaw {
    array: NonNull<UnionArray>,
    fields: UnionFields,
    mode: UnionMode,
    row: usize,
    base_path: Path,
}

impl DynUnionViewRaw {
    fn from_view(view: DynUnionView<'_>) -> Self {
        Self {
            array: NonNull::from(view.array),
            fields: view.fields.clone(),
            mode: view.mode,
            row: view.row,
            base_path: view.base_path.clone(),
        }
    }

    /// Reborrow the union view with an explicit lifetime.
    ///
    /// # Safety
    /// The caller must ensure the underlying `UnionArray` outlives `'a`.
    pub unsafe fn as_view<'a>(&self) -> DynUnionView<'a> {
        DynUnionView {
            array: self.array.as_ref(),
            fields: self.fields.clone(),
            mode: self.mode,
            row: self.row,
            base_path: self.base_path.clone(),
        }
    }

    /// Consume the raw union view, yielding a scoped [`DynUnionView`].
    ///
    /// # Safety
    /// The caller must ensure the underlying `UnionArray` outlives `'a`.
    pub unsafe fn into_view<'a>(self) -> DynUnionView<'a> {
        DynUnionView {
            array: self.array.as_ref(),
            fields: self.fields,
            mode: self.mode,
            row: self.row,
            base_path: self.base_path,
        }
    }
}

fn non_null_from_bytes(bytes: &[u8]) -> NonNull<u8> {
    let ptr = bytes.as_ptr() as *mut u8;
    // `NonNull::dangling` is acceptable for zero-length slices/strings.
    NonNull::new(ptr).unwrap_or_else(NonNull::dangling)
}

/// Iterator over borrowed dynamic rows.
pub struct DynRowViews<'a> {
    batch: &'a RecordBatch,
    fields: Fields,
    mapping: Option<Arc<[usize]>>,
    row: usize,
    len: usize,
}

impl<'a> DynRowViews<'a> {
    /// Create a dynamic view iterator from a record batch after validating schema compatibility.
    pub fn new(batch: &'a RecordBatch, schema: &'a Schema) -> Result<Self, DynViewError> {
        validate_schema_matches(batch, schema)?;
        Ok(Self {
            batch,
            fields: schema.fields().clone(),
            mapping: None,
            row: 0,
            len: batch.num_rows(),
        })
    }

    /// Borrow the underlying schema fields.
    #[inline]
    pub fn fields(&self) -> &Fields {
        &self.fields
    }

    /// Apply a top-level projection to this iterator, yielding views that expose only the mapped
    /// columns.
    ///
    /// The projection is lazy: rows are fetched on demand from the underlying iterator, and only
    /// the referenced columns are materialized.
    ///
    /// # Errors
    /// Returns `DynViewError::Invalid` if the projection was derived from a schema with a different
    /// width than this iterator.
    pub fn project(self, projection: DynProjection) -> Result<Self, DynViewError> {
        let DynRowViews {
            batch,
            fields,
            mapping,
            row,
            len,
        } = self;

        let base_view = DynRowView {
            batch,
            fields,
            mapping,
            row,
        };

        let projected_view = base_view.project(&projection)?;
        let DynRowView {
            batch,
            fields,
            mapping,
            row,
        } = projected_view;

        Ok(Self {
            batch,
            fields,
            mapping,
            row,
            len,
        })
    }
}

impl<'a> Iterator for DynRowViews<'a> {
    type Item = Result<DynRowView<'a>, DynViewError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.len {
            return None;
        }
        let view = DynRowView {
            batch: self.batch,
            fields: self.fields.clone(),
            mapping: self.mapping.clone(),
            row: self.row,
        };
        self.row += 1;
        Some(Ok(view))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len.saturating_sub(self.row);
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for DynRowViews<'a> {}

/// Borrowed dynamic row backed by an `arrow_array::RecordBatch`.
pub struct DynRowView<'a> {
    batch: &'a RecordBatch,
    fields: Fields,
    mapping: Option<Arc<[usize]>>,
    row: usize,
}

impl<'a> DynRowView<'a> {
    /// Number of columns in this row.
    #[inline]
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns true when the row has zero columns.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrow the schema fields.
    #[inline]
    pub fn fields(&self) -> &Fields {
        &self.fields
    }

    /// Retrieve the cell at `column` as a borrowed [`DynCellRef`].
    pub fn get(&self, column: usize) -> Result<Option<DynCellRef<'_>>, DynViewError> {
        let width = self.fields.len();
        if column >= width {
            return Err(DynViewError::ColumnOutOfBounds { column, width });
        }
        if self.row >= self.batch.num_rows() {
            return Err(DynViewError::RowOutOfBounds {
                row: self.row,
                len: self.batch.num_rows(),
            });
        }
        let source_index = match &self.mapping {
            Some(mapping) => mapping[column],
            None => column,
        };
        if source_index >= self.batch.num_columns() {
            return Err(DynViewError::Invalid {
                column,
                path: self
                    .fields
                    .get(column)
                    .map(|f| f.name().to_string())
                    .unwrap_or_else(|| "<unknown>".to_string()),
                message: format!(
                    "projection index {source_index} exceeds batch width {}",
                    self.batch.num_columns()
                ),
            });
        }
        let field = self.fields.get(column).expect("index validated");
        let array = self.batch.column(source_index);
        let path = Path::new(column, field.name());
        view_cell(&path, field.as_ref(), array.as_ref(), self.row)
    }

    /// Retrieve a column by name, returning `None` if the field does not exist.
    pub fn get_by_name(&self, name: &str) -> Option<Result<Option<DynCellRef<'_>>, DynViewError>> {
        self.fields
            .iter()
            .position(|f| f.name() == name)
            .map(move |idx| self.get(idx))
    }

    /// Clone this row into an owned [`DynRow`], allocating owned dynamic cells for each column.
    pub fn to_owned(&self) -> Result<DynRow, DynViewError> {
        let width = self.len();
        let mut cells = Vec::with_capacity(width);
        for idx in 0..width {
            let value = self.get(idx)?;
            let owned = match value {
                None => None,
                Some(cell) => Some(cell.into_owned()?),
            };
            cells.push(owned);
        }
        Ok(DynRow(cells))
    }

    /// Consume this row view and capture its values as lifetime-erased [`DynCellRaw`] entries.
    pub fn into_raw(self) -> Result<DynRowRaw, DynViewError> {
        let fields = self.fields.clone();
        let mut cells = Vec::with_capacity(fields.len());
        for idx in 0..fields.len() {
            let value = self.get(idx)?;
            cells.push(value.map(DynCellRef::into_raw));
        }
        Ok(DynRowRaw { fields, cells })
    }

    /// Apply a projection to this view, yielding a new view that references only the mapped
    /// columns.
    ///
    /// The projection is lazy and reuses the underlying batch buffers.
    ///
    /// # Errors
    /// Returns `DynViewError::Invalid` if the projection was derived from a schema whose width
    /// differs from the underlying batch.
    pub fn project(self, projection: &DynProjection) -> Result<DynRowView<'a>, DynViewError> {
        if projection.source_width() != self.batch.num_columns() {
            return Err(DynViewError::Invalid {
                column: 0,
                path: "<projection>".to_string(),
                message: format!(
                    "projection source width {} does not match batch width {}",
                    projection.source_width(),
                    self.batch.num_columns()
                ),
            });
        }
        Ok(DynRowView {
            batch: self.batch,
            fields: projection.fields().clone(),
            mapping: Some(projection.mapping_arc()),
            row: self.row,
        })
    }

    /// Access the underlying row index.
    pub fn row_index(&self) -> usize {
        self.row
    }
}

/// Lifetime-erased dynamic row produced by [`DynRowView::into_raw`].
#[derive(Clone, Debug)]
pub struct DynRowRaw {
    fields: Fields,
    cells: Vec<Option<DynCellRaw>>,
}

fn validate_row_width(fields: &Fields, cells_len: usize) -> Result<(), DynViewError> {
    if fields.len() != cells_len {
        let column = fields.len().min(cells_len);
        return Err(DynViewError::Invalid {
            column,
            path: "<row>".to_string(),
            message: format!(
                "field count {} does not match cell count {}",
                fields.len(),
                cells_len
            ),
        });
    }
    Ok(())
}

fn validate_field_shape(
    column: usize,
    field_name: &str,
    expected_type: &DataType,
    expected_nullable: bool,
    actual: &Field,
) -> Result<(), DynViewError> {
    if actual.data_type() != expected_type {
        return Err(DynViewError::SchemaMismatch {
            column,
            field: field_name.to_string(),
            expected: expected_type.clone(),
            actual: actual.data_type().clone(),
        });
    }
    if actual.is_nullable() != expected_nullable {
        return Err(DynViewError::Invalid {
            column,
            path: field_name.to_string(),
            message: format!(
                "nullability mismatch: expected {}, got {}",
                expected_nullable,
                actual.is_nullable()
            ),
        });
    }
    Ok(())
}

impl DynRowRaw {
    /// Construct a raw row from explicit schema fields and raw cells.
    ///
    /// # Errors
    /// Returns [`DynViewError::Invalid`] when the number of cells does not match
    /// the number of fields in the provided schema slice.
    pub fn try_new(fields: Fields, cells: Vec<Option<DynCellRaw>>) -> Result<Self, DynViewError> {
        validate_row_width(&fields, cells.len())?;
        Ok(Self { fields, cells })
    }

    /// Construct a raw row from non-null cells.
    ///
    /// # Errors
    /// Returns [`DynViewError::Invalid`] when the number of cells does not match the schema.
    pub fn from_cells(fields: Fields, cells: Vec<DynCellRaw>) -> Result<Self, DynViewError> {
        let wrapped = cells.into_iter().map(Some).collect();
        Self::try_new(fields, wrapped)
    }

    /// Number of columns carried by this raw row.
    #[inline]
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Returns true when the row has zero columns.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// Borrow the schema fields associated with this row.
    #[inline]
    pub fn fields(&self) -> &Fields {
        &self.fields
    }

    /// Borrow the raw cell payloads.
    #[inline]
    pub fn cells(&self) -> &[Option<DynCellRaw>] {
        &self.cells
    }

    /// Consume the raw row, yielding the underlying raw cells.
    #[inline]
    pub fn into_cells(self) -> Vec<Option<DynCellRaw>> {
        self.cells
    }

    /// Convert this raw row into an owned [`DynRow`], cloning nested data as needed.
    pub fn into_owned(self) -> Result<DynRow, DynViewError> {
        let mut cells = Vec::with_capacity(self.cells.len());
        for cell in self.cells {
            let owned = match cell {
                None => None,
                Some(raw) => Some(raw.into_owned()?),
            };
            cells.push(owned);
        }
        Ok(DynRow(cells))
    }

    /// Clone this raw row into an owned [`DynRow`] without consuming the raw payloads.
    pub fn to_owned(&self) -> Result<DynRow, DynViewError> {
        self.clone().into_owned()
    }
}

/// Owned dynamic row that retains schema metadata alongside owned cell payloads.
#[derive(Clone, Debug)]
pub struct DynRowOwned {
    fields: Fields,
    cells: Vec<Option<DynCell>>,
}

impl DynRowOwned {
    /// Construct an owned row from explicit schema fields and owned cells.
    ///
    /// # Errors
    /// Returns [`DynViewError::Invalid`] when the number of cells does not match the schema.
    pub fn try_new(fields: Fields, cells: Vec<Option<DynCell>>) -> Result<Self, DynViewError> {
        validate_row_width(&fields, cells.len())?;
        Ok(Self { fields, cells })
    }

    /// Construct an owned row from a [`DynRow`].
    pub fn from_dyn_row(fields: Fields, row: DynRow) -> Result<Self, DynViewError> {
        Self::try_new(fields, row.0)
    }

    /// Clone the lifetime-erased raw row into an owned representation.
    pub fn from_raw(raw: &DynRowRaw) -> Result<Self, DynViewError> {
        let owned = raw.to_owned()?;
        Self::from_dyn_row(raw.fields().clone(), owned)
    }

    /// Borrow the schema fields associated with this row.
    #[inline]
    pub fn fields(&self) -> &Fields {
        &self.fields
    }

    /// Borrow the owned cell payloads.
    #[inline]
    pub fn cells(&self) -> &[Option<DynCell>] {
        &self.cells
    }

    /// Number of columns carried by this row.
    #[inline]
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Returns true when the row has zero columns.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// Borrow this owned row as a lifetime-erased raw row referencing the owned buffers.
    pub fn as_raw(&self) -> Result<DynRowRaw, DynViewError> {
        let mut raw_cells = Vec::with_capacity(self.cells.len());
        for (idx, cell) in self.cells.iter().enumerate() {
            match cell {
                None => raw_cells.push(None),
                Some(value) => {
                    let raw =
                        owned_cell_to_raw(value).map_err(|message| DynViewError::Invalid {
                            column: idx,
                            path: self
                                .fields
                                .get(idx)
                                .map(|f| f.name().to_string())
                                .unwrap_or_else(|| format!("col{idx}")),
                            message,
                        })?;
                    raw_cells.push(Some(raw));
                }
            }
        }
        DynRowRaw::try_new(self.fields.clone(), raw_cells)
    }

    /// Consume this owned row, yielding the underlying dynamic row cells.
    pub fn into_dyn_row(self) -> DynRow {
        DynRow(self.cells)
    }

    /// Clone this owned row into a [`DynRow`].
    pub fn to_dyn_row(&self) -> DynRow {
        DynRow(self.cells.clone())
    }

    /// Decompose the owned row into its schema fields and owned cells.
    pub fn into_parts(self) -> (Fields, Vec<Option<DynCell>>) {
        (self.fields, self.cells)
    }
}

fn owned_cell_to_raw(cell: &DynCell) -> Result<DynCellRaw, String> {
    use DynCell::*;
    match cell {
        Null => Ok(DynCellRaw::Null),
        Bool(v) => Ok(DynCellRaw::Bool(*v)),
        I8(v) => Ok(DynCellRaw::I8(*v)),
        I16(v) => Ok(DynCellRaw::I16(*v)),
        I32(v) => Ok(DynCellRaw::I32(*v)),
        I64(v) => Ok(DynCellRaw::I64(*v)),
        U8(v) => Ok(DynCellRaw::U8(*v)),
        U16(v) => Ok(DynCellRaw::U16(*v)),
        U32(v) => Ok(DynCellRaw::U32(*v)),
        U64(v) => Ok(DynCellRaw::U64(*v)),
        F32(v) => Ok(DynCellRaw::F32(*v)),
        F64(v) => Ok(DynCellRaw::F64(*v)),
        Str(value) => Ok(DynCellRaw::from_str(value)),
        Bin(value) => Ok(DynCellRaw::from_bin(value)),
        Struct(_) => Err("struct key component not supported".to_string()),
        List(_) => Err("list key component not supported".to_string()),
        FixedSizeList(_) => Err("fixed-size list key component not supported".to_string()),
        Map(_) => Err("map key component not supported".to_string()),
        Union { .. } => Err("union key component not supported".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Fields};

    use super::{DynCell, DynCellRaw, DynRowOwned};

    #[test]
    fn dyn_row_owned_round_trip_utf8() {
        let fields = Fields::from(vec![Arc::new(Field::new("id", DataType::Utf8, false))]);
        let row = DynRowOwned::try_new(fields.clone(), vec![Some(DynCell::Str("hello".into()))])
            .expect("owned key");
        let raw = row.as_raw().expect("raw");
        assert!(matches!(raw.cells()[0], Some(DynCellRaw::Str { .. })));

        let rebuilt = DynRowOwned::from_raw(&raw).expect("from raw");
        assert_eq!(rebuilt.len(), 1);
        assert!(matches!(rebuilt.cells()[0], Some(DynCell::Str(_))));
    }

    #[test]
    fn dyn_row_owned_rejects_nested() {
        let fields = Fields::from(vec![Arc::new(Field::new("map", DataType::Binary, false))]);
        let row = DynRowOwned::try_new(fields, vec![Some(DynCell::Map(Vec::new()))]).unwrap();
        assert!(row.as_raw().is_err());
    }
}

/// Column projection descriptor used to derive projected dynamic views.
#[derive(Clone)]
pub struct DynProjection(Arc<DynProjectionData>);

#[derive(Debug)]
struct DynProjectionData {
    source_width: usize,
    mapping: Arc<[usize]>,
    fields: Fields,
}

impl DynProjection {
    fn new_internal(source_width: usize, mapping: Vec<usize>, fields: Fields) -> Self {
        Self(Arc::new(DynProjectionData {
            source_width,
            mapping: Arc::from(mapping),
            fields,
        }))
    }

    /// Create a projection from explicit column indices.
    ///
    /// # Errors
    /// Returns `DynViewError::ColumnOutOfBounds` if any index exceeds the schema width.
    pub fn from_indices<I>(schema: &Schema, indices: I) -> Result<Self, DynViewError>
    where
        I: IntoIterator<Item = usize>,
    {
        let schema_fields = schema.fields();
        let width = schema_fields.len();
        let mut mapping = Vec::new();
        let mut projected = Vec::new();
        for idx in indices.into_iter() {
            if idx >= width {
                return Err(DynViewError::ColumnOutOfBounds { column: idx, width });
            }
            mapping.push(idx);
            projected.push(schema_fields[idx].clone());
        }
        Ok(Self::new_internal(width, mapping, Fields::from(projected)))
    }

    /// Create a projection by matching a projected schema against the source schema.
    ///
    /// Fields are matched by name; data type and nullability must also align.
    ///
    /// # Errors
    /// Returns `DynViewError` when a projected field is missing from the source schema or when its
    /// metadata disagrees.
    pub fn from_schema(source: &Schema, projection: &Schema) -> Result<Self, DynViewError> {
        let source_fields = source.fields();
        let width = source_fields.len();
        let mut mapping = Vec::with_capacity(projection.fields().len());
        let mut projected = Vec::with_capacity(projection.fields().len());
        for (pos, field) in projection.fields().iter().enumerate() {
            let source_idx = match source.index_of(field.name()) {
                Ok(idx) => idx,
                Err(_) => {
                    return Err(DynViewError::Invalid {
                        column: pos,
                        path: field.name().to_string(),
                        message: "field not found in source schema".to_string(),
                    })
                }
            };
            let source_field = source_fields[source_idx].as_ref();
            validate_field_shape(
                pos,
                field.name(),
                field.data_type(),
                field.is_nullable(),
                source_field,
            )?;
            mapping.push(source_idx);
            projected.push(field.clone());
        }
        Ok(Self::new_internal(width, mapping, Fields::from(projected)))
    }

    /// Width of the source schema this projection was derived from.
    fn source_width(&self) -> usize {
        self.0.source_width
    }

    fn mapping_arc(&self) -> Arc<[usize]> {
        Arc::clone(&self.0.mapping)
    }

    /// Projected schema fields in order.
    pub fn fields(&self) -> &Fields {
        &self.0.fields
    }

    /// Number of projected columns.
    pub fn len(&self) -> usize {
        self.0.mapping.len()
    }

    /// Returns `true` when the projection contains zero columns.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Project a single row from `batch` using this projection, returning a borrowed view.
    ///
    /// # Errors
    /// Returns `DynViewError` when schema validation fails, the row index is out of bounds,
    /// or the projection width mismatches the batch.
    pub fn project_row_view<'a>(
        &self,
        schema: &'a DynSchema,
        batch: &'a RecordBatch,
        row: usize,
    ) -> Result<DynRowView<'a>, DynViewError> {
        let view = schema.view_at(batch, row)?;
        view.project(self)
    }

    /// Project a single row from `batch` and capture it as lifetime-erased raw cells.
    pub fn project_row_raw(
        &self,
        schema: &DynSchema,
        batch: &RecordBatch,
        row: usize,
    ) -> Result<DynRowRaw, DynViewError> {
        let view = self.project_row_view(schema, batch, row)?;
        view.into_raw()
    }
}

/// Validate that the batch schema matches the runtime schema exactly.
fn validate_schema_matches(batch: &RecordBatch, schema: &Schema) -> Result<(), DynViewError> {
    let batch_schema = batch.schema();
    let batch_fields = batch_schema.fields();
    let expected = schema.fields();
    if batch_fields.len() != expected.len() {
        return Err(DynViewError::Invalid {
            column: expected.len().min(batch_fields.len()),
            path: "<schema>".to_string(),
            message: format!(
                "column count mismatch: schema has {}, batch has {}",
                expected.len(),
                batch_fields.len()
            ),
        });
    }

    for (idx, (expected_field, actual_field)) in
        expected.iter().zip(batch_fields.iter()).enumerate()
    {
        if expected_field.name() != actual_field.name() {
            return Err(DynViewError::Invalid {
                column: idx,
                path: expected_field.name().to_string(),
                message: format!(
                    "field name mismatch: expected '{}', got '{}'",
                    expected_field.name(),
                    actual_field.name()
                ),
            });
        }
        validate_field_shape(
            idx,
            expected_field.name(),
            expected_field.data_type(),
            expected_field.is_nullable(),
            actual_field.as_ref(),
        )?;
    }

    Ok(())
}

/// Helper for building dot/index annotated paths through nested structures.
#[derive(Debug, Clone)]
struct Path {
    column: usize,
    path: String,
}

impl Path {
    fn new(column: usize, name: &str) -> Self {
        Self {
            column,
            path: name.to_string(),
        }
    }

    fn push_field(&self, name: &str) -> Self {
        let mut next = self.path.clone();
        if !next.is_empty() {
            next.push('.');
        }
        next.push_str(name);
        Self {
            column: self.column,
            path: next,
        }
    }

    fn push_index(&self, index: usize) -> Self {
        let mut next = self.path.clone();
        next.push('[');
        next.push_str(&index.to_string());
        next.push(']');
        Self {
            column: self.column,
            path: next,
        }
    }

    fn push_key(&self) -> Self {
        let mut next = self.path.clone();
        next.push_str(".<key>");
        Self {
            column: self.column,
            path: next,
        }
    }

    fn push_value(&self) -> Self {
        let mut next = self.path.clone();
        next.push_str(".<value>");
        Self {
            column: self.column,
            path: next,
        }
    }

    fn push_variant(&self, name: &str, tag: i8) -> Self {
        let mut next = self.path.clone();
        if !next.is_empty() {
            next.push('.');
        }
        next.push_str(name);
        next.push_str(&format!("#{}", tag));
        Self {
            column: self.column,
            path: next,
        }
    }
}

fn view_cell<'a>(
    path: &Path,
    field: &Field,
    array: &'a dyn Array,
    index: usize,
) -> Result<Option<DynCellRef<'a>>, DynViewError> {
    if index >= array.len() {
        return Err(DynViewError::RowOutOfBounds {
            row: index,
            len: array.len(),
        });
    }
    if array.is_null(index) {
        return Ok(None);
    }
    Ok(Some(view_non_null(path, field, array, index)?))
}

fn view_non_null<'a>(
    path: &Path,
    field: &Field,
    array: &'a dyn Array,
    index: usize,
) -> Result<DynCellRef<'a>, DynViewError> {
    let dt = field.data_type();
    match dt {
        DataType::Null => Ok(DynCellRef::null()),
        DataType::Boolean => {
            let arr = as_bool(array, path)?;
            Ok(DynCellRef::bool(arr.value(index)))
        }
        DataType::Int8 => {
            let arr = as_primitive::<Int8Type>(array, path, dt)?;
            Ok(DynCellRef::i8(arr.value(index)))
        }
        DataType::Int16 => {
            let arr = as_primitive::<Int16Type>(array, path, dt)?;
            Ok(DynCellRef::i16(arr.value(index)))
        }
        DataType::Int32 => {
            let arr = as_primitive::<Int32Type>(array, path, dt)?;
            Ok(DynCellRef::i32(arr.value(index)))
        }
        DataType::Date32 => {
            let arr = as_primitive::<Date32Type>(array, path, dt)?;
            Ok(DynCellRef::i32(arr.value(index)))
        }
        DataType::Time32(unit) => match unit {
            arrow_schema::TimeUnit::Second => {
                let arr = as_primitive::<Time32SecondType>(array, path, dt)?;
                Ok(DynCellRef::i32(arr.value(index)))
            }
            arrow_schema::TimeUnit::Millisecond => {
                let arr = as_primitive::<Time32MillisecondType>(array, path, dt)?;
                Ok(DynCellRef::i32(arr.value(index)))
            }
            other => Err(DynViewError::Invalid {
                column: path.column,
                path: path.path.clone(),
                message: format!("unsupported Time32 unit {other:?}"),
            }),
        },
        DataType::Int64 => {
            let arr = as_primitive::<Int64Type>(array, path, dt)?;
            Ok(DynCellRef::i64(arr.value(index)))
        }
        DataType::Date64 => {
            let arr = as_primitive::<Date64Type>(array, path, dt)?;
            Ok(DynCellRef::i64(arr.value(index)))
        }
        DataType::Timestamp(unit, _) => match unit {
            arrow_schema::TimeUnit::Second => {
                let arr = as_primitive::<TimestampSecondType>(array, path, dt)?;
                Ok(DynCellRef::i64(arr.value(index)))
            }
            arrow_schema::TimeUnit::Millisecond => {
                let arr = as_primitive::<TimestampMillisecondType>(array, path, dt)?;
                Ok(DynCellRef::i64(arr.value(index)))
            }
            arrow_schema::TimeUnit::Microsecond => {
                let arr = as_primitive::<TimestampMicrosecondType>(array, path, dt)?;
                Ok(DynCellRef::i64(arr.value(index)))
            }
            arrow_schema::TimeUnit::Nanosecond => {
                let arr = as_primitive::<TimestampNanosecondType>(array, path, dt)?;
                Ok(DynCellRef::i64(arr.value(index)))
            }
        },
        DataType::Time64(unit) => match unit {
            arrow_schema::TimeUnit::Microsecond => {
                let arr = as_primitive::<Time64MicrosecondType>(array, path, dt)?;
                Ok(DynCellRef::i64(arr.value(index)))
            }
            arrow_schema::TimeUnit::Nanosecond => {
                let arr = as_primitive::<Time64NanosecondType>(array, path, dt)?;
                Ok(DynCellRef::i64(arr.value(index)))
            }
            other => Err(DynViewError::Invalid {
                column: path.column,
                path: path.path.clone(),
                message: format!("unsupported Time64 unit {other:?}"),
            }),
        },
        DataType::Duration(unit) => match unit {
            arrow_schema::TimeUnit::Second => {
                let arr = as_primitive::<DurationSecondType>(array, path, dt)?;
                Ok(DynCellRef::i64(arr.value(index)))
            }
            arrow_schema::TimeUnit::Millisecond => {
                let arr = as_primitive::<DurationMillisecondType>(array, path, dt)?;
                Ok(DynCellRef::i64(arr.value(index)))
            }
            arrow_schema::TimeUnit::Microsecond => {
                let arr = as_primitive::<DurationMicrosecondType>(array, path, dt)?;
                Ok(DynCellRef::i64(arr.value(index)))
            }
            arrow_schema::TimeUnit::Nanosecond => {
                let arr = as_primitive::<DurationNanosecondType>(array, path, dt)?;
                Ok(DynCellRef::i64(arr.value(index)))
            }
        },
        DataType::UInt8 => {
            let arr = as_primitive::<UInt8Type>(array, path, dt)?;
            Ok(DynCellRef::u8(arr.value(index)))
        }
        DataType::UInt16 => {
            let arr = as_primitive::<UInt16Type>(array, path, dt)?;
            Ok(DynCellRef::u16(arr.value(index)))
        }
        DataType::UInt32 => {
            let arr = as_primitive::<UInt32Type>(array, path, dt)?;
            Ok(DynCellRef::u32(arr.value(index)))
        }
        DataType::UInt64 => {
            let arr = as_primitive::<UInt64Type>(array, path, dt)?;
            Ok(DynCellRef::u64(arr.value(index)))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            Ok(DynCellRef::f32(arr.value(index)))
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            Ok(DynCellRef::f64(arr.value(index)))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            Ok(DynCellRef::string(arr.value(index)))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            Ok(DynCellRef::string(arr.value(index)))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            Ok(DynCellRef::binary(arr.value(index)))
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            Ok(DynCellRef::binary(arr.value(index)))
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            Ok(DynCellRef::binary(arr.value(index)))
        }
        DataType::Struct(children) => {
            let arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            let view = DynStructView {
                array: arr,
                fields: children.clone(),
                row: index,
                base_path: path.clone(),
            };
            Ok(DynCellRef::structure(view))
        }
        DataType::List(item) => {
            let arr = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            let view = DynListView::new_list(arr, item.clone(), path.clone(), index)?;
            Ok(DynCellRef::list(view))
        }
        DataType::LargeList(item) => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            let view = DynListView::new_large_list(arr, item.clone(), path.clone(), index)?;
            Ok(DynCellRef::list(view))
        }
        DataType::FixedSizeList(item, len) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            let view =
                DynFixedSizeListView::new(arr, item.clone(), *len as usize, path.clone(), index)?;
            Ok(DynCellRef::fixed_size_list(view))
        }
        DataType::Map(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            let view = DynMapView::new(arr, path.clone(), index)?;
            Ok(DynCellRef::map(view))
        }
        DataType::Union(fields, mode) => {
            let arr = array
                .as_any()
                .downcast_ref::<UnionArray>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            let view = DynUnionView::new(arr, fields.clone(), *mode, path.clone(), index)?;
            Ok(DynCellRef::union(view))
        }
        DataType::Dictionary(key_type, value_type) => dictionary_value(
            path,
            field,
            array,
            index,
            key_type.as_ref(),
            value_type.as_ref(),
        ),
        other => Err(DynViewError::Invalid {
            column: path.column,
            path: path.path.clone(),
            message: format!("unsupported data type {other:?}"),
        }),
    }
}

fn dictionary_value<'a>(
    path: &Path,
    field: &Field,
    array: &'a dyn Array,
    index: usize,
    key_type: &DataType,
    value_type: &DataType,
) -> Result<DynCellRef<'a>, DynViewError> {
    macro_rules! match_dict {
        ($key_ty:ty) => {{
            let dict = array
                .as_any()
                .downcast_ref::<DictionaryArray<$key_ty>>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            dict_value(
                path,
                dict.keys().value(index) as usize,
                dict.values(),
                value_type,
            )
        }};
    }

    match key_type {
        DataType::Int8 => match_dict!(Int8Type),
        DataType::Int16 => match_dict!(Int16Type),
        DataType::Int32 => match_dict!(Int32Type),
        DataType::Int64 => match_dict!(Int64Type),
        DataType::UInt8 => match_dict!(UInt8Type),
        DataType::UInt16 => match_dict!(UInt16Type),
        DataType::UInt32 => match_dict!(UInt32Type),
        DataType::UInt64 => match_dict!(UInt64Type),
        other => Err(DynViewError::Invalid {
            column: path.column,
            path: path.path.clone(),
            message: format!("unsupported dictionary key type {other:?}"),
        }),
    }
}

fn dict_value<'a>(
    path: &Path,
    key_index: usize,
    values: &'a ArrayRef,
    value_type: &DataType,
) -> Result<DynCellRef<'a>, DynViewError> {
    if key_index >= values.len() {
        return Err(DynViewError::Invalid {
            column: path.column,
            path: path.path.clone(),
            message: format!(
                "dictionary key index {} out of bounds for {}",
                key_index,
                values.len()
            ),
        });
    }
    if values.is_null(key_index) {
        return Err(DynViewError::UnexpectedNull {
            column: path.column,
            path: path.path.clone(),
        });
    }
    match value_type {
        DataType::Utf8 => {
            let arr = values
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| type_mismatch(path, value_type.clone(), values.data_type()))?;
            Ok(DynCellRef::string(arr.value(key_index)))
        }
        DataType::LargeUtf8 => {
            let arr = values
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| type_mismatch(path, value_type.clone(), values.data_type()))?;
            Ok(DynCellRef::string(arr.value(key_index)))
        }
        DataType::Binary => {
            let arr = values
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| type_mismatch(path, value_type.clone(), values.data_type()))?;
            Ok(DynCellRef::binary(arr.value(key_index)))
        }
        DataType::LargeBinary => {
            let arr = values
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| type_mismatch(path, value_type.clone(), values.data_type()))?;
            Ok(DynCellRef::binary(arr.value(key_index)))
        }
        DataType::FixedSizeBinary(_) => {
            let arr = values
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| type_mismatch(path, value_type.clone(), values.data_type()))?;
            Ok(DynCellRef::binary(arr.value(key_index)))
        }
        DataType::Int8 => {
            let arr = values
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| type_mismatch(path, value_type.clone(), values.data_type()))?;
            Ok(DynCellRef::i8(arr.value(key_index)))
        }
        DataType::Int16 => {
            let arr = values
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| type_mismatch(path, value_type.clone(), values.data_type()))?;
            Ok(DynCellRef::i16(arr.value(key_index)))
        }
        DataType::Int32 => {
            let arr = values
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| type_mismatch(path, value_type.clone(), values.data_type()))?;
            Ok(DynCellRef::i32(arr.value(key_index)))
        }
        DataType::Int64 => {
            let arr = values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| type_mismatch(path, value_type.clone(), values.data_type()))?;
            Ok(DynCellRef::i64(arr.value(key_index)))
        }
        DataType::UInt8 => {
            let arr = values
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| type_mismatch(path, value_type.clone(), values.data_type()))?;
            Ok(DynCellRef::u8(arr.value(key_index)))
        }
        DataType::UInt16 => {
            let arr = values
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| type_mismatch(path, value_type.clone(), values.data_type()))?;
            Ok(DynCellRef::u16(arr.value(key_index)))
        }
        DataType::UInt32 => {
            let arr = values
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| type_mismatch(path, value_type.clone(), values.data_type()))?;
            Ok(DynCellRef::u32(arr.value(key_index)))
        }
        DataType::UInt64 => {
            let arr = values
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| type_mismatch(path, value_type.clone(), values.data_type()))?;
            Ok(DynCellRef::u64(arr.value(key_index)))
        }
        DataType::Float32 => {
            let arr = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| type_mismatch(path, value_type.clone(), values.data_type()))?;
            Ok(DynCellRef::f32(arr.value(key_index)))
        }
        DataType::Float64 => {
            let arr = values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| type_mismatch(path, value_type.clone(), values.data_type()))?;
            Ok(DynCellRef::f64(arr.value(key_index)))
        }
        other => Err(DynViewError::Invalid {
            column: path.column,
            path: path.path.clone(),
            message: format!("unsupported dictionary value type {other:?}"),
        }),
    }
}

fn type_mismatch(path: &Path, expected: DataType, actual: &DataType) -> DynViewError {
    DynViewError::TypeMismatch {
        column: path.column,
        path: path.path.clone(),
        expected,
        actual: actual.clone(),
    }
}

fn as_bool<'a>(array: &'a dyn Array, path: &Path) -> Result<&'a BooleanArray, DynViewError> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| type_mismatch(path, DataType::Boolean, array.data_type()))
}

fn as_primitive<'a, T>(
    array: &'a dyn Array,
    path: &Path,
    expected: &DataType,
) -> Result<&'a PrimitiveArray<T>, DynViewError>
where
    T: arrow_array::types::ArrowPrimitiveType,
{
    array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| type_mismatch(path, expected.clone(), array.data_type()))
}

/// View over a struct column.
#[derive(Debug, Clone)]
pub struct DynStructView<'a> {
    array: &'a StructArray,
    fields: Fields,
    row: usize,
    base_path: Path,
}

impl<'a> DynStructView<'a> {
    /// Number of child fields.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns true if the struct has no fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Retrieve the value of a struct field by index.
    pub fn get(&'a self, index: usize) -> Result<Option<DynCellRef<'a>>, DynViewError> {
        if index >= self.fields.len() {
            return Err(DynViewError::ColumnOutOfBounds {
                column: index,
                width: self.fields.len(),
            });
        }
        let field = self.fields.get(index).expect("index validated");
        let child = self.array.column(index);
        let path = self.base_path.push_field(field.name());
        view_cell(&path, field.as_ref(), child.as_ref(), self.row)
    }

    /// Retrieve a struct field by name.
    pub fn get_by_name(
        &'a self,
        name: &str,
    ) -> Option<Result<Option<DynCellRef<'a>>, DynViewError>> {
        self.fields
            .iter()
            .position(|f| f.name() == name)
            .map(move |idx| self.get(idx))
    }
}

/// View over `List<T>` / `LargeList<T>` values.
#[derive(Debug, Clone)]
pub struct DynListView<'a> {
    values: ArrayRef,
    item_field: FieldRef,
    start: usize,
    end: usize,
    base_path: Path,
    _marker: PhantomData<&'a ()>,
}

impl<'a> DynListView<'a> {
    fn new_list(
        array: &'a ListArray,
        item_field: FieldRef,
        base_path: Path,
        row: usize,
    ) -> Result<Self, DynViewError> {
        let offsets = array.value_offsets();
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        Ok(Self {
            values: array.values().clone(),
            item_field,
            start,
            end,
            base_path,
            _marker: PhantomData,
        })
    }

    fn new_large_list(
        array: &'a LargeListArray,
        item_field: FieldRef,
        base_path: Path,
        row: usize,
    ) -> Result<Self, DynViewError> {
        let offsets = array.value_offsets();
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        Ok(Self {
            values: array.values().clone(),
            item_field,
            start,
            end,
            base_path,
            _marker: PhantomData,
        })
    }

    /// Number of elements in the list.
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Returns true when the list contains no elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Retrieve the list element at `index`.
    pub fn get(&'a self, index: usize) -> Result<Option<DynCellRef<'a>>, DynViewError> {
        if index >= self.len() {
            return Err(DynViewError::RowOutOfBounds {
                row: index,
                len: self.len(),
            });
        }
        let absolute = self.start + index;
        let path = self.base_path.push_index(index);
        view_cell(
            &path,
            self.item_field.as_ref(),
            self.values.as_ref(),
            absolute,
        )
    }
}

/// View over a fixed-size list.
#[derive(Debug, Clone)]
pub struct DynFixedSizeListView<'a> {
    values: ArrayRef,
    item_field: FieldRef,
    start: usize,
    len: usize,
    base_path: Path,
    _marker: PhantomData<&'a ()>,
}

impl<'a> DynFixedSizeListView<'a> {
    fn new(
        array: &'a FixedSizeListArray,
        item_field: FieldRef,
        len: usize,
        base_path: Path,
        row: usize,
    ) -> Result<Self, DynViewError> {
        let start = row * len;
        Ok(Self {
            values: array.values().clone(),
            item_field,
            start,
            len,
            base_path,
            _marker: PhantomData,
        })
    }

    /// Number of items (constant for all rows).
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the fixed-size list length is zero.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Retrieve the element at `index`.
    pub fn get(&'a self, index: usize) -> Result<Option<DynCellRef<'a>>, DynViewError> {
        if index >= self.len {
            return Err(DynViewError::RowOutOfBounds {
                row: index,
                len: self.len,
            });
        }
        let absolute = self.start + index;
        let path = self.base_path.push_index(index);
        view_cell(
            &path,
            self.item_field.as_ref(),
            self.values.as_ref(),
            absolute,
        )
    }
}

/// View over a map column.
#[derive(Debug, Clone)]
pub struct DynMapView<'a> {
    array: &'a MapArray,
    start: usize,
    end: usize,
    base_path: Path,
}

impl<'a> DynMapView<'a> {
    fn new(array: &'a MapArray, base_path: Path, row: usize) -> Result<Self, DynViewError> {
        let offsets = array.value_offsets();
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        Ok(Self {
            array,
            start,
            end,
            base_path,
        })
    }

    /// Number of key/value pairs in the map entry.
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Returns true if the entry has no items.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the key/value pair at `index`.
    pub fn get(
        &'a self,
        index: usize,
    ) -> Result<(DynCellRef<'a>, Option<DynCellRef<'a>>), DynViewError> {
        if index >= self.len() {
            return Err(DynViewError::RowOutOfBounds {
                row: index,
                len: self.len(),
            });
        }
        let entries = self.array.entries();
        let struct_entry = entries
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("map entries must be struct array");

        let keys = struct_entry.column(0);
        let values = struct_entry.column(1);
        let entry_fields = struct_entry.fields();
        let key_field = Arc::clone(
            entry_fields
                .first()
                .expect("map entries must contain key field"),
        );
        let value_field = Arc::clone(
            entry_fields
                .get(1)
                .expect("map entries must contain value field"),
        );

        let absolute = self.start + index;
        let key_path = self.base_path.push_index(index).push_key();
        let key = view_non_null(&key_path, key_field.as_ref(), keys.as_ref(), absolute)?;

        let value_path = self.base_path.push_index(index).push_value();
        let value = view_cell(&value_path, value_field.as_ref(), values.as_ref(), absolute)?;

        Ok((key, value))
    }
}

/// View over a union value.
#[derive(Debug, Clone)]
pub struct DynUnionView<'a> {
    array: &'a UnionArray,
    fields: UnionFields,
    mode: UnionMode,
    row: usize,
    base_path: Path,
}

impl<'a> DynUnionView<'a> {
    fn new(
        array: &'a UnionArray,
        fields: UnionFields,
        mode: UnionMode,
        base_path: Path,
        row: usize,
    ) -> Result<Self, DynViewError> {
        if row >= array.len() {
            return Err(DynViewError::RowOutOfBounds {
                row,
                len: array.len(),
            });
        }
        Ok(Self {
            array,
            fields,
            mode,
            row,
            base_path,
        })
    }

    /// Active type id for this row.
    pub fn type_id(&self) -> i8 {
        self.array.type_id(self.row)
    }

    /// Active variant metadata.
    fn variant_field(&self) -> Result<(i8, FieldRef), DynViewError> {
        let tag = self.type_id();
        self.fields
            .iter()
            .find_map(|(t, field)| {
                if t == tag {
                    Some((t, Arc::clone(field)))
                } else {
                    None
                }
            })
            .ok_or_else(|| DynViewError::Invalid {
                column: self.base_path.column,
                path: self.base_path.path.clone(),
                message: format!("unknown union type id {tag}"),
            })
    }

    /// Returns the name of the active variant, if present.
    pub fn variant_name(&self) -> Option<&str> {
        let tag = self.type_id();
        self.fields
            .iter()
            .find(|(t, _)| *t == tag)
            .map(|(_, field)| field.name().as_str())
    }

    /// Retrieve the active value (or `None` if the variant payload is null).
    pub fn value(&'a self) -> Result<Option<DynCellRef<'a>>, DynViewError> {
        let (tag, field) = self.variant_field()?;
        let child = self.array.child(tag);
        let child_index = match self.mode {
            UnionMode::Dense => self.array.value_offset(self.row),
            UnionMode::Sparse => self.row,
        };
        let path = self.base_path.push_variant(field.name().as_str(), tag);
        view_cell(&path, field.as_ref(), child.as_ref(), child_index)
    }
}

/// Create dynamic views for a batch using the provided schema reference.
pub fn iter_batch_views<'a>(
    schema: &'a DynSchema,
    batch: &'a RecordBatch,
) -> Result<DynRowViews<'a>, DynViewError> {
    DynRowViews::new(batch, schema.schema.as_ref())
}

/// Borrow a single row from `batch` as a dynamic view after schema validation.
pub fn view_batch_row<'a>(
    schema: &'a DynSchema,
    batch: &'a RecordBatch,
    row: usize,
) -> Result<DynRowView<'a>, DynViewError> {
    validate_schema_matches(batch, schema.schema.as_ref())?;
    let len = batch.num_rows();
    if row >= len {
        return Err(DynViewError::RowOutOfBounds { row, len });
    }
    Ok(DynRowView {
        batch,
        fields: schema.schema.fields().clone(),
        mapping: None,
        row,
    })
}
