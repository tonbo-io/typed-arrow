use std::{marker::PhantomData, ptr::NonNull, slice, str, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, DictionaryArray, FixedSizeBinaryArray,
    FixedSizeListArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray, PrimitiveArray,
    StringArray, StructArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array, UnionArray,
    types::{
        Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType,
        DurationNanosecondType, DurationSecondType, Int8Type, Int16Type, Int32Type, Int64Type,
        Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_schema::{DataType, Field};

use super::{
    path::Path,
    projection::{FieldProjector, StructProjection},
    raw::{
        DynFixedSizeListViewRaw, DynListViewRaw, DynMapViewRaw, DynStructViewRaw, DynUnionViewRaw,
    },
    views::{DynFixedSizeListView, DynListView, DynMapView, DynStructView, DynUnionView},
};
use crate::{DynViewError, cell::DynCell};

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

    pub(super) fn from_str(value: &str) -> Self {
        Self::Str {
            ptr: non_null_from_bytes(value.as_bytes()),
            len: value.len(),
        }
    }

    pub(super) fn from_bin(value: &[u8]) -> Self {
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

fn non_null_from_bytes(bytes: &[u8]) -> NonNull<u8> {
    let ptr = bytes.as_ptr() as *mut u8;
    // `NonNull::dangling` is acceptable for zero-length slices/strings.
    NonNull::new(ptr).unwrap_or_else(NonNull::dangling)
}

fn view_cell_identity<'a>(
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

pub(super) fn view_cell_with_projector<'a>(
    path: &Path,
    field: &Field,
    projector: Option<&FieldProjector>,
    array: &'a dyn Array,
    index: usize,
) -> Result<Option<DynCellRef<'a>>, DynViewError> {
    match projector {
        None | Some(FieldProjector::Identity) => view_cell_identity(path, field, array, index),
        Some(projector) => view_cell_projected(path, field, projector, array, index),
    }
}

fn view_cell_projected<'a>(
    path: &Path,
    field: &Field,
    projector: &FieldProjector,
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
    let value = match projector {
        FieldProjector::Identity => view_non_null(path, field, array, index)?,
        FieldProjector::Struct(struct_proj) => {
            view_struct_projected(path, field, struct_proj, array, index)?
        }
        FieldProjector::List(item_proj) => {
            view_list_projected(path, field, item_proj, array, index)?
        }
        FieldProjector::LargeList(item_proj) => {
            view_large_list_projected(path, field, item_proj, array, index)?
        }
        FieldProjector::FixedSizeList(item_proj) => {
            view_fixed_size_list_projected(path, field, item_proj, array, index)?
        }
        FieldProjector::Map(entry_proj) => {
            view_map_projected(path, field, entry_proj, array, index)?
        }
    };
    Ok(Some(value))
}

fn view_struct_projected<'a>(
    path: &Path,
    field: &Field,
    projection: &Arc<StructProjection>,
    array: &'a dyn Array,
    index: usize,
) -> Result<DynCellRef<'a>, DynViewError> {
    let DataType::Struct(children) = field.data_type() else {
        return Err(DynViewError::Invalid {
            column: path.column,
            path: path.path.clone(),
            message: "expected struct field for projected struct".to_string(),
        });
    };
    let arr = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
    let view = DynStructView {
        array: arr,
        fields: children.clone(),
        row: index,
        base_path: path.clone(),
        projection: Some(Arc::clone(projection)),
    };
    Ok(DynCellRef::structure(view))
}

fn view_list_projected<'a>(
    path: &Path,
    field: &Field,
    item_projector: &FieldProjector,
    array: &'a dyn Array,
    index: usize,
) -> Result<DynCellRef<'a>, DynViewError> {
    let DataType::List(item_field) = field.data_type() else {
        return Err(DynViewError::Invalid {
            column: path.column,
            path: path.path.clone(),
            message: "expected list field for projected list".to_string(),
        });
    };
    let arr = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
    let view = DynListView::new_list(
        arr,
        item_field.clone(),
        path.clone(),
        index,
        Some(item_projector.clone()),
    )?;
    Ok(DynCellRef::list(view))
}

fn view_large_list_projected<'a>(
    path: &Path,
    field: &Field,
    item_projector: &FieldProjector,
    array: &'a dyn Array,
    index: usize,
) -> Result<DynCellRef<'a>, DynViewError> {
    let DataType::LargeList(item_field) = field.data_type() else {
        return Err(DynViewError::Invalid {
            column: path.column,
            path: path.path.clone(),
            message: "expected large list field for projected list".to_string(),
        });
    };
    let arr = array
        .as_any()
        .downcast_ref::<LargeListArray>()
        .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
    let view = DynListView::new_large_list(
        arr,
        item_field.clone(),
        path.clone(),
        index,
        Some(item_projector.clone()),
    )?;
    Ok(DynCellRef::list(view))
}

fn view_fixed_size_list_projected<'a>(
    path: &Path,
    field: &Field,
    item_projector: &FieldProjector,
    array: &'a dyn Array,
    index: usize,
) -> Result<DynCellRef<'a>, DynViewError> {
    let DataType::FixedSizeList(item_field, len) = field.data_type() else {
        return Err(DynViewError::Invalid {
            column: path.column,
            path: path.path.clone(),
            message: "expected fixed-size list field for projection".to_string(),
        });
    };
    let arr = array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
    let view = DynFixedSizeListView::new(
        arr,
        item_field.clone(),
        *len as usize,
        path.clone(),
        index,
        Some(item_projector.clone()),
    )?;
    Ok(DynCellRef::fixed_size_list(view))
}

fn view_map_projected<'a>(
    path: &Path,
    field: &Field,
    entry_projection: &Arc<StructProjection>,
    array: &'a dyn Array,
    index: usize,
) -> Result<DynCellRef<'a>, DynViewError> {
    let DataType::Map(entry_field, _) = field.data_type() else {
        return Err(DynViewError::Invalid {
            column: path.column,
            path: path.path.clone(),
            message: "expected map field for projection".to_string(),
        });
    };
    let arr = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
    let entry_fields = match entry_field.data_type() {
        DataType::Struct(children) => children.clone(),
        other => {
            return Err(DynViewError::Invalid {
                column: path.column,
                path: path.path.clone(),
                message: format!("map entry must be struct, found {other:?}"),
            });
        }
    };
    let view = DynMapView::with_projection(
        arr,
        entry_fields,
        path.clone(),
        index,
        Some(Arc::clone(entry_projection)),
    )?;
    Ok(DynCellRef::map(view))
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
                projection: None,
            };
            Ok(DynCellRef::structure(view))
        }
        DataType::List(item) => {
            let arr = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            let view = DynListView::new_list(arr, item.clone(), path.clone(), index, None)?;
            Ok(DynCellRef::list(view))
        }
        DataType::LargeList(item) => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            let view = DynListView::new_large_list(arr, item.clone(), path.clone(), index, None)?;
            Ok(DynCellRef::list(view))
        }
        DataType::FixedSizeList(item, len) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| type_mismatch(path, field.data_type().clone(), array.data_type()))?;
            let view = DynFixedSizeListView::new(
                arr,
                item.clone(),
                *len as usize,
                path.clone(),
                index,
                None,
            )?;
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

pub(super) fn type_mismatch(path: &Path, expected: DataType, actual: &DataType) -> DynViewError {
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
