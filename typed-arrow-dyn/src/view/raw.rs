use std::{marker::PhantomData, ptr::NonNull, sync::Arc};

use arrow_array::{ArrayRef, MapArray, StructArray, UnionArray};
use arrow_schema::{FieldRef, Fields, UnionFields, UnionMode};

use super::{
    path::Path,
    projection::{FieldProjector, StructProjection},
    views::{DynFixedSizeListView, DynListView, DynMapView, DynStructView, DynUnionView},
};

/// Lifetime-erased struct view backing a [`DynCellRaw::Struct`] cell.
#[derive(Debug, Clone)]
pub struct DynStructViewRaw {
    array: NonNull<StructArray>,
    fields: Fields,
    row: usize,
    base_path: Path,
    projection: Option<Arc<StructProjection>>,
}

impl DynStructViewRaw {
    pub(super) fn from_view(view: DynStructView<'_>) -> Self {
        Self {
            array: NonNull::from(view.array),
            fields: view.fields.clone(),
            row: view.row,
            base_path: view.base_path.clone(),
            projection: view.projection.clone(),
        }
    }

    /// Reborrow the struct view with an explicit lifetime.
    ///
    /// # Safety
    /// The caller must ensure the underlying `StructArray` outlives `'a`.
    pub unsafe fn as_view<'a>(&self) -> DynStructView<'a> {
        DynStructView {
            array: unsafe { self.array.as_ref() },
            fields: self.fields.clone(),
            row: self.row,
            base_path: self.base_path.clone(),
            projection: self.projection.clone(),
        }
    }

    /// Consume the raw view, yielding a scoped [`DynStructView`].
    ///
    /// # Safety
    /// The caller must ensure the underlying `StructArray` outlives `'a`.
    pub unsafe fn into_view<'a>(self) -> DynStructView<'a> {
        let array = unsafe { self.array.as_ref() };
        DynStructView {
            array,
            fields: self.fields,
            row: self.row,
            base_path: self.base_path,
            projection: self.projection,
        }
    }
}

/// Lifetime-erased list view backing a [`DynCellRaw::List`] cell.
#[derive(Debug, Clone)]
pub struct DynListViewRaw {
    values: ArrayRef,
    item_field: FieldRef,
    start: usize,
    end: usize,
    base_path: Path,
    item_projector: Option<FieldProjector>,
}

impl DynListViewRaw {
    pub(super) fn from_view(view: DynListView<'_>) -> Self {
        Self {
            values: view.values.clone(),
            item_field: Arc::clone(&view.item_field),
            start: view.start,
            end: view.end,
            base_path: view.base_path.clone(),
            item_projector: view.item_projector.clone(),
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
            item_projector: self.item_projector.clone(),
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
            item_projector: self.item_projector,
            _marker: PhantomData,
        }
    }
}

/// Lifetime-erased fixed-size list view backing a [`DynCellRaw::FixedSizeList`] cell.

#[derive(Debug, Clone)]
pub struct DynFixedSizeListViewRaw {
    values: ArrayRef,
    item_field: FieldRef,
    start: usize,
    len: usize,
    base_path: Path,
    item_projector: Option<FieldProjector>,
}

impl DynFixedSizeListViewRaw {
    pub(super) fn from_view(view: DynFixedSizeListView<'_>) -> Self {
        Self {
            values: view.values.clone(),
            item_field: Arc::clone(&view.item_field),
            start: view.start,
            len: view.len,
            base_path: view.base_path.clone(),
            item_projector: view.item_projector.clone(),
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
            item_projector: self.item_projector.clone(),
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
            item_projector: self.item_projector,
            _marker: PhantomData,
        }
    }
}

/// Lifetime-erased map view backing a [`DynCellRaw::Map`] cell.
#[derive(Debug, Clone)]
pub struct DynMapViewRaw {
    array: NonNull<MapArray>,
    start: usize,
    end: usize,
    base_path: Path,
    fields: Fields,
    projection: Option<Arc<StructProjection>>,
}

impl DynMapViewRaw {
    pub(super) fn from_view(view: DynMapView<'_>) -> Self {
        Self {
            array: NonNull::from(view.array),
            start: view.start,
            end: view.end,
            base_path: view.base_path.clone(),
            fields: view.fields.clone(),
            projection: view.projection.clone(),
        }
    }

    /// Reborrow the map view with an explicit lifetime.
    ///
    /// # Safety
    /// The caller must ensure the underlying `MapArray` outlives `'a`.
    pub unsafe fn as_view<'a>(&self) -> DynMapView<'a> {
        DynMapView {
            array: unsafe { self.array.as_ref() },
            start: self.start,
            end: self.end,
            base_path: self.base_path.clone(),
            fields: self.fields.clone(),
            projection: self.projection.clone(),
        }
    }

    /// Consume the raw map view, yielding a scoped [`DynMapView`].
    ///
    /// # Safety
    /// The caller must ensure the underlying `MapArray` outlives `'a`.
    pub unsafe fn into_view<'a>(self) -> DynMapView<'a> {
        DynMapView {
            array: unsafe { self.array.as_ref() },
            start: self.start,
            end: self.end,
            base_path: self.base_path,
            fields: self.fields,
            projection: self.projection,
        }
    }
}

/// Lifetime-erased union view backing a [`DynCellRaw::Union`] cell.
#[derive(Debug, Clone)]
pub struct DynUnionViewRaw {
    array: NonNull<UnionArray>,
    fields: UnionFields,
    mode: UnionMode,
    row: usize,
    base_path: Path,
}

impl DynUnionViewRaw {
    pub(super) fn from_view(view: DynUnionView<'_>) -> Self {
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
            array: unsafe { self.array.as_ref() },
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
            array: unsafe { self.array.as_ref() },
            fields: self.fields,
            mode: self.mode,
            row: self.row,
            base_path: self.base_path,
        }
    }
}
