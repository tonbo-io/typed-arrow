use std::{marker::PhantomData, sync::Arc};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray,
    UnionArray,
};
use arrow_schema::{FieldRef, Fields, UnionFields, UnionMode};

use super::{
    cell::{view_cell_with_projector, DynCellRef},
    path::Path,
    projection::{FieldProjector, StructProjection},
};
use crate::DynViewError;

/// View over a struct column.
pub struct DynStructView<'a> {
    pub(super) array: &'a StructArray,
    pub(super) fields: Fields,
    pub(super) row: usize,
    pub(super) base_path: Path,
    pub(super) projection: Option<Arc<StructProjection>>,
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
        let field = self
            .fields
            .get(index)
            .ok_or_else(|| DynViewError::ColumnOutOfBounds {
                column: index,
                width: self.fields.len(),
            })?;
        let (source_index, projector) = if let Some(projection) = &self.projection {
            let child = projection
                .children
                .get(index)
                .ok_or(DynViewError::ColumnOutOfBounds {
                    column: index,
                    width: projection.children.len(),
                })?;
            (child.source_index, Some(&child.projector))
        } else {
            (index, None)
        };
        let child = self.array.column(source_index);
        let path = self.base_path.push_field(field.name());
        view_cell_with_projector(&path, field.as_ref(), projector, child.as_ref(), self.row)
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
    pub(super) values: ArrayRef,
    pub(super) item_field: FieldRef,
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) base_path: Path,
    pub(super) item_projector: Option<FieldProjector>,
    pub(super) _marker: PhantomData<&'a ()>,
}

impl<'a> DynListView<'a> {
    pub(super) fn new_list(
        array: &'a ListArray,
        item_field: FieldRef,
        base_path: Path,
        row: usize,
        item_projector: Option<FieldProjector>,
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
            item_projector,
            _marker: PhantomData,
        })
    }

    pub(super) fn new_large_list(
        array: &'a LargeListArray,
        item_field: FieldRef,
        base_path: Path,
        row: usize,
        item_projector: Option<FieldProjector>,
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
            item_projector,
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
        let projector = self.item_projector.as_ref();
        view_cell_with_projector(
            &path,
            self.item_field.as_ref(),
            projector,
            self.values.as_ref(),
            absolute,
        )
    }
}

/// View over a fixed-size list.
#[derive(Debug, Clone)]
pub struct DynFixedSizeListView<'a> {
    pub(super) values: ArrayRef,
    pub(super) item_field: FieldRef,
    pub(super) start: usize,
    pub(super) len: usize,
    pub(super) base_path: Path,
    pub(super) item_projector: Option<FieldProjector>,
    pub(super) _marker: PhantomData<&'a ()>,
}

impl<'a> DynFixedSizeListView<'a> {
    pub(super) fn new(
        array: &'a FixedSizeListArray,
        item_field: FieldRef,
        len: usize,
        base_path: Path,
        row: usize,
        item_projector: Option<FieldProjector>,
    ) -> Result<Self, DynViewError> {
        let start = row * len;
        Ok(Self {
            values: array.values().clone(),
            item_field,
            start,
            len,
            base_path,
            item_projector,
            _marker: PhantomData,
        })
    }

    /// Number of items (constant for all rows).
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true when the list contains no items.
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
        let projector = self.item_projector.as_ref();
        view_cell_with_projector(
            &path,
            self.item_field.as_ref(),
            projector,
            self.values.as_ref(),
            absolute,
        )
    }
}

/// View over a map column.
#[derive(Debug, Clone)]
pub struct DynMapView<'a> {
    pub(super) array: &'a MapArray,
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) base_path: Path,
    pub(super) fields: Fields,
    pub(super) projection: Option<Arc<StructProjection>>,
}

impl<'a> DynMapView<'a> {
    pub(super) fn new(
        array: &'a MapArray,
        base_path: Path,
        row: usize,
    ) -> Result<Self, DynViewError> {
        let entry_fields = array
            .entries()
            .as_any()
            .downcast_ref::<StructArray>()
            .map(|struct_arr| struct_arr.fields().clone())
            .ok_or_else(|| DynViewError::Invalid {
                column: 0,
                path: base_path.path.clone(),
                message: "map entries must be struct".to_string(),
            })?;
        Self::with_projection(array, entry_fields, base_path, row, None)
    }

    pub(super) fn with_projection(
        array: &'a MapArray,
        entry_fields: Fields,
        base_path: Path,
        row: usize,
        projection: Option<Arc<StructProjection>>,
    ) -> Result<Self, DynViewError> {
        let offsets = array.value_offsets();
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        Ok(Self {
            array,
            start,
            end,
            base_path,
            fields: entry_fields,
            projection,
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
            .ok_or_else(|| DynViewError::Invalid {
                column: self.base_path.column,
                path: self.base_path.path.clone(),
                message: "map entries must be struct arrays".to_string(),
            })?;

        let (key_source, key_projector) = if let Some(proj) = &self.projection {
            let child = proj.children.first().ok_or_else(|| DynViewError::Invalid {
                column: self.base_path.column,
                path: self.base_path.path.clone(),
                message: "map projection missing key child".to_string(),
            })?;
            (child.source_index, Some(&child.projector))
        } else {
            (0, None)
        };
        let (value_source, value_projector) = if let Some(proj) = &self.projection {
            let child = proj.children.get(1).ok_or_else(|| DynViewError::Invalid {
                column: self.base_path.column,
                path: self.base_path.path.clone(),
                message: "map projection missing value child".to_string(),
            })?;
            (child.source_index, Some(&child.projector))
        } else {
            (1, None)
        };
        let keys = struct_entry.column(key_source);
        let values = struct_entry.column(value_source);
        let key_field = Arc::clone(self.fields.first().ok_or_else(|| DynViewError::Invalid {
            column: self.base_path.column,
            path: self.base_path.path.clone(),
            message: "map schema missing key field".to_string(),
        })?);
        let value_field = Arc::clone(self.fields.get(1).ok_or_else(|| DynViewError::Invalid {
            column: self.base_path.column,
            path: self.base_path.path.clone(),
            message: "map schema missing value field".to_string(),
        })?);

        let absolute = self.start + index;
        let key_path = self.base_path.push_index(index).push_key();
        let key = view_cell_with_projector(
            &key_path,
            key_field.as_ref(),
            key_projector,
            keys.as_ref(),
            absolute,
        )?
        .ok_or_else(|| DynViewError::Invalid {
            column: key_path.column,
            path: key_path.path.clone(),
            message: "map keys may not be null".to_string(),
        })?;

        let value_path = self.base_path.push_index(index).push_value();
        let value = view_cell_with_projector(
            &value_path,
            value_field.as_ref(),
            value_projector,
            values.as_ref(),
            absolute,
        )?;

        Ok((key, value))
    }
}

/// View over a union value.
#[derive(Debug, Clone)]
pub struct DynUnionView<'a> {
    pub(super) array: &'a UnionArray,
    pub(super) fields: UnionFields,
    pub(super) mode: UnionMode,
    pub(super) row: usize,
    pub(super) base_path: Path,
}

impl<'a> DynUnionView<'a> {
    pub(super) fn new(
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
        view_cell_with_projector(&path, field.as_ref(), None, child.as_ref(), child_index)
    }
}
