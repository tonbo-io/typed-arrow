//! Nested dynamic builders used by the factory.

use arrow_array::{FixedSizeListArray, LargeListArray, MapArray};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{
    ArrowError::{self, ComputeError},
    DataType, FieldRef, Fields,
};

use crate::{cell::DynCell, dyn_builder::DynColumnBuilder, DynError};

type UnionMetadata = Vec<(usize, Vec<usize>)>;
type TryFinishResult<T> = Result<(T, UnionMetadata), ArrowError>;

/// Nested struct column builder.
pub(crate) struct StructCol {
    pub(crate) fields: Fields,
    pub(crate) children: Vec<Box<dyn DynColumnBuilder>>, // same len as fields
    pub(crate) validity: BooleanBufferBuilder,
}

impl StructCol {
    pub(crate) fn new_with_children(
        fields: Fields,
        children: Vec<Box<dyn DynColumnBuilder>>,
    ) -> Self {
        Self {
            fields,
            children,
            validity: BooleanBufferBuilder::new(0),
        }
    }
    pub(crate) fn append_null(&mut self) {
        for c in &mut self.children {
            c.append_null();
        }
        self.validity.append(false);
    }
    pub(crate) fn append_struct(&mut self, cells: Vec<Option<DynCell>>) -> Result<(), DynError> {
        if cells.len() != self.children.len() {
            return Err(DynError::Builder {
                message: format!(
                    "struct arity mismatch: expected {}, got {}",
                    self.children.len(),
                    cells.len()
                ),
            });
        }
        for (idx, (child, cell)) in self.children.iter_mut().zip(cells.into_iter()).enumerate() {
            match cell {
                None => child.append_null(),
                Some(v) => child.append_dyn(v).map_err(|e| e.at_col(idx))?,
            }
        }
        self.validity.append(true);
        Ok(())
    }
    pub(crate) fn finish(&mut self) -> arrow_array::StructArray {
        let cols: Vec<_> = self.children.iter_mut().map(|c| c.finish()).collect();
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(NullBuffer::new(v.finish()));
        arrow_array::StructArray::new(self.fields.clone(), cols, validity)
    }

    pub(crate) fn try_finish(&mut self) -> TryFinishResult<arrow_array::StructArray> {
        let finished_children: Vec<_> = self
            .children
            .iter_mut()
            .map(|c| c.try_finish().map_err(|e| ComputeError(e.to_string())))
            .collect::<Result<_, _>>()?;
        let mut cols = Vec::with_capacity(finished_children.len());
        let mut union_metadata = Vec::new();
        for mut child in finished_children {
            union_metadata.append(&mut child.union_metadata);
            cols.push(child.array);
        }
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(NullBuffer::new(v.finish()));
        let array = arrow_array::StructArray::try_new(self.fields.clone(), cols, validity)?;
        Ok((array, union_metadata))
    }
}

/// Variable-sized list builder.
pub(crate) struct ListCol {
    pub(crate) item_field: FieldRef,
    pub(crate) child: Box<dyn DynColumnBuilder>,
    pub(crate) offsets: Vec<i32>,
    pub(crate) validity: BooleanBufferBuilder,
}

impl ListCol {
    pub(crate) fn new_with_child(item: FieldRef, child: Box<dyn DynColumnBuilder>) -> Self {
        Self {
            item_field: item,
            child,
            offsets: vec![0],
            validity: BooleanBufferBuilder::new(0),
        }
    }
    pub(crate) fn append_null(&mut self) {
        self.validity.append(false);
        let last = *self.offsets.last().unwrap();
        self.offsets.push(last);
    }
    pub(crate) fn append_list(&mut self, items: Vec<Option<DynCell>>) -> Result<(), DynError> {
        let mut added = 0i32;
        for it in items {
            match it {
                None => self.child.append_null(),
                Some(v) => self.child.append_dyn(v)?,
            }
            added += 1;
        }
        let last = *self.offsets.last().unwrap();
        self.offsets.push(last + added);
        self.validity.append(true);
        Ok(())
    }
    pub(crate) fn finish(&mut self) -> arrow_array::ListArray {
        let values = self.child.finish();
        let offsets: OffsetBuffer<i32> =
            OffsetBuffer::new(self.offsets.iter().copied().collect::<ScalarBuffer<_>>());
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(NullBuffer::new(v.finish()));
        arrow_array::ListArray::new(self.item_field.clone(), offsets, values, validity)
    }

    pub(crate) fn try_finish(&mut self) -> TryFinishResult<arrow_array::ListArray> {
        let finished_child = self
            .child
            .try_finish()
            .map_err(|e| ComputeError(e.to_string()))?;
        let values = finished_child.array;
        let offsets: OffsetBuffer<i32> =
            OffsetBuffer::new(self.offsets.iter().copied().collect::<ScalarBuffer<_>>());
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(NullBuffer::new(v.finish()));
        let array =
            arrow_array::ListArray::try_new(self.item_field.clone(), offsets, values, validity)?;
        Ok((array, finished_child.union_metadata))
    }
}

/// Large list builder.
pub(crate) struct LargeListCol {
    pub(crate) item_field: FieldRef,
    pub(crate) child: Box<dyn DynColumnBuilder>,
    pub(crate) offsets: Vec<i64>,
    pub(crate) validity: BooleanBufferBuilder,
}

impl LargeListCol {
    pub(crate) fn new_with_child(item: FieldRef, child: Box<dyn DynColumnBuilder>) -> Self {
        Self {
            item_field: item,
            child,
            offsets: vec![0],
            validity: BooleanBufferBuilder::new(0),
        }
    }
    pub(crate) fn append_null(&mut self) {
        self.validity.append(false);
        let last = *self.offsets.last().unwrap();
        self.offsets.push(last);
    }
    pub(crate) fn append_list(&mut self, items: Vec<Option<DynCell>>) -> Result<(), DynError> {
        let mut added = 0i64;
        for it in items {
            match it {
                None => self.child.append_null(),
                Some(v) => self.child.append_dyn(v)?,
            }
            added += 1;
        }
        let last = *self.offsets.last().unwrap();
        self.offsets.push(last + added);
        self.validity.append(true);
        Ok(())
    }
    pub(crate) fn finish(&mut self) -> LargeListArray {
        let values = self.child.finish();
        let offsets: OffsetBuffer<i64> =
            OffsetBuffer::new(self.offsets.iter().copied().collect::<ScalarBuffer<_>>());
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(NullBuffer::new(v.finish()));
        LargeListArray::new(self.item_field.clone(), offsets, values, validity)
    }

    pub(crate) fn try_finish(&mut self) -> TryFinishResult<LargeListArray> {
        let finished_child = self
            .child
            .try_finish()
            .map_err(|e| ComputeError(e.to_string()))?;
        let values = finished_child.array;
        let offsets: OffsetBuffer<i64> =
            OffsetBuffer::new(self.offsets.iter().copied().collect::<ScalarBuffer<_>>());
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(NullBuffer::new(v.finish()));
        let array = LargeListArray::try_new(self.item_field.clone(), offsets, values, validity)?;
        Ok((array, finished_child.union_metadata))
    }
}

/// Map column builder storing key/value children and offsets.
pub(crate) struct MapCol {
    entry_field: FieldRef,
    value_nullable: bool,
    keys_sorted: bool,
    keys: Box<dyn DynColumnBuilder>,
    values: Box<dyn DynColumnBuilder>,
    offsets: Vec<i32>,
    validity: BooleanBufferBuilder,
}

impl MapCol {
    pub(crate) fn new_with_children(
        entry_field: FieldRef,
        keys_sorted: bool,
        keys: Box<dyn DynColumnBuilder>,
        values: Box<dyn DynColumnBuilder>,
    ) -> Self {
        let value_nullable = match entry_field.data_type() {
            DataType::Struct(children) => children.get(1).is_none_or(|field| field.is_nullable()),
            _ => true,
        };

        Self {
            entry_field,
            value_nullable,
            keys_sorted,
            keys,
            values,
            offsets: vec![0],
            validity: BooleanBufferBuilder::new(0),
        }
    }

    pub(crate) fn append_null(&mut self) {
        self.validity.append(false);
        let last = *self.offsets.last().unwrap();
        self.offsets.push(last);
    }

    pub(crate) fn append_map(
        &mut self,
        entries: Vec<(DynCell, Option<DynCell>)>,
    ) -> Result<(), DynError> {
        let entry_count = entries.len();
        for (idx, (key_cell, value_cell)) in entries.into_iter().enumerate() {
            match key_cell {
                DynCell::Null => {
                    return Err(DynError::Builder {
                        message: format!("map key at index {} cannot be null", idx),
                    });
                }
                key => self.keys.append_dyn(key)?,
            }

            match value_cell {
                None => {
                    if !self.value_nullable {
                        return Err(DynError::Builder {
                            message: format!(
                                "map value at index {} is null but values are not nullable",
                                idx
                            ),
                        });
                    }
                    self.values.append_null();
                }
                Some(DynCell::Null) => {
                    if !self.value_nullable {
                        return Err(DynError::Builder {
                            message: format!(
                                "map value at index {} is null but values are not nullable",
                                idx
                            ),
                        });
                    }
                    self.values.append_null();
                }
                Some(value) => self.values.append_dyn(value)?,
            }
        }

        let added = i32::try_from(entry_count).map_err(|_| DynError::Builder {
            message: "map entry count exceeds i32::MAX".to_string(),
        })?;
        let last = *self.offsets.last().unwrap();
        let next = last.checked_add(added).ok_or_else(|| DynError::Builder {
            message: "map entry offsets overflow i32".to_string(),
        })?;
        self.offsets.push(next);
        self.validity.append(true);
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> MapArray {
        let keys = self.keys.finish();
        let values = self.values.finish();
        let offsets: OffsetBuffer<i32> =
            OffsetBuffer::new(self.offsets.iter().copied().collect::<ScalarBuffer<_>>());
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(NullBuffer::new(v.finish()));
        let fields = match self.entry_field.data_type() {
            DataType::Struct(children) => children.clone(),
            _ => unreachable!("map entry field is not struct"),
        };
        let entries = arrow_array::StructArray::new(fields, vec![keys, values], None);
        MapArray::new(
            self.entry_field.clone(),
            offsets,
            entries,
            validity,
            self.keys_sorted,
        )
    }

    pub(crate) fn try_finish(&mut self) -> TryFinishResult<MapArray> {
        let finished_keys = self
            .keys
            .try_finish()
            .map_err(|e| ComputeError(e.to_string()))?;
        let mut finished_values = self
            .values
            .try_finish()
            .map_err(|e| ComputeError(e.to_string()))?;
        let offsets: OffsetBuffer<i32> =
            OffsetBuffer::new(self.offsets.iter().copied().collect::<ScalarBuffer<_>>());
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(NullBuffer::new(v.finish()));
        let fields = match self.entry_field.data_type() {
            DataType::Struct(children) => children.clone(),
            _ => unreachable!("map entry field is not struct"),
        };
        let entries = arrow_array::StructArray::try_new(
            fields,
            vec![finished_keys.array.clone(), finished_values.array.clone()],
            None,
        )
        .map_err(|e| ComputeError(e.to_string()))?;
        let array = MapArray::try_new(
            self.entry_field.clone(),
            offsets,
            entries,
            validity,
            self.keys_sorted,
        )?;
        let mut union_metadata = finished_keys.union_metadata;
        union_metadata.append(&mut finished_values.union_metadata);
        Ok((array, union_metadata))
    }
}

/// Fixed-size list builder.
pub(crate) struct FixedSizeListCol {
    pub(crate) item_field: FieldRef,
    pub(crate) child: Box<dyn DynColumnBuilder>,
    pub(crate) len: i32,
    pub(crate) validity: BooleanBufferBuilder,
}

impl FixedSizeListCol {
    pub(crate) fn new_with_child(
        item: FieldRef,
        len: i32,
        child: Box<dyn DynColumnBuilder>,
    ) -> Self {
        Self {
            item_field: item,
            child,
            len,
            validity: BooleanBufferBuilder::new(0),
        }
    }
    pub(crate) fn append_null(&mut self) {
        for _ in 0..self.len {
            self.child.append_null();
        }
        self.validity.append(false);
    }
    pub(crate) fn append_fixed(&mut self, items: Vec<Option<DynCell>>) -> Result<(), DynError> {
        if usize::try_from(self.len).ok() != Some(items.len()) {
            return Err(DynError::Builder {
                message: format!(
                    "fixed-size list length mismatch: expected {}, got {}",
                    self.len,
                    items.len()
                ),
            });
        }
        for it in items {
            match it {
                None => self.child.append_null(),
                Some(v) => self.child.append_dyn(v)?,
            }
        }
        self.validity.append(true);
        Ok(())
    }
    pub(crate) fn finish(&mut self) -> FixedSizeListArray {
        let values = self.child.finish();
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(NullBuffer::new(v.finish()));
        FixedSizeListArray::new(self.item_field.clone(), self.len, values, validity)
    }

    pub(crate) fn try_finish(&mut self) -> TryFinishResult<FixedSizeListArray> {
        let finished_child = self
            .child
            .try_finish()
            .map_err(|e| ComputeError(e.to_string()))?;
        let values = finished_child.array;
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(NullBuffer::new(v.finish()));
        let array =
            FixedSizeListArray::try_new(self.item_field.clone(), self.len, values, validity)?;
        Ok((array, finished_child.union_metadata))
    }
}
