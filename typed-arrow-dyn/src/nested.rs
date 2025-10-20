//! Nested dynamic builders used by the factory.

use std::sync::Arc;

use arrow_array::{ArrayRef, FixedSizeListArray, LargeListArray, MapArray};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{
    ArrowError::{self, ComputeError},
    FieldRef, Fields,
};

use crate::{cell::DynCell, dyn_builder::DynColumnBuilder, DynError};

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

    pub(crate) fn try_finish(&mut self) -> Result<arrow_array::StructArray, ArrowError> {
        let cols: Vec<_> = self
            .children
            .iter_mut()
            .map(|c| c.try_finish().map_err(|e| ComputeError(e.to_string())))
            .collect::<Result<_, _>>()?;
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(NullBuffer::new(v.finish()));
        arrow_array::StructArray::try_new(self.fields.clone(), cols, validity)
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

    pub(crate) fn try_finish(&mut self) -> Result<arrow_array::ListArray, ArrowError> {
        let values = self
            .child
            .try_finish()
            .map_err(|e| ComputeError(e.to_string()))?;
        let offsets: OffsetBuffer<i32> =
            OffsetBuffer::new(self.offsets.iter().copied().collect::<ScalarBuffer<_>>());
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(NullBuffer::new(v.finish()));
        arrow_array::ListArray::try_new(self.item_field.clone(), offsets, values, validity)
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

    pub(crate) fn try_finish(&mut self) -> Result<LargeListArray, ArrowError> {
        let values = self
            .child
            .try_finish()
            .map_err(|e| ComputeError(e.to_string()))?;
        let offsets: OffsetBuffer<i64> =
            OffsetBuffer::new(self.offsets.iter().copied().collect::<ScalarBuffer<_>>());
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(NullBuffer::new(v.finish()));
        LargeListArray::try_new(self.item_field.clone(), offsets, values, validity)
    }
}

/// Map column builder holding key/value child builders and offsets.
pub(crate) struct MapCol {
    pub(crate) entry_field: FieldRef,
    pub(crate) entry_fields: Fields,
    pub(crate) keys_sorted: bool,
    pub(crate) value_nullable: bool,
    pub(crate) keys: Box<dyn DynColumnBuilder>,
    pub(crate) values: Box<dyn DynColumnBuilder>,
    pub(crate) offsets: Vec<i32>,
    pub(crate) validity: BooleanBufferBuilder,
}

impl MapCol {
    pub(crate) fn new(
        entry_field: FieldRef,
        entry_fields: Fields,
        keys_sorted: bool,
        keys: Box<dyn DynColumnBuilder>,
        values: Box<dyn DynColumnBuilder>,
    ) -> Self {
        let value_nullable = entry_fields
            .get(1)
            .expect("map entries contain values field")
            .is_nullable();
        Self {
            entry_field,
            entry_fields,
            keys_sorted,
            value_nullable,
            keys,
            values,
            offsets: vec![0],
            validity: BooleanBufferBuilder::new(0),
        }
    }

    pub(crate) fn append_null(&mut self) {
        let last = *self.offsets.last().expect("map offsets initialized");
        self.offsets.push(last);
        self.validity.append(false);
    }

    pub(crate) fn append_map(
        &mut self,
        entries: Vec<(DynCell, Option<DynCell>)>,
    ) -> Result<(), DynError> {
        let mut added: i32 = 0;
        for (idx, (key_cell, value_cell)) in entries.into_iter().enumerate() {
            if matches!(key_cell, DynCell::Null) {
                return Err(DynError::Builder {
                    message: format!("map entry {idx} has null key"),
                });
            }
            self.keys
                .append_dyn(key_cell)
                .map_err(|e| annotate_nested_error(e, format!("map key at entry {idx}")))?;
            match value_cell {
                Some(value) => self
                    .values
                    .append_dyn(value)
                    .map_err(|e| annotate_nested_error(e, format!("map value at entry {idx}")))?,
                None => {
                    if !self.value_nullable {
                        return Err(DynError::Builder {
                            message: format!(
                                "map value at entry {idx} is null but the field is non-nullable"
                            ),
                        });
                    }
                    self.values.append_null();
                }
            }
            added = added.checked_add(1).ok_or_else(|| DynError::Builder {
                message: "map entry count exceeded i32::MAX".to_string(),
            })?;
        }
        let last = *self.offsets.last().expect("map offsets initialized");
        self.offsets.push(last + added);
        self.validity.append(true);
        Ok(())
    }

    pub(crate) fn finish_array(&mut self) -> ArrayRef {
        self.try_finish_array()
            .expect("map builder state validated before finish")
    }

    pub(crate) fn try_finish_array(&mut self) -> Result<ArrayRef, DynError> {
        let keys = self.keys.try_finish()?;
        let values = self.values.try_finish()?;
        let entries = arrow_array::StructArray::try_new(
            self.entry_fields.clone(),
            vec![keys, values],
            None,
        )
        .map_err(|e| DynError::Builder {
            message: e.to_string(),
        })?;

        let offsets: OffsetBuffer<i32> = OffsetBuffer::new(
            self.offsets
                .iter()
                .copied()
                .collect::<ScalarBuffer<i32>>(),
        );
        let mut validity_builder = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut validity_builder);
        let validity = Some(NullBuffer::new(validity_builder.finish()));

        let array = MapArray::try_new(
            Arc::clone(&self.entry_field),
            offsets,
            entries,
            validity,
            self.keys_sorted,
        )
        .map_err(|e| DynError::Builder {
            message: e.to_string(),
        })?;
        Ok(Arc::new(array) as ArrayRef)
    }
}

fn annotate_nested_error(err: DynError, context: String) -> DynError {
    match err {
        DynError::Builder { message } => DynError::Builder {
            message: format!("{context}: {message}"),
        },
        DynError::Append { col, message } => DynError::Builder {
            message: format!("{context}: child column {col}: {message}"),
        },
        other => other,
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

    pub(crate) fn try_finish(&mut self) -> Result<FixedSizeListArray, ArrowError> {
        let values = self
            .child
            .try_finish()
            .map_err(|e| ComputeError(e.to_string()))?;
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(NullBuffer::new(v.finish()));
        FixedSizeListArray::try_new(self.item_field.clone(), self.len, values, validity)
    }
}
