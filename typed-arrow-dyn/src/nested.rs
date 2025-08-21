//! Nested dynamic builders used by the factory.

use arrow_buffer::{BooleanBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_schema::FieldRef;

use crate::{cell::DynCell, dyn_builder::DynColumnBuilder, DynError};

/// Nested struct column builder.
pub(crate) struct StructCol {
    pub(crate) fields: arrow_schema::Fields,
    pub(crate) children: Vec<Box<dyn DynColumnBuilder>>, // same len as fields
    pub(crate) validity: BooleanBufferBuilder,
}

impl StructCol {
    pub(crate) fn new_with_children(
        fields: arrow_schema::Fields,
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
        let validity = Some(arrow_buffer::NullBuffer::new(v.finish()));
        arrow_array::StructArray::new(self.fields.clone(), cols, validity)
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
        for it in items.into_iter() {
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
            OffsetBuffer::new(ScalarBuffer::from_iter(self.offsets.iter().copied()));
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(arrow_buffer::NullBuffer::new(v.finish()));
        arrow_array::ListArray::new(self.item_field.clone(), offsets, values, validity)
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
        for it in items.into_iter() {
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
    pub(crate) fn finish(&mut self) -> arrow_array::LargeListArray {
        let values = self.child.finish();
        let offsets: OffsetBuffer<i64> =
            OffsetBuffer::new(ScalarBuffer::from_iter(self.offsets.iter().copied()));
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(arrow_buffer::NullBuffer::new(v.finish()));
        arrow_array::LargeListArray::new(self.item_field.clone(), offsets, values, validity)
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
        if items.len() as i32 != self.len {
            return Err(DynError::Builder {
                message: format!(
                    "fixed-size list length mismatch: expected {}, got {}",
                    self.len,
                    items.len()
                ),
            });
        }
        for it in items.into_iter() {
            match it {
                None => self.child.append_null(),
                Some(v) => self.child.append_dyn(v)?,
            }
        }
        self.validity.append(true);
        Ok(())
    }
    pub(crate) fn finish(&mut self) -> arrow_array::FixedSizeListArray {
        let values = self.child.finish();
        let mut v = BooleanBufferBuilder::new(0);
        std::mem::swap(&mut self.validity, &mut v);
        let validity = Some(arrow_buffer::NullBuffer::new(v.finish()));
        arrow_array::FixedSizeListArray::new(self.item_field.clone(), self.len, values, validity)
    }
}
