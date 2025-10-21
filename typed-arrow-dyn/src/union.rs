//! Dynamic dense and sparse union builders.

use std::{cell::RefCell, collections::HashMap, sync::Arc};

use arrow_array::{ArrayRef, UnionArray};
use arrow_buffer::ScalarBuffer;
use arrow_schema::UnionFields;

use crate::{cell::DynCell, dyn_builder::DynColumnBuilder, DynError};

thread_local! {
    static UNION_NULLS: RefCell<HashMap<usize, Vec<usize>>> = RefCell::new(HashMap::new());
}

fn array_key(array: &ArrayRef) -> usize {
    Arc::as_ptr(array) as *const () as usize
}

fn register_union_nulls(array: &ArrayRef, null_rows: Vec<usize>) {
    if null_rows.is_empty() {
        return;
    }
    UNION_NULLS.with(|m| {
        m.borrow_mut().insert(array_key(array), null_rows);
    });
}

pub(crate) fn take_union_nulls(array: &ArrayRef) -> Option<Vec<usize>> {
    UNION_NULLS.with(|m| m.borrow_mut().remove(&array_key(array)))
}

fn clear_union_nulls() {
    UNION_NULLS.with(|m| m.borrow_mut().clear());
}

/// Scope guard ensuring union null metadata is cleared even when validation errors bubble up.
pub(crate) struct NullMaskScope;

impl NullMaskScope {
    #[must_use]
    pub fn new() -> Self {
        clear_union_nulls();
        Self
    }
}

impl Drop for NullMaskScope {
    fn drop(&mut self) {
        clear_union_nulls();
    }
}

/// Dense union column builder.
pub struct DenseUnionCol {
    fields: UnionFields,
    children: Vec<Box<dyn DynColumnBuilder>>,
    type_ids: Vec<i8>,
    offsets: Vec<i32>,
    slots: Vec<i32>,
    tags: Vec<i8>,
    tag_to_index: Vec<Option<usize>>,
    null_index: usize,
    null_tag: i8,
    null_rows: Vec<usize>,
}

impl DenseUnionCol {
    /// Create a dense union builder from `UnionFields` and child builders.
    #[must_use]
    pub fn new(fields: UnionFields, children: Vec<Box<dyn DynColumnBuilder>>) -> Self {
        if fields.len() != children.len() {
            panic!("Union fields and builder count must match");
        }
        if fields.is_empty() {
            panic!("Union must contain at least one variant");
        }

        let mut tags = Vec::with_capacity(fields.len());
        let mut tag_to_index = vec![None; 256];
        let mut first_nullable: Option<usize> = None;

        for (idx, (tag, field)) in fields.iter().enumerate() {
            let pos = tag_slot(tag);
            if tag_to_index[pos].is_some() {
                panic!("Duplicate union type id {}", tag);
            }
            tag_to_index[pos] = Some(idx);
            tags.push(tag);
            if first_nullable.is_none() && field.is_nullable() {
                first_nullable = Some(idx);
            }
        }

        let null_index = first_nullable.unwrap_or(0);
        let null_tag = tags[null_index];

        Self {
            fields,
            children,
            type_ids: Vec::new(),
            offsets: Vec::new(),
            slots: vec![0; tags.len()],
            tags,
            tag_to_index,
            null_index,
            null_tag,
            null_rows: Vec::new(),
        }
    }

    /// Append a union value.
    pub fn append_union(&mut self, tag: i8, value: Option<Box<DynCell>>) -> Result<(), DynError> {
        let idx = match self.tag_to_index[tag_slot(tag)] {
            Some(i) => i,
            None => {
                return Err(DynError::Builder {
                    message: format!("unknown union type id {tag}"),
                })
            }
        };
        let canonical_tag = self.tags[idx];
        if canonical_tag != tag {
            return Err(DynError::Builder {
                message: format!(
                    "type id {tag} does not match union metadata (expected {canonical_tag})"
                ),
            });
        }

        let offset = self.slots[idx];
        let cell = value.map(|boxed| *boxed);
        match cell {
            Some(v) => self.children[idx].append_dyn(v)?,
            None => self.children[idx].append_null(),
        }

        self.type_ids.push(canonical_tag);
        self.offsets.push(offset);
        self.slots[idx] = offset.checked_add(1).ok_or_else(|| DynError::Builder {
            message: "dense union child exceeded i32::MAX length".to_string(),
        })?;
        Ok(())
    }

    /// Append a null encoded using the selected null carrier variant.
    pub fn append_null(&mut self) {
        let idx = self.null_index;
        let offset = self.slots[idx];
        self.children[idx].append_null();
        let row = self.type_ids.len();
        self.type_ids.push(self.null_tag);
        self.offsets.push(offset);
        self.slots[idx] = offset
            .checked_add(1)
            .expect("dense union child offsets exceeded i32::MAX");
        self.null_rows.push(row);
    }

    /// Finish into an `ArrayRef`, panicking if Arrow rejects the buffers.
    pub fn finish_array(&mut self) -> ArrayRef {
        self.try_finish_array()
            .expect("valid dense union builder state")
    }

    /// Try to finish into an `ArrayRef`, returning `DynError` on failure.
    pub fn try_finish_array(&mut self) -> Result<ArrayRef, DynError> {
        let type_ids_vec: Vec<i8> = std::mem::take(&mut self.type_ids);
        let offsets_vec: Vec<i32> = std::mem::take(&mut self.offsets);
        let type_ids: ScalarBuffer<i8> = type_ids_vec.into_iter().collect();
        let offsets: ScalarBuffer<i32> = offsets_vec.into_iter().collect();
        let fields = clone_union_fields(&self.fields);
        let children = self
            .children
            .iter_mut()
            .map(|c| c.try_finish())
            .collect::<Result<Vec<_>, _>>()?;

        let array =
            UnionArray::try_new(fields, type_ids, Some(offsets), children).map_err(|e| {
                DynError::Builder {
                    message: e.to_string(),
                }
            })?;

        let array_ref = Arc::new(array) as ArrayRef;
        let null_rows = std::mem::take(&mut self.null_rows);
        register_union_nulls(&array_ref, null_rows);

        for slot in &mut self.slots {
            *slot = 0;
        }
        Ok(array_ref)
    }
}

/// Sparse union column builder.
pub struct SparseUnionCol {
    fields: UnionFields,
    children: Vec<Box<dyn DynColumnBuilder>>,
    type_ids: Vec<i8>,
    tags: Vec<i8>,
    tag_to_index: Vec<Option<usize>>,
    null_tag: i8,
    len: usize,
    null_rows: Vec<usize>,
}

impl SparseUnionCol {
    /// Create a sparse union builder from `UnionFields` and child builders.
    #[must_use]
    pub fn new(fields: UnionFields, children: Vec<Box<dyn DynColumnBuilder>>) -> Self {
        if fields.len() != children.len() {
            panic!("Union fields and builder count must match");
        }
        if fields.is_empty() {
            panic!("Union must contain at least one variant");
        }

        let mut tags = Vec::with_capacity(fields.len());
        let mut tag_to_index = vec![None; 256];
        let mut first_nullable: Option<usize> = None;

        for (idx, (tag, field)) in fields.iter().enumerate() {
            let pos = tag_slot(tag);
            if tag_to_index[pos].is_some() {
                panic!("Duplicate union type id {}", tag);
            }
            tag_to_index[pos] = Some(idx);
            tags.push(tag);
            if first_nullable.is_none() && field.is_nullable() {
                first_nullable = Some(idx);
            }
        }

        let null_index = first_nullable.unwrap_or(0);
        let null_tag = tags[null_index];

        Self {
            fields,
            children,
            type_ids: Vec::new(),
            tags,
            tag_to_index,
            null_tag,
            len: 0,
            null_rows: Vec::new(),
        }
    }

    /// Append a union value.
    pub fn append_union(&mut self, tag: i8, value: Option<Box<DynCell>>) -> Result<(), DynError> {
        let idx = match self.tag_to_index[tag_slot(tag)] {
            Some(i) => i,
            None => {
                return Err(DynError::Builder {
                    message: format!("unknown union type id {tag}"),
                })
            }
        };
        let canonical_tag = self.tags[idx];
        if canonical_tag != tag {
            return Err(DynError::Builder {
                message: format!(
                    "type id {tag} does not match union metadata (expected {canonical_tag})"
                ),
            });
        }

        let mut cell = value.map(|boxed| *boxed);
        {
            let child = &mut self.children[idx];
            match cell.take() {
                Some(v) => child.append_dyn(v)?,
                None => child.append_null(),
            }
        }

        for (child_idx, child) in self.children.iter_mut().enumerate() {
            if child_idx != idx {
                child.append_null();
            }
        }

        self.type_ids.push(canonical_tag);
        self.len += 1;
        Ok(())
    }

    /// Append a null row.
    pub fn append_null(&mut self) {
        let row = self.len;
        for child in &mut self.children {
            child.append_null();
        }
        self.type_ids.push(self.null_tag);
        self.len += 1;
        self.null_rows.push(row);
    }

    /// Finish into an `ArrayRef`, panicking if Arrow rejects the buffers.
    pub fn finish_array(&mut self) -> ArrayRef {
        self.try_finish_array()
            .expect("valid sparse union builder state")
    }

    /// Try to finish into an `ArrayRef`, returning `DynError` on failure.
    pub fn try_finish_array(&mut self) -> Result<ArrayRef, DynError> {
        let type_ids_vec: Vec<i8> = std::mem::take(&mut self.type_ids);
        let type_ids: ScalarBuffer<i8> = type_ids_vec.into_iter().collect();
        let fields = clone_union_fields(&self.fields);
        let children = self
            .children
            .iter_mut()
            .map(|c| c.try_finish())
            .collect::<Result<Vec<_>, _>>()?;

        let array = UnionArray::try_new(fields, type_ids, None, children).map_err(|e| {
            DynError::Builder {
                message: e.to_string(),
            }
        })?;

        let array_ref = Arc::new(array) as ArrayRef;
        let null_rows = std::mem::take(&mut self.null_rows);
        register_union_nulls(&array_ref, null_rows);

        self.len = 0;
        Ok(array_ref)
    }
}

fn tag_slot(tag: i8) -> usize {
    (i16::from(tag) + 128) as usize
}

fn clone_union_fields(fields: &UnionFields) -> UnionFields {
    fields
        .iter()
        .map(|(tag, field)| (tag, field.clone()))
        .collect()
}
