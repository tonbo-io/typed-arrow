//! Trait for dynamic column builders.

use std::sync::Arc;

use crate::arrow_array::ArrayRef;
use crate::arrow_schema::DataType;

use crate::{DynError, cell::DynCell};

/// Result of finishing a dynamic column builder.
#[derive(Debug)]
pub struct FinishedColumn {
    pub array: ArrayRef,
    /// Metadata describing union arrays encountered in this subtree.
    /// Each entry stores the array pointer (for identity) and the list of
    /// top-level row indices that were appended as `None`.
    pub union_metadata: Vec<(usize, Vec<usize>)>,
}

impl FinishedColumn {
    #[must_use]
    pub fn from_array(array: ArrayRef) -> Self {
        Self {
            array,
            union_metadata: Vec::new(),
        }
    }
}

pub(crate) fn array_key(array: &ArrayRef) -> usize {
    Arc::as_ptr(array) as *const () as usize
}

/// Trait object for a column builder that accepts dynamic cells.
///
/// Marked `Send` so trait objects can be moved across threads without
/// repeating `+ Send` everywhere.
pub trait DynColumnBuilder: Send {
    /// The Arrow logical type this builder produces.
    fn data_type(&self) -> &DataType;

    /// Append a null value.
    fn append_null(&mut self);

    /// Append a dynamic value.
    ///
    /// # Errors
    /// Returns a `DynError` if the value is incompatible with the underlying Arrow type
    /// or if the Arrow builder reports an error while appending.
    fn append_dyn(&mut self, v: DynCell) -> Result<(), DynError>;

    /// Finish the builder into an `ArrayRef`.
    fn finish(&mut self) -> ArrayRef {
        self.try_finish()
            .expect("builder expected to finish without error")
            .array
    }

    /// Fallible finish returning column metadata.
    ///
    /// # Errors
    /// Returns a `DynError` if the builder is in an invalid state.
    fn try_finish(&mut self) -> Result<FinishedColumn, DynError>;
}
