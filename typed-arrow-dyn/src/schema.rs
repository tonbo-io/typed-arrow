//! Runtime Arrow Schema wrapper.

use std::sync::Arc;

use crate::arrow_array::RecordBatch;
use crate::arrow_schema::{Schema, SchemaRef};

use crate::{DynRowView, DynRowViews, DynViewError};

/// A runtime Arrow schema wrapper used by the unified facade.
#[derive(Debug, Clone)]
pub struct DynSchema {
    /// The underlying `arrow_schema::SchemaRef`.
    pub schema: SchemaRef,
}

impl DynSchema {
    /// Construct from owned `Schema`.
    #[must_use]
    pub fn new(schema: Schema) -> Self {
        Self {
            schema: Arc::new(schema),
        }
    }

    /// Construct from an existing `SchemaRef`.
    #[must_use]
    pub fn from_ref(schema: SchemaRef) -> Self {
        Self { schema }
    }

    /// Create a dynamic row view iterator over `batch`, validating shapes first.
    ///
    /// # Errors
    /// Returns `DynViewError` if the batch schema does not match this schema.
    pub fn iter_views<'a>(
        &'a self,
        batch: &'a RecordBatch,
    ) -> Result<DynRowViews<'a>, DynViewError> {
        crate::view::DynRowViews::new(batch, self.schema.as_ref())
    }

    /// Borrow a single row from `batch` at `row` as a dynamic view.
    ///
    /// # Errors
    /// Returns `DynViewError` if the batch schema mismatches this schema or if the
    /// requested row index is out of bounds.
    pub fn view_at<'a>(
        &'a self,
        batch: &'a RecordBatch,
        row: usize,
    ) -> Result<DynRowView<'a>, DynViewError> {
        crate::view::view_batch_row(self, batch, row)
    }
}
