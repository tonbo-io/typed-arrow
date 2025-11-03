//! Runtime Arrow Schema wrapper.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};

use crate::{DynRowViews, DynViewError};

/// A runtime Arrow schema wrapper used by the unified facade.
#[derive(Clone)]
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
}
