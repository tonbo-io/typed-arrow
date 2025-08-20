//! Runtime Arrow Schema wrapper.

use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};

/// A runtime Arrow schema wrapper used by the unified facade.
#[derive(Clone)]
pub struct DynSchema {
    /// The underlying `arrow_schema::SchemaRef`.
    pub schema: SchemaRef,
}

impl DynSchema {
    /// Construct from owned `Schema`.
    pub fn new(schema: Schema) -> Self {
        Self {
            schema: Arc::new(schema),
        }
    }

    /// Construct from an existing `SchemaRef`.
    pub fn from_ref(schema: SchemaRef) -> Self {
        Self { schema }
    }
}
