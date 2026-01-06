//! Builders collection for dynamic schema.

use crate::arrow_array as arrow_array;
use crate::arrow_schema as arrow_schema;

use std::collections::HashMap;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::{
    DynError, dyn_builder::DynColumnBuilder, factory::try_new_dyn_builder_with_capacity,
    rows::DynRow, validate_nullability,
};

/// Dynamic builders collection for a runtime schema.
pub struct DynBuilders {
    schema: SchemaRef,
    cols: Vec<Box<dyn DynColumnBuilder>>,
    len: usize,
}

impl DynBuilders {
    /// Create builders for each field in `schema` with the given capacity hint.
    ///
    /// # Errors
    /// Returns `DynError::Builder` if the schema contains invalid union definitions
    /// or unsupported data types.
    pub fn try_new(schema: SchemaRef, capacity: usize) -> Result<Self, DynError> {
        let cols: Result<Vec<_>, _> = schema
            .fields()
            .iter()
            .map(|f| try_new_dyn_builder_with_capacity(f.data_type(), capacity))
            .collect();
        Ok(Self {
            schema,
            cols: cols?,
            len: 0,
        })
    }

    /// Create builders for each field in `schema` with the given capacity hint.
    ///
    /// # Panics
    /// Panics if the schema contains invalid union definitions or unsupported data types.
    /// Prefer [`try_new`](Self::try_new) for fallible construction.
    #[must_use]
    pub fn new(schema: SchemaRef, capacity: usize) -> Self {
        Self::try_new(schema, capacity).expect("valid schema for builder construction")
    }

    /// Append an optional dynamic row.
    ///
    /// # Errors
    /// Returns
    /// - `DynError::ArityMismatch` when row width differs from schema.
    /// - `DynError::TypeMismatch` or `DynError::Append` on builder/type issues.
    pub fn append_option_row(&mut self, row: Option<DynRow>) -> Result<(), DynError> {
        match row {
            None => {
                for c in &mut self.cols {
                    c.append_null();
                }
            }
            Some(r) => {
                let fields = self.schema.fields();
                r.append_into_with_fields(fields, &mut self.cols)?;
            }
        }
        self.len += 1;
        Ok(())
    }

    /// Finish and assemble a `RecordBatch`.
    ///
    /// # Panics
    /// Panics if Arrow rejects the arrays when assembling the `RecordBatch`.
    #[must_use]
    pub fn finish_into_batch(mut self) -> RecordBatch {
        let arrays: Vec<_> = self.cols.iter_mut().map(|c| c.finish()).collect();
        RecordBatch::try_new(self.schema.clone(), arrays).expect("shape verified")
    }

    /// Finish building a batch, returning a `DynError` if nullability is violated.
    ///
    /// # Errors
    /// Returns a `DynError` for nullability violations or Arrow construction failures.
    pub fn try_finish_into_batch(mut self) -> Result<RecordBatch, DynError> {
        let schema = self.schema.clone();
        let mut arrays = Vec::with_capacity(self.cols.len());
        let mut union_null_rows: HashMap<usize, Vec<usize>> = HashMap::new();
        for col in &mut self.cols {
            let mut finished = col.try_finish()?;
            for (key, mut rows) in finished.union_metadata.drain(..) {
                union_null_rows.entry(key).or_default().append(&mut rows);
            }
            arrays.push(finished.array);
        }
        // Validate nullability using the schema before constructing the RecordBatch.
        validate_nullability(&schema, &arrays, &union_null_rows)?;
        // Build RecordBatch using fallible constructor.
        let rb = RecordBatch::try_new(schema, arrays).map_err(|e| DynError::Builder {
            message: e.to_string(),
        })?;
        Ok(rb)
    }
}
