//! Builders collection for dynamic schema.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::{
    dyn_builder::DynColumnBuilder, factory::new_dyn_builder, rows::DynRow, validate_nullability,
    DynError,
};

/// Dynamic builders collection for a runtime schema.
pub struct DynBuilders {
    schema: SchemaRef,
    cols: Vec<Box<dyn DynColumnBuilder>>,
    len: usize,
}

impl DynBuilders {
    /// Create builders for each field in `schema`.
    #[must_use]
    pub fn new(schema: SchemaRef, capacity: usize) -> Self {
        let cols = schema
            .fields()
            .iter()
            .map(|f| new_dyn_builder(f.data_type()))
            .collect();
        let _ = capacity; // reserve in concrete builders once implemented
        Self {
            schema,
            cols,
            len: 0,
        }
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
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let schema = self.schema.clone();

        // Finish arrays, catching panics from Arrow array constructors (e.g., nested nullability).
        let arrays = catch_unwind(AssertUnwindSafe(|| {
            self.cols.iter_mut().map(|c| c.finish()).collect::<Vec<_>>()
        }))
        .map_err(|p| DynError::Builder {
            message: panic_message(&p),
        })?;

        // Validate nullability using the schema before constructing the RecordBatch.
        validate_nullability(&schema, &arrays)?;

        // Build RecordBatch, catching potential panics and mapping to errors.
        let rb = catch_unwind(AssertUnwindSafe(|| RecordBatch::try_new(schema, arrays)))
            .map_err(|p| DynError::Builder {
                message: panic_message(&p),
            })
            .and_then(|r| {
                r.map_err(|e| DynError::Builder {
                    message: e.to_string(),
                })
            })?;

        Ok(rb)
    }
}

fn panic_message(p: &(dyn std::any::Any + Send + 'static)) -> String {
    if let Some(s) = p.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic during array/batch construction".to_string()
    }
}
