//! Builders collection for dynamic schema.

use arrow_schema::SchemaRef;

use crate::{dyn_builder::DynColumnBuilder, factory::new_dyn_builder, rows::DynRow, DynError};

/// Dynamic builders collection for a runtime schema.
pub struct DynBuilders {
    schema: SchemaRef,
    cols: Vec<Box<dyn DynColumnBuilder>>,
    len: usize,
}

impl DynBuilders {
    /// Create builders for each field in `schema`.
    pub fn new(schema: SchemaRef, capacity: usize) -> Self {
        let cols = schema
            .fields()
            .iter()
            .map(|f| new_dyn_builder(f.data_type(), f.is_nullable()))
            .collect();
        let _ = capacity; // reserve in concrete builders once implemented
        Self {
            schema,
            cols,
            len: 0,
        }
    }

    /// Append an optional dynamic row.
    pub fn append_option_row(&mut self, row: Option<DynRow>) -> Result<(), DynError> {
        match row {
            None => {
                for (i, c) in self.cols.iter_mut().enumerate() {
                    if !c.is_nullable() {
                        return Err(DynError::Append {
                            col: i,
                            message: "null not allowed for non-nullable column".into(),
                        });
                    }
                    c.append_null();
                }
            }
            Some(r) => {
                r.append_into(&mut self.cols)?;
            }
        }
        self.len += 1;
        Ok(())
    }

    /// Finish and assemble a `RecordBatch`.
    pub fn finish_into_batch(mut self) -> arrow_array::RecordBatch {
        use arrow_array::RecordBatch;
        let arrays: Vec<_> = self.cols.iter_mut().map(|c| c.finish()).collect();
        RecordBatch::try_new(self.schema.clone(), arrays).expect("shape verified")
    }
}
