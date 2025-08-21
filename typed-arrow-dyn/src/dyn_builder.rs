//! Trait for dynamic column builders.

use arrow_array::ArrayRef;
use arrow_schema::DataType;

use crate::{cell::DynCell, DynError};

/// Trait object for a column builder that accepts dynamic cells.
pub trait DynColumnBuilder {
    /// The Arrow logical type this builder produces.
    fn data_type(&self) -> &DataType;

    /// Append a null value.
    fn append_null(&mut self);

    /// Append a dynamic value.
    fn append_dyn(&mut self, v: DynCell) -> Result<(), DynError>;

    /// Finish the builder into an `ArrayRef`.
    fn finish(&mut self) -> ArrayRef;
}
