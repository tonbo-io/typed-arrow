//! Trait for dynamic column builders.

use arrow_array::ArrayRef;
use arrow_schema::DataType;

use crate::{cell::DynCell, DynError};

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
    fn finish(&mut self) -> ArrayRef;

    /// Fallible finish into an `ArrayRef` when construction may fail.
    ///
    /// Default implementation delegates to `finish()` for builder types
    /// that are infallible at construction time.
    ///
    /// # Errors
    /// Returns a `DynError` if the builder is in an invalid state.
    fn try_finish(&mut self) -> Result<ArrayRef, DynError> {
        Ok(self.finish())
    }
}
