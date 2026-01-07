//! Error types for dynamic builders and rows.

use arrow_schema::DataType;
use thiserror::Error;

use crate::arrow_schema;

/// Errors that can occur when appending dynamic rows/cells.
#[derive(Debug, Error)]
pub enum DynError {
    /// The number of cells in a row did not match the schema width.
    #[error("row length {got} does not match schema width {expected}")]
    ArityMismatch {
        /// Expected number of columns (schema width).
        expected: usize,
        /// Actual number of cells present in the provided row.
        got: usize,
    },

    /// Post-build nullability violation detected by the validator.
    #[error("nullability violation at column {col} ({path}) index {index}: {message}")]
    Nullability {
        /// Top-level column index where the violation occurred.
        col: usize,
        /// Dot-annotated path to the offending field (e.g., "`struct_field.child`[]").
        path: String,
        /// Row or value index where the violation was found.
        index: usize,
        /// Message describing the violation.
        message: String,
    },

    /// A cell's Rust value did not match the target Arrow `DataType` for a column.
    #[error("type mismatch at column {col}: expected {expected:?}")]
    TypeMismatch {
        /// The zero-based column index where the mismatch occurred.
        col: usize,
        /// The Arrow logical type expected for that column.
        expected: DataType,
    },

    /// The underlying Arrow builder reported an error while appending a value.
    #[error("builder error: {message}")]
    Builder {
        /// Human-readable error from the underlying Arrow builder.
        message: String,
    },

    /// Append failed at a specific column with a message.
    #[error("append error at column {col}: {message}")]
    Append {
        /// The zero-based column index where the builder failed.
        col: usize,
        /// Human-readable error message from the builder.
        message: String,
    },
}

impl DynError {
    /// Add column context to a builder error.
    #[must_use]
    pub fn at_col(self, col: usize) -> DynError {
        match self {
            DynError::Builder { message } => DynError::Append { col, message },
            other => other,
        }
    }
}

/// Errors that can occur when constructing dynamic views over Arrow data.
#[derive(Debug, Error)]
pub enum DynViewError {
    /// Requested row index exceeded the batch length.
    #[error("row index {row} out of bounds for batch length {len}")]
    RowOutOfBounds {
        /// Provided row index.
        row: usize,
        /// Total number of rows in the batch.
        len: usize,
    },

    /// Requested column index exceeded the schema width.
    #[error("column index {column} out of bounds for schema width {width}")]
    ColumnOutOfBounds {
        /// Provided column index.
        column: usize,
        /// Number of columns in the schema.
        width: usize,
    },

    /// Column schema did not match the array data type present in the `RecordBatch`.
    #[error(
        "schema mismatch at column {column} ('{field}'): expected {expected:?}, got {actual:?}"
    )]
    SchemaMismatch {
        /// Column index.
        column: usize,
        /// Column field name.
        field: String,
        /// Expected Arrow data type.
        expected: DataType,
        /// Actual Arrow data type encountered.
        actual: DataType,
    },

    /// Array downcast failed due to an unexpected runtime type.
    #[error("type mismatch at {path}: expected {expected:?}, got {actual:?}")]
    TypeMismatch {
        /// Column index.
        column: usize,
        /// Dot/segment annotated path within the column.
        path: String,
        /// Expected Arrow data type.
        expected: DataType,
        /// Actual Arrow data type encountered.
        actual: DataType,
    },

    /// Encountered a null value where a non-null was required.
    #[error("unexpected null at {path}")]
    UnexpectedNull {
        /// Column index.
        column: usize,
        /// Dot/segment annotated path within the column.
        path: String,
    },

    /// Invalid data encountered while materializing a view.
    #[error("invalid data at {path}: {message}")]
    Invalid {
        /// Column index.
        column: usize,
        /// Dot/segment annotated path within the column.
        path: String,
        /// Explanation of the invalid condition.
        message: String,
    },
}
