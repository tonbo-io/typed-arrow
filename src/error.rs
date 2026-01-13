//! Error types for typed-arrow.

use arrow_schema::DataType;
use thiserror::Error;

/// Error type for schema validation failures.
#[derive(Debug, Clone, Error)]
pub enum SchemaError {
    /// Type mismatch between expected and actual schema
    #[error("schema type mismatch: expected {expected}, got {actual}")]
    TypeMismatch {
        /// Expected Arrow DataType
        expected: DataType,
        /// Actual Arrow DataType
        actual: DataType,
    },
    /// Missing required field
    #[error("missing required field: {field_name}")]
    MissingField {
        /// Name of the missing field
        field_name: String,
    },
    /// Invalid schema configuration
    #[error("invalid schema: {message}")]
    InvalidSchema {
        /// Error message
        message: String,
    },
}

impl SchemaError {
    /// Create a type mismatch error
    pub fn type_mismatch(expected: DataType, actual: DataType) -> Self {
        Self::TypeMismatch { expected, actual }
    }

    /// Create a missing field error
    pub fn missing_field(field_name: impl Into<String>) -> Self {
        Self::MissingField {
            field_name: field_name.into(),
        }
    }

    /// Create an invalid schema error
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::InvalidSchema {
            message: message.into(),
        }
    }
}

/// Error type for view access failures when reading from Arrow arrays.
#[cfg(feature = "views")]
#[derive(Debug, Error)]
pub enum ViewAccessError {
    /// Index out of bounds
    #[error("index {index} out of bounds (len {len}){}", field_name.map(|n| format!(" for field '{n}'")).unwrap_or_default())]
    OutOfBounds {
        /// The invalid index
        index: usize,
        /// The array length
        len: usize,
        /// Optional field name for context
        field_name: Option<&'static str>,
    },
    /// Unexpected null value
    #[error("unexpected null at index {index}{}", field_name.map(|n| format!(" for field '{n}'")).unwrap_or_default())]
    UnexpectedNull {
        /// The index where null was found
        index: usize,
        /// Optional field name for context
        field_name: Option<&'static str>,
    },
    /// Type mismatch during array downcast
    #[error("type mismatch: expected {expected}, got {actual}{}", field_name.map(|n| format!(" for field '{n}'")).unwrap_or_default())]
    TypeMismatch {
        /// Expected Arrow DataType
        expected: DataType,
        /// Actual Arrow DataType
        actual: DataType,
        /// Optional field name for context
        field_name: Option<&'static str>,
    },
    /// Custom user-defined error from domain-specific validation
    ///
    /// This variant allows custom types (newtypes) to wrap their own error types
    /// while still using the common `ViewAccessError` type. The error can be
    /// downcast to the specific type when needed.
    #[error("custom validation error: {0}")]
    Custom(Box<dyn std::error::Error + Send + Sync + 'static>),
}

/// Allows generic code to uniformly handle both infallible and fallible view-to-owned conversions.
///
/// When converting views to owned types, primitives and `String` never fail (`TryFrom<Primitive,
/// Error = Infallible>`), while nested structs can fail (`TryFrom<StructView, Error =
/// ViewAccessError>`). This blanket conversion allows generic code like `List<T>` or `Map<K, V>` to
/// use a single implementation with `E: Into<ViewAccessError>` bounds that works for both cases.
///
/// The empty match is safe because `Infallible` is an uninhabited type that can never be
/// constructed.
#[cfg(feature = "views")]
impl From<core::convert::Infallible> for ViewAccessError {
    fn from(x: core::convert::Infallible) -> Self {
        match x {}
    }
}

/// Conversion from `TryFromSliceError` to allow fixed-size arrays like `[u8; N]` to work with
/// generic List/Map implementations. This error occurs when converting `&[T]` to `[T; N]` and
/// the slice length doesn't match the array length.
#[cfg(feature = "views")]
impl From<std::array::TryFromSliceError> for ViewAccessError {
    fn from(e: std::array::TryFromSliceError) -> Self {
        ViewAccessError::Custom(Box::new(e))
    }
}
