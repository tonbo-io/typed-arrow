//! Test custom error types wrapped in ViewAccessError::Custom

use arrow_array::{Array, StringArray};
use thiserror::Error;
use typed_arrow::{bridge::ArrowBindingView, schema::ViewAccessError};

/// Custom error type for email validation
#[derive(Debug, Clone, Error, PartialEq)]
pub enum EmailError {
    #[error("invalid email format: {reason}")]
    InvalidFormat { reason: String },

    #[error("email too long: {len} > {max}")]
    TooLong { len: usize, max: usize },

    #[error("email field is null at index {index}")]
    Null { index: usize },
}

/// Email newtype with validation
#[derive(Debug, Clone, PartialEq)]
pub struct Email(String);

impl Email {
    pub fn new(s: impl Into<String>) -> Result<Self, EmailError> {
        let s = s.into();
        if !s.contains('@') {
            return Err(EmailError::InvalidFormat {
                reason: "missing @ symbol".into(),
            });
        }
        if s.len() > 255 {
            return Err(EmailError::TooLong {
                len: s.len(),
                max: 255,
            });
        }
        Ok(Email(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// Implement ArrowBinding for building Email columns
impl typed_arrow::bridge::ArrowBinding for Email {
    type Array = StringArray;
    type Builder = arrow_array::builder::StringBuilder;

    fn data_type() -> arrow_schema::DataType {
        arrow_schema::DataType::Utf8
    }

    fn new_builder(capacity: usize) -> Self::Builder {
        arrow_array::builder::StringBuilder::with_capacity(capacity, 1024)
    }

    fn append_value(b: &mut Self::Builder, v: &Self) {
        b.append_value(&v.0);
    }

    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }

    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

// Implement ArrowBindingView wrapping errors in ViewAccessError::Custom
#[cfg(feature = "views")]
impl ArrowBindingView for Email {
    type Array = StringArray;
    type View<'a> = &'a str;

    fn get_view(array: &Self::Array, index: usize) -> Result<Self::View<'_>, ViewAccessError> {
        if index >= array.len() {
            return Err(ViewAccessError::OutOfBounds {
                index,
                len: array.len(),
                field_name: None,
            });
        }

        if array.is_null(index) {
            return Err(ViewAccessError::Custom(Box::new(EmailError::Null {
                index,
            })));
        }

        let s = array.value(index);

        // Validate email format
        if !s.contains('@') {
            return Err(ViewAccessError::Custom(Box::new(
                EmailError::InvalidFormat {
                    reason: "missing @ symbol".into(),
                },
            )));
        }

        if s.len() > 255 {
            return Err(ViewAccessError::Custom(Box::new(EmailError::TooLong {
                len: s.len(),
                max: 255,
            })));
        }

        Ok(s)
    }
}

#[test]
fn test_custom_error_wrapping_and_downcasting() {
    let mut builder = arrow_array::builder::StringBuilder::new();
    builder.append_value("valid@example.com");
    builder.append_value("invalid-no-at"); // Missing @ - will trigger custom error
    let array = builder.finish();

    // Valid email works
    let result = Email::get_view(&array, 0);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "valid@example.com");

    // Invalid email returns Custom error wrapping EmailError
    let result = Email::get_view(&array, 1);
    assert!(result.is_err());

    match result.unwrap_err() {
        ViewAccessError::Custom(e) => {
            // Can downcast to recover the specific error type
            let email_err = e
                .downcast_ref::<EmailError>()
                .expect("should downcast to EmailError");

            match email_err {
                EmailError::InvalidFormat { reason } => {
                    assert!(reason.contains("missing @ symbol"));
                }
                other => panic!("Expected InvalidFormat, got {other:?}"),
            }
        }
        other => panic!("Expected Custom variant, got {other:?}"),
    }
}
