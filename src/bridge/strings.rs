//! `Utf8`, `LargeUtf8`, and `Utf8View` string bindings.

use arrow_array::{
    Array, LargeStringArray, StringArray, StringViewArray,
    builder::{LargeStringBuilder, StringBuilder, StringViewBuilder},
};
use arrow_schema::DataType;

use super::ArrowBinding;
#[cfg(feature = "views")]
use super::ArrowBindingView;

// Utf8/String
impl ArrowBinding for String {
    type Builder = StringBuilder;
    type Array = StringArray;
    fn data_type() -> DataType {
        DataType::Utf8
    }
    fn new_builder(capacity: usize) -> Self::Builder {
        StringBuilder::with_capacity(capacity, 0)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        b.append_value(v.as_str());
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

#[cfg(feature = "views")]
impl ArrowBindingView for String {
    type Array = StringArray;
    type View<'a> = &'a str;

    fn get_view(
        array: &Self::Array,
        index: usize,
    ) -> Result<Self::View<'_>, crate::schema::ViewAccessError> {
        if index >= array.len() {
            return Err(crate::schema::ViewAccessError::OutOfBounds {
                index,
                len: array.len(),
                field_name: None,
            });
        }
        if array.is_null(index) {
            return Err(crate::schema::ViewAccessError::UnexpectedNull {
                index,
                field_name: None,
            });
        }
        Ok(array.value(index))
    }
}

/// Wrapper denoting Arrow `LargeUtf8` values. Use when individual strings can be
/// extremely large or when 64-bit offsets are preferred.
pub struct LargeUtf8(String);

impl LargeUtf8 {
    /// Construct a new `LargeUtf8` from a `String`.
    #[inline]
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(value)
    }
    /// Return the underlying string slice.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
    /// Consume and return the underlying `String`.
    #[inline]
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<String> for LargeUtf8 {
    /// Convert a `String` into a `LargeUtf8`.
    #[inline]
    fn from(value: String) -> Self {
        Self::new(value)
    }
}
impl From<&str> for LargeUtf8 {
    /// Convert a `&str` into a `LargeUtf8` by allocating a `String`.
    #[inline]
    fn from(s: &str) -> Self {
        Self::new(s.to_string())
    }
}

impl ArrowBinding for LargeUtf8 {
    type Builder = LargeStringBuilder;
    type Array = LargeStringArray;
    fn data_type() -> DataType {
        DataType::LargeUtf8
    }
    fn new_builder(capacity: usize) -> Self::Builder {
        LargeStringBuilder::with_capacity(capacity, 0)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        b.append_value(v.0.as_str());
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

#[cfg(feature = "views")]
impl ArrowBindingView for LargeUtf8 {
    type Array = LargeStringArray;
    type View<'a> = &'a str;

    fn get_view(
        array: &Self::Array,
        index: usize,
    ) -> Result<Self::View<'_>, crate::schema::ViewAccessError> {
        if index >= array.len() {
            return Err(crate::schema::ViewAccessError::OutOfBounds {
                index,
                len: array.len(),
                field_name: None,
            });
        }
        if array.is_null(index) {
            return Err(crate::schema::ViewAccessError::UnexpectedNull {
                index,
                field_name: None,
            });
        }
        Ok(array.value(index))
    }
}

/// Wrapper denoting Arrow `Utf8View` values. Use when you want to take advantage
/// of the more efficient view-based string representation which stores short strings
/// inline and provides better cache locality for string comparisons.
pub struct Utf8View(String);

impl Utf8View {
    /// Construct a new `Utf8View` from a `String`.
    #[inline]
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(value)
    }
    /// Return the underlying string slice.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
    /// Consume and return the underlying `String`.
    #[inline]
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<String> for Utf8View {
    /// Convert a `String` into a `Utf8View`.
    #[inline]
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for Utf8View {
    /// Convert a `&str` into a `Utf8View` by allocating a `String`.
    #[inline]
    fn from(s: &str) -> Self {
        Self::new(s.to_string())
    }
}

impl ArrowBinding for Utf8View {
    type Builder = StringViewBuilder;
    type Array = StringViewArray;
    fn data_type() -> DataType {
        DataType::Utf8View
    }
    fn new_builder(capacity: usize) -> Self::Builder {
        StringViewBuilder::with_capacity(capacity)
    }
    fn append_value(b: &mut Self::Builder, v: &Self) {
        b.append_value(v.0.as_str());
    }
    fn append_null(b: &mut Self::Builder) {
        b.append_null();
    }
    fn finish(mut b: Self::Builder) -> Self::Array {
        b.finish()
    }
}

#[cfg(feature = "views")]
impl ArrowBindingView for Utf8View {
    type Array = StringViewArray;
    type View<'a> = &'a str;

    fn get_view(
        array: &Self::Array,
        index: usize,
    ) -> Result<Self::View<'_>, crate::schema::ViewAccessError> {
        if index >= array.len() {
            return Err(crate::schema::ViewAccessError::OutOfBounds {
                index,
                len: array.len(),
                field_name: None,
            });
        }
        if array.is_null(index) {
            return Err(crate::schema::ViewAccessError::UnexpectedNull {
                index,
                field_name: None,
            });
        }
        Ok(array.value(index))
    }
}
