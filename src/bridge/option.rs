//! ArrowBindingView implementation for Option<T>.
//!
//! This module provides view support for nullable types, allowing `Option<T>` to have
//! `View = Option<T::View>` while non-nullable types return errors on null values.

#[cfg(feature = "views")]
use super::ArrowBindingView;

/// Implement ArrowBindingView for Option<T> where T implements ArrowBindingView.
///
/// This allows nullable fields to properly handle null values by returning Ok(None)
/// instead of Err(UnexpectedNull), making the type system enforce correct null handling.
#[cfg(feature = "views")]
impl<T> ArrowBindingView for Option<T>
where
    T: ArrowBindingView,
{
    type Array = T::Array;
    type View<'a>
        = Option<T::View<'a>>
    where
        Self: 'a;

    fn get_view(
        array: &Self::Array,
        index: usize,
    ) -> Result<Self::View<'_>, crate::schema::ViewAccessError> {
        use arrow_array::Array;

        if index >= array.len() {
            return Err(crate::schema::ViewAccessError::OutOfBounds {
                index,
                len: array.len(),
                field_name: None,
            });
        }

        // For nullable types, null is valid data
        if array.is_null(index) {
            return Ok(None);
        }

        // Delegate to the inner type's get_view
        T::get_view(array, index).map(Some)
    }
}
