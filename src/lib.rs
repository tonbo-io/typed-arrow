#![deny(missing_docs)]
//! arrow-native core: compile-time Arrow schema traits and primitive markers.

pub mod bridge;
pub mod schema;

/// Prelude exporting the most common traits and markers.
pub mod prelude {
    pub use crate::schema::{BuildRows, ColAt, ColumnVisitor, FieldMeta, ForEachCol, Record};
}

// Re-export the derive macro when enabled
#[cfg(feature = "derive")]
pub use arrow_native_derive::Record;

// Public re-exports for convenience
pub use crate::bridge::{
    Dictionary, List, ListNullable, Microsecond, Millisecond, Nanosecond, Second, TimeZoneSpec,
    Timestamp, TimestampTz, Utc,
};
