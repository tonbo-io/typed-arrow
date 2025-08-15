#![deny(missing_docs)]
//! typed-arrow core: compile-time Arrow schema traits and primitive markers.

pub mod bridge;
pub mod schema;

/// Prelude exporting the most common traits and markers.
pub mod prelude {
    pub use crate::schema::{BuildRows, ColAt, ColumnVisitor, FieldMeta, ForEachCol, Record};
}

// Re-export the derive macro when enabled
#[cfg(feature = "derive")]
pub use typed_arrow_derive::{Record, Union};

// Public re-exports for convenience
pub use crate::bridge::{
    Date32, Date64, Decimal128, Decimal256, Dictionary, Duration, FixedSizeList,
    FixedSizeListNullable, IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth, LargeBinary,
    LargeList, LargeUtf8, List, Map, Microsecond, Millisecond, Nanosecond, Null, OrderedMap,
    Second, Time32, Time64, TimeZoneSpec, Timestamp, TimestampTz, Utc,
};
