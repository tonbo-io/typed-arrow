//! Dynamic zero-copy views over Arrow data.
//!
//! This module provides runtime equivalents to the typed `#[derive(Record)]`
//! view APIs. It allows callers to iterate rows of an `arrow_array::RecordBatch`
//! using a runtime schema (`DynSchema`) while retrieving borrowed values
//! (`DynCellRef<'_>`). The implementation mirrors the owned dynamic builders
//! (`DynCell`) so consumers can switch between owned and borrowed access paths.

mod cell;
mod path;
mod projection;
mod raw;
mod rows;
mod views;

pub use cell::{DynCellRaw, DynCellRef};
pub use projection::DynProjection;
pub use rows::{iter_batch_views, view_batch_row, DynRowOwned, DynRowRaw, DynRowView, DynRowViews};
pub use views::{DynFixedSizeListView, DynListView, DynMapView, DynStructView, DynUnionView};

#[cfg(test)]
mod tests;
