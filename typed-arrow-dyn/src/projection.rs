//! Projection masks for nested Arrow schemas.

use arrow_schema::SchemaRef;
use parquet::arrow::ProjectionMask as ParquetProjectionMask;
use thiserror::Error;

/// Errors surfaced while validating projection paths against an Arrow schema.
#[derive(Debug, Error)]
pub enum ProjectionMaskError {
    /// Projection mask referenced an invalid schema path.
    #[error("invalid projection path {path:?}: {message}")]
    InvalidPath {
        /// Path components that failed validation.
        path: Vec<usize>,
        /// Human-readable reason for the failure.
        message: String,
    },
}

/// A set of columns within a potentially nested schema to project.
///
/// Paths are expressed as index sequences traversing the Arrow schema tree. For example, the schema
/// below shows how `[3, 0, 1]` targets the `phones.item.number` field:
///
/// ```text
/// // 0 ─ id: Int64                              [0]
/// // 1 ─ name: Utf8                             [1]
/// // 2 ─ address: Struct
/// //     ├─ 0 ─ street: Utf8                    [2]
/// //     ├─ 1 ─ city: Utf8                      [3]
/// //     └─ 2 ─ zip: Utf8                       [4]
/// // 3 ─ phones: List<Struct>
/// //     └─ 0 ─ item: Struct
/// //         ├─ 0 ─ kind: Utf8                  [5]
/// //         └─ 1 ─ number: Utf8                [6]
/// ```
///
/// When the mask stores `None` internally (see [`ProjectionMask::all`]) it represents the identity
/// projection and keeps every column.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProjectionMask {
    paths: Option<Vec<Vec<usize>>>,
}

impl ProjectionMask {
    /// Creates a mask from the provided index paths.
    ///
    /// Paths are canonicalized by sorting and deduplicating; an empty input yields
    /// [`ProjectionMask::all`].
    pub fn new(mut paths: Vec<Vec<usize>>) -> Self {
        if paths.is_empty() {
            return Self::all();
        }
        paths.sort();
        paths.dedup();
        Self { paths: Some(paths) }
    }

    /// Returns a mask that keeps every column.
    pub fn all() -> Self {
        Self { paths: None }
    }

    /// Returns the underlying index paths when the mask is not the identity projection.
    pub fn paths(&self) -> Option<&[Vec<usize>]> {
        self.paths.as_deref()
    }

    /// Convert this mask into Parquet's flattened [`ProjectionMask`](ParquetProjectionMask).
    ///
    /// When the mask keeps every column this returns `Ok(ParquetProjectionMask::all())`; otherwise
    /// the returned bitmap only marks Parquet leaves that intersect the requested nested paths.
    pub fn to_parquet(
        &self,
        _schema: &SchemaRef,
    ) -> Result<ParquetProjectionMask, ProjectionMaskError> {
        let _ = &self.paths;
        unimplemented!("projection mask conversion is pending refactor");
    }

    /// Build an Arrow schema containing only the columns selected by this mask.
    ///
    /// The projected schema preserves field metadata and ordering for every referenced path and
    /// returns an error if any index is invalid for the provided `SchemaRef`.
    pub fn to_schema(&self, _schema: &SchemaRef) -> Result<SchemaRef, ProjectionMaskError> {
        let _ = &self.paths;
        unimplemented!("projection mask schema projection is pending refactor");
    }

    /// Validate that every path indexes a concrete column (or nested child) in `schema`.
    pub fn validate(&self, _schema: &SchemaRef) -> Result<(), ProjectionMaskError> {
        let _ = &self.paths;
        unimplemented!("projection mask validation is pending refactor");
    }
}
