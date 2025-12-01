use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Fields, Schema};

use super::{
    cell::{DynCellRaw, DynCellRef, view_cell_with_projector},
    path::Path,
    projection::{DynProjection, FieldProjector},
};
use crate::{DynViewError, cell::DynCell, rows::DynRow, schema::DynSchema};

/// Iterator over borrowed dynamic rows.
#[derive(Debug)]
pub struct DynRowViews<'a> {
    batch: &'a RecordBatch,
    fields: Fields,
    mapping: Option<Arc<[usize]>>,
    projectors: Option<Arc<[FieldProjector]>>,
    row: usize,
    len: usize,
}

impl<'a> DynRowViews<'a> {
    /// Create a dynamic view iterator from a record batch after validating schema compatibility.
    pub fn new(batch: &'a RecordBatch, schema: &'a Schema) -> Result<Self, DynViewError> {
        validate_schema_matches(batch, schema)?;
        Ok(Self {
            batch,
            fields: schema.fields().clone(),
            mapping: None,
            projectors: None,
            row: 0,
            len: batch.num_rows(),
        })
    }

    /// Borrow the underlying schema fields.
    #[inline]
    pub fn fields(&self) -> &Fields {
        &self.fields
    }

    /// Apply a top-level projection to this iterator, yielding views that expose only the mapped
    /// columns.
    ///
    /// The projection is lazy: rows are fetched on demand from the underlying iterator, and only
    /// the referenced columns are materialized.
    ///
    /// # Errors
    /// Returns `DynViewError::Invalid` if the projection was derived from a schema with a different
    /// width than this iterator.
    pub fn project(self, projection: DynProjection) -> Result<Self, DynViewError> {
        let DynRowViews {
            batch,
            fields,
            mapping,
            projectors,
            row,
            len,
        } = self;

        let base_view = DynRowView {
            batch,
            fields,
            mapping,
            projectors,
            row,
        };

        let projected_view = base_view.project(&projection)?;
        let DynRowView {
            batch,
            fields,
            mapping,
            projectors,
            row,
        } = projected_view;

        Ok(Self {
            batch,
            fields,
            mapping,
            projectors,
            row,
            len,
        })
    }
}

impl<'a> Iterator for DynRowViews<'a> {
    type Item = Result<DynRowView<'a>, DynViewError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.len {
            return None;
        }
        let view = DynRowView {
            batch: self.batch,
            fields: self.fields.clone(),
            mapping: self.mapping.clone(),
            projectors: self.projectors.clone(),
            row: self.row,
        };
        self.row += 1;
        Some(Ok(view))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len.saturating_sub(self.row);
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for DynRowViews<'a> {}

/// Borrowed dynamic row backed by an `arrow_array::RecordBatch`.
#[derive(Debug)]
pub struct DynRowView<'a> {
    batch: &'a RecordBatch,
    fields: Fields,
    mapping: Option<Arc<[usize]>>,
    projectors: Option<Arc<[FieldProjector]>>,
    row: usize,
}

impl<'a> DynRowView<'a> {
    #[cfg(test)]
    pub(super) fn new_for_testing(
        batch: &'a RecordBatch,
        fields: Fields,
        mapping: Option<Arc<[usize]>>,
        projectors: Option<Arc<[FieldProjector]>>,
        row: usize,
    ) -> Self {
        Self {
            batch,
            fields,
            mapping,
            projectors,
            row,
        }
    }

    /// Number of columns in this row.
    #[inline]
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns true when the row has zero columns.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrow the schema fields.
    #[inline]
    pub fn fields(&self) -> &Fields {
        &self.fields
    }

    /// Retrieve the cell at `column` as a borrowed [`DynCellRef`].
    pub fn get(&self, column: usize) -> Result<Option<DynCellRef<'_>>, DynViewError> {
        if self.row >= self.batch.num_rows() {
            return Err(DynViewError::RowOutOfBounds {
                row: self.row,
                len: self.batch.num_rows(),
            });
        }
        let width = self.fields.len();
        let field = self
            .fields
            .get(column)
            .ok_or(DynViewError::ColumnOutOfBounds { column, width })?;
        let source_index = match &self.mapping {
            Some(mapping) => mapping[column],
            None => column,
        };
        if source_index >= self.batch.num_columns() {
            return Err(DynViewError::Invalid {
                column,
                path: self
                    .fields
                    .get(column)
                    .map(|f| f.name().to_string())
                    .unwrap_or_else(|| "<unknown>".to_string()),
                message: format!(
                    "projection index {source_index} exceeds batch width {}",
                    self.batch.num_columns()
                ),
            });
        }
        let array = self.batch.column(source_index);
        let path = Path::new(column, field.name());
        let projector = match self.projectors.as_ref() {
            Some(projectors) => {
                Some(
                    projectors
                        .get(column)
                        .ok_or_else(|| DynViewError::Invalid {
                            column,
                            path: field.name().to_string(),
                            message: "projection width mismatch".to_string(),
                        })?,
                )
            }
            None => None,
        };
        view_cell_with_projector(&path, field.as_ref(), projector, array.as_ref(), self.row)
    }

    /// Retrieve a column by name, returning `None` if the field does not exist.
    pub fn get_by_name(&self, name: &str) -> Option<Result<Option<DynCellRef<'_>>, DynViewError>> {
        self.fields
            .iter()
            .position(|f| f.name() == name)
            .map(move |idx| self.get(idx))
    }

    /// Clone this row into an owned [`DynRow`], allocating owned dynamic cells for each column.
    pub fn to_owned(&self) -> Result<DynRow, DynViewError> {
        let width = self.len();
        let mut cells = Vec::with_capacity(width);
        for idx in 0..width {
            let value = self.get(idx)?;
            let owned = match value {
                None => None,
                Some(cell) => Some(cell.into_owned()?),
            };
            cells.push(owned);
        }
        Ok(DynRow(cells))
    }

    /// Consume this row view and capture its values as lifetime-erased [`DynCellRaw`] entries.
    pub fn into_raw(self) -> Result<DynRowRaw, DynViewError> {
        let fields = self.fields.clone();
        let mut cells = Vec::with_capacity(fields.len());
        for idx in 0..fields.len() {
            let value = self.get(idx)?;
            cells.push(value.map(DynCellRef::into_raw));
        }
        Ok(DynRowRaw { fields, cells })
    }

    /// Apply a projection to this view, yielding a new view that references only the mapped
    /// columns.
    ///
    /// The projection is lazy and reuses the underlying batch buffers.
    ///
    /// # Errors
    /// Returns `DynViewError::Invalid` if the projection was derived from a schema whose width
    /// differs from the underlying batch.
    pub fn project(self, projection: &DynProjection) -> Result<DynRowView<'a>, DynViewError> {
        if projection.source_width() != self.batch.num_columns() {
            return Err(DynViewError::Invalid {
                column: 0,
                path: "<projection>".to_string(),
                message: format!(
                    "projection source width {} does not match batch width {}",
                    projection.source_width(),
                    self.batch.num_columns()
                ),
            });
        }
        Ok(DynRowView {
            batch: self.batch,
            fields: projection.fields().clone(),
            mapping: Some(projection.mapping_arc()),
            projectors: Some(projection.projectors_arc()),
            row: self.row,
        })
    }

    /// Access the underlying row index.
    pub fn row_index(&self) -> usize {
        self.row
    }
}

/// Lifetime-erased dynamic row produced by [`DynRowView::into_raw`].
#[derive(Clone, Debug)]
pub struct DynRowRaw {
    fields: Fields,
    cells: Vec<Option<DynCellRaw>>,
}

// Safety: this type is a lightweight handle over raw cells and schema metadata. The same lifetime
// caveats as `DynCellRaw` apply: callers must ensure the backing Arrow data outlives any moved
// `DynRowRaw` instances.
unsafe impl Send for DynRowRaw {}
unsafe impl Sync for DynRowRaw {}

fn validate_row_width(fields: &Fields, cells_len: usize) -> Result<(), DynViewError> {
    if fields.len() != cells_len {
        let column = fields.len().min(cells_len);
        return Err(DynViewError::Invalid {
            column,
            path: "<row>".to_string(),
            message: format!(
                "field count {} does not match cell count {}",
                fields.len(),
                cells_len
            ),
        });
    }
    Ok(())
}

fn validate_field_shape(
    column: usize,
    field_name: &str,
    expected_type: &DataType,
    expected_nullable: bool,
    actual: &Field,
) -> Result<(), DynViewError> {
    if actual.data_type() != expected_type {
        return Err(DynViewError::SchemaMismatch {
            column,
            field: field_name.to_string(),
            expected: expected_type.clone(),
            actual: actual.data_type().clone(),
        });
    }
    if actual.is_nullable() != expected_nullable {
        return Err(DynViewError::Invalid {
            column,
            path: field_name.to_string(),
            message: format!(
                "nullability mismatch: expected {}, got {}",
                expected_nullable,
                actual.is_nullable()
            ),
        });
    }
    Ok(())
}

impl DynRowRaw {
    /// Construct a raw row from explicit schema fields and raw cells.
    ///
    /// # Errors
    /// Returns [`DynViewError::Invalid`] when the number of cells does not match
    /// the number of fields in the provided schema slice.
    pub fn try_new(fields: Fields, cells: Vec<Option<DynCellRaw>>) -> Result<Self, DynViewError> {
        validate_row_width(&fields, cells.len())?;
        Ok(Self { fields, cells })
    }

    /// Construct a raw row from non-null cells.
    ///
    /// # Errors
    /// Returns [`DynViewError::Invalid`] when the number of cells does not match the schema.
    pub fn from_cells(fields: Fields, cells: Vec<DynCellRaw>) -> Result<Self, DynViewError> {
        let wrapped = cells.into_iter().map(Some).collect();
        Self::try_new(fields, wrapped)
    }

    /// Number of columns carried by this raw row.
    #[inline]
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Returns true when the row has zero columns.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// Borrow the schema fields associated with this row.
    #[inline]
    pub fn fields(&self) -> &Fields {
        &self.fields
    }

    /// Borrow the raw cell payloads.
    #[inline]
    pub fn cells(&self) -> &[Option<DynCellRaw>] {
        &self.cells
    }

    /// Consume the raw row, yielding the underlying raw cells.
    #[inline]
    pub fn into_cells(self) -> Vec<Option<DynCellRaw>> {
        self.cells
    }

    /// Convert this raw row into an owned [`DynRow`], cloning nested data as needed.
    pub fn into_owned(self) -> Result<DynRow, DynViewError> {
        let mut cells = Vec::with_capacity(self.cells.len());
        for cell in self.cells {
            let owned = match cell {
                None => None,
                Some(raw) => Some(raw.into_owned()?),
            };
            cells.push(owned);
        }
        Ok(DynRow(cells))
    }

    /// Clone this raw row into an owned [`DynRow`] without consuming the raw payloads.
    pub fn to_owned(&self) -> Result<DynRow, DynViewError> {
        self.clone().into_owned()
    }
}

/// Owned dynamic row that retains schema metadata alongside owned cell payloads.
#[derive(Clone, Debug)]
pub struct DynRowOwned {
    fields: Fields,
    cells: Vec<Option<DynCell>>,
}

impl DynRowOwned {
    /// Construct an owned row from explicit schema fields and owned cells.
    ///
    /// # Errors
    /// Returns [`DynViewError::Invalid`] when the number of cells does not match the schema.
    pub fn try_new(fields: Fields, cells: Vec<Option<DynCell>>) -> Result<Self, DynViewError> {
        validate_row_width(&fields, cells.len())?;
        Ok(Self { fields, cells })
    }

    /// Construct an owned row from a [`DynRow`].
    pub fn from_dyn_row(fields: Fields, row: DynRow) -> Result<Self, DynViewError> {
        Self::try_new(fields, row.0)
    }

    /// Clone the lifetime-erased raw row into an owned representation.
    pub fn from_raw(raw: &DynRowRaw) -> Result<Self, DynViewError> {
        let owned = raw.to_owned()?;
        Self::from_dyn_row(raw.fields().clone(), owned)
    }

    /// Borrow the schema fields associated with this row.
    #[inline]
    pub fn fields(&self) -> &Fields {
        &self.fields
    }

    /// Borrow the owned cell payloads.
    #[inline]
    pub fn cells(&self) -> &[Option<DynCell>] {
        &self.cells
    }

    /// Number of columns carried by this row.
    #[inline]
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Returns true when the row has zero columns.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// Borrow this owned row as a lifetime-erased raw row referencing the owned buffers.
    pub fn as_raw(&self) -> Result<DynRowRaw, DynViewError> {
        let mut raw_cells = Vec::with_capacity(self.cells.len());
        for (idx, cell) in self.cells.iter().enumerate() {
            match cell {
                None => raw_cells.push(None),
                Some(value) => {
                    let raw =
                        owned_cell_to_raw(value).map_err(|message| DynViewError::Invalid {
                            column: idx,
                            path: self
                                .fields
                                .get(idx)
                                .map(|f| f.name().to_string())
                                .unwrap_or_else(|| format!("col{idx}")),
                            message,
                        })?;
                    raw_cells.push(Some(raw));
                }
            }
        }
        DynRowRaw::try_new(self.fields.clone(), raw_cells)
    }

    /// Consume this owned row, yielding the underlying dynamic row cells.
    pub fn into_dyn_row(self) -> DynRow {
        DynRow(self.cells)
    }

    /// Clone this owned row into a [`DynRow`].
    pub fn to_dyn_row(&self) -> DynRow {
        DynRow(self.cells.clone())
    }

    /// Decompose the owned row into its schema fields and owned cells.
    pub fn into_parts(self) -> (Fields, Vec<Option<DynCell>>) {
        (self.fields, self.cells)
    }
}

fn owned_cell_to_raw(cell: &DynCell) -> Result<DynCellRaw, String> {
    use DynCell::*;
    match cell {
        Null => Ok(DynCellRaw::Null),
        Bool(v) => Ok(DynCellRaw::Bool(*v)),
        I8(v) => Ok(DynCellRaw::I8(*v)),
        I16(v) => Ok(DynCellRaw::I16(*v)),
        I32(v) => Ok(DynCellRaw::I32(*v)),
        I64(v) => Ok(DynCellRaw::I64(*v)),
        U8(v) => Ok(DynCellRaw::U8(*v)),
        U16(v) => Ok(DynCellRaw::U16(*v)),
        U32(v) => Ok(DynCellRaw::U32(*v)),
        U64(v) => Ok(DynCellRaw::U64(*v)),
        F32(v) => Ok(DynCellRaw::F32(*v)),
        F64(v) => Ok(DynCellRaw::F64(*v)),
        Str(value) => Ok(DynCellRaw::from_str(value)),
        Bin(value) => Ok(DynCellRaw::from_bin(value)),
        Struct(_) => Err("struct key component not supported".to_string()),
        List(_) => Err("list key component not supported".to_string()),
        FixedSizeList(_) => Err("fixed-size list key component not supported".to_string()),
        Map(_) => Err("map key component not supported".to_string()),
        Union { .. } => Err("union key component not supported".to_string()),
    }
}

fn validate_schema_matches(batch: &RecordBatch, schema: &Schema) -> Result<(), DynViewError> {
    let batch_schema = batch.schema();
    let batch_fields = batch_schema.fields();
    let expected = schema.fields();
    if batch_fields.len() != expected.len() {
        return Err(DynViewError::Invalid {
            column: expected.len().min(batch_fields.len()),
            path: "<schema>".to_string(),
            message: format!(
                "column count mismatch: schema has {}, batch has {}",
                expected.len(),
                batch_fields.len()
            ),
        });
    }

    for (idx, (expected_field, actual_field)) in
        expected.iter().zip(batch_fields.iter()).enumerate()
    {
        if expected_field.name() != actual_field.name() {
            return Err(DynViewError::Invalid {
                column: idx,
                path: expected_field.name().to_string(),
                message: format!(
                    "field name mismatch: expected '{}', got '{}'",
                    expected_field.name(),
                    actual_field.name()
                ),
            });
        }
        validate_field_shape(
            idx,
            expected_field.name(),
            expected_field.data_type(),
            expected_field.is_nullable(),
            actual_field.as_ref(),
        )?;
    }

    Ok(())
}

/// Create dynamic views for a batch using the provided schema reference.
pub fn iter_batch_views<'a>(
    schema: &'a DynSchema,
    batch: &'a RecordBatch,
) -> Result<DynRowViews<'a>, DynViewError> {
    DynRowViews::new(batch, schema.schema.as_ref())
}

/// Borrow a single row from `batch` as a dynamic view after schema validation.
pub fn view_batch_row<'a>(
    schema: &'a DynSchema,
    batch: &'a RecordBatch,
    row: usize,
) -> Result<DynRowView<'a>, DynViewError> {
    validate_schema_matches(batch, schema.schema.as_ref())?;
    let len = batch.num_rows();
    if row >= len {
        return Err(DynViewError::RowOutOfBounds { row, len });
    }
    Ok(DynRowView {
        batch,
        fields: schema.schema.fields().clone(),
        mapping: None,
        projectors: None,
        row,
    })
}
