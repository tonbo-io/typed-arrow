//! Dynamic row wrapper.

use arrow_schema::DataType;

use crate::{cell::DynCell, dyn_builder::DynColumnBuilder, DynError};

/// A thin row wrapper used to append into a set of dynamic column builders.
pub struct DynRow(pub Vec<Option<DynCell>>);

impl DynRow {
    /// Append this row into the builders (1:1 by index).
    /// Returns an error if the number of cells does not match the number of
    /// columns, or if any cell fails type validation for the target column.
    ///
    /// # Errors
    /// Returns a `DynError` for arity mismatches or type/builder errors while appending.
    pub fn append_into(self, cols: &mut [Box<dyn DynColumnBuilder>]) -> Result<(), DynError> {
        // 1) Validate arity
        if self.0.len() != cols.len() {
            return Err(DynError::ArityMismatch {
                expected: cols.len(),
                got: self.0.len(),
            });
        }

        // 2) Lightweight pre-validation to avoid partial writes when possible.
        // Only validate type compatibility here; Arrow enforces nullability at finish.
        for (i, (cell_opt, b)) in self.0.iter().zip(cols.iter()).enumerate() {
            match cell_opt {
                None => {}
                Some(cell) => {
                    let dt = b.data_type();
                    if !accepts_cell(dt, cell) {
                        return Err(DynError::TypeMismatch {
                            col: i,
                            expected: dt.clone(),
                        });
                    }
                }
            }
        }

        // 3) Perform the actual appends
        let mut cells = self.0.into_iter();
        for (i, b) in cols.iter_mut().enumerate() {
            match cells.next() {
                // End of iterator should be impossible due to arity check
                None => unreachable!("cells length pre-checked to match columns"),
                Some(None) => b.append_null(),
                Some(Some(v)) => {
                    b.append_dyn(v).map_err(|e| e.at_col(i))?;
                }
            }
        }
        Ok(())
    }

    /// Append this row into the builders using field metadata to enrich errors.
    ///
    /// Use this from `DynBuilders` so type mismatches can report column names
    /// and expected vs found types.
    ///
    /// # Errors
    /// Returns a `DynError` for arity mismatches or type/builder errors while appending.
    pub fn append_into_with_fields(
        self,
        fields: &arrow_schema::Fields,
        cols: &mut [Box<dyn DynColumnBuilder>],
    ) -> Result<(), DynError> {
        // 1) Validate arity
        if self.0.len() != cols.len() {
            return Err(DynError::ArityMismatch {
                expected: cols.len(),
                got: self.0.len(),
            });
        }

        // 2) Pre-validate types to avoid partial writes
        for (i, (cell_opt, b)) in self.0.iter().zip(cols.iter()).enumerate() {
            match cell_opt {
                None => {}
                Some(cell) => {
                    let dt = b.data_type();
                    if !accepts_cell(dt, cell) {
                        let name = fields.get(i).map_or("?", |f| f.name().as_str());
                        return Err(DynError::Append {
                            col: i,
                            message: format!(
                                "type mismatch at column '{}' expected {:?}, found {}",
                                name,
                                dt,
                                cell.type_name()
                            ),
                        });
                    }
                }
            }
        }

        // 3) Perform the actual appends
        let mut cells = self.0.into_iter();
        for (i, b) in cols.iter_mut().enumerate() {
            match cells.next() {
                None => unreachable!("cells length pre-checked to match columns"),
                Some(None) => b.append_null(),
                Some(Some(v)) => {
                    b.append_dyn(v).map_err(|e| e.at_col(i))?;
                }
            }
        }
        Ok(())
    }
}

#[allow(clippy::match_same_arms)]
fn accepts_cell(dt: &DataType, cell: &DynCell) -> bool {
    match (dt, cell) {
        (_, DynCell::Null) => true,
        (DataType::Boolean, DynCell::Bool(_)) => true,
        (DataType::Int8, DynCell::I8(_)) => true,
        (DataType::Int16, DynCell::I16(_)) => true,
        (DataType::Int32, DynCell::I32(_)) => true,
        (DataType::Int64, DynCell::I64(_)) => true,
        (DataType::UInt8, DynCell::U8(_)) => true,
        (DataType::UInt16, DynCell::U16(_)) => true,
        (DataType::UInt32, DynCell::U32(_)) => true,
        (DataType::UInt64, DynCell::U64(_)) => true,
        (DataType::Float32, DynCell::F32(_)) => true,
        (DataType::Float64, DynCell::F64(_)) => true,
        (DataType::Date32, DynCell::I32(_)) => true,
        (DataType::Date64, DynCell::I64(_)) => true,
        (DataType::Timestamp(_, _), DynCell::I64(_)) => true,
        (DataType::Time32(_), DynCell::I32(_)) => true,
        (DataType::Time64(_), DynCell::I64(_)) => true,
        (DataType::Duration(_), DynCell::I64(_)) => true,
        (DataType::Utf8, DynCell::Str(_)) => true,
        (DataType::Binary, DynCell::Bin(_)) => true,
        (DataType::Struct(_), DynCell::Struct(_)) => true,
        (DataType::List(_), DynCell::List(_)) => true,
        (DataType::LargeList(_), DynCell::List(_)) => true,
        (DataType::FixedSizeList(_, _), DynCell::FixedSizeList(_)) => true,
        // Dictionary value-side validation (key width irrelevant here).
        (DataType::Dictionary(_, value), c) => match &**value {
            DataType::Utf8 | DataType::LargeUtf8 => matches!(c, DynCell::Str(_)),
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                matches!(c, DynCell::Bin(_))
            }
            // Primitive dictionary values
            DataType::Int8 => matches!(c, DynCell::I8(_)),
            DataType::Int16 => matches!(c, DynCell::I16(_)),
            DataType::Int32 => matches!(c, DynCell::I32(_)),
            DataType::Int64 => matches!(c, DynCell::I64(_)),
            DataType::UInt8 => matches!(c, DynCell::U8(_)),
            DataType::UInt16 => matches!(c, DynCell::U16(_)),
            DataType::UInt32 => matches!(c, DynCell::U32(_)),
            DataType::UInt64 => matches!(c, DynCell::U64(_)),
            DataType::Float32 => matches!(c, DynCell::F32(_)),
            DataType::Float64 => matches!(c, DynCell::F64(_)),
            _ => false,
        },
        _ => false,
    }
}
