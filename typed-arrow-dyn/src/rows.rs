//! Dynamic row wrapper.

use crate::arrow_schema as arrow_schema;

use arrow_schema::{DataType, Field, UnionFields};

use crate::{DynError, cell::DynCell, dyn_builder::DynColumnBuilder};

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
            if let Some(cell) = cell_opt {
                let dt = b.data_type();
                if let Err(message) = validate_cell_against_field(dt, cell) {
                    let name = fields.get(i).map_or("?", |f| f.name().as_str());
                    return Err(DynError::Append {
                        col: i,
                        message: format!("{} at column '{}'", message, name),
                    });
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
        (DataType::Map(entry_field, _), DynCell::Map(entries)) => {
            let DataType::Struct(entry_fields) = entry_field.data_type() else {
                return false;
            };
            if entry_fields.len() != 2 {
                return false;
            }
            let Some(key_field) = entry_fields.first() else {
                return false;
            };
            let Some(value_field) = entry_fields.get(1) else {
                return false;
            };
            entries.iter().all(|(key_cell, value_cell)| {
                if matches!(key_cell, DynCell::Null) {
                    return false;
                }
                if !accepts_cell(key_field.data_type(), key_cell) {
                    return false;
                }
                match value_cell {
                    Some(cell) => accepts_cell(value_field.data_type(), cell),
                    None => true,
                }
            })
        }
        (DataType::Union(fields, _), DynCell::Union { type_id, value }) => {
            let field = fields
                .iter()
                .find_map(|(tag, field)| if tag == *type_id { Some(field) } else { None });
            match field {
                None => false,
                Some(field) => match value.as_deref() {
                    None => true,
                    Some(inner) => accepts_cell(field.data_type(), inner),
                },
            }
        }
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

fn validate_map_cell(cell: &DynCell, entry_field: &Field) -> Result<(), String> {
    let entries = match cell {
        DynCell::Map(entries) => entries,
        other => return Err(format!("expected map value, found {}", other.type_name())),
    };

    let DataType::Struct(children) = entry_field.data_type() else {
        return Err("map entry field is not a struct".to_string());
    };
    if children.len() != 2 {
        return Err(format!(
            "map entry struct must have 2 fields (keys, values), found {}",
            children.len()
        ));
    }

    let key_field = &children[0];
    let value_field = &children[1];
    let value_nullable = value_field.is_nullable();

    for (idx, (key_cell, value_cell)) in entries.iter().enumerate() {
        if matches!(key_cell, DynCell::Null) {
            return Err(format!("entry {} has a null map key", idx));
        }
        if !accepts_cell(key_field.data_type(), key_cell) {
            return Err(format!(
                "map key {} expected {:?}, found {}",
                idx,
                key_field.data_type(),
                key_cell.type_name()
            ));
        }

        match value_cell {
            None => {
                if !value_nullable {
                    return Err(format!(
                        "map value {} is null but '{}' is not nullable",
                        idx,
                        value_field.name()
                    ));
                }
            }
            Some(DynCell::Null) => {
                if !value_nullable {
                    return Err(format!(
                        "map value {} is null but '{}' is not nullable",
                        idx,
                        value_field.name()
                    ));
                }
            }
            Some(inner) => {
                if !accepts_cell(value_field.data_type(), inner) {
                    return Err(format!(
                        "map value {} expected {:?}, found {}",
                        idx,
                        value_field.data_type(),
                        inner.type_name()
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_union_cell(cell: &DynCell, fields: &UnionFields) -> Result<(), String> {
    let DynCell::Union { type_id, value } = cell else {
        return Err(format!("expected union value, found {}", cell.type_name()));
    };

    let Some(field) = fields
        .iter()
        .find_map(|(tag, field)| if tag == *type_id { Some(field) } else { None })
    else {
        return Err(format!("union value uses unknown type id {}", type_id));
    };

    match value.as_deref() {
        None => Ok(()),
        Some(inner) => validate_cell_against_field(field.data_type(), inner),
    }
}

fn validate_cell_against_field(dt: &DataType, cell: &DynCell) -> Result<(), String> {
    match dt {
        DataType::Map(entry_field, _) => validate_map_cell(cell, entry_field.as_ref()),
        DataType::Union(fields, _) => validate_union_cell(cell, fields),
        _ if accepts_cell(dt, cell) => Ok(()),
        _ => Err(format!(
            "type mismatch: expected {:?}, found {}",
            dt,
            cell.type_name()
        )),
    }
}
