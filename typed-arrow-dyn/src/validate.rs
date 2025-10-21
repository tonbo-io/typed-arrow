//! Validate nullability invariants in nested Arrow arrays using the schema.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray,
    UnionArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, UnionFields};

use crate::DynError;

/// Validate that arrays satisfy nullability constraints declared by `schema`.
/// Returns the first violation encountered with a descriptive path.
///
/// # Errors
/// Returns a `DynError::Nullability` describing the first violation encountered.
pub fn validate_nullability(schema: &Schema, arrays: &[ArrayRef]) -> Result<(), DynError> {
    for (col, (field, array)) in schema.fields().iter().zip(arrays.iter()).enumerate() {
        // Top-level field nullability
        if !field.is_nullable() && array.null_count() > 0 {
            if let Some(idx) = first_null_index(array.as_ref()) {
                return Err(DynError::Nullability {
                    col,
                    path: field.name().to_string(),
                    index: idx,
                    message: "non-nullable field contains null".to_string(),
                });
            }
        }

        // Nested
        validate_nested(field.name(), field.data_type(), array, col, None)?;
    }
    Ok(())
}

fn validate_nested(
    col_name: &str,
    dt: &DataType,
    array: &ArrayRef,
    col: usize,
    // An optional mask: when present, only indices with `true` are considered.
    parent_valid_mask: Option<Vec<bool>>,
) -> Result<(), DynError> {
    match dt {
        DataType::Struct(children) => {
            validate_struct(col_name, children, array, col, parent_valid_mask)
        }
        DataType::List(item) => validate_list(col_name, item, array, col, parent_valid_mask),
        DataType::LargeList(item) => {
            validate_large_list(col_name, item, array, col, parent_valid_mask)
        }
        DataType::FixedSizeList(item, _len) => {
            validate_fixed_list(col_name, item, array, col, parent_valid_mask)
        }
        DataType::Union(children, _) => {
            validate_union(col_name, children, array, col, parent_valid_mask)
        }
        DataType::Map(entry_field, _) => {
            validate_map(col_name, entry_field, array, col, parent_valid_mask)
        }
        // Other data types have no nested children.
        _ => Ok(()),
    }
}

fn validate_union(
    col_name: &str,
    fields: &UnionFields,
    array: &ArrayRef,
    col: usize,
    parent_mask: Option<Vec<bool>>,
) -> Result<(), DynError> {
    let union = array
        .as_any()
        .downcast_ref::<UnionArray>()
        .expect("array/DataType mismatch");

    let parent_valid = parent_mask.unwrap_or_else(|| validity_mask(union));

    let variants: Vec<(i8, FieldRef)> = fields
        .iter()
        .map(|(tag, field)| (*tag, field.clone()))
        .collect();

    let mut tag_to_index = vec![None; 256];
    for (idx, (tag, _)) in variants.iter().enumerate() {
        tag_to_index[tag_slot(*tag)] = Some(idx);
    }

    let mut rows_per_variant: Vec<Vec<(usize, usize)>> =
        variants.iter().map(|_| Vec::new()).collect();

    for (row, &is_valid) in parent_valid.iter().enumerate() {
        if !is_valid {
            continue;
        }
        let tag = union.type_id(row);
        let Some(idx) = tag_to_index[tag_slot(tag)] else {
            return Err(DynError::Builder {
                message: format!("union value uses unknown type id {tag}"),
            });
        };
        let offset = union.value_offset(row);
        rows_per_variant[idx].push((row, offset));
    }

    for (idx, rows) in rows_per_variant.iter().enumerate() {
        if rows.is_empty() {
            continue;
        }
        let (tag, field) = &variants[idx];
        let child = union.child(*tag).clone();
        let path = format!("{}.{}", col_name, field.name());

        if !field.is_nullable() {
            for (row_index, child_index) in rows {
                if child.is_null(*child_index) {
                    return Err(DynError::Nullability {
                        col,
                        path: path.clone(),
                        index: *row_index,
                        message: "non-nullable union variant contains null".to_string(),
                    });
                }
            }
        }

        let mut child_mask = vec![false; child.len()];
        for (_, child_index) in rows {
            if *child_index >= child_mask.len() {
                return Err(DynError::Builder {
                    message: format!(
                        "union child index {} out of bounds for variant '{}'",
                        child_index,
                        field.name()
                    ),
                });
            }
            child_mask[*child_index] = true;
        }

        validate_nested(&path, field.data_type(), &child, col, Some(child_mask))?;
    }

    Ok(())
}

fn validate_struct(
    col_name: &str,
    fields: &Fields,
    array: &ArrayRef,
    col: usize,
    parent_mask: Option<Vec<bool>>,
) -> Result<(), DynError> {
    let s = array
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("array/DataType mismatch");

    // Compute mask of valid parent rows: respect parent validity if provided, else
    // derive from the struct's own validity.
    let arr: &dyn Array = s;
    let mask = parent_mask.unwrap_or_else(|| validity_mask(arr));

    for (child_field, child_array) in fields.iter().zip(s.columns().iter()) {
        // Enforce child field nullability only where parent struct is valid.
        if !child_field.is_nullable() {
            let child = child_array.as_ref();
            for (i, &pvalid) in mask.iter().enumerate() {
                if pvalid && child.is_null(i) {
                    return Err(DynError::Nullability {
                        col,
                        path: format!("{}.{}", col_name, child_field.name()),
                        index: i,
                        message: "non-nullable struct field contains null".to_string(),
                    });
                }
            }
        }

        // Recurse into nested children with the same row mask.
        validate_nested(
            &format!("{}.{}", col_name, child_field.name()),
            child_field.data_type(),
            child_array,
            col,
            Some(mask.clone()),
        )?;
    }
    Ok(())
}

fn validate_list(
    col_name: &str,
    item: &Arc<Field>,
    array: &ArrayRef,
    col: usize,
    parent_mask: Option<Vec<bool>>,
) -> Result<(), DynError> {
    let l = array
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("array/DataType mismatch");

    let arr: &dyn Array = l;
    let parent_valid = parent_mask.unwrap_or_else(|| validity_mask(arr));
    let offsets: &OffsetBuffer<i32> = l.offsets();
    let child = l.values().clone();

    if !item.is_nullable() {
        for (row, &pvalid) in parent_valid.iter().enumerate() {
            if !pvalid {
                continue;
            }
            let start = usize::try_from(*offsets.get(row).expect("offset in range"))
                .expect("non-negative offset");
            let end = usize::try_from(*offsets.get(row + 1).expect("offset in range"))
                .expect("non-negative offset");
            for idx in start..end {
                if child.is_null(idx) {
                    return Err(DynError::Nullability {
                        col,
                        path: format!("{col_name}[]"),
                        index: idx,
                        message: "non-nullable list item contains null".to_string(),
                    });
                }
            }
        }
    }

    // Recurse into child type. Construct mask of child indices belonging to
    // valid parent rows.
    let mut child_mask = vec![false; child.len()];
    for (row, &pvalid) in parent_valid.iter().enumerate() {
        if !pvalid {
            continue;
        }
        let start = usize::try_from(*offsets.get(row).expect("offset in range"))
            .expect("non-negative offset");
        let end = usize::try_from(*offsets.get(row + 1).expect("offset in range"))
            .expect("non-negative offset");
        for item in child_mask.iter_mut().take(end).skip(start) {
            *item = true;
        }
    }

    validate_nested(
        &format!("{col_name}[]"),
        item.data_type(),
        &child,
        col,
        Some(child_mask),
    )
}

fn validate_large_list(
    col_name: &str,
    item: &Arc<Field>,
    array: &ArrayRef,
    col: usize,
    parent_mask: Option<Vec<bool>>,
) -> Result<(), DynError> {
    let l = array
        .as_any()
        .downcast_ref::<LargeListArray>()
        .expect("array/DataType mismatch");
    let arr: &dyn Array = l;
    let parent_valid = parent_mask.unwrap_or_else(|| validity_mask(arr));
    let offsets = l.offsets();
    let child = l.values().clone();

    if !item.is_nullable() {
        for (row, &pvalid) in parent_valid.iter().enumerate() {
            if !pvalid {
                continue;
            }
            let start = usize::try_from(*offsets.get(row).expect("offset in range"))
                .expect("non-negative offset");
            let end = usize::try_from(*offsets.get(row + 1).expect("offset in range"))
                .expect("non-negative offset");
            for idx in start..end {
                if child.is_null(idx) {
                    return Err(DynError::Nullability {
                        col,
                        path: format!("{col_name}[]"),
                        index: idx,
                        message: "non-nullable large-list item contains null".to_string(),
                    });
                }
            }
        }
    }

    let mut child_mask = vec![false; child.len()];
    for (row, &pvalid) in parent_valid.iter().enumerate() {
        if !pvalid {
            continue;
        }
        let start = usize::try_from(*offsets.get(row).expect("offset in range"))
            .expect("non-negative offset");
        let end = usize::try_from(*offsets.get(row + 1).expect("offset in range"))
            .expect("non-negative offset");
        for item in child_mask.iter_mut().take(end).skip(start) {
            *item = true;
        }
    }

    validate_nested(
        &format!("{col_name}[]"),
        item.data_type(),
        &child,
        col,
        Some(child_mask),
    )
}

fn validate_fixed_list(
    col_name: &str,
    item: &Arc<Field>,
    array: &ArrayRef,
    col: usize,
    parent_mask: Option<Vec<bool>>,
) -> Result<(), DynError> {
    let l = array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .expect("array/DataType mismatch");
    let arr: &dyn Array = l;
    let parent_valid = parent_mask.unwrap_or_else(|| validity_mask(arr));
    let child = l.values().clone();
    let width = usize::try_from(l.value_length()).expect("non-negative width");

    if !item.is_nullable() {
        for (row, &pvalid) in parent_valid.iter().enumerate() {
            if !pvalid {
                continue;
            }
            let start = row * width;
            let end = start + width;
            for idx in start..end {
                if child.is_null(idx) {
                    return Err(DynError::Nullability {
                        col,
                        path: format!("{col_name}[{row}]"),
                        index: idx,
                        message: "non-nullable fixed-size list item contains null".to_string(),
                    });
                }
            }
        }
    }

    let mut child_mask = vec![false; child.len()];
    for (row, &pvalid) in parent_valid.iter().enumerate() {
        if !pvalid {
            continue;
        }
        let start = row * width;
        let end = start + width;
        for item in child_mask.iter_mut().take(end).skip(start) {
            *item = true;
        }
    }

    validate_nested(
        &format!("{col_name}[]"),
        item.data_type(),
        &child,
        col,
        Some(child_mask),
    )
}

fn validate_map(
    col_name: &str,
    entry_field: &Arc<Field>,
    array: &ArrayRef,
    col: usize,
    parent_mask: Option<Vec<bool>>,
) -> Result<(), DynError> {
    let map = array
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("array/DataType mismatch");

    let arr: &dyn Array = map;
    let parent_valid = parent_mask.unwrap_or_else(|| validity_mask(arr));
    let offsets = map.offsets();
    let keys = map.keys().clone();
    let values = map.values().clone();

    let DataType::Struct(children) = entry_field.data_type() else {
        return Err(DynError::Builder {
            message: "map entry field is not a struct".to_string(),
        });
    };
    if children.len() != 2 {
        return Err(DynError::Builder {
            message: format!(
                "map entry struct must have 2 fields, found {}",
                children.len()
            ),
        });
    }
    let key_field = &children[0];
    let value_field = &children[1];

    for (row, &pvalid) in parent_valid.iter().enumerate() {
        if !pvalid {
            continue;
        }
        let start = usize::try_from(*offsets.get(row).expect("offset in range"))
            .expect("non-negative offset");
        let end = usize::try_from(*offsets.get(row + 1).expect("offset in range"))
            .expect("non-negative offset");
        for idx in start..end {
            if keys.as_ref().is_null(idx) {
                return Err(DynError::Nullability {
                    col,
                    path: format!("{col_name}.keys"),
                    index: idx,
                    message: "map keys cannot contain nulls".to_string(),
                });
            }
            if !value_field.is_nullable() && values.as_ref().is_null(idx) {
                return Err(DynError::Nullability {
                    col,
                    path: format!("{col_name}.values"),
                    index: idx,
                    message: "map values marked non-nullable contain null".to_string(),
                });
            }
        }
    }

    let mut key_mask = vec![false; keys.len()];
    let mut value_mask = vec![false; values.len()];
    for (row, &pvalid) in parent_valid.iter().enumerate() {
        if !pvalid {
            continue;
        }
        let start = usize::try_from(*offsets.get(row).expect("offset in range"))
            .expect("non-negative offset");
        let end = usize::try_from(*offsets.get(row + 1).expect("offset in range"))
            .expect("non-negative offset");
        for idx in start..end {
            key_mask[idx] = true;
            if values.as_ref().is_valid(idx) {
                value_mask[idx] = true;
            }
        }
    }

    validate_nested(
        &format!("{col_name}.keys"),
        key_field.data_type(),
        &keys,
        col,
        Some(key_mask),
    )?;
    validate_nested(
        &format!("{col_name}.values"),
        value_field.data_type(),
        &values,
        col,
        Some(value_mask),
    )?;
    Ok(())
}

fn validity_mask(array: &dyn Array) -> Vec<bool> {
    (0..array.len()).map(|i| array.is_valid(i)).collect()
}

fn first_null_index(array: &dyn Array) -> Option<usize> {
    (0..array.len()).find(|&i| array.is_null(i))
}

fn tag_slot(tag: i8) -> usize {
    (i16::from(tag) + 128) as usize
}
