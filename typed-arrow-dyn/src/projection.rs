//! Projection masks for nested Arrow schemas.

use std::{collections::BTreeMap, sync::Arc};

use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use parquet::arrow::{ArrowSchemaConverter, ProjectionMask as ParquetProjectionMask};
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

/// A set of columns within a potentially nested schema to project. If `None`, all columns
/// are included.
///
/// Paths are expressed as index sequences traversing the schema tree. For example,
/// `[3, 0, 1]` targets top-level column `3`, then the list element at `0`, and finally
/// field `1` within the struct stored inside the list.
///
/// # Example
///
/// ```text
/// // Schema layout (paths are ProjectionMask indices; `[idx]` is the flattened Parquet leaf):
/// //
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
/// Selecting `[0]`, `[2, 1]`, and `[3, 0, 1]` keeps the `id`, `address.city`, and
/// `phones.item.number` fields. Calling [`ProjectionMask::to_parquet`] then marks Parquet leaves
/// `0`, `3`, and `6` as `true`, producing the bitmap `[true, false, false, true, false, false,
/// true]`.
///
/// Call [`ProjectionMask::validate`] before using a mask to ensure every indexed path exists in the
/// runtime schema. Valid masks only clone full top-level fields, keeping downstream
/// [`DynProjection`](crate::view::DynProjection) construction safe.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProjectionMask {
    paths: Option<Vec<Vec<usize>>>,
}

impl ProjectionMask {
    /// Creates a mask from the provided index paths.
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
    pub fn to_parquet(&self, schema: &SchemaRef) -> ParquetProjectionMask {
        let Some(paths) = self.paths() else {
            return ParquetProjectionMask::all();
        };

        let leaf_paths = collect_leaf_paths(schema.as_ref());
        if leaf_paths.is_empty() {
            return ParquetProjectionMask::all();
        }

        let mut indices = Vec::new();
        'leaf: for (idx, leaf_path) in leaf_paths.iter().enumerate() {
            for mask_path in paths {
                if path_covers_leaf(mask_path, leaf_path) {
                    indices.push(idx);
                    continue 'leaf;
                }
            }
        }

        if indices.len() == leaf_paths.len() {
            return ParquetProjectionMask::all();
        }

        let descriptor = ArrowSchemaConverter::new()
            .convert(schema.as_ref())
            .expect("arrow schema convertible to parquet");

        if indices.is_empty() {
            ParquetProjectionMask::none(descriptor.num_columns())
        } else {
            ParquetProjectionMask::leaves(&descriptor, indices)
        }
    }

    /// Build an Arrow schema containing only the columns selected by this mask.
    pub fn to_schema(&self, schema: &SchemaRef) -> SchemaRef {
        let Some(paths) = self.paths() else {
            return schema.clone();
        };
        let mut root = MaskNode::default();
        for path in paths {
            if path.is_empty() {
                return schema.clone();
            }
            root.insert(path);
        }
        let mut selected = Vec::with_capacity(root.children.len());
        for (&idx, node) in &root.children {
            let field = schema.fields().get(idx).map(Arc::clone).unwrap_or_else(|| {
                panic!(
                    "projection index {idx} out of bounds for schema with {} fields",
                    schema.fields().len()
                )
            });
            selected.push(project_field(&field, node));
        }
        Arc::new(Schema::new(selected))
    }

    /// Validate that every path indexes a concrete column (or nested child) in `schema`.
    pub fn validate(&self, schema: &SchemaRef) -> Result<(), ProjectionMaskError> {
        let Some(paths) = self.paths() else {
            return Ok(());
        };
        for path in paths {
            validate_mask_path(schema.as_ref(), path)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
struct MaskNode {
    children: BTreeMap<usize, MaskNode>,
}

impl MaskNode {
    fn insert(&mut self, path: &[usize]) {
        if path.is_empty() {
            return;
        }
        let (first, rest) = path.split_first().expect("non-empty path");
        let child = self.children.entry(*first).or_default();
        child.insert(rest);
    }
}

fn project_field(field: &FieldRef, node: &MaskNode) -> FieldRef {
    if node.children.is_empty() {
        return field.clone();
    }
    match field.data_type() {
        DataType::Struct(children) => {
            let mut projected = Vec::with_capacity(node.children.len());
            for (&idx, child_node) in &node.children {
                let child = children.get(idx).unwrap_or_else(|| {
                    panic!(
                        "struct child index {idx} out of bounds (len={}) for field {}",
                        children.len(),
                        field.name()
                    )
                });
                projected.push(project_field(child, child_node));
            }
            clone_field_with_type(field.as_ref(), DataType::Struct(Fields::from(projected)))
        }
        DataType::List(item) => {
            let child_node = node
                .children
                .get(&0)
                .unwrap_or_else(|| panic!("list field {} must use child index 0", field.name()));
            let projected_item = project_field(item, child_node);
            clone_field_with_type(field.as_ref(), DataType::List(projected_item))
        }
        DataType::LargeList(item) => {
            let child_node = node.children.get(&0).unwrap_or_else(|| {
                panic!("large list field {} must use child index 0", field.name())
            });
            let projected_item = project_field(item, child_node);
            clone_field_with_type(field.as_ref(), DataType::LargeList(projected_item))
        }
        DataType::FixedSizeList(item, len) => {
            let child_node = node.children.get(&0).unwrap_or_else(|| {
                panic!(
                    "fixed-size list field {} must use child index 0",
                    field.name()
                )
            });
            let projected_item = project_field(item, child_node);
            clone_field_with_type(
                field.as_ref(),
                DataType::FixedSizeList(projected_item, *len),
            )
        }
        DataType::Map(entry, keys_sorted) => {
            let mut entry_node = node.children.get(&0).cloned().unwrap_or_default();
            entry_node
                .children
                .entry(0)
                .or_insert_with(MaskNode::default);
            entry_node
                .children
                .entry(1)
                .or_insert_with(MaskNode::default);
            let projected_entry = project_field(entry, &entry_node);
            clone_field_with_type(field.as_ref(), DataType::Map(projected_entry, *keys_sorted))
        }
        other => {
            if !node.children.is_empty() {
                panic!("type {other:?} has no children to project");
            }
            field.clone()
        }
    }
}

fn clone_field_with_type(field: &Field, data_type: DataType) -> FieldRef {
    let projected = Field::new(field.name(), data_type, field.is_nullable())
        .with_metadata(field.metadata().clone());
    Arc::new(projected)
}

fn collect_leaf_paths(schema: &Schema) -> Vec<Vec<usize>> {
    let mut leaves = Vec::new();
    for (ordinal, field) in schema.fields().iter().enumerate() {
        collect_field_paths(field, vec![ordinal], &mut leaves);
    }
    leaves
}

fn collect_field_paths(field: &FieldRef, path: Vec<usize>, leaves: &mut Vec<Vec<usize>>) {
    match field.data_type() {
        DataType::Struct(children) => {
            for (ordinal, child) in children.iter().enumerate() {
                let mut child_path = path.clone();
                child_path.push(ordinal);
                collect_field_paths(child, child_path, leaves);
            }
        }
        DataType::List(child)
        | DataType::LargeList(child)
        | DataType::FixedSizeList(child, _)
        | DataType::Map(child, _) => {
            let mut child_path = path;
            child_path.push(0);
            collect_field_paths(child, child_path, leaves);
        }
        DataType::Union(children, _) => {
            for (ordinal, (_, child)) in children.iter().enumerate() {
                let mut child_path = path.clone();
                child_path.push(ordinal);
                collect_field_paths(child, child_path, leaves);
            }
        }
        _ => leaves.push(path),
    }
}

fn path_covers_leaf(path: &[usize], leaf: &[usize]) -> bool {
    !path.is_empty() && path.len() <= leaf.len() && leaf.starts_with(path)
}

fn validate_mask_path(schema: &Schema, path: &[usize]) -> Result<(), ProjectionMaskError> {
    if path.is_empty() {
        return Err(invalid_path(path, "path may not be empty".to_string()));
    }
    let fields = schema.fields();
    let mut current_type = fields
        .get(path[0])
        .ok_or_else(|| {
            invalid_path(
                path,
                format!(
                    "column {} out of bounds (num_columns={})",
                    path[0],
                    fields.len()
                ),
            )
        })?
        .data_type()
        .clone();
    for (depth, &idx) in path.iter().enumerate().skip(1) {
        match &current_type {
            DataType::Struct(children) => {
                let child = children.get(idx).ok_or_else(|| {
                    invalid_path(
                        path,
                        format!("struct child {idx} out of bounds (len={})", children.len()),
                    )
                })?;
                current_type = child.data_type().clone();
            }
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::FixedSizeList(child, _)
            | DataType::Map(child, _) => {
                if idx != 0 {
                    return Err(invalid_path(
                        path,
                        format!("list-like types use index 0, got {idx} at depth {depth}"),
                    ));
                }
                current_type = child.data_type().clone();
            }
            DataType::Union(children, _) => {
                let child_type = children
                    .iter()
                    .nth(idx)
                    .map(|(_, child)| child.data_type().clone())
                    .ok_or_else(|| {
                        invalid_path(
                            path,
                            format!("union child {idx} out of bounds (len={})", children.len()),
                        )
                    })?;
                current_type = child_type;
            }
            other => {
                return Err(invalid_path(
                    path,
                    format!("type {other:?} has no children at depth {depth}"),
                ));
            }
        }
    }
    Ok(())
}

fn invalid_path(path: &[usize], message: String) -> ProjectionMaskError {
    ProjectionMaskError::InvalidPath {
        path: path.to_vec(),
        message,
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, StructArray};
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
    use parquet::arrow::ArrowSchemaConverter;
    use std::sync::Arc;

    use super::{ProjectionMask, ProjectionMaskError};

    fn included_leaf_indices(schema: &SchemaRef, mask: &ProjectionMask) -> Vec<usize> {
        let parquet_mask = mask.to_parquet(schema);
        let descriptor = ArrowSchemaConverter::new()
            .convert(schema.as_ref())
            .expect("arrow schema convertible");
        (0..descriptor.num_columns())
            .filter(|idx| parquet_mask.leaf_included(*idx))
            .collect()
    }

    #[test]
    fn projection_mask_converts_to_parquet_mask() {
        let (schema, _batch, mask) = nested_projection_fixture();
        let included = included_leaf_indices(&schema, &mask);
        assert_eq!(included, vec![0, 3, 6]);
    }

    #[test]
    fn projection_mask_struct_prefix_to_parquet_mask() {
        let (schema, _batch, _) = nested_projection_fixture();
        let mask = ProjectionMask::new(vec![vec![2]]);
        let included = included_leaf_indices(&schema, &mask);
        assert_eq!(included, vec![2, 3, 4]);
    }

    #[test]
    fn projection_mask_to_schema_trims_nested_fields() {
        let (schema, _batch, mask) = nested_projection_fixture();
        let projected = mask.to_schema(&schema);
        assert_eq!(projected.fields().len(), 3);
        let address = projected.field_with_name("address").expect("address");
        match address.data_type() {
            DataType::Struct(children) => {
                assert_eq!(children.len(), 1);
                assert_eq!(children[0].name(), "city");
            }
            other => panic!("expected struct, got {other:?}"),
        }
        let phones = projected.field_with_name("phones").expect("phones");
        match phones.data_type() {
            DataType::List(item) => match item.data_type() {
                DataType::Struct(children) => {
                    assert_eq!(children.len(), 1);
                    assert_eq!(children[0].name(), "number");
                }
                other => panic!("expected struct item, got {other:?}"),
            },
            other => panic!("expected list, got {other:?}"),
        }
    }

    #[test]
    fn projection_mask_validate_rejects_bad_paths() {
        let (schema, _batch, _) = nested_projection_fixture();
        let missing = ProjectionMask::new(vec![vec![99]]);
        match missing.validate(&schema) {
            Err(ProjectionMaskError::InvalidPath { .. }) => {}
            other => panic!("expected invalid path error, got {other:?}"),
        }

        let bad_list = ProjectionMask::new(vec![vec![3, 1]]);
        match bad_list.validate(&schema) {
            Err(ProjectionMaskError::InvalidPath { .. }) => {}
            other => panic!("expected invalid path error, got {other:?}"),
        }
    }

    fn nested_projection_fixture() -> (SchemaRef, RecordBatch, ProjectionMask) {
        let id = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
        let name = Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef;

        let street = Arc::new(StringArray::from(vec!["1st", "2nd"])) as ArrayRef;
        let city_values =
            Arc::new(StringArray::from(vec!["Springfield", "Shelbyville"])) as ArrayRef;
        let zip = Arc::new(StringArray::from(vec!["11111", "22222"])) as ArrayRef;
        let address_fields = Fields::from(vec![
            Arc::new(Field::new("street", DataType::Utf8, false)),
            Arc::new(Field::new("city", DataType::Utf8, false)),
            Arc::new(Field::new("zip", DataType::Utf8, false)),
        ]);
        let address = Arc::new(StructArray::new(
            address_fields.clone(),
            vec![street, city_values.clone(), zip],
            None,
        )) as ArrayRef;

        let phone_fields = Fields::from(vec![
            Arc::new(Field::new("kind", DataType::Utf8, false)),
            Arc::new(Field::new("number", DataType::Utf8, false)),
        ]);
        let phone_kinds = Arc::new(StringArray::from(vec!["home", "work", "mobile"])) as ArrayRef;
        let phone_numbers = Arc::new(StringArray::from(vec!["111", "222", "333"])) as ArrayRef;
        let phone_values = Arc::new(StructArray::new(
            phone_fields.clone(),
            vec![phone_kinds, phone_numbers.clone()],
            None,
        )) as ArrayRef;
        let offsets = OffsetBuffer::new(vec![0i32, 2, 3].into());
        let phones = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Struct(phone_fields), true)),
            offsets,
            phone_values,
            None,
        )) as ArrayRef;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("address", DataType::Struct(address_fields), false),
            Field::new(
                "phones",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Arc::new(Field::new("kind", DataType::Utf8, false)),
                        Arc::new(Field::new("number", DataType::Utf8, false)),
                    ])),
                    true,
                ))),
                true,
            ),
        ]));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![id, name, address, phones]).expect("batch");
        let mask = ProjectionMask::new(vec![vec![0], vec![2, 1], vec![3, 0, 1]]);
        (schema, batch, mask)
    }
}
