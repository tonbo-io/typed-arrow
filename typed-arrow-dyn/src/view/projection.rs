use std::sync::Arc;

use crate::arrow_array::RecordBatch;
use crate::arrow_schema::{DataType, Field, FieldRef, Fields, Schema};
use crate::parquet::arrow::{ArrowSchemaConverter, ProjectionMask as ParquetProjectionMask};

use super::{
    path::Path,
    rows::{DynRowRaw, DynRowView},
};
use crate::{DynViewError, schema::DynSchema};

/// Column projection descriptor used to derive projected dynamic views.
#[derive(Debug, Clone)]
pub struct DynProjection(Arc<DynProjectionData>);

#[derive(Debug, Clone)]
pub(super) enum FieldProjector {
    Identity,
    Struct(Arc<StructProjection>),
    List(Box<FieldProjector>),
    LargeList(Box<FieldProjector>),
    FixedSizeList(Box<FieldProjector>),
    Map(Arc<StructProjection>),
}

impl FieldProjector {
    fn is_identity(&self) -> bool {
        matches!(self, FieldProjector::Identity)
    }
}

#[derive(Debug, Clone)]
pub(super) struct StructProjection {
    pub(super) children: Arc<[StructChildProjection]>,
}

#[derive(Debug, Clone)]
pub(super) struct StructChildProjection {
    pub(super) source_index: usize,
    pub(super) projector: FieldProjector,
}

#[derive(Debug)]
struct DynProjectionData {
    source_width: usize,
    mapping: Arc<[usize]>,
    fields: Fields,
    projectors: Arc<[FieldProjector]>,
    parquet_mask: ParquetProjectionMask,
}

impl DynProjection {
    fn new_internal(
        schema: &Schema,
        source_width: usize,
        mapping: Vec<usize>,
        fields: Fields,
        projectors: Vec<FieldProjector>,
        selected_paths: Vec<Vec<usize>>,
    ) -> Result<Self, DynViewError> {
        debug_assert_eq!(
            mapping.len(),
            projectors.len(),
            "projection mapping and projector width mismatch"
        );
        let parquet_mask = build_parquet_mask(schema, selected_paths)?;
        Ok(Self(Arc::new(DynProjectionData {
            source_width,
            mapping: Arc::from(mapping),
            fields,
            projectors: Arc::from(projectors),
            parquet_mask,
        })))
    }

    /// Create a projection from explicit column indices.
    ///
    /// # Errors
    /// Returns `DynViewError::ColumnOutOfBounds` if any index exceeds the schema width.
    pub fn from_indices<I>(schema: &Schema, indices: I) -> Result<Self, DynViewError>
    where
        I: IntoIterator<Item = usize>,
    {
        let schema_fields = schema.fields();
        let width = schema_fields.len();
        let mut mapping = Vec::new();
        let mut projected = Vec::new();
        let mut projectors = Vec::new();
        let mut selected_paths = Vec::new();
        for idx in indices.into_iter() {
            if idx >= width {
                return Err(DynViewError::ColumnOutOfBounds { column: idx, width });
            }
            mapping.push(idx);
            projected.push(schema_fields[idx].clone());
            projectors.push(FieldProjector::Identity);
            let mut index_path = vec![idx];
            collect_all_leaf_paths_for_field(
                schema_fields[idx].as_ref(),
                &mut index_path,
                &mut selected_paths,
            );
        }
        Self::new_internal(
            schema,
            width,
            mapping,
            Fields::from(projected),
            projectors,
            selected_paths,
        )
    }

    /// Create a projection by matching a projected schema against the source schema.
    ///
    /// Fields are matched by name; data type and nullability must also align.
    ///
    /// # Errors
    /// Returns `DynViewError` when a projected field is missing from the source schema or when its
    /// metadata disagrees.
    pub fn from_schema(source: &Schema, projection: &Schema) -> Result<Self, DynViewError> {
        let source_fields = source.fields();
        let width = source_fields.len();
        let mut mapping = Vec::with_capacity(projection.fields().len());
        let mut projected = Vec::with_capacity(projection.fields().len());
        let mut projectors = Vec::with_capacity(projection.fields().len());
        let mut selected_paths = Vec::new();
        for (pos, field) in projection.fields().iter().enumerate() {
            let source_idx = match source.index_of(field.name()) {
                Ok(idx) => idx,
                Err(_) => {
                    return Err(DynViewError::Invalid {
                        column: pos,
                        path: field.name().to_string(),
                        message: "field not found in source schema".to_string(),
                    });
                }
            };
            let source_field = source_fields[source_idx].as_ref();
            let path = Path::new(source_idx, field.name());
            let mut index_path = vec![source_idx];
            let projector = build_field_projector(
                &path,
                source_field,
                field.as_ref(),
                &mut index_path,
                &mut selected_paths,
            )?;
            mapping.push(source_idx);
            projected.push(field.clone());
            projectors.push(projector);
        }
        Self::new_internal(
            source,
            width,
            mapping,
            Fields::from(projected),
            projectors,
            selected_paths,
        )
    }

    /// Width of the source schema this projection was derived from.
    pub(super) fn source_width(&self) -> usize {
        self.0.source_width
    }

    pub(super) fn mapping_arc(&self) -> Arc<[usize]> {
        Arc::clone(&self.0.mapping)
    }

    pub(super) fn projectors_arc(&self) -> Arc<[FieldProjector]> {
        Arc::clone(&self.0.projectors)
    }

    /// Projected schema fields in order.
    pub fn fields(&self) -> &Fields {
        &self.0.fields
    }

    /// Number of projected columns.
    pub fn len(&self) -> usize {
        self.0.mapping.len()
    }

    /// Returns `true` when the projection contains zero columns.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the Parquet projection mask corresponding to this projection.
    pub fn to_parquet_mask(&self) -> ParquetProjectionMask {
        self.0.parquet_mask.clone()
    }

    /// Project a single row from `batch` using this projection, returning a borrowed view.
    ///
    /// # Errors
    /// Returns `DynViewError` when schema validation fails, the row index is out of bounds,
    /// or the projection width mismatches the batch.
    pub fn project_row_view<'a>(
        &self,
        schema: &'a DynSchema,
        batch: &'a RecordBatch,
        row: usize,
    ) -> Result<DynRowView<'a>, DynViewError> {
        let view = schema.view_at(batch, row)?;
        view.project(self)
    }

    /// Project a single row from `batch` and capture it as lifetime-erased raw cells.
    pub fn project_row_raw(
        &self,
        schema: &DynSchema,
        batch: &RecordBatch,
        row: usize,
    ) -> Result<DynRowRaw, DynViewError> {
        let view = self.project_row_view(schema, batch, row)?;
        view.into_raw()
    }
}

fn build_parquet_mask(
    schema: &Schema,
    mut selected_paths: Vec<Vec<usize>>,
) -> Result<ParquetProjectionMask, DynViewError> {
    let converter = ArrowSchemaConverter::new();
    let descriptor = converter
        .convert(schema)
        .map_err(|err| DynViewError::Invalid {
            column: 0,
            path: "<projection>".to_string(),
            message: format!("failed to convert schema to Parquet: {err}"),
        })?;

    if selected_paths.is_empty() {
        return Ok(ParquetProjectionMask::leaves(&descriptor, []));
    }

    selected_paths.sort();
    selected_paths.dedup();

    let mut leaf_paths = Vec::new();
    collect_schema_leaf_paths(schema.fields(), &mut Vec::new(), &mut leaf_paths);
    if selected_paths.len() == leaf_paths.len() {
        return Ok(ParquetProjectionMask::all());
    }
    let leaf_indices = map_paths_to_leaf_indices(&selected_paths, &leaf_paths);
    if leaf_indices.is_empty() {
        return Ok(ParquetProjectionMask::leaves(&descriptor, []));
    }
    Ok(ParquetProjectionMask::leaves(&descriptor, leaf_indices))
}

fn collect_all_leaf_paths_for_field(
    field: &Field,
    path: &mut Vec<usize>,
    acc: &mut Vec<Vec<usize>>,
) {
    match field.data_type() {
        DataType::Struct(children) => {
            for (idx, child) in children.iter().enumerate() {
                path.push(idx);
                collect_all_leaf_paths_for_field(child.as_ref(), path, acc);
                path.pop();
            }
        }
        DataType::List(child) | DataType::LargeList(child) => {
            path.push(0);
            collect_all_leaf_paths_for_field(child.as_ref(), path, acc);
            path.pop();
        }
        DataType::FixedSizeList(child, _) => {
            path.push(0);
            collect_all_leaf_paths_for_field(child.as_ref(), path, acc);
            path.pop();
        }
        DataType::Map(entry, _) => {
            path.push(0);
            collect_all_leaf_paths_for_field(entry.as_ref(), path, acc);
            path.pop();
        }
        _ => acc.push(path.clone()),
    }
}

fn collect_schema_leaf_paths(
    fields: &Fields,
    prefix: &mut Vec<usize>,
    leaves: &mut Vec<Vec<usize>>,
) {
    for (idx, field) in fields.iter().enumerate() {
        prefix.push(idx);
        collect_all_leaf_paths_for_field(field.as_ref(), prefix, leaves);
        prefix.pop();
    }
}

fn map_paths_to_leaf_indices(
    selected_paths: &[Vec<usize>],
    leaf_paths: &[Vec<usize>],
) -> Vec<usize> {
    let mut indices = Vec::new();
    'outer: for (idx, leaf_path) in leaf_paths.iter().enumerate() {
        for selected in selected_paths {
            if is_prefix(selected, leaf_path) {
                indices.push(idx);
                continue 'outer;
            }
        }
    }
    indices
}

fn is_prefix(prefix: &[usize], whole: &[usize]) -> bool {
    if prefix.len() > whole.len() {
        return false;
    }
    prefix.iter().zip(whole.iter()).all(|(lhs, rhs)| lhs == rhs)
}

/// Validate that the batch schema matches the runtime schema exactly.
fn build_field_projector(
    path: &Path,
    source: &Field,
    projected: &Field,
    index_path: &mut Vec<usize>,
    selected_paths: &mut Vec<Vec<usize>>,
) -> Result<FieldProjector, DynViewError> {
    if source.is_nullable() != projected.is_nullable() {
        return Err(DynViewError::Invalid {
            column: path.column,
            path: path.path.clone(),
            message: "nullability mismatch between source and projection".to_string(),
        });
    }
    if source.data_type() == projected.data_type() {
        collect_all_leaf_paths_for_field(source, index_path, selected_paths);
        return Ok(FieldProjector::Identity);
    }
    match (source.data_type(), projected.data_type()) {
        (DataType::Struct(source_children), DataType::Struct(projected_children)) => {
            build_struct_projector(
                path,
                source_children,
                projected_children,
                index_path,
                selected_paths,
            )
        }
        (DataType::List(source_child), DataType::List(projected_child)) => {
            build_list_like_projector(
                path,
                source_child,
                projected_child,
                source.data_type(),
                index_path,
                selected_paths,
            )
        }
        (DataType::LargeList(source_child), DataType::LargeList(projected_child)) => {
            build_list_like_projector(
                path,
                source_child,
                projected_child,
                source.data_type(),
                index_path,
                selected_paths,
            )
        }
        (
            DataType::FixedSizeList(source_child, source_len),
            DataType::FixedSizeList(projected_child, projected_len),
        ) => {
            if source_len != projected_len {
                return Err(DynViewError::Invalid {
                    column: path.column,
                    path: path.path.clone(),
                    message: "fixed-size list length mismatch between source and projection"
                        .to_string(),
                });
            }
            build_list_like_projector(
                path,
                source_child,
                projected_child,
                source.data_type(),
                index_path,
                selected_paths,
            )
        }
        (
            DataType::Map(source_entry, keys_sorted),
            DataType::Map(projected_entry, projected_sorted),
        ) => {
            if keys_sorted != projected_sorted {
                return Err(DynViewError::Invalid {
                    column: path.column,
                    path: path.path.clone(),
                    message: "map key ordering mismatch between source and projection".to_string(),
                });
            }
            build_map_projector(
                path,
                source_entry,
                projected_entry,
                index_path,
                selected_paths,
            )
        }
        _ => Err(DynViewError::SchemaMismatch {
            column: path.column,
            field: path.path.clone(),
            expected: source.data_type().clone(),
            actual: projected.data_type().clone(),
        }),
    }
}

fn build_struct_projector(
    path: &Path,
    source_children: &Fields,
    projected_children: &Fields,
    index_path: &mut Vec<usize>,
    selected_paths: &mut Vec<Vec<usize>>,
) -> Result<FieldProjector, DynViewError> {
    let mut children = Vec::with_capacity(projected_children.len());
    for projected_child in projected_children.iter() {
        let Some(source_index) = source_children
            .iter()
            .position(|f| f.name() == projected_child.name())
        else {
            return Err(DynViewError::Invalid {
                column: path.column,
                path: path.push_field(projected_child.name()).path,
                message: "field not found in source schema".to_string(),
            });
        };
        let child_path = path.push_field(projected_child.name());
        index_path.push(source_index);
        let child_projector = build_field_projector(
            &child_path,
            source_children[source_index].as_ref(),
            projected_child.as_ref(),
            index_path,
            selected_paths,
        )?;
        index_path.pop();
        children.push(StructChildProjection {
            source_index,
            projector: child_projector,
        });
    }
    let is_identity = projected_children.len() == source_children.len()
        && children
            .iter()
            .enumerate()
            .all(|(idx, child)| child.source_index == idx && child.projector.is_identity());
    if is_identity {
        Ok(FieldProjector::Identity)
    } else {
        Ok(FieldProjector::Struct(Arc::new(StructProjection {
            children: children.into(),
        })))
    }
}

fn build_list_like_projector(
    path: &Path,
    source_child: &FieldRef,
    projected_child: &FieldRef,
    parent_type: &DataType,
    index_path: &mut Vec<usize>,
    selected_paths: &mut Vec<Vec<usize>>,
) -> Result<FieldProjector, DynViewError> {
    let child_path = path.push_index(0);
    index_path.push(0);
    let child_projector = build_field_projector(
        &child_path,
        source_child.as_ref(),
        projected_child.as_ref(),
        index_path,
        selected_paths,
    )?;
    index_path.pop();
    if child_projector.is_identity() {
        Ok(FieldProjector::Identity)
    } else {
        match parent_type {
            DataType::List(_) => Ok(FieldProjector::List(Box::new(child_projector))),
            DataType::LargeList(_) => Ok(FieldProjector::LargeList(Box::new(child_projector))),
            DataType::FixedSizeList(_, _) => {
                Ok(FieldProjector::FixedSizeList(Box::new(child_projector)))
            }
            _ => unreachable!("list-like projector invoked for non list type"),
        }
    }
}

fn build_map_projector(
    path: &Path,
    source_entry: &FieldRef,
    projected_entry: &FieldRef,
    index_path: &mut Vec<usize>,
    selected_paths: &mut Vec<Vec<usize>>,
) -> Result<FieldProjector, DynViewError> {
    let DataType::Struct(source_children) = source_entry.data_type() else {
        return Err(DynViewError::Invalid {
            column: path.column,
            path: path.path.clone(),
            message: "map entry must be a struct field".to_string(),
        });
    };
    let DataType::Struct(projected_children) = projected_entry.data_type() else {
        return Err(DynViewError::Invalid {
            column: path.column,
            path: path.path.clone(),
            message: "projected map entry must be a struct field".to_string(),
        });
    };
    if projected_children.len() != 2 {
        return Err(DynViewError::Invalid {
            column: path.column,
            path: path.path.clone(),
            message: "map projection must contain exactly two fields (key then value)".to_string(),
        });
    }
    let entry_path = path.push_index(0);
    index_path.push(0);
    let projector = build_struct_projector(
        &entry_path,
        source_children,
        projected_children,
        index_path,
        selected_paths,
    )?;
    index_path.pop();
    match projector {
        FieldProjector::Struct(proj) => {
            let children = proj.children.as_ref();
            if children.len() != 2 {
                return Err(DynViewError::Invalid {
                    column: path.column,
                    path: path.path.clone(),
                    message: "map projection must preserve exactly two children".to_string(),
                });
            }
            if children[0].source_index != 0 || children[1].source_index != 1 {
                return Err(DynViewError::Invalid {
                    column: path.column,
                    path: path.path.clone(),
                    message: "map projection must keep the key field before the value field"
                        .to_string(),
                });
            }
            Ok(FieldProjector::Map(proj))
        }
        FieldProjector::Identity => Ok(FieldProjector::Identity),
        _ => Err(DynViewError::Invalid {
            column: path.column,
            path: path.path.clone(),
            message: "unsupported map projection".to_string(),
        }),
    }
}
