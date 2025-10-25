//! Dynamic cell values accepted by dynamic column builders.
//!
//! Notes on mapping to Arrow types:
//! - Dictionary columns accept the same value variants as their value type (for example, `Str` for
//!   `Dictionary(_, Utf8)` or `Bin` for `Dictionary(_, FixedSizeBinary(_))`). Keys are managed by
//!   the builder.
//! - `FixedSizeBinary(w)` requires `Bin` values with exact length `w`.
//! - `List` is used for both `List` and `LargeList` logical types; the builder selects the correct
//!   offset width. `FixedSizeList` must match the declared list length.

/// A dynamic cell to be appended into a dynamic column builder.
pub enum DynCell {
    /// Append a null to the target column.
    Null,
    // Scalars
    /// Boolean value for `DataType::Boolean`.
    Bool(bool),
    /// 8-bit signed integer for `DataType::Int8`.
    I8(i8),
    /// 16-bit signed integer for `DataType::Int16`.
    I16(i16),
    /// 32-bit signed integer for `DataType::Int32`.
    I32(i32),
    /// 64-bit signed integer for `DataType::Int64`.
    I64(i64),
    /// 8-bit unsigned integer for `DataType::UInt8`.
    U8(u8),
    /// 16-bit unsigned integer for `DataType::UInt16`.
    U16(u16),
    /// 32-bit unsigned integer for `DataType::UInt32`.
    U32(u32),
    /// 64-bit unsigned integer for `DataType::UInt64`.
    U64(u64),
    /// 32-bit floating point for `DataType::Float32`.
    F32(f32),
    /// 64-bit floating point for `DataType::Float64`.
    F64(f64),
    /// UTF-8 string for `DataType::Utf8` or `DataType::LargeUtf8` (and their dictionary forms).
    Str(String),
    /// Arbitrary bytes for `DataType::Binary`, `DataType::LargeBinary`, or
    /// `DataType::FixedSizeBinary(w)` (length must equal `w`) and their dictionary forms.
    Bin(Vec<u8>),
    // Nested
    /// Struct cell with one entry per child field (same length as the struct's fields).
    /// Each child may be `None` (null) or a nested `DynCell` matching the child's type.
    Struct(Vec<Option<DynCell>>),
    /// Variable-size list (used for both `List` and `LargeList`); items can be null.
    /// The child element type must match the list's item field.
    List(Vec<Option<DynCell>>),
    /// Fixed-size list; the number of items must match the list's declared length.
    /// Each item may be `None` (null) or a nested `DynCell` matching the child type.
    FixedSizeList(Vec<Option<DynCell>>),
    /// Map cell containing key/value entries. Keys must be non-null; values may be null depending
    /// on the schema's value-field nullability.
    Map(Vec<(DynCell, Option<DynCell>)>),
    /// Union cell selects a variant by `type_id` and optionally carries a nested value.
    Union {
        /// Tag defined in `UnionFields` identifying the active variant.
        type_id: i8,
        /// Nested payload for the selected variant; `None` encodes a null in that child.
        value: Option<Box<DynCell>>,
    },
}

impl DynCell {
    /// A short, human-readable type name for diagnostics.
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            DynCell::Null => "null",
            DynCell::Bool(_) => "bool",
            DynCell::I8(_) => "i8",
            DynCell::I16(_) => "i16",
            DynCell::I32(_) => "i32",
            DynCell::I64(_) => "i64",
            DynCell::U8(_) => "u8",
            DynCell::U16(_) => "u16",
            DynCell::U32(_) => "u32",
            DynCell::U64(_) => "u64",
            DynCell::F32(_) => "f32",
            DynCell::F64(_) => "f64",
            DynCell::Str(_) => "utf8",
            DynCell::Bin(_) => "binary",
            DynCell::Struct(_) => "struct",
            DynCell::List(_) => "list",
            DynCell::FixedSizeList(_) => "fixed_size_list",
            DynCell::Map(_) => "map",
            DynCell::Union { .. } => "union",
        }
    }

    /// Construct a union cell with a nested value.
    #[must_use]
    pub fn union_value(type_id: i8, value: DynCell) -> Self {
        DynCell::Union {
            type_id,
            value: Some(Box::new(value)),
        }
    }

    /// Construct a union cell that encodes a null for the given variant.
    #[must_use]
    pub fn union_null(type_id: i8) -> Self {
        DynCell::Union {
            type_id,
            value: None,
        }
    }
}
