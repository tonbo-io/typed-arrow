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
}
