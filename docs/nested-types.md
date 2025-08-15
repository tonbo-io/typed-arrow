# Nested Types Design — Compile-Time Arrow Schema

## Overview
- Goal: Support Arrow nested types (Struct, List/LargeList, FixedSizeList, Map; optional Dictionary; future Union) while preserving compile-time schema and monomorphized dispatch.
- Approach: Represent containers with natural Rust shapes, drive layout via attributes, and recurse derive-generated metadata through nested fields. Nullability is expressed with `Option<_>` at each level.

## Supported Shapes
- Struct: nested `#[derive(Record)]` structs map to Arrow `Struct`.
- List/LargeList: `Vec<T>` maps to `List<T>`; `#[arrow(list(large = true))]` selects `LargeList<T>`.
  - Note: In the current prototype, use `arrow_native::List<T>` for non-null items and `arrow_native::ListNullable<T>` for nullable items to avoid a coherence conflict with `Vec<u8>` → Binary. We’ll switch to direct `Vec<T>`/`Vec<Option<T>>` mapping in a follow-up.
- Fixed-Size List: `[T; N]` maps to `FixedSizeList<T; N>`.
- Map: `Vec<MapEntry<K, V>>` maps to Arrow Map (List of Struct<key, value>), keys non-null.
- Dictionary: use `arrow_native::Dictionary<K, V>` wrapper for dictionary encoding (keys integral). Initial support focuses on `V = String` (Utf8 values).
- Timestamp: `arrow_native::Timestamp<U>` for epoch values with unit markers `Second`, `Millisecond`, `Microsecond`, `Nanosecond` (timezone None in this phase).
- Union (future): derived over enums; deferred due to complexity.

## Rust Shapes and Attributes
- Struct field:
  - Rust: `nested: Option<Nested>` where `Nested: Record` → `DataType::Struct(children)` with field-level nullability.
  - Attributes: allow renaming child fields via `#[arrow(child(name = "..."))]` on nested struct fields.
- List field:
  - Rust: `Vec<T>` → `List<T>`; `Vec<Option<T>>` → item-nullable list.
  - Large offsets: `#[arrow(list(large = true))]` → `LargeList<T>`.
  - List-level nullability: `Option<Vec<T>>`.
  - Both: `Option<Vec<Option<T>>>`.
- Fixed-size list field:
  - Rust: `[T; N]` → `FixedSizeList<T; N>`; `[Option<T>; N]` → item-nullable fixed-size list.
  - Nullability: `Option<[T; N]>` at list level.
- Map field:
  - Rust: `Vec<MapEntry<K, V>>` with `struct MapEntry<K, V> { key: K, value: Option<V> }`.
  - Arrow: `DataType::Map(Box<entries_struct>, keys_sorted)` with `keys_sorted` from `#[arrow(map(keys_sorted = bool))]`.
- Dictionary field:
  - Rust: `Dictionary<K, V>` newtype wrapper with integral `K` and supported `V` (currently `String` for Utf8).
  - Nullability: `Option<Dictionary<..>>` at column, and null semantics follow the keys (null key for null value).
  - Example: `code: Dictionary<i32, String>`, `opt_code: Option<Dictionary<i8, String>>`.
- Timestamp field:
  - Rust: `Timestamp<Millisecond>` etc.; nullability via `Option<_>`.
  - Arrow: `DataType::Timestamp(TimeUnit::{Second|Millisecond|Microsecond|Nanosecond}, None)`.
  - Example: `created_at: Option<Timestamp<Millisecond>>`.

### Attributes Summary
- `#[arrow(list(large = bool))]`
- `#[arrow(fixed_size_list)]` (usually inferred from `[T; N]`)
- `#[arrow(map(keys_sorted = bool))]`
- `#[arrow(child(name = "..."))]` for nested struct fields
- Existing: `#[arrow(nullable)]`, `#[arrow(type = "...")]`, `#[arrow(decimal(...))]`, `#[arrow(timestamp(...))]`

## Nullability Semantics
- Each container level’s nullability is controlled by `Option<_>` on that level’s Rust type.
- Item nullability within lists/fixed-size lists uses `Option<T>` for element type.
- Nested struct child nullability remains independent of parent nullability.

## Builders API (Typed and Nested)
- Struct:
  - Access nested builders by path: `b.addr.city.append_value(..)`.
- List/LargeList:
  - Low-level:
    - `b.tags.start(); for t in row.tags { b.tags.item().append_value(t); } b.tags.end();`
  - Ergonomic:
    - `b.tags.append_iter(row.tags.iter().map(Some))` for non-null items.
    - `b.tags.append_option_iter(row.tags.as_ref().map(|v| v.iter().map(|x| x.as_ref())))` for option list and optional items.
- Fixed-size list:
  - `b.point.append_value([x, y, z])` or `b.mask.append_option([Some(a), None, Some(c)])`.
- Map:
  - `b.attrs.start();
     for e in &row.attrs {
       b.attrs.key().append_value(&e.key);
       b.attrs.value().append_option(e.value.as_ref());
     }
     b.attrs.end();`

## Compile-Time Dispatch Over Leaves
- Add `LeafVisitor` and `for_each_leaf::<R, V>()` utilities:
  - Visits primitive/utf8/binary/decimal/timestamp leaves beneath containers.
  - Provides a type-level path (indices or const path array), the leaf’s `ArrowMarker`, and Rust leaf type.
- Keep container-aware visitor variants for kernels needing offsets/lengths (e.g., list-aware operations).

## Schema Generation Algorithm (Recursive)
- Struct:
  - For each child field of `Nested: Record`, collect `Field { name, data_type, nullable }` and wrap in `DataType::Struct(children)`.
- List/LargeList:
  - Build child field recursively; wrap as `DataType::List(Box<child>)` or `LargeList` based on attribute.
- Fixed-size list:
  - Build child field; wrap as `DataType::FixedSizeList(Box<child>, N)` where `N` from array length.
- Map:
  - Build key/value child fields; enforce non-null keys; create entries struct `(key, value)` and wrap in `DataType::Map(..)`.
- Dictionary:
  - Wrap value `DataType` with dictionary metadata (key type) when supported.
 - Timestamp:
   - `Timestamp<U>` maps to `DataType::Timestamp(U::unit(), None)`, builder/array types are `Primitive{Builder,Array}<U::Arrow>`.
- Nullability: derive from `Option<_>` presence and item-level `Option<T>` for children.

## Validation Rules (in derive)
- Map keys must be non-nullable; reject `Option<K>`.
- Fixed-size list length `N > 0`; support `[Option<T>; N]`.
- Reject conflicting attributes (e.g., `type = "Utf8"` on a nested struct field).
- For `Vec<T>`, if both `#[arrow(list(large = true))]` and fixed size attributes are present, error.

## Examples
```rust
#[derive(Record)]
struct Address { city: String, zip: Option<i32> }

#[derive(Record)]
struct Person {
  id: i64,
  address: Option<Address>,                  // Struct(nullable)
  tags: Vec<String>,                         // List<Utf8>
  scores: Option<Vec<Option<i32>>>,          // Nullable list, nullable items
  #[arrow(list(large = true))]
  parts: Vec<Part>,                          // LargeList<Struct<...>> when Part: Record
  point: [f32; 3],                           // FixedSizeList<f32; 3>
  mask: Option<[Option<bool>; 8]>,           // Nullable FSL with nullable items
  attrs: Option<Vec<MapEntry<String, String>>> // Map<Utf8, Utf8>
}
```

## Phased Rollout
- Phase A: Struct + List + FixedSizeList end-to-end (builders + schema + leaf visitor).
- Phase B: Map + LargeList + docs and examples.
- Phase C: Optional Dictionary wrappers; revisit ergonomics.
- Phase D: Consider Union with dedicated derive, pending builder story.

## Open Questions
- Path encoding for `LeafVisitor`: const array of indices vs. type-level tuples; prefer const arrays for simplicity.
- Timestamp:
  - Builders are typed primitive builders, e.g., `PrimitiveBuilder<TimestampMillisecondType>`.
  - Use `append_value(i64)` epoch values and `append_null()` for nulls.
- Dictionary support level: schema-only first vs. full builder.
- Performance ergonomics: provide bulk append APIs for lists and maps to reduce per-item calls.
