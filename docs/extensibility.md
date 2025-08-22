Extending `#[derive(Record)]`

This project supports user-level extensions to the `Record` derive without adding new core attributes. The pattern uses a single carrier attribute `#[record(...)]` with two kinds of hooks you can opt into:

- Container hooks on the struct:
  - `visit(Path)`: inject a compile-time visitor that runs for each column type via `ForEachCol`. Useful for enforcing bounds or monomorphizing kernels. No runtime branching.
  - `field_macro = path::macro_name`: call a user macro once per field at item scope (you can generate impls, consts, etc.).
  - `record_macro = path::macro_name`: call a user macro once per record at item scope.
  - `ext(...)`: free-form tokens forwarded to your macros at the record level.

- Field hooks on each field:
  - `#[record(ext(...))]`: free-form tokens forwarded to your per-field macro.

Why a carrier attribute?
- Stable Rust requires derive macros to declare which attributes they accept. A general `#[key]` cannot be seen by `typed-arrow-derive` unless we declare `key` up front. Instead, write `#[record(ext(key))]`, or define your own tiny adapter attribute that rewrites `#[key]` into `#[record(ext(key))]` in your extension crate.

What the derive forwards to your macros
- Per-field macro invocation receives:
  - `owner = Type`: the struct type
  - `index = { I }`: the column index (const)
  - `field = ident`: the field identifier
  - `ty = Type`: the field Rust type with nullability unwrapped
  - `nullable = true|false`: whether the column is nullable
  - `is_nested = true|false`: whether `#[record(nested)]` is set
  - `ext = ( ... )`: tokens from `#[record(ext(...))]` on the field (or `()` if absent)

- Per-record macro invocation receives:
  - `owner = Type`: the struct type
  - `len = N`: number of columns
  - `ext = ( ... )`: tokens from container-level `#[record(ext(...))]` (or `()` if absent)

Using a compile-time visitor
- Provide a type that implements `ColumnVisitor` and opt-in via `#[record(visit(my_ext::VisitorType))]`.
- The derive expands a zero-cost instantiation: `<T as ForEachCol>::for_each_col::<VisitorType>()`, monomorphized per column and base type.

Example

See `examples/12_ext_hooks.rs` for a runnable demonstration that:
- marks a `#[record(ext(key))]` column and generates a `PrimaryKey` impl via a user macro, and
- injects a visitor with `#[record(visit(PrintVisitor))]`.

Adapter attribute for ergonomics

In your extension crate you can provide:

```rust
#[proc_macro_attribute]
pub fn key(_attr: proc_macro::TokenStream, item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Rewrites `#[key]` field into `#[typed_arrow::record(ext(key))]`
    let expanded = quote::quote!(#[typed_arrow::record(ext(key))] #item);
    expanded.into()
}
```

This keeps the core derive small while letting you layer your own attributes and codegen.
