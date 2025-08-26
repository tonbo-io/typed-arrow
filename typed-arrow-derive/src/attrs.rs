#[cfg(feature = "ext-hooks")]
use quote::ToTokens;
#[cfg(feature = "ext-hooks")]
use syn::Path;
use syn::{Attribute, LitStr};

pub(crate) fn parse_schema_metadata_pairs(
    attrs: &[Attribute],
) -> syn::Result<Vec<(String, String)>> {
    let mut out: Vec<(String, String)> = Vec::new();
    for attr in attrs {
        if attr.path().is_ident("schema_metadata") {
            let mut key: Option<String> = None;
            let mut val: Option<String> = None;
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("k") {
                    let s: LitStr = meta.value()?.parse()?;
                    key = Some(s.value());
                } else if meta.path.is_ident("v") {
                    let s: LitStr = meta.value()?.parse()?;
                    val = Some(s.value());
                }
                Ok(())
            })?;
            if let (Some(k), Some(vv)) = (key, val) {
                out.push((k, vv));
            }
        } else if attr.path().is_ident("record") {
            // Support nested: #[record(schema_metadata(k="...", v="..."))]
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("schema_metadata") {
                    let mut key: Option<String> = None;
                    let mut val: Option<String> = None;
                    meta.parse_nested_meta(|inner| {
                        if inner.path.is_ident("k") {
                            let s: LitStr = inner.value()?.parse()?;
                            key = Some(s.value());
                        } else if inner.path.is_ident("v") {
                            let s: LitStr = inner.value()?.parse()?;
                            val = Some(s.value());
                        }
                        Ok(())
                    })?;
                    if let (Some(k), Some(vv)) = (key, val) {
                        out.push((k, vv));
                    }
                } else {
                    // Consume unknown nested entries (e.g., visit, field_macro, record_macro, ext)
                    if let Ok(v) = meta.value() {
                        let _expr: syn::Expr = v.parse()?;
                    } else if meta.input.is_empty() {
                        // bare flag like `nested` — nothing to consume
                    } else {
                        meta.parse_nested_meta(|inner| {
                            if let Ok(v2) = inner.value() {
                                let _expr: syn::Expr = v2.parse()?;
                            } else if inner.input.is_empty() {
                                // bare flag inside list
                            } else {
                                let _ = inner.parse_nested_meta(|_| Ok(()));
                            }
                            Ok(())
                        })?;
                    }
                }
                Ok(())
            })?;
        }
    }
    Ok(out)
}

pub(crate) fn parse_field_metadata_pairs(
    attrs: &[Attribute],
) -> syn::Result<Option<Vec<(String, String)>>> {
    let mut out: Option<Vec<(String, String)>> = None;
    for attr in attrs {
        if attr.path().is_ident("metadata") {
            let mut key: Option<String> = None;
            let mut val: Option<String> = None;
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("k") {
                    let s: LitStr = meta.value()?.parse()?;
                    key = Some(s.value());
                } else if meta.path.is_ident("v") {
                    let s: LitStr = meta.value()?.parse()?;
                    val = Some(s.value());
                }
                Ok(())
            })?;
            if let (Some(k), Some(vv)) = (key, val) {
                out.get_or_insert_with(Vec::new).push((k, vv));
            }
        } else if attr.path().is_ident("record") {
            // Support nested: #[record(metadata(k="...", v="..."))]
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("metadata") {
                    let mut key: Option<String> = None;
                    let mut val: Option<String> = None;
                    meta.parse_nested_meta(|inner| {
                        if inner.path.is_ident("k") {
                            let s: LitStr = inner.value()?.parse()?;
                            key = Some(s.value());
                        } else if inner.path.is_ident("v") {
                            let s: LitStr = inner.value()?.parse()?;
                            val = Some(s.value());
                        }
                        Ok(())
                    })?;
                    if let (Some(k), Some(vv)) = (key, val) {
                        out.get_or_insert_with(Vec::new).push((k, vv));
                    }
                } else {
                    // Consume unknown nested entries
                    if let Ok(v) = meta.value() {
                        let _expr: syn::Expr = v.parse()?;
                    } else if meta.input.is_empty() {
                        // bare flag like `nested`
                    } else {
                        meta.parse_nested_meta(|inner| {
                            if let Ok(v2) = inner.value() {
                                let _expr: syn::Expr = v2.parse()?;
                            } else if inner.input.is_empty() {
                                // bare flag inside list
                            } else {
                                let _ = inner.parse_nested_meta(|_| Ok(()));
                            }
                            Ok(())
                        })?;
                    }
                }
                Ok(())
            })?;
        }
    }
    Ok(out)
}

// -------- extension hooks parsing (feature-gated) --------

// Container-level: #[record(visit(path::ToVisitor, other::Visitor))]
// Also supports repeated: #[record(visit(path))]
#[cfg(feature = "ext-hooks")]
pub(crate) fn parse_record_ext_visitors(attrs: &[Attribute]) -> syn::Result<Vec<Path>> {
    let mut out: Vec<Path> = Vec::new();
    for attr in attrs {
        if attr.path().is_ident("record") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("visit") {
                    // Accept either visit = path or visit(path)
                    // First try key-value form
                    if let Ok(v) = meta.value() {
                        // Support visit = "path::ToVisitor" or visit = path::ToVisitor
                        // Prefer string literal for robust attribute grammar
                        if let Ok(ls) = v.parse::<LitStr>() {
                            let p: Path = syn::parse_str(&ls.value())?;
                            out.push(p);
                        } else {
                            let ep: syn::ExprPath = v.parse()?;
                            out.push(ep.path);
                        }
                    } else {
                        // Fallback to nested meta list
                        meta.parse_nested_meta(|mi| {
                            // Permit either bare ident or full path
                            let p: Path = if let Some(id) = mi.path.get_ident() {
                                let mut path = Path {
                                    leading_colon: None,
                                    segments: Default::default(),
                                };
                                path.segments.push(syn::PathSegment {
                                    ident: id.clone(),
                                    arguments: syn::PathArguments::None,
                                });
                                path
                            } else {
                                mi.path.clone()
                            };
                            out.push(p);
                            Ok(())
                        })?;
                    }
                } else {
                    // Consume unknown nested entries to satisfy parser
                    if let Ok(v) = meta.value() {
                        let _expr: syn::Expr = v.parse()?;
                    } else {
                        meta.parse_nested_meta(|inner| {
                            if let Ok(v2) = inner.value() {
                                let _expr: syn::Expr = v2.parse()?;
                            } else {
                                let _ = inner.parse_nested_meta(|_| Ok(()));
                            }
                            Ok(())
                        })?;
                    }
                }
                Ok(())
            })?;
        }
    }
    Ok(out)
}

// Container-level: #[record(field_macro = my_ext::per_field)] (repeatable)
#[cfg(feature = "ext-hooks")]
pub(crate) fn parse_record_field_macros(attrs: &[Attribute]) -> syn::Result<Vec<Path>> {
    let mut out: Vec<Path> = Vec::new();
    for attr in attrs {
        if attr.path().is_ident("record") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("field_macro") {
                    if let Ok(v) = meta.value() {
                        if let Ok(ls) = v.parse::<LitStr>() {
                            let p: Path = syn::parse_str(&ls.value())?;
                            out.push(p);
                        } else {
                            let ep: syn::ExprPath = v.parse()?;
                            out.push(ep.path);
                        }
                    } else {
                        meta.parse_nested_meta(|mi| {
                            let p: Path = mi.path.clone();
                            out.push(p);
                            Ok(())
                        })?;
                    }
                } else if let Ok(v) = meta.value() {
                    let _expr: syn::Expr = v.parse()?;
                } else {
                    meta.parse_nested_meta(|inner| {
                        if let Ok(v2) = inner.value() {
                            let _expr: syn::Expr = v2.parse()?;
                        } else {
                            let _ = inner.parse_nested_meta(|_| Ok(()));
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            })?;
        }
    }
    Ok(out)
}

// Container-level: #[record(record_macro = my_ext::per_record)] (repeatable)
#[cfg(feature = "ext-hooks")]
pub(crate) fn parse_record_record_macros(attrs: &[Attribute]) -> syn::Result<Vec<Path>> {
    let mut out: Vec<Path> = Vec::new();
    for attr in attrs {
        if attr.path().is_ident("record") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("record_macro") {
                    if let Ok(v) = meta.value() {
                        if let Ok(ls) = v.parse::<LitStr>() {
                            let p: Path = syn::parse_str(&ls.value())?;
                            out.push(p);
                        } else {
                            let ep: syn::ExprPath = v.parse()?;
                            out.push(ep.path);
                        }
                    } else {
                        meta.parse_nested_meta(|mi| {
                            let p: Path = mi.path.clone();
                            out.push(p);
                            Ok(())
                        })?;
                    }
                } else if let Ok(v) = meta.value() {
                    let _expr: syn::Expr = v.parse()?;
                } else {
                    meta.parse_nested_meta(|inner| {
                        if let Ok(v2) = inner.value() {
                            let _expr: syn::Expr = v2.parse()?;
                        } else {
                            let _ = inner.parse_nested_meta(|_| Ok(()));
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            })?;
        }
    }
    Ok(out)
}

// Container-level: #[record(record_fields_macro = my_ext::per_record_fields)] (repeatable)
// Allows passing the entire field list and types to a user macro.
#[cfg(feature = "ext-hooks")]
pub(crate) fn parse_record_fields_macros(attrs: &[Attribute]) -> syn::Result<Vec<Path>> {
    let mut out: Vec<Path> = Vec::new();
    for attr in attrs {
        if attr.path().is_ident("record") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("record_fields_macro") {
                    if let Ok(v) = meta.value() {
                        if let Ok(ls) = v.parse::<LitStr>() {
                            let p: Path = syn::parse_str(&ls.value())?;
                            out.push(p);
                        } else {
                            let ep: syn::ExprPath = v.parse()?;
                            out.push(ep.path);
                        }
                    } else {
                        meta.parse_nested_meta(|mi| {
                            let p: Path = mi.path.clone();
                            out.push(p);
                            Ok(())
                        })?;
                    }
                } else if let Ok(v) = meta.value() {
                    let _expr: syn::Expr = v.parse()?;
                } else {
                    meta.parse_nested_meta(|inner| {
                        if let Ok(v2) = inner.value() {
                            let _expr: syn::Expr = v2.parse()?;
                        } else {
                            let _ = inner.parse_nested_meta(|_| Ok(()));
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            })?;
        }
    }
    Ok(out)
}

// Container-level: #[record(ext(...))] → capture tokens for forwarding
#[cfg(feature = "ext-hooks")]
pub(crate) fn parse_ext_token_list_on_record(
    attrs: &[Attribute],
) -> syn::Result<Option<Vec<proc_macro2::TokenStream>>> {
    let mut out: Option<Vec<proc_macro2::TokenStream>> = None;
    for attr in attrs {
        if attr.path().is_ident("record") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("ext") {
                    let mut items: Vec<proc_macro2::TokenStream> = Vec::new();
                    meta.parse_nested_meta(|inner| {
                        let ts = inner.path.to_token_stream();
                        items.push(ts);
                        Ok(())
                    })?;
                    if !items.is_empty() {
                        out.get_or_insert_with(Vec::new).extend(items);
                    }
                } else {
                    // Consume others to avoid parse errors when combined with ext
                    if let Ok(v) = meta.value() {
                        let _expr: syn::Expr = v.parse()?;
                    } else if meta.input.is_empty() {
                        // bare flag
                    } else {
                        meta.parse_nested_meta(|inner| {
                            if let Ok(v2) = inner.value() {
                                let _expr: syn::Expr = v2.parse()?;
                            } else if inner.input.is_empty() {
                                // bare flag
                            } else {
                                let _ = inner.parse_nested_meta(|_| Ok(()));
                            }
                            Ok(())
                        })?;
                    }
                }
                Ok(())
            })?;
        }
    }
    Ok(out)
}

// Field-level: #[record(ext(...))] → capture tokens for forwarding
#[cfg(feature = "ext-hooks")]
pub(crate) fn parse_ext_token_list_on_field(
    attrs: &[Attribute],
) -> syn::Result<Option<Vec<proc_macro2::TokenStream>>> {
    let mut out: Option<Vec<proc_macro2::TokenStream>> = None;
    for attr in attrs {
        if attr.path().is_ident("record") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("ext") {
                    let mut items: Vec<proc_macro2::TokenStream> = Vec::new();
                    meta.parse_nested_meta(|inner| {
                        let ts = inner.path.to_token_stream();
                        items.push(ts);
                        Ok(())
                    })?;
                    if !items.is_empty() {
                        out.get_or_insert_with(Vec::new).extend(items);
                    }
                } else {
                    // Consume others
                    if let Ok(v) = meta.value() {
                        let _expr: syn::Expr = v.parse()?;
                    } else if meta.input.is_empty() {
                        // bare flag
                    } else {
                        meta.parse_nested_meta(|inner| {
                            if let Ok(v2) = inner.value() {
                                let _expr: syn::Expr = v2.parse()?;
                            } else if inner.input.is_empty() {
                                // bare flag
                            } else {
                                let _ = inner.parse_nested_meta(|_| Ok(()));
                            }
                            Ok(())
                        })?;
                    }
                }
                Ok(())
            })?;
        }
    }
    Ok(out)
}
