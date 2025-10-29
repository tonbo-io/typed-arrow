use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{Attribute, Data, DataEnum, DeriveInput, Fields, Ident, LitStr};

pub(crate) fn derive_union(input: &DeriveInput) -> TokenStream {
    match impl_union(input) {
        Ok(ts) => ts.into(),
        Err(e) => e.into_compile_error().into(),
    }
}

#[allow(clippy::too_many_lines)]
fn impl_union(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;

    let Data::Enum(DataEnum { variants, .. }) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "#[derive(Union)] only supports enums",
        ));
    };

    if variants.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "#[derive(Union)] requires at least one variant",
        ));
    }

    // Collect variant data: (variant_ident, field_type)
    let mut var_idents: Vec<&Ident> = Vec::new();
    let mut var_types: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut per_variant_attrs: Vec<UnionVariantAttrs> = Vec::new();
    for v in variants {
        var_idents.push(&v.ident);
        match &v.fields {
            Fields::Unnamed(un) if un.unnamed.len() == 1 => {
                let ty = &un.unnamed.first().unwrap().ty;
                var_types.push(ty.to_token_stream());
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    &v.ident,
                    "#[derive(Union)] variants must be tuple variants with exactly 1 field",
                ));
            }
        }

        // Parse per-variant #[union(...)] attributes
        per_variant_attrs.push(parse_union_variant_attrs(&v.attrs)?);
    }

    let n = var_idents.len();
    let builder_ident = Ident::new(&format!("{name}UnionDenseBuilder"), name.span());

    // Container-level attributes on the enum itself
    let var_names: Vec<String> = var_idents.iter().map(ToString::to_string).collect();
    let cont_attrs = parse_union_container_attrs(&input.attrs, &var_names)?;
    let is_sparse = matches!(cont_attrs.mode.as_deref(), Some("sparse"));

    // Resolve tags, field names, and null-carrying variant index
    let (tags_i8, field_names, null_idx) =
        resolve_union_config(&var_names, &per_variant_attrs, &cont_attrs, name)?;
    let null_tag: i8 = tags_i8[null_idx];

    // Generate fields for per-variant builders and slots
    let mut builder_fields = Vec::with_capacity(n);
    let mut builder_inits = Vec::with_capacity(n);
    let mut builder_finish_children = Vec::with_capacity(n);
    let mut builder_idents: Vec<Ident> = Vec::with_capacity(n);
    let mut match_arms_append = Vec::with_capacity(n);
    let mut field_pairs = Vec::with_capacity(n);
    for (idx, (v_ident, v_ty)) in var_idents.iter().zip(var_types.iter()).enumerate() {
        let bname = Ident::new(&format!("b{idx}"), name.span());
        builder_idents.push(bname.clone());
        let tag = tags_i8[idx];
        builder_fields
            .push(quote! { #bname: <#v_ty as ::typed_arrow::bridge::ArrowBinding>::Builder });
        builder_inits.push(quote! { #bname: <#v_ty as ::typed_arrow::bridge::ArrowBinding>::new_builder(capacity) });
        builder_finish_children
            .push(quote! { <#v_ty as ::typed_arrow::bridge::ArrowBinding>::finish(b.#bname) });

        // Variant match arm
        match_arms_append.push(quote! {
            #name::#v_ident(inner) => {
                b.type_ids.push(#tag as i8);
                b.offsets.push(b.slots[#idx] as i32);
                <#v_ty as ::typed_arrow::bridge::ArrowBinding>::append_value(&mut b.#bname, inner);
                b.slots[#idx] += 1;
            }
        });

        // Field pair for UnionFields
        let v_name_str = &field_names[idx];
        field_pairs.push(quote! { (#tag, ::std::sync::Arc::new(::typed_arrow::arrow_schema::Field::new(#v_name_str, <#v_ty as ::typed_arrow::bridge::ArrowBinding>::data_type(), true))) });
    }

    // Null-carrying variant type used for encoding nulls
    let null_variant_ty = &var_types[null_idx];
    let null_variant_builder_ident = Ident::new(&format!("b{null_idx}"), name.span());

    // For generating per-variant code in ArrayBuilder impls
    let builder_idents_clone = builder_idents.clone();
    let var_types_clone = var_types.clone();

    // Precompute children expressions for different finish paths (in insertion/variant order)
    let mut children_finish: Vec<proc_macro2::TokenStream> = Vec::with_capacity(n);
    let mut children_finish_reset: Vec<proc_macro2::TokenStream> = Vec::with_capacity(n);
    let mut children_finish_cloned: Vec<proc_macro2::TokenStream> = Vec::with_capacity(n);
    for i in 0..n {
        let bident = &builder_idents[i];
        let vty = &var_types[i];
        children_finish.push(quote! {
            ::std::sync::Arc::new(<#vty as ::typed_arrow::bridge::ArrowBinding>::finish(b.#bident)) as ::typed_arrow::arrow_array::ArrayRef
        });
        children_finish_reset.push(quote! {
            ::std::sync::Arc::new(<#vty as ::typed_arrow::bridge::ArrowBinding>::finish(::std::mem::replace(&mut self.#bident, <#vty as ::typed_arrow::bridge::ArrowBinding>::new_builder(0)))) as ::typed_arrow::arrow_array::ArrayRef
        });
        let vtyc = &var_types_clone[i];
        let bidentc = &builder_idents_clone[i];
        children_finish_cloned.push(quote! {
            <<#vtyc as ::typed_arrow::bridge::ArrowBinding>::Builder as ::typed_arrow::arrow_array::builder::ArrayBuilder>::finish_cloned(&self.#bidentc)
        });
    }

    // Dense codegen block
    let dense_ts = quote! {
        /// Dense union builder generated by `#[derive(Union)]`
        pub struct #builder_ident {
            type_ids: ::std::vec::Vec<i8>,
            offsets: ::std::vec::Vec<i32>,
            slots: [usize; #n],
            #(#builder_fields,)*
        }

        impl #builder_ident {
            fn with_capacity(capacity: usize) -> Self {
                Self {
                    type_ids: ::std::vec::Vec::with_capacity(capacity),
                    offsets: ::std::vec::Vec::with_capacity(capacity),
                    slots: [0; #n],
                    #(#builder_inits,)*
                }
            }
        }

        impl ::typed_arrow::bridge::ArrowBinding for #name {
            type Builder = #builder_ident;
            type Array = ::typed_arrow::arrow_array::UnionArray;

            fn data_type() -> ::typed_arrow::arrow_schema::DataType {
                let fields: ::typed_arrow::arrow_schema::UnionFields = [#(#field_pairs),*].into_iter().collect();
                ::typed_arrow::arrow_schema::DataType::Union(fields, ::typed_arrow::arrow_schema::UnionMode::Dense)
            }

            fn new_builder(capacity: usize) -> Self::Builder {
                #builder_ident::with_capacity(capacity)
            }

            fn append_value(b: &mut Self::Builder, v: &Self) {
                match v { #(#match_arms_append,)* }
            }

            fn append_null(b: &mut Self::Builder) {
                // Encode nulls into the configured null-carrying variant
                b.type_ids.push(#null_tag);
                b.offsets.push(b.slots[#null_idx] as i32);
                <#null_variant_ty as ::typed_arrow::bridge::ArrowBinding>::append_null(&mut b.#null_variant_builder_ident);
                b.slots[#null_idx] += 1;
            }

            fn finish(mut b: Self::Builder) -> Self::Array {
                // Finish children in insertion order (must match fields order)
                let children: ::std::vec::Vec<::typed_arrow::arrow_array::ArrayRef> = vec![#(
                    #children_finish
                ),*];
                let fields: ::typed_arrow::arrow_schema::UnionFields = [#(#field_pairs),*].into_iter().collect();
                let type_ids: ::typed_arrow::arrow_buffer::ScalarBuffer<i8> = b.type_ids.into_iter().collect();
                let offsets: ::typed_arrow::arrow_buffer::ScalarBuffer<i32> = b.offsets.into_iter().collect();
                ::typed_arrow::arrow_array::UnionArray::try_new(fields, type_ids, Some(offsets), children).expect("valid dense union")
            }
        }

        // Implement ArrayBuilder so this union can be used as a struct field builder
        impl ::typed_arrow::arrow_array::builder::ArrayBuilder for #builder_ident {
            fn as_any(&self) -> &dyn ::std::any::Any { self }
            fn as_any_mut(&mut self) -> &mut dyn ::std::any::Any { self }
            fn into_box_any(self: ::std::boxed::Box<Self>) -> ::std::boxed::Box<dyn ::std::any::Any> { self }
            fn len(&self) -> usize { self.type_ids.len() }

            fn finish(&mut self) -> ::typed_arrow::arrow_array::ArrayRef {
                // Finish children in insertion order and reset builders
                let children: ::std::vec::Vec<::typed_arrow::arrow_array::ArrayRef> = vec![
                    #( #children_finish_reset ),*
                ];
                self.slots = [0; #n];

                let fields: ::typed_arrow::arrow_schema::UnionFields = [#(#field_pairs),*].into_iter().collect();
                let type_ids: ::typed_arrow::arrow_buffer::ScalarBuffer<i8> = ::std::mem::take(&mut self.type_ids).into_iter().collect();
                let offsets: ::typed_arrow::arrow_buffer::ScalarBuffer<i32> = ::std::mem::take(&mut self.offsets).into_iter().collect();
                let u = ::typed_arrow::arrow_array::UnionArray::try_new(fields, type_ids, Some(offsets), children).expect("valid dense union");
                ::std::sync::Arc::new(u)
            }

            fn finish_cloned(&self) -> ::typed_arrow::arrow_array::ArrayRef {
                // Build from current state without resetting child builders
                let children: ::std::vec::Vec<::typed_arrow::arrow_array::ArrayRef> = vec![
                    #( #children_finish_cloned ),*
                ];
                let fields: ::typed_arrow::arrow_schema::UnionFields = [#(#field_pairs),*].into_iter().collect();
                let type_ids: ::typed_arrow::arrow_buffer::ScalarBuffer<i8> = self.type_ids.clone().into_iter().collect();
                let offsets: ::typed_arrow::arrow_buffer::ScalarBuffer<i32> = self.offsets.clone().into_iter().collect();
                let u = ::typed_arrow::arrow_array::UnionArray::try_new(fields, type_ids, Some(offsets), children).expect("valid dense union");
                ::std::sync::Arc::new(u)
            }
        }
    };

    // Sparse codegen block
    let builder_ident_sparse = Ident::new(&format!("{name}UnionSparseBuilder"), name.span());

    // Match arms for sparse: append to active child, null to others
    let mut sparse_match_arms = Vec::with_capacity(n);
    for (idx, (v_ident, v_ty)) in var_idents.iter().zip(var_types.iter()).enumerate() {
        let tag = tags_i8[idx];
        let mut null_others = Vec::with_capacity(n - 1);
        for (j, v_ty_j) in var_types.iter().enumerate() {
            if j != idx {
                let bj = Ident::new(&format!("b{j}"), name.span());
                null_others.push(quote! { <#v_ty_j as ::typed_arrow::bridge::ArrowBinding>::append_null(&mut b.#bj); });
            }
        }
        let bi = Ident::new(&format!("b{idx}"), name.span());
        sparse_match_arms.push(quote! {
            #name::#v_ident(inner) => {
                b.type_ids.push(#tag);
                <#v_ty as ::typed_arrow::bridge::ArrowBinding>::append_value(&mut b.#bi, inner);
                #(#null_others)*
            }
        });
    }

    // Append null pushes nulls to all children
    let mut sparse_append_null_all = Vec::with_capacity(n);
    for (j, v_ty_j) in var_types.iter().enumerate() {
        let bj = Ident::new(&format!("b{j}"), name.span());
        sparse_append_null_all.push(
            quote! { <#v_ty_j as ::typed_arrow::bridge::ArrowBinding>::append_null(&mut b.#bj); },
        );
    }

    let mut sparse_children_finish: Vec<proc_macro2::TokenStream> = Vec::with_capacity(n);
    let mut sparse_children_finish_reset: Vec<proc_macro2::TokenStream> = Vec::with_capacity(n);
    let mut sparse_children_finish_cloned: Vec<proc_macro2::TokenStream> = Vec::with_capacity(n);
    for i in 0..n {
        let bident = &builder_idents[i];
        let vty = &var_types[i];
        sparse_children_finish.push(quote! { ::std::sync::Arc::new(<#vty as ::typed_arrow::bridge::ArrowBinding>::finish(b.#bident)) as ::typed_arrow::arrow_array::ArrayRef });
        sparse_children_finish_reset.push(quote! { ::std::sync::Arc::new(<#vty as ::typed_arrow::bridge::ArrowBinding>::finish(::std::mem::replace(&mut self.#bident, <#vty as ::typed_arrow::bridge::ArrowBinding>::new_builder(0)))) as ::typed_arrow::arrow_array::ArrayRef });
        let vtyc = &var_types_clone[i];
        let bidentc = &builder_idents_clone[i];
        sparse_children_finish_cloned.push(quote! { <<#vtyc as ::typed_arrow::bridge::ArrowBinding>::Builder as ::typed_arrow::arrow_array::builder::ArrayBuilder>::finish_cloned(&self.#bidentc) });
    }

    let sparse_ts = quote! {
        /// Sparse union builder generated by `#[derive(Union)]`
        pub struct #builder_ident_sparse {
            type_ids: ::std::vec::Vec<i8>,
            #(#builder_fields,)*
        }

        impl #builder_ident_sparse {
            fn with_capacity(capacity: usize) -> Self {
                Self {
                    type_ids: ::std::vec::Vec::with_capacity(capacity),
                    #(#builder_inits,)*
                }
            }
        }

        impl ::typed_arrow::bridge::ArrowBinding for #name {
            type Builder = #builder_ident_sparse;
            type Array = ::typed_arrow::arrow_array::UnionArray;

            fn data_type() -> ::typed_arrow::arrow_schema::DataType {
                let fields: ::typed_arrow::arrow_schema::UnionFields = [#(#field_pairs),*].into_iter().collect();
                ::typed_arrow::arrow_schema::DataType::Union(fields, ::typed_arrow::arrow_schema::UnionMode::Sparse)
            }

            fn new_builder(capacity: usize) -> Self::Builder {
                #builder_ident_sparse::with_capacity(capacity)
            }

            fn append_value(b: &mut Self::Builder, v: &Self) {
                match v { #(#sparse_match_arms,)* }
            }

            fn append_null(b: &mut Self::Builder) {
                b.type_ids.push(#null_tag);
                #(#sparse_append_null_all)*
            }

            fn finish(mut b: Self::Builder) -> Self::Array {
                let children: ::std::vec::Vec<::typed_arrow::arrow_array::ArrayRef> = vec![#(
                    #sparse_children_finish
                ),*];
                let fields: ::typed_arrow::arrow_schema::UnionFields = [#(#field_pairs),*].into_iter().collect();
                let type_ids: ::typed_arrow::arrow_buffer::ScalarBuffer<i8> = b.type_ids.into_iter().collect();
                ::typed_arrow::arrow_array::UnionArray::try_new(fields, type_ids, None, children).expect("valid sparse union")
            }
        }

        // Implement ArrayBuilder so this union can be used as a struct field builder
        impl ::typed_arrow::arrow_array::builder::ArrayBuilder for #builder_ident_sparse {
            fn as_any(&self) -> &dyn ::std::any::Any { self }
            fn as_any_mut(&mut self) -> &mut dyn ::std::any::Any { self }
            fn into_box_any(self: ::std::boxed::Box<Self>) -> ::std::boxed::Box<dyn ::std::any::Any> { self }
            fn len(&self) -> usize { self.type_ids.len() }

            fn finish(&mut self) -> ::typed_arrow::arrow_array::ArrayRef {
                let children: ::std::vec::Vec<::typed_arrow::arrow_array::ArrayRef> = vec![
                    #( #sparse_children_finish_reset ),*
                ];
                let fields: ::typed_arrow::arrow_schema::UnionFields = [#(#field_pairs),*].into_iter().collect();
                let type_ids: ::typed_arrow::arrow_buffer::ScalarBuffer<i8> = ::std::mem::take(&mut self.type_ids).into_iter().collect();
                let u = ::typed_arrow::arrow_array::UnionArray::try_new(fields, type_ids, None, children).expect("valid sparse union");
                ::std::sync::Arc::new(u)
            }

            fn finish_cloned(&self) -> ::typed_arrow::arrow_array::ArrayRef {
                let children: ::std::vec::Vec<::typed_arrow::arrow_array::ArrayRef> = vec![
                    #( #sparse_children_finish_cloned ),*
                ];
                let fields: ::typed_arrow::arrow_schema::UnionFields = [#(#field_pairs),*].into_iter().collect();
                let type_ids: ::typed_arrow::arrow_buffer::ScalarBuffer<i8> = self.type_ids.clone().into_iter().collect();
                let u = ::typed_arrow::arrow_array::UnionArray::try_new(fields, type_ids, None, children).expect("valid sparse union");
                ::std::sync::Arc::new(u)
            }
        }
    };

    let gen = if is_sparse { sparse_ts } else { dense_ts };

    // Generate View enum for ArrowBindingView
    let view_ident = Ident::new(&format!("{name}View"), name.span());

    // Generate view enum variants
    let mut view_variants = Vec::with_capacity(n);
    for (v_ident, v_ty) in var_idents.iter().zip(var_types.iter()) {
        view_variants.push(quote! {
            #v_ident(<#v_ty as ::typed_arrow::bridge::ArrowBindingView>::View<'a>)
        });
    }

    // Generate match arms for get_view based on type_id
    let mut view_match_arms = Vec::with_capacity(n);
    for (idx, (v_ident, v_ty)) in var_idents.iter().zip(var_types.iter()).enumerate() {
        let tag = tags_i8[idx];
        view_match_arms.push(quote! {
            #tag => {
                let child_array_ref = array.child(#tag);
                let child_array = child_array_ref
                    .as_any()
                    .downcast_ref::<<#v_ty as ::typed_arrow::bridge::ArrowBindingView>::Array>()
                    .ok_or_else(|| ::typed_arrow::schema::ViewAccessError::TypeMismatch {
                        expected: <#v_ty as ::typed_arrow::bridge::ArrowBinding>::data_type(),
                        actual: child_array_ref.data_type().clone(),
                        field_name: ::core::option::Option::Some(stringify!(#v_ident)),
                    })?;
                let value_index = if let Some(offsets) = array.offsets() {
                    // Dense union: use offset
                    offsets[index] as usize
                } else {
                    // Sparse union: use same index
                    index
                };
                ::core::result::Result::Ok(#view_ident::#v_ident(
                    <#v_ty as ::typed_arrow::bridge::ArrowBindingView>::get_view(child_array, value_index)?
                ))
            }
        });
    }

    let view_impl = if cfg!(feature = "views") {
        quote! {
            // View enum for union types
            #[derive(Debug, Clone)]
            pub enum #view_ident<'a> {
                #(#view_variants,)*
            }

            // ArrowBindingView implementation
            impl ::typed_arrow::bridge::ArrowBindingView for #name
            where
                #(#var_types: ::typed_arrow::bridge::ArrowBindingView + 'static,)*
            {
                type Array = ::typed_arrow::arrow_array::UnionArray;
                type View<'a> = #view_ident<'a> where Self: 'a;

                fn get_view(array: &Self::Array, index: usize) -> ::core::result::Result<Self::View<'_>, ::typed_arrow::schema::ViewAccessError> {
                    use ::typed_arrow::arrow_array::Array;
                    if index >= array.len() {
                        return ::core::result::Result::Err(::typed_arrow::schema::ViewAccessError::OutOfBounds {
                            index,
                            len: array.len(),
                            field_name: ::core::option::Option::None,
                        });
                    }
                    if array.is_null(index) {
                        return ::core::result::Result::Err(::typed_arrow::schema::ViewAccessError::UnexpectedNull {
                            index,
                            field_name: ::core::option::Option::None,
                        });
                    }

                    let type_id = array.type_id(index);

                    match type_id {
                        #(#view_match_arms)*
                        _ => ::core::result::Result::Err(::typed_arrow::schema::ViewAccessError::OutOfBounds {
                            index: type_id as usize,
                            len: #n,
                            field_name: ::core::option::Option::Some("union type_id"),
                        }),
                    }
                }
            }

            // TryFrom implementation for converting view to owned
            impl<'a> ::core::convert::TryFrom<#view_ident<'a>> for #name
            where
                #(#var_types: ::typed_arrow::bridge::ArrowBindingView + 'static,)*
            {
                type Error = ::typed_arrow::schema::ViewAccessError;

                fn try_from(view: #view_ident<'a>) -> ::core::result::Result<Self, Self::Error> {
                    match view {
                        #(#view_ident::#var_idents(inner) => ::core::result::Result::Ok(#name::#var_idents(inner.try_into()?)),)*
                    }
                }
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #gen
        #view_impl
    };

    Ok(expanded)
}

#[derive(Default, Debug)]
struct UnionContainerAttrs {
    mode: Option<String>,
    null_variant: Option<String>,
    tags: Vec<(String, i8)>,
}

#[derive(Default, Debug)]
struct UnionVariantAttrs {
    tag: Option<i8>,
    field: Option<String>,
    is_null: bool,
}

fn parse_union_container_attrs(
    attrs: &[Attribute],
    variant_names: &[String],
) -> syn::Result<UnionContainerAttrs> {
    let mut out = UnionContainerAttrs::default();
    for attr in attrs {
        if attr.path().is_ident("union") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("mode") {
                    let s: LitStr = meta.value()?.parse()?;
                    out.mode = Some(s.value());
                } else if meta.path.is_ident("null_variant") {
                    let s: LitStr = meta.value()?.parse()?;
                    out.null_variant = Some(s.value());
                } else if meta.path.is_ident("tags") {
                    meta.parse_nested_meta(|mi| {
                        if let Some(ident) = mi.path.get_ident() {
                            let name = ident.to_string();
                            let val_expr: syn::Expr = mi.value()?.parse()?;
                            let n = eval_i64_expr(&val_expr)?;
                            if !(i64::from(i8::MIN)..=i64::from(i8::MAX)).contains(&n) {
                                return Err(syn::Error::new_spanned(
                                    &val_expr,
                                    "#[union] tag must fit in i8 (-128..=127)",
                                ));
                            }
                            let tag = i8::try_from(n).expect("validated range");
                            out.tags.push((name, tag));
                        }
                        Ok(())
                    })?;
                } else if meta.path.is_ident("fields") {
                    return Err(syn::Error::new_spanned(
                        attr,
                        "container-level #[union(fields(...))] is no longer supported; use \
                         #[union(field = \"name\")] on each variant",
                    ));
                }
                Ok(())
            })?;
        }
    }

    // Validate mode
    if let Some(m) = &out.mode {
        if m != "dense" && m != "sparse" {
            return Err(syn::Error::new_spanned(
                &attrs[0],
                "#[derive(Union)] supports mode=\"dense\" or mode=\"sparse\"",
            ));
        }
    }

    // Validate unknown names in container-level maps
    let known: std::collections::HashSet<&str> = variant_names.iter().map(String::as_str).collect();
    for (k, _) in &out.tags {
        if !known.contains(k.as_str()) {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("#[union(tags(...))] references unknown variant '{k}'"),
            ));
        }
    }
    if let Some(nv) = &out.null_variant {
        if !known.contains(nv.as_str()) {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("#[union(null_variant = \"{nv}\")] references unknown variant"),
            ));
        }
    }

    Ok(out)
}

fn parse_union_variant_attrs(attrs: &[Attribute]) -> syn::Result<UnionVariantAttrs> {
    let mut out = UnionVariantAttrs::default();
    for attr in attrs {
        if attr.path().is_ident("union") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("tag") {
                    let val_expr: syn::Expr = meta.value()?.parse()?;
                    let n = eval_i64_expr(&val_expr)?;
                    if !(i64::from(i8::MIN)..=i64::from(i8::MAX)).contains(&n) {
                        return Err(syn::Error::new_spanned(
                            &val_expr,
                            "#[union(tag = ...)] must fit in i8 (-128..=127)",
                        ));
                    }
                    out.tag = Some(i8::try_from(n).expect("validated range"));
                } else if meta.path.is_ident("field") {
                    let s: LitStr = meta.value()?.parse()?;
                    out.field = Some(s.value());
                } else if meta.path.is_ident("null") {
                    out.is_null = true;
                }
                Ok(())
            })?;
        }
    }
    Ok(out)
}

fn eval_i64_expr(e: &syn::Expr) -> syn::Result<i64> {
    match e {
        syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Int(li),
            ..
        }) => li.base10_parse::<i64>(),
        syn::Expr::Unary(syn::ExprUnary {
            op: syn::UnOp::Neg(_),
            expr,
            ..
        }) => {
            if let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Int(li),
                ..
            }) = &**expr
            {
                let v = li.base10_parse::<i64>()?;
                Ok(-v)
            } else {
                Err(syn::Error::new_spanned(e, "expected integer literal"))
            }
        }
        _ => Err(syn::Error::new_spanned(e, "expected integer literal")),
    }
}

fn resolve_union_config(
    var_names: &[String],
    per_variant: &[UnionVariantAttrs],
    container: &UnionContainerAttrs,
    enum_ident: &Ident,
) -> syn::Result<(Vec<i8>, Vec<String>, usize)> {
    let n = var_names.len();
    // Start with defaults
    let mut tags: Vec<Option<i8>> = vec![None; n];
    let mut fields: Vec<Option<String>> = var_names.iter().map(|_| None).collect();
    let mut null_idx: Option<usize> = None;

    // Container-level defaults
    // tags map
    for (name, tag) in &container.tags {
        if let Some(idx) = var_names.iter().position(|s| s == name) {
            if tags[idx].is_some() {
                return Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    format!("duplicate tag assignment for variant '{name}'"),
                ));
            }
            tags[idx] = Some(*tag);
        }
    }
    // null variant by name
    if let Some(nv) = &container.null_variant {
        let idx = var_names.iter().position(|s| s == nv).unwrap();
        null_idx = Some(idx);
    }

    // Per-variant overrides (take precedence)
    for (idx, va) in per_variant.iter().enumerate() {
        if let Some(t) = va.tag {
            if tags[idx].is_some() {
                // Already assigned by container-level; but variant-level wins if not conflicting
                // with others
                tags[idx] = Some(t);
            } else {
                tags[idx] = Some(t);
            }
        }
        if let Some(f) = &va.field {
            fields[idx] = Some(f.clone());
        }
        if va.is_null {
            if null_idx.is_some() {
                return Err(syn::Error::new_spanned(
                    enum_ident,
                    "multiple null carriers specified (use only one #[union(null)] or \
                     null_variant)",
                ));
            }
            null_idx = Some(idx);
        }
    }

    // Assign default field names
    for (idx, f) in fields.iter_mut().enumerate() {
        if f.is_none() {
            *f = Some(var_names[idx].clone());
        }
    }

    // Assign remaining tags with auto-increment, skipping used
    let mut used: std::collections::HashSet<i8> = tags.iter().filter_map(|&t| t).collect();
    let mut next: i16 = 0; // use wider type to avoid overflow in loop
    for t in &mut tags {
        if t.is_none() {
            while next < i16::from(i8::MAX) + 1
                && used.contains(&i8::try_from(next).unwrap_or(i8::MAX))
            {
                next += 1;
            }
            if next > i16::from(i8::MAX) {
                return Err(syn::Error::new_spanned(
                    enum_ident,
                    "exhausted i8 tag space while auto-assigning tags",
                ));
            }
            let v = i8::try_from(next).expect("<= i8::MAX");
            *t = Some(v);
            used.insert(v);
            next += 1;
        }
    }

    // Now finalize vectors and validate duplicates
    let tags_i8: Vec<i8> = tags.into_iter().map(|t| t.unwrap()).collect();
    let mut seen: std::collections::HashSet<i8> = std::collections::HashSet::new();
    for (idx, t) in tags_i8.iter().enumerate() {
        if !seen.insert(*t) {
            return Err(syn::Error::new_spanned(
                enum_ident,
                format!("duplicate union tag {t} across variants (first seen earlier)"),
            ));
        }
        let _ = idx;
    }

    let field_names: Vec<String> = fields.into_iter().map(|f| f.unwrap()).collect();

    let null_idx = null_idx.unwrap_or(0);

    Ok((tags_i8, field_names, null_idx))
}
