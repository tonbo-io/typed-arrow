//! Proc-macros for typed-arrow: `#[derive(Record)]` and `#[derive(Union)]`.

use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{
    parse_macro_input, Attribute, Data, DataEnum, DataStruct, DeriveInput, Fields, Ident, LitStr,
    Type,
};

#[proc_macro_derive(Record, attributes(nested, schema_metadata, metadata, record))]
pub fn derive_record(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match impl_record(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.into_compile_error().into(),
    }
}

fn impl_record(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let builders_ident = Ident::new(&format!("{name}Builders"), name.span());
    let arrays_ident = Ident::new(&format!("{name}Arrays"), name.span());

    let Data::Struct(DataStruct {
        fields: Fields::Named(fields),
        ..
    }) = &input.data
    else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "#[derive(Record)] only supports structs with named fields",
        ));
    };

    let len = fields.named.len();
    let mut col_impls = Vec::with_capacity(len);
    let mut visit_calls = Vec::with_capacity(len);

    let mut child_field_stmts = Vec::with_capacity(len);
    let mut child_builder_stmts = Vec::with_capacity(len);

    // Row-builders supporting code
    let mut builder_struct_fields = Vec::with_capacity(len);
    let mut arrays_struct_fields = Vec::with_capacity(len);
    let mut builders_init_fields = Vec::with_capacity(len);
    let mut append_row_stmts = Vec::with_capacity(len);
    let mut finish_fields = Vec::with_capacity(len);
    let mut field_idents: Vec<&Ident> = Vec::with_capacity(len);
    let mut append_struct_owned_stmts = Vec::with_capacity(len);
    let mut append_struct_null_stmts = Vec::with_capacity(len);
    let mut append_null_row_stmts = Vec::with_capacity(len);

    // Parse top-level schema metadata from struct attributes
    let schema_meta_pairs = parse_schema_metadata_pairs(&input.attrs)?;
    let schema_meta_inserts = schema_meta_pairs
        .iter()
        .map(|(k, v)| {
            quote! { __m.insert(::std::string::String::from(#k), ::std::string::String::from(#v)); }
        })
        .collect::<Vec<_>>();

    for (i, f) in fields.named.iter().enumerate() {
        let idx = syn::Index::from(i);
        let fname = f.ident.as_ref().expect("named");
        field_idents.push(fname);
        let (inner_ty, nullable) = unwrap_option(&f.ty);
        let is_nested = has_nested_attr(&f.attrs)?;

        let inner_ty_ts = inner_ty.to_token_stream();
        let nullable_lit = if nullable {
            quote!(true)
        } else {
            quote!(false)
        };

        // impl ColAt<I> for Type
        let col_impl = quote! {
            impl ::typed_arrow::schema::ColAt<{ #idx }> for #name {
                type Rust = #inner_ty_ts;
                type ColumnArray = < #inner_ty_ts as ::typed_arrow::bridge::ArrowBinding >::Array;
                type ColumnBuilder = < #inner_ty_ts as ::typed_arrow::bridge::ArrowBinding >::Builder;
                const NULLABLE: bool = #nullable_lit;
                const NAME: &'static str = stringify!(#fname);
                fn data_type() -> ::arrow_schema::DataType { < #inner_ty_ts as ::typed_arrow::bridge::ArrowBinding >::data_type() }
            }
        };
        col_impls.push(col_impl);

        // V::visit::<I, Arrow, Rust>(FieldMeta::new(name, nullable))
        let visit = quote! {
            V::visit::<{ #idx }, #inner_ty_ts>(
                ::typed_arrow::schema::FieldMeta::new(stringify!(#fname), #nullable_lit)
            );
        };
        visit_calls.push(visit);

        // Field-level metadata
        let field_meta_pairs = parse_field_metadata_pairs(&f.attrs)?;

        // StructMeta: child Field (with optional metadata)
        if let Some(pairs) = &field_meta_pairs {
            let inserts = pairs.iter().map(|(k, v)| {
                quote! { __m.insert(::std::string::String::from(#k), ::std::string::String::from(#v)); }
            });
            child_field_stmts.push(quote! {
                let mut __f = ::arrow_schema::Field::new(
                    stringify!(#fname),
                    <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::data_type(),
                    #nullable_lit,
                );
                let mut __m: ::std::collections::HashMap<::std::string::String, ::std::string::String> = ::std::collections::HashMap::new();
                #(#inserts)*
                __f = __f.with_metadata(__m);
                fields.push(__f);
            });
        } else {
            child_field_stmts.push(quote! {
                fields.push(::arrow_schema::Field::new(
                    stringify!(#fname),
                    <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::data_type(),
                    #nullable_lit,
                ));
            });
        }

        // StructMeta: child builder boxed as ArrayBuilder
        child_builder_stmts.push(quote! {
            let b: <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::Builder =
                <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::new_builder(capacity);
            builders.push(Box::new(b));
        });

        // Row-based: struct fields and init
        builder_struct_fields.push(quote! {
            pub #fname: <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::Builder
        });
        arrays_struct_fields.push(quote! {
            pub #fname: <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::Array
        });
        builders_init_fields.push(quote! {
            #fname: <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::new_builder(capacity)
        });
        // Append row logic per field
        if is_nested {
            if nullable {
                append_row_stmts.push(quote! {
                    match #fname {
                        Some(v) => {
                            <#inner_ty_ts as ::typed_arrow::schema::AppendStruct>::append_owned_into(v, &mut self.#fname);
                            self.#fname.append(true);
                        }
                        None => {
                            <#inner_ty_ts as ::typed_arrow::schema::AppendStruct>::append_null_into(&mut self.#fname);
                            self.#fname.append(false);
                        }
                    }
                });
                // Null-row handling for nested optional struct field: append nulls to children then
                // mark invalid
                append_null_row_stmts.push(quote! {
                    <#inner_ty_ts as ::typed_arrow::schema::AppendStruct>::append_null_into(&mut self.#fname);
                    self.#fname.append(false);
                });
            } else {
                append_row_stmts.push(quote! {
                    <#inner_ty_ts as ::typed_arrow::schema::AppendStruct>::append_owned_into(#fname, &mut self.#fname);
                    self.#fname.append(true);
                });
                // Null-row handling for nested required struct field: append nulls to children then
                // mark invalid
                append_null_row_stmts.push(quote! {
                    <#inner_ty_ts as ::typed_arrow::schema::AppendStruct>::append_null_into(&mut self.#fname);
                    self.#fname.append(false);
                });
            }
        } else if nullable {
            append_row_stmts.push(quote! {
                match #fname {
                    Some(v) => <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::append_value(&mut self.#fname, &v),
                    None => <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::append_null(&mut self.#fname),
                }
            });
            append_null_row_stmts.push(quote! {
                <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::append_null(&mut self.#fname);
            });
        } else {
            append_row_stmts.push(quote! {
                <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::append_value(&mut self.#fname, &#fname);
            });
            append_null_row_stmts.push(quote! {
                <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::append_null(&mut self.#fname);
            });
        }
        finish_fields.push(quote! {
            #fname: <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::finish(self.#fname)
        });

        // Generate AppendStruct implementations' bodies for this struct's fields
        let child_builder_ty =
            quote! { <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::Builder };
        if is_nested {
            if nullable {
                append_struct_owned_stmts.push(quote! {
                    let cb: &mut #child_builder_ty = __sb
                        .field_builder::<#child_builder_ty>({ #idx })
                        .expect("child builder type matches");
                    match #fname {
                        Some(v) => {
                            <#inner_ty_ts as ::typed_arrow::schema::AppendStruct>::append_owned_into(v, cb);
                            cb.append(true);
                        }
                        None => {
                            <#inner_ty_ts as ::typed_arrow::schema::AppendStruct>::append_null_into(cb);
                            cb.append(false);
                        }
                    }
                });
            } else {
                append_struct_owned_stmts.push(quote! {
                    let cb: &mut #child_builder_ty = __sb
                        .field_builder::<#child_builder_ty>({ #idx })
                        .expect("child builder type matches");
                    <#inner_ty_ts as ::typed_arrow::schema::AppendStruct>::append_owned_into(#fname, cb);
                    cb.append(true);
                });
            }
            append_struct_null_stmts.push(quote! {
                let cb: &mut #child_builder_ty = __sb
                    .field_builder::<#child_builder_ty>({ #idx })
                    .expect("child builder type matches");
                <#inner_ty_ts as ::typed_arrow::schema::AppendStruct>::append_null_into(cb);
                cb.append(false);
            });
        } else {
            if nullable {
                append_struct_owned_stmts.push(quote! {
                    let cb: &mut #child_builder_ty = __sb
                        .field_builder::<#child_builder_ty>({ #idx })
                        .expect("child builder type matches");
                    match #fname {
                        Some(v) => <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::append_value(cb, &v),
                        None => <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::append_null(cb),
                    }
                });
            } else {
                append_struct_owned_stmts.push(quote! {
                    let cb: &mut #child_builder_ty = __sb
                        .field_builder::<#child_builder_ty>({ #idx })
                        .expect("child builder type matches");
                    <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::append_value(cb, &#fname);
                });
            }
            append_struct_null_stmts.push(quote! {
                let cb: &mut #child_builder_ty = __sb
                    .field_builder::<#child_builder_ty>({ #idx })
                    .expect("child builder type matches");
                <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::append_null(cb);
            });
        }
    }

    // impl Record and ForEachCol
    let rec_impl = quote! {
        impl ::typed_arrow::schema::Record for #name {
            const LEN: usize = #len;
        }

        impl ::typed_arrow::schema::ForEachCol for #name {
            fn for_each_col<V: ::typed_arrow::schema::ColumnVisitor>() {
                #(#visit_calls)*
            }
        }

        impl ::typed_arrow::schema::StructMeta for #name {
            fn child_fields() -> ::std::vec::Vec<::arrow_schema::Field> {
                let mut fields = ::std::vec::Vec::with_capacity(#len);
                #(#child_field_stmts)*
                fields
            }

            fn new_struct_builder(capacity: usize) -> ::arrow_array::builder::StructBuilder {
                use ::std::sync::Arc;
                let fields: ::std::vec::Vec<Arc<::arrow_schema::Field>> =
                    <#name as ::typed_arrow::schema::StructMeta>::child_fields()
                        .into_iter()
                        .map(Arc::new)
                        .collect();
                let mut builders: ::std::vec::Vec<Box<dyn ::arrow_array::builder::ArrayBuilder>> =
                    ::std::vec::Vec::with_capacity(#len);
                #(#child_builder_stmts)*
                ::arrow_array::builder::StructBuilder::new(fields, builders)
            }
        }

        impl ::typed_arrow::schema::SchemaMeta for #name {
            fn fields() -> ::std::vec::Vec<::arrow_schema::Field> {
                let mut fields = ::std::vec::Vec::with_capacity(#len);
                #(#child_field_stmts)*
                fields
            }
            fn metadata() -> ::std::collections::HashMap<::std::string::String, ::std::string::String> {
                let mut __m: ::std::collections::HashMap<::std::string::String, ::std::string::String> = ::std::collections::HashMap::new();
                #(#schema_meta_inserts)*
                __m
            }
        }

        // Row-based: builders + arrays + construction
        pub struct #builders_ident {
            #(#builder_struct_fields,)*
        }

        pub struct #arrays_ident {
            #(#arrays_struct_fields,)*
        }

        impl ::typed_arrow::schema::BuildRows for #name {
            type Builders = #builders_ident;
            type Arrays = #arrays_ident;
            fn new_builders(capacity: usize) -> Self::Builders {
                #builders_ident { #(#builders_init_fields,)* }
            }
        }

        impl #builders_ident {
            pub fn append_row(&mut self, row: #name) {
                let #name { #( #field_idents ),* } = row;
                #(#append_row_stmts)*
            }
            pub fn append_null_row(&mut self) {
                #(#append_null_row_stmts)*
            }
            pub fn append_option_row(&mut self, row: ::core::option::Option<#name>) {
                match row {
                    ::core::option::Option::Some(r) => self.append_row(r),
                    ::core::option::Option::None => self.append_null_row(),
                }
            }
            pub fn append_rows<I: ::core::iter::IntoIterator<Item = #name>>(&mut self, rows: I) {
                for r in rows { self.append_row(r); }
            }
            pub fn append_option_rows<I: ::core::iter::IntoIterator<Item = ::core::option::Option<#name>>>(&mut self, rows: I) {
                for r in rows { self.append_option_row(r); }
            }
            pub fn finish(self) -> #arrays_ident {
                #arrays_ident { #(#finish_fields,)* }
            }
        }

        impl #arrays_ident {
            /// Build an Arrow RecordBatch from these arrays and the generated schema.
            pub fn into_record_batch(self) -> ::arrow_array::RecordBatch {
                use ::std::sync::Arc;
                let schema = <#name as ::typed_arrow::schema::SchemaMeta>::schema();
                let mut cols: ::std::vec::Vec<Arc<dyn ::arrow_array::Array>> = ::std::vec::Vec::with_capacity(#len);
                #( cols.push(Arc::new(self.#field_idents)); )*
                ::arrow_array::RecordBatch::try_new(schema, cols).expect("valid record batch")
            }
        }

        impl ::typed_arrow::schema::AppendStruct for #name {
            fn append_owned_into(self, __sb: &mut ::arrow_array::builder::StructBuilder) {
                let #name { #( #field_idents ),* } = self;
                #(#append_struct_owned_stmts)*
            }
            fn append_null_into(__sb: &mut ::arrow_array::builder::StructBuilder) {
                #(#append_struct_null_stmts)*
            }
        }
    };

    let expanded = quote! {
        #(#col_impls)*
        #rec_impl
    };
    Ok(expanded)
}

/// Derive a Dense Arrow Union for an enum.
///
/// Constraints (initial MVP):
/// - Only enums with tuple variants containing exactly 1 field are supported.
/// - Dense mode only; union-level nulls are encoded into the first variant.
#[proc_macro_derive(Union, attributes(union))]
pub fn derive_union(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match impl_union_dense(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.into_compile_error().into(),
    }
}

fn impl_union_dense(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
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
    let var_names: Vec<String> = var_idents.iter().map(|i| i.to_string()).collect();
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
        field_pairs.push(quote! { (#tag, ::std::sync::Arc::new(::arrow_schema::Field::new(#v_name_str, <#v_ty as ::typed_arrow::bridge::ArrowBinding>::data_type(), true))) });
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
            ::std::sync::Arc::new(<#vty as ::typed_arrow::bridge::ArrowBinding>::finish(b.#bident)) as ::arrow_array::ArrayRef
        });
        children_finish_reset.push(quote! {
            ::std::sync::Arc::new(<#vty as ::typed_arrow::bridge::ArrowBinding>::finish(::std::mem::replace(&mut self.#bident, <#vty as ::typed_arrow::bridge::ArrowBinding>::new_builder(0)))) as ::arrow_array::ArrayRef
        });
        let vtyc = &var_types_clone[i];
        let bidentc = &builder_idents_clone[i];
        children_finish_cloned.push(quote! {
            <<#vtyc as ::typed_arrow::bridge::ArrowBinding>::Builder as ::arrow_array::builder::ArrayBuilder>::finish_cloned(&self.#bidentc)
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
            type Array = ::arrow_array::UnionArray;

            fn data_type() -> ::arrow_schema::DataType {
                let fields: ::arrow_schema::UnionFields = [#(#field_pairs),*].into_iter().collect();
                ::arrow_schema::DataType::Union(fields, ::arrow_schema::UnionMode::Dense)
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
                let children: ::std::vec::Vec<::arrow_array::ArrayRef> = vec![#(
                    #children_finish
                ),*];
                let fields: ::arrow_schema::UnionFields = [#(#field_pairs),*].into_iter().collect();
                let type_ids: ::arrow_buffer::ScalarBuffer<i8> = b.type_ids.into_iter().collect();
                let offsets: ::arrow_buffer::ScalarBuffer<i32> = b.offsets.into_iter().collect();
                ::arrow_array::UnionArray::try_new(fields, type_ids, Some(offsets), children).expect("valid dense union")
            }
        }

        // Implement ArrayBuilder so this union can be used as a struct field builder
        impl ::arrow_array::builder::ArrayBuilder for #builder_ident {
            fn as_any(&self) -> &dyn ::std::any::Any { self }
            fn as_any_mut(&mut self) -> &mut dyn ::std::any::Any { self }
            fn into_box_any(self: ::std::boxed::Box<Self>) -> ::std::boxed::Box<dyn ::std::any::Any> { self }
            fn len(&self) -> usize { self.type_ids.len() }

            fn finish(&mut self) -> ::arrow_array::ArrayRef {
                // Finish children in insertion order and reset builders
                let children: ::std::vec::Vec<::arrow_array::ArrayRef> = vec![
                    #( #children_finish_reset ),*
                ];
                self.slots = [0; #n];

                let fields: ::arrow_schema::UnionFields = [#(#field_pairs),*].into_iter().collect();
                let type_ids: ::arrow_buffer::ScalarBuffer<i8> = ::std::mem::take(&mut self.type_ids).into_iter().collect();
                let offsets: ::arrow_buffer::ScalarBuffer<i32> = ::std::mem::take(&mut self.offsets).into_iter().collect();
                let u = ::arrow_array::UnionArray::try_new(fields, type_ids, Some(offsets), children).expect("valid dense union");
                ::std::sync::Arc::new(u)
            }

            fn finish_cloned(&self) -> ::arrow_array::ArrayRef {
                // Build from current state without resetting child builders
                let children: ::std::vec::Vec<::arrow_array::ArrayRef> = vec![
                    #( #children_finish_cloned ),*
                ];
                let fields: ::arrow_schema::UnionFields = [#(#field_pairs),*].into_iter().collect();
                let type_ids: ::arrow_buffer::ScalarBuffer<i8> = self.type_ids.clone().into_iter().collect();
                let offsets: ::arrow_buffer::ScalarBuffer<i32> = self.offsets.clone().into_iter().collect();
                let u = ::arrow_array::UnionArray::try_new(fields, type_ids, Some(offsets), children).expect("valid dense union");
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
        sparse_children_finish.push(quote! { ::std::sync::Arc::new(<#vty as ::typed_arrow::bridge::ArrowBinding>::finish(b.#bident)) as ::arrow_array::ArrayRef });
        sparse_children_finish_reset.push(quote! { ::std::sync::Arc::new(<#vty as ::typed_arrow::bridge::ArrowBinding>::finish(::std::mem::replace(&mut self.#bident, <#vty as ::typed_arrow::bridge::ArrowBinding>::new_builder(0)))) as ::arrow_array::ArrayRef });
        let vtyc = &var_types_clone[i];
        let bidentc = &builder_idents_clone[i];
        sparse_children_finish_cloned.push(quote! { <<#vtyc as ::typed_arrow::bridge::ArrowBinding>::Builder as ::arrow_array::builder::ArrayBuilder>::finish_cloned(&self.#bidentc) });
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
            type Array = ::arrow_array::UnionArray;

            fn data_type() -> ::arrow_schema::DataType {
                let fields: ::arrow_schema::UnionFields = [#(#field_pairs),*].into_iter().collect();
                ::arrow_schema::DataType::Union(fields, ::arrow_schema::UnionMode::Sparse)
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
                let children: ::std::vec::Vec<::arrow_array::ArrayRef> = vec![#(
                    #sparse_children_finish
                ),*];
                let fields: ::arrow_schema::UnionFields = [#(#field_pairs),*].into_iter().collect();
                let type_ids: ::arrow_buffer::ScalarBuffer<i8> = b.type_ids.into_iter().collect();
                ::arrow_array::UnionArray::try_new(fields, type_ids, None, children).expect("valid sparse union")
            }
        }

        // Implement ArrayBuilder so this union can be used as a struct field builder
        impl ::arrow_array::builder::ArrayBuilder for #builder_ident_sparse {
            fn as_any(&self) -> &dyn ::std::any::Any { self }
            fn as_any_mut(&mut self) -> &mut dyn ::std::any::Any { self }
            fn into_box_any(self: ::std::boxed::Box<Self>) -> ::std::boxed::Box<dyn ::std::any::Any> { self }
            fn len(&self) -> usize { self.type_ids.len() }

            fn finish(&mut self) -> ::arrow_array::ArrayRef {
                let children: ::std::vec::Vec<::arrow_array::ArrayRef> = vec![
                    #( #sparse_children_finish_reset ),*
                ];
                let fields: ::arrow_schema::UnionFields = [#(#field_pairs),*].into_iter().collect();
                let type_ids: ::arrow_buffer::ScalarBuffer<i8> = ::std::mem::take(&mut self.type_ids).into_iter().collect();
                let u = ::arrow_array::UnionArray::try_new(fields, type_ids, None, children).expect("valid sparse union");
                ::std::sync::Arc::new(u)
            }

            fn finish_cloned(&self) -> ::arrow_array::ArrayRef {
                let children: ::std::vec::Vec<::arrow_array::ArrayRef> = vec![
                    #( #sparse_children_finish_cloned ),*
                ];
                let fields: ::arrow_schema::UnionFields = [#(#field_pairs),*].into_iter().collect();
                let type_ids: ::arrow_buffer::ScalarBuffer<i8> = self.type_ids.clone().into_iter().collect();
                let u = ::arrow_array::UnionArray::try_new(fields, type_ids, None, children).expect("valid sparse union");
                ::std::sync::Arc::new(u)
            }
        }
    };

    let gen = if is_sparse { sparse_ts } else { dense_ts };

    Ok(gen)
}

fn has_nested_attr(attrs: &[Attribute]) -> syn::Result<bool> {
    for attr in attrs {
        if attr.path().is_ident("nested") {
            return Ok(true);
        }
        if attr.path().is_ident("record") {
            let mut is_nested = false;
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("nested") {
                    is_nested = true;
                }
                Ok(())
            })?;
            if is_nested {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn unwrap_option(ty: &Type) -> (Type, bool) {
    if let Type::Path(tp) = ty {
        if let Some(seg) = tp.path.segments.last() {
            if seg.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                        return (inner.clone(), true);
                    }
                }
            }
        }
    }
    (ty.clone(), false)
}

// -------- metadata parsing helpers --------

fn parse_schema_metadata_pairs(attrs: &[Attribute]) -> syn::Result<Vec<(String, String)>> {
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
                }
                Ok(())
            })?;
        }
    }
    Ok(out)
}

fn parse_field_metadata_pairs(attrs: &[Attribute]) -> syn::Result<Option<Vec<(String, String)>>> {
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
                }
                Ok(())
            })?;
        }
    }
    Ok(out)
}

// -------- union attribute parsing --------

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
                            if !(i8::MIN as i64..=i8::MAX as i64).contains(&n) {
                                return Err(syn::Error::new_spanned(
                                    &val_expr,
                                    "#[union] tag must fit in i8 (-128..=127)",
                                ));
                            }
                            out.tags.push((name, n as i8));
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
    let known: std::collections::HashSet<&str> = variant_names.iter().map(|s| s.as_str()).collect();
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
                    if !(i8::MIN as i64..=i8::MAX as i64).contains(&n) {
                        return Err(syn::Error::new_spanned(
                            &val_expr,
                            "#[union(tag = ...)] must fit in i8 (-128..=127)",
                        ));
                    }
                    out.tag = Some(n as i8);
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
                // with other variants
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
    for t in tags.iter_mut() {
        if t.is_none() {
            while next < i16::from(i8::MAX) + 1 && used.contains(&(next as i8)) {
                next += 1;
            }
            if next > i16::from(i8::MAX) {
                return Err(syn::Error::new_spanned(
                    enum_ident,
                    "exhausted i8 tag space while auto-assigning tags",
                ));
            }
            let v = next as i8;
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
        // Validate i8 range was already checked
        let _ = idx; // unused in validation
    }

    let field_names: Vec<String> = fields.into_iter().map(|f| f.unwrap()).collect();

    let null_idx = null_idx.unwrap_or(0);

    Ok((tags_i8, field_names, null_idx))
}
