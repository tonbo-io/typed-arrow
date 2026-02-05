use std::collections::HashSet;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{ToTokens, quote};
use syn::{
    Attribute, Data, DataStruct, DeriveInput, Fields, GenericParam, Generics, Ident, Lifetime,
    LifetimeParam, Path, Type, parse_quote, punctuated::Punctuated,
};

#[cfg(feature = "ext-hooks")]
use crate::attrs::parse_ext_token_list_on_field;
#[cfg(feature = "ext-hooks")]
use crate::attrs::parse_ext_token_list_on_record;
#[cfg(feature = "ext-hooks")]
use crate::attrs::parse_record_ext_visitors;
#[cfg(feature = "ext-hooks")]
use crate::attrs::parse_record_field_macros;
#[cfg(feature = "ext-hooks")]
use crate::attrs::parse_record_fields_macros;
#[cfg(feature = "ext-hooks")]
use crate::attrs::parse_record_record_macros;
use crate::attrs::{
    parse_field_metadata_pairs, parse_field_name_override, parse_schema_metadata_pairs,
};

pub(crate) fn derive_record(input: &DeriveInput) -> TokenStream {
    match impl_record(input) {
        Ok(ts) => ts.into(),
        Err(e) => e.into_compile_error().into(),
    }
}

#[allow(clippy::too_many_lines)]
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

    let view_lt = fresh_view_lifetime(&input.generics);
    let generic_type_idents: HashSet<Ident> = input
        .generics
        .params
        .iter()
        .filter_map(|p| match p {
            GenericParam::Type(tp) => Some(tp.ident.clone()),
            _ => None,
        })
        .collect();

    let len = fields.named.len();
    let mut col_impls = Vec::with_capacity(len);
    let mut col_infos = Vec::with_capacity(len);
    let mut visit_calls = Vec::with_capacity(len);

    let mut child_field_stmts = Vec::with_capacity(len);
    let mut child_builder_stmts = Vec::with_capacity(len);

    // Row-builders supporting code
    let mut builder_struct_fields = Vec::with_capacity(len);
    let mut arrays_struct_fields = Vec::with_capacity(len);
    let mut builders_init_fields = Vec::with_capacity(len);
    let mut append_row_stmts = Vec::with_capacity(len);
    let mut append_row_ref_stmts = Vec::with_capacity(len);
    let mut finish_fields = Vec::with_capacity(len);
    let mut field_idents: Vec<&Ident> = Vec::with_capacity(len);
    let mut append_struct_owned_stmts = Vec::with_capacity(len);
    let mut append_struct_null_stmts = Vec::with_capacity(len);
    let mut append_struct_borrowed_stmts = Vec::with_capacity(len);
    let mut append_null_row_stmts = Vec::with_capacity(len);
    let mut inner_tys_for_view = Vec::with_capacity(len);
    let mut try_from_tys_for_view = Vec::with_capacity(len);

    struct ColInfo {
        idx: syn::Index,
        inner_ty_ts: proc_macro2::TokenStream,
        nullable: bool,
        arrow_field_name: String,
    }

    // Parse top-level schema metadata from struct attributes
    let schema_meta_pairs = parse_schema_metadata_pairs(&input.attrs)?;
    let schema_meta_inserts = schema_meta_pairs
        .iter()
        .map(|(k, v)| {
            quote! { __m.insert(::std::string::String::from(#k), ::std::string::String::from(#v)); }
        })
        .collect::<Vec<_>>();

    // Extensibility hooks: behind `ext-hooks` feature (off by default)
    #[cfg(feature = "ext-hooks")]
    let ext_visitors: Vec<Path> = parse_record_ext_visitors(&input.attrs)?;
    #[cfg(not(feature = "ext-hooks"))]
    let ext_visitors: Vec<Path> = Vec::new();

    #[cfg(feature = "ext-hooks")]
    let field_macros: Vec<Path> = parse_record_field_macros(&input.attrs)?;

    #[cfg(feature = "ext-hooks")]
    let record_macros: Vec<Path> = parse_record_record_macros(&input.attrs)?;
    #[cfg(not(feature = "ext-hooks"))]
    let record_macros: Vec<Path> = Vec::new();

    #[cfg(feature = "ext-hooks")]
    let record_fields_macros: Vec<Path> = parse_record_fields_macros(&input.attrs)?;
    #[cfg(not(feature = "ext-hooks"))]
    let record_fields_macros: Vec<Path> = Vec::new();

    #[cfg(feature = "ext-hooks")]
    let record_ext_tokens: Option<Vec<proc_macro2::TokenStream>> =
        parse_ext_token_list_on_record(&input.attrs)?;
    #[cfg(not(feature = "ext-hooks"))]
    let record_ext_tokens: Option<Vec<proc_macro2::TokenStream>> = None;

    // Per-field macro invocations (gated by feature)
    #[cfg(feature = "ext-hooks")]
    let mut field_macro_invocations: Vec<proc_macro2::TokenStream> = Vec::new();
    #[cfg(not(feature = "ext-hooks"))]
    let field_macro_invocations: Vec<proc_macro2::TokenStream> = Vec::new();

    for (i, f) in fields.named.iter().enumerate() {
        let idx = syn::Index::from(i);
        let fname = f.ident.as_ref().expect("named");
        field_idents.push(fname);
        let (inner_ty, nullable) = unwrap_option(&f.ty);
        // Backward-compat cleanup: #[record(nested)] and #[nested] are no longer supported.
        // Nested structs are now the default behavior.
        check_no_legacy_nested_attr(&f.attrs)?;

        // Check for field name override: #[record(name = "...")]
        let field_name_override = parse_field_name_override(&f.attrs)?;
        let arrow_field_name = field_name_override
            .as_ref()
            .map_or_else(|| fname.to_string(), |s| s.clone());

        let inner_ty_ts = inner_ty.to_token_stream();
        inner_tys_for_view.push(inner_ty_ts.clone());
        let needs_try_into = !(is_copy_primitive(&inner_ty)
            || is_string(&inner_ty)
            || is_fixed_size_binary(&inner_ty));
        if needs_try_into && type_contains_generic(&inner_ty, &generic_type_idents) {
            try_from_tys_for_view.push(inner_ty_ts.clone());
        }
        let nullable_lit = if nullable {
            quote!(true)
        } else {
            quote!(false)
        };

        col_infos.push(ColInfo {
            idx: idx.clone(),
            inner_ty_ts: inner_ty_ts.clone(),
            nullable,
            arrow_field_name: arrow_field_name.clone(),
        });

        // V::visit::<I, Arrow, Rust>(FieldMeta::new(name, nullable))
        let visit = quote! {
            V::visit::<{ #idx }, #inner_ty_ts>(
                ::typed_arrow::schema::FieldMeta::new(#arrow_field_name, #nullable_lit)
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
                let mut __f = ::typed_arrow::arrow_schema::Field::new(
                    #arrow_field_name,
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
                fields.push(::typed_arrow::arrow_schema::Field::new(
                    #arrow_field_name,
                    <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::data_type(),
                    #nullable_lit,
                ));
            });
        }

        // Per-field extension: collect tokens under #[record(ext(...))]
        #[cfg(feature = "ext-hooks")]
        let field_ext_tokens: Option<Vec<proc_macro2::TokenStream>> =
            parse_ext_token_list_on_field(&f.attrs)?;

        #[cfg(feature = "ext-hooks")]
        if !field_macros.is_empty() {
            for m in &field_macros {
                let ext_group = if let Some(ts) = &field_ext_tokens {
                    quote! { ( #( #ts ),* ) }
                } else {
                    quote! { () }
                };
                field_macro_invocations.push(quote! {
                    #m!(owner = #name, index = { #idx }, field = #fname, ty = #inner_ty_ts, nullable = #nullable_lit, ext = #ext_group);
                });
            }
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
        // Append row logic per field (owned)
        if nullable {
            append_row_stmts.push(quote! {
                match #fname {
                    Some(v) => <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::append_value(&mut self.#fname, &v),
                    None => <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::append_null(&mut self.#fname),
                }
            });
            append_row_ref_stmts.push(quote! {
                match &#fname {
                    Some(v) => <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::append_value(&mut self.#fname, v),
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
            append_row_ref_stmts.push(quote! {
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
            append_struct_borrowed_stmts.push(quote! {
                let cb: &mut #child_builder_ty = __sb
                    .field_builder::<#child_builder_ty>({ #idx })
                    .expect("child builder type matches");
                match &#fname {
                    Some(v) => <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::append_value(cb, v),
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
            append_struct_borrowed_stmts.push(quote! {
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

    let mut base_generics = input.generics.clone();
    add_arrow_binding_bounds(&mut base_generics, &inner_tys_for_view);
    let (base_impl_generics, base_ty_generics, base_where_clause) = base_generics.split_for_impl();

    for info in &col_infos {
        let idx = &info.idx;
        let inner_ty_ts = &info.inner_ty_ts;
        let nullable_lit = if info.nullable {
            quote!(true)
        } else {
            quote!(false)
        };
        let arrow_field_name = &info.arrow_field_name;

        col_impls.push(quote! {
            impl #base_impl_generics ::typed_arrow::schema::ColAt<{ #idx }> for #name #base_ty_generics #base_where_clause {
                type Native = #inner_ty_ts;
                type ColumnArray = < #inner_ty_ts as ::typed_arrow::bridge::ArrowBinding >::Array;
                type ColumnBuilder = < #inner_ty_ts as ::typed_arrow::bridge::ArrowBinding >::Builder;
                const NULLABLE: bool = #nullable_lit;
                const NAME: &'static str = #arrow_field_name;
                fn data_type() -> ::typed_arrow::arrow_schema::DataType { < #inner_ty_ts as ::typed_arrow::bridge::ArrowBinding >::data_type() }
            }
        });
    }

    let mut view_generics = base_generics.clone();
    prepend_view_lifetime(&mut view_generics, view_lt.clone());
    add_arrow_binding_view_bounds(&mut view_generics, &inner_tys_for_view, false);
    add_view_lifetime_bounds(&mut view_generics, &inner_tys_for_view, &view_lt);
    let (_view_impl_generics, view_ty_generics, view_where_clause) = view_generics.split_for_impl();

    let mut view_try_generics = view_generics.clone();
    add_view_try_from_bounds(&mut view_try_generics, &try_from_tys_for_view, &view_lt);
    let (view_try_impl_generics, view_try_ty_generics, view_try_where_clause) =
        view_try_generics.split_for_impl();

    let mut view_iter_generics = base_generics.clone();
    prepend_view_lifetime(&mut view_iter_generics, view_lt.clone());
    add_arrow_binding_view_bounds(&mut view_iter_generics, &inner_tys_for_view, true);
    let (view_iter_impl_generics, view_iter_ty_generics, view_iter_where_clause) =
        view_iter_generics.split_for_impl();

    let mut view_record_generics = base_generics.clone();
    add_arrow_binding_view_bounds(&mut view_record_generics, &inner_tys_for_view, true);
    let (view_record_impl_generics, view_record_ty_generics, view_record_where_clause) =
        view_record_generics.split_for_impl();

    // impl Record and ForEachCol
    let rec_impl = quote! {
        impl #base_impl_generics ::typed_arrow::schema::Record for #name #base_ty_generics #base_where_clause {
            const LEN: usize = #len;
        }

        impl #base_impl_generics ::typed_arrow::schema::ForEachCol for #name #base_ty_generics #base_where_clause {
            fn for_each_col<V: ::typed_arrow::schema::ColumnVisitor>() {
                #(#visit_calls)*
            }
        }

        impl #base_impl_generics ::typed_arrow::schema::StructMeta for #name #base_ty_generics #base_where_clause {
            fn child_fields() -> ::std::vec::Vec<::typed_arrow::arrow_schema::Field> {
                let mut fields = ::std::vec::Vec::with_capacity(#len);
                #(#child_field_stmts)*
                fields
            }

            fn new_struct_builder(capacity: usize) -> ::typed_arrow::arrow_array::builder::StructBuilder {
                use ::std::sync::Arc;
                let fields: ::std::vec::Vec<Arc<::typed_arrow::arrow_schema::Field>> =
                    <#name #base_ty_generics as ::typed_arrow::schema::StructMeta>::child_fields()
                        .into_iter()
                        .map(Arc::new)
                        .collect();
                let mut builders: ::std::vec::Vec<Box<dyn ::typed_arrow::arrow_array::builder::ArrayBuilder>> =
                    ::std::vec::Vec::with_capacity(#len);
                #(#child_builder_stmts)*
                ::typed_arrow::arrow_array::builder::StructBuilder::new(fields, builders)
            }
        }

        impl #base_impl_generics ::typed_arrow::schema::SchemaMeta for #name #base_ty_generics #base_where_clause {
            fn fields() -> ::std::vec::Vec<::typed_arrow::arrow_schema::Field> {
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
        pub struct #builders_ident #base_ty_generics #base_where_clause {
            #(#builder_struct_fields,)*
        }

        pub struct #arrays_ident #base_ty_generics #base_where_clause {
            #(#arrays_struct_fields,)*
        }

        impl #base_impl_generics ::typed_arrow::schema::BuildRows for #name #base_ty_generics #base_where_clause {
            type Builders = #builders_ident #base_ty_generics;
            type Arrays = #arrays_ident #base_ty_generics;
            fn new_builders(capacity: usize) -> Self::Builders {
                #builders_ident { #(#builders_init_fields,)* }
            }
        }

        impl #base_impl_generics #builders_ident #base_ty_generics #base_where_clause {
            #[inline]
            pub fn append_row(&mut self, row: #name #base_ty_generics) {
                let #name { #( #field_idents ),* } = row;
                #(#append_row_stmts)*
            }
            #[inline]
            pub fn append_row_ref(&mut self, row: &#name #base_ty_generics) {
                let #name { #( #field_idents ),* } = row;
                #(#append_row_ref_stmts)*
            }
            #[inline]
            pub fn append_null_row(&mut self) {
                #(#append_null_row_stmts)*
            }
            #[inline]
            pub fn append_option_row(&mut self, row: ::core::option::Option<#name #base_ty_generics>) {
                match row {
                    ::core::option::Option::Some(r) => self.append_row(r),
                    ::core::option::Option::None => self.append_null_row(),
                }
            }
            #[inline]
            pub fn append_option_row_ref(&mut self, row: ::core::option::Option<&#name #base_ty_generics>) {
                match row {
                    ::core::option::Option::Some(r) => self.append_row_ref(r),
                    ::core::option::Option::None => self.append_null_row(),
                }
            }
            #[inline]
            pub fn append_rows<I: ::core::iter::IntoIterator<Item = #name #base_ty_generics>>(&mut self, rows: I) {
                for r in rows { self.append_row(r); }
            }
            #[inline]
            pub fn append_rows_ref<'a, I: ::core::iter::IntoIterator<Item = &'a #name #base_ty_generics>>(
                &mut self,
                rows: I,
            )
            where
                #name #base_ty_generics: 'a,
            {
                for r in rows { self.append_row_ref(r); }
            }
            #[inline]
            pub fn append_option_rows<I: ::core::iter::IntoIterator<Item = ::core::option::Option<#name #base_ty_generics>>>(&mut self, rows: I) {
                for r in rows { self.append_option_row(r); }
            }
            #[inline]
            pub fn append_option_rows_ref<'a, I: ::core::iter::IntoIterator<Item = ::core::option::Option<&'a #name #base_ty_generics>>>(
                &mut self,
                rows: I,
            )
            where
                #name #base_ty_generics: 'a,
            {
                for r in rows { self.append_option_row_ref(r); }
            }
            #[inline]
            pub fn finish(self) -> #arrays_ident #base_ty_generics {
                #arrays_ident { #(#finish_fields,)* }
            }
        }

        // Implement the generic RowBuilder trait for the generated builders
        impl #base_impl_generics ::typed_arrow::schema::RowBuilder<#name #base_ty_generics> for #builders_ident #base_ty_generics #base_where_clause {
            type Arrays = #arrays_ident #base_ty_generics;
            fn append_row(&mut self, row: #name #base_ty_generics) { Self::append_row(self, row) }
            fn append_null_row(&mut self) { Self::append_null_row(self) }
            fn append_option_row(&mut self, row: ::core::option::Option<#name #base_ty_generics>) { Self::append_option_row(self, row) }
            fn append_rows<I: ::core::iter::IntoIterator<Item = #name #base_ty_generics>>(&mut self, rows: I) { Self::append_rows(self, rows) }
            fn append_option_rows<I: ::core::iter::IntoIterator<Item = ::core::option::Option<#name #base_ty_generics>>>(
                &mut self,
                rows: I,
            ) { Self::append_option_rows(self, rows) }
            fn finish(self) -> #arrays_ident #base_ty_generics { Self::finish(self) }
        }

        impl #base_impl_generics #arrays_ident #base_ty_generics #base_where_clause {
            /// Build an Arrow RecordBatch from these arrays and the generated schema.
            pub fn into_record_batch(self) -> ::typed_arrow::arrow_array::RecordBatch {
                use ::std::sync::Arc;
                let schema = <#name #base_ty_generics as ::typed_arrow::schema::SchemaMeta>::schema();
                let mut cols: ::std::vec::Vec<Arc<dyn ::typed_arrow::arrow_array::Array>> = ::std::vec::Vec::with_capacity(#len);
                #( cols.push(Arc::new(self.#field_idents)); )*
                ::typed_arrow::arrow_array::RecordBatch::try_new(schema, cols).expect("valid record batch")
            }
        }

        impl #base_impl_generics ::typed_arrow::schema::IntoRecordBatch for #arrays_ident #base_ty_generics #base_where_clause {
            fn into_record_batch(self) -> ::typed_arrow::arrow_array::RecordBatch { Self::into_record_batch(self) }
        }

        impl #base_impl_generics ::typed_arrow::schema::AppendStruct for #name #base_ty_generics #base_where_clause {
            fn append_owned_into(self, __sb: &mut ::typed_arrow::arrow_array::builder::StructBuilder) {
                let #name { #( #field_idents ),* } = self;
                #(#append_struct_owned_stmts)*
            }
            fn append_null_into(__sb: &mut ::typed_arrow::arrow_array::builder::StructBuilder) {
                #(#append_struct_null_stmts)*
            }
        }

        impl #base_impl_generics ::typed_arrow::schema::AppendStructRef for #name #base_ty_generics #base_where_clause {
            fn append_borrowed_into(&self, __sb: &mut ::typed_arrow::arrow_array::builder::StructBuilder) {
                let #name { #( #field_idents ),* } = self;
                #(#append_struct_borrowed_stmts)*
            }
        }
    };

    // Invoke any record-level callback macros once, after the main impls
    let mut record_macro_invocations: Vec<proc_macro2::TokenStream> = Vec::new();
    if !record_macros.is_empty() {
        let ext_group = if let Some(ts) = &record_ext_tokens {
            quote! { ( #( #ts ),* ) }
        } else {
            quote! { () }
        };
        for m in &record_macros {
            record_macro_invocations
                .push(quote! { #m!(owner = #name, len = #len, ext = #ext_group); });
        }
    }

    // Optionally invoke record-fields macros with the list of (field: type)
    if !record_fields_macros.is_empty() {
        let mut field_pairs: Vec<proc_macro2::TokenStream> = Vec::new();
        for f in &fields.named {
            let fname = f.ident.as_ref().expect("named");
            let (inner_ty, _nullable) = unwrap_option(&f.ty);
            let inner_ty_ts = inner_ty.to_token_stream();
            field_pairs.push(quote! { ( #fname : #inner_ty_ts ) });
        }
        for m in &record_fields_macros {
            record_macro_invocations.push(quote! { #m!( #name, #( #field_pairs ),* ); });
        }
    }

    // If any external visitors are registered, instantiate them at compile time.
    let mut visitor_instantiations: Vec<proc_macro2::TokenStream> = Vec::new();
    for v in &ext_visitors {
        visitor_instantiations.push(quote! {
            impl #base_impl_generics #name #base_ty_generics #base_where_clause {
                const _: () = { <#name #base_ty_generics as ::typed_arrow::schema::ForEachCol>::for_each_col::<#v>(); };
            }
        });
    }

    // Generate view struct and iterator for FromRecordBatch
    let view_ident = Ident::new(&format!("{name}View"), name.span());
    let views_ident = Ident::new(&format!("{name}Views"), name.span());
    let view_try_into_ident = Ident::new(&format!("__ta_view_try_into_{name}"), name.span());

    let mut view_struct_fields = Vec::with_capacity(len);
    let mut views_array_fields = Vec::with_capacity(len);
    let mut views_init_fields = Vec::with_capacity(len);
    let mut view_extract_stmts = Vec::with_capacity(len);
    let mut struct_view_extract_stmts = Vec::with_capacity(len);
    let mut view_conversion_exprs = Vec::with_capacity(len);

    for (i, f) in fields.named.iter().enumerate() {
        let fname = f.ident.as_ref().expect("named");
        let idx = syn::Index::from(i);
        let (inner_ty, nullable) = unwrap_option(&f.ty);
        let inner_ty_ts = inner_ty.to_token_stream();
        let view_ty = generate_view_type(&f.ty, nullable, &view_lt);

        // View struct field
        view_struct_fields.push(quote! {
            pub #fname: #view_ty
        });

        // Views iterator: store arrays with lifetimes (public for direct column access)
        views_array_fields.push(quote! {
            pub #fname: &#view_lt <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::Array
        });

        // Initialize views arrays from RecordBatch columns - downcast with error handling
        views_init_fields.push(quote! {
            #fname: batch.column(#idx)
                .as_any()
                .downcast_ref::<<#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::Array>()
                .ok_or_else(|| ::typed_arrow::error::SchemaError::type_mismatch(
                    <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::data_type(),
                    batch.column(#idx).data_type().clone()
                ))?,
        });

        // Extract value at index for each field (for iterator)
        if nullable {
            // For nullable fields, use Option<T>::get_view which handles nulls
            view_extract_stmts.push(quote! {
                #fname: <::core::option::Option<#inner_ty_ts> as ::typed_arrow::bridge::ArrowBindingView>::get_view(self.#fname, self.index)?
            });
        } else {
            view_extract_stmts.push(quote! {
                #fname: <#inner_ty_ts as ::typed_arrow::bridge::ArrowBindingView>::get_view(self.#fname, self.index)?
            });
        }

        // Extract value from StructArray child column (for StructView)
        if nullable {
            // For nullable fields, call Option<T>::get_view which handles nulls by returning
            // Ok(None)
            struct_view_extract_stmts.push(quote! {
                #fname: {
                    let __arr = array.column(#idx)
                        .as_any()
                        .downcast_ref::<<#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::Array>()
                        .ok_or_else(|| ::typed_arrow::schema::ViewAccessError::TypeMismatch {
                            expected: <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::data_type(),
                            actual: array.column(#idx).data_type().clone(),
                            field_name: ::core::option::Option::Some(stringify!(#fname)),
                        })?;
                    <::core::option::Option<#inner_ty_ts> as ::typed_arrow::bridge::ArrowBindingView>::get_view(__arr, index)?
                }
            });
        } else {
            // For non-nullable fields, call T::get_view which returns Err(UnexpectedNull) on null
            struct_view_extract_stmts.push(quote! {
                #fname: {
                    let __arr = array.column(#idx)
                        .as_any()
                        .downcast_ref::<<#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::Array>()
                        .ok_or_else(|| ::typed_arrow::schema::ViewAccessError::TypeMismatch {
                            expected: <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::data_type(),
                            actual: array.column(#idx).data_type().clone(),
                            field_name: ::core::option::Option::Some(stringify!(#fname)),
                        })?;
                    <#inner_ty_ts as ::typed_arrow::bridge::ArrowBindingView>::get_view(__arr, index)?
                }
            });
        }

        // Generate view-to-owned conversion expression
        view_conversion_exprs.push(generate_view_conversion_expr(
            fname,
            &f.ty,
            nullable,
            &view_try_into_ident,
        ));
    }

    let view_impl = if cfg!(feature = "views") {
        quote! {
            #[allow(non_snake_case)]
            #[inline]
            fn #view_try_into_ident<T, U>(v: T) -> ::core::result::Result<U, ::typed_arrow::schema::ViewAccessError>
            where
                T: ::core::convert::TryInto<U>,
                ::typed_arrow::schema::ViewAccessError: ::core::convert::From<
                    <T as ::core::convert::TryInto<U>>::Error
                >,
            {
                v.try_into().map_err(::typed_arrow::schema::ViewAccessError::from)
            }

            /// Zero-copy view of a single row from a RecordBatch.
            pub struct #view_ident #view_ty_generics #view_where_clause {
                #(#view_struct_fields,)*
                _phantom: ::core::marker::PhantomData<&#view_lt ()>,
            }

            impl #view_try_impl_generics ::core::convert::TryFrom<#view_ident #view_try_ty_generics> for #name #base_ty_generics #view_try_where_clause {
                type Error = ::typed_arrow::schema::ViewAccessError;

                fn try_from(view: #view_ident #view_try_ty_generics) -> ::core::result::Result<Self, Self::Error> {
                    ::core::result::Result::Ok(#name {
                        #(#view_conversion_exprs,)*
                    })
                }
            }

            /// Iterator yielding views over RecordBatch rows.
            pub struct #views_ident #view_ty_generics #view_where_clause {
                #(#views_array_fields,)*
                index: usize,
                len: usize,
            }

            impl #view_iter_impl_generics ::core::iter::Iterator for #views_ident #view_iter_ty_generics #view_iter_where_clause {
                type Item = ::core::result::Result<#view_ident #view_iter_ty_generics, ::typed_arrow::schema::ViewAccessError>;

                fn next(&mut self) -> ::core::option::Option<Self::Item> {
                    if self.index >= self.len {
                        return ::core::option::Option::None;
                    }
                    let result = (|| -> ::core::result::Result<#view_ident #view_iter_ty_generics, ::typed_arrow::schema::ViewAccessError> {
                        ::core::result::Result::Ok(#view_ident {
                            #(#view_extract_stmts,)*
                            _phantom: ::core::marker::PhantomData,
                        })
                    })();
                    self.index += 1;
                    ::core::option::Option::Some(result)
                }

                fn size_hint(&self) -> (usize, ::core::option::Option<usize>) {
                    let remaining = self.len - self.index;
                    (remaining, ::core::option::Option::Some(remaining))
                }
            }

            impl #view_iter_impl_generics ::core::iter::ExactSizeIterator for #views_ident #view_iter_ty_generics #view_iter_where_clause {
                fn len(&self) -> usize {
                    self.len - self.index
                }
            }

            impl #view_record_impl_generics ::typed_arrow::schema::FromRecordBatch for #name #view_record_ty_generics #view_record_where_clause {
                type View<#view_lt> = #view_ident #view_ty_generics;
                type Views<#view_lt> = #views_ident #view_ty_generics;

                fn from_record_batch(batch: &::typed_arrow::arrow_array::RecordBatch) -> ::core::result::Result<Self::Views<'_>, ::typed_arrow::error::SchemaError> {
                    // Validate column count
                    if batch.num_columns() != #len {
                        return ::core::result::Result::Err(::typed_arrow::error::SchemaError::invalid(
                            format!("Column count mismatch: expected {} columns for {}, but RecordBatch has {} columns",
                                #len, stringify!(#name), batch.num_columns())
                        ));
                    }

                    // Downcast each column and validate types
                    ::core::result::Result::Ok(#views_ident {
                        #(#views_init_fields)*
                        index: 0,
                        len: batch.num_rows(),
                    })
                }
            }

            impl #view_record_impl_generics ::typed_arrow::schema::StructView for #name #view_record_ty_generics #view_record_where_clause {
                type View<#view_lt> = #view_ident #view_ty_generics;

                fn view_at(array: &::typed_arrow::arrow_array::StructArray, index: usize) -> ::core::result::Result<Self::View<'_>, ::typed_arrow::schema::ViewAccessError> {
                    use ::typed_arrow::arrow_array::Array;
                    ::core::result::Result::Ok(#view_ident {
                        #(#struct_view_extract_stmts,)*
                        _phantom: ::core::marker::PhantomData,
                    })
                }

                fn is_null_at(array: &::typed_arrow::arrow_array::StructArray, index: usize) -> bool {
                    use ::typed_arrow::arrow_array::Array;
                    array.is_null(index)
                }
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #(#col_impls)*
        #rec_impl
        #view_impl
        #(#record_macro_invocations)*
        #(#field_macro_invocations)*
        #(#visitor_instantiations)*
    };
    if std::env::var("TYPED_ARROW_DERIVE_DEBUG").is_ok() {
        eprintln!("{expanded}");
    }
    Ok(expanded)
}

fn check_no_legacy_nested_attr(attrs: &[Attribute]) -> syn::Result<()> {
    for attr in attrs {
        if attr.path().is_ident("nested") {
            return Err(syn::Error::new_spanned(
                attr,
                "#[nested] and #[record(nested)] were removed. Nested structs are now the \
                 default; remove this attribute.",
            ));
        }
        if attr.path().is_ident("record") {
            let mut found_nested = false;
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("nested") {
                    found_nested = true;
                } else {
                    // Consume unknown nested items so other uses of #[record(...)] don't break
                    // parsing
                    if let Ok(v) = meta.value() {
                        let _expr: syn::Expr = v.parse()?;
                    } else {
                        let _ = meta.parse_nested_meta(|_| Ok(()));
                    }
                }
                Ok(())
            })?;
            if found_nested {
                return Err(syn::Error::new_spanned(
                    attr,
                    "#[record(nested)] was removed. Nested structs are now the default; remove \
                     this attribute.",
                ));
            }
        }
    }
    Ok(())
}

fn unwrap_option(ty: &Type) -> (Type, bool) {
    if let Type::Path(tp) = ty
        && let Some(seg) = tp.path.segments.last()
        && seg.ident == "Option"
        && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
        && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
    {
        return (inner.clone(), true);
    }
    (ty.clone(), false)
}

fn type_contains_generic(ty: &Type, generic_idents: &HashSet<Ident>) -> bool {
    match ty {
        Type::Path(tp) => {
            for seg in &tp.path.segments {
                if generic_idents.contains(&seg.ident) {
                    return true;
                }
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    for arg in &args.args {
                        if let syn::GenericArgument::Type(arg_ty) = arg
                            && type_contains_generic(arg_ty, generic_idents)
                        {
                            return true;
                        }
                    }
                }
            }
            false
        }
        Type::Reference(tr) => type_contains_generic(&tr.elem, generic_idents),
        Type::Array(ta) => type_contains_generic(&ta.elem, generic_idents),
        Type::Slice(ts) => type_contains_generic(&ts.elem, generic_idents),
        Type::Tuple(tt) => tt
            .elems
            .iter()
            .any(|t| type_contains_generic(t, generic_idents)),
        Type::Paren(tp) => type_contains_generic(&tp.elem, generic_idents),
        Type::Group(tg) => type_contains_generic(&tg.elem, generic_idents),
        Type::Ptr(tp) => type_contains_generic(&tp.elem, generic_idents),
        _ => false,
    }
}

fn fresh_view_lifetime(generics: &Generics) -> Lifetime {
    let mut existing = Vec::new();
    for param in &generics.params {
        if let GenericParam::Lifetime(lp) = param {
            existing.push(lp.lifetime.ident.to_string());
        }
    }

    let base = "__ta_view";
    let mut idx = 0usize;
    loop {
        let name = if idx == 0 {
            base.to_string()
        } else {
            format!("{base}{idx}")
        };
        if !existing.iter().any(|s| s == &name) {
            return Lifetime::new(&format!("'{}", name), Span::call_site());
        }
        idx += 1;
    }
}

fn prepend_view_lifetime(generics: &mut Generics, lt: Lifetime) {
    let mut params: Punctuated<GenericParam, syn::token::Comma> = Punctuated::new();
    params.push(GenericParam::Lifetime(LifetimeParam::new(lt)));
    params.extend(generics.params.clone());
    generics.params = params;
}

fn add_arrow_binding_bounds(generics: &mut Generics, inner_tys: &[proc_macro2::TokenStream]) {
    if inner_tys.is_empty() {
        return;
    }
    let where_clause = generics.make_where_clause();
    for ty in inner_tys {
        where_clause
            .predicates
            .push(parse_quote!(#ty: ::typed_arrow::bridge::ArrowBinding));
        where_clause.predicates.push(parse_quote!(
            <#ty as ::typed_arrow::bridge::ArrowBinding>::Builder:
                ::typed_arrow::arrow_array::builder::ArrayBuilder
        ));
        where_clause.predicates.push(parse_quote!(
            <#ty as ::typed_arrow::bridge::ArrowBinding>::Builder: 'static
        ));
        where_clause.predicates.push(parse_quote!(
            <#ty as ::typed_arrow::bridge::ArrowBinding>::Array: 'static
        ));
    }
}

fn add_arrow_binding_view_bounds(
    generics: &mut Generics,
    inner_tys: &[proc_macro2::TokenStream],
    add_static: bool,
) {
    if inner_tys.is_empty() {
        return;
    }
    let where_clause = generics.make_where_clause();
    for ty in inner_tys {
        if add_static {
            where_clause.predicates.push(parse_quote!(
                #ty: ::typed_arrow::bridge::ArrowBindingView<
                    Array = <#ty as ::typed_arrow::bridge::ArrowBinding>::Array
                > + 'static
            ));
        } else {
            where_clause.predicates.push(parse_quote!(
                #ty: ::typed_arrow::bridge::ArrowBindingView<
                    Array = <#ty as ::typed_arrow::bridge::ArrowBinding>::Array
                >
            ));
        }
    }
}

fn add_view_lifetime_bounds(
    generics: &mut Generics,
    inner_tys: &[proc_macro2::TokenStream],
    view_lt: &Lifetime,
) {
    if inner_tys.is_empty() {
        return;
    }
    let where_clause = generics.make_where_clause();
    for ty in inner_tys {
        where_clause.predicates.push(parse_quote!(#ty: #view_lt));
    }
}

fn add_view_try_from_bounds(
    generics: &mut Generics,
    inner_tys: &[proc_macro2::TokenStream],
    view_lt: &Lifetime,
) {
    if inner_tys.is_empty() {
        return;
    }
    let where_clause = generics.make_where_clause();
    for ty in inner_tys {
        where_clause
            .predicates
            .push(parse_quote!(#ty: ::core::convert::TryFrom<
                <#ty as ::typed_arrow::bridge::ArrowBindingView>::View<#view_lt>
            >));
        where_clause.predicates.push(parse_quote!(
            ::typed_arrow::schema::ViewAccessError: ::core::convert::From<
                <#ty as ::core::convert::TryFrom<
                    <#ty as ::typed_arrow::bridge::ArrowBindingView>::View<#view_lt>
                >>::Error
            >
        ));
    }
}

/// Generate the view type for a field. Uses ArrowBindingView::View<'a> for all types.
/// - Option<T> → Option<View<T>>
fn generate_view_type(ty: &Type, nullable: bool, view_lt: &Lifetime) -> proc_macro2::TokenStream {
    let (inner_ty, _) = unwrap_option(ty);
    let inner_ty_ts = inner_ty.to_token_stream();

    // Always use the ArrowBindingView::View associated type
    let view_inner =
        quote! { <#inner_ty_ts as ::typed_arrow::bridge::ArrowBindingView>::View<#view_lt> };

    if nullable {
        quote! { ::core::option::Option<#view_inner> }
    } else {
        view_inner
    }
}

/// Check if a type is a Copy value type where View<'a> = Self.
/// This includes primitives and temporal types.
fn is_copy_primitive(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty
        && type_path.path.segments.len() == 1
    {
        let segment = &type_path.path.segments[0];
        let ident = &segment.ident;
        let name = ident.to_string();

        return matches!(
            name.as_str(),
            // Integer types
            "i8" | "i16" | "i32" | "i64" |
            "u8" | "u16" | "u32" | "u64" |
            // Float types
            "f16" | "f32" | "f64" |
            // Boolean
            "bool" |
            // Timestamp types
            "Timestamp" | "TimestampTz" |
            // Date types
            "Date32" | "Date64" |
            // Time types
            "Time32" | "Time64" |
            // Duration
            "Duration" |
            // Interval types
            "IntervalYearMonth" | "IntervalDayTime" | "IntervalMonthDayNano"
        );
    }
    false
}

/// Check if a type is String (which has infallible conversion from &str).
fn is_string(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty
        && type_path.path.segments.len() == 1
    {
        let segment = &type_path.path.segments[0];
        return segment.ident == "String";
    }
    false
}

/// Check if a type is a fixed-size byte array [u8; N].
fn is_fixed_size_binary(ty: &Type) -> bool {
    if let Type::Array(type_array) = ty
        && let Type::Path(elem_type) = &*type_array.elem
        && elem_type.path.segments.len() == 1
    {
        let seg = &elem_type.path.segments[0];
        return seg.ident == "u8";
    }
    false
}

/// Generate the conversion expression from view to owned for a field.
fn generate_view_conversion_expr(
    fname: &syn::Ident,
    ty: &Type,
    nullable: bool,
    view_try_into_ident: &syn::Ident,
) -> proc_macro2::TokenStream {
    let (inner_ty, _) = unwrap_option(ty);
    let is_primitive = is_copy_primitive(&inner_ty);
    let is_string = is_string(&inner_ty);
    let is_fsb = is_fixed_size_binary(&inner_ty);

    if nullable {
        if is_primitive {
            // Option<primitive>: just copy
            quote! { #fname: view.#fname }
        } else if is_string {
            // Option<String>: use infallible .into() conversion
            quote! { #fname: view.#fname.map(|__v| __v.into()) }
        } else if is_fsb {
            // Option<[u8; N]>: need to copy from &[u8] slice
            quote! { #fname: view.#fname.map(|__slice| {
                let mut __arr = <#inner_ty>::default();
                __arr.copy_from_slice(__slice);
                __arr
            }) }
        } else {
            // Option<non-primitive>: map view to owned via TryInto
            quote! { #fname: match view.#fname {
                ::core::option::Option::Some(__v) => ::core::option::Option::Some(#view_try_into_ident(__v)?),
                ::core::option::Option::None => ::core::option::Option::None,
            } }
        }
    } else if is_primitive {
        // Non-nullable primitive: just copy
        quote! { #fname: view.#fname }
    } else if is_string {
        // Non-nullable String: use infallible .into() conversion
        quote! { #fname: view.#fname.into() }
    } else if is_fsb {
        // Non-nullable [u8; N]: need to copy from &[u8] slice
        quote! { #fname: {
            let mut __arr = <#inner_ty>::default();
            __arr.copy_from_slice(view.#fname);
            __arr
        } }
    } else {
        // Non-nullable non-primitive: convert view to owned via TryInto
        quote! { #fname: #view_try_into_ident(view.#fname)? }
    }
}
