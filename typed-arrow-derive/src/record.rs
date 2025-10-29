use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{Attribute, Data, DataStruct, DeriveInput, Fields, Ident, Path, Type};

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
use crate::attrs::{parse_field_metadata_pairs, parse_schema_metadata_pairs};

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
    let mut append_struct_borrowed_stmts = Vec::with_capacity(len);
    let mut append_null_row_stmts = Vec::with_capacity(len);
    let mut inner_tys_for_view = Vec::with_capacity(len);

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

        let inner_ty_ts = inner_ty.to_token_stream();
        inner_tys_for_view.push(inner_ty_ts.clone());
        let nullable_lit = if nullable {
            quote!(true)
        } else {
            quote!(false)
        };

        // impl ColAt<I> for Type
        let col_impl = quote! {
            impl ::typed_arrow::schema::ColAt<{ #idx }> for #name {
                type Native = #inner_ty_ts;
                type ColumnArray = < #inner_ty_ts as ::typed_arrow::bridge::ArrowBinding >::Array;
                type ColumnBuilder = < #inner_ty_ts as ::typed_arrow::bridge::ArrowBinding >::Builder;
                const NULLABLE: bool = #nullable_lit;
                const NAME: &'static str = stringify!(#fname);
                fn data_type() -> ::typed_arrow::arrow_schema::DataType { < #inner_ty_ts as ::typed_arrow::bridge::ArrowBinding >::data_type() }
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
                let mut __f = ::typed_arrow::arrow_schema::Field::new(
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
                fields.push(::typed_arrow::arrow_schema::Field::new(
                    stringify!(#fname),
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
        // Append row logic per field
        if nullable {
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
            fn child_fields() -> ::std::vec::Vec<::typed_arrow::arrow_schema::Field> {
                let mut fields = ::std::vec::Vec::with_capacity(#len);
                #(#child_field_stmts)*
                fields
            }

            fn new_struct_builder(capacity: usize) -> ::typed_arrow::arrow_array::builder::StructBuilder {
                use ::std::sync::Arc;
                let fields: ::std::vec::Vec<Arc<::typed_arrow::arrow_schema::Field>> =
                    <#name as ::typed_arrow::schema::StructMeta>::child_fields()
                        .into_iter()
                        .map(Arc::new)
                        .collect();
                let mut builders: ::std::vec::Vec<Box<dyn ::typed_arrow::arrow_array::builder::ArrayBuilder>> =
                    ::std::vec::Vec::with_capacity(#len);
                #(#child_builder_stmts)*
                ::typed_arrow::arrow_array::builder::StructBuilder::new(fields, builders)
            }
        }

        impl ::typed_arrow::schema::SchemaMeta for #name {
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

        // Implement the generic RowBuilder trait for the generated builders
        impl ::typed_arrow::schema::RowBuilder<#name> for #builders_ident {
            type Arrays = #arrays_ident;
            fn append_row(&mut self, row: #name) { Self::append_row(self, row) }
            fn append_null_row(&mut self) { Self::append_null_row(self) }
            fn append_option_row(&mut self, row: ::core::option::Option<#name>) { Self::append_option_row(self, row) }
            fn append_rows<I: ::core::iter::IntoIterator<Item = #name>>(&mut self, rows: I) { Self::append_rows(self, rows) }
            fn append_option_rows<I: ::core::iter::IntoIterator<Item = ::core::option::Option<#name>>>(
                &mut self,
                rows: I,
            ) { Self::append_option_rows(self, rows) }
            fn finish(self) -> #arrays_ident { Self::finish(self) }
        }

        impl #arrays_ident {
            /// Build an Arrow RecordBatch from these arrays and the generated schema.
            pub fn into_record_batch(self) -> ::typed_arrow::arrow_array::RecordBatch {
                use ::std::sync::Arc;
                let schema = <#name as ::typed_arrow::schema::SchemaMeta>::schema();
                let mut cols: ::std::vec::Vec<Arc<dyn ::typed_arrow::arrow_array::Array>> = ::std::vec::Vec::with_capacity(#len);
                #( cols.push(Arc::new(self.#field_idents)); )*
                ::typed_arrow::arrow_array::RecordBatch::try_new(schema, cols).expect("valid record batch")
            }
        }

        impl ::typed_arrow::schema::IntoRecordBatch for #arrays_ident {
            fn into_record_batch(self) -> ::typed_arrow::arrow_array::RecordBatch { Self::into_record_batch(self) }
        }

        impl ::typed_arrow::schema::AppendStruct for #name {
            fn append_owned_into(self, __sb: &mut ::typed_arrow::arrow_array::builder::StructBuilder) {
                let #name { #( #field_idents ),* } = self;
                #(#append_struct_owned_stmts)*
            }
            fn append_null_into(__sb: &mut ::typed_arrow::arrow_array::builder::StructBuilder) {
                #(#append_struct_null_stmts)*
            }
        }

        impl ::typed_arrow::schema::AppendStructRef for #name {
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
            const _: () = { <#name as ::typed_arrow::schema::ForEachCol>::for_each_col::<#v>(); };
        });
    }

    // Generate view struct and iterator for FromRecordBatch
    let view_ident = Ident::new(&format!("{name}View"), name.span());
    let views_ident = Ident::new(&format!("{name}Views"), name.span());

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
        let view_ty = generate_view_type(&f.ty, nullable);

        // View struct field
        view_struct_fields.push(quote! {
            pub #fname: #view_ty
        });

        // Views iterator: store arrays with lifetimes
        views_array_fields.push(quote! {
            #fname: &'a <#inner_ty_ts as ::typed_arrow::bridge::ArrowBinding>::Array
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
        view_conversion_exprs.push(generate_view_conversion_expr(fname, &f.ty, nullable));
    }

    let view_impl = if cfg!(feature = "views") {
        quote! {
            /// Zero-copy view of a single row from a RecordBatch.
            pub struct #view_ident<'a> {
                #(#view_struct_fields,)*
                _phantom: ::core::marker::PhantomData<&'a ()>,
            }

            impl<'a> ::core::convert::TryFrom<#view_ident<'a>> for #name {
                type Error = ::typed_arrow::schema::ViewAccessError;

                fn try_from(view: #view_ident<'a>) -> ::core::result::Result<Self, Self::Error> {
                    ::core::result::Result::Ok(#name {
                        #(#view_conversion_exprs,)*
                    })
                }
            }

            /// Iterator yielding views over RecordBatch rows.
            pub struct #views_ident<'a> {
                #(#views_array_fields,)*
                index: usize,
                len: usize,
            }

            impl<'a> ::core::iter::Iterator for #views_ident<'a>
            where
                #(#inner_tys_for_view: ::typed_arrow::bridge::ArrowBindingView + 'static,)*
            {
                type Item = ::core::result::Result<#view_ident<'a>, ::typed_arrow::schema::ViewAccessError>;

                fn next(&mut self) -> ::core::option::Option<Self::Item> {
                    if self.index >= self.len {
                        return ::core::option::Option::None;
                    }
                    let result = (|| -> ::core::result::Result<#view_ident<'a>, ::typed_arrow::schema::ViewAccessError> {
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

            impl<'a> ::core::iter::ExactSizeIterator for #views_ident<'a>
            where
                #(#inner_tys_for_view: ::typed_arrow::bridge::ArrowBindingView + 'static,)*
            {
                fn len(&self) -> usize {
                    self.len - self.index
                }
            }

            impl ::typed_arrow::schema::FromRecordBatch for #name
            where
                #(#inner_tys_for_view: ::typed_arrow::bridge::ArrowBindingView + 'static,)*
            {
                type View<'a> = #view_ident<'a>;
                type Views<'a> = #views_ident<'a>;

                fn from_record_batch(batch: &::arrow_array::RecordBatch) -> ::core::result::Result<Self::Views<'_>, ::typed_arrow::error::SchemaError> {
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

            impl ::typed_arrow::schema::StructView for #name
            where
                #(#inner_tys_for_view: ::typed_arrow::bridge::ArrowBindingView + 'static,)*
            {
                type View<'a> = #view_ident<'a>;

                fn view_at(array: &::arrow_array::StructArray, index: usize) -> ::core::result::Result<Self::View<'_>, ::typed_arrow::schema::ViewAccessError> {
                    use ::arrow_array::Array;
                    ::core::result::Result::Ok(#view_ident {
                        #(#struct_view_extract_stmts,)*
                        _phantom: ::core::marker::PhantomData,
                    })
                }

                fn is_null_at(array: &::arrow_array::StructArray, index: usize) -> bool {
                    use ::arrow_array::Array;
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

/// Generate the view type for a field. Uses ArrowBindingView::View<'a> for all types.
/// - Option<T> → Option<View<T>>
fn generate_view_type(ty: &Type, nullable: bool) -> proc_macro2::TokenStream {
    let (inner_ty, _) = unwrap_option(ty);
    let inner_ty_ts = inner_ty.to_token_stream();

    // Always use the ArrowBindingView::View associated type
    let view_inner = quote! { <#inner_ty_ts as ::typed_arrow::bridge::ArrowBindingView>::View<'a> };

    if nullable {
        quote! { ::core::option::Option<#view_inner> }
    } else {
        view_inner
    }
}

/// Check if a type is a Copy value type where View<'a> = Self.
/// This includes primitives and temporal types.
fn is_copy_primitive(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if type_path.path.segments.len() == 1 {
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
    }
    false
}

/// Check if a type is String (which has infallible conversion from &str).
fn is_string(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if type_path.path.segments.len() == 1 {
            let segment = &type_path.path.segments[0];
            return segment.ident == "String";
        }
    }
    false
}

/// Check if a type is a fixed-size byte array [u8; N].
fn is_fixed_size_binary(ty: &Type) -> bool {
    if let Type::Array(type_array) = ty {
        if let Type::Path(elem_type) = &*type_array.elem {
            if elem_type.path.segments.len() == 1 {
                let seg = &elem_type.path.segments[0];
                return seg.ident == "u8";
            }
        }
    }
    false
}

/// Generate the conversion expression from view to owned for a field.
fn generate_view_conversion_expr(
    fname: &syn::Ident,
    ty: &Type,
    nullable: bool,
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
                ::core::option::Option::Some(__v) => ::core::option::Option::Some(__v.try_into()?),
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
        quote! { #fname: view.#fname.try_into()? }
    }
}
