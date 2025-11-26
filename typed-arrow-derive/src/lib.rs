//! Proc-macros for typed-arrow: `#[derive(Record)]` and `#[derive(Union)]`.

mod attrs;
mod record;
mod union;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

#[proc_macro_derive(Record, attributes(nested, schema_metadata, metadata, record))]
pub fn derive_record(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    record::derive_record(&input)
}

#[proc_macro_derive(Union, attributes(union))]
pub fn derive_union(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    union::derive_union(&input)
}
