#![deny(missing_docs)]
//! Dynamic runtime facade for typed-arrow.
//!
//! This crate provides minimal runtime schema and builders abstractions that
//! complement the compile-time APIs in `typed-arrow`.

mod builders;
mod cell;
mod dyn_builder;
mod error;
mod factory;
mod nested;
mod rows;
mod schema;
mod union;
mod validate;

pub use builders::DynBuilders;
pub use cell::DynCell;
pub use dyn_builder::DynColumnBuilder;
pub use error::DynError;
pub use factory::new_dyn_builder;
pub use rows::DynRow;
pub use schema::DynSchema;
pub use validate::validate_nullability;
