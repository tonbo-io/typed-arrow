#![allow(clippy::assertions_on_constants, clippy::bool_assert_comparison)]
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::Array;
use typed_arrow::prelude::*;

// Define a record using derive macro
#[derive(typed_arrow::Record)]
pub struct Person {
    pub id: i64,
    pub name: Option<String>,
    pub email: Option<String>,
    pub score: f32,
    pub blob: Option<Vec<u8>>,
    pub active: bool,
}

#[test]
fn arrow_types_exposed_via_colat() {
    // Assert DATA_TYPE mapping is available at type-level
    use arrow_schema::DataType;
    assert_eq!(<Person as ColAt<0>>::data_type(), DataType::Int64);
    assert_eq!(<Person as ColAt<1>>::data_type(), DataType::Utf8);
    assert_eq!(<Person as ColAt<3>>::data_type(), DataType::Float32);
    assert_eq!(<Person as ColAt<4>>::data_type(), DataType::Binary);
    assert_eq!(<Person as ColAt<5>>::data_type(), DataType::Boolean);

    // Assert ColumnBuilder/Array associated items exist and are usable
    type BId = <Person as ColAt<0>>::ColumnBuilder;
    type AId = <Person as ColAt<0>>::ColumnArray;
    let mut b: BId =
        arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Int64Type>::with_capacity(2);
    b.append_value(1);
    b.append_value(2);
    let _: AId = b.finish();
}

#[test]
fn build_arrays_via_associated_types() {
    use arrow_array::{
        builder::{BinaryBuilder, PrimitiveBuilder, StringBuilder},
        types::Int64Type,
    };

    // Int64 column (id)
    type IdBuilder = <Person as ColAt<0>>::ColumnBuilder;
    type IdArray = <Person as ColAt<0>>::ColumnArray;
    let mut idb: IdBuilder = PrimitiveBuilder::<Int64Type>::with_capacity(3);
    idb.append_value(10);
    idb.append_value(20);
    idb.append_value(30);
    let ida: IdArray = idb.finish();
    assert_eq!(ida.len(), 3);
    assert_eq!(ida.value(1), 20);

    // Utf8 column (name)
    type NameBuilder = <Person as ColAt<1>>::ColumnBuilder;
    type NameArray = <Person as ColAt<1>>::ColumnArray;
    let mut nb: NameBuilder = StringBuilder::with_capacity(3, 0);
    nb.append_value("alice");
    nb.append_null();
    nb.append_value("carol");
    let na: NameArray = nb.finish();
    assert_eq!(na.len(), 3);
    assert_eq!(na.value(0), "alice");
    assert!(na.is_null(1));

    // Binary column (blob)
    type BlobBuilder = <Person as ColAt<4>>::ColumnBuilder;
    type BlobArray = <Person as ColAt<4>>::ColumnArray;
    let mut bb: BlobBuilder = BinaryBuilder::with_capacity(2, 0);
    bb.append_value(b"ab");
    bb.append_null();
    let ba: BlobArray = bb.finish();
    assert_eq!(ba.len(), 2);
    assert_eq!(ba.value(0), b"ab");
    assert!(ba.is_null(1));
}

// Helper trait to assert type equality at compile-time
trait Same<T> {}
impl<T> Same<T> for T {}

#[test]
fn record_len_and_names_and_nullability() {
    assert_eq!(<Person as Record>::LEN, 6);

    // Names
    assert_eq!(<Person as ColAt<0>>::NAME, "id");
    assert_eq!(<Person as ColAt<1>>::NAME, "name");
    assert_eq!(<Person as ColAt<2>>::NAME, "email");
    assert_eq!(<Person as ColAt<3>>::NAME, "score");
    assert_eq!(<Person as ColAt<4>>::NAME, "blob");
    assert_eq!(<Person as ColAt<5>>::NAME, "active");

    // Nullability
    assert_eq!(<Person as ColAt<0>>::NULLABLE, false);
    assert_eq!(<Person as ColAt<1>>::NULLABLE, true);
    assert_eq!(<Person as ColAt<2>>::NULLABLE, true);
    assert_eq!(<Person as ColAt<3>>::NULLABLE, false);
    assert_eq!(<Person as ColAt<4>>::NULLABLE, true);
    assert_eq!(<Person as ColAt<5>>::NULLABLE, false);
}

#[test]
fn rust_types_exposed() {
    // Rust content types (non-Option inner types)
    fn _r0<T: Same<i64>>() {}
    fn _r1<T: Same<String>>() {}
    fn _r2<T: Same<String>>() {}
    fn _r3<T: Same<f32>>() {}
    fn _r4<T: Same<Vec<u8>>>() {}
    fn _r5<T: Same<bool>>() {}

    type R0 = <Person as ColAt<0>>::Native;
    type R1 = <Person as ColAt<1>>::Native;
    type R2 = <Person as ColAt<2>>::Native;
    type R3 = <Person as ColAt<3>>::Native;
    type R4 = <Person as ColAt<4>>::Native;
    type R5 = <Person as ColAt<5>>::Native;

    _r0::<R0>();
    _r1::<R1>();
    _r2::<R2>();
    _r3::<R3>();
    _r4::<R4>();
    _r5::<R5>();
}

#[test]
fn for_each_col_invokes_visitor_for_all_columns() {
    static VISITS: AtomicUsize = AtomicUsize::new(0);
    struct Count;
    impl ColumnVisitor for Count {
        fn visit<const I: usize, R>(_m: FieldMeta<R>) {
            let _ = I; // exercise const generic
            VISITS.fetch_add(1, Ordering::SeqCst);
        }
    }

    VISITS.store(0, Ordering::SeqCst);
    Person::for_each_col::<Count>();
    assert_eq!(VISITS.load(Ordering::SeqCst), <Person as Record>::LEN);
}
