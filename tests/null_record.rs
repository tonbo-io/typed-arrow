use arrow_array::Array;
use arrow_schema::DataType;
use typed_arrow::{Null, prelude::*};

#[derive(Record)]
struct ContainsNull {
    always_null: Null,
}

#[test]
fn null_column_metadata_and_building() {
    assert_eq!(<ContainsNull as Record>::LEN, 1);
    assert_eq!(<ContainsNull as ColAt<0>>::NAME, "always_null");
    assert_eq!(<ContainsNull as ColAt<0>>::NULLABLE, true);
    assert_eq!(<ContainsNull as ColAt<0>>::data_type(), DataType::Null);

    let rows = vec![
        ContainsNull { always_null: Null },
        ContainsNull { always_null: Null },
        ContainsNull { always_null: Null },
    ];

    let mut b = <ContainsNull as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    assert_eq!(arrays.always_null.len(), 3);
    assert_eq!(arrays.always_null.logical_null_count(), 3);
    assert_eq!(arrays.always_null.data_type(), &DataType::Null);
}
