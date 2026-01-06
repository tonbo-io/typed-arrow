//! Test view-to-owned conversion functionality

use typed_arrow::arrow_array::RecordBatch;
use typed_arrow::{Dictionary, FixedSizeList, List, Map, OrderedMap, prelude::*};

#[derive(Record, Clone)]
struct Address {
    city: String,
    zip: Option<i32>,
}

#[derive(Record, Clone)]
struct Person {
    id: i64,
    name: String,
    address: Option<Address>,
    score: f64,
}

#[test]
fn test_view_conversion_flat() -> Result<(), SchemaError> {
    let rows = vec![
        Person {
            id: 1,
            name: "Alice".into(),
            address: Some(Address {
                city: "NYC".into(),
                zip: Some(10001),
            }),
            score: 95.5,
        },
        Person {
            id: 2,
            name: "Bob".into(),
            address: None,
            score: 87.3,
        },
        Person {
            id: 3,
            name: "Carol".into(),
            address: Some(Address {
                city: "SF".into(),
                zip: None,
            }),
            score: 92.1,
        },
    ];

    // Build RecordBatch
    let expected_rows = rows.clone();
    let mut b = <Person as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    // Read views and convert to owned
    let views = batch.iter_views::<Person>()?;
    let owned_from_views: Vec<Person> = views
        .try_flatten()?
        .into_iter()
        .map(|view| view.try_into())
        .collect::<Result<Vec<_>, _>>()?;

    // Verify counts match
    assert_eq!(owned_from_views.len(), expected_rows.len());

    // Verify individual fields (since we removed PartialEq derives)
    for (owned, original) in owned_from_views.iter().zip(expected_rows.iter()) {
        assert_eq!(owned.id, original.id);
        assert_eq!(owned.name, original.name);
        assert_eq!(owned.score, original.score);
        match (&owned.address, &original.address) {
            (Some(a1), Some(a2)) => {
                assert_eq!(a1.city, a2.city);
                assert_eq!(a1.zip, a2.zip);
            }
            (None, None) => {}
            _ => panic!("Address mismatch"),
        }
    }

    Ok(())
}

#[test]
fn test_view_conversion_nested() -> Result<(), SchemaError> {
    let original = Person {
        id: 42,
        name: "Dave".into(),
        address: Some(Address {
            city: "Boston".into(),
            zip: Some(2101),
        }),
        score: 88.8,
    };

    // Build single-row RecordBatch
    let expected = original.clone();
    let mut b = <Person as BuildRows>::new_builders(1);
    b.append_row(original);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    // Read view and convert to owned
    let views = batch.iter_views::<Person>()?;
    let view_vec = views.try_flatten()?;
    let view = view_vec.into_iter().next().unwrap();
    let owned: Person = view.try_into()?;

    // Verify nested struct was properly converted
    assert_eq!(owned.id, expected.id);
    assert_eq!(owned.name, "Dave");
    assert_eq!(owned.score, expected.score);
    assert_eq!(owned.address.as_ref().unwrap().city, "Boston");
    assert_eq!(owned.address.as_ref().unwrap().zip, Some(2101));

    Ok(())
}

#[test]
fn test_view_references_are_borrowed() -> Result<(), SchemaError> {
    let rows = vec![Person {
        id: 100,
        name: "TestUser".into(),
        address: Some(Address {
            city: "Portland".into(),
            zip: Some(97201),
        }),
        score: 75.0,
    }];

    let mut b = <Person as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    let views = batch.iter_views::<Person>()?;
    for view in views.try_flatten()? {
        // View fields are borrowed references (&str for strings)
        let name_ref: &str = view.name;
        assert_eq!(name_ref, "TestUser");

        if let Some(ref addr_view) = view.address {
            let city_ref: &str = addr_view.city;
            assert_eq!(city_ref, "Portland");
        }

        // Convert to owned allocates new data
        let owned: Person = view.try_into()?;
        assert_eq!(owned.name, "TestUser");
    }

    Ok(())
}

// Tests for direct collection conversions (not through Record)

#[test]
fn test_list_conversion() -> Result<(), SchemaError> {
    #[derive(Record)]
    struct Data {
        items: List<i32>,
    }

    let rows = vec![
        Data {
            items: List::new(vec![1, 2, 3]),
        },
        Data {
            items: List::new(vec![4, 5]),
        },
    ];

    let mut b = <Data as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let batch = b.finish().into_record_batch();

    let views = batch.iter_views::<Data>()?;
    for view in views.try_flatten()? {
        let owned: Data = view.try_into()?;
        assert!(!owned.items.values().is_empty());
    }

    Ok(())
}

#[test]
fn test_list_nullable_items_conversion() -> Result<(), SchemaError> {
    #[derive(Record)]
    struct Data {
        items: List<Option<String>>,
    }

    let rows = vec![Data {
        items: List::new(vec![Some("hello".into()), None, Some("world".into())]),
    }];

    let mut b = <Data as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let batch = b.finish().into_record_batch();

    let views = batch.iter_views::<Data>()?;
    let view = views.try_flatten()?.into_iter().next().unwrap();
    let owned: Data = view.try_into()?;

    assert_eq!(owned.items.values().len(), 3);
    assert_eq!(owned.items.values()[0].as_ref().unwrap(), "hello");
    assert!(owned.items.values()[1].is_none());
    assert_eq!(owned.items.values()[2].as_ref().unwrap(), "world");

    Ok(())
}

#[test]
fn test_fixed_size_list_conversion() -> Result<(), SchemaError> {
    #[derive(Record)]
    struct Data {
        coords: FixedSizeList<f64, 3>,
    }

    let rows = vec![
        Data {
            coords: FixedSizeList::new([1.0, 2.0, 3.0]),
        },
        Data {
            coords: FixedSizeList::new([4.0, 5.0, 6.0]),
        },
    ];

    let mut b = <Data as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let batch = b.finish().into_record_batch();

    let views = batch.iter_views::<Data>()?;
    for view in views.try_flatten()? {
        let owned: Data = view.try_into()?;
        assert_eq!(owned.coords.values().len(), 3);
    }

    Ok(())
}

#[test]
fn test_map_conversion() -> Result<(), SchemaError> {
    #[derive(Record)]
    struct Data {
        metadata: Map<String, i32>,
    }

    let rows = vec![Data {
        metadata: Map::new(vec![("count".into(), 10), ("offset".into(), 20)]),
    }];

    let mut b = <Data as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let batch = b.finish().into_record_batch();

    let views = batch.iter_views::<Data>()?;
    let view = views.try_flatten()?.into_iter().next().unwrap();
    let owned: Data = view.try_into()?;

    assert_eq!(owned.metadata.entries().len(), 2);

    Ok(())
}

#[test]
fn test_ordered_map_conversion() -> Result<(), SchemaError> {
    use std::collections::BTreeMap;

    #[derive(Record)]
    struct Data {
        metadata: OrderedMap<String, Option<String>>,
    }

    let mut map = BTreeMap::new();
    map.insert("key1".into(), Some("value1".into()));
    map.insert("key2".into(), None);

    let rows = vec![Data {
        metadata: OrderedMap::new(map),
    }];

    let mut b = <Data as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let batch = b.finish().into_record_batch();

    let views = batch.iter_views::<Data>()?;
    let view = views.try_flatten()?.into_iter().next().unwrap();
    let owned: Data = view.try_into()?;

    assert_eq!(owned.metadata.map().len(), 2);

    Ok(())
}

#[test]
fn test_dictionary_conversion() -> Result<(), SchemaError> {
    #[derive(Record)]
    struct Data {
        category: Dictionary<i32, String>,
    }

    let rows = vec![
        Data {
            category: Dictionary::new("electronics".into()),
        },
        Data {
            category: Dictionary::new("books".into()),
        },
        Data {
            category: Dictionary::new("electronics".into()),
        },
    ];

    let mut b = <Data as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let batch = b.finish().into_record_batch();

    let views = batch.iter_views::<Data>()?;
    for view in views.try_flatten()? {
        let owned: Data = view.try_into()?;
        assert!(!owned.category.value().is_empty());
    }

    Ok(())
}

#[test]
fn test_dictionary_primitive_value_conversion() -> Result<(), SchemaError> {
    #[derive(Record)]
    struct Data {
        code: Dictionary<i32, i64>,
    }

    let rows = vec![
        Data {
            code: Dictionary::new(100),
        },
        Data {
            code: Dictionary::new(200),
        },
    ];

    let mut b = <Data as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let batch = b.finish().into_record_batch();

    let views = batch.iter_views::<Data>()?;
    let owned_values: Vec<Data> = views
        .try_flatten()?
        .into_iter()
        .map(|view| view.try_into())
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(*owned_values[0].code.value(), 100);
    assert_eq!(*owned_values[1].code.value(), 200);

    Ok(())
}

#[test]
fn test_fixed_size_binary_conversion() -> Result<(), SchemaError> {
    #[derive(Record)]
    struct Data {
        hash: [u8; 4],
    }

    let rows = vec![Data { hash: [1, 2, 3, 4] }, Data { hash: [5, 6, 7, 8] }];

    let mut b = <Data as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let batch = b.finish().into_record_batch();

    let views = batch.iter_views::<Data>()?;
    let owned_values: Vec<Data> = views
        .try_flatten()?
        .into_iter()
        .map(|view| view.try_into())
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(owned_values[0].hash, [1, 2, 3, 4]);
    assert_eq!(owned_values[1].hash, [5, 6, 7, 8]);

    Ok(())
}

#[test]
fn test_union_conversion() -> Result<(), SchemaError> {
    #[derive(Union)]
    #[union(mode = "dense")]
    enum Value {
        #[union(tag = 0)]
        Int(i64),
        #[union(tag = 1)]
        Text(String),
    }

    #[derive(Record)]
    struct Data {
        value: Value,
    }

    let rows = vec![
        Data {
            value: Value::Int(42),
        },
        Data {
            value: Value::Text("hello".into()),
        },
    ];

    let mut b = <Data as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let batch = b.finish().into_record_batch();

    let views = batch.iter_views::<Data>()?;
    let owned_values: Vec<Data> = views
        .try_flatten()?
        .into_iter()
        .map(|view| view.try_into())
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(owned_values.len(), 2);

    Ok(())
}

#[test]
fn test_conversion_error_out_of_bounds() {
    use typed_arrow::{bridge::ArrowBindingView, schema::ViewAccessError};

    #[derive(Record)]
    struct Data {
        value: i64,
    }

    let rows = vec![Data { value: 42 }];
    let mut b = <Data as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let batch = b.finish().into_record_batch();

    // Attempt to access an out-of-bounds index
    let result = batch.iter_views::<Data>();
    assert!(result.is_ok());

    // Try to get view at index 999 (out of bounds)
    let array = batch.column(0);
    let typed_array = array
        .as_any()
        .downcast_ref::<<i64 as ArrowBindingView>::Array>()
        .unwrap();

    let result = <i64 as ArrowBindingView>::get_view(typed_array, 999);
    assert!(matches!(result, Err(ViewAccessError::OutOfBounds { .. })));
}

#[test]
fn test_nested_conversion_with_nulls() -> Result<(), SchemaError> {
    #[derive(Record)]
    struct Inner {
        text: String,
    }

    #[derive(Record)]
    struct Outer {
        inner: Option<Inner>,
    }

    let rows = vec![
        Outer {
            inner: Some(Inner {
                text: "present".into(),
            }),
        },
        Outer { inner: None },
    ];

    let mut b = <Outer as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let batch = b.finish().into_record_batch();

    let views = batch.iter_views::<Outer>()?;
    let owned_values: Vec<Outer> = views
        .try_flatten()?
        .into_iter()
        .map(|view| view.try_into())
        .collect::<Result<Vec<_>, _>>()?;

    assert!(owned_values[0].inner.is_some());
    assert_eq!(owned_values[0].inner.as_ref().unwrap().text, "present");
    assert!(owned_values[1].inner.is_none());

    Ok(())
}
