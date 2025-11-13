use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, TimeUnit, UnionFields, UnionMode};
use parquet::arrow::ArrowSchemaConverter;
use typed_arrow_dyn::{DynBuilders, DynCell, DynProjection, DynRow, DynSchema, DynViewError};

fn build_batch(schema: &Arc<Schema>, rows: Vec<Option<DynRow>>) -> RecordBatch {
    let mut builders = DynBuilders::new(Arc::clone(schema), rows.len());
    for row in rows {
        builders.append_option_row(row).unwrap();
    }
    builders.try_finish_into_batch().unwrap()
}

/// Helper mirroring the deep nested schema used across runtime tests.
fn deep_projection_schema() -> Arc<Schema> {
    let device_fields = vec![
        Arc::new(Field::new("id", DataType::Int64, false)),
        Arc::new(Field::new(
            "last_seen",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )),
    ];
    let devices_item = Arc::new(Field::new(
        "item",
        DataType::Struct(device_fields.into()),
        false,
    ));
    let user_fields = vec![
        Arc::new(Field::new("name", DataType::Utf8, false)),
        Arc::new(Field::new("devices", DataType::List(devices_item), false)),
    ];
    let root_user = Arc::new(Field::new(
        "user",
        DataType::Struct(user_fields.into()),
        false,
    ));
    let root_field = Field::new("root", DataType::Struct(vec![root_user].into()), true);

    let metrics_inner = Arc::new(Field::new("item", DataType::Int32, false));
    let metrics_list_item = Arc::new(Field::new(
        "item",
        DataType::FixedSizeList(metrics_inner, 3),
        false,
    ));
    let metrics_field = Field::new("metrics", DataType::LargeList(metrics_list_item), true);

    Arc::new(Schema::new(vec![root_field, metrics_field]))
}

#[test]
fn primitive_views() -> Result<(), DynViewError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float32, false),
    ]));

    let batch = build_batch(
        &schema,
        vec![
            Some(DynRow(vec![
                Some(DynCell::I64(1)),
                Some(DynCell::Str("alice".into())),
                Some(DynCell::F32(9.5)),
            ])),
            Some(DynRow(vec![
                Some(DynCell::I64(2)),
                None,
                Some(DynCell::F32(4.25)),
            ])),
        ],
    );

    let dyn_schema = DynSchema::from_ref(Arc::clone(&schema));
    let mut rows = dyn_schema.iter_views(&batch)?;

    let first = rows.next().expect("row 0")?;
    assert_eq!(first.get(0)?.and_then(|cell| cell.into_i64()), Some(1));
    assert_eq!(
        first.get(1)?.and_then(|cell| cell.into_str()),
        Some("alice")
    );
    if let Some(score) = first.get(2)?.and_then(|cell| cell.into_f32()) {
        assert!((score - 9.5).abs() < f32::EPSILON);
    } else {
        panic!("expected float view for score");
    }

    let second = rows.next().expect("row 1")?;
    assert_eq!(second.get(0)?.and_then(|cell| cell.into_i64()), Some(2));
    assert!(second.get(1)?.is_none(), "expected null name");
    if let Some(score) = second.get(2)?.and_then(|cell| cell.into_f32()) {
        assert!((score - 4.25).abs() < f32::EPSILON);
    } else {
        panic!("expected float view for score");
    }

    assert!(rows.next().is_none());
    Ok(())
}

#[test]
fn random_access_view() -> Result<(), DynViewError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float32, false),
    ]));

    let batch = build_batch(
        &schema,
        vec![
            Some(DynRow(vec![
                Some(DynCell::I64(1)),
                Some(DynCell::Str("alice".into())),
                Some(DynCell::F32(9.0)),
            ])),
            Some(DynRow(vec![
                Some(DynCell::I64(2)),
                None,
                Some(DynCell::F32(3.5)),
            ])),
        ],
    );

    let dyn_schema = DynSchema::from_ref(Arc::clone(&schema));
    let row = dyn_schema.view_at(&batch, 1)?;

    assert_eq!(row.row_index(), 1);
    assert_eq!(row.len(), 3);
    assert_eq!(row.get(0)?.and_then(|cell| cell.into_i64()), Some(2));
    assert!(row.get(1)?.is_none());
    let score = row
        .get(2)?
        .and_then(|cell| cell.into_f32())
        .expect("score should be present");
    assert!((score - 3.5).abs() < f32::EPSILON);

    Ok(())
}

#[test]
fn projection_on_single_row() -> Result<(), DynViewError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float32, true),
    ]));

    let batch = build_batch(
        &schema,
        vec![
            Some(DynRow(vec![
                Some(DynCell::U64(7)),
                Some(DynCell::Str("alpha".into())),
                Some(DynCell::F32(3.0)),
            ])),
            Some(DynRow(vec![
                Some(DynCell::U64(8)),
                Some(DynCell::Str("beta".into())),
                None,
            ])),
        ],
    );

    let dyn_schema = DynSchema::from_ref(Arc::clone(&schema));
    let projection = DynProjection::from_indices(schema.as_ref(), [1, 2])?;

    let view = projection.project_row_view(&dyn_schema, &batch, 0)?;
    assert_eq!(view.len(), 2);
    assert_eq!(view.get(0)?.and_then(|cell| cell.into_str()), Some("alpha"));
    assert_eq!(view.get(1)?.and_then(|cell| cell.into_f32()), Some(3.0));

    let raw = projection.project_row_raw(&dyn_schema, &batch, 1)?;
    assert_eq!(raw.len(), 2);
    let owned = raw.to_owned()?;
    let cells = owned.0;
    let name = cells[0]
        .as_ref()
        .and_then(|cell| match cell {
            DynCell::Str(value) => Some(value.as_str()),
            _ => None,
        })
        .expect("projected name");
    assert_eq!(name, "beta");
    assert!(cells[1].is_none(), "score should be null");

    Ok(())
}

#[test]
fn random_access_view_out_of_bounds() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = build_batch(&schema, vec![Some(DynRow(vec![Some(DynCell::I64(1))]))]);

    let dyn_schema = DynSchema::from_ref(Arc::clone(&schema));
    let err = match dyn_schema.view_at(&batch, 2) {
        Err(err) => err,
        Ok(_) => panic!("expected error for out-of-bounds row"),
    };
    match err {
        DynViewError::RowOutOfBounds { row, len } => {
            assert_eq!(row, 2);
            assert_eq!(len, 1);
        }
        other => panic!("expected RowOutOfBounds, got {other:?}"),
    }
}

#[test]
fn into_owned_converts_borrowed_cells() -> Result<(), DynViewError> {
    let address_field = Field::new(
        "address",
        DataType::Struct(
            vec![
                Arc::new(Field::new("city", DataType::Utf8, false)),
                Arc::new(Field::new("zip", DataType::Int32, true)),
            ]
            .into(),
        ),
        true,
    );
    let tags_field = Field::new(
        "tags",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    );
    let map_field = Field::new(
        "attrs",
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("keys", DataType::Utf8, false)),
                        Arc::new(Field::new("values", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        true,
    );
    let union_fields: UnionFields = [
        (0_i8, Arc::new(Field::new("count", DataType::Int32, true))),
        (1_i8, Arc::new(Field::new("label", DataType::Utf8, true))),
    ]
    .into_iter()
    .collect();
    let union_field = Field::new(
        "payload",
        DataType::Union(union_fields, UnionMode::Dense),
        true,
    );
    let fixed_field = Field::new(
        "triplet",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 3),
        true,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("blob", DataType::Binary, false),
        address_field.clone(),
        tags_field.clone(),
        map_field.clone(),
        union_field.clone(),
        fixed_field.clone(),
    ]));

    let batch = build_batch(
        &schema,
        vec![Some(DynRow(vec![
            Some(DynCell::I64(42)),
            Some(DynCell::Str("alice".into())),
            Some(DynCell::Bin(vec![0, 1, 2])),
            Some(DynCell::Struct(vec![
                Some(DynCell::Str("Seattle".into())),
                Some(DynCell::I32(98101)),
            ])),
            Some(DynCell::List(vec![Some(DynCell::Str("vip".into())), None])),
            Some(DynCell::Map(vec![(
                DynCell::Str("tier".into()),
                Some(DynCell::I64(2)),
            )])),
            Some(DynCell::union_value(1, DynCell::Str("ok".into()))),
            Some(DynCell::FixedSizeList(vec![
                Some(DynCell::I32(7)),
                Some(DynCell::I32(8)),
                Some(DynCell::I32(9)),
            ])),
        ]))],
    );

    let dyn_schema = DynSchema::from_ref(Arc::clone(&schema));
    let mut rows = dyn_schema.iter_views(&batch)?;
    let row = rows.next().expect("row 0")?;

    fn assert_expected(row: &DynRow) {
        let cells = &row.0;
        assert_eq!(cells.len(), 8);
        match cells[0].as_ref() {
            Some(DynCell::I64(value)) => assert_eq!(*value, 42),
            _ => panic!("unexpected id cell"),
        }
        match cells[1].as_ref() {
            Some(DynCell::Str(name)) => assert_eq!(name, "alice"),
            _ => panic!("unexpected name cell"),
        }
        match cells[2].as_ref() {
            Some(DynCell::Bin(bytes)) => assert_eq!(bytes, &vec![0, 1, 2]),
            _ => panic!("unexpected binary cell"),
        }
        match cells[3].as_ref() {
            Some(DynCell::Struct(fields)) => {
                assert_eq!(fields.len(), 2);
                match fields[0].as_ref() {
                    Some(DynCell::Str(city)) => assert_eq!(city, "Seattle"),
                    _ => panic!("unexpected city field"),
                }
                match fields[1].as_ref() {
                    Some(DynCell::I32(zip)) => assert_eq!(*zip, 98101),
                    _ => panic!("unexpected zip field"),
                }
            }
            _ => panic!("unexpected address cell"),
        }
        match cells[4].as_ref() {
            Some(DynCell::List(items)) => {
                assert_eq!(items.len(), 2);
                match items[0].as_ref() {
                    Some(DynCell::Str(tag)) => assert_eq!(tag, "vip"),
                    _ => panic!("unexpected tag item"),
                }
                assert!(items[1].is_none());
            }
            _ => panic!("unexpected tags cell"),
        }
        match cells[5].as_ref() {
            Some(DynCell::Map(entries)) => {
                assert_eq!(entries.len(), 1);
                let (key, value) = &entries[0];
                match key {
                    DynCell::Str(name) => assert_eq!(name, "tier"),
                    _ => panic!("unexpected map key"),
                }
                match value.as_ref() {
                    Some(DynCell::I64(v)) => assert_eq!(*v, 2),
                    _ => panic!("unexpected map value"),
                }
            }
            _ => panic!("unexpected attrs cell"),
        }
        match cells[6].as_ref() {
            Some(DynCell::Union { type_id, value }) => {
                assert_eq!(*type_id, 1);
                match value.as_deref() {
                    Some(DynCell::Str(label)) => assert_eq!(label, "ok"),
                    _ => panic!("unexpected union payload"),
                }
            }
            _ => panic!("unexpected payload cell"),
        }
        match cells[7].as_ref() {
            Some(DynCell::FixedSizeList(items)) => {
                assert_eq!(items.len(), 3);
                for (idx, expected) in [7, 8, 9].into_iter().enumerate() {
                    match items[idx].as_ref() {
                        Some(DynCell::I32(value)) => assert_eq!(*value, expected),
                        _ => panic!("unexpected fixed-size list item"),
                    }
                }
            }
            _ => panic!("unexpected triplet cell"),
        }
    }

    let owned_from_view = row.to_owned()?;
    assert_expected(&owned_from_view);

    let raw = row.into_raw()?;
    assert_eq!(raw.len(), 8);
    assert_eq!(raw.fields().len(), 8);

    let owned_from_raw = raw.to_owned()?;
    assert_expected(&owned_from_raw);

    let owned_via_into_owned = raw.clone().into_owned()?;
    assert_expected(&owned_via_into_owned);

    let raw_cells = raw.clone().into_cells();
    assert_eq!(raw_cells.len(), 8);
    assert!(raw_cells.iter().all(|cell| cell.is_some()));

    assert!(rows.next().is_none());
    Ok(())
}

#[test]
fn nested_views() -> Result<(), DynViewError> {
    let address_field = Field::new(
        "address",
        DataType::Struct(
            vec![
                Arc::new(Field::new("city", DataType::Utf8, false)),
                Arc::new(Field::new("zip", DataType::Int32, true)),
            ]
            .into(),
        ),
        true,
    );
    let tags_field = Field::new(
        "tags",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        true,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        address_field.clone(),
        tags_field.clone(),
    ]));

    let batch = build_batch(
        &schema,
        vec![
            Some(DynRow(vec![
                Some(DynCell::I64(1)),
                Some(DynCell::Struct(vec![
                    Some(DynCell::Str("NYC".into())),
                    None,
                ])),
                Some(DynCell::List(vec![
                    Some(DynCell::Str("vip".into())),
                    Some(DynCell::Str("beta".into())),
                ])),
            ])),
            Some(DynRow(vec![
                Some(DynCell::I64(2)),
                None,
                Some(DynCell::List(vec![None])),
            ])),
        ],
    );

    let dyn_schema = DynSchema::from_ref(Arc::clone(&schema));
    let mut rows = dyn_schema.iter_views(&batch)?;

    let first = rows.next().unwrap()?;
    let addr = first
        .get(1)?
        .and_then(|cell| cell.into_struct())
        .expect("address struct");
    let city = addr
        .get(0)?
        .and_then(|cell| cell.into_str())
        .expect("city value");
    assert_eq!(city, "NYC");
    assert!(addr.get(1)?.is_none());

    let tags = first
        .get(2)?
        .and_then(|cell| cell.into_list())
        .expect("tags list");
    let mut collected = Vec::new();
    for idx in 0..tags.len() {
        let tag = tags
            .get(idx)?
            .and_then(|cell| cell.into_str())
            .expect("tag item");
        collected.push(tag.to_string());
    }
    assert_eq!(collected, ["vip", "beta"]);

    let second = rows.next().unwrap()?;
    assert!(second.get(1)?.is_none(), "address should be null");
    let tags = second
        .get(2)?
        .and_then(|cell| cell.into_list())
        .expect("tags list");
    let mut tags_vec = Vec::new();
    for idx in 0..tags.len() {
        let mapped = tags
            .get(idx)?
            .and_then(|cell| cell.into_str().map(|s| s.to_string()));
        tags_vec.push(mapped);
    }
    assert_eq!(tags_vec, vec![None]);

    Ok(())
}

#[test]
fn map_and_union_views() -> Result<(), DynViewError> {
    let map_field = Field::new(
        "attrs",
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("keys", DataType::Utf8, false)),
                        Arc::new(Field::new("values", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            true,
        ),
        true,
    );
    let union_fields: UnionFields = [
        (0_i8, Arc::new(Field::new("count", DataType::Int32, true))),
        (1_i8, Arc::new(Field::new("label", DataType::Utf8, true))),
    ]
    .into_iter()
    .collect();
    let union_field = Field::new(
        "payload",
        DataType::Union(union_fields, UnionMode::Dense),
        true,
    );
    let schema = Arc::new(Schema::new(vec![map_field.clone(), union_field.clone()]));

    let batch = build_batch(
        &schema,
        vec![
            Some(DynRow(vec![
                Some(DynCell::Map(vec![
                    (DynCell::Str("a".into()), Some(DynCell::I64(1))),
                    (DynCell::Str("b".into()), None),
                ])),
                Some(DynCell::union_value(0, DynCell::I32(99))),
            ])),
            Some(DynRow(vec![
                Some(DynCell::Map(vec![(
                    DynCell::Str("z".into()),
                    Some(DynCell::I64(7)),
                )])),
                Some(DynCell::union_value(1, DynCell::Str("done".into()))),
            ])),
        ],
    );

    let dyn_schema = DynSchema::from_ref(Arc::clone(&schema));
    let mut rows = dyn_schema.iter_views(&batch)?;

    let first = rows.next().unwrap()?;

    // Map assertions
    let map = first
        .get(0)?
        .and_then(|cell| cell.into_map())
        .expect("map view");
    let mut entries = Vec::new();
    for idx in 0..map.len() {
        let (key, val) = map.get(idx)?;
        let key = key.into_str().map(|k| k.to_string()).expect("utf8 key");
        let val = val.map(|cell| cell.into_i64().expect("int64 value"));
        entries.push((key, val));
    }
    assert_eq!(
        entries,
        vec![("a".to_string(), Some(1)), ("b".to_string(), None)]
    );

    // Union assertions
    let payload = first
        .get(1)?
        .and_then(|cell| cell.into_union())
        .expect("union view");
    assert_eq!(payload.type_id(), 0);
    let value = payload
        .value()?
        .and_then(|cell| cell.into_i32())
        .expect("count payload");
    assert_eq!(value, 99);

    let second = rows.next().unwrap()?;
    let payload = second
        .get(1)?
        .and_then(|cell| cell.into_union())
        .expect("union view");
    assert_eq!(payload.type_id(), 1);
    let label = payload
        .value()?
        .and_then(|cell| cell.into_str())
        .expect("label payload");
    assert_eq!(label, "done");

    Ok(())
}

#[test]
fn large_and_fixed_size_list_views() -> Result<(), DynViewError> {
    let large_list_field = Field::new(
        "large",
        DataType::LargeList(Arc::new(Field::new("item", DataType::Int16, true))),
        true,
    );
    let fixed_struct_fields: Vec<_> = vec![
        Arc::new(Field::new("flag", DataType::Boolean, false)),
        Arc::new(Field::new("value", DataType::Utf8, true)),
    ];
    let fixed_list_field = Field::new(
        "fixed",
        DataType::FixedSizeList(
            Arc::new(Field::new(
                "item",
                DataType::Struct(fixed_struct_fields.clone().into()),
                true,
            )),
            2,
        ),
        true,
    );

    let schema = Arc::new(Schema::new(vec![
        large_list_field.clone(),
        fixed_list_field.clone(),
    ]));

    let batch = build_batch(
        &schema,
        vec![
            Some(DynRow(vec![
                Some(DynCell::List(vec![
                    Some(DynCell::I16(5)),
                    None,
                    Some(DynCell::I16(-7)),
                ])),
                Some(DynCell::FixedSizeList(vec![
                    Some(DynCell::Struct(vec![
                        Some(DynCell::Bool(true)),
                        Some(DynCell::Str("ok".into())),
                    ])),
                    Some(DynCell::Struct(vec![Some(DynCell::Bool(false)), None])),
                ])),
            ])),
            Some(DynRow(vec![Some(DynCell::List(vec![])), None])),
            Some(DynRow(vec![
                None,
                Some(DynCell::FixedSizeList(vec![
                    Some(DynCell::Struct(vec![
                        Some(DynCell::Bool(true)),
                        Some(DynCell::Str("z".into())),
                    ])),
                    Some(DynCell::Struct(vec![
                        Some(DynCell::Bool(true)),
                        Some(DynCell::Str("z".into())),
                    ])),
                ])),
            ])),
        ],
    );

    let dyn_schema = DynSchema::from_ref(Arc::clone(&schema));
    let mut rows = dyn_schema.iter_views(&batch)?;

    let first = rows.next().unwrap()?;
    let large = first
        .get(0)?
        .and_then(|cell| cell.into_list())
        .expect("large list");
    assert_eq!(large.len(), 3);
    assert_eq!(large.get(0)?.and_then(|cell| cell.into_i16()), Some(5));
    assert!(large.get(1)?.is_none());
    assert_eq!(large.get(2)?.and_then(|cell| cell.into_i16()), Some(-7));

    let fixed = first
        .get(1)?
        .and_then(|cell| cell.into_fixed_size_list())
        .expect("fixed list");
    assert_eq!(fixed.len(), 2);
    let first_item = fixed
        .get(0)?
        .and_then(|cell| cell.into_struct())
        .expect("first struct entry");
    assert_eq!(
        first_item.get(0)?.and_then(|cell| cell.into_bool()),
        Some(true)
    );
    assert_eq!(
        first_item.get(1)?.and_then(|cell| cell.into_str()),
        Some("ok")
    );
    let second_item = fixed
        .get(1)?
        .and_then(|cell| cell.into_struct())
        .expect("second struct entry");
    assert_eq!(
        second_item.get(0)?.and_then(|cell| cell.into_bool()),
        Some(false)
    );
    assert!(second_item.get(1)?.is_none());

    let second = rows.next().unwrap()?;
    let large = second
        .get(0)?
        .and_then(|cell| cell.into_list())
        .expect("large list");
    assert!(large.is_empty());
    assert!(second.get(1)?.is_none());

    let third = rows.next().unwrap()?;
    assert!(third.get(0)?.is_none());
    let fixed = third
        .get(1)?
        .and_then(|cell| cell.into_fixed_size_list())
        .expect("fixed list");
    for idx in 0..fixed.len() {
        let entry = fixed
            .get(idx)?
            .and_then(|cell| cell.into_struct())
            .expect("struct entry");
        assert_eq!(entry.get(0)?.and_then(|cell| cell.into_bool()), Some(true));
        assert_eq!(entry.get(1)?.and_then(|cell| cell.into_str()), Some("z"));
    }

    assert!(rows.next().is_none());
    Ok(())
}

#[test]
fn projected_views() -> Result<(), DynViewError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, true),
    ]));

    let batch = build_batch(
        &schema,
        vec![
            Some(DynRow(vec![
                Some(DynCell::I64(1)),
                Some(DynCell::Str("alice".into())),
                Some(DynCell::F64(10.0)),
            ])),
            Some(DynRow(vec![
                Some(DynCell::I64(2)),
                Some(DynCell::Str("bob".into())),
                None,
            ])),
        ],
    );

    let projection_schema = Schema::new(vec![
        Field::new("score", DataType::Float64, true),
        Field::new("name", DataType::Utf8, false),
    ]);
    let projection = DynProjection::from_schema(schema.as_ref(), &projection_schema)?;

    let dyn_schema = DynSchema::from_ref(Arc::clone(&schema));
    let mut rows = dyn_schema.iter_views(&batch)?.project(projection)?;

    let first = rows.next().unwrap()?;
    assert_eq!(first.len(), 2);
    if let Some(score) = first.get(0)?.and_then(|cell| cell.into_f64()) {
        assert!((score - 10.0).abs() < f64::EPSILON);
    } else {
        panic!("expected projected score");
    }
    assert_eq!(
        first.get(1)?.and_then(|cell| cell.into_str()),
        Some("alice")
    );
    assert!(first.get(2).is_err());
    assert!(first.get_by_name("id").is_none());

    let second = rows.next().unwrap()?;
    assert_eq!(second.len(), 2);
    assert!(second.get(0)?.is_none());
    assert_eq!(second.get(1)?.and_then(|cell| cell.into_str()), Some("bob"));
    assert!(rows.next().is_none());

    // Index-based projection should yield the same values in a different order.
    let index_projection = DynProjection::from_indices(schema.as_ref(), [2, 0])?;
    let mut index_rows = dyn_schema.iter_views(&batch)?.project(index_projection)?;

    let first = index_rows.next().unwrap()?;
    if let Some(score) = first.get(0)?.and_then(|cell| cell.into_f64()) {
        assert!((score - 10.0).abs() < f64::EPSILON);
    } else {
        panic!("expected score via indices");
    }
    assert_eq!(first.get(1)?.and_then(|cell| cell.into_i64()), Some(1));

    let second = index_rows.next().unwrap()?;
    assert!(second.get(0)?.is_none());
    assert_eq!(second.get(1)?.and_then(|cell| cell.into_i64()), Some(2));
    assert!(index_rows.next().is_none());

    Ok(())
}

#[test]
fn projected_nested_schema_from_schema() -> Result<(), DynViewError> {
    let schema = deep_projection_schema();
    let projected_device_last_seen = Arc::new(Field::new(
        "last_seen",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        true,
    ));
    let projected_devices_item = Arc::new(Field::new(
        "item",
        DataType::Struct(vec![projected_device_last_seen].into()),
        false,
    ));
    let projection_schema = Schema::new(vec![
        Field::new(
            "root",
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "user",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("name", DataType::Utf8, false)),
                            Arc::new(Field::new(
                                "devices",
                                DataType::List(Arc::clone(&projected_devices_item)),
                                false,
                            )),
                        ]
                        .into(),
                    ),
                    false,
                ))]
                .into(),
            ),
            true,
        ),
        Field::new(
            "metrics",
            DataType::LargeList(Arc::new(Field::new(
                "item",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 3),
                false,
            ))),
            true,
        ),
    ]);
    let projection = DynProjection::from_schema(schema.as_ref(), &projection_schema)?;

    let batch = build_batch(
        &schema,
        vec![Some(DynRow(vec![
            Some(DynCell::Struct(vec![Some(DynCell::Struct(vec![
                Some(DynCell::Str("carol".into())),
                Some(DynCell::List(vec![
                    Some(DynCell::Struct(vec![
                        Some(DynCell::I64(42)),
                        Some(DynCell::I64(1_000)),
                    ])),
                    Some(DynCell::Struct(vec![Some(DynCell::I64(43)), None])),
                ])),
            ]))])),
            Some(DynCell::List(vec![Some(DynCell::FixedSizeList(vec![
                Some(DynCell::I32(1)),
                Some(DynCell::I32(2)),
                Some(DynCell::I32(3)),
            ]))])),
        ]))],
    );
    let dyn_schema = DynSchema::from_ref(Arc::clone(&schema));
    let mut projected_rows = dyn_schema.iter_views(&batch)?.project(projection.clone())?;

    let row = projected_rows.next().expect("row 0")?;
    assert_eq!(row.len(), 2);
    let root = row
        .get(0)?
        .and_then(|cell| cell.into_struct())
        .expect("root struct");
    let user = root
        .get_by_name("user")
        .expect("user field")?
        .and_then(|cell| cell.into_struct())
        .expect("user struct");
    assert_eq!(
        user.get_by_name("name")
            .expect("name field")?
            .and_then(|cell| cell.into_str()),
        Some("carol")
    );
    let devices = user
        .get_by_name("devices")
        .expect("devices field")?
        .and_then(|cell| cell.into_list())
        .expect("devices list");
    assert_eq!(devices.len(), 2);
    let first_device = devices
        .get(0)?
        .and_then(|cell| cell.into_struct())
        .expect("first device");
    assert_eq!(first_device.len(), 1, "projected struct keeps single field");
    assert_eq!(
        first_device
            .get(0)?
            .and_then(|cell| cell.into_i64())
            .expect("last_seen value"),
        1_000
    );
    assert!(
        first_device.get_by_name("id").is_none(),
        "device id should not be projected"
    );
    let second_device = devices
        .get(1)?
        .and_then(|cell| cell.into_struct())
        .expect("second device");
    assert!(second_device.get(0)?.is_none());

    let metrics = row
        .get(1)?
        .and_then(|cell| cell.into_list())
        .expect("metrics list");
    assert_eq!(metrics.len(), 1);
    let bucket = metrics
        .get(0)?
        .and_then(|cell| cell.into_fixed_size_list())
        .expect("fixed-size bucket");
    let mut metric_values = Vec::new();
    for idx in 0..bucket.len() {
        let value = bucket
            .get(idx)?
            .and_then(|cell| cell.into_i32())
            .expect("metric value");
        metric_values.push(value);
    }
    assert_eq!(metric_values, vec![1, 2, 3]);

    let mask = projection.to_parquet_mask();
    let descriptor = ArrowSchemaConverter::new()
        .convert(schema.as_ref())
        .expect("convert schema");
    let mut included_paths = Vec::new();
    for idx in 0..descriptor.num_columns() {
        let path = descriptor.column(idx).path().string();
        if mask.leaf_included(idx) {
            included_paths.push(path);
        }
    }
    assert_eq!(
        included_paths,
        vec![
            "root.user.name".to_string(),
            "root.user.devices.list.item.last_seen".to_string(),
            "metrics.list.item.list.item".to_string(),
        ]
    );
    let column_paths: Vec<_> = (0..descriptor.num_columns())
        .map(|idx| descriptor.column(idx).path().string())
        .collect();
    let id_index = column_paths
        .iter()
        .position(|path| path == "root.user.devices.list.item.id")
        .expect("device id column path");
    assert!(
        !mask.leaf_included(id_index),
        "non-projected device id leaf should be excluded"
    );

    assert!(projected_rows.next().is_none());
    Ok(())
}

#[test]
fn dictionary_views() -> Result<(), DynViewError> {
    let dict_utf8 = Field::new(
        "dict_utf8",
        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
        true,
    );
    let dict_bin = Field::new(
        "dict_bin",
        DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Binary)),
        true,
    );

    let schema = Arc::new(Schema::new(vec![dict_utf8.clone(), dict_bin.clone()]));

    let batch = build_batch(
        &schema,
        vec![
            Some(DynRow(vec![
                Some(DynCell::Str("hello".into())),
                Some(DynCell::Bin(vec![1, 2, 3])),
            ])),
            Some(DynRow(vec![Some(DynCell::Str("world".into())), None])),
            Some(DynRow(vec![
                Some(DynCell::Str("hello".into())),
                Some(DynCell::Bin(vec![4, 5, 6])),
            ])),
        ],
    );

    let dyn_schema = DynSchema::from_ref(Arc::clone(&schema));
    let mut rows = dyn_schema.iter_views(&batch)?;

    let first = rows.next().unwrap()?;
    assert_eq!(
        first.get(0)?.and_then(|cell| cell.into_str()),
        Some("hello")
    );
    let first_bin = first
        .get(1)?
        .and_then(|cell| cell.into_bin())
        .expect("binary value");
    assert_eq!(first_bin, &[1, 2, 3]);

    let second = rows.next().unwrap()?;
    assert_eq!(
        second.get(0)?.and_then(|cell| cell.into_str()),
        Some("world")
    );
    assert!(second.get(1)?.is_none());

    let third = rows.next().unwrap()?;
    assert_eq!(
        third.get(0)?.and_then(|cell| cell.into_str()),
        Some("hello")
    );
    let third_bin = third
        .get(1)?
        .and_then(|cell| cell.into_bin())
        .expect("binary value");
    assert_eq!(third_bin, &[4, 5, 6]);

    assert!(rows.next().is_none());
    Ok(())
}

#[test]
fn deep_nested_views() -> Result<(), DynViewError> {
    let device_fields = vec![
        Arc::new(Field::new("id", DataType::Int64, false)),
        Arc::new(Field::new(
            "last_seen",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )),
    ];
    let devices_item = Arc::new(Field::new(
        "item",
        DataType::Struct(device_fields.into()),
        false,
    ));
    let user_fields = vec![
        Arc::new(Field::new("name", DataType::Utf8, false)),
        Arc::new(Field::new("devices", DataType::List(devices_item), false)),
    ];
    let root_user = Arc::new(Field::new(
        "user",
        DataType::Struct(user_fields.into()),
        false,
    ));
    let root_struct = Field::new("root", DataType::Struct(vec![root_user].into()), true);

    let metrics_inner = Arc::new(Field::new("item", DataType::Int32, false));
    let metrics_list_item = Arc::new(Field::new(
        "item",
        DataType::FixedSizeList(metrics_inner, 3),
        false,
    ));
    let metrics = Field::new("metrics", DataType::LargeList(metrics_list_item), true);

    let schema = Arc::new(Schema::new(vec![root_struct, metrics]));

    let batch = build_batch(
        &schema,
        vec![
            Some(DynRow(vec![
                None,
                Some(DynCell::List(vec![
                    Some(DynCell::FixedSizeList(vec![
                        Some(DynCell::I32(1)),
                        Some(DynCell::I32(2)),
                        Some(DynCell::I32(3)),
                    ])),
                    Some(DynCell::FixedSizeList(vec![
                        Some(DynCell::I32(4)),
                        Some(DynCell::I32(5)),
                        Some(DynCell::I32(6)),
                    ])),
                ])),
            ])),
            Some(DynRow(vec![
                Some(DynCell::Struct(vec![Some(DynCell::Struct(vec![
                    Some(DynCell::Str("alice".into())),
                    Some(DynCell::List(vec![
                        Some(DynCell::Struct(vec![Some(DynCell::I64(1)), None])),
                        Some(DynCell::Struct(vec![
                            Some(DynCell::I64(2)),
                            Some(DynCell::I64(1000)),
                        ])),
                    ])),
                ]))])),
                None,
            ])),
            Some(DynRow(vec![
                Some(DynCell::Struct(vec![Some(DynCell::Struct(vec![
                    Some(DynCell::Str("bob".into())),
                    Some(DynCell::List(vec![])),
                ]))])),
                Some(DynCell::List(vec![Some(DynCell::FixedSizeList(vec![
                    Some(DynCell::I32(7)),
                    Some(DynCell::I32(8)),
                    Some(DynCell::I32(9)),
                ]))])),
            ])),
        ],
    );

    let dyn_schema = DynSchema::from_ref(Arc::clone(&schema));
    let mut rows = dyn_schema.iter_views(&batch)?;

    let first = rows.next().unwrap()?;
    assert!(first.get(0)?.is_none());
    let metrics = first
        .get(1)?
        .and_then(|cell| cell.into_list())
        .expect("metrics large list");
    assert_eq!(metrics.len(), 2);
    for (idx, expected) in [[1, 2, 3], [4, 5, 6]].into_iter().enumerate() {
        let entry = metrics
            .get(idx)?
            .and_then(|cell| cell.into_fixed_size_list())
            .expect("fixed size list entry");
        assert_eq!(entry.len(), 3);
        for (pos, &value) in expected.iter().enumerate() {
            let item = entry
                .get(pos)?
                .and_then(|cell| cell.into_i32())
                .expect("int32 value");
            assert_eq!(item, value);
        }
    }

    let second = rows.next().unwrap()?;
    let root = second
        .get(0)?
        .and_then(|cell| cell.into_struct())
        .expect("root struct");
    let user = root
        .get(0)?
        .and_then(|cell| cell.into_struct())
        .expect("user struct");
    assert_eq!(user.get(0)?.and_then(|cell| cell.into_str()), Some("alice"));
    let devices = user
        .get(1)?
        .and_then(|cell| cell.into_list())
        .expect("devices list");
    assert_eq!(devices.len(), 2);
    let first_device = devices
        .get(0)?
        .and_then(|cell| cell.into_struct())
        .expect("device struct");
    assert_eq!(
        first_device.get(0)?.and_then(|cell| cell.into_i64()),
        Some(1)
    );
    assert!(first_device.get(1)?.is_none());
    let second_device = devices
        .get(1)?
        .and_then(|cell| cell.into_struct())
        .expect("device struct");
    assert_eq!(
        second_device.get(0)?.and_then(|cell| cell.into_i64()),
        Some(2)
    );
    assert_eq!(
        second_device.get(1)?.and_then(|cell| cell.into_i64()),
        Some(1000)
    );
    assert!(second.get(1)?.is_none());

    let third = rows.next().unwrap()?;
    let root = third
        .get(0)?
        .and_then(|cell| cell.into_struct())
        .expect("root struct");
    let user = root
        .get(0)?
        .and_then(|cell| cell.into_struct())
        .expect("user struct");
    assert_eq!(user.get(0)?.and_then(|cell| cell.into_str()), Some("bob"));
    let devices = user
        .get(1)?
        .and_then(|cell| cell.into_list())
        .expect("devices list");
    assert_eq!(devices.len(), 0);
    let metrics = third
        .get(1)?
        .and_then(|cell| cell.into_list())
        .expect("metrics list");
    assert_eq!(metrics.len(), 1);
    let fixed = metrics
        .get(0)?
        .and_then(|cell| cell.into_fixed_size_list())
        .expect("fixed size list");
    assert_eq!(fixed.len(), 3);
    for (idx, expected) in [7, 8, 9].into_iter().enumerate() {
        let value = fixed
            .get(idx)?
            .and_then(|cell| cell.into_i32())
            .expect("metric value");
        assert_eq!(value, expected);
    }

    assert!(rows.next().is_none());
    Ok(())
}
