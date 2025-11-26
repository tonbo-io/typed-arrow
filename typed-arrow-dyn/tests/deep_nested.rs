use std::sync::Arc;

use arrow_array::{Array, RecordBatch, cast};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use typed_arrow_dyn::{DynBuilders, DynCell, DynRow};

fn build_deep_nested_batch() -> RecordBatch {
    // root: Struct{
    //   user: Struct{
    //     name: Utf8 (req),
    //     devices: List<Struct{id: Int64 (req), last_seen: Timestamp(Millisecond) (opt)}> (req)
    //   } (req)
    // } (root field itself nullable)
    // metrics: LargeList<FixedSizeList<Int32 (req), 3>> (nullable)

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

    let mut b = DynBuilders::new(Arc::clone(&schema), 0);

    // Row 0: root = null, metrics = [[1,2,3], [4,5,6]]
    b.append_option_row(Some(DynRow(vec![
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
    ])))
    .unwrap();

    // Row 1: root.user = { name: "alice", devices: [{1, null}, {2, 1000}] }, metrics = null
    let user_row1 = DynCell::Struct(vec![Some(DynCell::Struct(vec![
        Some(DynCell::Str("alice".into())),
        Some(DynCell::List(vec![
            Some(DynCell::Struct(vec![Some(DynCell::I64(1)), None])),
            Some(DynCell::Struct(vec![
                Some(DynCell::I64(2)),
                Some(DynCell::I64(1000)),
            ])),
        ])),
    ]))]);
    b.append_option_row(Some(DynRow(vec![Some(user_row1), None])))
        .unwrap();

    // Row 2: root.user = { name: "bob", devices: [] }, metrics = [[7,8,9]]
    let user_row2 = DynCell::Struct(vec![Some(DynCell::Struct(vec![
        Some(DynCell::Str("bob".into())),
        Some(DynCell::List(vec![])),
    ]))]);
    b.append_option_row(Some(DynRow(vec![
        Some(user_row2),
        Some(DynCell::List(vec![Some(DynCell::FixedSizeList(vec![
            Some(DynCell::I32(7)),
            Some(DynCell::I32(8)),
            Some(DynCell::I32(9)),
        ]))])),
    ])))
    .unwrap();

    // Build via safe path
    b.try_finish_into_batch().unwrap()
}

#[test]
fn deep_nested_struct_and_lists_build() {
    let batch = build_deep_nested_batch();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);

    // Column 0: root Struct
    let root = cast::as_struct_array(batch.column(0));
    assert!(root.is_null(0));
    assert!(root.is_valid(1));
    assert!(root.is_valid(2));
    // root.user: Struct{name, devices}
    let user = cast::as_struct_array(root.column(0));
    let name = cast::as_string_array(user.column(0));
    assert!(name.is_null(0)); // masked by parent null
    assert_eq!(name.value(1), "alice");
    assert_eq!(name.value(2), "bob");
    let devices = user
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .unwrap();
    // Offsets across 3 rows: [0, 0, 2, 2] (row0 null, row1 two items, row2 empty)
    assert_eq!(devices.value_offsets(), &[0, 0, 2, 2]);
    // Device values form a StructArray of length 2
    let dev_values = cast::as_struct_array(devices.values());
    let ids = cast::as_primitive_array::<arrow_array::types::Int64Type>(dev_values.column(0));
    let last_seen = cast::as_primitive_array::<arrow_array::types::TimestampMillisecondType>(
        dev_values.column(1),
    );
    assert_eq!(ids.len(), 2);
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert!(last_seen.is_null(0));
    assert_eq!(last_seen.value(1), 1000);

    // Column 1: metrics LargeList<FixedSizeList<Int32,3>>
    let metrics = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::LargeListArray>()
        .unwrap();
    // Offsets across 3 rows: [0,2,2,3]
    assert_eq!(metrics.value_offsets(), &[0, 2, 2, 3]);
    let fl = metrics
        .values()
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeListArray>()
        .unwrap();
    assert_eq!(fl.len(), 3);
    let vals = cast::as_primitive_array::<arrow_array::types::Int32Type>(fl.values());
    assert_eq!(vals.len(), 9);
    assert_eq!(vals.value(0), 1);
    assert_eq!(vals.value(1), 2);
    assert_eq!(vals.value(2), 3);
    assert_eq!(vals.value(3), 4);
    assert_eq!(vals.value(4), 5);
    assert_eq!(vals.value(5), 6);
    assert_eq!(vals.value(6), 7);
    assert_eq!(vals.value(7), 8);
    assert_eq!(vals.value(8), 9);
}
