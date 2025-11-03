use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use typed_arrow_dyn::{DynBuilders, DynCell, DynProjection, DynRow, DynSchema};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // schema: { id: Int64, profile: Struct{name: Utf8, age: Int32?}, tags: LargeList<Utf8?> }
    let profile_fields = vec![
        Arc::new(Field::new("name", DataType::Utf8, false)),
        Arc::new(Field::new("age", DataType::Int32, true)),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("profile", DataType::Struct(profile_fields.into()), true),
        Field::new(
            "tags",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
    ]));

    // Build the batch using dynamic rows.
    let mut builders = DynBuilders::new(Arc::clone(&schema), 3);
    builders.append_option_row(Some(DynRow(vec![
        Some(DynCell::I64(1)),
        Some(DynCell::Struct(vec![
            Some(DynCell::Str("alice".into())),
            Some(DynCell::I32(34)),
        ])),
        Some(DynCell::List(vec![
            Some(DynCell::Str("rust".into())),
            Some(DynCell::Str("arrow".into())),
        ])),
    ])))?;
    builders.append_option_row(Some(DynRow(vec![
        Some(DynCell::I64(2)),
        None,
        Some(DynCell::List(vec![
            Some(DynCell::Str("analytics".into())),
            None,
        ])),
    ])))?;
    builders.append_option_row(Some(DynRow(vec![
        Some(DynCell::I64(3)),
        Some(DynCell::Struct(vec![
            Some(DynCell::Str("carol".into())),
            None,
        ])),
        Some(DynCell::List(vec![])),
    ])))?;
    let batch = builders.try_finish_into_batch()?;

    // Iterate over borrowed views with zero-copy access.
    let dyn_schema = DynSchema::from_ref(Arc::clone(&schema));
    for row in dyn_schema.iter_views(&batch)? {
        let row = row?;

        let id = row
            .get(0)?
            .and_then(|cell| cell.into_i64())
            .expect("id column must be i64");

        let name = row
            .get_by_name("profile")
            .and_then(|res| res.ok())
            .and_then(|opt| opt)
            .and_then(|cell| cell.into_struct())
            .and_then(|profile| {
                profile
                    .get(0)
                    .ok()
                    .and_then(|opt| opt)
                    .and_then(|cell| cell.into_str())
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| "<anonymous>".to_string());

        let mut tags = Vec::new();
        if let Some(list) = row
            .get_by_name("tags")
            .and_then(|res| res.ok())
            .and_then(|opt| opt)
            .and_then(|cell| cell.into_list())
        {
            for idx in 0..list.len() {
                let entry = list.get(idx)?;
                tags.push(entry.and_then(|cell| cell.into_str().map(str::to_owned)));
            }
        }

        println!("id={id} name={name} tags={tags:?}");
    }

    // Project down to just `id` and `tags` and iterate lazily.
    let projection_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "tags",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
    ]);
    let projection = DynProjection::from_schema(schema.as_ref(), &projection_schema)?;
    let mut projected = dyn_schema.iter_views(&batch)?.project(projection)?;

    println!("-- projected columns --");
    while let Some(row) = projected.next() {
        let row = row?;
        let id = row
            .get(0)?
            .and_then(|cell| cell.into_i64())
            .expect("projected id");
        let tags = row
            .get(1)?
            .and_then(|cell| cell.into_list())
            .map(|list| list.len())
            .unwrap_or(0);
        println!("id={id} tag_count={tags}");
    }

    Ok(())
}
