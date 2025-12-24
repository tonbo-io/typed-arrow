//! Benchmark: typed-arrow vs serde_arrow vs typed-arrow-dyn vs arrow-rs raw
//!
//! This benchmark demonstrates the performance characteristics of compile-time vs runtime schema:
//! - typed-arrow: compile-time schema, monomorphized code
//! - serde_arrow: serde-based runtime serialization
//! - typed-arrow-dyn: runtime schema, dynamic dispatch via trait objects
//! - arrow-rs raw: manual builder construction (baseline)
//!
//! Benchmark groups:
//! - primitives_only: write benchmark with pure primitives (isolates dispatch overhead)
//! - with_strings: write benchmark with strings (real-world scenario)
//! - read_primitives: read benchmark iterating over rows
//! - read_with_strings: read benchmark with string access

use std::sync::Arc;

use arrow_array::{
    Array, RecordBatch,
    builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder},
    cast::AsArray,
    types::{Float64Type, Int32Type, Int64Type},
};
use arrow_schema::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use typed_arrow::prelude::*;
use typed_arrow_dyn::{DynBuilders, DynCell, DynRow, DynSchema};

// ============================================================================
// Primitives-only records (isolates dispatch overhead)
// ============================================================================

#[derive(Record, Clone, Copy, Serialize, Deserialize)]
struct Primitive {
    a: i64,
    b: f64,
    c: i32,
    d: bool,
}

fn generate_primitives(n: usize) -> Vec<Primitive> {
    (0..n)
        .map(|i| Primitive {
            a: i as i64,
            b: i as f64 * 1.5,
            c: (i % 1000) as i32,
            d: i % 2 == 0,
        })
        .collect()
}

fn primitives_to_dyn_rows(records: &[Primitive]) -> Vec<DynRow> {
    records
        .iter()
        .map(|r| {
            DynRow(vec![
                Some(DynCell::I64(r.a)),
                Some(DynCell::F64(r.b)),
                Some(DynCell::I32(r.c)),
                Some(DynCell::Bool(r.d)),
            ])
        })
        .collect()
}

fn primitive_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Float64, false),
        Field::new("c", DataType::Int32, false),
        Field::new("d", DataType::Boolean, false),
    ]))
}

// ============================================================================
// Records with strings (real-world scenario)
// ============================================================================

#[derive(Record, Clone, Serialize, Deserialize)]
struct WithStrings {
    id: i64,
    value: f64,
    active: bool,
    name: Option<String>,
}

fn generate_with_strings(n: usize) -> Vec<WithStrings> {
    (0..n)
        .map(|i| WithStrings {
            id: i as i64,
            value: i as f64 * 1.5,
            active: i % 2 == 0,
            name: if i % 3 == 0 {
                Some(format!("name_{}", i))
            } else {
                None
            },
        })
        .collect()
}

fn with_strings_to_dyn_rows(records: &[WithStrings]) -> Vec<DynRow> {
    records
        .iter()
        .map(|r| {
            DynRow(vec![
                Some(DynCell::I64(r.id)),
                Some(DynCell::F64(r.value)),
                Some(DynCell::Bool(r.active)),
                r.name.as_ref().map(|s| DynCell::Str(s.clone())),
            ])
        })
        .collect()
}

fn with_strings_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

// ============================================================================
// Benchmark: Primitives only (dispatch overhead)
// ============================================================================

fn bench_primitives(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitives_only");

    for size in [100, 1_000, 10_000] {
        let records = generate_primitives(size);
        let schema = primitive_schema();

        group.throughput(Throughput::Elements(size as u64));

        // typed-arrow: Copy semantics, no allocation
        group.bench_with_input(BenchmarkId::new("typed", size), &records, |b, records| {
            b.iter(|| {
                let mut builders = Primitive::new_builders(records.len());
                for &record in records {
                    builders.append_row(record);
                }
                black_box(builders.finish().into_record_batch())
            })
        });

        // typed-arrow-dyn: dynamic dispatch
        group.bench_with_input(
            BenchmarkId::new("dynamic", size),
            &(&records, &schema),
            |b, (records, schema)| {
                b.iter_batched(
                    || primitives_to_dyn_rows(records),
                    |rows| {
                        let mut builders = DynBuilders::new(Arc::clone(schema), rows.len());
                        for row in rows {
                            builders.append_option_row(Some(row)).unwrap();
                        }
                        black_box(builders.finish_into_batch())
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );

        // arrow-rs raw: baseline
        group.bench_with_input(
            BenchmarkId::new("arrow_raw", size),
            &(&records, &schema),
            |b, (records, schema)| {
                b.iter(|| {
                    let mut a = Int64Builder::with_capacity(records.len());
                    let mut b_builder = Float64Builder::with_capacity(records.len());
                    let mut c = arrow_array::builder::Int32Builder::with_capacity(records.len());
                    let mut d = BooleanBuilder::with_capacity(records.len());

                    for record in *records {
                        a.append_value(record.a);
                        b_builder.append_value(record.b);
                        c.append_value(record.c);
                        d.append_value(record.d);
                    }

                    black_box(
                        RecordBatch::try_new(
                            Arc::clone(schema),
                            vec![
                                Arc::new(a.finish()),
                                Arc::new(b_builder.finish()),
                                Arc::new(c.finish()),
                                Arc::new(d.finish()),
                            ],
                        )
                        .unwrap(),
                    )
                })
            },
        );

        // serde_arrow: serde-based serialization
        use serde_arrow::schema::{SchemaLike, TracingOptions};
        let serde_fields =
            Vec::<arrow_schema::FieldRef>::from_type::<Primitive>(TracingOptions::default())
                .unwrap();
        group.bench_with_input(
            BenchmarkId::new("serde_arrow", size),
            &(&records, &serde_fields),
            |b, (records, fields)| {
                b.iter(|| black_box(serde_arrow::to_record_batch(fields, *records).unwrap()))
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark: With strings (real-world scenario)
// ============================================================================

fn bench_with_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("with_strings");

    for size in [100, 1_000, 10_000] {
        let records = generate_with_strings(size);
        let schema = with_strings_schema();

        group.throughput(Throughput::Elements(size as u64));

        // typed-arrow: using append_row_ref to avoid clone
        group.bench_with_input(BenchmarkId::new("typed", size), &records, |b, records| {
            b.iter(|| {
                let mut builders = WithStrings::new_builders(records.len());
                for record in records {
                    builders.append_row_ref(record);
                }
                black_box(builders.finish().into_record_batch())
            })
        });

        // typed-arrow-dyn: dynamic dispatch + string handling
        group.bench_with_input(
            BenchmarkId::new("dynamic", size),
            &(&records, &schema),
            |b, (records, schema)| {
                b.iter_batched(
                    || with_strings_to_dyn_rows(records),
                    |rows| {
                        let mut builders = DynBuilders::new(Arc::clone(schema), rows.len());
                        for row in rows {
                            builders.append_option_row(Some(row)).unwrap();
                        }
                        black_box(builders.finish_into_batch())
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );

        // arrow-rs raw: baseline (references only)
        group.bench_with_input(
            BenchmarkId::new("arrow_raw", size),
            &(&records, &schema),
            |b, (records, schema)| {
                b.iter(|| {
                    let mut id = Int64Builder::with_capacity(records.len());
                    let mut value = Float64Builder::with_capacity(records.len());
                    let mut active = BooleanBuilder::with_capacity(records.len());
                    let mut name = StringBuilder::with_capacity(records.len(), records.len() * 8);

                    for record in *records {
                        id.append_value(record.id);
                        value.append_value(record.value);
                        active.append_value(record.active);
                        match &record.name {
                            Some(s) => name.append_value(s),
                            None => name.append_null(),
                        }
                    }

                    black_box(
                        RecordBatch::try_new(
                            Arc::clone(schema),
                            vec![
                                Arc::new(id.finish()),
                                Arc::new(value.finish()),
                                Arc::new(active.finish()),
                                Arc::new(name.finish()),
                            ],
                        )
                        .unwrap(),
                    )
                })
            },
        );

        // serde_arrow: serde-based serialization
        use serde_arrow::schema::{SchemaLike, TracingOptions};
        let serde_fields =
            Vec::<arrow_schema::FieldRef>::from_type::<WithStrings>(TracingOptions::default())
                .unwrap();
        group.bench_with_input(
            BenchmarkId::new("serde_arrow", size),
            &(&records, &serde_fields),
            |b, (records, fields)| {
                b.iter(|| black_box(serde_arrow::to_record_batch(fields, *records).unwrap()))
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark: Read primitives (iterate over rows)
// ============================================================================

fn bench_read_primitives(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_primitives");

    for size in [100, 1_000, 10_000] {
        // Prepare batch once
        let records = generate_primitives(size);
        let schema = primitive_schema();
        let mut builders = <Primitive as BuildRows>::new_builders(records.len());
        for &record in &records {
            builders.append_row(record);
        }
        let batch = builders.finish().into_record_batch();

        group.throughput(Throughput::Elements(size as u64));

        // typed-arrow: zero-copy views (row iteration)
        group.bench_with_input(BenchmarkId::new("typed_row", size), &batch, |b, batch| {
            b.iter(|| {
                let mut sum: i64 = 0;
                let views = batch.iter_views::<Primitive>().unwrap();
                for view in views.try_flatten().unwrap() {
                    sum = sum.wrapping_add(view.a);
                    sum = sum.wrapping_add(view.b as i64);
                    sum = sum.wrapping_add(view.c as i64);
                    sum = sum.wrapping_add(view.d as i64);
                }
                black_box(sum)
            })
        });

        // typed-arrow: typed arrays (columnar access)
        group.bench_with_input(BenchmarkId::new("typed_col", size), &batch, |b, batch| {
            use typed_arrow::schema::FromRecordBatch;
            b.iter(|| {
                let mut sum: i64 = 0;
                let views = Primitive::from_record_batch(batch).unwrap();
                let n = views.len();
                for i in 0..n {
                    sum = sum.wrapping_add(views.a.value(i));
                    sum = sum.wrapping_add(views.b.value(i) as i64);
                    sum = sum.wrapping_add(views.c.value(i) as i64);
                    sum = sum.wrapping_add(views.d.value(i) as i64);
                }
                black_box(sum)
            })
        });

        // typed-arrow-dyn: dynamic views
        let dyn_schema = DynSchema::from_ref(Arc::clone(&schema));
        group.bench_with_input(
            BenchmarkId::new("dynamic", size),
            &(&batch, &dyn_schema),
            |b, (batch, dyn_schema)| {
                b.iter(|| {
                    let mut sum: i64 = 0;
                    for row in dyn_schema.iter_views(batch).unwrap() {
                        let row = row.unwrap();
                        if let Some(cell) = row.get(0).unwrap() {
                            sum = sum.wrapping_add(cell.into_i64().unwrap_or(0));
                        }
                        if let Some(cell) = row.get(1).unwrap() {
                            sum = sum.wrapping_add(cell.into_f64().unwrap_or(0.0) as i64);
                        }
                        if let Some(cell) = row.get(2).unwrap() {
                            sum = sum.wrapping_add(cell.into_i32().unwrap_or(0) as i64);
                        }
                        if let Some(cell) = row.get(3).unwrap() {
                            sum = sum.wrapping_add(cell.into_bool().unwrap_or(false) as i64);
                        }
                    }
                    black_box(sum)
                })
            },
        );

        // arrow-rs raw: direct column access
        group.bench_with_input(BenchmarkId::new("arrow_raw", size), &batch, |b, batch| {
            b.iter(|| {
                let mut sum: i64 = 0;
                let col_a = batch.column(0).as_primitive::<Int64Type>();
                let col_b = batch.column(1).as_primitive::<Float64Type>();
                let col_c = batch.column(2).as_primitive::<Int32Type>();
                let col_d = batch.column(3).as_boolean();

                for i in 0..batch.num_rows() {
                    sum = sum.wrapping_add(col_a.value(i));
                    sum = sum.wrapping_add(col_b.value(i) as i64);
                    sum = sum.wrapping_add(col_c.value(i) as i64);
                    sum = sum.wrapping_add(col_d.value(i) as i64);
                }
                black_box(sum)
            })
        });

        // serde_arrow: deserialize to Vec<T>
        use serde_arrow::schema::{SchemaLike, TracingOptions};
        let serde_fields =
            Vec::<arrow_schema::FieldRef>::from_type::<Primitive>(TracingOptions::default())
                .unwrap();
        // Prepare batch for serde_arrow (needs matching schema)
        let serde_batch = serde_arrow::to_record_batch(&serde_fields, &records).unwrap();
        group.bench_with_input(
            BenchmarkId::new("serde_arrow", size),
            &serde_batch,
            |b, batch| {
                b.iter(|| {
                    let decoded: Vec<Primitive> = serde_arrow::from_record_batch(batch).unwrap();
                    let mut sum: i64 = 0;
                    for record in &decoded {
                        sum = sum.wrapping_add(record.a);
                        sum = sum.wrapping_add(record.b as i64);
                        sum = sum.wrapping_add(record.c as i64);
                        sum = sum.wrapping_add(record.d as i64);
                    }
                    black_box(sum)
                })
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark: Read with strings
// ============================================================================

fn bench_read_with_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_with_strings");

    for size in [100, 1_000, 10_000] {
        // Prepare batch once
        let records = generate_with_strings(size);
        let schema = with_strings_schema();
        let mut builders = <WithStrings as BuildRows>::new_builders(records.len());
        for record in &records {
            builders.append_row(record.clone());
        }
        let batch = builders.finish().into_record_batch();

        group.throughput(Throughput::Elements(size as u64));

        // typed-arrow: zero-copy views (row iteration)
        group.bench_with_input(BenchmarkId::new("typed_row", size), &batch, |b, batch| {
            b.iter(|| {
                let mut sum: i64 = 0;
                let mut name_len: usize = 0;
                let views = batch.iter_views::<WithStrings>().unwrap();
                for view in views.try_flatten().unwrap() {
                    sum = sum.wrapping_add(view.id);
                    sum = sum.wrapping_add(view.value as i64);
                    sum = sum.wrapping_add(view.active as i64);
                    if let Some(name) = view.name {
                        name_len += name.len();
                    }
                }
                black_box((sum, name_len))
            })
        });

        // typed-arrow: typed arrays (columnar access)
        group.bench_with_input(BenchmarkId::new("typed_col", size), &batch, |b, batch| {
            use typed_arrow::schema::FromRecordBatch;
            b.iter(|| {
                let mut sum: i64 = 0;
                let mut name_len: usize = 0;
                let views = WithStrings::from_record_batch(batch).unwrap();
                let n = views.len();
                for i in 0..n {
                    sum = sum.wrapping_add(views.id.value(i));
                    sum = sum.wrapping_add(views.value.value(i) as i64);
                    sum = sum.wrapping_add(views.active.value(i) as i64);
                    if !views.name.is_null(i) {
                        name_len += views.name.value(i).len();
                    }
                }
                black_box((sum, name_len))
            })
        });

        // typed-arrow-dyn: dynamic views
        let dyn_schema = DynSchema::from_ref(Arc::clone(&schema));
        group.bench_with_input(
            BenchmarkId::new("dynamic", size),
            &(&batch, &dyn_schema),
            |b, (batch, dyn_schema)| {
                b.iter(|| {
                    let mut sum: i64 = 0;
                    let mut name_len: usize = 0;
                    for row in dyn_schema.iter_views(batch).unwrap() {
                        let row = row.unwrap();
                        if let Some(cell) = row.get(0).unwrap() {
                            sum = sum.wrapping_add(cell.into_i64().unwrap_or(0));
                        }
                        if let Some(cell) = row.get(1).unwrap() {
                            sum = sum.wrapping_add(cell.into_f64().unwrap_or(0.0) as i64);
                        }
                        if let Some(cell) = row.get(2).unwrap() {
                            sum = sum.wrapping_add(cell.into_bool().unwrap_or(false) as i64);
                        }
                        if let Some(cell) = row.get(3).unwrap() {
                            if let Some(s) = cell.into_str() {
                                name_len += s.len();
                            }
                        }
                    }
                    black_box((sum, name_len))
                })
            },
        );

        // arrow-rs raw: direct column access
        group.bench_with_input(BenchmarkId::new("arrow_raw", size), &batch, |b, batch| {
            b.iter(|| {
                let mut sum: i64 = 0;
                let mut name_len: usize = 0;
                let col_id = batch.column(0).as_primitive::<Int64Type>();
                let col_value = batch.column(1).as_primitive::<Float64Type>();
                let col_active = batch.column(2).as_boolean();
                let col_name = batch.column(3).as_string::<i32>();

                for i in 0..batch.num_rows() {
                    sum = sum.wrapping_add(col_id.value(i));
                    sum = sum.wrapping_add(col_value.value(i) as i64);
                    sum = sum.wrapping_add(col_active.value(i) as i64);
                    if !col_name.is_null(i) {
                        name_len += col_name.value(i).len();
                    }
                }
                black_box((sum, name_len))
            })
        });

        // serde_arrow: deserialize to Vec<T>
        use serde_arrow::schema::{SchemaLike, TracingOptions};
        let serde_fields =
            Vec::<arrow_schema::FieldRef>::from_type::<WithStrings>(TracingOptions::default())
                .unwrap();
        // Prepare batch for serde_arrow (needs matching schema)
        let serde_batch = serde_arrow::to_record_batch(&serde_fields, &records).unwrap();
        group.bench_with_input(
            BenchmarkId::new("serde_arrow", size),
            &serde_batch,
            |b, batch| {
                b.iter(|| {
                    let decoded: Vec<WithStrings> = serde_arrow::from_record_batch(batch).unwrap();
                    let mut sum: i64 = 0;
                    let mut name_len: usize = 0;
                    for record in &decoded {
                        sum = sum.wrapping_add(record.id);
                        sum = sum.wrapping_add(record.value as i64);
                        sum = sum.wrapping_add(record.active as i64);
                        if let Some(ref name) = record.name {
                            name_len += name.len();
                        }
                    }
                    black_box((sum, name_len))
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_primitives,
    bench_with_strings,
    bench_read_primitives,
    bench_read_with_strings
);
criterion_main!(benches);
