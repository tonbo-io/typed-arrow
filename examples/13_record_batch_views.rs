//! Showcase: Zero-copy views over RecordBatch rows.
//!
//! The derive macro automatically generates `{Name}View<'a>` structs and
//! `{Name}Views<'a>` iterators that provide borrowed access to RecordBatch
//! data without copying.

use arrow_array::RecordBatch;
use typed_arrow::prelude::*;

#[derive(Record)]
struct Product {
    id: i64,
    name: String,
    price: f64,
    in_stock: Option<bool>,
}

#[derive(Record)]
struct Coordinates {
    lat: f64,
    lon: f64,
}

#[derive(Record)]
struct Location {
    city: String,
    coords: Option<Coordinates>,
}

fn main() -> Result<(), typed_arrow::schema::SchemaError> {
    println!("=== Example 1: Simple flat record views ===\n");
    flat_record_example()?;

    println!("\n=== Example 2: Nested struct views ===\n");
    nested_record_example()?;

    println!("\n=== Example 3: Converting views to owned records ===\n");
    view_conversion_example()?;

    Ok(())
}

fn flat_record_example() -> Result<(), typed_arrow::schema::SchemaError> {
    // Build rows
    let rows = vec![
        Product {
            id: 1,
            name: "Widget".into(),
            price: 9.99,
            in_stock: Some(true),
        },
        Product {
            id: 2,
            name: "Gadget".into(),
            price: 19.99,
            in_stock: None,
        },
        Product {
            id: 3,
            name: "Doohickey".into(),
            price: 14.50,
            in_stock: Some(false),
        },
    ];

    // Build RecordBatch
    let mut b = <Product as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    println!("RecordBatch has {} rows", batch.num_rows());

    // Create zero-copy views over the batch using the convenient API
    let views = batch.iter_views::<Product>()?;

    println!("Products in stock:");
    for view in views.try_flatten()? {
        // All fields are borrowed references - no copying!
        // Strings are &str, primitives are copied (they're small)
        match view.in_stock {
            Some(true) => println!("  #{}: {} - ${:.2}", view.id, view.name, view.price),
            Some(false) => {}
            None => {}
        }
    }

    Ok(())
}

fn nested_record_example() -> Result<(), typed_arrow::schema::SchemaError> {
    let locations = vec![
        Location {
            city: "New York".into(),
            coords: Some(Coordinates {
                lat: 40.7128,
                lon: -74.0060,
            }),
        },
        Location {
            city: "Unknown City".into(),
            coords: None,
        },
        Location {
            city: "San Francisco".into(),
            coords: Some(Coordinates {
                lat: 37.7749,
                lon: -122.4194,
            }),
        },
    ];

    let mut b = <Location as BuildRows>::new_builders(locations.len());
    b.append_rows(locations);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    println!("RecordBatch has {} rows", batch.num_rows());

    // Iterate with zero-copy views using the convenient API
    let views = batch.iter_views::<Location>()?;

    println!("Locations with coordinates:");
    for view in views.try_flatten()? {
        print!("  {}: ", view.city);
        match view.coords {
            Some(coords) => println!("({:.4}, {:.4})", coords.lat, coords.lon),
            None => println!("(no coordinates)"),
        }
    }

    Ok(())
}

fn view_conversion_example() -> Result<(), typed_arrow::schema::SchemaError> {
    let products = vec![
        Product {
            id: 100,
            name: "Laptop".into(),
            price: 999.99,
            in_stock: Some(true),
        },
        Product {
            id: 101,
            name: "Mouse".into(),
            price: 29.99,
            in_stock: Some(true),
        },
    ];

    let mut b = <Product as BuildRows>::new_builders(products.len());
    b.append_rows(products);
    let arrays = b.finish();
    let batch: RecordBatch = arrays.into_record_batch();

    println!("RecordBatch has {} rows", batch.num_rows());

    // Iterate over views and convert selected ones to owned records
    let views = batch.iter_views::<Product>()?;
    let mut owned_products: Vec<Product> = Vec::new();

    for view in views.try_flatten()? {
        // Views provide zero-copy access - strings are &str, not String
        println!("View: {} (price: ${:.2})", view.name, view.price);

        // Convert view to owned when you need to store it beyond the batch lifetime
        if view.price > 50.0 {
            let owned: Product = view.try_into()?;
            owned_products.push(owned);
        }
    }

    println!(
        "\nCollected {} expensive products as owned records",
        owned_products.len()
    );
    for product in &owned_products {
        println!("  Owned: {} - ${:.2}", product.name, product.price);
    }

    Ok(())
}
