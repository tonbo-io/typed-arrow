use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use typed_arrow::{
    Decimal128, Dictionary, LargeList, LargeUtf8, List, Map, Null, OrderedMap, Record,
};

#[derive(Serialize, Deserialize, Record, PartialEq, Debug)]
struct AllTypes {
    list: List<i32>,
    large_list: LargeList<String>,
    large_utf8: LargeUtf8,
    map: Map<String, i32>,
    ordered_map: OrderedMap<String, i32>,
    dict: Dictionary<i32, String>,
    decimal: Decimal128<10, 2>,
    null: Null,
}

#[test]
fn all_types_roundtrip() {
    let mut btree = BTreeMap::new();
    btree.insert("b".to_string(), 2i32);
    btree.insert("a".to_string(), 1i32);

    let original = AllTypes {
        list: vec![1, 2, 3].into(),
        large_list: vec!["hello".to_string(), "world".to_string()].into(),
        large_utf8: LargeUtf8::new("large string".to_string()),
        map: vec![("x".to_string(), 10i32), ("y".to_string(), 20i32)].into(),
        ordered_map: OrderedMap::new(btree),
        dict: Dictionary::new("cat".to_string()),
        decimal: Decimal128::new(123456),
        null: Null,
    };

    let json = serde_json::to_string(&original).expect("serialize");
    let back: AllTypes = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(back, original);
}
