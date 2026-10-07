#![cfg(feature = "arrow-59")]

use typed_arrow::{
    Dictionary, Union,
    arrow_array::{
        Array, DictionaryArray, StringArray, StructArray, UnionArray,
        builder::{ArrayBuilder, StructBuilder},
        types::Int32Type,
    },
    arrow_schema::Field,
    bridge::ArrowBinding,
};

#[derive(Union)]
enum Dense {
    Text(Dictionary<i32, String>),
    Number(i32),
}

#[derive(Union)]
#[union(mode = "sparse")]
enum Sparse {
    Text(Dictionary<i32, String>),
    Number(i32),
}

fn check<U: ArrowBinding>(text: impl Fn(&str) -> U, number: impl Fn() -> U)
where
    U::Builder: ArrayBuilder,
{
    let mut builder = StructBuilder::new(
        vec![Field::new("union", U::data_type(), false)],
        vec![Box::new(U::new_builder(0))],
    );
    for (round, words) in [["old", "shared"], ["shared", "new"]]
        .into_iter()
        .enumerate()
    {
        for value in [text(words[0]), number(), text(words[1])] {
            U::append_value(builder.field_builder::<U::Builder>(0).unwrap(), &value);
            builder.append(true);
        }
        let array = ArrayBuilder::finish_preserve_values(&mut builder);
        assert_eq!(builder.len(), 0);
        let array = array.as_any().downcast_ref::<StructArray>().unwrap();
        let union = array
            .column(0)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        assert_eq!(union.len(), 3);
        let dict = union
            .child(union.type_id(0))
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let expected = if round == 0 {
            vec!["old", "shared"]
        } else {
            vec!["old", "shared", "new"]
        };
        assert_eq!(values.iter().flatten().collect::<Vec<_>>(), expected);
        assert_eq!(dict.keys().value(union.value_offset(0)), round as i32);
        assert_eq!(dict.keys().value(union.value_offset(2)), (round + 1) as i32);
    }
    assert_eq!(ArrayBuilder::finish_preserve_values(&mut builder).len(), 0);
    // A normal finish clears dictionary values.
    assert_eq!(builder.finish().len(), 0);
    U::append_value(
        builder.field_builder::<U::Builder>(0).unwrap(),
        &text("fresh"),
    );
    builder.append(true);
    let array = builder.finish();
    let union = array
        .column(0)
        .as_any()
        .downcast_ref::<UnionArray>()
        .unwrap();
    let dict = union
        .child(union.type_id(0))
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .unwrap();
    assert_eq!(dict.values().len(), 1);
    assert_eq!(dict.keys().value(0), 0);
}

#[test]
fn dense_dictionary_preserved_inside_struct() {
    check::<Dense>(
        |s| Dense::Text(Dictionary::new(s.into())),
        || Dense::Number(7),
    );
}

#[test]
fn sparse_dictionary_preserved_inside_struct() {
    check::<Sparse>(
        |s| Sparse::Text(Dictionary::new(s.into())),
        || Sparse::Number(7),
    );
}
