#[test]
fn build_test() {
    let t = trybuild::TestCases::new();
    t.pass("tests/success/*.rs");
}
