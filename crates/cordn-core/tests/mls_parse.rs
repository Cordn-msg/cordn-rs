//! Cross-validation of the minimal MLS parser against captured `ts-mls`
//! fixtures. The wire bytes are produced by `references/cordn/scripts/
//! gen_key_packages.test.ts` using the same `ts-mls` the production TS
//! coordinator encodes with; `expected_identity_hex` and
//! `expected_is_last_resort` are `ts-mls`'s own answers. If the parser agrees
//! here, it agrees on the real wire format.

use cordn_core::parse_key_package;

#[derive(serde::Deserialize)]
struct FixtureFile {
    fixtures: Vec<Fixture>,
}

#[derive(serde::Deserialize)]
struct Fixture {
    name: String,
    bytes_hex: String,
    expected_identity_hex: String,
    expected_is_last_resort: bool,
}

fn load() -> FixtureFile {
    let raw = include_str!("fixtures/key_packages.json");
    serde_json::from_str(raw).expect("fixture json must parse")
}

#[test]
fn parser_matches_tsmls_on_all_fixtures() {
    for f in load().fixtures {
        let bytes = hex::decode(&f.bytes_hex).expect("hex");
        let parsed = parse_key_package(&bytes).unwrap_or_else(|e| panic!("{}: {e:?}", f.name));
        assert_eq!(
            parsed.credential_identity, f.expected_identity_hex,
            "identity mismatch for {}",
            f.name
        );
        assert_eq!(
            parsed.is_last_resort, f.expected_is_last_resort,
            "is_last_resort mismatch for {}",
            f.name
        );
        // cipher suite 1 = MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519
        assert_eq!(parsed.cipher_suite, 1, "{} cipher suite", f.name);
    }
}

#[test]
fn last_resort_fixture_is_detected() {
    let file = load();
    let f = file
        .fixtures
        .iter()
        .find(|f| f.name == "last_resort_bob")
        .unwrap();
    let bytes = hex::decode(&f.bytes_hex).unwrap();
    assert!(parse_key_package(&bytes).unwrap().is_last_resort);
}

#[test]
fn bogus_appdata_extension_is_not_last_resort() {
    // Type 0x0006 extension present, but with a different component id — must
    // NOT trip the last-resort detector.
    let file = load();
    let f = file
        .fixtures
        .iter()
        .find(|f| f.name == "bogus_appdata_carol")
        .unwrap();
    let bytes = hex::decode(&f.bytes_hex).unwrap();
    assert!(!parse_key_package(&bytes).unwrap().is_last_resort);
}

#[test]
fn rejects_truncated_key_package() {
    let file = load();
    let f = file
        .fixtures
        .iter()
        .find(|f| f.name == "regular_alice")
        .unwrap();
    let bytes = hex::decode(&f.bytes_hex).unwrap();
    // Cut off the last 10 bytes (inside the signature).
    let truncated = &bytes[..bytes.len() - 10];
    assert!(parse_key_package(truncated).is_err());
}

#[test]
fn rejects_garbage() {
    assert!(parse_key_package(&[0x00; 5]).is_err());
    assert!(parse_key_package(&[]).is_err());
}
