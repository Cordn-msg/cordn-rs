//! Wire-format parity for `contracts.rs`: every struct must serialize to the
//! exact JSON shape the TS Zod schemas produce (field names, camelCase
//! `keyPackage`/`keyPackages`, omitted-when-None optionals). Key *order* is not
//! significant for JSON objects, so these compare the set of key names.

use cordn_core::contracts::*;
use std::collections::HashSet;

fn assert_keys(value: &serde_json::Value, expected: &[&str]) {
    let actual: HashSet<&str> = value
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    let want: HashSet<&str> = expected.iter().copied().collect();
    assert_eq!(actual, want, "key set mismatch");
}

#[test]
fn publish_key_package_output_field_names() {
    let out = PublishKeyPackageOutput {
        kp_ref: "kp-1".into(),
        last_resort: false,
        at: 123,
    };
    assert_keys(
        &serde_json::to_value(&out).unwrap(),
        &["kp_ref", "last_resort", "at"],
    );
}

#[test]
fn consume_output_uses_camel_case_key_package() {
    let out = ConsumeKeyPackageOutput { key_package: None };
    let v = serde_json::to_value(&out).unwrap();
    assert_keys(&v, &["keyPackage"]);
    assert!(v["keyPackage"].is_null());

    let out2 = ConsumeKeyPackageOutput {
        key_package: Some(ConsumedKeyPackage {
            pk: "pk".into(),
            kp_ref: "r".into(),
            last_resort: true,
            at: 1,
            event: NostrEvent {
                id: "id".into(),
                pubkey: "pk".into(),
                created_at: 1,
                kind: 1,
                tags: vec![],
                content: "c".into(),
                sig: "s".into(),
            },
        }),
    };
    let v2 = serde_json::to_value(&out2).unwrap();
    assert_keys(
        &v2["keyPackage"],
        &["pk", "kp_ref", "last_resort", "at", "event"],
    );
}

#[test]
fn list_output_uses_camel_case_key_packages() {
    let out = ListAvailableKeyPackagesOutput {
        key_packages: vec![],
    };
    assert_keys(&serde_json::to_value(&out).unwrap(), &["keyPackages"]);
}

#[test]
fn pending_welcome_omits_absent_after() {
    let w = PendingWelcome {
        kp_ref: "r".into(),
        welcome_64: "w".into(),
        at: 1,
        after: None,
    };
    assert_keys(
        &serde_json::to_value(&w).unwrap(),
        &["kp_ref", "welcome_64", "at"],
    );

    let w2 = PendingWelcome {
        after: Some(7),
        ..w
    };
    assert_keys(
        &serde_json::to_value(&w2).unwrap(),
        &["kp_ref", "welcome_64", "at", "after"],
    );
}

#[test]
fn group_message_serializes_exactly_to_wire_shape() {
    // Parity with the TS `groupMessageSchema = { cursor, gid, msg_64, at }`:
    // the encrypted-only refactor (cordn 0.5) dropped the `encrypted` field
    // from the wire, so the struct must serialize to exactly these four keys.
    let m = GroupMessage {
        cursor: 1,
        gid: "g".into(),
        msg_64: "m".into(),
        at: 1,
    };
    assert_keys(
        &serde_json::to_value(&m).unwrap(),
        &["cursor", "gid", "msg_64", "at"],
    );
}

#[test]
fn fetch_pending_welcomes_input_accepts_absent_consumed() {
    let s = r#"{"consumed":[{"kp_ref":"r","at":1}]}"#;
    let parsed: FetchPendingWelcomesInput = serde_json::from_str(s).unwrap();
    assert_eq!(parsed.consumed.as_ref().unwrap().len(), 1);

    let empty: FetchPendingWelcomesInput = serde_json::from_str("{}").unwrap();
    assert!(empty.consumed.is_none());
}

#[test]
fn subscribe_many_output_shape() {
    let out = SubscribeManyGroupMessagesOutput {
        subscribed: true,
        groups: vec!["a".into(), "b".into()],
    };
    assert_keys(
        &serde_json::to_value(&out).unwrap(),
        &["subscribed", "groups"],
    );
}

#[test]
fn join_request_with_group_shape() {
    let r = JoinRequestWithGroup {
        pk: "pk".into(),
        kp_ref: "r".into(),
        at: 1,
        gid: "g".into(),
    };
    assert_keys(
        &serde_json::to_value(&r).unwrap(),
        &["pk", "kp_ref", "at", "gid"],
    );
}

#[test]
fn store_welcome_input_round_trip() {
    let input = StoreWelcomeInput {
        target_pk: "pk".into(),
        kp_ref: "r".into(),
        welcome_64: "w".into(),
        after: Some(5),
    };
    let v: serde_json::Value = serde_json::to_value(&input).unwrap();
    assert_keys(&v, &["target_pk", "kp_ref", "welcome_64", "after"]);
    let back: StoreWelcomeInput = serde_json::from_value(v).unwrap();
    assert_eq!(back, input);
}

#[test]
fn method_name_constants_match_ts() {
    use cordn_core::contracts::methods::*;
    assert_eq!(PUBLISH_KEY_PACKAGE, "kp_publish");
    assert_eq!(CONSUME_KEY_PACKAGE, "kp_take");
    assert_eq!(REMOVE_KEY_PACKAGES, "kp_remove");
    assert_eq!(FETCH_PENDING_WELCOMES, "welcome_take");
    assert_eq!(STORE_WELCOME, "welcome_store");
    assert_eq!(STORE_JOIN_REQUEST, "join_request_store");
    assert_eq!(FETCH_MANY_PENDING_JOIN_REQUESTS, "join_request_take_many");
    assert_eq!(POST_GROUP_MESSAGE, "msg_post");
    assert_eq!(FETCH_MANY_GROUP_MESSAGES, "msg_fetch_many");
    assert_eq!(SUBSCRIBE_MANY_GROUP_MESSAGES, "msg_sub_many");
    assert_eq!(LIST_AVAILABLE_KEY_PACKAGES, "kp_list");
}
