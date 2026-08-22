//! Cross-language fixtures: bytes this implementation produces, committed so
//! the TypeScript side can prove it decodes them without running this server.
//!
//! Each fixture is checked two ways. The committed bytes must still decode to
//! the value they document, which is the semantic contract, and a fresh encode
//! must still equal the committed bytes, which turns silent encoding drift into
//! a test failure. Regenerate deliberately with `UPDATE_FIXTURES=1 cargo test`.

use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::{env, fs};

use omp_relayd::protocol::{
    self, ClientFrame, PROTOCOL_VERSION, ReceiptStatus, RoomId, ServerFrame,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../test-fixtures/protocol-v1")
}

fn updating() -> bool {
    env::var_os("UPDATE_FIXTURES").is_some()
}

/// Checks one fixture, or writes it when regenerating.
///
/// `risk` names the interoperability hazard the fixture exists to catch, so a
/// failure says what breaks rather than only that bytes differ.
fn check_fixture<T>(name: &str, expected: &T, risk: &str)
where
    T: Serialize + DeserializeOwned + PartialEq + Debug,
{
    let dir = fixture_dir();
    let path = dir.join(name);
    let fresh = protocol::encode(expected).expect("the fixture value encodes");

    if updating() || !path.exists() {
        fs::create_dir_all(&dir).expect("create the fixture directory");
        fs::write(&path, &fresh).expect("write the fixture");
        assert!(
            updating(),
            "{name} was missing and has been generated ({} bytes); commit it and re-run",
            fresh.len()
        );
        return;
    }

    let committed = fs::read(&path).expect("read the committed fixture");

    let decoded: T = protocol::decode(&committed).expect("the committed fixture decodes");
    assert_eq!(
        &decoded, expected,
        "{name} no longer decodes to the value it documents, so {risk} is no longer covered"
    );

    assert_eq!(
        committed,
        fresh.as_ref(),
        "{name} drifted: {} committed bytes against {} freshly encoded. The value still \
         decodes correctly, so if the new encoding is intended, regenerate with \
         UPDATE_FIXTURES=1 and commit the result. Risk covered: {risk}",
        committed.len(),
        fresh.len()
    );

    println!("{name}: {} bytes, covering {risk}", committed.len());
}

#[test]
fn hello_fixture_carries_a_nested_room_map() {
    check_fixture(
        "rust-hello.msgpack",
        &ClientFrame::Hello {
            protocol: PROTOCOL_VERSION,
            room: RoomId::new("omp-relayd", "implement-tcp-relay-server"),
            peer: "macbook-reviewer".to_owned(),
        },
        "a nested map (room.project and room.task) rather than a flattened or \
         combined room string",
    );
}

#[test]
fn send_fixture_omits_its_absent_optional_field() {
    let expected = ClientFrame::Send {
        id: "msg-1".to_owned(),
        to: "windows-main".to_owned(),
        body: "review the diff".to_owned(),
        reply_to: None,
    };

    check_fixture(
        "rust-send.msgpack",
        &expected,
        "an absent optional field omitted entirely rather than encoded as nil",
    );

    // The property the fixture exists for, asserted directly rather than left
    // implicit in the bytes.
    let committed = fs::read(fixture_dir().join("rust-send.msgpack")).expect("read the fixture");
    assert!(
        !committed
            .windows(b"reply_to".len())
            .any(|window| window == b"reply_to"),
        "the send fixture must contain no reply_to key: {committed:02x?}"
    );
}

#[test]
fn receipt_fixture_encodes_its_status_as_a_string() {
    check_fixture(
        "rust-receipt.msgpack",
        &ServerFrame::Receipt {
            id: "msg-1".to_owned(),
            to: "windows-main".to_owned(),
            status: ReceiptStatus::RecipientBackpressure,
        },
        "an enum encoded as the snake_case string `recipient_backpressure` \
         rather than as an integer",
    );

    let committed = fs::read(fixture_dir().join("rust-receipt.msgpack")).expect("read the fixture");
    assert!(
        committed
            .windows(b"recipient_backpressure".len())
            .any(|window| window == b"recipient_backpressure"),
        "the receipt fixture must spell its status out: {committed:02x?}"
    );
}
