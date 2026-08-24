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
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

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

/// The shape `rust-hello.msgpack` is required to have.
///
/// Decoding into a type that names the structure, rather than scanning the
/// bytes for key names: a flattened `room_project` key *contains* the byte
/// strings `room`, `project` and `task`, so a substring scan passes on the very
/// encoding it exists to reject. `deny_unknown_fields` rejects the flattened
/// pair, and [`RoomId`] rejects a positional room.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NestedRoomOnly {
    r#type: String,
    protocol: u32,
    room: RoomId,
    peer: String,
}

/// The drift [`NestedRoomOnly`] has to reject: `room` split into sibling keys.
#[derive(Serialize)]
struct FlattenedRoom {
    r#type: &'static str,
    protocol: u32,
    room_project: &'static str,
    room_task: &'static str,
    peer: &'static str,
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

    // The property the fixture exists for, asserted structurally rather than
    // left implicit in the bytes -- as the other two fixtures already do.
    //
    // Without this, the risk this fixture names is covered only by the
    // byte-equality comparison, which `UPDATE_FIXTURES=1` exists to overwrite.
    // A serializer change that flattened `room`, or emitted the combined
    // `<project>/<task>` spelling, would be blessed by the documented
    // regeneration command -- replacing the artifact meant to catch exactly
    // that drift. This assertion survives regeneration because it reads the
    // file back after it is written.
    let committed = fs::read(fixture_dir().join("rust-hello.msgpack")).expect("read the fixture");
    let hello: NestedRoomOnly = protocol::decode(&committed).expect(
        "the hello fixture must decode as a map carrying `room` as a nested map: a flattened \
         or combined room would fail here",
    );
    assert_eq!(hello.r#type, "hello");
    assert_eq!(hello.protocol, PROTOCOL_VERSION);
    assert_eq!(hello.peer, "macbook-reviewer");
    assert_eq!(
        (hello.room.project.as_str(), hello.room.task.as_str()),
        ("omp-relayd", "implement-tcp-relay-server"),
        "the two components must arrive separately, never as one combined string"
    );

    // And the drift a substring scan let through, so the hole cannot reopen.
    let flattened = protocol::encode(&FlattenedRoom {
        r#type: "hello",
        protocol: PROTOCOL_VERSION,
        room_project: "omp-relayd",
        room_task: "implement-tcp-relay-server",
        peer: "macbook-reviewer",
    })
    .expect("encodes");
    assert!(
        protocol::decode::<NestedRoomOnly>(&flattened).is_err(),
        "a flattened room must not satisfy this check; it is the drift the \
         fixture exists to catch"
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

#[test]
fn announce_fixture_carries_no_target_field() {
    let expected = ClientFrame::Announce {
        id: "ann-1".to_owned(),
        body: "the schema landed".to_owned(),
        reply_to: None,
    };

    check_fixture(
        "rust-announce.msgpack",
        &expected,
        "a room-wide address expressed as the *absence* of a target field, and an \
         absent optional omitted rather than encoded as nil",
    );

    // The property the fixture exists for. An implementation that expected a
    // target key here -- a reserved `all`, or an empty `to` -- would decode this
    // payload only by inventing a value the wire never carried.
    let committed =
        fs::read(fixture_dir().join("rust-announce.msgpack")).expect("read the fixture");
    for absent in ["to", "reply_to"] {
        assert!(
            !committed
                .windows(absent.len())
                .any(|window| window == absent.as_bytes()),
            "the announce fixture must contain no {absent} key: {committed:02x?}"
        );
    }
}

#[test]
fn notice_fixture_is_distinguished_from_a_message_by_its_type_alone() {
    let expected = ServerFrame::Notice {
        id: "ann-2".to_owned(),
        from: "macbook-reviewer".to_owned(),
        body: "and the migration with it".to_owned(),
        reply_to: Some("ann-1".to_owned()),
    };

    check_fixture(
        "rust-notice.msgpack",
        &expected,
        "a second delivery class carried by the discriminator rather than by a field \
         inside the frame, over a field set identical to `message`",
    );

    // The claim the class rests on: same fields, different `type`. A recipient
    // that told the two apart by anything else -- a marker field, a prefix in
    // the body -- would pass its own tests and fail against this.
    let committed = fs::read(fixture_dir().join("rust-notice.msgpack")).expect("read the fixture");
    let as_message = protocol::encode(&ServerFrame::Message {
        id: "ann-2".to_owned(),
        from: "macbook-reviewer".to_owned(),
        body: "and the migration with it".to_owned(),
        reply_to: Some("ann-1".to_owned()),
    })
    .expect("encodes");
    assert_eq!(
        committed.len() + 1,
        as_message.len(),
        "the same fields under `notice` must be exactly one byte shorter than under \
         `message`, which is the whole of the difference between the two classes and \
         the arithmetic the shared body budget rests on"
    );
}

#[test]
fn accepted_fixture_carries_two_integers_and_no_status() {
    check_fixture(
        "rust-accepted.msgpack",
        &ServerFrame::Accepted {
            id: "ann-1".to_owned(),
            delivered: 2,
            shed: 1,
        },
        "an aggregate outcome as two counts rather than as a `receipt` status, so a \
         mixed result needs no single value that would be a lie about it",
    );

    let committed =
        fs::read(fixture_dir().join("rust-accepted.msgpack")).expect("read the fixture");
    assert!(
        !committed
            .windows(b"status".len())
            .any(|window| window == b"status"),
        "the accepted fixture must carry no status field: {committed:02x?}"
    );
    for count in ["delivered", "shed"] {
        assert!(
            committed
                .windows(count.len())
                .any(|window| window == count.as_bytes()),
            "the accepted fixture must name both counts, and {count} is missing: \
             {committed:02x?}"
        );
    }
}

/// Checks a fixture produced by the *other* implementation.
///
/// Deliberately not [`check_fixture`]. That function also asserts that a fresh
/// encode reproduces the committed bytes, which is the right check for this
/// crate's own output and the wrong one here: `wire-protocol` requires the two
/// languages to agree on decoded *values*, not on bytes. Demanding byte
/// equality would couple this crate to `@msgpack/msgpack`'s encoder choices and
/// fail on a difference that breaks nothing.
///
/// A missing file is a hard failure rather than a regeneration, because this
/// side cannot produce a TypeScript fixture: run `bun test` in `extension/`.
fn check_foreign_fixture<T>(name: &str, expected: &T, risk: &str)
where
    T: DeserializeOwned + PartialEq + Debug,
{
    let path = fixture_dir().join(name);
    let committed = fs::read(&path).unwrap_or_else(|error| {
        panic!(
            "{name} is missing ({error}). It is produced by the TypeScript side; \
             run `bun test` in `extension/` and commit the result."
        )
    });

    let decoded: T = protocol::decode(&committed).unwrap_or_else(|error| {
        panic!(
            "{name} ({} bytes) did not decode as a protocol frame: {error}. \
             Risk no longer covered: {risk}",
            committed.len()
        )
    });
    assert_eq!(
        &decoded, expected,
        "{name} decoded to a different value than the contract documents, so {risk} \
         is no longer covered"
    );

    println!(
        "{name}: {} bytes from the TypeScript implementation, covering {risk}",
        committed.len()
    );
}

#[test]
fn typescript_hello_fixture_carries_a_nested_room_map() {
    check_foreign_fixture(
        "ts-hello.msgpack",
        &ClientFrame::Hello {
            protocol: PROTOCOL_VERSION,
            room: RoomId::new("omp-relayd", "implement-relay-client-library"),
            peer: "macbook-reviewer".to_owned(),
        },
        "a nested room map, decoded through RoomId's map-only deserializer",
    );

    // [`RoomId`] rejects a positional room, and `deny_unknown_fields` on
    // [`NestedRoomOnly`] rejects a flattened one, so decoding through that type
    // proves the shape rather than only the values -- exactly as the
    // Rust-produced hello fixture is checked.
    let committed = fs::read(fixture_dir().join("ts-hello.msgpack")).expect("read the fixture");
    let hello: NestedRoomOnly = protocol::decode(&committed).expect(
        "the TypeScript hello fixture must decode as a map carrying `room` as a nested map",
    );
    assert_eq!(hello.r#type, "hello");
    assert_eq!(hello.protocol, PROTOCOL_VERSION);
    assert_eq!(hello.peer, "macbook-reviewer");
    assert_eq!(
        (hello.room.project.as_str(), hello.room.task.as_str()),
        ("omp-relayd", "implement-relay-client-library")
    );
}

#[test]
fn typescript_send_fixture_yields_reply_to_as_none() {
    check_foreign_fixture(
        "ts-send.msgpack",
        &ClientFrame::Send {
            id: "msg-1".to_owned(),
            to: "windows-main".to_owned(),
            body: "review the diff".to_owned(),
            reply_to: None,
        },
        "an absent optional field omitted by the TypeScript encoder, not sent as nil",
    );

    // `reply_to: None` above would also be satisfied by an explicit nil, which
    // the decoder accepts on purpose. The key's absence is the property this
    // fixture exists to pin, so it is asserted over the bytes.
    let committed = fs::read(fixture_dir().join("ts-send.msgpack")).expect("read the fixture");
    assert!(
        !committed
            .windows(b"reply_to".len())
            .any(|window| window == b"reply_to"),
        "the TypeScript send fixture must contain no reply_to key: {committed:02x?}"
    );
}

#[test]
fn typescript_receipt_fixture_decodes_its_status_from_a_string() {
    check_foreign_fixture(
        "ts-receipt.msgpack",
        &ServerFrame::Receipt {
            id: "msg-1".to_owned(),
            to: "windows-main".to_owned(),
            status: ReceiptStatus::RecipientBackpressure,
        },
        "an enum the TypeScript side spells as a snake_case string",
    );
}

#[test]
fn typescript_announce_fixture_decodes_without_a_target() {
    check_foreign_fixture(
        "ts-announce.msgpack",
        &ClientFrame::Announce {
            id: "ann-1".to_owned(),
            body: "the schema landed".to_owned(),
            reply_to: None,
        },
        "a room-wide address the TypeScript encoder expressed as the absence of a target \
         field, with no reserved value for a peer to capture",
    );

    // Decoding into `Announce` already proves this side reads it as an
    // announcement, but not that the other side wrote no target: `ClientFrame`
    // ignores unknown fields on purpose, so a `to` key would decode silently.
    let committed = fs::read(fixture_dir().join("ts-announce.msgpack")).expect("read the fixture");
    assert!(
        !committed.windows(b"to".len()).any(|window| window == b"to"),
        "the TypeScript announce fixture must contain no target key: {committed:02x?}"
    );
}

#[test]
fn typescript_notice_fixture_decodes_as_a_notice_rather_than_a_message() {
    let expected = ServerFrame::Notice {
        id: "ann-2".to_owned(),
        from: "macbook-reviewer".to_owned(),
        body: "and the migration with it".to_owned(),
        reply_to: Some("ann-1".to_owned()),
    };

    check_foreign_fixture(
        "ts-notice.msgpack",
        &expected,
        "a second delivery class the TypeScript side spelled in the discriminator, over a \
         field set identical to `message`",
    );

    // `check_foreign_fixture` compares decoded values, so a fixture that had
    // decoded as a `message` would have failed above. This measures the class
    // relation instead: identical fields, a `type` one byte shorter. Both
    // implementations must agree on that number, because the shared body budget
    // is computed from it.
    let committed = fs::read(fixture_dir().join("ts-notice.msgpack")).expect("read the fixture");
    let as_message = protocol::encode(&ServerFrame::Message {
        id: "ann-2".to_owned(),
        from: "macbook-reviewer".to_owned(),
        body: "and the migration with it".to_owned(),
        reply_to: Some("ann-1".to_owned()),
    })
    .expect("encodes");
    assert_eq!(
        committed.len() + 1,
        as_message.len(),
        "the TypeScript notice is {} bytes and the same fields under `message` are {}; the \
         difference must be exactly the one byte of the `type` value, or the two encoders \
         disagree about the arithmetic the shared body budget rests on",
        committed.len(),
        as_message.len()
    );
}

#[test]
fn typescript_accepted_fixture_decodes_its_counts_as_integers() {
    check_foreign_fixture(
        "ts-accepted.msgpack",
        &ServerFrame::Accepted {
            id: "ann-1".to_owned(),
            delivered: 2,
            shed: 1,
        },
        "two integer counts the TypeScript side wrote as numbers rather than as a status \
         string or a single total",
    );

    let committed = fs::read(fixture_dir().join("ts-accepted.msgpack")).expect("read the fixture");
    assert!(
        !committed
            .windows(b"status".len())
            .any(|window| window == b"status"),
        "the TypeScript accepted fixture must carry no status field: {committed:02x?}"
    );
}
