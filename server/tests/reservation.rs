//! The reservation frames and the attachment field, over real sockets.
//!
//! Two subjects in one binary because they are one contract: a reference is only
//! ever attached to a message after a reservation was granted, so the tests that
//! prove a reference travels uninterpreted and the tests that prove a
//! reservation is answered exactly once share a fixture and a relay.

mod support;

use omp_relayd::blob::{self, MAX_PAYLOAD_BYTES, MAX_ROOM_BYTES, PAYLOAD_TIME_TO_LIVE};
use omp_relayd::protocol::{
    ClientFrame, ErrorCode, MAX_CORRELATION_BYTES, ReceiptStatus, ReserveStatus, ServerFrame,
};
use omp_relayd::relay::OUTBOUND_QUEUE_CAPACITY;
use support::{Client, QUIET, Relay, fill_pipeline, room};

/// A valid 43-character address, so a rejection is never about the shape.
fn digest_of(bytes: &[u8]) -> String {
    blob::digest(bytes)
}

fn reserve(request_id: &str, digest: &str, bytes: u64) -> ClientFrame {
    ClientFrame::Reserve {
        request_id: request_id.to_owned(),
        digest: digest.to_owned(),
        bytes,
    }
}

// 3.5: answered exactly once.
#[tokio::test]
async fn a_reservation_is_answered_exactly_once() {
    let relay = Relay::with_store("reserve-once").await;
    let room = room("reserve-once");
    let mut peer = Client::join(&relay, &room, "macbook").await;
    let digest = digest_of(b"a diff worth reviewing");

    peer.send(&reserve("res-1", &digest, 301_824)).await;

    assert_eq!(
        peer.recv().await,
        ServerFrame::Reserved {
            request_id: "res-1".to_owned(),
            status: ReserveStatus::Granted,
            expires_in: Some(PAYLOAD_TIME_TO_LIVE.as_secs()),
        },
        "the reservation was not granted with its lifetime stated"
    );
    assert_eq!(
        peer.recv_within(QUIET).await,
        None,
        "a second answer arrived for one reservation"
    );
}

// 3.5: nothing reaches other peers.
#[tokio::test]
async fn a_reservation_delivers_nothing_to_other_peers() {
    let relay = Relay::with_store("reserve-private").await;
    let room = room("reserve-private");
    let mut reserver = Client::join(&relay, &room, "macbook").await;
    let mut witness = Client::join(&relay, &room, "linux-box").await;

    reserver
        .send(&reserve(
            "res-1",
            &digest_of(b"private to the reserver"),
            4096,
        ))
        .await;
    assert!(
        matches!(
            reserver.recv().await,
            ServerFrame::Reserved {
                status: ReserveStatus::Granted,
                ..
            }
        ),
        "the reservation was not granted"
    );

    assert_eq!(
        witness.recv_within(QUIET).await,
        None,
        "a reservation is not a message and must reach nobody else"
    );
}

// 3.5: pre-handshake, on the same terms as any other frame.
#[tokio::test]
async fn a_reservation_before_hello_is_refused_like_any_other_frame() {
    let relay = Relay::with_store("reserve-before-hello").await;
    let mut client = Client::connect(&relay).await;

    client
        .send(&reserve("res-1", &digest_of(b"too early"), 4096))
        .await;

    // `invalid_hello`, exactly as a `send` or a `list` before the handshake:
    // an unadmitted connection has no room, so a reservation has nothing to be
    // scoped to.
    client
        .expect_error_then_close(ErrorCode::InvalidHello)
        .await;
}

// 3.5: an over-sized reservation is a status on an open connection.
#[tokio::test]
async fn an_over_sized_reservation_is_a_status_and_the_connection_survives() {
    let relay = Relay::with_store("reserve-too-large").await;
    let room = room("reserve-too-large");
    let mut peer = Client::join(&relay, &room, "macbook").await;

    peer.send(&reserve(
        "res-1",
        &digest_of(b"far too large"),
        MAX_PAYLOAD_BYTES + 1,
    ))
    .await;

    assert_eq!(
        peer.recv().await,
        ServerFrame::Reserved {
            request_id: "res-1".to_owned(),
            status: ReserveStatus::PayloadTooLarge,
            // A refusal has no payload, so it has no lifetime to state.
            expires_in: None,
        },
        "an over-sized reservation was not refused as a status"
    );

    // The connection is open and usable, which is the difference between a
    // status and an error code.
    peer.send(&ClientFrame::Ping).await;
    assert_eq!(
        peer.recv().await,
        ServerFrame::Pong,
        "a refused reservation closed the connection"
    );
    assert_eq!(
        relay.state.list_peers(&room),
        vec!["macbook".to_owned()],
        "a refused reservation cost the peer its registration"
    );
}

// Each bound reports itself, because the three mean different things to a sender.
#[tokio::test]
async fn a_full_room_refuses_with_the_bound_it_reached() {
    let relay = Relay::with_store("reserve-room-full").await;
    let room = room("reserve-room-full");
    let mut peer = Client::join(&relay, &room, "macbook").await;

    let units = MAX_ROOM_BYTES / MAX_PAYLOAD_BYTES;
    for n in 0..units {
        peer.send(&reserve(
            &format!("res-{n}"),
            &digest_of(&n.to_le_bytes()),
            MAX_PAYLOAD_BYTES,
        ))
        .await;
        assert!(
            matches!(
                peer.recv().await,
                ServerFrame::Reserved {
                    status: ReserveStatus::Granted,
                    ..
                }
            ),
            "reservation {n} inside the room total was refused"
        );
    }

    peer.send(&reserve(
        "res-over",
        &digest_of(b"one too many"),
        MAX_PAYLOAD_BYTES,
    ))
    .await;
    assert_eq!(
        peer.recv().await,
        ServerFrame::Reserved {
            request_id: "res-over".to_owned(),
            status: ReserveStatus::RoomFull,
            expires_in: None,
        },
        "the room's own bound was not the one reported"
    );
}

#[tokio::test]
async fn a_reservation_names_the_room_the_connection_was_admitted_to() {
    let relay = Relay::with_store("reserve-room-scoped").await;
    let mine = room("reserve-mine");
    let theirs = room("reserve-theirs");
    let digest = digest_of(b"scoped to one room");

    let mut peer = Client::join(&relay, &mine, "macbook").await;
    peer.send(&reserve("res-1", &digest, 4096)).await;
    assert!(matches!(
        peer.recv().await,
        ServerFrame::Reserved {
            status: ReserveStatus::Granted,
            ..
        }
    ));

    // The reservation exists in the reserving connection's own room and nowhere
    // else. There is no field for a room on the frame, so this is the whole of
    // what "ignores any supplied room" can mean: the store holds it under the
    // registration's room.
    assert!(
        relay.store().begin_upload(&mine, &digest, 4096).is_ok(),
        "the reservation was not made in the connection's own room"
    );
    assert!(
        relay.store().begin_upload(&theirs, &digest, 4096).is_err(),
        "the reservation reached a room the connection was never admitted to"
    );
}

#[tokio::test]
async fn a_malformed_digest_is_refused_by_name_and_keeps_the_connection() {
    let relay = Relay::with_store("reserve-bad-digest").await;
    let room = room("reserve-bad-digest");
    let mut peer = Client::join(&relay, &room, "macbook").await;

    for bad in [
        "",
        "short",
        &"A".repeat(44),
        &format!("{}=", "A".repeat(42)),
    ] {
        peer.send(&reserve("res-1", bad, 4096)).await;
        match peer.recv().await {
            ServerFrame::Error {
                code: ErrorCode::InvalidIdentifier,
                message: Some(message),
                request_id,
            } => {
                assert!(
                    message.contains("reserve.digest"),
                    "the rejection must name the field: {message}"
                );
                assert_eq!(
                    request_id,
                    Some("res-1".to_owned()),
                    "a rejection with a valid request_id must name the frame it answers"
                );
            }
            other => panic!("{bad:?} was not rejected by name: {other:?}"),
        }
    }

    // Still usable: a malformed digest is recoverable.
    peer.send(&ClientFrame::Ping).await;
    assert_eq!(peer.recv().await, ServerFrame::Pong);
}

#[tokio::test]
async fn a_malformed_request_id_is_refused_before_the_digest() {
    let relay = Relay::with_store("reserve-bad-token").await;
    let room = room("reserve-bad-token");
    let mut peer = Client::join(&relay, &room, "macbook").await;

    // Both fields are invalid. The correlation token is checked first, because
    // no rejection can name the frame it answers without one.
    peer.send(&reserve(
        &"t".repeat(MAX_CORRELATION_BYTES + 1),
        "not-a-digest",
        4096,
    ))
    .await;

    match peer.recv().await {
        ServerFrame::Error {
            code: ErrorCode::InvalidIdentifier,
            message: Some(message),
            request_id,
        } => {
            assert!(
                message.contains("reserve.request_id"),
                "the token must be rejected before the digest: {message}"
            );
            assert_eq!(
                request_id, None,
                "a rejected token cannot be echoed as the frame's own identifier"
            );
        }
        other => panic!("an invalid request_id was not rejected: {other:?}"),
    }
}

// A relay without a store answers as an older relay would, which is what makes
// the client's capability probe work against both.
#[tokio::test]
async fn a_relay_without_a_store_reports_reservations_unsupported() {
    let relay = Relay::start().await;
    let room = room("reserve-unsupported");
    let mut peer = Client::join(&relay, &room, "macbook").await;

    peer.send(&reserve("res-1", &digest_of(b"nowhere to put it"), 4096))
        .await;

    match peer.recv().await {
        ServerFrame::Error {
            code: ErrorCode::UnsupportedFrame,
            request_id,
            ..
        } => assert_eq!(request_id, Some("res-1".to_owned())),
        other => panic!("expected unsupported_frame, got {other:?}"),
    }

    // Open, so the client reports attachments unavailable rather than losing its
    // connection over asking.
    peer.send(&ClientFrame::Ping).await;
    assert_eq!(peer.recv().await, ServerFrame::Pong);
}

// 3.5: a saturated reply path leaves no reservation made.
//
// Built on the pattern the equivalent `send` and `announce` tests use, because
// the premise is the same and is not easy to reach: the peer's *writer* must be
// blocked, which needs its outbound queue full, and only then does the reply
// channel saturate. Filling the reply channel with unread `pong`s does not
// work -- a reply this test never reads is a reply the relay's writer already
// took.
#[tokio::test]
async fn a_saturated_reply_path_makes_no_reservation() {
    let relay = Relay::with_store("reserve-saturated").await;
    let here = room("reserve-saturated");

    let mut filler = Client::join(&relay, &here, "filler").await;
    let mut witness = Client::join(&relay, &here, "witness").await;
    // Never read from, so its socket buffer and outbound queue both fill and the
    // relay's writer for this peer is blocked for the rest of the test.
    let mut stalled = Client::join(&relay, &here, "stalled").await;

    let backlog = fill_pipeline(&mut filler, "stalled").await;
    assert!(
        backlog >= OUTBOUND_QUEUE_CAPACITY,
        "the premise is a blocked writer; observed a backlog of {backlog}"
    );

    // Walk the reply channel to saturation with reservations, one at a time,
    // stopping at the refusal. Each granted reservation queues one `reserved`
    // the blocked writer cannot take. `witness` is the barrier: a `send` that
    // still arrives means the relay's reader is still running, and the first one
    // that does not means it has stopped -- which is when `stalled` may start
    // reading without freeing the slots whose absence is under test.
    let mut granted = 0_usize;
    for n in 1..=OUTBOUND_QUEUE_CAPACITY {
        stalled
            .send(&reserve(
                &format!("res-{n}"),
                &digest_of(&n.to_le_bytes()),
                MAX_PAYLOAD_BYTES,
            ))
            .await;
        // A `send` to the witness after each reservation, so the reader's
        // progress is observable from outside the stalled connection.
        stalled
            .send(&ClientFrame::Send {
                id: format!("probe-{n}"),
                to: "witness".to_owned(),
                body: "still reading".to_owned(),
                reply_to: None,
                attachment: None,
            })
            .await;

        match witness.recv_within(QUIET).await {
            Some(ServerFrame::Message { id, .. }) => {
                assert_eq!(id, format!("probe-{n}"), "the reader fell behind");
                granted += 1;
            }
            Some(other) => panic!("unexpected frame at the witness: {other:?}"),
            None => break,
        }
    }
    assert!(
        granted < OUTBOUND_QUEUE_CAPACITY,
        "the reply channel must have saturated for this test to mean anything, and all \
         {OUTBOUND_QUEUE_CAPACITY} reservations were answered instead"
    );
    println!("{granted} reservations answered before the reply channel saturated");

    // The close names its cause, as every relay-initiated close must.
    match stalled.drain_until_error().await {
        Some(ServerFrame::Error { code, message, .. }) => assert_eq!(
            code,
            ErrorCode::IdleTimeout,
            "the close must name the saturated reply path; message {message:?}"
        ),
        None => panic!("the relay closed without an `error` frame"),
        other => panic!("expected an error frame, got {other:?}"),
    }
    stalled.expect_closed().await;

    // The other half, and the reason this test exists: the reservation the relay
    // declined to answer was never made. Exactly the answered ones hold an
    // allowance, so exactly the remaining units are still available and the one
    // after them is refused. A reservation made without an answer would show up
    // here as one unit missing.
    support::wait_until_deregistered(&relay, &here, "stalled").await;
    let units = usize::try_from(MAX_ROOM_BYTES / MAX_PAYLOAD_BYTES).expect("fits");
    assert!(
        granted < units,
        "the reply channel saturated after {granted} reservations, which is the whole \
         room -- this test cannot distinguish held from unheld allowances"
    );
    for n in 0..units - granted {
        relay
            .store()
            .reserve(
                &here,
                &digest_of(&(1000 + n).to_le_bytes()),
                MAX_PAYLOAD_BYTES,
            )
            .unwrap_or_else(|refusal| {
                panic!(
                    "unit {n} of the {} the room should still have was refused with \
                     {refusal}: an unanswered reservation was made after all",
                    units - granted
                )
            });
    }
    assert_eq!(
        relay
            .store()
            .reserve(
                &here,
                &digest_of(b"one past the ceiling"),
                MAX_PAYLOAD_BYTES
            )
            .err(),
        Some(blob::Refusal::RoomFull),
        "the room admitted more than the {granted} answered reservations plus its \
         remaining {} units",
        units - granted
    );
}

// 4.4: a reference passes through unchanged.
#[tokio::test]
async fn a_reference_passes_through_a_send_unchanged() {
    let relay = Relay::with_store("attachment-send").await;
    let room = room("attachment-send");
    let mut sender = Client::join(&relay, &room, "macbook").await;
    let mut recipient = Client::join(&relay, &room, "linux-box").await;
    let digest = digest_of(b"the diff itself");

    sender
        .send(&ClientFrame::Send {
            id: "msg-1".to_owned(),
            to: "linux-box".to_owned(),
            body: "the diff is attached".to_owned(),
            reply_to: None,
            attachment: Some(digest.clone()),
        })
        .await;

    assert_eq!(
        recipient.recv().await,
        ServerFrame::Message {
            id: "msg-1".to_owned(),
            from: "macbook".to_owned(),
            body: "the diff is attached".to_owned(),
            reply_to: None,
            attachment: Some(digest.clone()),
        },
        "the reference did not arrive unchanged"
    );
    assert_eq!(
        sender.recv().await,
        ServerFrame::Receipt {
            id: "msg-1".to_owned(),
            to: "linux-box".to_owned(),
            status: ReceiptStatus::Routed,
        }
    );
}

// 4.4: omitted when absent.
#[tokio::test]
async fn a_message_without_a_reference_carries_none() {
    let relay = Relay::with_store("attachment-absent").await;
    let room = room("attachment-absent");
    let mut sender = Client::join(&relay, &room, "macbook").await;
    let mut recipient = Client::join(&relay, &room, "linux-box").await;

    sender
        .send(&ClientFrame::Send {
            id: "msg-1".to_owned(),
            to: "linux-box".to_owned(),
            body: "nothing attached".to_owned(),
            reply_to: None,
            attachment: None,
        })
        .await;

    match recipient.recv().await {
        ServerFrame::Message { attachment, .. } => {
            assert_eq!(attachment, None, "a reference appeared from nowhere");
        }
        other => panic!("expected a message, got {other:?}"),
    }
}

// 4.4: a reference to a payload that was never uploaded is still routed.
#[tokio::test]
async fn a_reference_to_nothing_is_routed_with_its_usual_receipt() {
    let relay = Relay::with_store("attachment-dangling").await;
    let room = room("attachment-dangling");
    let mut sender = Client::join(&relay, &room, "macbook").await;
    let mut recipient = Client::join(&relay, &room, "linux-box").await;

    // Never reserved, never uploaded. The routing path performs no store lookup,
    // so this is routed exactly as a reference to a stored payload would be:
    // whether it resolves is the recipient's question at the moment it asks.
    let dangling = digest_of(b"a payload that does not exist");
    assert_eq!(
        relay.store().payload_len(&room, &dangling),
        None,
        "the fixture must reference nothing"
    );

    sender
        .send(&ClientFrame::Send {
            id: "msg-1".to_owned(),
            to: "linux-box".to_owned(),
            body: "attached, supposedly".to_owned(),
            reply_to: None,
            attachment: Some(dangling.clone()),
        })
        .await;

    assert_eq!(
        sender.recv().await,
        ServerFrame::Receipt {
            id: "msg-1".to_owned(),
            to: "linux-box".to_owned(),
            status: ReceiptStatus::Routed,
        },
        "a dangling reference changed the receipt status"
    );
    match recipient.recv().await {
        ServerFrame::Message { attachment, .. } => {
            assert_eq!(attachment, Some(dangling));
        }
        other => panic!("expected a message, got {other:?}"),
    }
}

// 4.4: an announcement's reference reaches every recipient.
#[tokio::test]
async fn an_announced_reference_reaches_every_recipient() {
    let relay = Relay::with_store("attachment-announce").await;
    let room = room("attachment-announce");
    let mut announcer = Client::join(&relay, &room, "macbook").await;
    let mut first = Client::join(&relay, &room, "linux-box").await;
    let mut second = Client::join(&relay, &room, "windows-main").await;
    let digest = digest_of(b"the build log everyone wants");

    announcer
        .send(&ClientFrame::Announce {
            id: "ann-1".to_owned(),
            body: "the build log is attached".to_owned(),
            reply_to: None,
            attachment: Some(digest.clone()),
        })
        .await;

    assert_eq!(
        announcer.recv().await,
        ServerFrame::Accepted {
            id: "ann-1".to_owned(),
            delivered: 2,
            shed: 0,
        }
    );

    for recipient in [&mut first, &mut second] {
        assert_eq!(
            recipient.recv().await,
            ServerFrame::Notice {
                id: "ann-1".to_owned(),
                from: "macbook".to_owned(),
                body: "the build log is attached".to_owned(),
                reply_to: None,
                attachment: Some(digest.clone()),
            },
            "a recipient did not receive the announced reference"
        );
    }

    // The announcer is excluded from its own fanout, reference and all.
    assert_eq!(
        announcer.recv_within(QUIET).await,
        None,
        "the announcer received its own announcement"
    );
}

// The whole sequence a client will run, end to end over one relay: reserve,
// upload, then attach. Here to pin the order the design requires -- nothing is
// attached before a grant.
#[tokio::test]
async fn reserve_then_upload_then_attach_delivers_a_resolvable_reference() {
    let relay = Relay::with_store("attachment-round-trip").await;
    let room = room("attachment-round-trip");
    let mut sender = Client::join(&relay, &room, "macbook").await;
    let mut recipient = Client::join(&relay, &room, "linux-box").await;

    let payload: Vec<u8> = (0..200_000_u32)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect();
    let digest = digest_of(&payload);

    sender
        .send(&reserve("res-1", &digest, payload.len() as u64))
        .await;
    let ServerFrame::Reserved {
        status: ReserveStatus::Granted,
        expires_in: Some(lifetime),
        ..
    } = sender.recv().await
    else {
        panic!("the reservation was not granted");
    };
    assert_eq!(lifetime, PAYLOAD_TIME_TO_LIVE.as_secs());

    let path = support::blob_path(&room, &digest);
    assert_eq!(
        support::http_put(relay.addr, &path, &payload).await.status,
        201,
        "the upload was refused after a granted reservation"
    );

    sender
        .send(&ClientFrame::Send {
            id: "msg-1".to_owned(),
            to: "linux-box".to_owned(),
            body: format!("attached; it resolves for {lifetime} seconds"),
            reply_to: None,
            attachment: Some(digest.clone()),
        })
        .await;
    assert!(matches!(
        sender.recv().await,
        ServerFrame::Receipt {
            status: ReceiptStatus::Routed,
            ..
        }
    ));

    let ServerFrame::Message {
        attachment: Some(reference),
        ..
    } = recipient.recv().await
    else {
        panic!("the delivery carried no reference");
    };
    assert_eq!(reference, digest);

    // The recipient resolves what it was told about, from the same address and
    // port its frame connection uses.
    let fetched = support::http(
        relay.addr,
        "GET",
        &support::blob_path(&room, &reference),
        &[],
        &[],
    )
    .await;
    assert_eq!(fetched.status, 200, "the reference did not resolve");
    assert_eq!(fetched.body, payload, "the fetched bytes differ");
}
