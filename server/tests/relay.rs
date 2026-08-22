//! Integration tests driving a real loopback listener with a framed
//! MessagePack client.
//!
//! Every test here asserts wire-observable behavior. Where a claim is about the
//! registry rather than the wire -- that a superseded connection's cleanup left
//! the replacement alone -- the test reads the relay's own state.

mod support;

use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Duration;

use omp_relayd::protocol::{
    self, ClientFrame, ErrorCode, MAX_BODY_BYTES, MAX_FRAME_BYTES, MAX_IDENTIFIER_BYTES,
    PROTOCOL_VERSION, ReceiptStatus, ServerFrame,
};
use omp_relayd::relay::{self, Deadlines, OUTBOUND_QUEUE_CAPACITY, ServerState};
use serde::Serialize;
use tokio::sync::watch;

use support::{Client, QUIET, Relay, length_prefix, room, wait_until_deregistered};

fn send(id: &str, to: &str, body: &str) -> ClientFrame {
    ClientFrame::Send {
        id: id.to_owned(),
        to: to.to_owned(),
        body: body.to_owned(),
        reply_to: None,
    }
}

fn routed(id: &str, to: &str) -> ServerFrame {
    ServerFrame::Receipt {
        id: id.to_owned(),
        to: to.to_owned(),
        status: ReceiptStatus::Routed,
    }
}

// ---------------------------------------------------------------- handshake

#[tokio::test]
async fn a_valid_handshake_is_answered_with_ready() {
    let relay = Relay::start().await;
    // `join` asserts `ready` with the negotiated protocol version.
    let _peer = Client::join(&relay, &room("handshake"), "macbook-reviewer").await;

    assert_eq!(
        relay.state.list_peers(&room("handshake")),
        vec!["macbook-reviewer"],
        "a successful handshake must register the peer"
    );
}

#[tokio::test]
async fn an_identifier_at_the_length_limit_is_accepted() {
    let relay = Relay::start().await;
    let peer = "p".repeat(protocol::MAX_IDENTIFIER_BYTES);

    let _client = Client::join(&relay, &room("limits"), &peer).await;
    assert_eq!(
        relay.state.list_peers(&room("limits")),
        vec![peer.clone()],
        "a {}-byte peer name must be accepted",
        peer.len()
    );
}

#[tokio::test]
async fn a_first_frame_that_is_not_hello_is_rejected() {
    let relay = Relay::start().await;
    let mut client = Client::connect(&relay).await;

    client
        .send(&ClientFrame::List {
            request_id: "req-1".to_owned(),
        })
        .await;

    client
        .expect_error_then_close(ErrorCode::InvalidHello)
        .await;
}

#[tokio::test]
async fn a_silent_connection_is_closed_when_the_handshake_deadline_elapses() {
    let relay = Relay::with_deadlines(Deadlines {
        hello: Duration::from_millis(200),
        idle: Duration::from_secs(90),
    })
    .await;

    let mut client = Client::connect(&relay).await;
    client
        .expect_error_then_close(ErrorCode::HelloTimeout)
        .await;
}

#[tokio::test]
async fn an_unsupported_protocol_version_is_rejected() {
    let relay = Relay::start().await;
    let mut client = Client::connect(&relay).await;

    client
        .send(&ClientFrame::Hello {
            protocol: 2,
            room: room("versions"),
            peer: "from-the-future".to_owned(),
        })
        .await;

    client
        .expect_error_then_close(ErrorCode::UnsupportedProtocol)
        .await;
}

#[tokio::test]
async fn invalid_identifiers_are_rejected_at_the_handshake() {
    for (label, project, peer) in [
        ("slash in project", "omp/relayd", "reviewer"),
        ("at sign in peer", "omp-relayd", "reviewer@host"),
        ("empty peer", "omp-relayd", ""),
        ("leading whitespace", "omp-relayd", " reviewer"),
    ] {
        let relay = Relay::start().await;
        let mut client = Client::connect(&relay).await;

        client
            .send(&ClientFrame::Hello {
                protocol: PROTOCOL_VERSION,
                room: protocol::RoomId::new(project, "identifiers"),
                peer: peer.to_owned(),
            })
            .await;

        client
            .expect_error_then_close(ErrorCode::InvalidIdentifier)
            .await;
        assert!(
            relay
                .state
                .list_peers(&protocol::RoomId::new(project, "identifiers"))
                .is_empty(),
            "{label}: a rejected handshake must register nothing"
        );
    }
}

#[tokio::test]
async fn a_second_hello_closes_the_connection() {
    let relay = Relay::start().await;
    let mut client = Client::join(&relay, &room("duplicate"), "reviewer").await;

    client
        .send(&ClientFrame::Hello {
            protocol: PROTOCOL_VERSION,
            room: room("duplicate"),
            peer: "reviewer".to_owned(),
        })
        .await;

    client
        .expect_error_then_close(ErrorCode::DuplicateHello)
        .await;
}

// ------------------------------------------------------------------ listing

#[tokio::test]
async fn both_peers_appear_in_a_sorted_room_scoped_listing() {
    let relay = Relay::start().await;
    let here = room("listing");
    let elsewhere = room("listing-other");

    let mut reviewer = Client::join(&relay, &here, "macbook-reviewer").await;
    let _main = Client::join(&relay, &here, "windows-main").await;
    let _other = Client::join(&relay, &elsewhere, "another-room").await;

    reviewer
        .send(&ClientFrame::List {
            request_id: "abc123".to_owned(),
        })
        .await;

    assert_eq!(
        reviewer.recv().await,
        ServerFrame::Peers {
            request_id: "abc123".to_owned(),
            peers: vec!["macbook-reviewer".to_owned(), "windows-main".to_owned()],
        },
        "the listing must be sorted, include the sender, exclude other rooms, \
         and echo the request id unchanged"
    );
}

#[tokio::test]
async fn case_distinct_peer_names_are_distinct_peers() {
    let relay = Relay::start().await;
    let here = room("case");

    let mut upper = Client::join(&relay, &here, "Reviewer").await;
    let mut lower = Client::join(&relay, &here, "reviewer").await;

    upper
        .send(&ClientFrame::List {
            request_id: "req-1".to_owned(),
        })
        .await;
    assert_eq!(
        upper.recv().await,
        ServerFrame::Peers {
            request_id: "req-1".to_owned(),
            peers: vec!["Reviewer".to_owned(), "reviewer".to_owned()],
        },
        "identifiers are compared bytewise, never case-folded"
    );

    upper
        .send(&send("m1", "reviewer", "for the lowercase one"))
        .await;
    assert_eq!(upper.recv().await, routed("m1", "reviewer"));
    assert_eq!(
        lower.recv().await,
        ServerFrame::Message {
            id: "m1".to_owned(),
            from: "Reviewer".to_owned(),
            body: "for the lowercase one".to_owned(),
            reply_to: None,
        },
        "the message must reach the lowercase peer, not the sender"
    );
}

#[tokio::test]
async fn an_over_long_request_id_is_rejected_without_closing_the_connection() {
    let relay = Relay::start().await;
    let mut client = Client::join(&relay, &room("request-ids"), "reviewer").await;

    client
        .send(&ClientFrame::List {
            request_id: "r".repeat(protocol::MAX_CORRELATION_BYTES + 1),
        })
        .await;

    match client.recv().await {
        ServerFrame::Error { code, .. } => assert_eq!(code, ErrorCode::InvalidIdentifier),
        other => panic!("expected an invalid_identifier error, received {other:?}"),
    }

    client.send(&ClientFrame::Ping).await;
    assert_eq!(
        client.recv().await,
        ServerFrame::Pong,
        "an application-level rejection must keep the connection open"
    );
}

// ------------------------------------------------------------------ routing

#[tokio::test]
async fn three_messages_from_one_sender_arrive_in_order() {
    let relay = Relay::start().await;
    let here = room("ordering");

    let mut sender = Client::join(&relay, &here, "macbook-reviewer").await;
    let mut recipient = Client::join(&relay, &here, "windows-main").await;

    for id in ["m1", "m2", "m3"] {
        sender
            .send(&send(id, "windows-main", &format!("body of {id}")))
            .await;
    }

    for id in ["m1", "m2", "m3"] {
        assert_eq!(
            sender.recv().await,
            routed(id, "windows-main"),
            "one receipt per send, in send order"
        );
    }

    for id in ["m1", "m2", "m3"] {
        assert_eq!(
            recipient.recv().await,
            ServerFrame::Message {
                id: id.to_owned(),
                from: "macbook-reviewer".to_owned(),
                body: format!("body of {id}"),
                reply_to: None,
            },
            "frames from one sender must arrive in read order"
        );
    }
}

#[tokio::test]
async fn a_send_to_an_unregistered_name_delivers_nothing() {
    let relay = Relay::start().await;
    let here = room("offline");

    let mut sender = Client::join(&relay, &here, "macbook-reviewer").await;
    let mut bystander = Client::join(&relay, &here, "windows-main").await;

    sender.send(&send("m1", "ghost", "anyone there")).await;

    assert_eq!(
        sender.recv().await,
        ServerFrame::Receipt {
            id: "m1".to_owned(),
            to: "ghost".to_owned(),
            status: ReceiptStatus::PeerOffline,
        }
    );
    assert_eq!(
        bystander.recv_within(QUIET).await,
        None,
        "an undeliverable message must not reach anyone else"
    );
}

#[tokio::test]
async fn an_invalid_target_yields_a_receipt_and_keeps_the_connection_open() {
    let relay = Relay::start().await;
    let mut sender = Client::join(&relay, &room("targets"), "macbook-reviewer").await;

    for target in ["", "windows@main"] {
        sender.send(&send("m1", target, "hello")).await;
        assert_eq!(
            sender.recv().await,
            ServerFrame::Receipt {
                id: "m1".to_owned(),
                to: target.to_owned(),
                status: ReceiptStatus::InvalidTarget,
            },
            "target {target:?} must be reported as invalid, not offline"
        );
    }

    sender.send(&ClientFrame::Ping).await;
    assert_eq!(
        sender.recv().await,
        ServerFrame::Pong,
        "an invalid target must not close the connection"
    );
}

/// Regression: `receipt.to` echoes the value that failed validation, so an
/// over-long target used to build a receipt larger than the frame cap. The
/// encode failure surfaced as a bare close, delivering neither the receipt
/// `peer-relay` requires for every valid `send` nor a stated cause.
///
/// The sizes are printed because they are the whole point: the inbound frame
/// fits exactly and the naive receipt does not.
#[tokio::test]
async fn an_oversized_target_is_answered_with_a_receipt_rather_than_a_bare_close() {
    let relay = Relay::start().await;
    let mut sender = Client::join(&relay, &room("oversized-targets"), "sender").await;

    // The largest `to` a client can put on the wire: grown until the inbound
    // `send` frame would exceed the cap, then stepped back one byte.
    let mut to = String::new();
    loop {
        to.push('x');
        let candidate = ClientFrame::Send {
            id: "m1".to_owned(),
            to: to.clone(),
            body: String::new(),
            reply_to: None,
        };
        if protocol::encode(&candidate).expect("encodes").len() > MAX_FRAME_BYTES {
            to.pop();
            break;
        }
    }

    let naive = protocol::encode(&ServerFrame::Receipt {
        id: "m1".to_owned(),
        to: to.clone(),
        status: ReceiptStatus::InvalidTarget,
    })
    .expect("encodes")
    .len();
    println!(
        "to = {} bytes; unclamped receipt = {naive} bytes; frame cap = {MAX_FRAME_BYTES}",
        to.len()
    );
    assert!(
        naive > MAX_FRAME_BYTES,
        "this test is only meaningful while an unclamped receipt exceeds the cap; \
         observed {naive} bytes against a {MAX_FRAME_BYTES}-byte cap"
    );

    sender
        .send(&ClientFrame::Send {
            id: "m1".to_owned(),
            to: to.clone(),
            body: String::new(),
            reply_to: None,
        })
        .await;

    assert_eq!(
        sender.recv().await,
        ServerFrame::Receipt {
            id: "m1".to_owned(),
            to: to[..MAX_IDENTIFIER_BYTES].to_owned(),
            status: ReceiptStatus::InvalidTarget,
        },
        "an over-long target must be reported as invalid, with the echo clamped \
         to the identifier limit"
    );

    sender.send(&ClientFrame::Ping).await;
    assert_eq!(
        sender.recv().await,
        ServerFrame::Pong,
        "an over-long target is a target-validation failure, so the connection \
         must stay open like every other one"
    );
}

/// A rejected `reply_to` is the one `send` failure whose `id` is already known
/// good, so it is the one the client can be told about by name. Without the
/// echo a client pipelining sends cannot tell which frame the error answers.
#[tokio::test]
async fn a_rejected_reply_to_names_the_send_it_rejected() {
    let relay = Relay::start().await;
    let mut sender = Client::join(&relay, &room("reply-correlation"), "sender").await;

    sender
        .send(&ClientFrame::Send {
            id: "send-42".to_owned(),
            to: "recipient".to_owned(),
            body: "hi".to_owned(),
            reply_to: Some("r".repeat(129)),
        })
        .await;

    match sender.recv().await {
        ServerFrame::Error {
            code,
            request_id,
            message,
        } => {
            assert_eq!(
                code,
                ErrorCode::InvalidIdentifier,
                "an over-long reply_to is an identifier failure; message was {message:?}"
            );
            assert_eq!(
                request_id.as_deref(),
                Some("send-42"),
                "the error must name the send it rejected"
            );
        }
        other => panic!("expected an invalid_identifier error, received {other:?}"),
    }

    sender.send(&ClientFrame::Ping).await;
    assert_eq!(
        sender.recv().await,
        ServerFrame::Pong,
        "a rejected reply_to is recoverable and must keep the connection open"
    );
}

#[tokio::test]
async fn a_self_addressed_send_is_delivered_to_the_sender() {
    let relay = Relay::start().await;
    let mut solo = Client::join(&relay, &room("self"), "solo").await;

    solo.send(&send("m1", "solo", "note to self")).await;

    // The receipt and the delivered message both arrive; their relative order
    // is not part of the contract.
    let frames = [solo.recv().await, solo.recv().await];
    assert!(
        frames.contains(&routed("m1", "solo")),
        "expected a routed receipt among {frames:?}"
    );
    assert!(
        frames.contains(&ServerFrame::Message {
            id: "m1".to_owned(),
            from: "solo".to_owned(),
            body: "note to self".to_owned(),
            reply_to: None,
        }),
        "expected the delivered message among {frames:?}"
    );
}

#[tokio::test]
async fn a_reply_reference_is_carried_through_and_omitted_when_absent() {
    let relay = Relay::start().await;
    let here = room("replies");

    let mut sender = Client::join(&relay, &here, "macbook-reviewer").await;
    let mut recipient = Client::join(&relay, &here, "windows-main").await;

    sender
        .send(&ClientFrame::Send {
            id: "m2".to_owned(),
            to: "windows-main".to_owned(),
            body: "answering".to_owned(),
            reply_to: Some("m1".to_owned()),
        })
        .await;
    assert_eq!(sender.recv().await, routed("m2", "windows-main"));
    assert_eq!(
        recipient.recv().await,
        ServerFrame::Message {
            id: "m2".to_owned(),
            from: "macbook-reviewer".to_owned(),
            body: "answering".to_owned(),
            reply_to: Some("m1".to_owned()),
        },
        "reply_to must be carried through unchanged"
    );

    sender.send(&send("m3", "windows-main", "unprompted")).await;
    assert_eq!(sender.recv().await, routed("m3", "windows-main"));
    assert_eq!(
        recipient.recv().await,
        ServerFrame::Message {
            id: "m3".to_owned(),
            from: "macbook-reviewer".to_owned(),
            body: "unprompted".to_owned(),
            reply_to: None,
        },
        "an absent reply_to must stay absent"
    );
}

#[tokio::test]
async fn a_repeated_message_id_is_not_deduplicated() {
    let relay = Relay::start().await;
    let here = room("duplicates");

    let mut sender = Client::join(&relay, &here, "macbook-reviewer").await;
    let mut recipient = Client::join(&relay, &here, "windows-main").await;

    for _ in 0..2 {
        sender.send(&send("same-id", "windows-main", "twice")).await;
        assert_eq!(sender.recv().await, routed("same-id", "windows-main"));
    }

    for round in 1..=2 {
        assert_eq!(
            recipient.recv().await,
            ServerFrame::Message {
                id: "same-id".to_owned(),
                from: "macbook-reviewer".to_owned(),
                body: "twice".to_owned(),
                reply_to: None,
            },
            "delivery {round} of a repeated id must still arrive"
        );
    }
}

/// A `send` carrying fields the relay does not define, the way a newer or
/// buggier client might.
#[derive(Serialize)]
struct SendWithExtras<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    id: &'a str,
    to: &'a str,
    body: &'a str,
    priority: u32,
    from: &'a str,
}

#[tokio::test]
async fn unknown_fields_are_ignored_and_a_supplied_sender_identity_is_not_trusted() {
    let relay = Relay::start().await;
    let here = room("extras");

    let mut sender = Client::join(&relay, &here, "macbook-reviewer").await;
    let mut recipient = Client::join(&relay, &here, "windows-main").await;

    let payload = protocol::encode(&SendWithExtras {
        kind: "send",
        id: "m1",
        to: "windows-main",
        body: "with extras",
        priority: 9,
        from: "impersonated",
    })
    .expect("encode");
    sender.send_payload(payload.to_vec()).await;

    assert_eq!(sender.recv().await, routed("m1", "windows-main"));
    assert_eq!(
        recipient.recv().await,
        ServerFrame::Message {
            id: "m1".to_owned(),
            from: "macbook-reviewer".to_owned(),
            body: "with extras".to_owned(),
            reply_to: None,
        },
        "an unknown field must be ignored and `from` must come from the registration"
    );
}

#[tokio::test]
async fn a_full_recipient_queue_reports_backpressure_and_keeps_the_sender_open() {
    let relay = Relay::start().await;
    let here = room("backpressure");

    let mut sender = Client::join(&relay, &here, "sender").await;
    // Registered and never read from, so its outbound queue fills.
    let _stalled = Client::join(&relay, &here, "stalled").await;

    // A large body fills the recipient's socket buffers within a few frames, so
    // the 128-slot queue is reached in tens of sends rather than thousands.
    let body = "x".repeat(32 * 1024);
    let mut observed = None;

    for attempt in 1..=2000 {
        sender
            .send(&ClientFrame::Send {
                id: format!("m{attempt}"),
                to: "stalled".to_owned(),
                body: body.clone(),
                reply_to: None,
            })
            .await;

        match sender.recv().await {
            ServerFrame::Receipt {
                status: ReceiptStatus::Routed,
                ..
            } => {}
            ServerFrame::Receipt {
                status: ReceiptStatus::RecipientBackpressure,
                ..
            } => {
                observed = Some(attempt);
                break;
            }
            other => panic!("attempt {attempt}: unexpected {other:?}"),
        }
    }

    let attempt = observed.expect("backpressure must appear before 2000 sends of 32 KiB");
    assert!(
        attempt > OUTBOUND_QUEUE_CAPACITY,
        "backpressure appeared at attempt {attempt}, before the \
         {OUTBOUND_QUEUE_CAPACITY}-slot queue could have filled"
    );
    println!(
        "backpressure observed at attempt {attempt} with a {}-byte body \
         and a {OUTBOUND_QUEUE_CAPACITY}-slot queue",
        body.len()
    );

    sender.send(&ClientFrame::Ping).await;
    assert_eq!(
        sender.recv().await,
        ServerFrame::Pong,
        "a backpressured recipient must not close the sender's connection"
    );
}

// -------------------------------------------------------------- replacement

#[tokio::test]
async fn a_replacement_connection_takes_over_the_peer_name() {
    let relay = Relay::start().await;
    let here = room("replacement");

    let mut stale = Client::join(&relay, &here, "windows-main").await;
    let mut sender = Client::join(&relay, &here, "macbook-reviewer").await;
    let mut fresh = Client::join(&relay, &here, "windows-main").await;

    stale.expect_error_then_close(ErrorCode::PeerReplaced).await;

    sender
        .send(&send("m1", "windows-main", "after the takeover"))
        .await;
    assert_eq!(sender.recv().await, routed("m1", "windows-main"));
    assert_eq!(
        fresh.recv().await,
        ServerFrame::Message {
            id: "m1".to_owned(),
            from: "macbook-reviewer".to_owned(),
            body: "after the takeover".to_owned(),
            reply_to: None,
        },
        "traffic must follow the replacement"
    );
}

/// Fills `to`'s socket buffer and then its outbound queue, returning how many
/// sends were accepted before backpressure. Large bodies get there in tens of
/// frames rather than thousands.
async fn fill_pipeline(sender: &mut Client<tokio::net::TcpStream>, to: &str) -> usize {
    let body = "x".repeat(32 * 1024);
    let mut routed = 0usize;
    for attempt in 1..=2000 {
        sender
            .send(&ClientFrame::Send {
                id: format!("m{attempt}"),
                to: to.to_owned(),
                body: body.clone(),
                reply_to: None,
            })
            .await;
        match sender.recv().await {
            ServerFrame::Receipt {
                status: ReceiptStatus::Routed,
                ..
            } => routed += 1,
            ServerFrame::Receipt {
                status: ReceiptStatus::RecipientBackpressure,
                ..
            } => return routed,
            other => panic!("attempt {attempt}: unexpected {other:?}"),
        }
    }
    panic!("backpressure never appeared after 2000 sends of 32 KiB");
}

/// Regression: the eviction signal used to be the closure of the outbound
/// queue, and a closed `mpsc` channel yields every buffered frame before it
/// reports closure. So the diagnostic sat behind the backlog -- and the design
/// notes that a peer being replaced is *usually* one that stopped reading, so
/// usually one whose backlog is full.
///
/// Measured before the fix: `PeerReplaced` arrived after 145 queued messages
/// when the peer resumed reading, and when it did not read it received no
/// diagnostic at all and held its socket until the idle deadline.
#[tokio::test]
async fn a_superseded_connection_is_told_before_its_backlog_is_drained() {
    let relay = Relay::start().await;
    let here = room("replacement-backlog");

    let mut sender = Client::join(&relay, &here, "sender").await;
    // Registered and never read from, so a backlog builds up behind it.
    let mut stale = Client::join(&relay, &here, "target").await;

    let backlog = fill_pipeline(&mut sender, "target").await;
    assert!(
        backlog > 0,
        "the fixture needs a non-empty backlog to be meaningful"
    );

    // Supersede it, then read. Eviction must overtake the *backlog*; it cannot
    // un-send bytes already committed to the socket or to the codec's write
    // buffer, so some frames still precede the diagnostic. That residue is
    // bounded by socket buffer sizes and does not scale with queue depth --
    // observed between 2 and 17 frames depending on load, against a 145-frame
    // backlog. What must not happen is the diagnostic arriving behind the whole
    // backlog, which is what the queue-closure signal did: measured at 145 of
    // 145 before the eviction flag existed.
    //
    // So the assertion is proportional rather than absolute. An exact count
    // would be asserting the kernel's buffer sizes, which is not the contract.
    let _fresh = Client::join(&relay, &here, "target").await;

    let mut ahead_of_diagnostic = 0usize;
    loop {
        match stale.recv().await {
            ServerFrame::Message { .. } => ahead_of_diagnostic += 1,
            ServerFrame::Error { code, .. } => {
                assert_eq!(
                    code,
                    ErrorCode::PeerReplaced,
                    "the superseded connection must be told it was replaced"
                );
                break;
            }
            other => panic!("unexpected frame on a superseded connection: {other:?}"),
        }
        assert!(
            ahead_of_diagnostic * 4 < backlog,
            "eviction must overtake the {backlog}-frame backlog rather than queue behind \
             it: only frames already committed to the socket may precede the diagnostic, \
             and that residue must not scale with queue depth, but {ahead_of_diagnostic} \
             frames arrived first"
        );
    }
    println!(
        "backlog {backlog} frames; {ahead_of_diagnostic} already-committed frame(s) \
         preceded peer_replaced"
    );
    stale.expect_closed().await;
}

/// Regression: a peer that keeps sending heartbeats must not be disconnected
/// for silence it did not commit. TCP is full-duplex, so a peer whose receive
/// buffer is full can still send -- but while the relay was blocked writing to
/// it, the inbound half was not polled, so those frames went unread and the
/// idle deadline was never reset.
///
/// Measured before the fix: a peer that sent 9 pings over 2.72 s against a
/// 1.5 s idle deadline was disconnected anyway.
#[tokio::test]
async fn a_stalled_peer_that_keeps_pinging_is_not_disconnected() {
    let idle = Duration::from_millis(600);
    let relay = Relay::with_deadlines(Deadlines {
        hello: Duration::from_secs(5),
        idle,
    })
    .await;
    let here = room("stalled-heartbeat");

    let mut sender = Client::join(&relay, &here, "sender").await;
    // Never read from: its socket buffer and queue both fill, so the relay's
    // writer for this peer is blocked for the rest of the test.
    let mut slow = Client::join(&relay, &here, "slow").await;

    fill_pipeline(&mut sender, "slow").await;

    // Both peers behave like busy but conscientious clients: heartbeats on
    // time. `slow` additionally never drains, so the relay's writer for it is
    // blocked for the rest of the test. The sender heartbeats too, because its
    // own idle deadline is running and a disconnected sender would confuse the
    // result with an unrelated timeout.
    let mut pings = 0usize;
    let started = tokio::time::Instant::now();
    while started.elapsed() < idle * 3 {
        slow.send(&ClientFrame::Ping).await;
        pings += 1;

        sender.send(&ClientFrame::Ping).await;
        assert_eq!(
            sender.recv().await,
            ServerFrame::Pong,
            "the sender is reading normally and must stay healthy while another \
             peer's writer is blocked"
        );

        tokio::time::sleep(idle / 4).await;
    }

    assert!(
        relay.state.list_peers(&here).iter().any(|n| n == "slow"),
        "a peer that sent {pings} valid pings over {:?} -- three times the {idle:?} idle \
         deadline -- must still be registered; `peer-relay` resets the deadline on any \
         valid inbound frame, and a blocked write must not stop those frames being read",
        started.elapsed()
    );
}

#[tokio::test]
async fn late_cleanup_of_a_superseded_connection_keeps_the_replacement() {
    let relay = Relay::start().await;
    let here = room("late-cleanup");

    let mut stale = Client::join(&relay, &here, "windows-main").await;
    let mut sender = Client::join(&relay, &here, "macbook-reviewer").await;
    let mut fresh = Client::join(&relay, &here, "windows-main").await;

    stale.expect_error_then_close(ErrorCode::PeerReplaced).await;
    // Drop the socket so the superseded task finishes and runs its cleanup now,
    // after the replacement has already registered.
    drop(stale);
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(
        relay.state.list_peers(&here),
        vec!["macbook-reviewer", "windows-main"],
        "the superseded connection's cleanup must not evict the replacement"
    );

    sender
        .send(&send("m1", "windows-main", "still reachable"))
        .await;
    assert_eq!(sender.recv().await, routed("m1", "windows-main"));
    assert_eq!(
        fresh.recv().await,
        ServerFrame::Message {
            id: "m1".to_owned(),
            from: "macbook-reviewer".to_owned(),
            body: "still reachable".to_owned(),
            reply_to: None,
        }
    );
}

// ----------------------------------------------------------------- deadlines

#[tokio::test]
async fn an_idle_connection_is_closed_and_deregistered() {
    let here = room("idle");
    let relay = Relay::with_deadlines(Deadlines {
        hello: Duration::from_secs(5),
        idle: Duration::from_millis(300),
    })
    .await;

    let mut quiet = Client::join(&relay, &here, "quiet").await;
    quiet.expect_error_then_close(ErrorCode::IdleTimeout).await;
    drop(quiet);

    wait_until_deregistered(&relay, &here, "quiet").await;
}

#[tokio::test]
async fn ping_is_answered_and_resets_the_idle_deadline() {
    let relay = Relay::with_deadlines(Deadlines {
        hello: Duration::from_secs(5),
        idle: Duration::from_millis(600),
    })
    .await;

    let mut peer = Client::join(&relay, &room("heartbeat"), "beating").await;

    // Three rounds of 300 ms is 900 ms of connection life against a 600 ms
    // deadline: without the reset the second round would already have failed.
    for round in 1..=3 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        peer.send(&ClientFrame::Ping).await;
        assert_eq!(
            peer.recv().await,
            ServerFrame::Pong,
            "round {round} must be answered"
        );
    }

    // Once the pings stop, the deadline is enforced.
    peer.expect_error_then_close(ErrorCode::IdleTimeout).await;
}

#[tokio::test]
async fn a_peer_that_stops_reading_is_closed_by_the_idle_deadline() {
    let relay = Relay::with_deadlines(Deadlines {
        hello: Duration::from_secs(5),
        idle: Duration::from_millis(400),
    })
    .await;
    let here = room("write-stall");

    let mut sender = Client::join(&relay, &here, "sender").await;
    // Registered and never read from. Once its outbound queue and socket
    // buffers fill, its connection task is inside a socket write that nobody
    // is draining.
    let _stalled = Client::join(&relay, &here, "stalled").await;

    let body = "x".repeat(32 * 1024);
    for attempt in 1..=2000 {
        sender
            .send(&ClientFrame::Send {
                id: format!("m{attempt}"),
                to: "stalled".to_owned(),
                body: body.clone(),
                reply_to: None,
            })
            .await;

        if let ServerFrame::Receipt {
            status: ReceiptStatus::RecipientBackpressure,
            ..
        } = sender.recv().await
        {
            break;
        }
    }

    // The stalled peer has sent nothing since its `hello`, so the idle deadline
    // must reclaim it along with the megabytes its queue is holding. A stalled
    // write must not be able to outlive the deadline that exists to reclaim it.
    wait_until_deregistered(&relay, &here, "stalled").await;
}

#[tokio::test]
async fn a_peer_that_never_reads_its_ready_frame_is_abandoned() {
    // Same defect class as the test above, but before registration completes:
    // the `ready` write is the first thing the relay sends, and a peer that
    // never reads it must not park the task either.
    let here = room("ready-stall");
    let state = Arc::new(ServerState::with_deadlines(Deadlines {
        hello: Duration::from_millis(200),
        idle: Duration::from_secs(90),
    }));

    // A 16-byte buffer, so the ~22-byte `ready` frame cannot be handed over in
    // one write.
    let (server_io, client_io) = tokio::io::duplex(16);
    let (_shutdown, shutdown_rx) = watch::channel(false);
    let connection = tokio::spawn(relay::run_connection(
        server_io,
        Arc::clone(&state),
        shutdown_rx,
        "127.0.0.1:0".parse().expect("a literal loopback address"),
    ));

    let mut client = Client::new(client_io);
    client
        .send(&ClientFrame::Hello {
            protocol: PROTOCOL_VERSION,
            room: here.clone(),
            peer: "never-reads".to_owned(),
        })
        .await;

    tokio::time::timeout(Duration::from_secs(5), connection)
        .await
        .expect("the connection task must finish rather than park inside the ready write")
        .expect("the connection task did not panic");

    assert!(
        state.list_peers(&here).is_empty(),
        "an abandoned handshake must leave no registration behind"
    );
}

#[tokio::test]
async fn nothing_is_replayed_to_a_reconnecting_peer() {
    let relay = Relay::start().await;
    let here = room("no-replay");

    let mut sender = Client::join(&relay, &here, "sender").await;
    {
        let mut absent = Client::join(&relay, &here, "absent").await;
        absent.send(&ClientFrame::Ping).await;
        assert_eq!(absent.recv().await, ServerFrame::Pong);
    }
    wait_until_deregistered(&relay, &here, "absent").await;

    sender
        .send(&send("m1", "absent", "while you were out"))
        .await;
    assert_eq!(
        sender.recv().await,
        ServerFrame::Receipt {
            id: "m1".to_owned(),
            to: "absent".to_owned(),
            status: ReceiptStatus::PeerOffline,
        }
    );

    let mut reconnected = Client::join(&relay, &here, "absent").await;
    assert_eq!(
        reconnected.recv_within(QUIET).await,
        None,
        "a message refused while a peer was absent must never be replayed"
    );
}

// ---------------------------------------------------------------- violations

#[tokio::test]
async fn an_oversized_declared_length_is_rejected_before_its_payload() {
    let relay = Relay::start().await;
    let mut peer = Client::join(&relay, &room("oversized"), "peer").await;

    let declared = u32::try_from(MAX_FRAME_BYTES).expect("the cap fits in u32") + 1;
    // No payload follows: the rejection must come from the prefix alone, which
    // is what "without buffering the payload" means.
    peer.send_unframed(&length_prefix(declared)).await;

    peer.expect_error_then_close(ErrorCode::FrameTooLarge).await;
}

#[tokio::test]
async fn a_zero_length_frame_is_rejected() {
    let relay = Relay::start().await;
    let mut peer = Client::join(&relay, &room("zero-length"), "peer").await;

    peer.send_unframed(&length_prefix(0)).await;

    peer.expect_error_then_close(ErrorCode::MalformedFrame)
        .await;
}

#[tokio::test]
async fn a_truncated_frame_is_not_decoded() {
    let relay = Relay::start().await;
    let here = room("truncated");
    let mut peer = Client::join(&relay, &here, "peer").await;

    // A legal declared length whose payload never fully arrives. This is a
    // different path from the oversized declaration above: the length passes
    // the cap, so the codec waits for bytes that end instead.
    peer.send_unframed(&length_prefix(32)).await;
    peer.send_unframed(b"\x84\xa4type").await;
    peer.shutdown_write().await;

    // A partial payload must not be decoded into anything, and a transport
    // failure has no socket left to explain itself on, so no error frame is
    // expected -- only a close.
    peer.expect_closed().await;

    wait_until_deregistered(&relay, &here, "peer").await;
}

#[tokio::test]
async fn an_unknown_first_frame_type_is_rejected_as_an_invalid_hello() {
    #[derive(Serialize)]
    struct Broadcast<'a> {
        #[serde(rename = "type")]
        kind: &'a str,
        body: &'a str,
    }

    let relay = Relay::start().await;
    let mut client = Client::connect(&relay).await;

    let payload = protocol::encode(&Broadcast {
        kind: "broadcast",
        body: "before saying hello",
    })
    .expect("encode");
    client.send_payload(payload.to_vec()).await;

    // The handshake requirement takes precedence over the unknown-frame rule: a
    // connection with no room cannot process any frame but `hello`, so this
    // closes with `invalid_hello` rather than staying open with
    // `unsupported_frame` as it would after registration.
    client
        .expect_error_then_close(ErrorCode::InvalidHello)
        .await;
}

#[tokio::test]
async fn undecodable_and_non_map_payloads_are_rejected_as_malformed() {
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("corrupt bytes", vec![0xc1, 0x00, 0x00]),
        ("nil", vec![0xc0]),
        ("integer", vec![0x2a]),
        ("positional array", rmp_serde_positional_send()),
    ];

    for (label, payload) in cases {
        let relay = Relay::start().await;
        let mut peer = Client::join(&relay, &room("malformed"), "peer").await;

        peer.send_payload(payload).await;
        peer.expect_error_then_close(ErrorCode::MalformedFrame)
            .await;
        println!("{label} was rejected as malformed_frame");
    }
}

/// The positional encoding of a `send` frame: what a client using
/// `rmp_serde::to_vec` instead of `to_vec_named` would put on the wire.
fn rmp_serde_positional_send() -> Vec<u8> {
    rmp_serde::to_vec(&("send", "m1", "windows-main", "positional")).expect("encode")
}

#[tokio::test]
async fn an_unknown_frame_type_is_reported_and_the_connection_stays_open() {
    #[derive(Serialize)]
    struct Broadcast<'a> {
        #[serde(rename = "type")]
        kind: &'a str,
        body: &'a str,
    }

    let relay = Relay::start().await;
    let mut peer = Client::join(&relay, &room("unknown-type"), "peer").await;

    let payload = protocol::encode(&Broadcast {
        kind: "broadcast",
        body: "everyone",
    })
    .expect("encode");
    peer.send_payload(payload.to_vec()).await;

    match peer.recv().await {
        ServerFrame::Error { code, .. } => assert_eq!(code, ErrorCode::UnsupportedFrame),
        other => panic!("expected unsupported_frame, received {other:?}"),
    }

    peer.send(&ClientFrame::Ping).await;
    assert_eq!(
        peer.recv().await,
        ServerFrame::Pong,
        "an unknown frame type is recoverable and must not close the connection"
    );
}

/// A length-delimited payload declares exactly how many bytes the frame
/// occupies, so a complete frame plus anything else is not a frame. Serde's
/// deserializer stops at the end of the first value and does not care, which
/// made this the one malformed shape the relay accepted: the leading frame was
/// routed and the remainder discarded in silence.
///
/// It matters because a stricter decoder on the other side of the contract --
/// `@msgpack/msgpack` throws on extra bytes -- would reject exactly what this
/// relay accepted, and the two implementations would disagree about whether
/// the same bytes are a valid frame.
#[tokio::test]
async fn a_frame_followed_by_trailing_bytes_is_rejected() {
    for (label, trailer) in [
        ("a single junk byte", vec![0xc1u8]),
        ("a nil", vec![0xc0u8]),
        ("ascii garbage", b"NOT MESSAGEPACK".to_vec()),
        (
            "a second complete send frame",
            protocol::encode(&send("m2", "windows-main", "smuggled"))
                .expect("encode")
                .to_vec(),
        ),
    ] {
        let relay = Relay::start().await;
        let here = room("trailing-bytes");
        let mut peer = Client::join(&relay, &here, "macbook-reviewer").await;
        let mut recipient = Client::join(&relay, &here, "windows-main").await;

        let mut payload = protocol::encode(&send("m1", "windows-main", "leading"))
            .expect("encode")
            .to_vec();
        payload.extend_from_slice(&trailer);

        peer.send_payload(payload).await;
        peer.expect_error_then_close(ErrorCode::MalformedFrame)
            .await;

        assert_eq!(
            recipient.recv_within(QUIET).await,
            None,
            "the leading frame of a payload with {label} appended must not be \
             routed before the payload is rejected"
        );
    }
}

/// Builds a `send` payload of exactly `target` bytes by tuning `reply_to`,
/// leaving `body` empty so the body budget cannot be what rejects it.
fn send_payload_with_long_reply_to(target: usize, to: &str) -> Vec<u8> {
    let mut reply_len = target;

    for _ in 0..10 {
        let payload = protocol::encode(&ClientFrame::Send {
            id: "i".to_owned(),
            to: to.to_owned(),
            body: String::new(),
            reply_to: Some("r".repeat(reply_len)),
        })
        .expect("encode");

        match payload.len().cmp(&target) {
            Ordering::Equal => return payload.to_vec(),
            Ordering::Greater => reply_len -= payload.len() - target,
            Ordering::Less => reply_len += target - payload.len(),
        }
    }

    panic!("could not build a payload of exactly {target} bytes");
}

#[tokio::test]
async fn an_oversized_reply_to_cannot_close_the_recipient() {
    let relay = Relay::start().await;
    let here = room("reply-to-budget");

    let mut sender = Client::join(&relay, &here, "sender").await;
    let mut recipient = Client::join(&relay, &here, "recipient").await;

    // The body budget guards `body` only. An oversized `reply_to` with an empty
    // body passes every check, and the `message` built from it exceeds the frame
    // cap -- so the encode failure would land on the recipient.
    let payload = send_payload_with_long_reply_to(MAX_FRAME_BYTES, "recipient");
    assert_eq!(payload.len(), MAX_FRAME_BYTES, "fixture size");
    sender.send_payload(payload).await;

    match sender.recv().await {
        ServerFrame::Error { code, .. } => assert_eq!(
            code,
            ErrorCode::InvalidIdentifier,
            "an unrelayable reply_to must be refused at the sender"
        ),
        other => panic!("expected invalid_identifier, received {other:?}"),
    }

    assert_eq!(
        recipient.recv_within(QUIET).await,
        None,
        "nothing may be delivered"
    );
    recipient.send(&ClientFrame::Ping).await;
    assert_eq!(
        recipient.recv().await,
        ServerFrame::Pong,
        "one sender must not be able to close another peer's connection"
    );
}

/// A `hello` whose `protocol` is a string, so serde's type-mismatch message
/// quotes the value back. Quote characters double under `{:?}` escaping.
#[derive(Serialize)]
struct MalformedHello<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    protocol: &'a str,
}

#[tokio::test]
async fn an_oversized_diagnostic_still_states_its_cause() {
    let relay = Relay::start().await;
    let mut client = Client::connect(&relay).await;

    // Inside the inbound cap, but its escaped diagnostic is not: every `"`
    // becomes `\"`, so ~33 KiB in becomes ~66 KiB of error text.
    let quotes = "\"".repeat(33 * 1024);
    let payload = protocol::encode(&MalformedHello {
        kind: "hello",
        protocol: &quotes,
    })
    .expect("encode");
    assert!(payload.len() < MAX_FRAME_BYTES, "fixture must be accepted");
    client.send_payload(payload.to_vec()).await;

    // A close the relay initiates must state its cause. A diagnostic too large
    // to encode would leave the peer with a bare EOF, which is the outcome the
    // error-before-close rule exists to avoid.
    client
        .expect_error_then_close(ErrorCode::MalformedFrame)
        .await;
}

#[tokio::test]
async fn an_over_long_message_id_is_rejected_without_closing_the_connection() {
    let relay = Relay::start().await;
    let mut peer = Client::join(&relay, &room("long-ids"), "peer").await;

    peer.send(&send(
        &"i".repeat(protocol::MAX_CORRELATION_BYTES + 1),
        "peer",
        "hello",
    ))
    .await;

    match peer.recv().await {
        ServerFrame::Error { code, .. } => assert_eq!(code, ErrorCode::InvalidIdentifier),
        other => panic!("expected invalid_identifier, received {other:?}"),
    }

    peer.send(&ClientFrame::Ping).await;
    assert_eq!(peer.recv().await, ServerFrame::Pong);
}

// ------------------------------------------------------------- frame budget

/// Builds a `send` payload of exactly `target` bytes by tuning the body length.
fn send_payload_of_exact_size(target: usize, to: &str) -> Vec<u8> {
    let mut body_len = target;

    for _ in 0..10 {
        let payload = protocol::encode(&ClientFrame::Send {
            id: "max".to_owned(),
            to: to.to_owned(),
            body: "x".repeat(body_len),
            reply_to: None,
        })
        .expect("encode");

        match payload.len().cmp(&target) {
            Ordering::Equal => return payload.to_vec(),
            Ordering::Greater => body_len -= payload.len() - target,
            Ordering::Less => body_len += target - payload.len(),
        }
    }

    panic!("could not build a payload of exactly {target} bytes");
}

#[tokio::test]
async fn a_frame_at_the_cap_is_accepted_by_the_framing_layer() {
    let relay = Relay::start().await;
    let mut peer = Client::join(&relay, &room("max-frame"), "peer").await;

    // Exactly at the cap: accepted by the framing layer, then refused by the
    // body budget, because a `message` built from it would not fit.
    let payload = send_payload_of_exact_size(MAX_FRAME_BYTES, "peer");
    assert_eq!(payload.len(), MAX_FRAME_BYTES, "fixture size");
    peer.send_payload(payload).await;

    peer.expect_error_then_close(ErrorCode::FrameTooLarge).await;
}

#[tokio::test]
async fn a_body_at_the_relayable_budget_is_delivered_intact() {
    let relay = Relay::start().await;
    let here = room("body-budget");

    let mut sender = Client::join(&relay, &here, "sender").await;
    let mut recipient = Client::join(&relay, &here, "recipient").await;

    let body = "b".repeat(MAX_BODY_BYTES);
    sender
        .send(&ClientFrame::Send {
            id: "i".repeat(protocol::MAX_CORRELATION_BYTES),
            to: "recipient".to_owned(),
            body: body.clone(),
            reply_to: Some("r".repeat(protocol::MAX_CORRELATION_BYTES)),
        })
        .await;

    match sender.recv().await {
        ServerFrame::Receipt { status, .. } => assert_eq!(
            status,
            ReceiptStatus::Routed,
            "a body at the budget must be relayable"
        ),
        other => panic!("expected a receipt, received {other:?}"),
    }

    match recipient.recv().await {
        ServerFrame::Message {
            body: delivered, ..
        } => assert_eq!(
            delivered.len(),
            body.len(),
            "the delivered body must be intact"
        ),
        other => panic!("expected a message, received {other:?}"),
    }
}

#[tokio::test]
async fn a_body_one_byte_over_the_budget_is_refused_and_the_recipient_survives() {
    let relay = Relay::start().await;
    let here = room("over-budget");

    let mut sender = Client::join(&relay, &here, "sender").await;
    let mut recipient = Client::join(&relay, &here, "recipient").await;

    sender
        .send(&ClientFrame::Send {
            id: "m1".to_owned(),
            to: "recipient".to_owned(),
            body: "b".repeat(MAX_BODY_BYTES + 1),
            reply_to: None,
        })
        .await;

    sender
        .expect_error_then_close(ErrorCode::FrameTooLarge)
        .await;

    // The point of the budget: the sender's mistake must not reach, or close,
    // the recipient's connection.
    assert_eq!(
        recipient.recv_within(QUIET).await,
        None,
        "an over-budget body must not be delivered"
    );
    recipient.send(&ClientFrame::Ping).await;
    assert_eq!(
        recipient.recv().await,
        ServerFrame::Pong,
        "the recipient's connection must survive an over-budget send"
    );
}

// ------------------------------------------------------- lifecycle and seam

#[tokio::test]
async fn shutdown_closes_live_connections() {
    let mut relay = Relay::start().await;
    let mut peer = Client::join(&relay, &room("shutdown"), "peer").await;

    relay.shutdown().await;

    peer.expect_closed().await;
}

#[tokio::test]
async fn the_connection_task_runs_over_any_byte_stream() {
    // The transport seam, exercised without a socket: `run_connection` is
    // generic so a later change can pass a TLS stream. An in-memory duplex is
    // proof the generic parameter is real and not merely declared.
    let state = Arc::new(ServerState::new());
    let (server_io, client_io) = tokio::io::duplex(64 * 1024);
    let (_shutdown, shutdown_rx) = watch::channel(false);

    let peer_addr = "127.0.0.1:0".parse().expect("a literal loopback address");
    let connection = tokio::spawn(relay::run_connection(
        server_io,
        Arc::clone(&state),
        shutdown_rx,
        peer_addr,
    ));

    let mut client = Client::new(client_io);
    let here = room("duplex");
    client.handshake(&here, "in-memory").await;

    client.send(&send("m1", "in-memory", "loopback")).await;
    let frames = [client.recv().await, client.recv().await];
    assert!(
        frames.contains(&routed("m1", "in-memory")),
        "expected a routed receipt among {frames:?}"
    );

    drop(client);
    connection.await.expect("the connection task finishes");
    assert!(
        state.list_peers(&here).is_empty(),
        "the connection task must deregister when its stream ends"
    );
}
