//! Message bodies must never reach a log record.
//!
//! This is its own test binary because it installs a global `tracing`
//! subscriber, which is process-wide and must not race another test's
//! expectations. It contains exactly one test for the same reason.

mod support;

use std::io;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use omp_relayd::blob;
use omp_relayd::protocol::{
    self, ClientFrame, ErrorCode, ReceiptStatus, ReserveStatus, ServerFrame,
};
use serde::Serialize;
use tracing_subscriber::fmt::MakeWriter;

use support::{Client, Relay, blob_path, http, http_put, room};

/// A string that must not appear in any log record.
const CANARY: &str = "SECRET-CANARY";

/// Collects every emitted log record in memory.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn contents(&self) -> String {
        let bytes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

impl io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl MakeWriter<'_> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&self) -> Self::Writer {
        self.clone()
    }
}

/// Asserts every log path under test actually emitted a record.
///
/// Separate from the canary assertion, and run before it: a capture that missed
/// a path would let the canary check pass by having nothing to find, which is
/// the one way this test could be green and worthless.
fn assert_every_log_path_was_exercised(captured: &str, lines: usize, transferred: &Transferred) {
    for expected in [
        "peer registered",
        "message routed",
        "message not delivered",
        "recipient queue full",
        "frame decode failed",
        "announcement fanned out",
        "announcement shed by a recipient queue",
        "reservation granted",
        "payload stored",
    ] {
        assert!(
            captured.contains(expected),
            "expected a {expected:?} record among the {lines} captured lines; \
             without it this test would not have exercised that log path"
        );
    }

    // The transfer's records carry the address and the byte count, which is what
    // the requirement asks them to carry *instead* of the content.
    let stored = captured
        .lines()
        .find(|line| line.contains("payload stored"))
        .expect("the upload was exercised above");
    assert!(
        stored.contains(&transferred.digest) && stored.contains(&transferred.bytes.to_string()),
        "the upload record must name the digest and the byte count; observed: {stored}"
    );
}

/// A `send` whose `protocol` field is a string, so serde's type-mismatch error
/// carries the offending value. A relay that logged that error text verbatim
/// would leak the payload and emit an unbounded log record.
#[derive(Serialize)]
struct MalformedHello<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    protocol: &'a str,
}

#[tokio::test]
async fn no_log_record_carries_a_message_body() {
    let logs = CapturedLogs::default();
    tracing_subscriber::fmt()
        .with_writer(logs.clone())
        // TRACE so that even the debug-level "message routed" record is
        // emitted: a test that captured nothing would pass vacuously.
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .init();

    // Store-backed, because the payload transfer routes are part of the surface
    // under test: a relay started without a store would refuse every upload and
    // the transfer exercise below would pass without touching `http.rs`.
    let relay = Relay::with_store("redaction").await;
    let here = room("redaction");

    let mut sender = Client::join(&relay, &here, "sender").await;
    let mut recipient = Client::join(&relay, &here, "recipient").await;

    exercise_a_routed_body(&mut sender, &mut recipient).await;
    exercise_an_undeliverable_body(&mut sender).await;
    exercise_an_announced_body(&mut sender).await;
    let transferred = exercise_a_transferred_payload(&relay, &here, &mut sender).await;
    // The stalled peer is kept alive by the caller: its queue stays full, which
    // is what lets the announcement below take the shed path.
    let (backpressured_at, _stalled) =
        exercise_a_backpressured_body(&relay, &here, &mut sender).await;
    // A third reading recipient, so the announcement below fans out to three
    // peers: this one, `recipient`, and the stalled one. Kept alive for the same
    // reason as `_stalled`: dropping it deregisters the peer, and the fanout
    // would see two recipients instead of the three the scenario specifies.
    let _reading = Client::join(&relay, &here, "reading").await;
    exercise_a_shed_announcement(&mut sender).await;
    exercise_a_malformed_frame(&relay).await;

    // Let the connection-close records be written.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let captured = logs.contents();
    let lines = captured.lines().count();

    assert_every_log_path_was_exercised(&captured, lines, &transferred);

    let decode_record = captured
        .lines()
        .find(|line| line.contains("frame decode failed"))
        .expect("a decode failure was exercised above");
    assert!(
        decode_record.contains("undecodable"),
        "the decode failure must be logged as a classification, not as the serde \
         error text that quotes the payload; observed: {decode_record}"
    );

    // One record per announcement, never one per recipient, so the count is the
    // assertion: a fanout over three recipients of which one was not reading
    // must leave exactly one shed record behind.
    let shed_records: Vec<&str> = captured
        .lines()
        .filter(|line| line.contains("announcement shed by a recipient queue"))
        .collect();
    assert_eq!(
        shed_records.len(),
        1,
        "one announcement must emit one shed record; observed {} among the {lines} \
         captured lines",
        shed_records.len()
    );
    let shed_record = shed_records[0];
    for (name, expected) in [
        ("room", "omp-relayd/redaction"),
        ("from", "sender"),
        ("id", "a2"),
        ("delivered", "2"),
        ("shed", "1"),
    ] {
        assert_eq!(
            log_field(shed_record, name),
            Some(expected),
            "the shed record must report {name} as {expected:?}; observed {:?} in \
             {shed_record}",
            log_field(shed_record, name)
        );
    }
    let fanned_out = captured
        .lines()
        .filter(|line| line.contains("announcement fanned out"))
        .count();
    assert_eq!(
        fanned_out, 1,
        "the announcement that shed nothing must emit exactly one record; observed \
         {fanned_out} among the {lines} captured lines"
    );

    let longest = captured.lines().map(str::len).max().unwrap_or(0);
    assert!(
        !captured.contains(CANARY),
        "a log record carried the message body; {lines} lines, longest {longest} bytes"
    );
    assert!(
        longest < 1024,
        "a log record grew to {longest} bytes, which means untrusted input is \
         reaching log output unbounded"
    );

    println!(
        "captured {} bytes over {lines} log records, longest {longest} bytes, \
         no occurrence of {CANARY}; backpressure at attempt {backpressured_at}",
        captured.len()
    );
}

/// Reads one `name=value` field out of a formatted log record.
///
/// Values are compared rather than substring-matched, because
/// `contains("shed=1")` would also accept a record reporting `shed=10`. Quotes
/// are trimmed because the `fmt` layer renders a string field with them and a
/// numeric field without.
fn log_field<'a>(record: &'a str, name: &str) -> Option<&'a str> {
    record
        .split_whitespace()
        .find_map(|token| {
            token
                .strip_prefix(name)
                .and_then(|rest| rest.strip_prefix('='))
        })
        .map(|value| value.trim_matches('"'))
}

/// A body that is routed successfully, which takes the debug-level path.
async fn exercise_a_routed_body(
    sender: &mut Client<tokio::net::TcpStream>,
    recipient: &mut Client<tokio::net::TcpStream>,
) {
    let body = format!("please review {CANARY} before merging");
    sender
        .send(&ClientFrame::Send {
            id: "m1".to_owned(),
            to: "recipient".to_owned(),
            body: body.clone(),
            reply_to: None,
            attachment: None,
        })
        .await;

    assert!(matches!(
        sender.recv().await,
        ServerFrame::Receipt {
            status: ReceiptStatus::Routed,
            ..
        }
    ));
    assert_eq!(
        recipient.recv().await,
        ServerFrame::Message {
            id: "m1".to_owned(),
            from: "sender".to_owned(),
            body,
            reply_to: None,
            attachment: None,
        },
        "the body must be delivered intact even though it is never logged"
    );
}

/// A body nobody can receive, which takes the `peer_offline` path.
async fn exercise_an_undeliverable_body(sender: &mut Client<tokio::net::TcpStream>) {
    sender
        .send(&ClientFrame::Send {
            id: "m2".to_owned(),
            to: "ghost".to_owned(),
            body: format!("nobody will read {CANARY}"),
            reply_to: None,
            attachment: None,
        })
        .await;

    assert!(matches!(
        sender.recv().await,
        ServerFrame::Receipt {
            status: ReceiptStatus::PeerOffline,
            ..
        }
    ));
}

/// An announced body that reaches every other peer, which takes the
/// debug-level fanout path.
async fn exercise_an_announced_body(announcer: &mut Client<tokio::net::TcpStream>) {
    announcer
        .send(&ClientFrame::Announce {
            id: "a1".to_owned(),
            body: format!("the room should know about {CANARY}"),
            reply_to: None,
            attachment: None,
        })
        .await;

    match announcer.recv().await {
        ServerFrame::Accepted {
            delivered, shed, ..
        } => {
            assert!(
                delivered > 0 && shed == 0,
                "the fanout log path under test is the one with nothing shed; observed \
                 {delivered} delivered and {shed} shed"
            );
        }
        other => panic!("expected an acceptance, received {other:?}"),
    }
}

/// What one exercised transfer put on the wire, for the assertions to name.
struct Transferred {
    digest: String,
    bytes: usize,
}

/// A payload uploaded and fetched over the transfer routes.
///
/// The reservation goes through a frame rather than the store's own API, so the
/// `reserve` path's log record is exercised too: it names a digest and a byte
/// count, and a relay that named the payload instead would fail the canary
/// assertion from this record rather than from `http.rs`.
async fn exercise_a_transferred_payload(
    relay: &Relay,
    here: &omp_relayd::protocol::RoomId,
    sender: &mut Client<tokio::net::TcpStream>,
) -> Transferred {
    // Large enough to cross more than one read, so the mid-transfer log paths
    // are reachable, and carrying the canary throughout rather than once.
    let payload = format!("{CANARY} in a stored payload\n").repeat(4096);
    let payload = payload.as_bytes();
    let digest = blob::digest(payload);

    sender
        .send(&ClientFrame::Reserve {
            request_id: "res-canary".to_owned(),
            digest: digest.clone(),
            bytes: payload.len() as u64,
        })
        .await;
    match sender.recv().await {
        ServerFrame::Reserved { status, .. } => assert_eq!(
            status,
            ReserveStatus::Granted,
            "the reservation under test must be granted, or no upload follows"
        ),
        other => panic!("expected a reservation reply, received {other:?}"),
    }

    let path = blob_path(here, &digest);
    let stored = http_put(relay.addr, &path, payload).await;
    assert_eq!(stored.status, 201, "the upload must be accepted");

    let fetched = http(relay.addr, "GET", &path, &[], &[]).await;
    assert_eq!(fetched.status, 200, "the fetch must be answered");
    assert_eq!(
        fetched.body, payload,
        "the fetched bytes must be the ones uploaded, or the canary never travelled"
    );

    Transferred {
        digest,
        bytes: payload.len(),
    }
}

/// An announced body one of whose recipients is not reading, which takes the
/// warn-level fanout path.
async fn exercise_a_shed_announcement(announcer: &mut Client<tokio::net::TcpStream>) {
    announcer
        .send(&ClientFrame::Announce {
            id: "a2".to_owned(),
            body: format!("one of you is stalled, and {CANARY} is why"),
            reply_to: None,
            attachment: None,
        })
        .await;

    match announcer.recv().await {
        ServerFrame::Accepted {
            delivered, shed, ..
        } => {
            assert!(
                delivered == 2 && shed == 1,
                "the premise is three recipients of which one is not reading, so the \
                 warn-level record is emitted; observed {delivered} delivered and \
                 {shed} shed"
            );
        }
        other => panic!("expected an acceptance, received {other:?}"),
    }
}

/// Bodies that fill a stalled peer's queue, which takes the warn-level path.
///
/// Returns the attempt at which backpressure appeared, and the stalled client
/// itself: dropping it here would deregister the peer and empty the queue the
/// announcement path exercised next depends on.
async fn exercise_a_backpressured_body(
    relay: &Relay,
    here: &omp_relayd::protocol::RoomId,
    sender: &mut Client<tokio::net::TcpStream>,
) -> (u32, Client<tokio::net::TcpStream>) {
    let body = format!("{CANARY}{}", "x".repeat(32 * 1024));
    let stalled = Client::join(relay, here, "stalled").await;

    for attempt in 1..=2000 {
        sender
            .send(&ClientFrame::Send {
                id: format!("b{attempt}"),
                to: "stalled".to_owned(),
                body: body.clone(),
                reply_to: None,
                attachment: None,
            })
            .await;

        if let ServerFrame::Receipt {
            status: ReceiptStatus::RecipientBackpressure,
            ..
        } = sender.recv().await
        {
            return (attempt, stalled);
        }
    }

    panic!("a stalled recipient must backpressure within 2000 sends");
}

/// A frame whose decode error quotes the payload, which is the log path that
/// would leak untrusted bytes if the error text were logged verbatim.
async fn exercise_a_malformed_frame(relay: &Relay) {
    let mut client = Client::connect(relay).await;
    let leaky = format!("{CANARY}{}", "y".repeat(4096));

    let payload = protocol::encode(&MalformedHello {
        kind: "hello",
        protocol: &leaky,
    })
    .expect("encode");
    client.send_payload(payload.to_vec()).await;

    client
        .expect_error_then_close(ErrorCode::MalformedFrame)
        .await;
}
