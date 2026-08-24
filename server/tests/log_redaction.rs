//! Message bodies must never reach a log record.
//!
//! This is its own test binary because it installs a global `tracing`
//! subscriber, which is process-wide and must not race another test's
//! expectations. It contains exactly one test for the same reason.

mod support;

use std::io;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use omp_relayd::protocol::{self, ClientFrame, ErrorCode, ReceiptStatus, ServerFrame};
use serde::Serialize;
use tracing_subscriber::fmt::MakeWriter;

use support::{Client, Relay, room};

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

    let relay = Relay::start().await;
    let here = room("redaction");

    let mut sender = Client::join(&relay, &here, "sender").await;
    let mut recipient = Client::join(&relay, &here, "recipient").await;

    exercise_a_routed_body(&mut sender, &mut recipient).await;
    exercise_an_undeliverable_body(&mut sender).await;
    exercise_an_announced_body(&mut sender).await;
    // The stalled peer is kept alive by the caller: its queue stays full, which
    // is what lets the announcement below take the shed path.
    let (backpressured_at, _stalled) =
        exercise_a_backpressured_body(&relay, &here, &mut sender).await;
    exercise_a_shed_announcement(&mut sender).await;
    exercise_a_malformed_frame(&relay).await;

    // Let the connection-close records be written.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let captured = logs.contents();
    let lines = captured.lines().count();

    // The capture is working: if it were not, the canary assertion below would
    // pass without proving anything.
    for expected in [
        "peer registered",
        "message routed",
        "message not delivered",
        "recipient queue full",
        "frame decode failed",
        "announcement fanned out",
        "announcement shed by a recipient queue",
    ] {
        assert!(
            captured.contains(expected),
            "expected a {expected:?} record among the {lines} captured lines; \
             without it this test would not have exercised that log path"
        );
    }
    let decode_record = captured
        .lines()
        .find(|line| line.contains("frame decode failed"))
        .expect("a decode failure was exercised above");
    assert!(
        decode_record.contains("undecodable"),
        "the decode failure must be logged as a classification, not as the serde \
         error text that quotes the payload; observed: {decode_record}"
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

/// An announced body one of whose recipients is not reading, which takes the
/// warn-level fanout path.
async fn exercise_a_shed_announcement(announcer: &mut Client<tokio::net::TcpStream>) {
    announcer
        .send(&ClientFrame::Announce {
            id: "a2".to_owned(),
            body: format!("one of you is stalled, and {CANARY} is why"),
            reply_to: None,
        })
        .await;

    match announcer.recv().await {
        ServerFrame::Accepted { shed, .. } => {
            assert!(
                shed > 0,
                "the premise is a recipient whose queue is full, so the warn-level record \
                 is emitted; observed {shed} shed"
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
