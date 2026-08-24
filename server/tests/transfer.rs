//! Payload transfer against a real socket: one port speaking two protocols, the
//! routes, and the rules that only a real transfer can show.
//!
//! Reservations here are made through the store's own API rather than through a
//! `reserve` frame, which does not exist yet. That keeps this file about the
//! transfer surface: the frame and its authorization arrive in the next step and
//! bring their own tests.

mod support;

use std::time::{Duration, Instant};

use omp_relayd::blob::{self, MAX_PAYLOAD_BYTES, MAX_ROOM_BYTES};
use omp_relayd::http::{BODY_PROGRESS_TIMEOUT, HEADER_READ_TIMEOUT};
use omp_relayd::protocol::{ClientFrame, ErrorCode, MAX_FRAME_BYTES, ServerFrame};
use omp_relayd::relay::Deadlines;
use support::{Client, Relay, blob_path, http, http_put, http_put_declaring, length_prefix, room};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// A payload larger than one write, so the streaming paths are what run.
///
/// A byte pattern with a period that is not a power of two, so a truncated or
/// reordered transfer is visible rather than accidentally equal.
fn payload(bytes: usize) -> Vec<u8> {
    (0..bytes)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect()
}

/// Every file under the store's root, as absolute paths.
fn files_under(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

/// Waits until the store has opened a temporary file for an upload in flight.
///
/// The store opens it on the first chunk written, so its appearance is the
/// observable "the relay has begun consuming this upload" that a test asserting
/// mid-upload behaviour needs.
async fn wait_for_a_partial_file(relay: &Relay) {
    for _ in 0..200 {
        if !files_under(relay.store().root()).is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the relay never began writing the upload");
}

// 2.6: both protocols on one port.
#[tokio::test]
async fn a_frame_connection_and_a_payload_fetch_share_the_port() {
    let relay = Relay::with_store("share-the-port").await;
    let room = room("share-the-port");

    // A live frame connection, so the fetch below is served beside it rather
    // than instead of it.
    let mut peer = Client::join(&relay, &room, "macbook").await;

    let bytes = payload(300_000);
    let digest = blob::digest(&bytes);
    relay.reserve(&room, &digest, bytes.len() as u64);

    let path = blob_path(&room, &digest);
    let stored = http_put(relay.addr, &path, &bytes).await;
    assert_eq!(stored.status, 201, "upload was not created: {stored:?}");

    let fetched = http(relay.addr, "GET", &path, &[], &[]).await;
    assert_eq!(fetched.status, 200, "fetch failed: {fetched:?}");
    assert_eq!(fetched.body, bytes, "the fetched bytes differ");
    assert_eq!(
        fetched.header("content-length"),
        Some(bytes.len().to_string().as_str())
    );

    // The frame connection is untouched by any of it.
    peer.send(&ClientFrame::Ping).await;
    assert_eq!(
        peer.recv().await,
        ServerFrame::Pong,
        "the frame connection did not survive a transfer on the same port"
    );
}

// 2.6: a fetch is not answered as an over-long frame, which is what this port
// did before the discrimination existed.
#[tokio::test]
async fn a_fetch_is_not_answered_as_an_over_long_frame() {
    let relay = Relay::with_store("not-a-frame").await;
    let room = room("not-a-frame");
    let digest = blob::digest(b"never uploaded");

    let response = http(relay.addr, "GET", &blob_path(&room, &digest), &[], &[]).await;

    // 404 rather than 200 because nothing was uploaded -- but an *HTTP* 404,
    // which is the point: before this change the same bytes declared a frame
    // length of 0x47455420 and were answered `frame_too_large`.
    assert_eq!(
        response.status, 404,
        "expected an HTTP answer: {response:?}"
    );
    assert!(
        !response.body.windows(5).any(|w| w == b"frame"),
        "the answer looks like a frame-protocol error: {response:?}"
    );
}

// 2.6: the only input near the discrimination boundary.
#[tokio::test]
async fn a_frame_at_the_size_cap_is_still_read_as_a_frame() {
    let relay = Relay::with_store("frame-at-the-cap").await;
    let mut stream = TcpStream::connect(relay.addr)
        .await
        .expect("connect to the relay");

    // Exactly the cap: 65536 = 0x00010000, whose leading byte is zero. The
    // payload is 65536 MessagePack `nil`s, so the frame is read and then
    // rejected as not-a-map -- and being rejected *as a frame* is the assertion.
    let declared = u32::try_from(MAX_FRAME_BYTES).expect("the cap fits in u32");
    stream
        .write_all(&length_prefix(declared))
        .await
        .expect("write the length prefix");
    stream
        .write_all(&vec![0xc0_u8; MAX_FRAME_BYTES])
        .await
        .expect("write the payload");
    stream.flush().await.expect("flush");

    let mut client = Client::new(stream);
    client
        .expect_error_then_close(ErrorCode::MalformedFrame)
        .await;
}

// 2.7: payload I/O never runs on a frame-reading task, asserted rather than
// argued.
#[tokio::test]
async fn a_maximal_upload_does_not_cost_a_heartbeating_peer_its_connection() {
    // An idle deadline far shorter than the upload below takes, so a transfer
    // that blocked the frame reader would be observable as a disconnection
    // rather than only as latency.
    let relay = Relay::with_store_and_deadlines(
        "maximal-upload",
        Deadlines {
            hello: Duration::from_secs(5),
            idle: Duration::from_millis(400),
        },
    )
    .await;
    let room = room("maximal-upload");

    let mut peer = Client::join(&relay, &room, "macbook").await;

    let bytes = payload(usize::try_from(MAX_PAYLOAD_BYTES).expect("the maximum fits in usize"));
    let digest = blob::digest(&bytes);
    relay.reserve(&room, &digest, MAX_PAYLOAD_BYTES);
    let path = blob_path(&room, &digest);

    // Written in chunks with a pause between them, so the transfer occupies more
    // than one idle deadline. On loopback an unthrottled 4 MiB upload finishes
    // in milliseconds and would prove nothing about the deadline.
    let addr = relay.addr;
    let uploader = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let head = format!(
            "PUT {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
            bytes.len()
        );
        stream.write_all(head.as_bytes()).await.expect("head");
        for chunk in bytes.chunks(64 * 1024) {
            stream.write_all(chunk).await.expect("chunk");
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        stream.flush().await.expect("flush");
        // Read the answer so the response is not lost to a close.
        let mut raw = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut raw)
            .await
            .expect("response");
        String::from_utf8_lossy(&raw[..raw.len().min(32)]).to_string()
    });

    // Heartbeats within the deadline, for longer than the deadline, while the
    // transfer runs.
    let mut pongs = 0_u32;
    for _ in 0..8 {
        peer.send(&ClientFrame::Ping).await;
        assert_eq!(
            peer.recv().await,
            ServerFrame::Pong,
            "a heartbeating peer lost its connection during a maximal upload"
        );
        pongs += 1;
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    let status = uploader.await.expect("the uploader did not panic");
    assert!(
        status.contains("201"),
        "the upload did not complete: {status:?}"
    );
    assert_eq!(pongs, 8, "not every heartbeat was answered");
    assert_eq!(
        relay.state.list_peers(&room),
        vec!["macbook".to_owned()],
        "the heartbeating peer was deregistered"
    );
    assert_eq!(
        relay.store().payload_len(&room, &digest),
        Some(MAX_PAYLOAD_BYTES),
        "the maximal payload was not stored"
    );
}

// 2.8: a fetch never observes a partial payload.
#[tokio::test]
async fn a_fetch_during_an_upload_reports_the_payload_absent() {
    let relay = Relay::with_store("fetch-during-upload").await;
    let room = room("fetch-during-upload");

    let bytes = payload(400_000);
    let digest = blob::digest(&bytes);
    relay.reserve(&room, &digest, bytes.len() as u64);
    let path = blob_path(&room, &digest);

    // A connection that has sent its head and half its body, and is holding.
    let mut upload = TcpStream::connect(relay.addr).await.expect("connect");
    let head = format!(
        "PUT {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        relay.addr,
        bytes.len()
    );
    upload.write_all(head.as_bytes()).await.expect("head");
    upload
        .write_all(&bytes[..200_000])
        .await
        .expect("half the body");
    upload.flush().await.expect("flush");

    // A real synchronization point rather than a sleep: the store opens its
    // temporary file on the first chunk it writes, so a file under the store
    // root means the relay has begun consuming this upload. Without it the
    // assertions below could pass against an upload the relay had not started.
    wait_for_a_partial_file(&relay).await;

    let fetched = http(relay.addr, "GET", &path, &[], &[]).await;
    assert_eq!(
        fetched.status, 404,
        "a fetch observed an upload in progress: {fetched:?}"
    );
    let peeked = http(relay.addr, "HEAD", &path, &[], &[]).await;
    assert_eq!(
        peeked.status, 404,
        "a length request observed an upload in progress: {peeked:?}"
    );

    // Finish it, so the test also shows that the same upload was genuinely live
    // rather than already refused.
    upload
        .write_all(&bytes[200_000..])
        .await
        .expect("the rest of the body");
    upload.flush().await.expect("flush");
    let mut raw = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut upload, &mut raw)
        .await
        .expect("response");
    assert!(
        String::from_utf8_lossy(&raw).contains("201"),
        "the upload did not complete after the fetch"
    );
    assert_eq!(
        relay.store().payload_len(&room, &digest),
        Some(bytes.len() as u64)
    );
}

// 2.8: an upload whose connection closes partway leaves nothing fetchable and
// returns its allowance.
#[tokio::test]
async fn an_abandoned_upload_leaves_nothing_and_returns_its_allowance() {
    let mut relay = Relay::with_store("abandoned-upload").await;
    let room = room("abandoned-upload");

    let bytes = payload(usize::try_from(MAX_PAYLOAD_BYTES).expect("fits"));
    let digest = blob::digest(&bytes);
    relay.reserve(&room, &digest, MAX_PAYLOAD_BYTES);
    let path = blob_path(&room, &digest);

    // The rest of the room's ceiling, so the probe below can only succeed once
    // the abandoned upload gives its own allowance back. Without this the probe
    // succeeds immediately against a nearly empty room and proves nothing.
    let units = MAX_ROOM_BYTES / MAX_PAYLOAD_BYTES;
    for n in 1..units {
        relay.reserve(&room, &blob::digest(&n.to_le_bytes()), MAX_PAYLOAD_BYTES);
    }
    assert!(
        relay
            .store()
            .reserve(&room, &blob::digest(b"probe"), MAX_PAYLOAD_BYTES)
            .is_err(),
        "the room was not full before the upload was abandoned"
    );

    {
        let mut upload = TcpStream::connect(relay.addr).await.expect("connect");
        let head = format!(
            "PUT {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
            relay.addr,
            bytes.len()
        );
        upload.write_all(head.as_bytes()).await.expect("head");
        upload
            .write_all(&bytes[..128 * 1024])
            .await
            .expect("prefix");
        upload.flush().await.expect("flush");
        // The upload must be genuinely in flight before it is abandoned, or
        // this test would assert cleanup of something that never started.
        wait_for_a_partial_file(&relay).await;
        // Dropped without the rest: the client went away.
    }

    // The relay notices the close on its own task, so this is waited for rather
    // than assumed.
    let mut released = false;
    for _ in 0..200 {
        if relay
            .store()
            .reserve(&room, &blob::digest(b"probe"), MAX_PAYLOAD_BYTES)
            .is_ok()
        {
            released = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(
        relay.store().payload_len(&room, &digest),
        None,
        "an abandoned upload became fetchable"
    );
    assert!(
        released,
        "the abandoned upload's allowance was never returned"
    );

    relay.drain_removals().await;
    let leftovers = files_under(relay.store().root());
    assert!(
        leftovers.is_empty(),
        "the abandoned upload left {leftovers:?} on disk"
    );
}

#[tokio::test]
async fn an_upload_with_no_reservation_is_refused() {
    let relay = Relay::with_store("unreserved-upload").await;
    let room = room("unreserved-upload");
    let bytes = b"never reserved".to_vec();
    let digest = blob::digest(&bytes);

    let response = http_put(relay.addr, &blob_path(&room, &digest), &bytes).await;
    assert_eq!(response.status, 403, "expected a refusal: {response:?}");
    assert_eq!(
        relay.store().payload_len(&room, &digest),
        None,
        "an unreserved upload was stored"
    );
}

#[tokio::test]
async fn an_upload_over_its_reservation_is_refused() {
    let relay = Relay::with_store("over-reservation").await;
    let room = room("over-reservation");
    let bytes = payload(4096);
    let digest = blob::digest(&bytes);
    relay.reserve(&room, &digest, 1024);

    let response = http_put(relay.addr, &blob_path(&room, &digest), &bytes).await;
    assert_eq!(response.status, 413, "expected a refusal: {response:?}");
    assert_eq!(relay.store().payload_len(&room, &digest), None);
}

#[tokio::test]
async fn an_upload_whose_bytes_do_not_match_its_address_is_refused() {
    let relay = Relay::with_store("digest-mismatch").await;
    let room = room("digest-mismatch");
    let claimed = blob::digest(b"what the uploader claims");
    let sent = b"something else entirely".to_vec();
    relay.reserve(&room, &claimed, sent.len() as u64);

    let response = http_put(relay.addr, &blob_path(&room, &claimed), &sent).await;
    assert_eq!(response.status, 400, "expected a refusal: {response:?}");
    assert_eq!(
        relay.store().payload_len(&room, &claimed),
        None,
        "a mismatched upload became fetchable"
    );
}

#[tokio::test]
async fn an_upload_declaring_more_than_it_sends_is_refused() {
    let relay = Relay::with_store("length-mismatch").await;
    let room = room("length-mismatch");
    let bytes = payload(2048);
    let digest = blob::digest(&bytes);
    relay.reserve(&room, &digest, bytes.len() as u64);

    // Declares the whole payload, sends half, then closes.
    let response = http_put_declaring(
        relay.addr,
        &blob_path(&room, &digest),
        bytes.len(),
        &bytes[..1024],
    )
    .await;
    assert_eq!(
        response.status, 400,
        "a truncated upload must be told why rather than closed on: {response:?}"
    );
    assert_eq!(relay.store().payload_len(&room, &digest), None);
}

#[tokio::test]
async fn re_uploading_a_held_payload_is_answered_without_a_rewrite() {
    let relay = Relay::with_store("already-held").await;
    let room = room("already-held");
    let bytes = payload(8192);
    let digest = blob::digest(&bytes);
    let path = blob_path(&room, &digest);

    relay.reserve(&room, &digest, bytes.len() as u64);
    assert_eq!(http_put(relay.addr, &path, &bytes).await.status, 201);

    relay.reserve(&room, &digest, bytes.len() as u64);
    let again = http_put(relay.addr, &path, &bytes).await;
    assert_eq!(
        again.status, 204,
        "a retried upload was not accepted: {again:?}"
    );
    assert!(again.body.is_empty(), "a 204 carried a body");
}

#[tokio::test]
async fn a_length_request_transfers_no_payload() {
    let relay = Relay::with_store("length-only").await;
    let room = room("length-only");
    let bytes = payload(50_000);
    let digest = blob::digest(&bytes);
    let path = blob_path(&room, &digest);
    relay.reserve(&room, &digest, bytes.len() as u64);
    assert_eq!(http_put(relay.addr, &path, &bytes).await.status, 201);

    let peeked = http(relay.addr, "HEAD", &path, &[], &[]).await;
    assert_eq!(peeked.status, 200, "{peeked:?}");
    assert_eq!(
        peeked.header("content-length"),
        Some(bytes.len().to_string().as_str()),
        "the length was not reported"
    );
    assert!(peeked.body.is_empty(), "a length request carried a body");
}

#[tokio::test]
async fn a_fetch_is_immutably_cacheable_and_carries_no_validator() {
    let relay = Relay::with_store("cacheable").await;
    let room = room("cacheable");
    let bytes = payload(1024);
    let digest = blob::digest(&bytes);
    let path = blob_path(&room, &digest);
    relay.reserve(&room, &digest, bytes.len() as u64);
    assert_eq!(http_put(relay.addr, &path, &bytes).await.status, 201);

    let fetched = http(relay.addr, "GET", &path, &[], &[]).await;
    assert_eq!(fetched.status, 200);
    let cache_control = fetched
        .header("cache-control")
        .expect("a fetch states its cacheability");
    assert!(
        cache_control.contains("immutable"),
        "cache-control was {cache_control:?}"
    );
    assert_eq!(
        fetched.header("etag"),
        None,
        "an address describes its own content, so there is nothing to revalidate"
    );
}

#[tokio::test]
async fn a_malformed_address_is_rejected_without_reading_a_body() {
    let relay = Relay::with_store("malformed-address").await;
    let room = room("malformed-address");
    let valid = blob::digest(b"anything");

    for address in [
        // 42, 43, and 44 characters: only the middle one is a digest.
        &valid[..42],
        &format!("{valid}x"),
        // Inside the length and outside the alphabet.
        "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuF=",
        "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuF ",
    ] {
        let response = http_put(relay.addr, &blob_path(&room, address), b"body").await;
        assert_eq!(
            response.status, 400,
            "{address:?} was not rejected: {response:?}"
        );
    }

    // A `/` inside the address is a different thing and gets a different answer.
    // It does not reach digest validation at all: the route has exactly three
    // segments below its prefix, so four is not this route, and an unrecognized
    // path is an absent resource rather than a bad request.
    let with_slash = http_put(
        relay.addr,
        "/blob/omp-relayd/malformed-address/47DEQpj8HBSa-/TImW-5JCeuQeRkm5NMpJWZG3hSuFU",
        b"body",
    )
    .await;
    assert_eq!(with_slash.status, 404, "{with_slash:?}");

    // The valid one is refused for the reason it should be -- no reservation --
    // which shows the checks above rejected on shape rather than on state.
    let response = http_put(relay.addr, &blob_path(&room, &valid), b"body").await;
    assert_eq!(response.status, 403, "{response:?}");
}

#[tokio::test]
async fn an_unsupported_method_and_an_unknown_path_are_refused() {
    let relay = Relay::with_store("method-and-path").await;
    let room = room("method-and-path");
    let digest = blob::digest(b"anything");

    let deleted = http(relay.addr, "DELETE", &blob_path(&room, &digest), &[], &[]).await;
    assert_eq!(deleted.status, 405, "{deleted:?}");
    assert_eq!(
        deleted.header("allow"),
        Some("GET, HEAD, PUT"),
        "a 405 must name the methods that do exist"
    );

    for path in ["/", "/blob", "/blob/project/task", "/healthz"] {
        let response = http(relay.addr, "GET", path, &[], &[]).await;
        assert_eq!(response.status, 404, "{path} answered {response:?}");
    }
}

// The room in the URL is what scopes a payload, so two rooms holding identical
// bytes hold two payloads and neither can read the other's.
#[tokio::test]
async fn a_payload_is_reachable_only_in_the_room_it_was_stored_in() {
    let relay = Relay::with_store("room-scoped").await;
    let mine = room("room-scoped-mine");
    let theirs = room("room-scoped-theirs");
    let bytes = payload(4096);
    let digest = blob::digest(&bytes);

    relay.reserve(&mine, &digest, bytes.len() as u64);
    assert_eq!(
        http_put(relay.addr, &blob_path(&mine, &digest), &bytes)
            .await
            .status,
        201
    );

    let across = http(relay.addr, "GET", &blob_path(&theirs, &digest), &[], &[]).await;
    assert_eq!(
        across.status, 404,
        "a payload was readable from another room: {across:?}"
    );

    // And an upload naming a room the reservation was not made in is refused.
    let refused = http_put(relay.addr, &blob_path(&theirs, &digest), &bytes).await;
    assert_eq!(refused.status, 403, "{refused:?}");
}

// A room whose components are relative path segments is admissible, and the
// route must carry it without any of it becoming a path.
#[tokio::test]
async fn a_room_of_relative_segments_round_trips_through_the_route() {
    let relay = Relay::with_store("relative-room").await;
    let escaping = omp_relayd::protocol::RoomId::new("..", "..");
    let bytes = payload(2048);
    let digest = blob::digest(&bytes);
    let path = blob_path(&escaping, &digest);
    assert!(
        path.contains("%2E%2E"),
        "the room's components must be percent-encoded: {path}"
    );

    relay.reserve(&escaping, &digest, bytes.len() as u64);
    assert_eq!(http_put(relay.addr, &path, &bytes).await.status, 201);

    let fetched = http(relay.addr, "GET", &path, &[], &[]).await;
    assert_eq!(fetched.status, 200, "{fetched:?}");
    assert_eq!(fetched.body, bytes);
    assert!(
        relay
            .store()
            .payload_path(&escaping, &digest)
            .starts_with(relay.store().root()),
        "the payload escaped the store root"
    );
}

// A relay with no store speaks only the frame protocol, which is what keeps
// every existing test's expectations intact.
#[tokio::test]
async fn a_relay_without_a_store_reads_a_transfer_request_as_a_frame() {
    let relay = Relay::start().await;
    let mut stream = TcpStream::connect(relay.addr).await.expect("connect");
    stream
        .write_all(b"GET /blob/a/b/c HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .expect("write");
    stream.flush().await.expect("flush");

    // `GET ` declares a length of 0x47455420, which is over the cap.
    let mut client = Client::new(stream);
    client
        .expect_error_then_close(ErrorCode::FrameTooLarge)
        .await;
}

// Nothing bounded a transfer connection after the first-byte dispatch. The
// accept path's handshake window ends the moment a non-zero byte arrives, and
// the builder carried no timer -- so `hyper`'s nominal header timeout was
// inactive and one byte followed by silence held a task and a file descriptor
// for as long as the client cared to.
#[tokio::test]
async fn a_connection_that_sends_one_byte_and_stalls_is_closed() {
    let relay = Relay::with_store("header-bound").await;
    let mut stream = TcpStream::connect(relay.addr)
        .await
        .expect("connect to the relay");

    // A non-zero first byte is what sends this connection to the transfer
    // dispatch, and `G` is the byte a real `GET` starts with. Nothing follows
    // it.
    stream.write_all(b"G").await.expect("write the first byte");
    stream.flush().await.expect("flush");

    let started = Instant::now();
    let mut answered = Vec::new();
    let outcome = timeout(HEADER_READ_TIMEOUT * 3, stream.read_to_end(&mut answered)).await;
    let elapsed = started.elapsed();

    println!(
        "the stalled transfer connection ended after {elapsed:?} against a \
         {HEADER_READ_TIMEOUT:?} bound, having answered {} byte(s): {outcome:?}",
        answered.len()
    );
    assert!(
        outcome.is_ok(),
        "the connection was still open {elapsed:?} after one byte, with a \
         {HEADER_READ_TIMEOUT:?} header bound"
    );
    assert!(
        elapsed >= HEADER_READ_TIMEOUT,
        "closed after {elapsed:?}, before the {HEADER_READ_TIMEOUT:?} bound \
         elapsed: something other than the header bound closed it"
    );
}

// Round 1 bounded a head and left a body unbounded, and the body is the
// expensive half: `Store::begin_upload` moves the room's allowance out of the
// reservation table, the sweep scans `reserved` and `stored` but never
// `uploading`, and only the upload's `Drop` gives the allowance back. So a peer
// that declared a maximal length and then stopped sending held a room's whole
// budget for as long as it kept the socket open, with the store holding no
// payload at all.
#[tokio::test]
async fn an_upload_that_stops_sending_is_failed_and_gives_its_allowance_back() {
    let mut relay = Relay::with_store("stalled-body").await;
    let room = room("stalled-body");

    let bytes = payload(usize::try_from(MAX_PAYLOAD_BYTES).expect("fits"));
    let digest = blob::digest(&bytes);
    relay.reserve(&room, &digest, MAX_PAYLOAD_BYTES);

    // The rest of the room's ceiling, so the probe below can only succeed once
    // the stalled upload's allowance comes back. Without it the probe succeeds
    // against a nearly empty room and proves nothing.
    let units = MAX_ROOM_BYTES / MAX_PAYLOAD_BYTES;
    for n in 1..units {
        relay.reserve(&room, &blob::digest(&n.to_le_bytes()), MAX_PAYLOAD_BYTES);
    }
    assert!(
        relay
            .store()
            .reserve(&room, &blob::digest(b"probe"), MAX_PAYLOAD_BYTES)
            .is_err(),
        "the room was not full before the upload stalled"
    );

    let mut upload = TcpStream::connect(relay.addr).await.expect("connect");
    let head = format!(
        "PUT {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        blob_path(&room, &digest),
        relay.addr,
        bytes.len()
    );
    // Timed from before the head, so the relay's own bound necessarily started
    // later: a lower bound measured from here cannot be met by a shorter one.
    let started = Instant::now();
    upload.write_all(head.as_bytes()).await.expect("head");
    upload
        .write_all(&bytes[..128 * 1024])
        .await
        .expect("first chunk");
    upload.flush().await.expect("flush");
    // Genuinely in flight before it stalls, or this would assert cleanup of an
    // upload that never started.
    wait_for_a_partial_file(&relay).await;

    // Nothing more is ever sent and the socket stays open, so only the body
    // bound can end this.
    let mut answered = Vec::new();
    let outcome = timeout(BODY_PROGRESS_TIMEOUT * 3, upload.read_to_end(&mut answered)).await;
    let elapsed = started.elapsed();
    let status = String::from_utf8_lossy(&answered)
        .lines()
        .next()
        .unwrap_or("nothing")
        .to_owned();

    // The allowance comes back on the relay's own task, so this is waited for
    // rather than assumed.
    let mut released = None;
    for _ in 0..200 {
        if let Ok(grant) = relay
            .store()
            .reserve(&room, &blob::digest(b"probe"), MAX_PAYLOAD_BYTES)
        {
            released = Some(grant);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    println!(
        "the stalled upload ended after {elapsed:?} against a \
         {BODY_PROGRESS_TIMEOUT:?} bound with {outcome:?}, answering \
         {status:?}; the room then answered a maximal reservation with \
         {released:?}"
    );
    assert!(
        outcome.is_ok(),
        "the upload was still open {elapsed:?} after it stopped sending, with \
         a {BODY_PROGRESS_TIMEOUT:?} body bound"
    );
    assert!(
        elapsed >= BODY_PROGRESS_TIMEOUT,
        "the upload was failed after {elapsed:?}, before the \
         {BODY_PROGRESS_TIMEOUT:?} bound elapsed: something other than the \
         body bound ended it"
    );
    assert!(
        status.contains("408"),
        "the stalled upload was answered {status:?} rather than a 408"
    );
    assert!(
        released.is_some(),
        "the stalled upload's allowance was never returned: a maximal \
         reservation is still refused after {elapsed:?}"
    );
    assert_eq!(
        relay.store().payload_len(&room, &digest),
        None,
        "a stalled upload became fetchable"
    );

    relay.drain_removals().await;
    let leftovers = files_under(relay.store().root());
    assert!(
        leftovers.is_empty(),
        "the stalled upload left {leftovers:?} on disk"
    );
}

// The complementary half, and without it the bound above is indistinguishable
// from a total-transfer deadline -- which is the defect that fails a maximal
// payload on a slow link *because it was working*. The gaps here are shorter
// than the bound and their sum is longer than it, which is exactly the shape a
// slow link produces.
#[tokio::test]
async fn an_upload_that_keeps_moving_outlives_the_body_bound() {
    let relay = Relay::with_store("slow-but-moving").await;
    let room = room("slow-but-moving");

    // Two chunks, so there are two gaps and their sum exceeds the bound.
    let bytes = payload(96 * 1024);
    let digest = blob::digest(&bytes);
    relay.reserve(&room, &digest, bytes.len() as u64);

    let mut upload = TcpStream::connect(relay.addr).await.expect("connect");
    let head = format!(
        "PUT {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        blob_path(&room, &digest),
        relay.addr,
        bytes.len()
    );
    upload.write_all(head.as_bytes()).await.expect("head");

    // Three fifths of the bound: comfortably inside it, and two of them are
    // comfortably past it.
    let gap = BODY_PROGRESS_TIMEOUT * 3 / 5;
    let started = Instant::now();
    for chunk in bytes.chunks(48 * 1024) {
        tokio::time::sleep(gap).await;
        upload.write_all(chunk).await.expect("chunk");
        upload.flush().await.expect("flush");
    }

    let mut answered = Vec::new();
    let outcome = timeout(BODY_PROGRESS_TIMEOUT, upload.read_to_end(&mut answered)).await;
    let elapsed = started.elapsed();
    let status = String::from_utf8_lossy(&answered)
        .lines()
        .next()
        .unwrap_or("nothing")
        .to_owned();

    println!(
        "an upload in {gap:?} steps ran {elapsed:?} against a \
         {BODY_PROGRESS_TIMEOUT:?} bound and was answered {status:?} \
         ({outcome:?}); the store holds {:?} byte(s)",
        relay.store().payload_len(&room, &digest)
    );
    assert!(
        elapsed > BODY_PROGRESS_TIMEOUT,
        "the transfer took {elapsed:?}, inside the {BODY_PROGRESS_TIMEOUT:?} \
         bound: this test proves nothing unless it outlasts it"
    );
    assert!(
        status.contains("201"),
        "an upload that kept moving in {gap:?} steps was answered {status:?}"
    );
    assert_eq!(
        relay.store().payload_len(&room, &digest),
        Some(bytes.len() as u64),
        "an upload that kept moving did not become the payload it sent"
    );
}

// A removal is queued under the index lock and performed later, so between the
// two the same room can be recreated and the same digest re-uploaded. The stale
// removal must not unlink the replacement: a `HEAD` reporting a length beside a
// `GET` answering `404` is the inconsistency this pins.
#[tokio::test]
async fn a_queued_room_removal_cannot_unlink_a_re_uploaded_payload() {
    let mut relay = Relay::with_store("stale-room-removal").await;
    let here = room("stale-room-removal");
    let bytes = payload(4096);
    let digest = blob::digest(&bytes);
    let path = blob_path(&here, &digest);

    relay.reserve(&here, &digest, bytes.len() as u64);
    assert_eq!(
        http_put(relay.addr, &path, &bytes).await.status,
        201,
        "the first upload must be stored"
    );

    // The room's last peer deregisters: the index entry goes at once and the
    // directory is queued, because the caller is a connection task's `Drop`.
    relay.store().forget_room(&here);

    // The window: a new peer joins, and the artifact is sent again.
    relay.reserve(&here, &digest, bytes.len() as u64);
    assert_eq!(
        http_put(relay.addr, &path, &bytes).await.status,
        201,
        "the resent upload must be stored"
    );

    // The queued removal, performed.
    relay.drain_removals().await;

    let length = http(relay.addr, "HEAD", &path, &[], &[]).await;
    let fetched = http(relay.addr, "GET", &path, &[], &[]).await;
    println!(
        "after the queued removal: HEAD {} content-length {:?}, GET {} with {} \
         byte(s)",
        length.status,
        length.header("content-length"),
        fetched.status,
        fetched.body.len()
    );
    assert_eq!(
        (length.status, fetched.status),
        (200, 200),
        "a length request and a fetch must agree: HEAD {length:?}, GET \
         {fetched:?}"
    );
    assert_eq!(
        fetched.body, bytes,
        "the fetched bytes are not the resent payload"
    );
}
