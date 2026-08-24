//! Payload transfer over HTTP/1.1, on the same listening socket as the frame
//! protocol.
//!
//! # Routes
//!
//! ```text
//!   PUT   /blob/<project>/<task>/<digest>   201 created | 204 already held
//!                                           400 digest or length mismatch
//!                                           403 no matching reservation
//!                                           413 over the reservation
//!   GET   /blob/<project>/<task>/<digest>   200 + body | 404 absent or expired
//!   HEAD  /blob/<project>/<task>/<digest>   200 + length | 404
//!   anything else                           404 | 405
//! ```
//!
//! `<project>` and `<task>` are percent-encoded, because a room identifier may
//! contain anything except `/` and `@`. `<digest>` is not encoded and cannot
//! need to be: its alphabet excludes every character a URL reserves.
//!
//! # What authorizes a request
//!
//! A `GET` is authorized by knowing a 256-bit address, which is genuine evidence
//! of having been told it. A `PUT` cannot be: an uploader computes its own
//! digest, so the address is no secret on that side. Write authority is instead
//! the reservation made over an admitted frame connection, which makes it
//! exactly the `hello` handshake that already decides who may address a room.
//! No credential is introduced on either route.
//!
//! # Why this shares the frame port
//!
//! Two ports would be two exposure decisions, and publishing the frame port to a
//! private network while leaving transfer on loopback yields a deployment where
//! messages work, every attachment silently fails to resolve, and nothing fails
//! at the moment of the mistake. One port makes that state unrepresentable. The
//! discrimination happens in [`crate::relay`]'s accept path.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use futures_util::TryStreamExt;
use http_body_util::{BodyExt, Either, Empty, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::io::ReaderStream;

use crate::blob::{self, Accepted, UploadError, UploadRefusal};
use crate::protocol::RoomId;

/// Path prefix every transfer route sits under.
const ROUTE_PREFIX: &str = "/blob/";

/// Content type of every payload.
///
/// The store holds opaque bytes and has no way to know better: a payload is a
/// diff, a log bundle, an archive, or a binary, and guessing from content would
/// be a sniffing heuristic in a place that has no need of one. A recipient names
/// its own file, so nothing downstream depends on this value.
const PAYLOAD_CONTENT_TYPE: &str = "application/octet-stream";

/// `Cache-Control` on a fetched payload.
///
/// An address describes its own content, so there is nothing for a validator to
/// detect and no cache that can go stale. No `ETag` accompanies it for the same
/// reason: revalidation has nothing to compare.
const IMMUTABLE: &str = "public, max-age=31536000, immutable";

/// A response body: either a streamed payload or nothing at all.
type TransferBody = Either<
    StreamBody<
        futures_util::stream::MapOk<ReaderStream<tokio::fs::File>, fn(Bytes) -> Frame<Bytes>>,
    >,
    Empty<Bytes>,
>;

/// Serves payload transfer on one connection until the client closes it.
///
/// Runs on its own task, which is the rule that keeps payload I/O off any
/// connection's frame-reading task: that task owns an idle deadline, and a
/// transfer is orders of magnitude longer than any frame, so performing one
/// there would close heartbeating peers for silence they did not commit.
pub async fn serve_connection<S>(io: S, store: Arc<blob::Store>, peer_addr: SocketAddr)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |request: Request<Incoming>| {
        let store = Arc::clone(&store);
        async move { Ok::<_, Infallible>(route(request, store, peer_addr).await) }
    });

    if let Err(error) = http1::Builder::new()
        // Keep-alive is on, deliberately: the extension asks for a payload's
        // length and then its bytes, and two requests on one connection is one
        // handshake instead of two.
        //
        // Half-closures are accepted, which `hyper` does not do by default. A
        // client that declared a length, sent less, and closed its write side is
        // exactly the truncated upload this surface has to *report*, and the
        // default closes the connection instead -- leaving the uploader with a
        // bare EOF where a `400` naming the mismatch belongs. It also lets a
        // client that has finished its request close its write side while
        // waiting, which is a legitimate thing for a client to do.
        .half_close(true)
        .serve_connection(TokioIo::new(io), service)
        .await
    {
        // A client that hangs up mid-request is ordinary rather than notable, so
        // this is a debug record: the store's own logs carry what was refused.
        tracing::debug!(%peer_addr, %error, "payload transfer connection ended");
    }
}

/// Dispatches one request.
async fn route(
    request: Request<Incoming>,
    store: Arc<blob::Store>,
    peer_addr: SocketAddr,
) -> Response<TransferBody> {
    let Some(target) = Target::parse(request.uri().path()) else {
        // Not a transfer route at all. `404` rather than `400`, because an
        // unrecognized path is an absent resource and the relay does not
        // describe its own routing surface to whatever asked.
        return empty(StatusCode::NOT_FOUND);
    };

    // Validated before the method dispatch, so a malformed address is rejected
    // without reading a body -- which is the whole point of checking it here
    // rather than in the store.
    if let Err(error) = crate::protocol::validate_digest(&target.digest) {
        tracing::debug!(%peer_addr, %error, "payload address rejected");
        return empty(StatusCode::BAD_REQUEST);
    }

    match *request.method() {
        Method::PUT => put(request, &store, &target, peer_addr).await,
        Method::GET => get(&store, &target, true).await,
        Method::HEAD => get(&store, &target, false).await,
        // `405` names the routes that do exist, which is the one thing a client
        // that used the wrong method can act on.
        _ => {
            let mut response = empty(StatusCode::METHOD_NOT_ALLOWED);
            response.headers_mut().insert(
                hyper::header::ALLOW,
                "GET, HEAD, PUT".parse().expect("static"),
            );
            response
        }
    }
}

/// Stores a payload against a live reservation.
async fn put(
    request: Request<Incoming>,
    store: &Arc<blob::Store>,
    target: &Target,
    peer_addr: SocketAddr,
) -> Response<TransferBody> {
    // `Content-Length` is required, and that is not pedantry: it is what lets
    // the reservation be checked before a byte of body is read. A chunked upload
    // would have to be admitted on trust and refused mid-transfer.
    let Some(declared) = content_length(&request) else {
        tracing::debug!(%peer_addr, "upload refused: no usable Content-Length");
        return empty(StatusCode::LENGTH_REQUIRED);
    };

    let accepted = match store.begin_upload(&target.room, &target.digest, declared) {
        Ok(accepted) => accepted,
        Err(UploadRefusal::Unreserved) => {
            tracing::info!(
                %peer_addr,
                room = %target.room,
                digest = %target.digest,
                declared,
                "upload refused: no live reservation"
            );
            return empty(StatusCode::FORBIDDEN);
        }
        Err(UploadRefusal::OverReservation { declared, reserved }) => {
            tracing::info!(
                %peer_addr,
                room = %target.room,
                digest = %target.digest,
                declared,
                reserved,
                "upload refused: over its reservation"
            );
            return empty(StatusCode::PAYLOAD_TOO_LARGE);
        }
    };

    let Accepted::Upload(mut upload) = accepted else {
        // Already held. Nothing is read from the body: the address names the
        // content, so a retried upload has nothing to add.
        tracing::debug!(
            room = %target.room,
            digest = %target.digest,
            "upload answered from the payload already held"
        );
        return empty(StatusCode::NO_CONTENT);
    };

    let mut body = request.into_body();
    while let Some(next) = body.frame().await {
        let frame = match next {
            Ok(frame) => frame,
            Err(error) => {
                // The connection failed mid-upload. Dropping `upload` returns
                // the allowance and removes the partial file.
                tracing::info!(
                    %peer_addr,
                    room = %target.room,
                    digest = %target.digest,
                    written = upload.written(),
                    %error,
                    "upload failed mid-transfer"
                );
                return empty(StatusCode::BAD_REQUEST);
            }
        };
        // Trailers carry no payload bytes and are simply not part of a payload.
        let Ok(chunk) = frame.into_data() else {
            continue;
        };
        if let Err(error) = upload.write(&chunk).await {
            return refuse_upload(&error, target, peer_addr);
        }
    }

    match upload.finish().await {
        Ok(bytes) => {
            tracing::info!(
                %peer_addr,
                room = %target.room,
                digest = %target.digest,
                bytes,
                "payload stored"
            );
            empty(StatusCode::CREATED)
        }
        Err(error) => refuse_upload(&error, target, peer_addr),
    }
}

/// Maps a store-level upload failure onto its status, logging what it was.
fn refuse_upload(
    error: &UploadError,
    target: &Target,
    peer_addr: SocketAddr,
) -> Response<TransferBody> {
    let status = match error {
        // Both are the uploader's claim failing against its own bytes, which is
        // a malformed request rather than a server fault.
        UploadError::DigestMismatch { .. } | UploadError::LengthMismatch { .. } => {
            StatusCode::BAD_REQUEST
        }
        // The room ended under the upload. `409`: the request was valid and the
        // state it addressed is gone.
        UploadError::RoomGone => StatusCode::CONFLICT,
        UploadError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    tracing::info!(
        %peer_addr,
        room = %target.room,
        digest = %target.digest,
        status = status.as_u16(),
        %error,
        "upload refused"
    );
    empty(status)
}

/// Answers a fetch, with the payload's bytes when `with_body` and its length
/// alone otherwise.
async fn get(store: &Arc<blob::Store>, target: &Target, with_body: bool) -> Response<TransferBody> {
    // The index is the authority, not the filesystem: an expired payload has an
    // entry that is gone before its file is, and reporting from the index is
    // what makes "expired" and "absent" the same answer.
    let Some(bytes) = store.payload_len(&target.room, &target.digest) else {
        return empty(StatusCode::NOT_FOUND);
    };

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, PAYLOAD_CONTENT_TYPE)
        .header(CONTENT_LENGTH, bytes)
        .header(CACHE_CONTROL, IMMUTABLE);

    if !with_body {
        // A length-only answer is what lets a recipient decide whether to
        // transfer at all, and it reports strictly more than a size carried in
        // the frame that referenced the payload: it also reports that the
        // payload still exists.
        return response
            .body(Either::Right(Empty::new()))
            .unwrap_or_else(|_| empty(StatusCode::INTERNAL_SERVER_ERROR));
    }

    let path = store.payload_path(&target.room, &target.digest);
    let file = match tokio::fs::File::open(&path).await {
        Ok(file) => file,
        Err(error) => {
            // The index held an entry whose file is gone. Reported as absent
            // rather than as a fault, because that is what it is to the caller,
            // and recorded at `warn` because it should not happen.
            tracing::warn!(
                room = %target.room,
                digest = %target.digest,
                path = %path.display(),
                %error,
                "a payload in the index has no file"
            );
            return empty(StatusCode::NOT_FOUND);
        }
    };

    // Streamed rather than read: a payload may be `MAX_PAYLOAD_BYTES`, and
    // buffering one to answer a fetch would put payload size back into the
    // relay's resident memory, which is the coupling the reference exists to
    // break.
    let stream = ReaderStream::new(file).map_ok(Frame::data as fn(Bytes) -> Frame<Bytes>);
    response = response.status(StatusCode::OK);
    response
        .body(Either::Left(StreamBody::new(stream)))
        .unwrap_or_else(|_| empty(StatusCode::INTERNAL_SERVER_ERROR))
}

/// A status and nothing else.
fn empty(status: StatusCode) -> Response<TransferBody> {
    let mut response = Response::new(Either::Right(Empty::new()));
    *response.status_mut() = status;
    response
}

/// The declared body length, when the header is present and usable.
fn content_length(request: &Request<Incoming>) -> Option<u64> {
    request
        .headers()
        .get(CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// What a transfer request addresses.
#[derive(Debug, PartialEq, Eq)]
struct Target {
    room: RoomId,
    digest: String,
}

impl Target {
    /// Parses `/blob/<project>/<task>/<digest>`, percent-decoding the room's two
    /// components.
    ///
    /// Returns `None` for any other shape, including one with a trailing segment
    /// or an empty component: a room identifier is non-empty by
    /// [`crate::protocol::validate_identifier`], so an empty one addresses no
    /// room that can exist.
    fn parse(path: &str) -> Option<Self> {
        let rest = path.strip_prefix(ROUTE_PREFIX)?;
        let mut segments = rest.split('/');
        let project = percent_decode(segments.next()?)?;
        let task = percent_decode(segments.next()?)?;
        let digest = segments.next()?;
        if segments.next().is_some() || project.is_empty() || task.is_empty() {
            return None;
        }
        Some(Self {
            room: RoomId::new(project, task),
            digest: digest.to_owned(),
        })
    }
}

/// Decodes `%XX` escapes, rejecting a malformed escape or invalid UTF-8.
///
/// Hand-written rather than a dependency: this is the whole of what the routes
/// need, decoding only, with no query string and no form encoding -- so `+` is a
/// literal plus and not a space.
fn percent_decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = bytes.get(index + 1)?;
            let low = bytes.get(index + 2)?;
            let byte = (hex_value(*high)? << 4) | hex_value(*low)?;
            out.push(byte);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_well_formed_route_parses_into_a_room_and_a_digest() {
        let digest = "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU";
        assert_eq!(
            Target::parse(&format!("/blob/omp-relayd/attachments/{digest}")),
            Some(Target {
                room: RoomId::new("omp-relayd", "attachments"),
                digest: digest.to_owned(),
            })
        );
    }

    #[test]
    fn a_rooms_components_are_percent_decoded() {
        let digest = "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU";
        let target = Target::parse(&format!("/blob/a%20b/%E6%97%A5%E6%9C%AC/{digest}"))
            .expect("a percent-encoded room parses");
        assert_eq!(target.room, RoomId::new("a b", "日本"));
    }

    #[test]
    fn a_relative_room_component_stays_one_component() {
        // `..` is admissible as an identifier, so it must survive parsing as the
        // literal room it is. It cannot become a path component: the store names
        // its directory by a digest of these values.
        let digest = "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU";
        let target =
            Target::parse(&format!("/blob/../../{digest}")).expect("a relative room parses");
        assert_eq!(target.room, RoomId::new("..", ".."));
    }

    #[test]
    fn a_path_that_is_not_a_transfer_route_is_not_a_target() {
        let digest = "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU";
        for path in [
            "/",
            "/blob",
            "/blob/",
            "/blob/project",
            "/blob/project/task",
            "/other/project/task/x",
            // A fourth segment: the route has exactly three below the prefix.
            &format!("/blob/project/task/{digest}/extra"),
            // Empty components address no room that can exist.
            &format!("/blob//task/{digest}"),
            &format!("/blob/project//{digest}"),
        ] {
            assert_eq!(Target::parse(path), None, "{path} parsed as a target");
        }
    }

    #[test]
    fn a_malformed_percent_escape_is_rejected() {
        let digest = "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU";
        for path in [
            &format!("/blob/%2/task/{digest}"),
            &format!("/blob/%zz/task/{digest}"),
            &format!("/blob/project/%/{digest}"),
            // Valid escapes that are not valid UTF-8.
            &format!("/blob/%FF%FE/task/{digest}"),
        ] {
            assert_eq!(Target::parse(path), None, "{path} parsed as a target");
        }
    }

    #[test]
    fn a_plus_is_a_literal_plus() {
        // Not form encoding: the routes carry no query string, so `+` has no
        // special meaning and a room named `a+b` round-trips unchanged.
        let digest = "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU";
        let target = Target::parse(&format!("/blob/a+b/task/{digest}")).expect("parses");
        assert_eq!(target.room, RoomId::new("a+b", "task"));
    }
}
