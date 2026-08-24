# Protocol v1 cross-language fixtures

These files are the contract between the Rust relay and the TypeScript client.
Each one is a single MessagePack frame payload, without the four-byte length
prefix that precedes it on the wire.

The set runs in both directions, and that is the point of it. The `rust-*` files
are produced by `server/tests/fixtures.rs` and decoded by
`extension/test/unit/fixtures.test.ts`; the `ts-*` files are produced by
`extension/test/unit/fixtures.test.ts` and decoded by
`server/tests/fixtures.rs`.
Neither implementation verifies only its own output, because a round-trip
through one library agrees with itself no matter what that library does.

## What each fixture is for

A fixture exists to catch one specific way two implementations can silently
disagree. Byte counts are from the committed files.

| File | Frame | Risk it covers |
|---|---|---|
| `rust-hello.msgpack` | `hello` | A **nested map**: `room` is a map of `project` and `task`. An implementation that flattened the room, or that sent the combined `<project>/<task>` spelling as one string, fails to decode this. |
| `rust-send.msgpack` | `send` | An **absent optional field is omitted**, not encoded as nil. The payload contains no `reply_to` key at all. An implementation that requires the key to be present fails here. |
| `rust-receipt.msgpack` | `receipt` | An **enum is a `snake_case` string**, not an integer. The payload spells out `recipient_backpressure`. An implementation that expected an ordinal fails here. |
| `rust-announce.msgpack` | `announce` | **No target field at all.** The room-wide address is the absence of a peer component, so the payload carries no `to` key for an implementation to reserve a value in. One that expected a target — a magic `all`, or an empty string — cannot decode this without inventing a value the wire never carried. |
| `rust-notice.msgpack` | `notice` | A **second delivery class carried by the discriminator**, over a field set identical to `message`. An implementation that told the classes apart by anything else — a marker field, a prefix in the body — passes its own tests and fails here. The fixture is asserted to be exactly one byte shorter than the same fields under `message`, which is the arithmetic the shared body budget rests on. |
| `rust-accepted.msgpack` | `accepted` | **Two integer counts and no status.** A mixed outcome has no honest `receipt` status, so the frame carries `delivered` and `shed` instead. An implementation expecting a `status` key, or one count plus a total, fails here. |
| `rust-reserve.msgpack` | `reserve` | A **byte count as an integer**, beside a digest as a 43-character string. An implementation that wrote the count as text would round-trip through its own encoder and fail here. The digest is the SHA-256 of the empty payload in unpadded base64url, so the value is one either side could have produced rather than a made-up 43 characters. |
| `rust-reserved.msgpack` | `reserved` | A **status as a `snake_case` string with the payload's stated lifetime beside it**. The frame exists to answer a `reserve`, and the lifetime is what closes §13.1's recorded trap — a reference that may no longer resolve when its recipient reads it. The refusal half of the rule, that a non-granted status carries no `expires_in`, is asserted over a freshly encoded frame rather than a second fixture. |
| `rust-send-attachment.msgpack` | `send` | A **reference as a bare string**, never a map. The fixture is asserted to carry none of `digest`, `bytes`, `host`, `port`, `path`, `filename`, or `name`, because each of those was rejected for a reason: a location would let a sender aim a recipient's fetch at an arbitrary host, a size is less informative than the length request, and a filename would make a path component out of another peer's text. |
| `ts-hello.msgpack` | `hello` | The same nested-map risk, in the other direction. Decoded through a type whose `room` deserializer accepts a map only, so a positional or flattened room fails rather than decoding by accident. |
| `ts-send.msgpack` | `send` | The same omitted-optional risk, in the other direction: the TypeScript encoder must omit `reply_to` rather than emit nil for it. |
| `ts-receipt.msgpack` | `receipt` | The same string-enum risk, in the other direction: `rmp-serde` must recover `ReceiptStatus::RecipientBackpressure` from the string the TypeScript side wrote. |
| `ts-announce.msgpack` | `announce` | The same no-target risk, in the other direction: the TypeScript encoder must write no `to` key, and `rmp-serde` must read the frame as an announcement rather than as something with a missing field. |
| `ts-notice.msgpack` | `notice` | The same class-in-the-discriminator risk, in the other direction. Both sides assert the same relation over these bytes: the frame is exactly one byte shorter than the identical fields under `message`. |
| `ts-accepted.msgpack` | `accepted` | The same two-counts risk, in the other direction: `rmp-serde` must recover both `u32` fields from the numbers the TypeScript side wrote, with no `status` anywhere. |

The two `hello` fixtures name different rooms, because each was produced by the
change that introduced it. Nothing depends on the values matching; the shape is
what is under test.

Every payload is also a MessagePack **map**, never a positional array. That is
the property the whole set depends on, and it is asserted separately in
`server/src/protocol.rs` and `extension/src/protocol.ts`.

## How they are checked

```sh
cd server    && cargo test --test fixtures   # decodes rust-* and ts-*
cd extension && bun test test/unit/fixtures.test.ts   # decodes rust-* and ts-*
```

Against **its own** fixtures each side asserts two things:

- the committed bytes still **decode to the value they document**, which is the
  semantic contract the other implementation depends on; and
- a **fresh encode still equals the committed bytes**, so an accidental change to
  a field name, field order, or enum representation fails the suite instead of
  quietly rewriting the file.

Against the **other side's** fixtures each asserts only the first. Comparing
decoded values rather than bytes is deliberate: the two libraries need not agree
byte for byte, only semantically, and demanding byte equality would couple each
implementation to the other's encoder choices and fail on a difference that
breaks nothing.

Where a fixture exists specifically to pin an encoding rather than a value, that
encoding is asserted directly over the bytes — the absence of a `reply_to` key,
the absence of a `to` key on `announce`, the absence of a `status` key on
`accepted`, and the presence of the literal `recipient_backpressure` — because
`reply_to: None` alone would also be satisfied by an explicit nil, which both
decoders accept on purpose, and because both decoders ignore unknown fields, so
a key that should not exist would decode in silence.

The `notice` pair is asserted as a *relation* rather than a size: each side
checks that the frame is exactly one byte shorter than the identical fields
under `message`. That one byte is the difference between the two delivery
classes, and the shared 65024-byte body budget is computed from it, so a change
that widened `notice` would fail on both sides rather than silently narrowing
the margin.

As it happens, `rmp_serde::to_vec_named` and `@msgpack/msgpack` currently emit
byte-identical payloads for `send`, `receipt`, `announce`, `notice`, and
`accepted`. That is an observation, not a requirement, and no test asserts it:
either library could change its integer or string encoding within the format and
still be correct.

## Regenerating

Only when an encoding change is intended, and only from the side that owns the
file:

```sh
cd server    && UPDATE_FIXTURES=1 cargo test --test fixtures        # rust-*
cd extension && UPDATE_FIXTURES=1 bun test test/unit/fixtures.test.ts   # ts-*
```

Then commit the changed files. A regenerated fixture is a protocol change and
belongs in the same commit as the spec update that authorizes it.

A missing fixture is written on the next run of its owning side and then fails,
telling you to commit it — so a fresh checkout cannot pass by generating what it
was supposed to verify. The *other* side does not generate it: a missing `ts-*`
fails `cargo test` with a message naming `bun test`, and vice versa, because a
side that regenerated its counterpart's fixture would be checking itself again.
