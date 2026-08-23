# Protocol v1 cross-language fixtures

These files are the contract between the Rust relay and the TypeScript client.
Each one is a single MessagePack frame payload, without the four-byte length
prefix that precedes it on the wire.

The `rust-*` files are produced and verified by `server/tests/fixtures.rs`.
`implement-relay-client-library` adds the `ts-*` counterparts and the
reverse-direction assertion, so that each implementation decodes bytes the other
produced rather than only its own.

## What each fixture is for

A fixture exists to catch one specific way two implementations can silently
disagree. Byte counts are from the committed files.

| File | Frame | Risk it covers |
|---|---|---|
| `rust-hello.msgpack` | `hello` | A **nested map**: `room` is a map of `project` and `task`. An implementation that flattened the room, or that sent the combined `<project>/<task>` spelling as one string, fails to decode this. |
| `rust-send.msgpack` | `send` | An **absent optional field is omitted**, not encoded as nil. The payload contains no `reply_to` key at all. An implementation that requires the key to be present fails here. |
| `rust-receipt.msgpack` | `receipt` | An **enum is a `snake_case` string**, not an integer. The payload spells out `recipient_backpressure`. An implementation that expected an ordinal fails here. |

Every payload is also a MessagePack **map**, never a positional array. That is
the property the whole set depends on, and it is asserted separately in
`server/src/protocol.rs`.

## How they are checked

`cargo test --test fixtures` asserts two things per fixture:

- the committed bytes still **decode to the value they document**, which is the
  semantic contract the other implementation depends on; and
- a **fresh encode still equals the committed bytes**, so an accidental change to
  a field name, field order, or enum representation fails the suite instead of
  quietly rewriting the file.

Tests across the two languages compare **decoded values**, not bytes. The two
libraries need not agree byte for byte, only semantically. The byte comparison
above is narrower: it is this implementation checked against its own past
output, which is what makes drift visible.

## Regenerating

Only when an encoding change is intended:

```sh
cd server && UPDATE_FIXTURES=1 cargo test --test fixtures
```

Then commit the changed files. A regenerated fixture is a protocol change and
belongs in the same commit as the spec update that authorizes it.

A missing fixture is written on the next test run and then fails, telling you to
commit it. That way a fresh checkout cannot pass by generating what it was
supposed to verify.
