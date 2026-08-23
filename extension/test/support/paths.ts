/**
 * Repository paths the test suite needs, resolved once from this file's own
 * location.
 *
 * Anchored here rather than computed in each test, because a test that walks
 * `..` the right number of times from wherever it happens to live silently
 * resolves to the wrong directory the moment it moves — and a fixture path that
 * points nowhere fails as "file missing", which reads like a missing fixture
 * rather than a broken path.
 */

import { join } from "node:path";

/** The repository root: this file is at `extension/test/support/`. */
export const REPO_ROOT = join(import.meta.dir, "..", "..", "..");

/** The Bun package holding the client library. */
export const PACKAGE_ROOT = join(REPO_ROOT, "extension");

/** The Rust crate holding the relay. */
export const SERVER_ROOT = join(REPO_ROOT, "server");

/** Cross-language MessagePack fixtures shared by both implementations. */
export const FIXTURE_DIR = join(REPO_ROOT, "test-fixtures", "protocol-v1");
