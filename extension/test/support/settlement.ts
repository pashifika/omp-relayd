/**
 * Captures how a promise settles, without asserting on it yet.
 *
 * Needed because the two obvious spellings both fail when the test itself is
 * what triggers the rejection:
 *
 * - Asserting *after* the trigger leaves a window in which the promise rejects
 *   with no handler attached, which is an unhandled rejection.
 * - Asserting *before* the trigger deadlocks: `expect(p).rejects.matcher()` in
 *   Bun 1.3 drains the event loop inside the matcher call rather than returning
 *   a pending promise, so the statement never returns and the trigger below it
 *   never runs.
 *
 * This attaches a handler synchronously and never rejects, so the trigger can
 * follow it and the assertion can follow the trigger.
 */
export type Settlement<T> =
  | { readonly status: "fulfilled"; readonly value: T }
  | { readonly status: "rejected"; readonly reason: unknown };

export function settlement<T>(promise: Promise<T>): Promise<Settlement<T>> {
  return promise.then(
    (value): Settlement<T> => ({ status: "fulfilled", value }),
    (reason: unknown): Settlement<T> => ({ status: "rejected", reason }),
  );
}
