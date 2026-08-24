/**
 * A {@link Scheduler} whose clock the test advances.
 *
 * This exists to make two claims checkable that a real clock cannot: that the
 * client routes every timer through the scheduler its host supplied, and that
 * shutdown cancels every one of them. Both are assertions about which timers
 * exist, not about how long they take, so a real clock would only add latency
 * and flakiness.
 */

import type { Scheduler, TimerHandle } from "../../src/client.ts";

/** One timer the client asked for. */
export interface ScheduledTimer {
  readonly handle: number;
  readonly kind: "timeout" | "interval";
  readonly delay: number;
  readonly callback: () => void;
  /** Set when the client cancelled it. */
  cancelled: boolean;
  /** How many times the test has run its callback. */
  fired: number;
}

export class FakeScheduler implements Scheduler {
  #nextHandle = 1;
  /** Every timer ever created, cancelled ones included, in creation order. */
  readonly history: ScheduledTimer[] = [];

  setTimeout(callback: () => void, milliseconds: number): TimerHandle {
    return this.#create("timeout", callback, milliseconds);
  }

  setInterval(callback: () => void, milliseconds: number): TimerHandle {
    return this.#create("interval", callback, milliseconds);
  }

  clearTimeout(handle: TimerHandle): void {
    this.#cancel(handle);
  }

  clearInterval(handle: TimerHandle): void {
    this.#cancel(handle);
  }

  /** Timers created and not cancelled. */
  get live(): readonly ScheduledTimer[] {
    return this.history.filter((timer) => !timer.cancelled);
  }

  /** Live timers of one kind, newest last. */
  liveOf(kind: "timeout" | "interval", delay?: number): readonly ScheduledTimer[] {
    return this.live.filter(
      (timer) => timer.kind === kind && (delay === undefined || timer.delay === delay),
    );
  }

  /** Every delay ever requested, in creation order. Cancellation aside. */
  delaysOf(kind: "timeout" | "interval"): readonly number[] {
    return this.history.filter((timer) => timer.kind === kind).map((timer) => timer.delay);
  }

  /**
   * Runs the callback of one live timer.
   *
   * Refuses a cancelled timer rather than silently doing nothing, because "the
   * callback did not run" is exactly what a cancellation test must distinguish
   * from "the test fired the wrong timer".
   */
  fire(timer: ScheduledTimer): void {
    if (timer.cancelled) {
      throw new Error(
        `timer ${timer.handle} (${timer.kind}, ${timer.delay} ms) was cancelled and cannot fire`,
      );
    }
    timer.fired += 1;
    if (timer.kind === "timeout") {
      timer.cancelled = true;
    }
    timer.callback();
  }

  /** Fires the newest live timer of `kind`, or throws when there is none. */
  fireNewest(kind: "timeout" | "interval", delay?: number): void {
    const candidates = this.liveOf(kind, delay);
    const timer = candidates.at(-1);
    if (timer === undefined) {
      throw new Error(
        `no live ${kind}${delay === undefined ? "" : ` of ${delay} ms`}; live timers: ${this.describe()}`,
      );
    }
    this.fire(timer);
  }

  /** A one-line summary of live timers, for a failure message. */
  describe(): string {
    if (this.live.length === 0) {
      return "none";
    }
    return this.live.map((timer) => `${timer.kind}/${timer.delay}ms`).join(", ");
  }

  /**
   * Resolves once `count` timers matching `kind` and `delay` have been created.
   *
   * A signal rather than a poll. A test that needs to know the client reacted to
   * something asynchronous — a chunk arriving, a socket draining — would
   * otherwise sleep and guess, which binds the test to wall-clock time and hides
   * the condition it is actually waiting for.
   */
  until(kind: "timeout" | "interval", count: number, delay?: number): Promise<void> {
    const waiter = Promise.withResolvers<void>();
    const made = (): number =>
      this.history.filter(
        (timer) => timer.kind === kind && (delay === undefined || timer.delay === delay),
      ).length;
    if (made() >= count) {
      waiter.resolve();
    } else {
      this.#waiters.push({ kind, delay, count, resolve: waiter.resolve, made });
    }
    return waiter.promise;
  }

  readonly #waiters: {
    readonly kind: "timeout" | "interval";
    readonly delay: number | undefined;
    readonly count: number;
    readonly resolve: () => void;
    readonly made: () => number;
  }[] = [];

  #create(
    kind: "timeout" | "interval",
    callback: () => void,
    delay: number,
  ): TimerHandle {
    const handle = this.#nextHandle;
    this.#nextHandle += 1;
    this.history.push({ handle, kind, delay, callback, cancelled: false, fired: 0 });
    for (let index = this.#waiters.length - 1; index >= 0; index -= 1) {
      const waiter = this.#waiters[index];
      if (waiter !== undefined && waiter.made() >= waiter.count) {
        this.#waiters.splice(index, 1);
        waiter.resolve();
      }
    }
    return handle;
  }

  #cancel(handle: TimerHandle): void {
    const timer = this.history.find((candidate) => candidate.handle === handle);
    if (timer !== undefined) {
      timer.cancelled = true;
    }
  }
}
