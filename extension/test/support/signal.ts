/**
 * A race-free "wait until this has happened N times" primitive.
 *
 * Deliberately counter-based rather than event-based: a `waitForNext()` shape
 * has to be called *before* the event it awaits, and in a test driving real
 * sockets that ordering is not something the test controls. Comparing against a
 * count that only ever grows removes the race instead of narrowing it, and it
 * removes the temptation to paper over the race with a sleep.
 */
export class Signal<T = void> {
  readonly observed: T[] = [];
  readonly #waiters: { readonly count: number; readonly resolve: () => void }[] = [];

  /** Records one occurrence and releases anyone waiting for it. */
  fire(value: T): void {
    this.observed.push(value);
    for (let index = this.#waiters.length - 1; index >= 0; index -= 1) {
      const waiter = this.#waiters[index];
      if (waiter !== undefined && this.observed.length >= waiter.count) {
        this.#waiters.splice(index, 1);
        waiter.resolve();
      }
    }
  }

  get count(): number {
    return this.observed.length;
  }

  /** The most recent value, or `undefined` when nothing has happened. */
  get last(): T | undefined {
    return this.observed.at(-1);
  }

  /** Resolves once this has fired at least `count` times. */
  until(count: number): Promise<void> {
    const waiter = Promise.withResolvers<void>();
    if (this.observed.length >= count) {
      waiter.resolve();
    } else {
      this.#waiters.push({ count, resolve: waiter.resolve });
    }
    return waiter.promise;
  }
}
