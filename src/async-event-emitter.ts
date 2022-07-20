// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

type AsyncCallback = (...args: any[]) => any | Promise<any>;

export type AsyncCallbackOptions = {
  /**
   * Whether to allow this callback to run concurrently
   * with other callbacks on the same event. Different
   * events can always run concurrently.
   *
   * By default, each callback will be awaited before the
   * next callback is started.
   */
  concurrent?: boolean;

  /**
   * Whether to swallow and ignore errors in this callback
   * and continue to the next.
   */
  continueOnError?: boolean;
};

type CallbackInfo = {
  fn: AsyncCallback;
  opts: AsyncCallbackOptions;
};

function isPromise(val: any | Promise<any>): val is Promise<any> {
  return 'then' in val;
}

async function runAndIgnore(
  fn: AsyncCallback,
  thisArg: any | null,
  args: any[]
): Promise<void> {
  try {
    await fn.call(fn, thisArg, args);
  } catch {
    /* ignore */
  }
}

/**
 * Base class for an asynchronous event emitter. Event
 * callbacks that return a promise will be awaited. Inheriting
 * classes or interfaces should document supported event types
 *  */
export class AsyncEventEmitter {
  readonly #supported: Set<string> | null;
  #callbacks: { [type: string]: CallbackInfo[] } = {};

  /** Create a new {@link AsyncEventEmitter} that supports any event type */
  constructor();

  /** Create a new {@link AsyncEventEmitter} that supports the provided event types */
  constructor(supported: string | string[]);

  constructor(supported?: string | string[]) {
    if (supported == null) {
      this.#supported = null;
    } else {
      if (typeof supported === 'string') {
        supported = [supported];
      }
      if (supported.length == 0) {
        throw new Error(
          'supported events list was empty but not null. Provide null to support all events, or at least one event type'
        );
      }
      this.#supported = new Set<string>(supported);
    }
  }

  /** Check whether the given event type is supported */
  supports(type: string): boolean {
    return this.#supported == null || this.#supported.has(type);
  }

  /** Schedule a callback for the given event type */
  on(type: string, fn: AsyncCallback, opts?: AsyncCallbackOptions): void {
    if (!this.supports(type)) {
      throw new Error(`Event type ${type} not supported`);
    }
    if (opts == null) {
      opts = {};
    }
    if (type in this.#callbacks) {
      this.#callbacks[type].push({ fn, opts });
    } else {
      this.#callbacks[type] = [{ fn, opts }];
    }
  }

  /** Remove all scheduled events for the given type */
  off(type: string): void;

  /** Remove the provided scheduled event for the given type */
  off(type: string, fn: AsyncCallback): void;

  off(type: string, fn?: AsyncCallback): void {
    const cbs = this.#callbacks[type];
    if (cbs == null || cbs.length == 0) {
      return;
    }

    if (fn == null) {
      // Remove all
      cbs.splice(0, cbs.length);
      return;
    }

    // Just look for the matching function
    const ix = cbs.findIndex((info) => info.fn === fn);
    if (ix >= 0) {
      cbs.splice(ix, 1);
    }
  }

  /**
   * Emit the provided event type with a thisArg and arguments.
   *
   * Awaits all callbacks.
   * */
  async emit(type: string, thisArg?: any | null, args?: any[]): Promise<void> {
    // YES, use actual array ref. Allows callbacks to schedule handlers
    const cbs = this.#callbacks[type];
    if (cbs == null || cbs.length == 0) {
      return Promise.resolve();
    }

    const promises = [];
    while (cbs.length > 0) {
      const cb = cbs.shift()!;
      let result: Promise<any> | any;
      if (cb.opts.continueOnError === true) {
        result = runAndIgnore(cb.fn, thisArg, args || []);
      } else {
        result = cb.fn.call(thisArg, args || []);
      }
      if (isPromise(result)) {
        if (cb.opts.concurrent === true) {
          promises.push(result);
        } else {
          await result;
        }
      }
    }

    if (promises.length) {
      await Promise.all(promises);
    }
  }
}

export interface AsyncCompleteEmitter<T> {
  /**
   * Schedule a callback to be run when the
   * transaction has completed all operations
   */
  on(
    type: 'complete',
    fn: (con: T) => any | Promise<any>,
    opts?: AsyncCallbackOptions
  ): void;

  /** Remove all scheduled 'complete' callbacks */
  off(type: 'complete'): void;
  /** Remove a scheduled 'complete' callback */
  off(type: 'complete', fn: (con: T) => any | Promise<any>): void;
}
