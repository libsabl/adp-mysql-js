// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

export function hasFlag<T extends number>(flags: T, flag: T): boolean {
  return (flags & flag) === flag;
}

export type FnReject = (reason: unknown) => void;
export type FnResolve<T> = (value: T | PromiseLike<T>) => void;

export class PromiseHandle<T> {
  constructor() {
    let res: FnResolve<T>;
    let rej: FnReject;

    this.promise = new Promise<T>((resolve, reject) => {
      res = resolve;
      rej = reject;
    });

    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    this.resolve = res!;
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    this.reject = rej!;
  }

  readonly resolve: FnResolve<T>;
  readonly reject: FnReject;
  readonly promise: Promise<T>;
}
