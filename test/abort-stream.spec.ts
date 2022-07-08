// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

async function* seq(
  cnt: number,
  flag: { msg?: string; cnt?: number }
): AsyncIterable<number> {
  let i = 1;

  try {
    while (i <= cnt) {
      flag.cnt = i;
      yield i;
      i++;
    }
  } finally {
    flag.msg = 'done';
  }
}

describe('iter', () => {
  it('iterates to the end', async () => {
    const flag = { msg: '', cnt: 0 };
    for await (const n of seq(10, flag)) {
      console.log(n);
    }
    expect(flag.cnt).toBe(10);
    expect(flag.msg).toBe('done');
  });

  it('cleans up if aborted', async () => {
    const flag = { msg: '', cnt: 0 };
    for await (const n of seq(10, flag)) {
      console.log(n);
      if (n > 5) {
        break;
      }
    }
    expect(flag.cnt).toBe(6);
    expect(flag.msg).toBe('done');
  });

  it('cleans up if exception', async () => {
    const flag = { msg: '', cnt: 0 };

    try {
      for await (const n of seq(10, flag)) {
        console.log(n);
        if (n > 5) {
          throw new Error('boom');
        }
      }
    } catch {
      /* */
    }
    expect(flag.cnt).toBe(6);
    expect(flag.msg).toBe('done');
  });
});
