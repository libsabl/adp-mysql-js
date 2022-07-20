// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { Canceler, IContext } from '@sabl/context';
import { Connection, Pool, PoolConnection } from 'mysql2';

async function getPoolConnection_Cancelable(
  pool: Pool,
  clr: Canceler
): Promise<PoolConnection> {
  return new Promise((resolve, reject) => {
    let timedOut = false;
    let resolved = false;

    const handle: {
      onCancel: () => void;
    } = { onCancel: null! };

    handle.onCancel = () => {
      clr.off(handle.onCancel);
      if (resolved) {
        // Already resolved
        return;
      }
      timedOut = true;
      reject(
        new Error('Context was canceled before a connection was available')
      );
    };

    clr.onCancel(handle.onCancel);

    pool.getConnection((err, con) => {
      if (timedOut) {
        // Already rejected.
        // Be sure to release the con back to the pool!
        con.release();
        return;
      }

      resolved = true;
      clr.off(handle.onCancel);
      if (err != null) {
        return reject(err);
      }
      return resolve(con);
    });
  });
}

export function getPoolConnection(
  ctx: IContext,
  pool: Pool
): Promise<PoolConnection> {
  const clr = ctx.canceler;
  if (clr != null) {
    return getPoolConnection_Cancelable(pool, clr);
  }

  return new Promise((resolve, reject) => {
    pool.getConnection((err, con) => {
      if (err != null) {
        return reject(err);
      }
      return resolve(con);
    });
  });
}

export async function usePoolConnection(
  ctx: IContext,
  pool: Pool,
  cb: (con: PoolConnection) => Promise<void>
): Promise<void> {
  const con = await getPoolConnection(ctx, pool);
  try {
    await cb(con);
  } finally {
    con.release();
  }
}

export function closeConnection(
  con: Connection,
  pool?: Pool,
  kill = false
): Promise<void> {
  if (kill) {
    con.destroy();
    return Promise.resolve();
  }

  if (pool) {
    (<PoolConnection>con).release();
    return Promise.resolve();
  }

  return new Promise((resolve, reject) => {
    con.end((err) => {
      if (err != null) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

export async function cancelQuery(
  ctx: IContext,
  con: Connection,
  pool: Pool
): Promise<void> {
  const threadId = con.threadId;
  // console.log(`KILL QUERY ${threadId}`);
  await usePoolConnection(ctx, pool, async (con) => {
    try {
      await con.promise().execute(`KILL QUERY ${threadId}`);
    } catch (err) {
      console.error(err);
    }
  });
}
