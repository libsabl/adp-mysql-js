// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { Connection, Pool, PoolConnection } from 'mysql2';

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

export async function cancelQuery(con: Connection, pool: Pool): Promise<void> {
  const threadId = con.threadId;
  console.log(`KILL QUERY ${threadId}`);
  await usePoolConnection(pool, async (con) => {
    try {
      await con.promise().execute(`KILL QUERY ${threadId}`);
    } catch (err) {
      console.error(err);
    }
  });
}

export function getPoolConnection(pool: Pool): Promise<PoolConnection> {
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
  pool: Pool,
  cb: (con: PoolConnection) => Promise<void>
): Promise<void> {
  const con = await getPoolConnection(pool);
  try {
    await cb(con);
  } finally {
    con.release();
  }
}
