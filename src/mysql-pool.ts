// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { IContext } from '@sabl/context';
import { DbConn, DbPool, DbTxn, Result, Row, Rows } from '@sabl/db-api';
import { StorageKind, StorageMode } from '@sabl/storage-pool';
import { TxnOptions } from '@sabl/txn';
import { createPool, Pool, PoolOptions } from 'mysql2';
import { MySQLConn } from './mysql-conn';
import { execStmt, openRows } from './mysql-query';
import { getPoolConnection, usePoolConnection } from './mysql-util';

export class MySQLPool implements DbPool {
  readonly #pool: Pool;
  #closed = false;

  constructor(options: PoolOptions);
  constructor(pool: Pool);
  constructor(poolOrOpts: Pool | PoolOptions) {
    if (typeof (<any>poolOrOpts).execute === 'function') {
      this.#pool = <Pool>poolOrOpts;
    } else {
      this.#pool = createPool(<PoolOptions>poolOrOpts);
    }
  }

  get mode(): StorageMode {
    return StorageMode.pool;
  }

  get kind(): string {
    return StorageKind.rdb;
  }

  async conn(ctx: IContext): Promise<DbConn> {
    if (this.#closed) {
      throw new Error('Pool is closed');
    }
    const con = await getPoolConnection(ctx, this.#pool);
    return new MySQLConn(ctx, true, con, this.#pool);
  }

  #checkStatus() {
    if (this.#closed) {
      throw new Error('Pool is closed');
    }
  }

  close(): Promise<void> {
    if (this.#closed) {
      return Promise.resolve();
    }

    this.#closed = true;
    return this.#pool.promise().end();
  }

  async queryRow(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Row | null> {
    this.#checkStatus();

    let row: Row | null = null;
    const pool = this.#pool;
    await usePoolConnection(ctx, pool, async (con) => {
      const rows = await openRows(ctx, con, pool, false, sql, ...params);
      try {
        const ok = await rows.next();
        if (ok) {
          row = Row.clone(rows.row);
        }
      } finally {
        await rows.close();
      }
    });

    return row;
  }

  async query(ctx: IContext, sql: string, ...params: unknown[]): Promise<Rows> {
    this.#checkStatus();

    const con = await getPoolConnection(ctx, this.#pool);
    return openRows(ctx, con, this.#pool, false, sql, ...params);
  }

  async exec(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Result> {
    this.#checkStatus();

    let result: Result | null = null;
    const pool = this.#pool;
    await usePoolConnection(ctx, pool, async (con) => {
      result = await execStmt(ctx, con, pool, false, sql, ...params);
    });

    return result!;
  }

  async beginTxn(ctx: IContext, opts?: TxnOptions | undefined): Promise<DbTxn> {
    this.#checkStatus();

    const con = await getPoolConnection(ctx, this.#pool);
    const msCon = new MySQLConn(ctx, false, con, this.#pool);
    return msCon.beginTxn(ctx, opts);
  }
}
