// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

/* eslint-disable @typescript-eslint/no-non-null-assertion */

import { Context, IContext } from '@sabl/context';
import { DbConn, DbPool, DbTxn, Result, Rows, Row } from '@sabl/db-api';
import { StorageKind, StorageMode } from '@sabl/storage-pool';
import { IsolationLevel, TxnOptions } from '@sabl/txn';

import { Pool, PoolConnection, PoolOptions, createPool } from 'mysql2';
import { execStmt, openRows } from './mysql-query';
import { getPoolConnection, usePoolConnection } from './mysql-util';

export class MySQLTxn implements DbTxn {
  readonly #con: MySQLConn;
  readonly #keepOpen: boolean;
  #ready = false;
  #closed = false;

  /** Initializes but does not begin the transaction */
  constructor(msCon: MySQLConn, keepOpen: boolean) {
    this.#con = msCon;
    this.#keepOpen = keepOpen;
  }

  get mode(): StorageMode {
    return StorageMode.txn;
  }

  get kind(): string {
    return StorageKind.rdb;
  }

  async begin(
    ctx: IContext,
    opts?: TxnOptions | undefined
  ): Promise<Error | null> {
    const con = this.#con;
    try {
      opts = opts || {
        isolationLevel: IsolationLevel.default,
        readOnly: false,
      };

      let isolationLevel = 'REPEATABLE READ';
      switch (opts.isolationLevel) {
        case IsolationLevel.default:
        case IsolationLevel.repeatableRead:
          isolationLevel = 'REPEATABLE READ';
          break;
        case IsolationLevel.readCommitted:
          isolationLevel = 'READ COMMITTED';
          break;
        case IsolationLevel.readUncommitted:
          isolationLevel = 'READ UNCOMMITTED';
          break;
        case IsolationLevel.serializable:
          isolationLevel = 'SERIALIZABLE';
          break;
        default:
          throw new Error('Unsupported isolation level');
      }

      await con.exec(ctx, `SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`);

      const rwMode = opts.readOnly === true ? 'READ ONLY' : 'READ WRITE';
      await con.exec(ctx, `START TRANSACTION ${rwMode}`);
      this.#ready = true;
      return null;
    } catch (err) {
      if (!this.#keepOpen) {
        await this.#con.close();
      }
      return <Error>err;
    }
  }

  #checkStatus(): void {
    if (!this.#ready) {
      throw new Error('Transaction is not yet ready');
    }
    if (this.#closed) {
      throw new Error('Transaction is already complete');
    }
  }

  queryRow(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Row | null> {
    this.#checkStatus();
    return this.#con.queryRow(ctx, sql, ...params);
  }

  query(ctx: IContext, sql: string, ...params: unknown[]): Promise<Rows> {
    this.#checkStatus();
    return this.#con.query(ctx, sql, ...params);
  }

  exec(ctx: IContext, sql: string, ...params: unknown[]): Promise<Result> {
    this.#checkStatus();
    return this.#con.exec(ctx, sql, ...params);
  }

  async commit(): Promise<void> {
    this.#checkStatus();
    this.#closed = true;
    try {
      await this.#con.exec(Context.background, 'COMMIT');
    } finally {
      if (!this.#keepOpen) {
        await this.#con.close();
      }
    }
  }

  async rollback(): Promise<void> {
    this.#checkStatus();
    this.#closed = true;
    try {
      await this.#con.exec(Context.background, 'ROLLBACK');
    } finally {
      if (!this.#keepOpen) {
        await this.#con.close();
      }
    }
  }
}

export class MySQLConn implements DbConn {
  readonly #pool: Pool;
  readonly #con: PoolConnection;
  #closed = false;

  constructor(con: PoolConnection, pool: Pool) {
    this.#con = con;
    this.#pool = pool;
  }

  get mode(): StorageMode {
    return StorageMode.conn;
  }

  get kind(): string {
    return StorageKind.rdb;
  }

  close(): Promise<void> {
    this.#con.release();
    this.#closed = true;
    return Promise.resolve();
  }

  async queryRow(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Row | null> {
    if (this.#closed) {
      throw new Error('Connection is closed');
    }
    let row: Row | null = null;
    const pool = this.#pool;
    const con = this.#con;
    const rows = await openRows(ctx, con, pool, true, sql, ...params);
    try {
      const ok = await rows.next();
      if (ok) {
        row = Row.clone(rows.row);
      }
    } finally {
      await rows.close();
    }
    return row;
  }

  query(ctx: IContext, sql: string, ...params: unknown[]): Promise<Rows> {
    if (this.#closed) {
      throw new Error('Connection is closed');
    }
    const pool = this.#pool;
    const con = this.#con;
    return openRows(ctx, con, pool, true, sql, ...params);
  }

  exec(ctx: IContext, sql: string, ...params: unknown[]): Promise<Result> {
    if (this.#closed) {
      throw new Error('Connection is closed');
    }
    const pool = this.#pool;
    const con = this.#con;
    return execStmt(ctx, con, pool, true, sql, ...params);
  }

  async beginTxn(ctx: IContext, opts?: TxnOptions | undefined): Promise<DbTxn> {
    if (this.#closed) {
      throw new Error('Connection is closed');
    }
    const txn = new MySQLTxn(this, true);
    const err = await txn.begin(ctx, opts);
    if (err != null) {
      throw err;
    }
    return txn;
  }
}

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
    return new MySQLConn(con, this.#pool);
  }

  close(): Promise<void> {
    this.#closed = true;
    return this.#pool.promise().end();
  }

  async queryRow(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Row | null> {
    if (this.#closed) {
      throw new Error('Pool is closed');
    }
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
    if (this.#closed) {
      throw new Error('Pool is closed');
    }
    const pool = this.#pool;
    const con = await getPoolConnection(ctx, pool);
    return openRows(ctx, con, pool, false, sql, ...params);
  }

  async exec(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Result> {
    if (this.#closed) {
      throw new Error('Pool is closed');
    }
    let result: Result | null = null;
    const pool = this.#pool;

    await usePoolConnection(ctx, pool, async (con) => {
      result = await execStmt(ctx, con, pool, false, sql, ...params);
    });

    return result!;
  }

  async beginTxn(ctx: IContext, opts?: TxnOptions | undefined): Promise<DbTxn> {
    if (this.#closed) {
      throw new Error('Pool is closed');
    }

    const pool = this.#pool;
    const con = await getPoolConnection(ctx, pool);
    const msCon = new MySQLConn(con, pool);
    const txn = new MySQLTxn(msCon, false);
    const err = await txn.begin(ctx, opts);
    if (err != null) {
      throw err;
    }
    return txn;
  }
}
