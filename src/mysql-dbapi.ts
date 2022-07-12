// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

/* eslint-disable @typescript-eslint/no-non-null-assertion */

import { Canceler, Context, IContext } from '@sabl/context';
import {
  ColumnInfo,
  DbConn,
  DbPool,
  DbTxn,
  NamedParam,
  PlainObject,
  Result,
  Rows,
} from './db-api';
import {
  IsolationLevel,
  StorageKind,
  StorageMode,
  TxnOptions,
} from './storage-api';

import {
  Connection,
  QueryError,
  Pool,
  Query,
  PoolConnection,
  PoolOptions,
  createPool,
} from 'mysql2';
import {
  cancelQuery,
  closeConnection,
  getPoolConnection,
  usePoolConnection,
} from './mysql-util';
import { ColumnDefinition, FieldFlags, parseType } from './mysql-types';
import { hasFlag, PromiseHandle } from './util';
import { Row } from './row';

const highWater = 100;
const resume = 75;

function makeRow(src: PlainObject, cols: string[]): Row {
  return Row.fromObject(src, cols);
}

function toColInfo(col: ColumnDefinition): ColumnInfo {
  const [typeName, length] = parseType(
    col.columnType,
    col.flags,
    col.columnLength
  );

  let decimalSize: { precision: number; scale: number } | undefined = undefined;
  if (typeName === 'DECIMAL') {
    decimalSize = {
      precision: col.columnLength - 2,
      scale: col.decimals,
    };
  }

  return {
    name: col.name,
    typeName,
    nullable: !hasFlag(col.flags, FieldFlags.NOT_NULL),
    length: length || undefined,
    decimalSize,
  };
}

async function openRows(
  ctx: IContext,
  con: Connection,
  pool: Pool,
  keepOpen: boolean,
  sql: string,
  ...params: unknown[]
): Promise<Rows> {
  const inputs = [];
  for (const val of params) {
    if ((<NamedParam>val).name !== undefined) {
      inputs.push((<NamedParam>val).value);
    } else {
      inputs.push(val);
    }
  }

  const qry = con.query(sql, inputs);
  const rows = new MySQLQuery(
    qry,
    con,
    pool,
    keepOpen,
    ctx.canceler || undefined
  );

  const err = await rows.ready();
  if (err != null) {
    await rows.close();
    throw err;
  }

  return rows;
}

async function execStmt(
  ctx: IContext,
  con: Connection,
  pool: Pool,
  keepOpen: boolean,
  sql: string,
  ...params: unknown[]
): Promise<Result> {
  const inputs = [];
  for (const val of params) {
    if ((<NamedParam>val).name !== undefined) {
      inputs.push((<NamedParam>val).value);
    } else {
      inputs.push(val);
    }
  }

  const qry = con.query(sql, inputs);
  const rows = new MySQLQuery(
    qry,
    con,
    pool,
    keepOpen,
    ctx.canceler || undefined
  );

  const err = await rows.ready();
  if (err != null) {
    await rows.close();
    throw err;
  }

  const result = rows.result();
  return {
    lastId: result.insertId,
    rowsAffected: result.affectedRows,
  };
}

interface MySQLResult {
  fieldCount: number;
  affectedRows: number;
  insertId: number;
  info: string;
  serverStatus: number;
}

export class MySQLQuery implements Rows {
  readonly #con: Connection;
  readonly #pool: Pool;
  readonly #keepOpen: boolean;
  readonly #buf: PlainObject[] = [];

  #row: Row | null = null;
  #err: QueryError | null = null;
  #columns: ColumnInfo[] | null = null;
  #fieldNames: string[] | null = null;
  #ready = false;
  #done = false;
  #paused = false;
  #isExec = false;
  #result: MySQLResult | null = null;

  #waitNext: PromiseHandle<boolean> | null = null;
  #waitClose: PromiseHandle<void> | null = null;
  #waitReady: PromiseHandle<Error | null> | null = null;
  #canceling = false;

  constructor(
    qry: Query,
    con: Connection,
    pool: Pool,
    keepOpen: boolean,
    clr?: Canceler
  ) {
    this.#con = con;
    this.#pool = pool;
    this.#keepOpen = keepOpen;

    if (clr != null) {
      clr.onCancel(this.#cancel.bind(this));
    }

    qry.on('error', this.#error.bind(this));
    qry.on('fields', this.#onFields.bind(this));
    qry.on('result', this.#pushRow.bind(this));
    qry.on('end', this.#end.bind(this));
  }

  async *[Symbol.asyncIterator](): AsyncIterator<Row, any, undefined> {
    try {
      while (await this.next()) {
        yield this.row;
      }
    } finally {
      await this.close();
    }
  }

  get row(): Row {
    if (this.#row == null) {
      throw new Error('No row loaded. Call next()');
    }

    return this.#row;
  }

  get err(): Error | null {
    return this.#err;
  }

  /**
   * Resolves when the columns have been received, or on error.
   * Note that an error is *resolved*, not rejected. Caller
   * can decide how to respond to error.
   */
  ready(): Promise<Error | null> {
    if (this.#ready) {
      return Promise.resolve(this.#err);
    }
    if (this.#waitReady != null) {
      return this.#waitReady.promise;
    }
    return (this.#waitReady = new PromiseHandle<Error | null>()).promise;
  }

  #error(err: QueryError) {
    this.#err = err;

    if (!this.#ready) {
      this.#resolveReady(err);
    }

    if (err.code === 'ER_QUERY_INTERRUPTED') {
      if (this.#canceling) {
        console.log('Ignoring expected ER_QUERY_INTERRUPTED error');
        // Intentional KILL QUERY
        return this.#end();
      }
    }

    const pNext = this.#waitNext;
    if (pNext != null) {
      this.#waitNext = null;
      pNext.reject(this.#err);
    }

    const pClose = this.#waitClose;
    if (pClose != null) {
      this.#waitClose = null;
      pClose.reject(err);
    }
  }

  #resolveReady(err: Error | null) {
    this.#ready = true;
    const wReady = this.#waitReady;
    if (wReady != null) {
      this.#waitReady = null;
      wReady.resolve(err);
    }
  }

  #onFields(fields: ColumnDefinition[]) {
    if (fields == null) {
      // OK -- Exec query
      this.#isExec = true;
      return;
    }

    this.#columns = fields.map(toColInfo);
    this.#fieldNames = fields.map((f) => f.name);
    if (!this.#ready) {
      this.#resolveReady(null);
    }
  }

  #pushRow(row: PlainObject) {
    if (this.#canceling) {
      // console.log('Ignoring row: cancelling');
      return;
    }

    // row is actually the exec result
    if (this.#isExec) {
      this.#result = <MySQLResult>(<unknown>row);
      if (!this.#ready) {
        this.#resolveReady(null);
      }
      if (this.#waitNext) {
        this.#resolveNext(false);
      }
      return;
    }

    if (this.#waitNext) {
      if (this.#buf.length) {
        throw new Error('Invalid state: waiting on non-empty buffer');
      }
      // Already waiting for a row. Load
      // it and resolve next() promise
      this.#row = makeRow(row, this.#fieldNames!);
      return this.#resolveNext(true);
    }

    // Not currently waiting
    const buf = this.#buf;
    buf.push(row);
    if (buf.length >= highWater) {
      if (this.#paused) {
        console.log(`BUF(${buf.length}): already paused`);
      } else {
        console.log(`BUF(${buf.length}): pausing`);
        this.#con.pause();
        this.#paused = true;
      }
    }
  }

  #end() {
    this.#done = true;
    if (this.#waitNext) {
      return this.#resolveNext(false);
    }
    const pClose = this.#waitClose;
    if (pClose != null) {
      this.#waitClose = null;
      pClose.resolve();
    }
  }

  #resolveNext(ok: boolean): void {
    const pNext = this.#waitNext!;
    this.#waitNext = null;
    pNext.resolve(ok);
  }

  #cancel(): Promise<void> {
    this.#canceling = true;

    if (!this.#keepOpen) {
      // Just close the connection
      return closeConnection(this.#con, this.#pool);
    }
    return cancelQuery(Context.background, this.#con, this.#pool);
  }

  async close(): Promise<void> {
    if (this.#done) {
      return Promise.resolve();
    }
    const pClose = (this.#waitClose = new PromiseHandle<void>());
    await this.#cancel();
    if (this.#paused) {
      this.#con.resume();
    }
    if (this.#done) {
      this.#waitClose = null;
      return Promise.resolve();
    }
    await pClose.promise;
  }

  columns(): string[] {
    if (this.#fieldNames == null) {
      if (this.#ready) {
        throw this.#err;
      }
      throw new Error('Rows object not ready yet. Await ready()');
    }
    return this.#fieldNames.concat();
  }

  columnTypes(): ColumnInfo[] {
    if (this.#columns == null) {
      if (this.#ready) {
        throw this.#err;
      }
      throw new Error('Rows object not ready yet. Await ready()');
    }
    return this.#columns.concat();
  }

  next(): Promise<boolean> {
    if (this.#err) return Promise.reject(this.#err);
    if (this.#buf.length) {
      this.#row = makeRow(this.#buf.shift()!, this.#fieldNames!);
      if (this.#paused && this.#buf.length <= resume) {
        console.log('resuming');
        this.#con.resume();
        this.#paused = false;
      }
      return Promise.resolve(true);
    }
    if (this.#done) {
      return Promise.resolve(false);
    }
    if (this.#isExec) {
      return Promise.resolve(false);
    }
    return (this.#waitNext = new PromiseHandle<boolean>()).promise;
  }

  result(): MySQLResult {
    if (this.#result == null) {
      if (this.#ready) {
        if (!this.#isExec) {
          throw new Error('Query returned rows, not a exec result');
        }
        throw this.#err;
      }
      throw new Error('Result object not ready yet. Await ready()');
    }
    return this.#result;
  }
}

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
    return StorageKind.db;
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
    return StorageKind.db;
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
        row = rows.row;
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
    return StorageKind.db;
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
          row = rows.row;
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
