// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

/* eslint-disable @typescript-eslint/no-non-null-assertion */

import { Canceler, IContext } from '@sabl/context';
import { DbConn, DbPool, DbTxn, Result, Row, Rows } from './db-api';
import { StorageKind, StorageMode, TxnOptions } from './storage-api';

import { Connection, QueryError, Pool, Query, Field } from 'mysql2';
import { cancelQuery, closeConnection } from './mysql-util';

type FnReject = (reason: unknown) => void;
type FnResolve<T> = (value: T | PromiseLike<T>) => void;
const highWater = 100;
const resume = 75;

class PromiseHandle<T> {
  constructor() {
    let res: FnResolve<T>;
    let rej: FnReject;

    this.#promise = new Promise<T>((resolve, reject) => {
      res = resolve;
      rej = reject;
    });

    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    this.#resolve = res!;
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    this.#reject = rej!;
  }

  readonly #resolve: FnResolve<T>;
  resolve(value: T | PromiseLike<T>): void {
    return this.#resolve(value);
  }

  readonly #reject: FnReject;
  reject(reason?: unknown): void {
    return this.#reject(reason);
  }

  readonly #promise: Promise<T>;
  get promise(): Promise<T> {
    return this.#promise;
  }
}

export class MySQLRows implements Rows {
  readonly #qry: Query;
  readonly #con: Connection;
  readonly #pool: Pool;
  readonly #keepOpen: boolean;
  readonly #clr: Canceler | null;
  readonly #buf: Row[] = [];

  #row: Row | null = null;
  #err: QueryError | null = null;
  #fields: string[] | null = null;
  #done = false;
  #paused = false;

  #next: PromiseHandle<boolean> | null = null;
  #close: PromiseHandle<void> | null = null;
  #canceling = false;

  constructor(
    qry: Query,
    con: Connection,
    pool: Pool,
    keepOpen: boolean,
    clr?: Canceler
  ) {
    this.#qry = qry;
    this.#con = con;
    this.#pool = pool;
    this.#keepOpen = keepOpen;
    this.#clr = clr || null;

    if (clr != null) {
      clr.onCancel(this.#cancel.bind(this));
    }

    qry.on('error', this.#error.bind(this));
    qry.on('fields', (fields: unknown) => {
      this.#fields = (<Field[]>fields).map((f) => f.name);
    });
    qry.on('result', this.#pushRow.bind(this));
    qry.on('end', this.#end.bind(this));
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

  #error(err: QueryError) {
    this.#err = err;

    if (err.code === 'ER_QUERY_INTERRUPTED') {
      if (this.#canceling) {
        console.log('Ignoring expected ER_QUERY_INTERRUPTED error');
        // Intentional KILL QUERY
        return this.#end();
      }
    }

    const pNext = this.#next;
    if (pNext != null) {
      this.#next = null;
      pNext.reject(this.#err);
    }

    const pClose = this.#close;
    if (pClose != null) {
      this.#close = null;
      pClose.reject(err);
    }
  }

  #pushRow(row: unknown) {
    if (this.#canceling) {
      // console.log('Ignoring row: cancelling');
      return;
    }

    if (this.#next) {
      if (this.#buf.length) {
        throw new Error('Invalid state: waiting on non-empty buffer');
      }
      // Already waiting for a row. Load
      // it and resolve next() promise
      this.#row = <Row>row;
      return this.#resolveNext(true);
    }

    // Not currently waiting
    const buf = this.#buf;
    buf.push(<Row>row);
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
    if (this.#next) {
      return this.#resolveNext(false);
    }
    const pClose = this.#close;
    if (pClose != null) {
      this.#close = null;
      pClose.resolve();
    }
  }

  #resolveNext(ok: boolean): void {
    const pNext = this.#next!;
    this.#next = null;
    pNext.resolve(ok);
  }

  #cancel(): Promise<void> {
    this.#canceling = true;

    if (!this.#keepOpen) {
      // Just close the connection
      return closeConnection(this.#con);
    }
    return cancelQuery(this.#con, this.#pool);
  }

  async close(): Promise<void> {
    if (this.#done) {
      return Promise.resolve();
    }
    const pClose = (this.#close = new PromiseHandle<void>());
    await this.#cancel();
    if (this.#paused) {
      this.#con.resume();
    }
    if (this.#done) {
      this.#close = null;
      return Promise.resolve();
    }
    await pClose.promise;
  }

  columns(): string[] {
    if (this.#fields == null) {
      throw new Error('Columns not received yet');
    }
    return this.#fields;
  }

  next(): Promise<boolean> {
    if (this.#err) return Promise.reject(this.#err);
    if (this.#buf.length) {
      this.#row = <Row>this.#buf.shift();
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
    return (this.#next = new PromiseHandle<boolean>()).promise;
  }

  values(): unknown[] {
    throw new Error('Method not implemented.');
  }
}

export class MySQLTxn implements DbTxn {
  readonly #con: Connection;

  constructor(con: Connection) {
    this.#con = con;
  }

  get mode(): StorageMode {
    return StorageMode.txn;
  }
  get kind(): number {
    return StorageKind.db;
  }

  queryRow(ctx: IContext, sql: string, ...params: unknown[]): Promise<Row> {
    throw new Error('Method not implemented.');
  }
  query(ctx: IContext, sql: string, ...params: unknown[]): Promise<Rows> {
    throw new Error('Method not implemented.');
  }
  exec(ctx: IContext, sql: string, ...params: unknown[]): Promise<Result> {
    throw new Error('Method not implemented.');
  }
  commit(): Promise<void> {
    throw new Error('Method not implemented.');
  }
  rollback(): Promise<void> {
    throw new Error('Method not implemented.');
  }
}

export class MySQLPool implements DbPool {
  get mode(): StorageMode {
    return StorageMode.pool;
  }
  get kind(): number {
    return StorageKind.db;
  }

  conn(ctx: IContext): Promise<DbConn> {
    throw new Error('Method not implemented.');
  }
  queryRow(ctx: IContext, sql: string, ...params: unknown[]): Promise<Row> {
    throw new Error('Method not implemented.');
  }
  query(ctx: IContext, sql: string, ...params: unknown[]): Promise<Rows> {
    throw new Error('Method not implemented.');
  }
  exec(ctx: IContext, sql: string, ...params: unknown[]): Promise<Result> {
    throw new Error('Method not implemented.');
  }
  beginTxn(ctx: IContext, opts?: TxnOptions | undefined): Promise<DbTxn> {
    throw new Error('Method not implemented.');
  }
}
