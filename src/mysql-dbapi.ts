// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { Canceler, IContext } from '@sabl/context';
import { DbConn, DbPool, DbTxn, Result, Row, Rows } from './db-api';
import { StorageKind, StorageMode, TxnOptions } from './storage-api';

import { Connection, MysqlError, Pool, Query } from 'mysql';

type FnReject = (reason: unknown) => void;
type FnResolve<T> = (value: T | PromiseLike<T>) => void;
const highWater = 100;

function closeConnection(con: Connection, kill = false): Promise<void> {
  if (kill) {
    con.destroy();
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

function cancelQuery(con: Connection, pool: Pool): Promise<void> {
  const threadId = con.threadId;
  return new Promise((resolve, reject) => {
    pool.query(`KILL QUERY ${threadId}`, (err) => {
      if (err != null) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

class MySQLRows implements Rows {
  readonly #qry: Query;
  readonly #con: Connection;
  readonly #pool: Pool;
  readonly #keepOpen: boolean;
  readonly #clr: Canceler | null;
  readonly #buf: Row[] = [];

  #row: Row | null = null;
  #err: MysqlError | null = null;
  #fields: string[] | null = null;
  #all = false;

  #nextReject: FnReject | null = null;
  #nextResolve: FnResolve<boolean> | null = null;

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
    qry.on('fields', (fields) => {
      this.#fields = fields.map((f) => f.name);
    });
    qry.on('result', this.#pushRow.bind(this));
    qry.on('end', this.#done.bind(this));
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

  #error(err: MysqlError) {
    this.#err = err;

    const rej = this.#nextReject;
    if (rej != null) {
      this.#nextResolve = null;
      this.#nextReject = null;
      rej(this.#err);
    }
  }

  #pushRow(row: unknown) {
    if (this.#nextResolve) {
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
      this.#con.pause();
    }
  }

  #done() {
    this.#all = true;
    if (this.#nextResolve) {
      return this.#resolveNext(false);
    }
  }

  #resolveNext(ok: boolean): void {
    const r = <FnResolve<boolean>>this.#nextResolve;
    this.#nextResolve = null;
    this.#nextReject = null;
    r(ok);
  }

  #cancel(): Promise<void> {
    if (!this.#keepOpen) {
      // Just close the connection
      return closeConnection(this.#con);
    }
    return cancelQuery(this.#con, this.#pool);
  }

  close(): Promise<void> {
    this.#cancel();
    return Promise.resolve();
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
      return Promise.resolve(true);
    }
    if (this.#all) {
      return Promise.resolve(false);
    }
    return new Promise((resolve, reject) => {
      this.#nextResolve = resolve;
      this.#nextReject = reject;
    });
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
