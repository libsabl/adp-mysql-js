// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

/* eslint-disable @typescript-eslint/no-non-null-assertion */

import { Canceler, IContext } from '@sabl/context';
import {
  ColumnInfo,
  DbConn,
  DbPool,
  DbTxn,
  PlainObject,
  Result,
  Row,
  Rows,
} from './db-api';
import { StorageKind, StorageMode, TxnOptions } from './storage-api';

import { Connection, QueryError, Pool, Query } from 'mysql2';
import { cancelQuery, closeConnection, usePoolConnection } from './mysql-util';
import { ObjectRow } from './object-row';
import { ColumnDefinition, FieldFlags, parseType } from './mysql-types';
import { hasFlag, PromiseHandle } from './util';

const highWater = 100;
const resume = 75;

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

function openRows(
  ctx: IContext,
  con: Connection,
  sql: string,
  ...params: unknown[]
): Promise<Rows> {}

export class MySQLRows implements Rows {
  readonly #con: Connection;
  readonly #pool: Pool;
  readonly #keepOpen: boolean;
  readonly #buf: PlainObject[] = [];

  #row: Row | null = null;
  #err: QueryError | null = null;
  #columns: ColumnInfo[] | null = null;
  #fieldNames: string[] | null = null;
  #done = false;
  #paused = false;

  #waitNext: PromiseHandle<boolean> | null = null;
  #waitClose: PromiseHandle<void> | null = null;
  #waitCols: PromiseHandle<string[]> | null = null;
  #waitColTypes: PromiseHandle<ColumnInfo[]> | null = null;
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

  #onFields(fields: ColumnDefinition[]) {
    this.#columns = fields.map(toColInfo);
    this.#fieldNames = fields.map((f) => f.name);
    const wc = this.#waitCols;
    if (wc != null) {
      this.#waitCols = null;
      wc.resolve(this.#fieldNames);
    }

    const wct = this.#waitColTypes;
    if (wct != null) {
      this.#waitColTypes = null;
      wct.resolve(this.#columns.concat());
    }
  }

  #pushRow(row: PlainObject) {
    if (this.#canceling) {
      // console.log('Ignoring row: cancelling');
      return;
    }

    if (this.#waitNext) {
      if (this.#buf.length) {
        throw new Error('Invalid state: waiting on non-empty buffer');
      }
      // Already waiting for a row. Load
      // it and resolve next() promise
      this.#row = ObjectRow.create(row, this.#fieldNames!);
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
    return cancelQuery(this.#con, this.#pool);
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

  columns(): Promise<string[]> {
    if (this.#fieldNames == null) {
      const pCols = (this.#waitCols = new PromiseHandle<string[]>()).promise;
      return pCols;
    }
    return Promise.resolve(this.#fieldNames.concat());
  }

  columnTypes(): Promise<ColumnInfo[]> {
    if (this.#columns == null) {
      const pColInfo = (this.#waitColTypes = new PromiseHandle<ColumnInfo[]>())
        .promise;
      return pColInfo;
    }
    return Promise.resolve(this.#columns.concat());
  }

  next(): Promise<boolean> {
    if (this.#err) return Promise.reject(this.#err);
    if (this.#buf.length) {
      this.#row = ObjectRow.create(this.#buf.shift()!, this.#fieldNames!);
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
    return (this.#waitNext = new PromiseHandle<boolean>()).promise;
  }
}

/*
export class MySQLTxn implements DbTxn {
  readonly #con: Connection;
  readonly #pool: Pool;
  readonly #keepOpen: boolean;

  constructor(con: Connection, pool: Pool, keepOpen: boolean) {
    this.#con = con;
  }

  get mode(): StorageMode {
    return StorageMode.txn;
  }
  get kind(): string {
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
*/

export class MySQLPool implements DbPool {
  readonly #pool: Pool;

  constructor(pool: Pool) {
    this.#pool = pool;
  }

  get mode(): StorageMode {
    return StorageMode.pool;
  }
  get kind(): string {
    return StorageKind.db;
  }

  conn(ctx: IContext): Promise<DbConn> {
    throw new Error('Method not implemented.');
  }

  queryRow(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Row | null> {
    let row: Row;
    usePoolConnection(pool, (con) => {
      const qry = con.query();
    });
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
