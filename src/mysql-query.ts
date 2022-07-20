// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

/* eslint-disable @typescript-eslint/no-non-null-assertion */

import { Canceler, Context, IContext } from '@sabl/context';
import {
  ColumnInfo,
  PlainObject,
  Rows,
  Row,
  Result,
  NamedParam,
} from '@sabl/db-api';

import { Connection, QueryError, Pool, Query } from 'mysql2';
import { cancelQuery, closeConnection } from './mysql-util';
import { ColumnDefinition, FieldFlags, parseType } from './mysql-types';
import { hasFlag, PromiseHandle } from './promise-handle';
import { AsyncCompleteEmitter, AsyncEventEmitter } from './async-event-emitter';

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

interface MySQLResult {
  fieldCount: number;
  affectedRows: number;
  insertId: number;
  info: string;
  serverStatus: number;
}

export class MySQLQuery
  extends AsyncEventEmitter
  implements Rows, AsyncCompleteEmitter<Rows>
{
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
    super('complete');
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
    this.emit('complete', null, [this]);
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

  get columns(): string[] {
    if (this.#fieldNames == null) {
      if (this.#ready) {
        throw this.#err;
      }
      throw new Error('Rows object not ready yet. Await ready()');
    }
    return this.#fieldNames.concat();
  }

  get columnTypes(): ColumnInfo[] {
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

export async function openRows(
  ctx: IContext,
  con: Connection,
  pool: Pool,
  keepOpen: boolean,
  sql: string,
  ...params: unknown[]
): Promise<Rows & AsyncCompleteEmitter<Rows>> {
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

export async function execStmt(
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
