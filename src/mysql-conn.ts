// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { IContext } from '@sabl/context';
import { TxnOptions } from '@sabl/txn';
import { DbConn, Result, Row, Rows } from '@sabl/db-api';
import { AsyncCompleteEmitter } from './async-event-emitter';
import { Pool, PoolConnection } from 'mysql2';
import { DbConnBase } from './dbconn-base';
import { execStmt, openRows } from './mysql-query';
import { MySQLTxn } from './mysql-txn';

const isMySQLConn = Symbol('MySQLConn');

export class MySQLConn extends DbConnBase {
  static isMySQLConn(con: DbConn | MySQLConn): con is MySQLConn {
    return isMySQLConn in con;
  }

  protected readonly [isMySQLConn] = true;
  #con: PoolConnection;
  #pool: Pool;

  constructor(
    ctx: IContext,
    keepOpen: boolean,
    con: PoolConnection,
    pool: Pool
  ) {
    super(ctx, keepOpen);
    this.#con = con;
    this.#pool = pool;
  }

  protected _exec(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Result> {
    return execStmt(ctx, this.#con, this.#pool, true, sql, ...params);
  }

  protected async _queryRow(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Row | null> {
    const rows = await openRows(
      ctx,
      this.#con,
      this.#pool,
      true,
      sql,
      ...params
    );
    try {
      const ok = await rows.next();
      return ok ? Row.clone(rows.row) : null;
    } finally {
      await rows.close();
    }
  }

  protected _query(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Rows & AsyncCompleteEmitter<Rows>> {
    return openRows(ctx, this.#con, this.#pool, this.keepOpen, sql, ...params);
  }

  protected _createTxn(ctx: IContext, opts: TxnOptions | undefined): MySQLTxn {
    const txnCon = new MySQLConn(ctx, this.keepOpen, this.#con, this.#pool);
    return new MySQLTxn(txnCon, opts);
  }

  protected cleanUp(): void {
    this.#con.release();
    this.#con = <PoolConnection>null!;
    this.#pool = <Pool>null!;
  }
}
