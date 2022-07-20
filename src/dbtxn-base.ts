// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { IContext } from '@sabl/context';
import { StorageKind, StorageMode } from '@sabl/storage-pool';
import { IsolationLevel, TxnOptions } from '@sabl/txn';
import { DbConn, DbTxn, Result, Row, Rows } from '@sabl/db-api';
import { AsyncCompleteEmitter, AsyncEventEmitter } from './async-event-emitter';

/**
 * Generic implementation for the lifecycle of a transaction.
 *
 * The connection object provided to the constructor will always
 * be closed when the transaction is fails to start, is canceled,
 * or completes, so implementers should provide a temporary
 * connection wrapper that does not close the underlying protocol
 * connection when 'close' is called.
 *
 * The default implementations of _commit and _rollback simply
 * execute 'COMMIT' and 'ROLLBACK', respectively, on the connection
 * provided to the transaction constructor.
 *
 * If the underlying platform supports nested transactions, implementers
 * should override _beginTxn and provide supportsNested = true to the
 * protected constructor.
 *
 */
export class DbTxnBase
  extends AsyncEventEmitter
  implements DbTxn, AsyncCompleteEmitter<DbTxn>
{
  #closed = false;
  protected started = false;
  protected con: DbConn;
  protected readonly opts: TxnOptions | null;

  protected constructor(
    txnCon: DbConn & AsyncCompleteEmitter<DbConn>,
    opts: TxnOptions | undefined,
    supportsNested = false
  ) {
    super('complete');
    this.con = txnCon;
    this.opts = opts || null;

    // Bubble up complete event from inner connection
    txnCon.on('complete', () => this.emit('complete', null, [this]));

    if (supportsNested) {
      Object.defineProperty(this, 'beginTxn', this._beginTxn);
    }
  }

  get mode(): StorageMode {
    return StorageMode.txn;
  }

  get kind(): string {
    return StorageKind.rdb;
  }

  #checkStatus() {
    if (this.#closed) {
      throw new Error('Transaction is already closed');
    }
    if (!this.started) {
      throw new Error('Transaction is not yet started');
    }
  }

  /**
   * Override to indicate which isolation levels are supported.
   * Default supports all levels. IsolationLevel.default is
   * always supported and skips SET TRANSACTION ISOLATION
   * LEVEL statement.
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  supportsIsolationLevel(level: IsolationLevel): boolean {
    return true;
  }

  /**
   * Override to indicate whether explicit READ ONLY / READ WRITE
   * mode is supported. Default is true.
   */
  supportsReadOnly(): boolean {
    return true;
  }

  /**
   * Get the SQL key word(s) for the provided
   * isolation level.
   */
  isolationLevelKeyword(level: IsolationLevel): string {
    switch (level) {
      case IsolationLevel.readUncommitted:
        return 'READ UNCOMMITTED';
      case IsolationLevel.readCommitted:
        return 'READ COMMITTED';
      case IsolationLevel.writeCommitted:
        return 'WRITE COMMITTED';
      case IsolationLevel.repeatableRead:
        return 'REPEATABLE READ';
      case IsolationLevel.snapshot:
        return 'SNAPSHOT';
      case IsolationLevel.serializable:
        return 'SERIALIZABLE';
      case IsolationLevel.linearizable:
        return 'LINEARIZABLE';
      default:
        throw new Error('Unsupported isolation level');
    }
  }

  /**  */
  async start(ctx: IContext): Promise<void> {
    const con = this.con;
    const opts = this.opts;
    try {
      if (opts != null) {
        const level = opts.isolationLevel;
        if (level != null && level != IsolationLevel.default) {
          if (!this.supportsIsolationLevel(level)) {
            throw new Error('Unsupported isolation level');
          }
          const keyWord = this.isolationLevelKeyword(level);
          await con.exec(ctx, `SET TRANSACTION ISOLATION LEVEL ${keyWord}`);
        }
      }

      if (opts != null && typeof opts.readOnly === 'boolean') {
        if (!this.supportsReadOnly()) {
          throw new Error('Explicit read/write mode not supported');
        }
        const rwMode = opts.readOnly === true ? 'READ ONLY' : 'READ WRITE';
        await con.exec(ctx, `START TRANSACTION ${rwMode}`);
      } else {
        await con.exec(ctx, 'START TRANSACTION');
      }

      this.started = true;
      return;
    } catch (err) {
      console.log(err);
      await this.con.close();
      throw err;
    }
  }

  exec(ctx: IContext, sql: string, ...params: unknown[]): Promise<Result> {
    this.#checkStatus();
    return this.con.exec(ctx, sql, ...params);
  }

  queryRow(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Row | null> {
    this.#checkStatus();
    return this.con.queryRow(ctx, sql, ...params);
  }

  query(ctx: IContext, sql: string, ...params: unknown[]): Promise<Rows> {
    this.#checkStatus();
    return this.con.query(ctx, sql, ...params);
  }

  async commit(ctx: IContext): Promise<void> {
    this.#checkStatus();
    this.#closed = true;

    // Start the commit
    const pCommit = this._commit(ctx);

    // Tell the connection to not allow any more operations
    const pClose = this.con.close();

    // Wait till both operations have resolved
    await Promise.all([pCommit, pClose]);
  }

  async rollback(ctx: IContext): Promise<void> {
    this.#checkStatus();
    this.#closed = true;

    // Start the rollback
    const pRollback = this._rollback(ctx);

    // Tell the connection to not allow any more operations
    const pClose = this.con.close();

    // Wait till both operations have resolved
    await Promise.all([pRollback, pClose]);
  }

  protected async _commit(ctx: IContext): Promise<void> {
    await this.con.exec(ctx, 'COMMIT');
  }

  protected async _rollback(ctx: IContext): Promise<void> {
    await this.con.exec(ctx, 'ROLLBACK');
  }

  protected _beginTxn(
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    ctx: IContext,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    opts?: TxnOptions | undefined
  ): Promise<DbTxn> {
    throw new Error('Nested transactions not supported');
  }
}
