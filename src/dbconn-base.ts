// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { Canceler, CancelFunc, IContext, withCancel } from '@sabl/context';
import { StorageKind, StorageMode } from '@sabl/storage-pool';
import { TxnOptions } from '@sabl/txn';
import { DbConn, DbTxn, Result, Row, Rows } from '@sabl/db-api';
import { AsyncCompleteEmitter, AsyncEventEmitter } from './async-event-emitter';
import { PromiseHandle } from './promise-handle';
import { DbTxnBase } from './dbtxn-base';

interface ConnOpBase {
  ctx: IContext;
  reject(reason: unknown): void;
}

interface BeginTxnOperation extends ConnOpBase {
  type: 'beginTxn';
  opts: TxnOptions | undefined;
  resolve(txn: DbTxn): void;
}

interface SqlOperationBase extends ConnOpBase {
  sql: string;
  params: unknown[];
}

interface QueryOperation extends SqlOperationBase {
  type: 'query';
  resolve(rows: Rows): void;
}

interface QueryRowOperation extends SqlOperationBase {
  type: 'queryRow';
  resolve(row: Row | null): void;
}

interface ExecOperation extends SqlOperationBase {
  type: 'exec';
  resolve(result: Result): void;
}

type ConnOperation =
  | BeginTxnOperation
  | QueryOperation
  | QueryRowOperation
  | ExecOperation;

const noOp = () => {
  /* no op */
};

export abstract class DbConnBase
  extends AsyncEventEmitter
  implements DbConn, AsyncCompleteEmitter<DbConn>
{
  protected readonly keepOpen: boolean;

  #closed = false;
  #busy = false;
  #waitClose: PromiseHandle<void> | null = null;
  #canceled = false;

  readonly #rootCtx: IContext;
  readonly #clr: Canceler | null;
  readonly #onCancel: null | (() => void) = null;
  readonly #opQueue: ConnOperation[] = [];

  constructor(ctx: IContext, keepOpen: boolean) {
    super(['complete' /* , 'cancel' */]);
    this.keepOpen = keepOpen;

    this.#rootCtx = ctx;
    const clr = (this.#clr = ctx.canceler || null);
    if (clr != null) {
      const onCancel = (this.#onCancel = () => this.#cancel());
      clr.onCancel(onCancel);
    }
  }

  get mode(): StorageMode {
    return StorageMode.conn;
  }

  get kind(): string {
    return StorageKind.rdb;
  }

  #checkStatus() {
    if (this.#closed) {
      throw new Error('Connection is already closed');
    }
  }

  #cancel() {
    if (this.#canceled) return;

    // Don't allow any more operations to start
    this.close();

    // Mark that the connection is already canceled / canceling
    this.#canceled = true;

    // Clear the queue and reject any pending operations
    const pending = this.#opQueue.splice(0, this.#opQueue.length);
    if (pending.length > 0) {
      for (const op of pending) {
        op.reject(new Error('Connection was canceled'));
      }
    }

    // Ensure queue is checked and final
    // 'complete' event is emitted
    this.#start();
  }

  // Ensure cancellations from the connections's
  // root context are cascaded to any operations
  // started on the context
  #wrapContext(ctx: IContext): [IContext, CancelFunc] {
    if (this.#onCancel == null || this.#clr == null) {
      return [ctx, noOp];
    }
    if (ctx.canceler === this.#clr) {
      // Already same context tree. Will automatically cancel
      return [ctx, noOp];
    }

    // Need to cascade cancellation to operation
    const [wrappedCtx, cancel] = withCancel(ctx);

    // Cascade root connection cancellation to this operation context
    const clr = this.#clr;
    clr.onCancel(cancel);

    // Ensure callback is cleaned up
    const wrappedCancel = () => {
      clr.off(cancel);
      cancel();
    };

    return [wrappedCtx, wrappedCancel];
  }

  close(): Promise<void> {
    if (this.#closed) {
      return Promise.resolve();
    }
    this.#closed = true;
    if (this.#waitClose != null) {
      return this.#waitClose.promise;
    }
    return (this.#waitClose = new PromiseHandle<void>()).promise;
  }

  exec(ctx: IContext, sql: string, ...params: unknown[]): Promise<Result> {
    this.#checkStatus();

    const ph = new PromiseHandle<Result>();
    this.#opQueue.push({
      type: 'exec',
      ctx,
      sql,
      params,
      resolve: ph.resolve,
      reject: ph.reject,
    });

    this.#start();

    return ph.promise;
  }

  queryRow(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Row | null> {
    this.#checkStatus();

    const ph = new PromiseHandle<Row | null>();
    this.#opQueue.push({
      type: 'queryRow',
      ctx,
      sql,
      params,
      resolve: ph.resolve,
      reject: ph.reject,
    });

    this.#start();

    return ph.promise;
  }

  query(ctx: IContext, sql: string, ...params: unknown[]): Promise<Rows> {
    this.#checkStatus();

    const ph = new PromiseHandle<Rows>();
    this.#opQueue.push({
      type: 'query',
      ctx,
      sql,
      params,
      resolve: ph.resolve,
      reject: ph.reject,
    });

    this.#start();

    return ph.promise;
  }

  beginTxn(ctx: IContext, opts?: TxnOptions | undefined): Promise<DbTxn> {
    this.#checkStatus();

    const ph = new PromiseHandle<DbTxn>();
    this.#opQueue.push({
      type: 'beginTxn',
      ctx,
      opts,
      resolve: ph.resolve,
      reject: ph.reject,
    });

    this.#start();

    return ph.promise;
  }

  protected abstract _exec(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Result>;

  protected abstract _queryRow(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Row | null>;

  protected abstract _query(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Rows & AsyncCompleteEmitter<Rows>>;

  protected async _beginTxn(
    ctx: IContext,
    opts?: TxnOptions | undefined
  ): Promise<DbTxn & AsyncCompleteEmitter<DbTxn>> {
    const txn = this._createTxn(ctx, opts);
    await txn.start(ctx);
    return txn;
  }

  /**
   * Create a new DbTxnBase object of the appropriate
   * type. `con` is already a new connection created
   * with `clone()`.
   */
  protected abstract _createTxn(
    ctx: IContext,
    opts: TxnOptions | undefined
  ): DbTxnBase;

  /* release any internal resources */
  protected cleanUp(): void {
    /* no op */
  }

  /**
   * Start the next queued operation.
   * Will clean up the connection and
   * emit 'complete' if there are no more
   * operations and #keepOpen is false
   */
  async #next(): Promise<void> {
    const nextOp = this.#opQueue.shift();
    if (nextOp == null) {
      this.#busy = false;

      if (this.keepOpen) return;
      await this.#done();

      return;
    }

    if (nextOp.type === 'exec') {
      const [ctx, cancel] = this.#wrapContext(nextOp.ctx);
      try {
        const result = await this._exec(ctx, nextOp.sql, ...nextOp.params);
        nextOp.resolve(result);
      } catch (e) {
        nextOp.reject(e);
      } finally {
        cancel();
      }
    } else if (nextOp.type === 'queryRow') {
      const [ctx, cancel] = this.#wrapContext(nextOp.ctx);
      try {
        const row = await this._queryRow(ctx, nextOp.sql, ...nextOp.params);
        nextOp.resolve(row);
      } catch (e) {
        nextOp.reject(e);
      } finally {
        cancel();
      }
    } else if (nextOp.type === 'query') {
      const [ctx, cancel] = this.#wrapContext(nextOp.ctx);
      try {
        const rows = await this._query(ctx, nextOp.sql, ...nextOp.params);

        // Don't begin the next operation until rows
        // are closed. Also don't call cleanup cancel()
        // until rows are closed
        rows.on('complete', () => {
          cancel();
          this.#next();
        });

        nextOp.resolve(rows);
      } catch (e) {
        // Ensure cancel is called if _query fails
        cancel();
        nextOp.reject(e);
      }
    } else if (nextOp.type === 'beginTxn') {
      const [ctx, cancel] = this.#wrapContext(nextOp.ctx);
      try {
        const txn = await this._beginTxn(ctx, nextOp.opts);

        // Don't begin the next operation until this transaction
        // is completely done. Also don't call cleanup cancel()
        // until transaction is done
        txn.on('complete', () => {
          cancel();
          this.#next();
        });

        nextOp.resolve(txn);

        return;
      } catch (e) {
        // Ensure cancel is called if _beginTxn fails
        cancel();
        nextOp.reject(e);
      }
    }

    setTimeout(() => this.#next(), 0);
  }

  #start(): void {
    if (this.#busy) return;
    this.#busy = true;
    this.#next();
  }

  async #done(): Promise<void> {
    if (this.#clr != null && this.#onCancel != null) {
      this.#clr.off(this.#onCancel);
    }

    // Prevent any further operations on the connection
    this.cleanUp();

    const wc = this.#waitClose;
    if (wc != null) {
      this.#waitClose = null;
      wc.resolve();
    }

    await this.emit('complete', null, [this]);
  }
}
