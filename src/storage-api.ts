// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { IContext } from '@sabl/context';

/**
 * Various isolation levels that storage drivers may support in beginTxn.
 * If a driver does not support a given isolation level an error may be returned.
 */
export enum IsolationLevel {
  default = 1,
  readUncommitted = 2,
  readCommitted = 3,
  writeCommitted = 4,
  repeatableRead = 5,
  snapshot = 6,
  serializable = 7,
  linearizable = 8,
}

/** Options to be used in beginTxn */
export interface TxnOptions {
  readonly isolationLevel?: IsolationLevel;
  readonly readOnly?: boolean;
}

export enum StorageMode {
  pool = 1,
  conn = 2,
  txn = 3,
}

export enum StorageKind {
  unknown = 'unknown',
  db = 'database',
}

/**
 * An abstract representation of a storage API, which
 * could be an entire pool, a single connection, or
 * an open transaction.
 */
export interface StorageApi {
  readonly mode: StorageMode;
  readonly kind: StorageKind | string;
}

/**
 * An abstract representation of a transaction
 * in the context of some storage service.
 */
export interface StorageTxn extends StorageApi {
  /** Commit all pending operations */
  commit(): Promise<void>;

  /** Rollback all pending operations. */
  rollback(): Promise<void>;
}

/**
 * An abstract representation of a storage pool or
 * connection that can begin a transaction.
 */
export interface Transactable extends StorageApi {
  /** Begin a transaction on the connection or pool */
  beginTxn(ctx: IContext, opts?: TxnOptions): Promise<StorageTxn>;
}

/**
 * An abstract representation of a storage connection,
 * regardless of the underlying storage type or protocol. It
 * supports the concept of transactions. A connection
 * should call close() to return it to its source pool.
 */
export interface StorageConn extends Transactable {
  /** Return the connection to its source pool */
  close(): Promise<void>;
}

/**
 * An abstract representation of a pool of storage connections,
 * regardless of the underlying storage type or protocol. It
 * supports the concept of transactions.
 */
export interface StoragePool extends Transactable {
  /**
   * Returns a single connection by either opening a new connection
   * or returning an existing connection from the connection pool. conn
   * will not resolve until either a connection is returned or ctx is canceled.
   * Queries run on the same Conn will be run in the same storage session.
   */
  conn(ctx: IContext): Promise<StorageConn>;
}
