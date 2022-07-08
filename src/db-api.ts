// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { IContext } from '@sabl/context';
import {
  StorageApi,
  StorageTxn,
  Transactable,
  TxnOptions,
} from './storage-api';

/** A map of named SQL parameters */
export type ParamMap = { [key: string]: unknown };

export type ParamValue = unknown;

/** A name-value pair uses as a database query parameter */
export class NamedParam {
  constructor(readonly name: string, readonly value: unknown) {}
}

/** Unwrap a map of parameters to an array of {@link NamedParam} */
export function toParamArray(map?: ParamMap): ParamValue[] {
  if (map == null) return [];
  const result = [];
  for (const k in map) {
    result.push(new NamedParam(k, map[k]));
  }
  return result;
}

/**
 * A simple interface that represents retrieving a value
 * by a string key or integer index. Useful for implementing
 * reusable generic relational database CRUD logic.
 */
export interface Row {
  /** Retrieve a value by name */
  [key: string]: unknown;

  /** Retrieve a value by zero-based index */
  [index: number]: unknown;

  /** Return the underlying data as a plain object */
  toObject(): Map<string, unknown>;
}

/** The result of executing a SQL command */
export interface Result {
  /** The number of rows affected by the command */
  rowsAffected: number;

  /**
   * The row id or auto-incrementing column value
   * of the last inserted row, if supported by the
   * underlying database driver
   */
  lastId: number | undefined;
}

/**
 * Rows is the result of a query. Its cursor starts before
 * the first row of the result set. Use `next()` to advance
 * from row to row.
 *
 * Also implements {@link Row} to read the values of the
 * current row by ordinal or name
 *
 * see golang: [`sql.Rows`](https://github.com/golang/go/blob/master/src/database/sql/sql.go)
 */
export interface Rows {
  /**
   * Closes the Rows, preventing further enumeration.
   * If next is called and returns false and there are
   * no further result sets, the Rows are closed
   * automatically. Implementations of close must be idempotent.
   */
  close(): Promise<void>;

  /** Return the column names of the row set */
  columns(): string[];

  /**
   * Advance to the next row. Returns true if another
   * row is available, or false if there are no more rows.
   */
  next(): Promise<boolean>;

  /** Return the raw values of the row */
  values(): unknown[];

  get err(): Error | null;

  /** Access the current row */
  get row(): Row;
}

/**
 * Abstraction of the queryable interface of a relational database.
 * Can represent an open transaction, an open connection, or an
 * entire database pool.
 */
export interface DbApi extends StorageApi {
  /** Execute an arbitrary SELECT query on the context connection and return the first row
   * @param ctx The context in which to execute the statement. May be used
   * to signal cancellation by providing a cancelable context.
   * @param sql The literal SQL statement
   * @param params The values of any SQL parameters  */
  queryRow(ctx: IContext, sql: string, ...params: ParamValue[]): Promise<Row>;

  /** Execute an arbitrary SELECT query on the context connection and iterate through the returned rows
   * @param ctx The context in which to execute the statement. May be used
   * to signal cancellation by providing a cancelable context.
   * @param sql The literal SQL statement
   * @param params The values of any SQL parameters  */
  query(ctx: IContext, sql: string, ...params: ParamValue[]): Promise<Rows>;

  /** Execute a SQL statement without returning rows
   * @param ctx The context in which to execute the statement. May be used
   * to signal cancellation by providing a cancelable context.
   * @param sql The literal SQL statement
   * @param params The values of any SQL parameters
   */
  exec(ctx: IContext, sql: string, ...params: ParamValue[]): Promise<Result>;
}

/** A database transaction */
export interface DbTxn extends DbApi, StorageTxn {}

/**
 * Either a {@link DbConn} or a {@link DbPool}.
 */
export interface DbTransactable extends Transactable {
  /** Begin a transaction on the connection or pool */
  beginTxn(ctx: IContext, opts?: TxnOptions): Promise<DbTxn>;
}

/**
 * An open database connection that implements
 * both {@link DbApi} and {@link DbTransactable}.
 * Structurally matches and can be used as a {@link StorageConn}
 */
export interface DbConn extends DbApi, DbTransactable {
  /** Return the connection to its source pool */
  close(): Promise<void>;
}

/**
 * A pool of database connections that implements
 * both {@link DbApi} and {@link DbTransactable}.
 * Structurally matches and can be used as a {@link StoragePool}
 */
export interface DbPool extends DbApi, DbTransactable {
  /**
   * Returns a single connection by either opening a new connection
   * or returning an existing connection from the connection pool. conn
   * will not resolve until either a connection is returned or ctx is canceled.
   * Queries run on the same Conn will be run in the same storage session.
   */
  conn(ctx: IContext): Promise<DbConn>;
}
