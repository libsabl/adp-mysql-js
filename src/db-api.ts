// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { IContext } from '@sabl/context';
import { Row } from './row';
import {
  StorageApi,
  StorageTxn,
  Transactable,
  TxnOptions,
} from './storage-api';

export type ParamValue = unknown;

/** A map of named SQL parameters */
export type ParamMap = { [key: string]: ParamValue };

export type PlainObject = { [key: string | symbol]: unknown };

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

/** Information about a column in a result set */
export interface ColumnInfo {
  /** The name of the column */
  readonly name: string;

  /** The database-specific type name of the column */
  readonly typeName: string;

  /** Whether the column is nullable */
  readonly nullable: boolean;

  /**
   * The precision and scale of the column. Undefined
   * if the column is not a decimal type
   */
  readonly decimalSize?: { precision: number; scale: number };

  /**
   * The defined length of the column. Undefined
   * if the column is an int or other non-variable length
   * type.
   */
  readonly length?: number;
}

/**
 * Rows is the result of a query. Its cursor starts before
 * the first row of the result set. Use `next()` to advance
 * from row to row.
 *
 * see golang: [`sql.Rows`](https://pkg.go.dev/database/sql#Rows)
 * ([source](https://github.com/golang/go/blob/master/src/database/sql/sql.go))
 */
export interface Rows extends AsyncIterable<Row> {
  /**
   * Closes the Rows, preventing further enumeration.
   * If next is called and returns false and there are
   * no further result sets, the Rows are closed
   * automatically. Implementations of close must be idempotent.
   */
  close(): Promise<void>;

  /** Return the column names of the row set */
  columns(): string[];

  /** Return the details about the columns in the row set */
  columnTypes(): ColumnInfo[];

  /**
   * Advance to the next row. Returns true if another
   * row is available, or false if there are no more rows.
   */
  next(): Promise<boolean>;

  /**
   * The latest error, if any, encountered while
   * opening or advancing the row set
   */
  get err(): Error | null;

  /**
   * Access the current row. The returned row is not
   * guaranteed to be valid after the Rows is advanced
   * or closed. To obtain a safe copy for retention,
   * use static Row.toObject(), Row.toArray(), or Row.clone()
   */
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
  queryRow(
    ctx: IContext,
    sql: string,
    ...params: ParamValue[]
  ): Promise<Row | null>;

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

  /**
   * Close and release all connections in the pool.
   * Generally only used at program termination
   */
  close(): Promise<void>;
}
