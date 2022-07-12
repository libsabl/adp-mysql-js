// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import type { PlainObject } from './db-api';

export const RowToObject = Symbol('[RowToObject]');
export const RowToArray = Symbol('[RowToArray]');
export const RowClone = Symbol('[RowClone]');

/**
 * A simple interface that represents retrieving a value
 * by a string key or integer index. Useful for implementing
 * reusable generic relational database CRUD logic.
 */
export interface Row {
  /** Retrieve a value by name */
  [key: string | symbol]: unknown;

  /** Retrieve a value by zero-based index */
  [index: number]: unknown;

  /** Return a copy of the underlying data as a plain object */
  [RowToObject](): PlainObject;

  /** Return a copy of the underlying data as an array of values */
  [RowToArray](): unknown[];

  /** Return a copy of the underlying data as a Row interface safe to store and retain */
  [RowClone](): Row;
}

function objToArray(data: PlainObject, cols: string[]): unknown[] {
  const result = [];
  for (const c of cols) {
    result.push(data[c]);
  }
  return result;
}

function arrayToObj(data: unknown[], cols: string[]): PlainObject {
  const result: PlainObject = {};
  for (let ix = 0; ix < cols.length; ix++) {
    result[cols[ix]] = data[ix];
  }
  return result;
}

export class Row {
  /** Return a copy of the underlying data as a plain object */
  static toObject(row: Row): PlainObject {
    return row[RowToObject]();
  }

  /** Return a copy of the underlying data as an array of values */
  static toArray(row: Row): unknown[] {
    return row[RowToArray]();
  }

  /** Return a copy of the underlying data as a Row interface safe to store and retain */
  static clone(row: Row): Row {
    return row[RowClone]();
  }

  /**
   * Create a proxy wrapper that supports the Row interface
   * for a plain object with an ordered set of column names. Uses
   * {@link Proxy} and lazy initialization to efficiently provide
   * access to underlying properties without copying data
   */
  static fromObject(data: PlainObject, cols: string[]): Row {
    // Method closures are lazy-initialized and memoized to avoid
    // allocation unless utility methods are actually called on a row
    let toArray: null | (() => unknown[]) = null;
    let toObject: null | (() => PlainObject) = null;
    let clone: null | (() => Row) = null;

    return new Proxy(<Row>(<unknown>data), {
      get: function (target: Row, p: string | symbol) {
        if (p === RowToArray) {
          if (toArray == null) {
            toArray = () => objToArray(data, cols);
          }
          return toArray;
        } else if (p === RowToObject) {
          if (toObject == null) {
            toObject = () => Object.assign({}, data);
          }
          return toObject;
        } else if (p === RowClone) {
          if (clone == null) {
            clone = () => Row.fromObject(Object.assign({}, data), cols);
          }
          return clone;
        } else if (p in target) {
          return target[p];
        } else if (typeof p === 'string') {
          const ix = +p;
          if (isNaN(ix)) {
            return undefined;
          }

          const prop = cols[ix];
          return data[prop];
        }
      },
    });
  }

  /**
   * Create a proxy wrapper that supports the Row interface
   * for a plain array with an ordered set of column names. Uses
   * {@link Proxy} and lazy initialization to efficiently provide
   * access to underlying data without copying values
   */
  static fromArray(data: unknown[], cols: string[]): Row {
    // Method closures are lazy-initialized and memoized to avoid
    // allocation unless utility methods are actually called on a row
    let toArray: null | (() => unknown[]) = null;
    let toObject: null | (() => PlainObject) = null;
    let clone: null | (() => Row) = null;
    let colMap: null | { [key: string]: number } = null;

    return new Proxy(<Row>(<unknown>data), {
      get: function (target: Row, p: string | symbol) {
        if (p === RowToArray) {
          if (toArray == null) {
            toArray = () => data.concat();
          }
          return toArray;
        } else if (p === RowToObject) {
          if (toObject == null) {
            toObject = () => arrayToObj(data, cols);
          }
          return toObject;
        } else if (p === RowClone) {
          if (clone == null) {
            clone = () => Row.fromArray(data.concat(), cols);
          }
          return clone;
        } else if (p in target) {
          return target[p];
        } else if (typeof p === 'string') {
          if (colMap == null) {
            colMap = {};
            for (let ix = 0; ix < cols.length; ix++) {
              colMap[cols[ix]] = ix;
            }
          }
          const ix = colMap[p];
          return data[ix];
        }
      },
    });
  }
}
