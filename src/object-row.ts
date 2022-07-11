// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { PlainObject, Row } from './db-api';

const token = Symbol('ObjectRow-constructor');
const COLS = Symbol('cols');
const DATA = Symbol('data');

function toObject(r: ObjectRow): PlainObject {
  return Object.assign({}, r[DATA]);
}

function toArray(r: ObjectRow): unknown[] {
  const result: unknown[] = [];
  for (const c of r[COLS]) {
    result.push(r[DATA][c]);
  }
  return result;
}

export class ObjectRow implements Row {
  readonly [DATA]: PlainObject;
  readonly [COLS]: string[];

  static create(data: PlainObject, cols: string[]): Row {
    const r = new ObjectRow(data, cols, token);
    return <Row>new Proxy(r, { get: this.#getCol });
  }

  static #getCol(target: ObjectRow, p: string | symbol): unknown {
    if (typeof p === 'string') {
      if (p === 'toObject') {
        return toObject.bind(null, target);
      } else if (p === 'toArray') {
        return toArray.bind(null, target);
      }

      const ix = +p;
      if (!isNaN(ix)) {
        const pname = target[COLS][ix];
        return target[DATA][pname];
      }
      return target[DATA][p];
    }
  }

  /** @private ObjectRow constructor is private. Use ObjectRow.create */
  constructor(data: PlainObject, cols: string[], tk: symbol) {
    if (tk !== token) {
      throw new Error('ObjectRow constructor is private. Use ObjectRow.create');
    }
    this[DATA] = data;
    this[COLS] = cols;
  }

  [index: number]: unknown;
  [key: string]: unknown;

  toObject(): PlainObject {
    return toObject(this);
  }

  toArray(): unknown[] {
    return toArray(this);
  }
}
