// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { PlainObject, Row } from './db-api';

const token = Symbol('ObjectRow-constructor');
const COLS = Symbol('cols');
const DATA = Symbol('data');

function toObject(r: ObjectRow_1): PlainObject {
  return Object.assign({}, r[DATA]);
}

function toArray(r: ObjectRow_1): unknown[] {
  const result: unknown[] = [];
  for (const c of r[COLS]) {
    result.push(r[DATA][c]);
  }
  return result;
}

function objToArray(data: PlainObject, cols: string[]): unknown[] {
  const result = [];
  for (const c of cols) {
    result.push(data[c]);
  }
  return result;
}

export class ObjectRow implements Row {
  constructor(data: PlainObject, cols: string[]) {
    Object.assign(this, data);
    const toArray = () => objToArray(data, cols);
    const toObject = () => Object.assign({}, data);

    return new Proxy(this, {
      get: function (target: ObjectRow, p: string) {
        if (p === 'toArray') {
          return toArray;
        } else if (p === 'toObject') {
          return toObject;
        } else if (p in target) {
          return target[p];
        }

        const ix = +p;
        if (isNaN(ix)) {
          return undefined;
        }

        const prop = cols[ix];
        return data[prop];
      },
    });
  }

  [index: number]: unknown;
  [key: string]: unknown;
  toObject(): PlainObject {
    throw new Error('Method not implemented.');
  }
  toArray(): unknown[] {
    throw new Error('Method not implemented.');
  }
}

export class ObjectRow_1 implements Row {
  readonly [DATA]: PlainObject;
  readonly [COLS]: string[];

  static create(data: PlainObject, cols: string[]): Row {
    return <Row>new Proxy(data, {
      get: this.#getCol,
      ownKeys(target: ObjectRow_1) {
        return target[COLS].concat();
      },
    });
  }

  static #getCol(target: ObjectRow_1, p: string | symbol): unknown {
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
