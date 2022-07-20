// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { Context, IContext, Maybe, withValue } from '@sabl/context';
import { DbApi, DbConn, DbTxn } from '@sabl/db-api';
import { Transactable, TxnAccessor } from '@sabl/txn';
import { MySQLConn } from './mysql-conn';
import { MySQLPool } from './mysql-pool';
import { MySQLTxn } from './mysql-txn';

const ctxKeyMySQLConn = Symbol('MySQLConn');
const ctxKeyMySQLPool = Symbol('MySQLPool');
const ctxKeyMySQLTxn = Symbol('MySQLTxn');

export function withMySQLPool(ctx: IContext, pool: MySQLPool): Context {
  return withValue(ctx, ctxKeyMySQLPool, pool);
}

export function getMySQLPool(ctx: IContext): Maybe<MySQLPool> {
  return <Maybe<MySQLPool>>ctx.value(ctxKeyMySQLPool);
}

export function withMySQLConn(ctx: IContext, con: DbConn): Context {
  if (!MySQLConn.isMySQLConn(con)) {
    throw new Error('Provided connection is not a MySQL connection');
  }
  return withValue(ctx, ctxKeyMySQLConn, con);
}

export function getMySQLConn(ctx: IContext): Maybe<DbConn> {
  return <Maybe<DbConn>>ctx.value(ctxKeyMySQLConn);
}

export function withMySQLTxn(ctx: IContext, txn: DbTxn): Context {
  if (!MySQLTxn.isMySQLTxn(txn)) {
    throw new Error('Provided transaction is not a MySQL transaction');
  }
  return withValue(ctx, ctxKeyMySQLTxn, txn);
}

export function getMySQLTxn(ctx: IContext): Maybe<DbTxn> {
  return <Maybe<DbTxn>>ctx.value(ctxKeyMySQLTxn);
}

// Get the closest transaction, connection, or pool
export function getMySQL(ctx: IContext): Maybe<DbApi> {
  return getMySQLTxn(ctx) || getMySQLConn(ctx) || getMySQLPool(ctx);
}

export const MySQLAccessor: TxnAccessor<DbTxn> = {
  getTransactable: function (ctx: IContext): Maybe<Transactable<DbTxn>> {
    return getMySQLConn(ctx) || getMySQLPool(ctx);
  },

  getTxn: getMySQLTxn,

  withTxn: withMySQLTxn,
};
