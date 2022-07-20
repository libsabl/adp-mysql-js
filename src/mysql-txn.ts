// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { DbConn, DbTxn } from '@sabl/db-api';
import { IsolationLevel, TxnOptions } from '@sabl/txn';
import { AsyncCompleteEmitter } from './async-event-emitter';
import { DbTxnBase } from './dbtxn-base';

const mySqlTxn = Symbol('MySQLTxn');

export class MySQLTxn extends DbTxnBase {
  static isMySQLTxn(txn: DbTxn | MySQLTxn): txn is MySQLTxn {
    return mySqlTxn in txn;
  }

  protected readonly [mySqlTxn] = true;

  constructor(
    txnCon: DbConn & AsyncCompleteEmitter<DbConn>,
    opts?: TxnOptions
  ) {
    super(txnCon, opts, false);
  }

  supportsIsolationLevel(level: IsolationLevel): boolean {
    switch (level) {
      case IsolationLevel.repeatableRead:
      case IsolationLevel.readCommitted:
      case IsolationLevel.readUncommitted:
      case IsolationLevel.serializable:
        return true;
    }
    return false;
  }
}
