// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { DbConn } from '@sabl/db-api';
import { IsolationLevel, TxnOptions } from '@sabl/txn';
import { AsyncCompleteEmitter } from './async-event-emitter';
import { DbTxnBase } from './dbtxn-base';

export class MySQLTxn extends DbTxnBase {
  constructor(
    txnCon: DbConn & AsyncCompleteEmitter<DbConn>,
    opts?: TxnOptions
  ) {
    super(txnCon, opts, true);
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
