// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { Context, IContext } from '@sabl/context';
import { RecordError } from '@sabl/record';
import { getDbApi } from './context';
import { ParamMap, toParamArray } from './db-api';
import { Row } from './row';
import {
  hasMappingFlag,
  MappingFlags,
  RecordMapper,
  SqlAdapter,
  SqlSource,
} from './types';

export class AdapterError {
  static readonly MISSING_STATEMENT = 'adapter:missing-statement';
  static readonly NON_QUERY = 'adapter:non-query';
}

export class SqlAdapterBase<TRecord, TKey> implements SqlAdapter<TRecord> {
  readonly sqlSource: SqlSource;
  readonly mapper: RecordMapper<TRecord, TKey>;

  constructor(sqlSource: SqlSource, mapper: RecordMapper<TRecord, TKey>) {
    this.sqlSource = sqlSource;
    this.mapper = mapper;
  }

  toRecord(row: Row): TRecord {
    const record = this.mapper.create();
    this.mapper.load(row, record);
    return record;
  }

  async insert(ctx: IContext, record: TRecord): Promise<void> {
    const sql = this.sqlSource.getInsertSQL();
    if (sql == null) {
      throw new RecordError(
        AdapterError.MISSING_STATEMENT,
        'INSERT SQL not defined'
      );
    }
    const db = Context.as(ctx).require(getDbApi);
    const params = this.mapper.getInsertParams(record);

    // Note: we use the lower-level selectRows API to
    // avoid copying the column values to a temporary
    // object and then to the record. This approach allows
    // the column values to be read directly from the
    // open cursor onto the record.

    if (hasMappingFlag(this.mapper.flags, MappingFlags.generatedOnInsert)) {
      // Statement should return entire row, which will be loaded back on to record
      const rows = await db.query(ctx, sql, ...params);
      try {
        const has = await rows.next();
        if (!has) {
          throw new RecordError(
            AdapterError.NON_QUERY,
            'Expected inserted row, but insert query returned no row'
          );
        }
        this.mapper.load(rows.row, record);
      } finally {
        rows.close();
      }
    } else if (hasMappingFlag(this.mapper.flags, MappingFlags.generatedId)) {
      // Statement will return just the inserted id, which will be set on record
      const rows = await db.query(ctx, sql, ...params);
      try {
        const has = await rows.next();
        if (!has) {
          throw new RecordError(
            AdapterError.NON_QUERY,
            'Expected generated id, but insert query returned no row'
          );
        }
        this.mapper.setKey(record, <TKey>rows.row[0]);
      } finally {
        rows.close();
      }
    } else {
      // No return value expected
      await db.exec(ctx, sql, ...params);
    }
  }

  async update(ctx: IContext, record: TRecord): Promise<void> {
    const sql = this.sqlSource.getUpdateSQL();
    if (sql == null) {
      throw new RecordError(
        AdapterError.MISSING_STATEMENT,
        'UPDATE SQL not defined'
      );
    }
    const db = Context.as(ctx).require(getDbApi);
    const params = this.mapper.getUpdateParams(record);

    // Note: we use the lower-level selectRows API to
    // avoid copying the column values to a temporary
    // object and then to the record. This approach allows
    // the column values to be read directly from the
    // open cursor onto the record.

    if (hasMappingFlag(this.mapper.flags, MappingFlags.generatedOnUpdate)) {
      // Statement should return entire row, which will be loaded back on to record
      const rows = await db.query(ctx, sql, ...params);
      try {
        const has = await rows.next();
        if (!has) {
          throw new RecordError(
            AdapterError.NON_QUERY,
            'Expected updated row, but update query returned no row'
          );
        }
        this.mapper.load(rows.row, record);
      } finally {
        rows.close();
      }
    } else {
      // No return value expected
      await db.exec(ctx, sql, ...params);
    }
  }

  async delete(ctx: IContext, record: TRecord): Promise<void> {
    const sql = this.sqlSource.getDeleteSQL();
    if (sql == null) {
      throw new RecordError(
        AdapterError.MISSING_STATEMENT,
        'DELETE SQL not defined'
      );
    }
    const db = Context.as(ctx).require(getDbApi);
    const params = this.mapper.getDeleteParams(record);

    // No return value expected
    await db.exec(ctx, sql, params);
  }

  selectOne(ctx: IContext, params: ParamMap): Promise<TRecord | null>;
  selectOne(
    ctx: IContext,
    sql: string,
    params: ParamMap
  ): Promise<TRecord | null>;
  async selectOne(
    ctx: IContext,
    sqlOrParams: string | ParamMap,
    params?: ParamMap
  ): Promise<TRecord | null> {
    let sql: string;
    if (typeof sqlOrParams === 'string') {
      sql = sqlOrParams;
    } else {
      params = sqlOrParams;
      sql = this.sqlSource.getSelectSQL(...Object.keys(params || {}));
    }

    // Note: we use the lower-level selectRows API to
    // avoid copying the column values to a temporary
    // object and then to the record. This approach allows
    // the column values to be read directly from the
    // open cursor onto the record.

    const db = Context.as(ctx).require(getDbApi);
    const rows = await db.query(ctx, sql, ...toParamArray(params));
    try {
      const has = await rows.next();
      if (!has) {
        return null;
      }
      return this.toRecord(rows.row);
    } finally {
      rows.close();
    }
  }

  selectAll(ctx: IContext): AsyncIterable<TRecord> {
    return this.selectMany(ctx, {});
  }

  selectMany(ctx: IContext, params: ParamMap): AsyncIterable<TRecord>;
  selectMany(
    ctx: IContext,
    sql: string,
    params?: ParamMap
  ): AsyncIterable<TRecord>;
  async *selectMany(
    ctx: IContext,
    sqlOrParams: string | ParamMap,
    params?: ParamMap
  ): AsyncIterable<TRecord> {
    let sql: string;
    if (typeof sqlOrParams === 'string') {
      sql = sqlOrParams;
    } else {
      params = sqlOrParams;
      sql = this.sqlSource.getSelectSQL(...Object.keys(params || {}));
    }

    // Note: we use the lower-level selectRows API to
    // avoid copying the column values to a temporary
    // object and then to the record. This approach allows
    // the column values to be read directly from the
    // open cursor onto the record.

    const db = Context.as(ctx).require(getDbApi);
    const rows = await db.query(ctx, sql, ...toParamArray(params));
    try {
      while (await rows.next()) {
        yield this.toRecord(rows.row);
      }
    } finally {
      rows.close();
    }
  }
}
