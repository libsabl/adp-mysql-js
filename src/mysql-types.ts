// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { hasFlag } from './util';

export interface ColumnDefinition {
  readonly name: string;
  readonly characterSet: unknown;
  readonly columnLength: number;
  readonly columnType: FieldType;
  readonly flags: FieldFlags;
  readonly decimals: number;
}

/**
 * Data type constants returned by underlying MySQL API describing a column in a result set
 *
 * See source: [`include/field_types.h`](https://github.com/mysql/mysql-server/blob/8.0/include/field_types.h#L57-L90) */
export enum FieldType {
  /**  DECIMAL (http://dev.mysql.com/doc/refman/5.0/en/precision-math-decimal-changes.html) */
  DECIMAL = 0,

  /** TINYINT, 1 byte */
  TINY = 1,

  /** SMALLINT, 2 bytes */
  SHORT = 2,

  /** INT, 4 bytes */
  LONG = 3,

  /** FLOAT, 4-8 bytes */
  FLOAT = 4,

  /** DOUBLE, 8 bytes */
  DOUBLE = 5,

  /** NULL (used for prepared statements, I think) */
  NULL = 6,

  /** TIMESTAMP */
  TIMESTAMP = 7,

  /** BIGINT, 8 bytes */
  LONGLONG = 8,

  /** MEDIUMINT, 3 bytes */
  INT24 = 9,

  /** DATE */
  DATE = 10,

  /** TIME */
  TIME = 11,

  /** DATETIME */
  DATETIME = 12,

  /** YEAR, 1 byte (don't ask) */
  YEAR = 13,

  NEWDATE = 14, // Internal to MySQL. Not used in protocol

  /** VARCHAR  */
  VARCHAR = 15,

  /** BIT, 1-8 byte */
  BIT = 16,

  TIMESTAMP2 = 17,

  DATETIME2 = 18, // Internal to MySQL. Not used in protocol

  TIME2 = 19, // Internal to MySQL. Not used in protocol

  TYPED_ARRAY = 20, // Used for replication only

  INVALID = 243,

  BOOL = 244, // Currently just a placeholder

  JSON = 245,

  /** DECIMAL */
  NEWDECIMAL = 246,

  /** ENUM */
  ENUM = 247,

  /** SET */
  SET = 248,

  /** TINYBLOB, TINYTEXT */
  TINY_BLOB = 249,

  /** MEDIUMBLOB, MEDIUMTEXT */
  MEDIUM_BLOB = 250,

  /** LONGBLOG, LONGTEXT */
  LONG_BLOB = 251,

  /** BLOB, TEXT */
  BLOB = 252,

  /** VARCHAR, VARBINARY */
  VAR_STRING = 253,

  /** CHAR, BINARY */
  STRING = 254,

  /** GEOMETRY */
  GEOMETRY = 255,
}

/**
 * Flags returned by underlying MySQL API describing a column in a result set
 *
 * See source: [`include/mysql_com.h`](https://github.com/mysql/mysql-server/blob/8.0/include/mysql_com.h#L153-L170)
 */
export enum FieldFlags {
  /* Field can't be NULL */
  NOT_NULL = 1,

  /* Field is part of a primary key */
  PRI_KEY = 2,

  /* Field is part of a unique key */
  UNIQUE_KEY = 4,

  /* Field is part of a key */
  MULTIPLE_KEY = 8,

  /* Field is a blob */
  BLOB = 16,

  /* Field is unsigned */
  UNSIGNED = 32,

  /* Field is zerofill */
  ZEROFILL = 64,

  /* Field is binary   */
  BINARY = 128,

  /* The following are only sent to new clients */
  /* field is an enum */
  ENUM = 256,

  /* field is a autoincrement field */
  AUTO_INCREMENT = 512,

  /* Field is a timestamp */
  TIMESTAMP = 1024,

  /* field is a set */
  SET = 2048,

  /* Field doesn't have default value */
  NO_DEFAULT_VALUE = 4096,

  /* Field is set to NOW on UPDATE */
  ON_UPDATE_NOW = 8192,

  /* Field is num (for clients) */
  NUM = 32768,
}

const typeNames: { [type: number]: string } = [];
typeNames[FieldType.DECIMAL] = 'DECIMAL';
typeNames[FieldType.TINY] = 'TINYINT';
typeNames[FieldType.SHORT] = 'SMALLINT';
typeNames[FieldType.LONG] = 'INT';
typeNames[FieldType.FLOAT] = 'FLOAT';
typeNames[FieldType.DOUBLE] = 'DOUBLE';
typeNames[FieldType.NULL] = 'NULL';
typeNames[FieldType.TIMESTAMP] = 'TIMESTAMP';
typeNames[FieldType.LONGLONG] = 'BIGINT';
typeNames[FieldType.INT24] = 'MEDIUMINT';
typeNames[FieldType.DATE] = 'DATE';
typeNames[FieldType.TIME] = 'TIME';
typeNames[FieldType.DATETIME] = 'DATETIME';
typeNames[FieldType.YEAR] = 'YEAR';
typeNames[FieldType.NEWDATE] = 'DATE';
typeNames[FieldType.VARCHAR] = 'VARCHAR';
typeNames[FieldType.BIT] = 'BIT';
typeNames[FieldType.JSON] = 'JSON';
typeNames[FieldType.NEWDECIMAL] = 'DECIMAL';
typeNames[FieldType.ENUM] = 'ENUM';
typeNames[FieldType.SET] = 'SET';
typeNames[FieldType.TINY_BLOB] = 'TINYBLOB'; // Also TINYTEXT
typeNames[FieldType.MEDIUM_BLOB] = 'MEDIUMBLOB'; // Also MEDIUMTEXT
typeNames[FieldType.LONG_BLOB] = 'LONGBLOG'; // Also LONGTEXT
typeNames[FieldType.BLOB] = 'BLOB'; // Also TEXT
typeNames[FieldType.VAR_STRING] = 'VARCHAR'; // Also VARBINARY
typeNames[FieldType.STRING] = 'CHAR';
typeNames[FieldType.GEOMETRY] = 'GEOMETRY';

export function parseType(
  type: FieldType,
  flags: FieldFlags,
  length: number
): [string, number | null] {
  switch (type) {
    case FieldType.BLOB:
      if (hasFlag(flags, FieldFlags.BINARY)) {
        if (length == 255) {
          return ['TINYBLOB', length];
        } else if (length == 65535) {
          return ['BLOB', length];
        } else if (length == 16777215) {
          return ['MEDIUMBLOB', length];
        } else if (length == 4294967295) {
          return ['LONGBLOB', length];
        }
        return ['BLOB', length];
      }
      if (length == 1020) {
        return ['TINYTEXT', length / 4];
      } else if (length == 262140) {
        return ['TEXT', length / 4];
      } else if (length == 67108860) {
        return ['MEDIUMTEXT', length / 4];
      } else if (length == 4294967295) {
        return ['LONGTEXT', Math.floor(length / 4)];
      }
      return ['TEXT', length / 4];

    case FieldType.VAR_STRING:
      if (hasFlag(flags, FieldFlags.BINARY)) {
        return ['VARBINARY', length];
      }
      return ['VARCHAR', length / 4];

    case FieldType.STRING:
      if (hasFlag(flags, FieldFlags.ENUM)) {
        return ['ENUM', null];
      } else if (hasFlag(flags, FieldFlags.SET)) {
        return ['SET', null];
      } else if (hasFlag(flags, FieldFlags.BINARY)) {
        return ['BINARY', length];
      }
      return ['CHAR', length / 4];

    // Types with fixed length: return null for length
    case FieldType.BIT:
    case FieldType.BOOL:
    case FieldType.DATE:
    case FieldType.DECIMAL:
    case FieldType.DOUBLE:
    case FieldType.ENUM:
    case FieldType.FLOAT:
    case FieldType.GEOMETRY:
    case FieldType.INT24:
    case FieldType.INVALID:
    case FieldType.LONG:
    case FieldType.LONGLONG:
    case FieldType.NEWDATE:
    case FieldType.NEWDECIMAL:
    case FieldType.NULL:
    case FieldType.SET:
    case FieldType.SHORT:
    case FieldType.TIME:
    case FieldType.TIME2:
    case FieldType.TIMESTAMP:
    case FieldType.TIMESTAMP2:
    case FieldType.TINY:
    case FieldType.YEAR:
      return [typeNames[type], null];
  }
  return [typeNames[type], length];
}
