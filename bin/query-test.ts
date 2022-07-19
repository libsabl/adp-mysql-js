import { createPool, Field, RowDataPacket } from 'mysql2';
import { config } from 'dotenv';
import { faker } from '@faker-js/faker';

import { usePoolConnection } from '../src/mysql-util';
import { MySQLPool } from '../src/mysql-dbapi';
import { MySQLQuery } from '../src/mysql-query';
import { Rows, Row } from '@sabl/db-api';
import { Canceler, Context } from '@sabl/context';
import { IsolationLevel } from '@sabl/txn';
// import { getDbApi, runTransaction, withDbApi } from '../src/context';

config({ path: './env/test.env' });

function getPool() {
  const { MYSQL_SERVER, MYSQL_USER, MYSQL_PASS } = process.env;

  return createPool({
    connectionLimit: 10,
    host: MYSQL_SERVER,
    user: MYSQL_USER,
    password: MYSQL_PASS,
    database: 'example_01',
  });
}

function getMySqlPool() {
  const { MYSQL_SERVER, MYSQL_USER, MYSQL_PASS } = process.env;

  return new MySQLPool({
    connectionLimit: 10,
    host: MYSQL_SERVER,
    user: MYSQL_USER,
    password: MYSQL_PASS,
    database: 'example_01',
  });
}

export async function queryRows() {
  const pool = getPool();
  await usePoolConnection(Context.background, pool, async (con) => {
    let rows: Rows | null = null;
    try {
      const qry = con.query('select * from some_data');
      rows = new MySQLQuery(qry, con, pool, true);

      while (await rows.next()) {
        console.log(rows.row);
      }
    } finally {
      await rows?.close();
    }
  });
  await pool.promise().end();
}

export async function queryRowsCancel() {
  const pool = getPool();
  await usePoolConnection(Context.background, pool, async (con) => {
    let rows: Rows | null = null;
    let i = 0;
    const [clr, cancel] = Canceler.create();
    try {
      const qry = con.query('select * from big_table');
      rows = new MySQLQuery(qry, con, pool, true, clr);

      while (await rows.next()) {
        const row = rows.row;
        console.log(row.id, row.label);
        if (++i == 300) {
          cancel();
        }
      }
    } finally {
      await rows?.close();
    }

    // Reuse open connection
    try {
      const qry = con.query('select * from some_data');
      rows = new MySQLQuery(qry, con, pool, true);

      while (await rows.next()) {
        const row = rows.row;
        console.log(row.id, row.label);
      }
    } finally {
      await rows?.close();
    }
  });

  await pool.promise().end();
}

export async function queryRowsClose() {
  const pool = getPool();
  await usePoolConnection(Context.background, pool, async (con) => {
    let rows: Rows | null = null;
    let i = 0;
    try {
      const qry = con.query('select * from big_table');
      rows = new MySQLQuery(qry, con, pool, false);

      const cols = rows.columns;
      console.log(cols);
      console.log(rows.columnTypes);

      while (await rows.next()) {
        const row = rows.row;
        console.log(row.id, row.label);
        if (++i == 300) {
          await rows.close();
        }
      }
    } finally {
      await rows?.close();
    }
  });

  await pool.promise().end();
}

export async function queryRowsOverlap() {
  const pool = getPool();

  await usePoolConnection(Context.background, pool, async (con) => {
    let rows1: Rows | null = null;
    let i = 0;
    try {
      const qry1 = con.query('select * from big_table');
      rows1 = new MySQLQuery(qry1, con, pool, false);
      await (<MySQLQuery>rows1).ready();

      const cols = rows1.columns;
      console.log(cols);
      console.log(rows1.columnTypes);

      const qry2 = con.query('select * from some_data');
      const rows2 = new MySQLQuery(qry2, con, pool, false);
      //

      while (await rows1.next()) {
        const row = rows1.row;
        console.log(Row.toArray(row));

        // if (await rows2.next()) {
        //   console.log(Row.toArray(rows2.row));
        // }

        if (++i == 300) {
          await rows1.close();
        }
      }

      await (<MySQLQuery>rows2).ready();
      for await (const r of rows2) {
        console.log(Row.toObject(r));
      }
    } catch (e) {
      console.log(e);
    } finally {
      await rows1?.close();
    }
  });

  await pool.promise().end();
}

export async function queryRowTypes() {
  const pool = getPool();
  await usePoolConnection(Context.background, pool, async (con) => {
    let rows: MySQLQuery | null = null;
    try {
      const qry = con.query('select * from many_types');
      rows = new MySQLQuery(qry, con, pool, false);
      await rows.ready();

      const colTypes = rows.columnTypes;
      console.log(colTypes);

      while (await rows.next()) {
        const row = rows.row;
        console.log(Row.toObject(row));
      }
    } finally {
      await rows?.close();
    }
  });

  await pool.promise().end();
}

export async function queryDirect() {
  const msPool = getMySqlPool();
  const rows = await msPool.query(
    Context.background,
    'select * from big_table limit 10'
  );

  for await (const row of rows) {
    console.log(row);
    console.log(row.id);
    console.log(row[0]);
    console.log(Row.toArray(row));
    console.log(Row.toObject(row));
  }

  await msPool.close();
}

export async function queryEvents() {
  const pool = getPool();
  await usePoolConnection(Context.background, pool, async (con) => {
    let res: (v: unknown) => void;
    let rej: (r?: unknown) => void;
    const p = new Promise((resolve, reject) => {
      res = resolve;
      rej = reject;
    });

    const qry = con.query('select * from some_data');
    qry.on('end', () => {
      console.log('query ended');
      res(null);
    });
    qry.on('fields', (info: any) => {
      if (info instanceof Array) {
        const fields = <Field[]>info;
        let i = 0;
        for (const f of fields) {
          console.log(`${i++}: ${f.name}`);
        }
      } else {
        rej(new Error('Unexpected fields packet'));
      }
    });
    qry.on('error', (err) => {
      console.log(err);
      rej(err);
    });
    qry.on('result', (pkt) => {
      console.log(pkt.constructor.name);
      if (pkt.constructor.name == 'RowDataPacket') {
        const row = <RowDataPacket>pkt;
        for (let i = 0; i < 3; i++) {
          console.log(`${i}: ${row[i]}`);
        }
      } else {
        console.log(pkt);
      }
    });

    await p;
  });

  await pool.promise().end();
}

export async function insertMany() {
  const pool = getPool();
  await usePoolConnection(Context.background, pool, async (con) => {
    const sql = 'insert big_table ( code , label, num, ts ) values ?';
    const cp = con.promise();
    let rows = [];
    for (let i = 0; i < 100_000; i++) {
      const code = faker.datatype.uuid();
      const label = faker.random.words(5);
      const num = faker.datatype.number(10000);
      const ts = faker.date.soon(30);
      rows.push([code, label, num, ts]);
      if (0 == (i + 1) % 1000) {
        const [result] = await cp.query(sql, [rows]);
        console.log(i + 1, result);
        rows = [];
      }
    }
  });

  await pool.promise().end();
}

export async function useTxn() {
  const msPool = new MySQLPool(getPool());
  const ctx = Context.background;
  const txn = await msPool.beginTxn(ctx, {
    isolationLevel: IsolationLevel.readUncommitted,
  });

  let row: Row | null = null;
  let rowId = 0;
  try {
    row = await txn.queryRow(ctx, 'select count(*) from big_table');
    console.log(row![0]);

    const result = await txn.exec(
      ctx,
      `insert into big_table ( code, num, ts )
      values ( ?, ?, now() )`,
      ...[faker.random.word(), 44]
    );
    console.log(result);

    row = await txn.queryRow(
      ctx,
      'select * from big_table where id = ?',
      (rowId = result.lastId!)
    );
    console.log(row);

    row = await txn.queryRow(ctx, 'select count(*) from big_table');
    console.log(row![0]);

    await txn.commit(ctx);
  } catch (err) {
    console.error(err);
    await txn.rollback(ctx);
  }

  row = await msPool.queryRow(ctx, 'select count(*) from big_table');
  console.log(row![0]);

  row = await msPool.queryRow(
    ctx,
    'select * from big_table where id = ?',
    rowId
  );
  console.log(row);

  await msPool.close();
}

/*
export async function ctxDbApi() {
  const mPool = getMySqlPool();
  const ctx = Context.value(withDbApi, mPool);

  let rowId;
  await runTransaction(ctx, async (ctx) => {
    const txn = Context.as(ctx).require(getDbApi);
    let row = await txn.queryRow(ctx, 'select count(*) from big_table');
    console.log(row![0]);

    const result = await txn.exec(
      ctx,
      `insert into big_table ( code, num, ts )
      values ( ?, ?, now() )`,
      ...[faker.random.word(), 44]
    );
    console.log(result);
    rowId = result.lastId;

    row = await txn.queryRow(ctx, 'select count(*) from big_table');
    console.log(row![0]);
  });

  const row = await mPool.queryRow(
    ctx,
    'select * from big_table where id = ?',
    rowId
  );
  console.log(row);

  await mPool.close();
}
*/

(async () => {
  await queryRowsOverlap();
})();
