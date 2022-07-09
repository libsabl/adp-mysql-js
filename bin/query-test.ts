/* eslint-disable @typescript-eslint/no-explicit-any */
import { createPool, Field, RowDataPacket } from 'mysql2';
import { config } from 'dotenv';
import { faker } from '@faker-js/faker';

import { usePoolConnection } from '../src/mysql-util';
import { MySQLRows } from '../src/mysql-dbapi';
import { Rows } from '../src/db-api';
import { Canceler } from '@sabl/context';

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

export async function queryRows() {
  const pool = getPool();
  await usePoolConnection(pool, async (con) => {
    let rows: Rows | null = null;
    try {
      const qry = con.query('select * from some_data');
      rows = new MySQLRows(qry, con, pool, true);

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
  await usePoolConnection(pool, async (con) => {
    let rows: Rows | null = null;
    let i = 0;
    const [clr, cancel] = Canceler.create();
    try {
      const qry = con.query('select * from big_table');
      rows = new MySQLRows(qry, con, pool, true, clr);

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
      rows = new MySQLRows(qry, con, pool, true);

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

export async function queryEvents() {
  const pool = getPool();
  await usePoolConnection(pool, async (con) => {
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
  await usePoolConnection(pool, async (con) => {
    const sql = 'insert big_table ( code , label, num, ts) values ?';
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

(async () => {
  await queryRowsCancel();
})();
