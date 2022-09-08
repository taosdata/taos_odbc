#!/usr/bin/env node

const assert = require('assert');
const odbc = require('odbc');


async function connectToDatabase(conn_str) {
  try {
    const conn = await odbc.connect(conn_str);
    await conn.close();
    return 0;
  } catch (error) {
    console.error(error);
    return -1;
  }
}

async function execute(conn_str, sql) {
  try {
    const conn = await odbc.connect(conn_str);
    const result = await conn.query(sql);
    console.log(result);
    await conn.close();
    return 0;
  } catch (error) {
    console.error(error);
    return -1;
  }
}

async function case1(conn_str) {
  try {
    const conn = await odbc.connect(conn_str);
    await conn.query('show databases');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query('create table if not exists t (ts timestamp, v int)');
    await conn.query('select * from t');
    await conn.query('insert into t (ts, v) values (now, 123)');
    await conn.query('select * from t');
    await conn.close();
    return 0;
  } catch (error) {
    console.error(error);
    return -1;
  }
}

async function do_test_cases() {
  assert.equal(await connectToDatabase('DSN=xTAOS_ODBC_DSN'), -1);
  assert.equal(await connectToDatabase('DSN=TAOS_ODBC_DSN'), 0);
  assert.equal(await execute('DSN=TAOS_ODBC_DSN', 'show databases'), 0);
  assert.equal(await case1('DSN=TAOS_ODBC_DSN'), 0);
  assert.equal(await execute('DSN=TAOS_ODBC_DSN', 'select * from foo.t'), 0);

  return 0;
}

(async () => {
  assert.equal(await do_test_cases(), 0);
})()

// assert(connectToDatabase('DSN=xTAOS_ODBC_DSN'), 0);

// odbc.connect(`DSN=TAOS_ODBC_DSN`, (error, connection) => {
//   console.log(error);
//   console.log(connection);
//   connection.createStatement((error, statement) => {
//     console.log(error);
//     console.log(statement);
//     console.log('closing...');
//     statement.close((error) => {
//       console.log(error);
//       connection.close((error) => {
//           console.log(error);
//           console.log(connection);
//           console.log('hello, world');
//       });
//     });
//   });
// });

