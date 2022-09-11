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
    // console.log(result);
    // const cursor = await conn.query(sql, {cursor: true, fetchSize: 1});
    // const result = await cursor.fetch();
    // console.log(result);
    // await cursor.close();
    // await conn.close();
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
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query('create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))');
    await conn.query('select * from t');
    await conn.query('insert into t (ts, name, age, sex, text) values (now+0s, "name1", 20, "male", "中国人")');
    await conn.query('insert into t (ts, name, age, sex, text) values (now+1s, "name2", 30, "female", "苏州人")');
    await conn.query('insert into t (ts, name, age, sex, text) values (now+2s, "name3", null, null, null)');

    var exp = [{name:'name1', age:20, sex:'male', text:'中国人'},
        {name:'name2', age:30, sex:'female', text:'苏州人'},
        {name:'name3', age:null, sex:null, text:null}];
    {
      var cursor = await conn.query('select * from foo.t' , {cursor: true, fetchSize: 3});
      var result = await cursor.fetch();
      delete result[0].ts;
      delete result[1].ts;
      delete result[2].ts;
      var rows = [result[0], result[1], result[2]];
      assert.equal(JSON.stringify(rows), JSON.stringify(exp));
    }

    {
      var result = await conn.query('select * from foo.t');
      delete result[0].ts;
      delete result[1].ts;
      delete result[2].ts;
      var rows = [result[0], result[1], result[2]];
      assert.equal(JSON.stringify(rows), JSON.stringify(exp));
    }

    {
      var cursor = await conn.query('select * from foo.t' , {cursor: true, fetchSize: 1});
      var result = await cursor.fetch();
      delete result[0].ts;
      assert.equal(JSON.stringify(result[0]), JSON.stringify(exp[0]));

      result = await cursor.fetch();
      delete result[0].ts;
      assert.equal(JSON.stringify(result[0]), JSON.stringify(exp[1]));

      result = await cursor.fetch();
      delete result[0].ts;
      assert.equal(JSON.stringify(result[0]), JSON.stringify(exp[2]));

      result = await cursor.fetch();
      assert.equal(result[0], undefined);
    }

    {
      var cursor = await conn.query('select * from foo.t' , {cursor: true, fetchSize: 2});
      var result = await cursor.fetch();
      delete result[0].ts;
      delete result[1].ts;
      assert.equal(JSON.stringify(result[0]), JSON.stringify(exp[0]));
      assert.equal(JSON.stringify(result[1]), JSON.stringify(exp[1]));

      result = await cursor.fetch();
      delete result[0].ts;
      assert.equal(JSON.stringify(result[0]), JSON.stringify(exp[2]));

      result = await cursor.fetch();
      assert.equal(result[0], undefined);
    }

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
  assert.equal(await case1('DSN=TAOS_ODBC_DSN'), 0);
  assert.equal(await execute('DSN=TAOS_ODBC_DSN', 'show databases'), 0);
  assert.equal(await execute('DSN=TAOS_ODBC_DSN', 'select ts, name from foo.t'), 0);
  assert.equal(await execute('DSN=TAOS_ODBC_DSN; LEGACY', 'select * from foo.t'), -1);
  assert.equal(await execute('DSN=TAOS_ODBC_DSN', 'xshow databases'), -1);
  assert.equal(await execute('DSN=TAOS_ODBC_DSN', 'select * from foo.t'), 0);
  assert.equal(await execute('DSN=TAOS_ODBC_DSN;FMT_TIME', 'select * from foo.t'), 0);

  return 0;
}

(async () => {
  assert.equal(await do_test_cases(), 0);
})()

