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
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861448752, "name1", 20, "male", "中国人")');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861449753, "name2", 30, "female", "苏州人")');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861450754, "name3", null, null, null)');

    var exp = [
        {ts:'2022-09-11 09:57:28.752', name:'name1', age:20, sex:'male', text:'中国人'},
        {ts:'2022-09-11 09:57:29.753', name:'name2', age:30, sex:'female', text:'苏州人'},
        {ts:'2022-09-11 09:57:30.754', name:'name3', age:null, sex:null, text:null}];
    {
      var cursor = await conn.query('select * from foo.t' , {cursor: true, fetchSize: 3});
      var result = await cursor.fetch();
      var rows = [result[0], result[1], result[2]];
      assert.equal(JSON.stringify(rows), JSON.stringify(exp));
    }

    {
      var result = await conn.query('select * from foo.t');
      var rows = [result[0], result[1], result[2]];
      assert.equal(JSON.stringify(rows), JSON.stringify(exp));
    }

    {
      var cursor = await conn.query('select * from foo.t' , {cursor: true, fetchSize: 1});
      var result = await cursor.fetch();
      assert.equal(JSON.stringify(result[0]), JSON.stringify(exp[0]));

      result = await cursor.fetch();
      assert.equal(JSON.stringify(result[0]), JSON.stringify(exp[1]));

      result = await cursor.fetch();
      assert.equal(JSON.stringify(result[0]), JSON.stringify(exp[2]));

      result = await cursor.fetch();
      assert.equal(result[0], undefined);
    }

    {
      var cursor = await conn.query('select * from foo.t' , {cursor: true, fetchSize: 2});
      var result = await cursor.fetch();
      assert.equal(JSON.stringify(result[0]), JSON.stringify(exp[0]));
      assert.equal(JSON.stringify(result[1]), JSON.stringify(exp[1]));

      result = await cursor.fetch();
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

async function case2(conn_str) {
  var r = 0;
  const conn = await odbc.connect(conn_str);

  try {
    await conn.query('show databases');
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query('create table if not exists t (ts timestamp, v timestamp)');
    await conn.query('select * from t');
    await conn.query('insert into t (ts, v) values (1662861448752, 1662871448752)');
    await conn.query('insert into t (ts, v) values (1662861449753, 1662871449753)');
    await conn.query('insert into t (ts, v) values (1662861450754, 1662871450754)');
    await conn.query('select * from t');

    const stmt = await conn.createStatement();

    try {
      await stmt.prepare('insert into t values (?, ?)');
      await stmt.bind(
        [1662861451755, 1662871450755]
      );
      const result = await stmt.execute();
      console.log(result);
    } catch (error) {
      console.error(error);
      r = -1;
    }

    await stmt.close();
  } catch (error) {
    console.error(error);
    r = -1;
  }

  await conn.close();
  return r;
}

async function case3(conn_str) {
  var r = 0;
  const conn = await odbc.connect(conn_str);

  try {
    await conn.query('show databases');
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query('create table if not exists t (ts timestamp, v varchar(10))');
    await conn.query('select * from t');
    await conn.query('insert into t (ts, v) values (1662861448752, "name1")');
    await conn.query('insert into t (ts, v) values (1662861449753, "name2")');
    await conn.query('insert into t (ts, v) values (1662861450754, "name3")');
    await conn.query('select * from t');

    const stmt = await conn.createStatement();

    try {
      await stmt.prepare('insert into t values (?, ?)');
      await stmt.bind(
        [1662861451755, 'name4']
      );
      const result = await stmt.execute();
      console.log(result);
    } catch (error) {
      console.error(error);
      r = -1;
    }

    await stmt.close();
  } catch (error) {
    console.error(error);
    r = -1;
  }

  await conn.close();
  return r;
}

async function case4(conn_str) {
  var r = 0;
  const conn = await odbc.connect(conn_str);

  try {
    await conn.query('show databases');
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query('create table if not exists t (ts timestamp, v int)');
    await conn.query('select * from t');
    await conn.query('insert into t (ts, v) values (1662861448752, 1)');
    await conn.query('insert into t (ts, v) values (1662861449753, 2)');
    await conn.query('insert into t (ts, v) values (1662861450754, 3)');
    await conn.query('select * from t');

    const stmt = await conn.createStatement();

    try {
      await stmt.prepare('insert into t values (?, ?)');
      await stmt.bind(
        [1662861451755, 4]
      );
      const result = await stmt.execute();
      console.log(result);
    } catch (error) {
      console.error(error);
      r = -1;
    }

    await stmt.close();
  } catch (error) {
    console.error(error);
    r = -1;
  }

  await conn.close();
  return r;
}

async function case5(conn_str) {
  var r = 0;
  const conn = await odbc.connect(conn_str);

  try {
    await conn.query('show databases');
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query('create table if not exists t (ts timestamp, name varchar(5), age int, sex varchar(8), text nchar(3))');
    await conn.query('select * from t');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861448752, "name1", 20, "male", "中国人")');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861449753, "name2", 30, "female", "苏州人")');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861450754, "name3", null, null, null)');
    await conn.query('select * from t');

    const stmt = await conn.createStatement();

    try {
      await stmt.prepare('insert into t values (?,?,?,?,?)');
      await stmt.bind(
        [1662861451755, 'name4', 40, 'male', '外星人'],
      );
      result = await stmt.execute();
      console.log(result);
      await stmt.bind(
        [1662861452756, 'name5', 50, 'female', '类地人'],
      );
      result = await stmt.execute();
      console.log(result);
      await stmt.bind(
        [1662861453757, null, null, null, null],
      );
      result = await stmt.execute();
      console.log(result);
    } catch (error) {
      console.error(error);
      r = -1;
    }

    await stmt.close();
  } catch (error) {
    console.error(error);
    r = -1;
  }

  await conn.close();
  return r;
}

async function do_test_cases() {
  assert.equal(await connectToDatabase('DSN=xTAOS_ODBC_DSN'), -1);
  assert.equal(await connectToDatabase('DSN=TAOS_ODBC_DSN'), 0);
  assert.equal(await execute('DSN=TAOS_ODBC_DSN', 'show databases'), 0);
  // assert.equal(await execute('DSN=TAOS_ODBC_DSN', 'select ts, name from foo.t'), 0);
  // assert.equal(await execute('DSN=TAOS_ODBC_DSN; LEGACY', 'select * from foo.t'), -1);
  assert.equal(await execute('DSN=TAOS_ODBC_DSN', 'xshow databases'), -1);
  assert.equal(await case1('DSN=TAOS_ODBC_DSN'), 0);
  assert.equal(await case2('DSN=TAOS_ODBC_DSN'), 0);
  assert.equal(await case3('DSN=TAOS_ODBC_DSN'), 0);
  assert.equal(await case4('DSN=TAOS_ODBC_DSN'), 0);
  assert.equal(await case5('DSN=TAOS_ODBC_DSN'), 0);

  return 0;
}

(async () => {
  assert.equal(await do_test_cases(), 0);
})()

