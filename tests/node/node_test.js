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
      await cursor.close();
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

      await cursor.close();
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

      await cursor.close();
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
        [1662861451755, 1662871451755]
      );
      const result = await stmt.execute();
    } catch (error) {
      console.error(error);
      r = -1;
    }

    await stmt.close();

    if (r == 0) {
      var exp = [
          {ts:'2022-09-11 09:57:28.752', v:'2022-09-11 12:44:08.752'},
          {ts:'2022-09-11 09:57:29.753', v:'2022-09-11 12:44:09.753'},
          {ts:'2022-09-11 09:57:30.754', v:'2022-09-11 12:44:10.754'},
          {ts:'2022-09-11 09:57:31.755', v:'2022-09-11 12:44:11.755'},
      ];
      {
        var cursor = await conn.query('select * from t' , {cursor: true, fetchSize: 5});
        var result = await cursor.fetch();
        await cursor.close();
        var rows = [result[0], result[1], result[2], result[3]];
        assert.equal(JSON.stringify(rows), JSON.stringify(exp));
      }
    }
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
    } catch (error) {
      console.error(error);
      r = -1;
    }

    await stmt.close();

    if (r == 0) {
      var exp = [
          {ts:'2022-09-11 09:57:28.752', v:'name1'},
          {ts:'2022-09-11 09:57:29.753', v:'name2'},
          {ts:'2022-09-11 09:57:30.754', v:'name3'},
          {ts:'2022-09-11 09:57:31.755', v:'name4'},
      ];
      {
        var cursor = await conn.query('select * from t' , {cursor: true, fetchSize: 5});
        var result = await cursor.fetch();
        await cursor.close();
        var rows = [result[0], result[1], result[2], result[3]];
        assert.equal(JSON.stringify(rows), JSON.stringify(exp));
      }
    }
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
    } catch (error) {
      console.error(error);
      r = -1;
    }

    await stmt.close();

    if (r == 0) {
      var exp = [
      {ts:'2022-09-11 09:57:28.752', v:1},
      {ts:'2022-09-11 09:57:29.753', v:2},
      {ts:'2022-09-11 09:57:30.754', v:3},
      {ts:'2022-09-11 09:57:31.755', v:4},
      ];
      {
        var cursor = await conn.query('select * from t' , {cursor: true, fetchSize: 5});
        var result = await cursor.fetch();
        await cursor.close();
        var rows = [result[0], result[1], result[2], result[3]];
        assert.equal(JSON.stringify(rows), JSON.stringify(exp));
      }
    }
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

    var exps = [
        {ts:'2022-09-11 09:57:28.752', name:'name1', age:20, sex:'male', text:'中国人'},
        {ts:'2022-09-11 09:57:29.753', name:'name2', age:30, sex:'female', text:'苏州人'},
        {ts:'2022-09-11 09:57:30.754', name:'name3', age:null, sex:null, text:null},
        {ts:'2022-09-11 09:57:31.755', name:'name4', age:40, sex:'male', text:'外星人'},
        {ts:'2022-09-11 09:57:32.756', name:'12345', age:50, sex:'female', text:'类地人'},
        {ts:'2022-09-11 09:57:33.757', name:null, age:null, sex:null, text:null},
        {ts:'2022-09-11 09:57:34.758', name:'54321', age:60, sex:'unknown', text:'测试人'},
    ];

    stmt = await conn.createStatement();

    try {
      await stmt.prepare('insert into t values (?,?,?,?,?)');
      await stmt.bind(
        [1662861451755, 'name4', 40, 'male', '外星人'],
      );
      result = await stmt.execute();
      {
        var cursor = await conn.query('select * from t' , {cursor: true, fetchSize: 4});
        var result = await cursor.fetch();
        await cursor.close();
        var rows = [result[0], result[1], result[2], result[3]];
        var exp = [exps[0], exps[1], exps[2], exps[3]];
        assert.equal(JSON.stringify(rows), JSON.stringify(exp));
      }

      await stmt.bind(
        ['1662861452756', '12345', 50, 'female', '类地人'],
      );
      result = await stmt.execute();
      {
        var cursor = await conn.query('select * from t' , {cursor: true, fetchSize: 5});
        var result = await cursor.fetch();
        await cursor.close();
        var rows = [result[0], result[1], result[2], result[3], result[4]];
        var exp = [exps[0], exps[1], exps[2], exps[3], exps[4]];
        assert.equal(JSON.stringify(rows), JSON.stringify(exp));
      }

      await stmt.bind(
        ['1662861453757', null, null, null, null],
      );
      result = await stmt.execute();
      {
        var cursor = await conn.query('select * from t', {cursor: true, fetchSize: 6});
        var result = await cursor.fetch();
        await cursor.close();
        var rows = [result[0], result[1], result[2], result[3], result[4], result[5]];
        var exp = [exps[0], exps[1], exps[2], exps[3], exps[4], exps[5]];
        assert.equal(JSON.stringify(rows), JSON.stringify(exp));
      }

    } catch (error) {
      console.error(error);
      r = -1;
    }

    await stmt.close();

    if (r == 0) {
      stmt = await conn.createStatement();

      try {
        await stmt.prepare('select * from t where text = ?');
        await stmt.bind(
            ['苏州人']
            );
        result = await stmt.execute();
        {
          var rows = [result[0]];
          var exp = [exps[1]];
          assert.equal(JSON.stringify(rows), JSON.stringify(exp));
        }

        await stmt.bind(
            ['中国人']
            );
        result = await stmt.execute();
        {
          var rows = [result[0]];
          var exp = [exps[0]];
          assert.equal(JSON.stringify(rows), JSON.stringify(exp));
        }
      } catch (error) {
        console.error(error);
        r = -1;
      }

      await stmt.close();
    }
  } catch (error) {
    console.error(error);
    r = -1;
  }

  await conn.close();
  return r;
}

async function case0(conn_str) {
  try {
    const conn = await odbc.connect(conn_str);
    await conn.query('show databases');
    await conn.query('drop database if exists foo');
    await conn.query('create database if not exists foo');
    await conn.query('use foo');
    await conn.query('drop table if exists t');
    await conn.query('create table if not exists t (ts timestamp, name varchar(20), age double, sex varchar(8), text nchar(3))');
    await conn.query('select * from t');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861448752, "name1", 20, "male", "中国人")');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861449753, "name2", 30, "female", "苏州人")');
    await conn.query('insert into t (ts, name, age, sex, text) values (1662861450754, "name3", null, null, null)');

    var exp = [
        {ts:'2022-09-11 09:57:28.752', name:'name1', age:20, sex:'male', text:'中国人'},
        {ts:'2022-09-11 09:57:29.753', name:'name2', age:30, sex:'female', text:'苏州人'},
        {ts:'2022-09-11 09:57:30.754', name:'name3', age:null, sex:null, text:null}];
    {
      var cursor = await conn.query('select * from foo.t where name="name1"' , {cursor: true, fetchSize: 1});
      var result = await cursor.fetch();
      await cursor.close();
      var rows = [result[0]];
      assert.equal(JSON.stringify(rows), JSON.stringify([exp[0]]));
    }
    {
      var cursor = await conn.query('select * from foo.t' , {cursor: true, fetchSize: 3});
      var result = await cursor.fetch();
      await cursor.close();
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
      await cursor.close();
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
      await cursor.close();
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
  assert.equal(await execute('DSN=TAOS_ODBC_DSN', 'show databases'), 0);
  // assert.equal(await execute('DSN=TAOS_ODBC_DSN', 'select ts, name from foo.t'), 0);
  // assert.equal(await execute('DSN=TAOS_ODBC_DSN; LEGACY', 'select * from foo.t'), -1);
  assert.equal(await execute('DSN=TAOS_ODBC_DSN', 'xshow databases'), -1);
  assert.equal(await case1('DSN=TAOS_ODBC_DSN'), 0);
  assert.equal(await case2('DSN=TAOS_ODBC_DSN'), 0);
  assert.equal(await case3('DSN=TAOS_ODBC_DSN'), 0);
  assert.equal(await case4('DSN=TAOS_ODBC_DSN'), 0);
  assert.equal(await case0('DSN=TAOS_ODBC_DSN'), 0);
  assert.equal(await case5('DSN=TAOS_ODBC_DSN'), 0);

  return 0;
}

(async () => {
  assert.equal(await do_test_cases(), 0);
})()

