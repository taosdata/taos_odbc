extern crate odbc;
// Use this crate and set environmet variable RUST_LOG=odbc to see ODBC warnings
extern crate env_logger;

extern crate json;

use odbc::*;
use odbc_safe::AutocommitOn;
use odbc_safe::Odbc3;

fn main() {

  env_logger::init();

  let env: Environment<Odbc3> = create_environment_v3().unwrap();
  do_test_cases_in_env(&env)
}

fn _test_connect(env: &Environment<Odbc3>, conn_str: &str) -> bool {
  match env.connect_with_connection_string(conn_str) {
    Ok(_) => { true }
    _     => { false }
  }
}

fn _exec_direct(conn: &Connection<'_, AutocommitOn>, sql: &str) -> Result<bool> {
  let stmt = Statement::with_parent(conn)?;
  let result = stmt.exec_direct(sql)?;
  match result {
    Data(_)   => { Ok(true) }
    NoData(_) => { Ok(false) }
  }
}

fn _test_exec_direct(conn: &Connection<'_, AutocommitOn>, sql: &str) -> bool {
  match _exec_direct(conn, sql) {
    Ok(_) => { true }
    _ => { false }
  }
}

fn _query(conn: &Connection<'_, AutocommitOn>, sql: &str) -> Result<json::JsonValue> {
  let stmt = Statement::with_parent(conn)?;
  match stmt.exec_direct(sql)? {
    Data(mut stmt) => {
      let mut rs = json::JsonValue::new_array();
      let cols = stmt.num_result_cols()?;
      let mut names = Vec::new();
      for i in 1..(cols + 1) {
        names.push(stmt.describe_col(i as u16).unwrap().name);
      }
      while let Some(mut cursor) = stmt.fetch()? {
        let mut row = json::JsonValue::new_array();
        for i in 1..(cols + 1) {
          let mut _o = json::JsonValue::new_object();
          let _k = &names[(i-1) as usize];
          match cursor.get_data::<String>(i as u16)? {
            Some(_val) => {
              _o.insert(_k, _val).unwrap();
              row.push(_o).unwrap();
            }
            None => {
              _o.insert(_k, json::JsonValue::Null).unwrap();
              row.push(_o).unwrap();
            }
          }
        }
        rs.push(row).unwrap();
      }
      Ok(rs)
    }
    NoData(_) => {
      Ok(json::JsonValue::Null)
    }
  }
}

fn _test_query(conn: &Connection<'_, AutocommitOn>, sql: &str) -> bool {
  match _query(conn, sql) {
    Ok(_) => { true }
    _     => { false }
  }
}

fn _case1(conn: &Connection<'_, AutocommitOn>) -> Result<bool> {
  assert_eq!(_test_exec_direct(&conn, r#"drop database if exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create database if not exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"use foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"drop table if exists t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"select * from t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861448752, "name1", 20, "male", "中国人")"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861449753, "name2", 30, "female", "苏州人")"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861450754, "name3", null, null, null)"#), true);

  let _parsed = json::array![
    [{ts:r#"2022-09-11 09:57:28.752"#}, {name:r#"name1"#}, {age:"20"}, {sex:r#"male"#}, {text:r#"中国人"#}],
    [{ts:r#"2022-09-11 09:57:29.753"#}, {name:r#"name2"#}, {age:"30"}, {sex:r#"female"#}, {text:r#"苏州人"#}],
    [{ts:r#"2022-09-11 09:57:30.754"#}, {name:r#"name3"#}, {age:null}, {sex:null}, {text:null}],
  ];

    let _rs = _query(&conn, r#"select * from t"#).unwrap();

    assert_eq!(_rs, _parsed);

    Ok(true)
}

fn _test_case1(conn: &Connection<'_, AutocommitOn>) -> bool {
  _case1(conn).unwrap()
}

fn _execute_to_json<'a> (stmt: Statement<'a,'a, Prepared, NoResult, safe::AutocommitOn>, jv: &mut json::JsonValue) ->
Result<Statement<'a, 'a, Prepared, NoResult, safe::AutocommitOn>>
{
  let stmt = if let Data(mut stmt) = stmt.execute()? {
    let cols = stmt.num_result_cols()?;
    let mut names = Vec::new();
    for i in 1..(cols + 1) {
      names.push(stmt.describe_col(i as u16).unwrap().name);
    }
    while let Some(mut cursor) = stmt.fetch()? {
      let mut row = json::JsonValue::new_array();
      for i in 1..(cols + 1) {
        let mut _o = json::JsonValue::new_object();
        let _k = &names[(i-1) as usize];
        match cursor.get_data::<String>(i as u16)? {
          Some(_val) => {
            _o.insert(_k, _val).unwrap();
            row.push(_o).unwrap();
          }
          None => {
            _o.insert(_k, json::JsonValue::Null).unwrap();
            row.push(_o).unwrap();
          }
        }
      }
      jv.push(row).unwrap();
    }
    stmt.close_cursor()?
  } else {
    panic!("SELECT statement returned no result set");
  };
  stmt.reset_parameters()
}

fn _test_case2(conn: &Connection<'_, AutocommitOn>) {
  assert_eq!(_test_exec_direct(&conn, r#"drop database if exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create database if not exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"use foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"drop table if exists t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"select * from t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861448752, "name1", 20, "male", "中国人")"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861449753, "name2", 30, "female", "苏州人")"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861450754, "name3", null, null, null)"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861451755, "3245", null, null, null)"#), true);

  let _parsed = json::array![
    [{ts:r#"2022-09-11 09:57:28.752"#}, {name:r#"name1"#}, {age:"20"}, {sex:r#"male"#}, {text:r#"中国人"#}],
    [{ts:r#"2022-09-11 09:57:29.753"#}, {name:r#"name2"#}, {age:"30"}, {sex:r#"female"#}, {text:r#"苏州人"#}],
    [{ts:r#"2022-09-11 09:57:30.754"#}, {name:r#"name3"#}, {age:null}, {sex:null}, {text:null}],
    [{ts:r#"2022-09-11 09:57:31.755"#}, {name:r#"3245"#}, {age:null}, {sex:null}, {text:null}],
  ];

    let stmt = Statement::with_parent(conn).unwrap();

    let stmt = stmt.prepare("select * from foo.t where name = ?").unwrap();

    let name = "name2";
    let stmt = stmt.bind_parameter(1, &name).unwrap();
    let mut jv = json::JsonValue::new_array();
    let stmt = _execute_to_json(stmt, &mut jv).unwrap();
    assert_eq!(jv[0].dump(), _parsed[1].dump());

    let name = "name1";
    let stmt = stmt.bind_parameter(1, &name).unwrap();
    let mut jv = json::JsonValue::new_array();
    let stmt = _execute_to_json(stmt, &mut jv).unwrap();
    assert_eq!(jv[0].dump(), _parsed[0].dump());

    let name = "name3";
    let stmt = stmt.bind_parameter(1, &name).unwrap();
    let mut jv = json::JsonValue::new_array();
    let _stmt = _execute_to_json(stmt, &mut jv).unwrap();
    assert_eq!(jv[0].dump(), _parsed[2].dump());

    let stmt = Statement::with_parent(conn).unwrap();
    let stmt = stmt.prepare("select * from foo.t where name <> ?").unwrap();
    let name = "name";
    let stmt = stmt.bind_parameter(1, &name).unwrap();
    let mut jv = json::JsonValue::new_array();
    let _stmt = _execute_to_json(stmt, &mut jv).unwrap();
    assert_eq!(jv.dump(), _parsed.dump());

    let stmt = Statement::with_parent(conn).unwrap();
    let stmt = stmt.prepare("select * from foo.t where age = ?").unwrap();
    let age = 30;
    let stmt = stmt.bind_parameter(1, &age).unwrap();
    let mut jv = json::JsonValue::new_array();
    let _stmt = _execute_to_json(stmt, &mut jv).unwrap();
    assert_eq!(jv[0].dump(), _parsed[1].dump());

    let stmt = Statement::with_parent(conn).unwrap();
    let stmt = stmt.prepare("select * from foo.t where name = ?").unwrap();
    let name = 3245;
    let stmt = stmt.bind_parameter(1, &name).unwrap();
    let mut jv = json::JsonValue::new_array();
    let _stmt = _execute_to_json(stmt, &mut jv).unwrap();
    assert_eq!(jv[0].dump(), _parsed[3].dump());
}

fn _test_case3(conn: &Connection<'_, AutocommitOn>) {
  assert_eq!(_test_exec_direct(&conn, r#"drop database if exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create database if not exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"use foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"drop table if exists t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create table if not exists t (ts timestamp, age bigint)"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"select * from t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, age) values (1662861448752, 1662861448752)"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, age) values (1662861449753, 1662861449753)"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, age) values (1662861450754, 1662861450754)"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, age) values (1662861451755, 1662861451755)"#), true);

  let _parsed = json::array![
    [{ts:r#"2022-09-11 09:57:28.752"#}, {age:r#"1662861448752"#}],
    [{ts:r#"2022-09-11 09:57:29.753"#}, {age:r#"1662861449753"#}],
    [{ts:r#"2022-09-11 09:57:30.754"#}, {age:r#"1662861450754"#}],
    [{ts:r#"2022-09-11 09:57:31.755"#}, {age:r#"1662861451755"#}],
  ];

    let stmt = Statement::with_parent(conn).unwrap();

    let stmt = stmt.prepare("select * from foo.t where age = ?").unwrap();
    let age = 1662861449753i64;
    let stmt = stmt.bind_parameter(1, &age).unwrap();
    let mut jv = json::JsonValue::new_array();
    let _stmt = _execute_to_json(stmt, &mut jv).unwrap();
    assert_eq!(jv[0].dump(), _parsed[1].dump());
}

fn _test_case4(conn: &Connection<'_, AutocommitOn>) {
  assert_eq!(_test_exec_direct(&conn, r#"drop database if exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create database if not exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"use foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"drop table if exists t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"select * from t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861448752, "name1", 20, "male", "中国人")"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861449753, "name2", 30, "female", "苏州人")"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861450754, "name3", null, null, null)"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861451755, "3245", null, null, null)"#), true);

  let _parsed = json::array![
    [{ts:r#"2022-09-11 09:57:28.752"#}, {name:r#"name1"#}, {age:"20"}, {sex:r#"male"#}, {text:r#"中国人"#}],
    [{ts:r#"2022-09-11 09:57:29.753"#}, {name:r#"name2"#}, {age:"30"}, {sex:r#"female"#}, {text:r#"苏州人"#}],
    [{ts:r#"2022-09-11 09:57:30.754"#}, {name:r#"name3"#}, {age:null}, {sex:null}, {text:null}],
    [{ts:r#"2022-09-11 09:57:31.755"#}, {name:r#"3245"#}, {age:null}, {sex:null}, {text:null}],
  ];

    let stmt = Statement::with_parent(conn).unwrap();

    let stmt = stmt.prepare("select * from foo.t where name = ?").unwrap();

    let name = "name2";
    let stmt = stmt.bind_parameter(1, &name).unwrap();
    let mut jv = json::JsonValue::new_array();
    let stmt = _execute_to_json(stmt, &mut jv).unwrap();
    assert_eq!(jv[0].dump(), _parsed[1].dump());

    let name = "name1";
    let stmt = stmt.bind_parameter(1, &name).unwrap();
    let mut jv = json::JsonValue::new_array();
    let stmt = _execute_to_json(stmt, &mut jv).unwrap();
    assert_eq!(jv[0].dump(), _parsed[0].dump());

    let name = "name3";
    let stmt = stmt.bind_parameter(1, &name).unwrap();
    let mut jv = json::JsonValue::new_array();
    let _stmt = _execute_to_json(stmt, &mut jv).unwrap();
    assert_eq!(jv[0].dump(), _parsed[2].dump());

    let stmt = Statement::with_parent(conn).unwrap();
    let stmt = stmt.prepare("select * from foo.t where name <> ?").unwrap();
    let name = "name";
    let stmt = stmt.bind_parameter(1, &name).unwrap();
    let mut jv = json::JsonValue::new_array();
    let _stmt = _execute_to_json(stmt, &mut jv).unwrap();
    assert_eq!(jv.dump(), _parsed.dump());

    let stmt = Statement::with_parent(conn).unwrap();
    let stmt = stmt.prepare("select * from foo.t where age = ?").unwrap();
    let age = 30;
    let stmt = stmt.bind_parameter(1, &age).unwrap();
    let mut jv = json::JsonValue::new_array();
    let _stmt = _execute_to_json(stmt, &mut jv).unwrap();
    assert_eq!(jv[0].dump(), _parsed[1].dump());

    let stmt = Statement::with_parent(conn).unwrap();
    let stmt = stmt.prepare("select * from foo.t where name = ?").unwrap();
    let name = 3245;
    let stmt = stmt.bind_parameter(1, &name).unwrap();
    let mut jv = json::JsonValue::new_array();
    let _stmt = _execute_to_json(stmt, &mut jv).unwrap();
    assert_eq!(jv[0].dump(), _parsed[3].dump());
}

fn do_test_cases_in_conn(conn: &Connection<'_, AutocommitOn>)
{
  assert_eq!(_test_exec_direct(&conn, "xshow databases"), false);
  assert_eq!(_test_exec_direct(&conn, "show databases"), true);
  assert_eq!(_test_exec_direct(&conn, r#"drop database if exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create database if not exists foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"use foo"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"drop table if exists t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"select * from t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861448752, "name1", 20, "male", "中国人")"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861449753, "name2", 30, "female", "苏州人")"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861450754, "name3", null, null, null)"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"select * from t"#), true);
  assert_eq!(_test_exec_direct(&conn, r#"select "abc" union all select "bcd" union all select "cde""#), true);
  assert_eq!(_test_query(&conn, r#"select "abc" union all select "bcd" union all select "cde""#), true);
  assert_eq!(_test_query(&conn, r#"select * from t"#), true);
  assert_eq!(_test_case1(&conn), true);
  assert_eq!(_test_exec_direct(&conn, r#"select * from t where name = ?"#), false);
  assert_eq!(_test_exec_direct(&conn, r#"insert into t (ts, name) values (now, ?)"#), false);
  _test_case2(&conn);
  _test_case3(&conn);
  _test_case4(&conn);
}

fn do_test_cases_in_env(env: &Environment<Odbc3>) {
  assert_eq!(_test_connect(env, "DSN=xTAOS_ODBC_DSN"), false);
  assert_eq!(_test_connect(env, "DSN=TAOS_ODBC_DSN"), true);

  let conn = env.connect_with_connection_string("DSN=TAOS_ODBC_DSN").unwrap();
  do_test_cases_in_conn(&conn);
}

#[cfg(test)]
mod tests {
    #[test]
    fn exploration() {
        assert_eq!(2 + 2, 4);
    }
}

