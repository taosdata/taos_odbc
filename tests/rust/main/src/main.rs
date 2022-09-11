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
    do_test_cases(&env)
}

fn test_connect(env: &Environment<Odbc3>, conn_str: &str) -> bool {
    match env.connect_with_connection_string(conn_str) {
      Ok(_) => true,
      _     => false,
    }
}

fn _execute(conn: &Connection<'_, AutocommitOn>, sql: &str) -> Result<bool> {
  let stmt = Statement::with_parent(conn)?;
  let result = stmt.exec_direct(sql)?;
  match result {
    Data(_) => Ok(true),
    NoData(_) => Ok(false),
  }
}

fn test_execute(conn: &Connection<'_, AutocommitOn>, sql: &str) -> bool {
  match _execute(conn, sql) {
    Ok(_) => true,
    _     => false,
  }
}

fn _query(conn: &Connection<'_, AutocommitOn>, sql: &str) -> Result<json::JsonValue> {
  let stmt = Statement::with_parent(conn)?;
  match stmt.exec_direct(sql)? {
    Data(mut stmt) => {
      println!("===");
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
              print!("{}:{},", _k, _val);
              _o.insert(_k, _val).unwrap();
              row.push(_o).unwrap();
            }
            None => {
              print!("{}:{},", _k, json::JsonValue::Null);
              _o.insert(_k, json::JsonValue::Null).unwrap();
              row.push(_o).unwrap();
            }
          }
        }
        println!("");
        rs.push(row).unwrap();
      }
      Ok(rs)
    }
    NoData(_) => {
      println!("");
      Ok(json::JsonValue::Null)
    }
  }
}

fn test_query(conn: &Connection<'_, AutocommitOn>, sql: &str) -> bool {
  match _query(conn, sql) {
    Ok(_) => true,
    _     => false,
  }
}

fn _case1(conn: &Connection<'_, AutocommitOn>) -> Result<bool> {
  assert_eq!(test_execute(&conn, r#"drop database if exists foo"#), true);
  assert_eq!(test_execute(&conn, r#"create database if not exists foo"#), true);
  assert_eq!(test_execute(&conn, r#"use foo"#), true);
  assert_eq!(test_execute(&conn, r#"drop table if exists t"#), true);
  assert_eq!(test_execute(&conn, r#"create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))"#), true);
  assert_eq!(test_execute(&conn, r#"select * from t"#), true);
  assert_eq!(test_execute(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861448752, "name1", 20, "male", "中国人")"#), true);
  assert_eq!(test_execute(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861449753, "name2", 30, "female", "苏州人")"#), true);
  assert_eq!(test_execute(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861450754, "name3", null, null, null)"#), true);

  let _parsed = json::array![
    [{ts:r#"2022-09-11 09:57:28.752"#}, {name:r#"name1"#}, {age:"20"}, {sex:r#"male"#}, {text:r#"中国人"#}],
    [{ts:r#"2022-09-11 09:57:29.753"#}, {name:r#"name2"#}, {age:"30"}, {sex:r#"female"#}, {text:r#"苏州人"#}],
    [{ts:r#"2022-09-11 09:57:30.754"#}, {name:r#"name3"#}, {age:null}, {sex:null}, {text:null}],
  ];

  let _rs = _query(&conn, r#"select * from t"#).unwrap();

  assert_eq!(_rs, _parsed);

  Ok(true)
}

fn test_case1(conn: &Connection<'_, AutocommitOn>) -> bool {
  _case1(conn).unwrap()
}

fn do_test_cases(env: &Environment<Odbc3>) {
  assert_eq!(test_connect(&env, "DSN=xTAOS_ODBC_DSN"), false);
  assert_eq!(test_connect(&env, "DSN=TAOS_ODBC_DSN"), true);

  let conn = env.connect_with_connection_string("DSN=TAOS_ODBC_DSN").unwrap();
  assert_eq!(test_execute(&conn, "xshow databases"), false);
  assert_eq!(test_execute(&conn, "show databases"), true);
  assert_eq!(test_execute(&conn, r#"drop database if exists foo"#), true);
  assert_eq!(test_execute(&conn, r#"create database if not exists foo"#), true);
  assert_eq!(test_execute(&conn, r#"use foo"#), true);
  assert_eq!(test_execute(&conn, r#"drop table if exists t"#), true);
  assert_eq!(test_execute(&conn, r#"create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))"#), true);
  assert_eq!(test_execute(&conn, r#"select * from t"#), true);
  assert_eq!(test_execute(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861448752, "name1", 20, "male", "中国人")"#), true);
  assert_eq!(test_execute(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861449753, "name2", 30, "female", "苏州人")"#), true);
  assert_eq!(test_execute(&conn, r#"insert into t (ts, name, age, sex, text) values (1662861450754, "name3", null, null, null)"#), true);
  assert_eq!(test_execute(&conn, r#"select * from t"#), true);
  assert_eq!(test_execute(&conn, r#"select "abc" union all select "bcd" union all select "cde""#), true);
  assert_eq!(test_query(&conn, r#"select "abc" union all select "bcd" union all select "cde""#), true);
  assert_eq!(test_query(&conn, r#"select * from t"#), true);
  assert_eq!(test_case1(&conn), true);
}

