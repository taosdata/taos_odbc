extern crate odbc;
// Use this crate and set environmet variable RUST_LOG=odbc to see ODBC warnings
extern crate env_logger;
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

fn _query(conn: &Connection<'_, AutocommitOn>, sql: &str) -> Result<bool> {
  let stmt = Statement::with_parent(conn)?;
  match stmt.exec_direct(sql)? {
    Data(mut stmt) => {
      let cols = stmt.num_result_cols()?;
      while let Some(mut cursor) = stmt.fetch()? {
        for i in 1..(cols + 1) {
          match cursor.get_data::<String>(i as u16)? {
            Some(val) => print!(" {}", val),
              None => print!(" NULL"),
          }
        }
        println!("");
      }
      Ok(true)
    }
    NoData(_) => {
      println!("");
      Ok(false)
    }
  }
}

fn test_query(conn: &Connection<'_, AutocommitOn>, sql: &str) -> bool {
  match _query(conn, sql) {
    Ok(_) => true,
    _     => false,
  }
}

fn do_test_cases(env: &Environment<Odbc3>) {
  assert_eq!(test_connect(&env, "DSN=xTAOS_ODBC_DSN"), false);
  assert_eq!(test_connect(&env, "DSN=TAOS_ODBC_DSN"), true);

  let conn = env.connect_with_connection_string("DSN=TAOS_ODBC_DSN").unwrap();
  assert_eq!(test_execute(&conn, "xshow databases"), false);
  assert_eq!(test_execute(&conn, "show databases"), true);
  assert_eq!(test_execute(&conn, r##"drop database if exists foo"##), true);
  assert_eq!(test_execute(&conn, r##"create database if not exists foo"##), true);
  assert_eq!(test_execute(&conn, r##"use foo"##), true);
  assert_eq!(test_execute(&conn, r##"drop table if exists t"##), true);
  assert_eq!(test_execute(&conn, r##"create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))"##), true);
  assert_eq!(test_execute(&conn, r##"select * from t"##), true);
  assert_eq!(test_execute(&conn, r##"insert into t (ts, name, age, sex, text) values (now+0s, "name1", 20, "male", "中国人")"##), true);
  assert_eq!(test_execute(&conn, r##"insert into t (ts, name, age, sex, text) values (now+1s, "name2", 30, "female", "苏州人")"##), true);
  assert_eq!(test_execute(&conn, r##"insert into t (ts, name, age, sex, text) values (now+2s, "name3", null, null, null)"##), true);
  assert_eq!(test_execute(&conn, r##"select * from t"##), true);
  assert_eq!(test_execute(&conn, r##"select "abc" union all select "bcd" union all select "cde""##), true);
  assert_eq!(test_query(&conn, r##"select "abc" union all select "bcd" union all select "cde""##), true);
  assert_eq!(test_query(&conn, r##"select * from t"##), true);

  let conn = env.connect_with_connection_string("DSN=TAOS_ODBC_DSN;FMT_TIME").unwrap();
  assert_eq!(test_query(&conn, r##"select * from foo.t"##), true);
}

