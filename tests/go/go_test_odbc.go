package main

import (
    _ "github.com/alexbrainman/odbc"
    "database/sql"
    "log"
    "fmt"
    "os"
    "reflect"
    "runtime"
    "strconv"
    )

func test_sql_server() {
  db, err := sql.Open("odbc", "DSN=SQLSERVER_ODBC_DSN")
  if err != nil {
    log.Fatal(err)
  }

  var (
      name string
      mark string
      )

  // rows, err := db.Query("SELECT * FROM x WHERE name = ?", "测试")
  rows, err := db.Query("SELECT * FROM x")
  if err != nil {
    log.Fatal(err)
  }
  defer rows.Close()
  for rows.Next() {
    err := rows.Scan(&name, &mark)
    if err != nil {
      log.Fatal(err)
    }
    fmt.Println(name, mark)
  }
  err = rows.Err()
  if err != nil {
    log.Fatal(err)
  }

  defer db.Close()
}

func test_chars() {
  db, err := sql.Open("odbc", "DSN=TAOS_ODBC_DSN")
  if err != nil {
    log.Fatal(err)
  }

  var (
      name string
      mark string
      )

  // rows, err := db.Query("SELECT * FROM x WHERE name = ?", "测试")
  rows, err := db.Query("SELECT name, mark FROM bar.x")
  if err != nil {
    log.Fatal(err)
  }
  defer rows.Close()
  for rows.Next() {
    err := rows.Scan(&name, &mark)
    if err != nil {
      log.Fatal(err)
    }
    fmt.Println(name, mark)
  }
  err = rows.Err()
  if err != nil {
    log.Fatal(err)
  }

  defer db.Close()
}

func test() {
  if false {
    test_sql_server()
    log.Fatal("==success==")
  }
  if false {
    test_chars()
    log.Fatal("==success==")
  }

  // db, err := sql.Open("odbc", "DSN=TAOS_ODBC_DSN")
  db, err := sql.Open("odbc", "Driver={TAOS_ODBC_DRIVER}")
  if err != nil {
    log.Fatal(err)
  }

  var (
      ts string
      name string
      mark string
      )

  res, err := db.Exec("drop database if exists xyz")
  if err != nil { log.Fatal(err) }
  n, err := res.RowsAffected()
  if err != nil { log.Fatal(err) }
  fmt.Println(n)

  // nn, err := res.LastInsertId()
  // if err != nil { log.Fatal(err) }
  // fmt.Println(nn)

  rows, err := db.Query("SELECT * FROM bar.x WHERE name = ?", "人a")
  if err != nil {
    log.Fatal(err)
  }
  defer rows.Close()
  for rows.Next() {
    err := rows.Scan(&ts, &name, &mark)
    if err != nil {
      log.Fatal(err)
    }
    fmt.Println(ts, name, mark)
  }
  err = rows.Err()
  if err != nil {
    log.Fatal(err)
  }

  defer db.Close()

  fmt.Println("==success==")
}

func test_case0() int {
  return 0
}

func test_case1() int {
  return 0
}

func usage(arg0 string) {
  fmt.Fprintf(os.Stderr,
      "%v -h\n"                           +
      "  show this help page\n"           +
      "%v [name]...\n"                    +
      "  running test case `name`\n"      +
      "%v -l\n"                           +
      "  list all test cases\n",
      arg0, arg0, arg0)
}

func find_case(cases []func() int, name string) func() int {
  for _,c := range cases {
    if runtime.FuncForPC(reflect.ValueOf(c).Pointer()).Name() == "main." + name {
      return c
    }
  }
  return nil
}

func list_cases(cases []func() int) {
  fmt.Println("supported test cases:")
  for _,c := range cases {
    fmt.Println("  " + runtime.FuncForPC(reflect.ValueOf(c).Pointer()).Name()[5:])
  }
}

func run_case(c func() int) {
  r := c()
  if r != 0 {
    log.Fatal(runtime.FuncForPC(reflect.ValueOf(c).Pointer()).Name() + " => " + strconv.Itoa(r) + ":failed")
  }
}

func run_cases(cases []func () int) {
  for _,c := range cases {
    run_case(c)
  }
}

func main() {
  cases := []func () int { test_case0, test_case1 }
  nr_args := len(os.Args)
  nr_cases := 0
  if nr_args > 1 {
    for i:=1; i<nr_args; i+=1 {
      arg := os.Args[i]
      if arg == "-h" {
        usage(os.Args[0])
        os.Exit(0)
      }
      if arg == "-l" {
        list_cases(cases)
        os.Exit(0)
      }
      c := find_case(cases, arg)
      if c == nil {
        log.Fatal("test case `" + arg + "`:not found")
      }
      run_case(c)
      nr_cases += 1
    }
  }

  if nr_cases == 0 {
    run_cases(cases)
  }

  fmt.Println("==success==")
  os.Exit(1)
}

