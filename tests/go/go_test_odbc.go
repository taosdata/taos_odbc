package main

import (
    _ "github.com/alexbrainman/odbc"
    "database/sql"
    "log"
    "fmt"
    )

func main() {
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
}

