/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "odbc_helpers.h"

#include "enums.h"
#include "test_config.h"

#include <assert.h>
#ifndef _WIN32
#include <dlfcn.h>
#endif
#include <errno.h>
#include <sql.h>
#include <sqlext.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>

#define TEST_CASE_BEG(_func)                                \
  D("test case: %s...", _func)

#define TEST_CASE_END(_func, _exp)                          \
  if (MATCH(r, _exp)) {                                     \
    D("test case: %s --> %s%s%s",                           \
                    _func,                                  \
                    color_green(),                          \
                    "succeeded",                            \
                    color_reset());                         \
  } else if (r) {                                           \
    D("test case: %s --> %s%s%s",                           \
                    _func,                                  \
                    color_red(),                            \
                    "failed",                               \
                    color_reset());                         \
    return -1;                                              \
  } else {                                                  \
    D("test case: %s --> %s%s%s",                           \
                    _func,                                  \
                    color_red(),                            \
                    "failure expected but succeeded",       \
                    color_reset());                         \
    return -1;                                              \
  }

#define CHK0(_func, _exp)                                                                          \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_exp ")");                                                           \
    r = _func();                                                                                   \
    TEST_CASE_END(#_func "(" #_exp ")", _exp);                                                     \
  }

#define CHK1(_func, _arg1, _exp)                                                                   \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_arg1 "," #_exp ")");                                                \
    r = _func(_arg1);                                                                              \
    TEST_CASE_END(#_func "(" #_arg1 "," #_exp ")", _exp);                                          \
  }

#define CHK2(_func, _v1, _v2, _exp)                                                                \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_v1 "," #_v2 "," #_exp ")");                                         \
    r = _func(_v1, _v2);                                                                           \
    TEST_CASE_END(#_func "(" #_v1 "," #_v2 "," #_exp ")", _exp);                                   \
  }

#define CHK3(_func, _v1, _v2, _v3, _exp)                                                           \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_v1 "," #_v2 "," #_v3 "," #_exp ")");                                \
    r = _func(_v1, _v2, _v3);                                                                      \
    TEST_CASE_END(#_func "(" #_v1 "," #_v2 "," #_v3 "," #_exp ")", _exp);                          \
  }

#define CHK4(_func, _v1, _v2, _v3, _v4, _exp)                                                      \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_v1 "," #_v2 "," #_v3 "," #_v4 "," #_exp ")");                       \
    r = _func(_v1, _v2, _v3, _v4);                                                                 \
    TEST_CASE_END(#_func "(" #_v1 "," #_v2 "," #_v3 "," #_v4 "," #_exp ")", _exp);                 \
  }

#define MATCH(a, b)  (!!(a) == !!(b))

__attribute__((unused))
static int test_ok(void)
{
  return 0;
}

__attribute__((unused))
static int test_failure(void)
{
  return -1;
}

__attribute__((unused))
static int test_so(const char *so)
{
  void *h1 = dlopen(so, RTLD_NOW);
  if (h1 == NULL) {
    D("%s", dlerror());
    return -1;
  }

  int (*get_nr_load)(void);

  int r = -1;

  get_nr_load = dlsym(h1, "get_nr_load");
  if (!get_nr_load) {
    D("`get_nr_load` not exported in [%s]", so);
    goto fail_dlsym;
  }

  int nr_load = get_nr_load();
  if (nr_load != 1) {
    D("internal logic error in [%s]", so);
    goto fail_get_nr_load;
  }

  r = 0;

fail_get_nr_load:
fail_dlsym:
  dlclose(h1);

  return r ? -1 : 0;
}

__attribute__((unused))
static int _sql_stmt_get_long_data_by_col(SQLHANDLE stmth, SQLSMALLINT ColumnNumber)
{
  char buf[1024];
  buf[0] = '\0';

  char *p = buf;

  SQLUSMALLINT   Col_or_Param_Num    = ColumnNumber;
  SQLSMALLINT    TargetType          = SQL_C_CHAR;
  SQLPOINTER     TargetValuePtr      = (SQLPOINTER)p;
  // TODO: remove this restriction
  SQLLEN         BufferLength        = 4;
  SQLLEN         StrLen_or_Ind;

  while (1) {
    SQLRETURN r = CALL_SQLGetData(stmth, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, &StrLen_or_Ind);
    if (r == SQL_NO_DATA) break;
    if (SUCCEEDED(r)) {
      if (StrLen_or_Ind == SQL_NULL_DATA) {
        D("Column[#%d]: [[null]]", ColumnNumber);
        return 0;
      }
      int n = (int)strlen(p);
      if (n == 0) {
        BufferLength += 2;
        A((size_t)BufferLength <= sizeof(buf), "");
        continue;
      }
      p += strlen(p);
      TargetValuePtr = p;
      continue;
    }
    A(0, "");
    return -1;
  }

  D("Column[#%d]: [%s]", ColumnNumber, buf);

  return 0;
}

__attribute__((unused))
static int _sql_stmt_get_long_data(SQLHANDLE stmth, SQLSMALLINT ColumnCount)
{
  int i = 1;
  for (i=1; i<=ColumnCount; ++i) {
    if (_sql_stmt_get_long_data_by_col(stmth, i)) break;
  }

  if (i <= ColumnCount) return -1;

  return 0;
}

__attribute__((unused))
static int _sql_stmt_get_data(SQLHANDLE stmth, SQLSMALLINT ColumnCount)
{
  char buf[1024];
  int i = 1;
  for (i=1; i<=ColumnCount; ++i) {
    SQLUSMALLINT   Col_or_Param_Num    = i;
    SQLSMALLINT    TargetType          = SQL_C_CHAR;
    SQLPOINTER     TargetValuePtr      = (SQLPOINTER)buf;
    SQLLEN         BufferLength        = sizeof(buf);
    SQLLEN         StrLen_or_Ind;

    buf[0] = '\0';
    SQLRETURN r = CALL_SQLGetData(stmth, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, &StrLen_or_Ind);
    if (r == SQL_NO_DATA) continue;
    if (r != SQL_SUCCESS) break;
    if (StrLen_or_Ind == SQL_NULL_DATA) {
      D("Column[#%d]: [[null]]", i);
      continue;
    }

    D("Column[#%d]: [%s]", i, buf);

    r = CALL_SQLGetData(stmth, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, &StrLen_or_Ind);
    if (FAILED(r) && r != SQL_NO_DATA) break;

    r = CALL_SQLGetData(stmth, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, &StrLen_or_Ind);
    if (FAILED(r) && r != SQL_NO_DATA) break;
  }

  if (i <= ColumnCount) return -1;

  return 0;
}

__attribute__((unused))
static int test_sql_stmt_execute_direct(SQLHANDLE stmth, const char *statement)
{
  SQLRETURN r = SQL_SUCCESS;
  SQLSMALLINT ColumnCount = 0;
  int rr = 0;

  r = CALL_SQLExecDirect(stmth, (SQLCHAR*)statement, (SQLINTEGER)strlen(statement));
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  r = CALL_SQLNumResultCols(stmth, &ColumnCount);
  if (FAILED(r)) return -1;

  r = CALL_SQLFetch(stmth);
  if (r == SQL_NO_DATA) return 0;

  rr = _sql_stmt_get_data(stmth, ColumnCount);
  if (rr == 0) {
    rr = _sql_stmt_get_data(stmth, ColumnCount);
  }

  CALL_SQLCloseCursor(stmth);

  if (rr) return -1;

  r = CALL_SQLExecDirect(stmth, (SQLCHAR*)statement, (SQLINTEGER)strlen(statement));
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  r = CALL_SQLNumResultCols(stmth, &ColumnCount);
  if (FAILED(r)) return -1;

  r = CALL_SQLFetch(stmth);
  if (r == SQL_NO_DATA) return 0;

  rr = _sql_stmt_get_long_data(stmth, ColumnCount);
  if (rr == 0) {
    if (ColumnCount > 1) {
      rr = _sql_stmt_get_long_data(stmth, ColumnCount);
    } else {
      rr = _sql_stmt_get_long_data(stmth, ColumnCount);
    }
  }

  CALL_SQLCloseCursor(stmth);

  return rr ? -1 : 0;
}

__attribute__((unused))
static int do_sql_stmt_execute_direct(SQLHANDLE stmth)
{
  CHK2(test_sql_stmt_execute_direct, stmth, "xshow databases", -1);
  CHK2(test_sql_stmt_execute_direct, stmth, "show databases", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "drop database if exists foo", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create database foo", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "use foo", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "show tables", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "drop table if exists t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create table t (ts timestamp, v int)", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select * from t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select count(*) from t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "insert into t (ts, v) values (now, 123)", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select * from t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select count(*) from t", 0);

  CHK2(test_sql_stmt_execute_direct, stmth, "drop table if exists t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "insert into t (ts, name, age, sex, text) values (1662861448752, 'name1', 20, 'male', '中国人')", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select * from t", 0);

  CHK2(test_sql_stmt_execute_direct, stmth, "drop table if exists t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "insert into t (ts, name, age, sex, text) values (1662861449753, 'name2', 30, 'female', '苏州人')", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select * from t", 0);

  CHK2(test_sql_stmt_execute_direct, stmth, "drop table if exists t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "insert into t (ts, name, age, sex, text) values (1662861450754, 'name3', null, null, null)", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select * from t", 0);

  CHK2(test_sql_stmt_execute_direct, stmth, "drop stable if exists s", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create stable s (ts timestamp, v int) tags (id int, name nchar(10))", 0);

  return 0;
}

__attribute__((unused))
static int do_sql_stmt_execute_direct_prepare(SQLHANDLE stmth)
{
  CHK2(test_sql_stmt_execute_direct, stmth, "create database if not exists foo", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "use foo", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create table if not exists t (ts timestamp, v int)", 0);

  // SQLRETURN r;
  // SQL_TIMESTAMP_STRUCT ts = {};
  // SQLLEN cbOrderDate;

  // r = CALL_SQLBindParameter(stmth, 1, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP, sizeof(ts), 0, &ts, 0, &cbOrderDate);
  // D("input: ipar: %d", 1);
  // D("input: fParamType: %d", SQL_PARAM_INPUT);
  // D("input: fCType: %d", SQL_C_TYPE_TIMESTAMP);
  // D("input: fSqlType: %d", SQL_TYPE_TIMESTAMP);
  // D("input: cbColDef: %ld", sizeof(ts));
  // D("input: ibScale: %d", 0);
  // D("input: rgbValue: %p", &ts);
  // D("input: cbValueMax: %d", 0);
  // D("input: pcbValue: %p", &cbOrderDate);
  // if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  // if (r != SQL_SUCCESS) return -1;

  return 0;
}

__attribute__((unused))
static int do_sql_stmt(SQLHANDLE connh)
{
  SQLRETURN r;
  SQLHANDLE stmth;

  r = CALL_SQLAllocHandle(SQL_HANDLE_STMT, connh, &stmth);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  do {
    r = do_sql_stmt_execute_direct(stmth);
    if (r) break;

    r = do_sql_stmt_execute_direct_prepare(stmth);
  } while (0);

  SQLFreeHandle(SQL_HANDLE_STMT, stmth);

  return r ? -1 : 0;
}

__attribute__((unused))
static int do_conn_get_info(SQLHANDLE connh)
{
  SQLRETURN  r;
  SQLSMALLINT     StringLength;

  char buf[1024];

  r = CALL_SQLGetInfo(connh, SQL_DRIVER_NAME, buf, sizeof(buf), &StringLength);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  D("SQL_DRIVER_NAME: %s", buf);

  r = CALL_SQLGetInfo(connh, SQL_DBMS_NAME, buf, sizeof(buf), &StringLength);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  D("SQL_DBMS_NAME: %s", buf);

  r = CALL_SQLGetInfo(connh, SQL_DM_VER, buf, sizeof(buf), &StringLength);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  D("SQL_DM_VER: %s", buf);

  return 0;
}

__attribute__((unused))
static int test_sql_driver_conn(SQLHANDLE connh, const char *conn_str)
{
  SQLRETURN r;
  SQLHWND WindowHandle = NULL;
  SQLCHAR *InConnectionString = (SQLCHAR*)conn_str;
  SQLSMALLINT StringLength1 = (SQLSMALLINT)strlen(conn_str);
  SQLCHAR OutConnectionString[1024];
  SQLSMALLINT BufferLength = sizeof(OutConnectionString);
  SQLSMALLINT StringLength2 = 0;
  SQLUSMALLINT DriverCompletion = SQL_DRIVER_NOPROMPT;

  OutConnectionString[0] = '\0';

  r = CALL_SQLDriverConnect(connh, WindowHandle, InConnectionString, StringLength1, OutConnectionString, BufferLength, &StringLength2, DriverCompletion);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  do {
// #define LOOPING
#ifdef LOOPING        /* { */
again:
#endif                /* } */
    r = do_conn_get_info(connh);
#ifdef LOOPING        /* { */
    if (r == 0) goto again;
#endif                /* } */

    if (r) break;

// #define LOOPING
#ifdef LOOPING        /* { */
again:
#endif                /* } */
    r = do_sql_stmt(connh);
#ifdef LOOPING        /* { */
    if (r == 0) goto again;
#endif                /* } */
  } while (0);

  SQLDisconnect(connh);

  return r ? -1 : 0;
}

__attribute__((unused))
static int test_sql_conn(SQLHANDLE connh, const char *dsn, const char *uid, const char *pwd)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLConnect(connh, (SQLCHAR*)dsn, SQL_NTS, (SQLCHAR*)uid, SQL_NTS, (SQLCHAR*)pwd, SQL_NTS);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;

  do {
// #define LOOPING
#ifdef LOOPING        /* { */
again:
#endif                /* } */
    r = do_conn_get_info(connh);
#ifdef LOOPING        /* { */
    if (r == 0) goto again;
#endif                /* } */

    if (r) break;

// #define LOOPING
#ifdef LOOPING        /* { */
again:
#endif                /* } */
    r = do_sql_stmt(connh);
#ifdef LOOPING        /* { */
    if (r == 0) goto again;
#endif                /* } */
  } while (0);

  CALL_SQLDisconnect(connh);

  return r ? -1 : 0;
}

__attribute__((unused))
static int do_sql_driver_conns(SQLHANDLE connh)
{
#ifndef FAKE_TAOS
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", NULL, NULL, 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "root", "taosdata", 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "root", NULL, 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", NULL, "taosdata", 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "root", "", -1);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "", "taosdata", -1);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "", "", -1);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "", NULL, -1);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", NULL, "", -1);
  CHK2(test_sql_driver_conn, connh, "bad", -1);
  CHK2(test_sql_driver_conn, connh, "DSN=NOT_EXIST", -1);
#ifndef _WIN32                     /* { */
  // NOTE: since TDengine 3.1.x.x, taosd is not included in so-called TDengine OSS package on Windows Platform
  //       no free lunch, haha
  CHK2(test_sql_driver_conn, connh, "Driver={TAOS_ODBC_DRIVER};Server=" SERVER_FOR_TEST "", 0);
#endif                             /* } */
  CHK2(test_sql_driver_conn, connh, "DSN=TAOS_ODBC_DSN", 0);
  CHK2(test_sql_driver_conn, connh, "Driver={TAOS_ODBC_DRIVER};DB=what", -1);
#ifndef _WIN32                     /* { */
  // NOTE: since TDengine 3.1.x.x, taosd is not included in so-called TDengine OSS package on Windows Platform
  //       no free lunch, haha
  CHK2(test_sql_driver_conn, connh, "DSN=TAOS_ODBC_DSN;Server=" SERVER_FOR_TEST "", 0);
#endif                             /* } */
  CHK2(test_sql_driver_conn, connh, "DSN=TAOS_ODBC_DSN;Server=127.0.0.1:6666", -1);
#endif

#ifdef HAVE_TAOSWS                /* { */
  CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", NULL, NULL, 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", "root", "taosdata", 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", "root", NULL, 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", NULL, "taosdata", 0);
  // CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", "root", "", -1);
  // CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", "", "taosdata", -1);
  // CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", "", "", -1);
  // CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", "", NULL, -1);
  // CHK4(test_sql_conn, connh, "TAOS_ODBC_WS_DSN", NULL, "", -1);
  CHK2(test_sql_driver_conn, connh, "bad", -1);
  CHK2(test_sql_driver_conn, connh, "DSN=NOT_EXIST", -1);
  CHK2(test_sql_driver_conn, connh, "Driver={TAOS_ODBC_DRIVER};URL={http://www.examples.com};Server=" WS_FOR_TEST "", 0);
  CHK2(test_sql_driver_conn, connh, "DSN=TAOS_ODBC_WS_DSN", 0);
  CHK2(test_sql_driver_conn, connh, "Driver={TAOS_ODBC_DRIVER};URL={http://" WS_FOR_TEST "};DB=what", -1);
  CHK2(test_sql_driver_conn, connh, "DSN=TAOS_ODBC_WS_DSN;URL={http://www.examples.com};Server=" WS_FOR_TEST "", 0);
  
  // Note that the ws_connect_with_dsn interface is blocking
  // CHK2(test_sql_driver_conn, connh, "DSN=TAOS_ODBC_WS_DSN;URL={http://www.examples.com};Server=127.0.0.1:6666", -1);
#endif                            /* } */

  return 0;
}

__attribute__((unused))
static int do_sql_alloc_conn(SQLHANDLE envh)
{
  SQLRETURN r;
  SQLHANDLE connh;

  r = CALL_SQLAllocHandle(SQL_HANDLE_DBC, envh, &connh);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

// #define LOOPING
#ifdef LOOPING        /* { */
again:
#endif                /* } */
  r = do_sql_driver_conns(connh);
#ifdef LOOPING        /* { */
  if (r == 0) goto again;
#endif                /* } */

  SQLFreeHandle(SQL_HANDLE_DBC, connh);
  return r ? -1 : 0;
}

__attribute__((unused))
static int test_sql_alloc_env(void)
{
  SQLRETURN r;
  SQLHANDLE envh;

  r = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &envh);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
    D("failure in SQLAllocHandle(SQL_HANDLE_ENV)");
    return -1;
  }

  r = CALL_SQLSetEnvAttr(envh, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

// #define LOOPING
#ifdef LOOPING        /* { */
again:
#endif                /* } */
  r = do_sql_alloc_conn(envh);
#ifdef LOOPING        /* { */
  if (r==0) goto again;
#endif                /* } */

  SQLFreeHandle(SQL_HANDLE_ENV, envh);

  return r ? -1 : 0;
}

static int do_cases(void)
{
  CHK0(test_ok, 0);
  CHK0(test_failure, -1);
#ifdef __APPLE__
  CHK1(test_so, "/tmp/not_exists.dylib", -1);
  CHK1(test_so, "libtaos_odbc.dylib", 0);
#elif defined(_WIN32)
  CHK1(test_so, "taos_odbc.dll", -1);
#ifdef TODBC_X86
  CHK1(test_so, "C:/Program Files (x86)/taos_odbc/bin/taos_odbc.dll", 0);
#else
  CHK1(test_so, "C:/Program Files/taos_odbc/bin/taos_odbc.dll", 0);
#endif
  CHK1(test_so, "taos_odbc.dll", -1);
#else
  CHK1(test_so, "/tmp/not_exists.so", -1);
  CHK1(test_so, "libtaos_odbc.so", 0);
#endif
  CHK0(test_sql_alloc_env, 0);

  return 0;
}

int main(void)
{
  int r = 0;

// #define CHK_LEAK
#ifdef CHK_LEAK                /* { */
again:
#endif                          /* } */
  r = do_cases();
#ifdef CHK_LEAK                /* { */
  if (r == 0) goto again;
#endif                          /* } */

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");

  return !!r;
}

