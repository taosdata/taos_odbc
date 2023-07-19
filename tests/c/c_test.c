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

#include <errno.h>
#include <stdarg.h>
#include <stdint.h>
#ifdef _WIN32       /* { */
#include <process.h>
#else               /* }{ */
#include <unistd.h>
#endif              /* } */


#define DUMP(fmt, ...)             fprintf(stderr, fmt "\n", ##__VA_ARGS__)
#define DCASE(fmt, ...)            fprintf(stderr, "@%d:%s():@%d:%s():" fmt "\n", __LINE__, __func__, line, func, ##__VA_ARGS__)

typedef struct handles_s                 handles_t;
struct handles_s {
  SQLHANDLE henv;
  SQLHANDLE hdbc;
  SQLHANDLE hstmt;

  uint8_t              connected:1;
};

static void handles_free_stmt(handles_t *handles)
{
  if (!handles) return;
  if (handles->hstmt == SQL_NULL_HANDLE) return;

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, handles->hstmt);
  handles->hstmt = SQL_NULL_HANDLE;
}

static void handles_disconnect(handles_t *handles)
{
  if (!handles) return;

  if (handles->hdbc == SQL_NULL_HANDLE) return;

  if (!handles->connected) return;

  handles_free_stmt(handles);

  CALL_SQLDisconnect(handles->hdbc);
  handles->connected = 0;
}

static void handles_free_conn(handles_t *handles)
{
  if (!handles) return;
  if (handles->hdbc == SQL_NULL_HANDLE) return;

  handles_disconnect(handles);

  CALL_SQLFreeHandle(SQL_HANDLE_DBC, handles->hdbc);
  handles->hdbc = SQL_NULL_HANDLE;
}

static void handles_release(handles_t *handles)
{
  if (!handles) return;

  handles_free_conn(handles);

  if (handles->henv != SQL_NULL_HANDLE) {
    CALL_SQLFreeHandle(SQL_HANDLE_ENV, handles->henv);
    handles->henv = SQL_NULL_HANDLE;
  }
}

static int _driver_connect(SQLHANDLE hdbc, const char *connstr)
{
  SQLRETURN sr = SQL_SUCCESS;

  char buf[1024];
  buf[0] = '\0';
  SQLSMALLINT StringLength = 0;
  sr = CALL_SQLDriverConnect(hdbc, NULL, (SQLCHAR*)connstr, SQL_NTS, (SQLCHAR*)buf, sizeof(buf), &StringLength, SQL_DRIVER_NOPROMPT);
  if (FAILED(sr)) return -1;

  return 0;
}

static int handles_init(handles_t *handles, const char *connstr)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  do {
    if (handles->henv == SQL_NULL_HANDLE) {
      sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &handles->henv);
      if (FAILED(sr)) break;

      sr = CALL_SQLSetEnvAttr(handles->henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
      if (FAILED(sr)) break;

      if (0) sr = CALL_SQLSetEnvAttr(handles->henv, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_DRIVER, 0);
      if (FAILED(sr)) break;
    }

    if (handles->hdbc == SQL_NULL_HANDLE) {
      sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, handles->henv, &handles->hdbc);
      if (FAILED(sr)) break;
    }

    if (!handles->connected) {
      r = _driver_connect(handles->hdbc, connstr);
      if (r) break;

      handles->connected = 1;
    }

    if (handles->hstmt == SQL_NULL_HANDLE) {
      sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, handles->hdbc, &handles->hstmt);
      if (FAILED(sr)) break;
    }

    return 0;
  } while (0);

  handles_release(handles);

  return -1;
}

static int _dump_result_sets(handles_t *handles)
{
  SQLRETURN sr = SQL_SUCCESS;
  int i = 0;
  do {
    DUMP("MoreResults[%d] ...", ++i);
    SQLLEN RowCount = 0;
    sr = CALL_SQLRowCount(handles->hstmt, &RowCount);
    if (FAILED(sr)) return -1;
    DUMP("Rows affected:%" PRId64 "", (int64_t)RowCount);

    SQLSMALLINT ColumnCount = 0;
    sr = CALL_SQLNumResultCols(handles->hstmt, &ColumnCount);
    if (FAILED(sr)) return -1;
    DUMP("ColumnCount:%d", ColumnCount);

    size_t j = 0;
    do {
      if (ColumnCount == 0) break;
      sr = CALL_SQLFetch(handles->hstmt);
      if (sr==SQL_NO_DATA) break;
      if (FAILED(sr)) return -1;
      ++j;
      for (int i=0; i<ColumnCount; ++i) {
        char colName[1024]; colName[0] = '\0';
        SQLSMALLINT NameLength      = 0;
        SQLSMALLINT DataType        = 0;
        SQLULEN     ColumnSize      = 0;
        SQLSMALLINT DecimalDigits   = 0;
        SQLSMALLINT Nullable        = 0;
        sr = CALL_SQLDescribeCol(handles->hstmt, (SQLUSMALLINT)i+1, (SQLCHAR*)colName, sizeof(colName), &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
        if (FAILED(sr)) return -1;

        char buf[4096]; buf[0] = '\0';
        SQLLEN len = 0;
        sr = CALL_SQLGetData(handles->hstmt, (SQLUSMALLINT)i+1, SQL_C_CHAR, buf, sizeof(buf), &len);
        if (FAILED(sr)) return -1;
        if (len == SQL_NULL_DATA) {
          DUMP("RowCol[%zd,%d]:%s=null", j, i+1, colName);
          continue;
        }
        DUMP("RowCol[%zd,%d]:%s=[%s]", j, i+1, colName, buf);
      }
    } while (1);

    sr = CALL_SQLMoreResults(handles->hstmt);
    if (sr == SQL_NO_DATA) break;
    if (FAILED(sr)) return -1;
  } while (1);

  return 0;
}

static int _execute_batches_of_statements(handles_t *handles, const char *sqls)
{
  SQLRETURN sr = SQL_SUCCESS;

  SQLUINTEGER bs_support = 0;

  sr = CALL_SQLGetInfo(handles->hdbc, SQL_BATCH_SUPPORT, &bs_support, sizeof(bs_support), NULL);
  if (FAILED(sr)) return -1;
  if (sr != SQL_SUCCESS) return -1;
  DUMP("bs_support:0x%x", bs_support);

  sr = CALL_SQLExecDirect(handles->hstmt, (SQLCHAR*)sqls, SQL_NTS);
  if (FAILED(sr)) return -1;

  return _dump_result_sets(handles);
}

static int execute_batches_of_statements(const char *connstr, const char *sqls)
{
  int r = 0;
  handles_t handles = {0};
  r = handles_init(&handles, connstr);
  if (r) return -1;

  if (sqls) r = _execute_batches_of_statements(&handles, sqls);

  handles_disconnect(&handles);

  return r ? -1 : 0;
}

static int _execute_with_params(handles_t *handles, const char *sql, size_t nr_params, va_list ap)
{
  SQLRETURN sr = SQL_SUCCESS;

  for (size_t i=0; i<nr_params; ++i) {
    const char *val = va_arg(ap, const char*);
    size_t len = strlen(val);
    sr = CALL_SQLBindParameter(handles->hstmt, (SQLUSMALLINT)i+1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, len, 0, (SQLPOINTER)val, (SQLLEN)len, NULL);
    if (FAILED(sr)) return -1;
  }

  sr = CALL_SQLPrepare(handles->hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (FAILED(sr)) return -1;

  sr = CALL_SQLExecute(handles->hstmt);
  if (FAILED(sr)) return -1;

  return _dump_result_sets(handles);
}

static int execute_with_params(const char *connstr, const char *sql, size_t nr_params, ...)
{
  int r = 0;
  handles_t handles = {0};
  r = handles_init(&handles, connstr);
  if (r) return -1;

  va_list ap;
  va_start(ap, nr_params);
  r = _execute_with_params(&handles, sql, nr_params, ap);
  va_end(ap);

  handles_disconnect(&handles);

  return r ? -1 : 0;
}

static int _execute_with_int(handles_t *handles, const char *sql, int v, size_t len)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLBindParameter(handles->hstmt, (SQLUSMALLINT)1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_VARCHAR, len, 0, (SQLPOINTER)&v, (SQLLEN)sizeof(v), NULL);
  if (FAILED(sr)) return -1;

  sr = CALL_SQLPrepare(handles->hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (FAILED(sr)) return -1;

  sr = CALL_SQLExecute(handles->hstmt);
  if (FAILED(sr)) return -1;

  return _dump_result_sets(handles);
}

static int execute_with_int(const char *connstr, const char *sql, int v, size_t len)
{
  int r = 0;
  handles_t handles = {0};
  r = handles_init(&handles, connstr);
  if (r) return -1;

  r = _execute_with_int(&handles, sql, v, len);

  handles_disconnect(&handles);

  return r ? -1 : 0;
}

static int test_case0(handles_t *handles, const char *conn_str, int ws)
{
  (void)handles;
  (void)ws;

  int r = 0;
  const char *connstr = NULL;
  const char *sqls = NULL;
  const char *sql = NULL;
  char tmbuf[128]; tmbuf[0] = '\0';

#ifdef _WIN32              /* { */
  connstr = "DSN=SQLSERVER_ODBC_DSN";
  sqls = "drop table if exists t;"
         "create table t(name varchar(7), mark nchar(20), i16 smallint);"
         "insert into t (name, mark) values ('测试', '人物');"
         "insert into t (name, mark) values ('测试', '人物a');"
         "insert into t (name, mark) values ('测试', '人物x');"
         "select * from t;";
  r = execute_batches_of_statements(connstr, sqls);
  if (r) return -1;
  if (!ws) {
    sql = "insert into t (name, mark) values (?, ?)";
    r = execute_with_params(connstr, sql, 2, "测试x", "人物y");
    if (r) return -1;
    sqls = "select * from t;";
    r = execute_batches_of_statements(connstr, sqls);
    if (r) return -1;
    sql = "select * from t where name like ?";
    r = execute_with_params(connstr, sql, 1, "测试");
    if (r) return -1;
    sql = "insert into t (i16) values (?)";
    r = execute_with_int(connstr, sql, 32767, 5);
    if (r) return -1;
  }
#endif                     /* } */
  connstr = conn_str;
  sqls = "drop database if exists bar;"
         "create database if not exists bar;"
         "use bar;"
         "drop table if exists t;"
         "create table t(ts timestamp, name varchar(20), mark nchar(20));"
         "insert into t (ts, name, mark) values (now(), '测试', '人物');"
         "insert into t (ts, name, mark) values (now(), '测试', '人物m');"
         "insert into t (ts, name, mark) values (now(), '测试', '人物n');"
         "select * from t;";
  r = execute_batches_of_statements(connstr, sqls);
  if (r) return -1;
  if (!ws) {
    sql = "insert into bar.t (ts, name, mark) values (?, ?, ?)";
    r = execute_with_params(connstr, sql, 3, "2023-05-07 10:11:44.215", "测试", "人物y");
    if (r) return -1;
    tod_get_format_current_local_timestamp_ms(tmbuf, sizeof(tmbuf));
    DUMP("current:%s", tmbuf);
    r = execute_with_params(connstr, sql, 3, tmbuf, "测试", "人物z");
    if (r) return -1;
    sqls = "select * from bar.t;";
    r = execute_batches_of_statements(connstr, sqls);
    if (r) return -1;
    sql = "select * from bar.t where name like ?";
    r = execute_with_params(connstr, sql, 1, "测试");
    if (r) return -1;
    sql = "insert into t (i16) values (?)";
    if (0) r = execute_with_int(connstr, sql, 32767, 5);
    if (r) return -1;
  }

  return 0;
}

static int _check_with_values_ap(int line, const char *func, handles_t *handles, const char *sql, size_t nr_rows, size_t nr_cols, va_list ap)
{
  SQLRETURN sr = SQL_SUCCESS;

  CALL_SQLCloseCursor(handles->hstmt);
  CALL_SQLFreeStmt(handles->hstmt, SQL_UNBIND);
  CALL_SQLFreeStmt(handles->hstmt, SQL_RESET_PARAMS);

  sr = CALL_SQLExecDirect(handles->hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (FAILED(sr)) return -1;

  SQLSMALLINT ColumnCount = 0;
  sr = CALL_SQLNumResultCols(handles->hstmt, &ColumnCount);
  if (FAILED(sr)) return -1;
  if ((size_t)ColumnCount != nr_cols) {
    DCASE("expected %zd columns, but got ==%d==", nr_cols, ColumnCount);
    return -1;
  }

  for (size_t i_row = 0; i_row < nr_rows; ++i_row) {
    sr = CALL_SQLFetch(handles->hstmt);
    if (sr==SQL_NO_DATA) {
      DCASE("expected %zd rows, but got ==%zd==", nr_rows, i_row);
      return -1;
    }
    if (FAILED(sr)) return -1;
    for (size_t i_col=0; i_col<(size_t)ColumnCount; ++i_col) {
      char colName[1024]; colName[0] = '\0';
      SQLSMALLINT NameLength      = 0;
      SQLSMALLINT DataType        = 0;
      SQLULEN     ColumnSize      = 0;
      SQLSMALLINT DecimalDigits   = 0;
      SQLSMALLINT Nullable        = 0;
      sr = CALL_SQLDescribeCol(handles->hstmt, (SQLUSMALLINT)i_col+1, (SQLCHAR*)colName, sizeof(colName), &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
      if (FAILED(sr)) return -1;

      char buf[4096]; buf[0] = '\0';
      SQLLEN len = 0;
      sr = CALL_SQLGetData(handles->hstmt, (SQLUSMALLINT)i_col+1, SQL_C_CHAR, buf, sizeof(buf), &len);
      if (FAILED(sr)) return -1;
      const char *v = va_arg(ap, const char*);
      if (len == SQL_NULL_DATA) {
        if (v) {
          DCASE("[%zd,%zd]:expected [%s], but got ==null==", i_row+1, i_col+1, v);
          return -1;
        }
        continue;
      }
      if (len == SQL_NTS) len = (SQLLEN)strlen(buf);
      if (strlen(v) != (size_t)len || strncmp(buf, v, (size_t)len)) {
        DCASE("[%zd,%zd]:expected [%s], but got ==%.*s==", i_row+1, i_col+1, v, (int)len, buf);
        return -1;
      }
    }
  }

  sr = CALL_SQLFetch(handles->hstmt);
  if (sr == SQL_NO_DATA) return 0;
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) {
    return -1;
  }

  DCASE("expected %zd rows, but got ==more rows==", nr_rows);
  return -1;
}

static int _check_col_bind_with_values_ap(int line, const char *func, handles_t *handles, const char *sql, size_t nr_rows, size_t nr_cols, va_list ap)
{
  SQLRETURN sr = SQL_SUCCESS;

  CALL_SQLCloseCursor(handles->hstmt);
  CALL_SQLFreeStmt(handles->hstmt, SQL_UNBIND);
  CALL_SQLFreeStmt(handles->hstmt, SQL_RESET_PARAMS);

  char bufs[10][1024];
  SQLLEN lens[10];

  size_t nr_bufs = sizeof(bufs)/sizeof(bufs[0]);
  if (nr_bufs < nr_cols) {
    DCASE("columns %zd more than %zd:not supported yet", nr_cols, nr_bufs);
    return -1;
  }

  for (size_t i=0; i<nr_cols; ++i) {
    bufs[i][0] = '\0';
    lens[0] = 0;

    sr = CALL_SQLBindCol(handles->hstmt, (SQLUSMALLINT)i+1, SQL_C_CHAR, bufs[i], sizeof(bufs[i]), lens + i);
    if (sr != SQL_SUCCESS) return -1;
  }

  sr = CALL_SQLExecDirect(handles->hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (FAILED(sr)) return -1;

  SQLSMALLINT ColumnCount = 0;
  sr = CALL_SQLNumResultCols(handles->hstmt, &ColumnCount);
  if (FAILED(sr)) return -1;
  if ((size_t)ColumnCount != nr_cols) {
    DCASE("expected %zd columns, but got ==%d==", nr_cols, ColumnCount);
    return -1;
  }

  for (size_t i_row = 0; i_row < nr_rows; ++i_row) {
    sr = CALL_SQLFetch(handles->hstmt);
    if (sr==SQL_NO_DATA) {
      DCASE("expected %zd rows, but got ==%zd==", nr_rows, i_row);
      return -1;
    }
    if (FAILED(sr)) return -1;
    for (size_t i_col=0; i_col<(size_t)ColumnCount; ++i_col) {
      char colName[1024]; colName[0] = '\0';
      SQLSMALLINT NameLength      = 0;
      SQLSMALLINT DataType        = 0;
      SQLULEN     ColumnSize      = 0;
      SQLSMALLINT DecimalDigits   = 0;
      SQLSMALLINT Nullable        = 0;
      sr = CALL_SQLDescribeCol(handles->hstmt, (SQLUSMALLINT)i_col+1, (SQLCHAR*)colName, sizeof(colName), &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
      if (FAILED(sr)) return -1;

      const char *v = va_arg(ap, const char*);
      if (lens[i_col] == SQL_NULL_DATA) {
        if (v) {
          DCASE("[%zd,%zd]:expected [%s], but got ==null==", i_row+1, i_col+1, v);
          return -1;
        }
        continue;
      }
      if (lens[i_col] == SQL_NTS) lens[i_col] = (SQLLEN)strlen(bufs[i_col]);
      if (strlen(v) != (size_t)lens[i_col] || strncmp(bufs[i_col], v, (size_t)lens[i_col])) {
        DCASE("[%zd,%zd]:expected [%s], but got ==%.*s==", i_row+1, i_col+1, v, (int)lens[i_col], bufs[i_col]);
        return -1;
      }
    }
  }

  sr = CALL_SQLFetch(handles->hstmt);
  if (sr == SQL_NO_DATA) return 0;
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) {
    return -1;
  }

  DCASE("expected %zd rows, but got ==more rows==", nr_rows);
  return -1;
}

static int _check_with_values(int line, const char *func, handles_t *handles, int with_col_bind, const char *sql, size_t nr_rows, size_t nr_cols, ...)
{
  int r = 0;

  va_list ap;
  va_start(ap, nr_cols);
  if (!with_col_bind) {
    r = _check_with_values_ap(line, func, handles, sql, nr_rows, nr_cols, ap);
  } else {
    r = _check_col_bind_with_values_ap(line, func, handles, sql, nr_rows, nr_cols, ap);
  }
  va_end(ap);

  return r ? -1 : 0;
}

#define CHECK_WITH_VALUES(...)       _check_with_values(__LINE__, __func__, ##__VA_ARGS__)

static int test_charsets(handles_t *handles, const char *connstr, int ws)
{
  (void)ws;

  int r = 0;

  const char *conn_str = NULL;
  const char *sqls = NULL;
  const char *sql = NULL;

  handles_disconnect(handles);

  conn_str = connstr;
  r = handles_init(handles, conn_str);

  sqls =
    "drop database if exists foo;"
    "create database if not exists foo;"
    "create table foo.t (ts timestamp, name varchar(20), mark nchar(20));"
    "insert into foo.t (ts, name, mark) values (now(), 'name', 'mark');"
    "insert into foo.t (ts, name, mark) values (now()+1s, '测试', '检验');";
  r = _execute_batches_of_statements(handles, sqls);
  if (r) return -1;

  sql = "select name from foo.t where name='name'";
  r = CHECK_WITH_VALUES(handles, 0, sql, 1, 1, "name");
  if (r) return -1;

  sql = "select mark from foo.t where mark='mark'";
  r = CHECK_WITH_VALUES(handles, 0, sql, 1, 1, "mark");
  if (r) return -1;

  sql = "select name from foo.t where name='测试'";
  r = CHECK_WITH_VALUES(handles, 0, sql, 1, 1, "测试");
  if (r) return -1;

  sql = "select mark from foo.t where mark='检验'";
  r = CHECK_WITH_VALUES(handles, 0, sql, 1, 1, "检验");
  if (r) return -1;

  return 0;
}

static int test_charsets_with_col_bind(handles_t *handles, const char *connstr, int ws)
{
  (void)ws;

  int r = 0;

  const char *conn_str = NULL;
  const char *sqls = NULL;
  const char *sql = NULL;

  handles_disconnect(handles);

  conn_str = connstr;
  r = handles_init(handles, conn_str);

  sqls =
    "drop database if exists foo;"
    "create database if not exists foo;"
    "create table foo.t (ts timestamp, name varchar(20), mark nchar(20));"
    "insert into foo.t (ts, name, mark) values (now(), 'name', 'mark');"
    "insert into foo.t (ts, name, mark) values (now()+1s, '测试', '检验');";
  r = _execute_batches_of_statements(handles, sqls);
  if (r) return -1;

  sql = "select name from foo.t where name='name'";
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, "name");
  if (r) return -1;

  sql = "select mark from foo.t where mark='mark'";
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, "mark");
  if (r) return -1;

  sql = "select name from foo.t where name='测试'";
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, "测试");
  if (r) return -1;

  sql = "select mark from foo.t where mark='检验'";
  r = CHECK_WITH_VALUES(handles, 1, sql, 1, 1, "检验");
  if (r) return -1;

  return 0;
}

static int _insert_with_values_ap(int line, const char *func, handles_t *handles, const char *sql, size_t nr_rows, size_t nr_cols, va_list ap)
{
  SQLRETURN sr = SQL_SUCCESS;

#define COLS         20
#define ROWS         20
  char values[COLS][ROWS][1024] = {0};
  SQLLEN lens[COLS][ROWS] = {0};
  SQLULEN ColumnSizes[COLS] = {0};
  SQLUSMALLINT ParamStatusArray[ROWS] = {0};
  SQLULEN ParamsProcessed = 0;
#undef ROWS
#undef COLS

  size_t cols = sizeof(values) / sizeof(values[0]);
  size_t rows = sizeof(values[0]) / sizeof(values[0][0]);
  if (nr_rows > rows) {
    DCASE("rows %zd more than %zd:not supported yet", nr_rows, rows);
    return -1;
  }
  if (nr_cols > cols) {
    DCASE("columns %zd more than %zd:not supported yet", nr_cols, cols);
    return -1;
  }

  // Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
  // column-wise binding.
  sr = CALL_SQLSetStmtAttr(handles->hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);
  if (sr != SQL_SUCCESS) return -1;

  // Specify the number of elements in each parameter array.
  sr = CALL_SQLSetStmtAttr(handles->hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)nr_rows, 0);
  if (sr != SQL_SUCCESS) return -1;

  // Specify an array in which to return the status of each set of
  // parameters.
  if (0) sr = CALL_SQLSetStmtAttr(handles->hstmt, SQL_ATTR_PARAM_STATUS_PTR, ParamStatusArray, 0);
  if (sr != SQL_SUCCESS) return -1;

  // Specify an SQLUINTEGER value in which to return the number of sets of
  // parameters processed.
  if (0) sr = CALL_SQLSetStmtAttr(handles->hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &ParamsProcessed, 0);
  if (sr != SQL_SUCCESS) return -1;

  for (size_t j=0; j<nr_cols; ++j) {
    for (size_t i=0; i<nr_rows; ++i) {
      const char *v = va_arg(ap, const char*);
      int n = snprintf(values[j][i], sizeof(values[j][i]), "%s", v);
      if (n < 0 || (size_t)n >= sizeof(values[j][i])) {
        DCASE("internal logic error:value[%zd,%zd]:`%s` too big", i+1, j+1, v);
        return -1;
      }
      lens[j][i] = n;
      if ((SQLULEN)n > ColumnSizes[j]) ColumnSizes[j] = n;
    }
    sr = CALL_SQLBindParameter(handles->hstmt, (SQLUSMALLINT)j+1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
      ColumnSizes[j], 0, values[j], sizeof(values[j][0])/sizeof(values[j][0][0]), lens[j]);
    if (sr != SQL_SUCCESS) return -1;
  }

  sr = CALL_SQLExecDirect(handles->hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  return 0;
}

static int _insert_with_values(int line, const char *func, handles_t *handles, const char *sql, size_t nr_rows, size_t nr_cols, ...)
{
  int r = 0;

  va_list ap;
  va_start(ap, nr_cols);
  r = _insert_with_values_ap(line, func, handles, sql, nr_rows, nr_cols, ap);
  va_end(ap);

  return r ? -1 : 0;
}

#define INSERT_WITH_VALUES(...)       _insert_with_values(__LINE__, __func__, ##__VA_ARGS__)

static int test_charsets_with_param_bind(handles_t *handles, const char *connstr, int ws)
{
  (void)ws;

  int r = 0;

  const char *conn_str = NULL;
  const char *sqls = NULL;
  const char *sql = NULL;

  handles_disconnect(handles);

  conn_str = connstr;
  r = handles_init(handles, conn_str);

  sqls =
    "drop database if exists foo;"
    "create database if not exists foo;"
    "create table foo.t (ts timestamp, name varchar(20), mark nchar(20));";
  r = _execute_batches_of_statements(handles, sqls);
  if (r) return -1;

  if (!ws) {
    sql = "insert into foo.t (ts, name, mark) values (?, ?, ?)";
    r = INSERT_WITH_VALUES(handles, sql, 2, 3, "2023-05-14 12:13:14.567", "2023-05-14 12:13:15.678", "name", "测试", "mark", "检验");
    if (r) return -1;

    sql = "select name from foo.t where name='name'";
    r = CHECK_WITH_VALUES(handles, 0, sql, 1, 1, "name");
    if (r) return -1;

    sql = "select mark from foo.t where mark='mark'";
    r = CHECK_WITH_VALUES(handles, 0, sql, 1, 1, "mark");
    if (r) return -1;

    sql = "select name from foo.t where name='测试'";
    r = CHECK_WITH_VALUES(handles, 0, sql, 1, 1, "测试");
    if (r) return -1;

    sql = "select mark from foo.t where mark='检验'";
    r = CHECK_WITH_VALUES(handles, 0, sql, 1, 1, "检验");
    if (r) return -1;
  }

  return 0;
}

static int _remove_topics(handles_t *handles, const char *db)
{
  SQLRETURN sr = SQL_SUCCESS;

  const char *sql = "select topic_name from information_schema.ins_topics where db_name = ?";
  char topic_name_buf[1024]; topic_name_buf[0] = '\0';
  char drop_buf[2048]; drop_buf[0] = '\0';

again:

  if (0) sr = CALL_SQLPrepare(handles->hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  sr = CALL_SQLBindParameter(handles->hstmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, strlen(db), 0, (SQLPOINTER)db, (SQLLEN)strlen(db), NULL);
  if (sr != SQL_SUCCESS) return -1;

  SQLLEN topic_name_len = 0;

  sr = CALL_SQLBindCol(handles->hstmt, 1, SQL_C_CHAR, topic_name_buf, sizeof(topic_name_buf), &topic_name_len);
  if (sr != SQL_SUCCESS) return -1;

  if (0) sr = CALL_SQLExecute(handles->hstmt);
  else   sr = CALL_SQLExecDirect(handles->hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  sr = CALL_SQLFetch(handles->hstmt);
  if (sr == SQL_NO_DATA) {
    CALL_SQLCloseCursor(handles->hstmt);
    CALL_SQLFreeStmt(handles->hstmt, SQL_UNBIND);
    CALL_SQLFreeStmt(handles->hstmt, SQL_RESET_PARAMS);
    return 0;
  }
  if (sr != SQL_SUCCESS) return -1;
  // FIXME: vulnerability
  snprintf(drop_buf, sizeof(drop_buf), "drop topic `%s`", topic_name_buf);
  CALL_SQLCloseCursor(handles->hstmt);
  CALL_SQLFreeStmt(handles->hstmt, SQL_UNBIND);
  CALL_SQLFreeStmt(handles->hstmt, SQL_RESET_PARAMS);
  
  sr = CALL_SQLExecDirect(handles->hstmt, (SQLCHAR*)drop_buf, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  goto again;
}


typedef struct test_topic_s      test_topic_t;
struct test_topic_s {
  const char *conn_str;
  const char *(*_cases)[2];
  size_t _nr_cases;
  const char *(*_expects)[2];
  volatile int8_t            flag; // 0: wait; 1: start; 2: stop
};

static void _test_topic_routine_impl(test_topic_t *ds)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;
  handles_t handles = {0};
  r = handles_init(&handles, ds->conn_str);
  if (r) return;

  while (ds->flag == 0) {
#ifdef _WIN32            /* { */
    Sleep(0);
#else                    /* }{ */
    sleep(0);
#endif                   /* } */
  }

  for (size_t i=0; i<ds->_nr_cases; ++i) {
    char sql[4096]; sql[0] = '\0';
    snprintf(sql, sizeof(sql), "insert into foobar.demo (ts, name, mark) values (now()+%zds, '%s', '%s')", i, ds->_cases[i][0], ds->_cases[i][1]);
    sr = CALL_SQLExecDirect(handles.hstmt, (SQLCHAR*)sql, SQL_NTS);
    CALL_SQLCloseCursor(handles.hstmt);
    if (sr != SQL_SUCCESS) {
      r = -1;
      break;
    }
  }

  handles_disconnect(&handles);
}

#ifdef _WIN32                   /* { */
static unsigned __stdcall _test_topic_routine(void *arg)
{
  test_topic_t *ds = (test_topic_t*)arg;
  _test_topic_routine_impl(ds);
  return 0;
}
#else                           /* }{ */
static void* _test_topic_routine(void *arg)
{
  test_topic_t *ds = (test_topic_t*)arg;
  _test_topic_routine_impl(ds);
  return NULL;
}
#endif                          /* } */

static int _test_topic_monitor(handles_t *handles, test_topic_t *ds)
{
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = handles->hstmt;

  size_t demo_limit = 0;
  SQLSMALLINT ColumnCount;

  char name[4096], value[4096];
  char sql[1024]; sql[0] = '\0';
  char row_buf[4096]; row_buf[0] = '\0';
  char *p = NULL;
  const char *end = NULL;
  SQLSMALLINT    NameLength;
  SQLSMALLINT    DataType;
  SQLULEN        ColumnSize;
  SQLSMALLINT    DecimalDigits;
  SQLSMALLINT    Nullable;

  // syntax: !topic [name]+ [{[key[=val];]*}]?");
  // ref: https://github.com/taosdata/TDengine/blob/main/docs/en/07-develop/07-tmq.mdx#create-a-consumer
  // NOTE: although both 'enable.auto.commit' and 'auto.commit.interval.ms' are still valid,
  //       taos_odbc chooses it's owner way to call `tmq_commit_sync` under the hood.
  snprintf(sql, sizeof(sql), "!topic demo {group.id=cgrpName; taos_odbc.limit.seconds=30; taos_odbc.limit.records=%zd}", ds->_nr_cases);

  DUMP("starting topic consumer ...");
  DUMP("will stop either 30 seconds have passed or %zd records have been fetched", ds->_nr_cases);
  DUMP("press Ctrl-C to abort");
  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

describe:

  sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
  if (sr != SQL_SUCCESS) return -1;

fetch:

  if (demo_limit == ds->_nr_cases) {
    DUMP("demo_limit of #%zd has been reached", ds->_nr_cases);
    return 0;
  }

  sr = CALL_SQLFetch(hstmt);
  if (sr == SQL_NO_DATA) {
    sr = CALL_SQLMoreResults(hstmt);
    if (sr == SQL_NO_DATA) return 0;
    if (sr == SQL_SUCCESS) goto describe;
  }
  if (sr != SQL_SUCCESS) return -1;

  demo_limit++;

  row_buf[0] = '\0';
  p = row_buf;
  end = row_buf + sizeof(row_buf);
  for (int i=0; i<ColumnCount; ++i) {
    sr = CALL_SQLDescribeCol(hstmt, i+1, (SQLCHAR*)name, sizeof(name), &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
    if (sr != SQL_SUCCESS) return -1;

    SQLLEN Len_or_Ind;
    sr = CALL_SQLGetData(hstmt, i+1, SQL_C_CHAR, value, sizeof(value), &Len_or_Ind);
    if (sr != SQL_SUCCESS) return -1;

    int n = snprintf(p, end - p, "%s%s:[%s]",
        i ? ";" : "",
        name,
        Len_or_Ind == SQL_NULL_DATA ? "null" : value);
    if (n < 0 || n >= end - p) {
      E("buffer too small");
      return -1;
    }
    p += n;

    if (i<3) continue;

    const char *exp = ds->_expects[demo_limit-1][i-3];
    if (Len_or_Ind == SQL_NULL_DATA) {
      if (exp) {
        E("[%zd,%d] expected `%s`, but got ==null==", demo_limit, i+1, ds->_expects[demo_limit-1][i]);
        return -1;
      }
    } else {
      if (Len_or_Ind == SQL_NTS) Len_or_Ind = strlen(value);
      if (!exp) {
        E("[%zd,%d] expected null, but got ==`%.*s`==", demo_limit, i+1, (int)Len_or_Ind, value);
        return -1;
      }
      if ((size_t)Len_or_Ind != strlen(exp) || strncmp(value, exp, (size_t)Len_or_Ind)) {
        E("[%zd,%d] expected `%s`, but got ==`%.*s`==", demo_limit, i+1, exp, (int)Len_or_Ind, value);
        return -1;
      }
    }
  }

  DUMP("new data:%s", row_buf);

  goto fetch;
}

static int test_topic(handles_t *handles, const char *connstr, int ws)
{
  (void)ws;

  if (ws) return 0;

  int r = 0;

  const char *conn_str = NULL;
  const char *sqls = NULL;

  handles_disconnect(handles);

  conn_str = connstr;
  r = handles_init(handles, conn_str);

  r = _remove_topics(handles, "foobar");
  if (r) return -1;

  sqls =
    "drop database if exists foobar;"
    "create database if not exists foobar WAL_RETENTION_PERIOD 2592000;"
    "create table foobar.demo (ts timestamp, name varchar(20), mark nchar(20));"
    "create topic demo as select name, mark from foobar.demo;";
  r = _execute_batches_of_statements(handles, sqls);
  if (r) return -1;

  const char *rows[][2] = {
    {"name", "mark"},
    {"测试", "检验"},
  };

  const char *expects[][2] = {
    {"name", "mark"},
    {"测试", "检验"},
  };

  test_topic_t ds = {
    conn_str,
    rows,
    sizeof(rows)/sizeof(rows[0]),
    expects,
    0,
  };

#ifdef _WIN32                   /* { */
  uintptr_t worker;
  worker = _beginthreadex(NULL, 0, _test_topic_routine, &ds, 0, NULL);
#else                           /* }{ */
  pthread_t worker;
  r = pthread_create(&worker, NULL, _test_topic_routine, &ds);
#endif                          /* } */
  if (r) {
    fprintf(stderr, "@%d:%s():pthread_create failed:[%d]%s\n", __LINE__, __func__, r, strerror(r));
    return -1;
  }
  ds.flag = 1;

  r = _test_topic_monitor(handles, &ds);
  ds.flag = 2;

#ifdef _WIN32                   /* { */
  WaitForSingleObject((HANDLE)worker, INFINITE);
#else                           /* }{ */
  pthread_join(worker, NULL);
#endif                          /* } */

  return r ? -1 : 0;
}

static int test_params_with_all_chars(handles_t *handles, const char *connstr, int ws)
{
  (void)ws;

  int r = 0;

  const char *conn_str = NULL;
  const char *sqls = NULL;
  const char *sql = NULL;

  handles_disconnect(handles);

  conn_str = connstr;
  r = handles_init(handles, conn_str);

  sqls =
    "drop database if exists foo;"
    "create database if not exists foo;"
    "create table foo.t (ts timestamp, b bool, "
                        "i8 tinyint, u8 tinyint unsigned, i16 smallint, u16 smallint unsigned, i32 int, u32 int unsigned, i64 bigint, u64 bigint unsigned, "
                        "flt float, dbl double, name varchar(20), mark nchar(20));";
  r = _execute_batches_of_statements(handles, sqls);
  if (r) return -1;

  if (!ws) {
    sql = "insert into foo.t (ts, b, i8, u8, i16, u16, i32, u32, i64, u64, flt, dbl, name, mark) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    r = INSERT_WITH_VALUES(handles, sql, 1, 14, "2023-05-14 12:13:14.567", "1", "127", "255", "32767", "65535",
        "2147483647", "4294967295", "9223372036854775807", "18446744073709551615", "1.23", "2.34", "测试", "检验");
    if (r) return -1;

    sql = "select * from foo.t where name='测试'";
    r = CHECK_WITH_VALUES(handles, 0, sql, 1, 14, "2023-05-14 12:13:14.567", "true", "127", "255", "32767", "65535",
        "2147483647", "4294967295", "9223372036854775807", "18446744073709551615", "1.23", "2.34", "测试", "检验");
    if (r) return -1;
  }

  return 0;
}

static int test_json_tag(handles_t *handles, const char *connstr, int ws)
{
  (void)ws;

  int r = 0;

  const char *conn_str = NULL;
  const char *sqls = NULL;
  const char *sql = NULL;

  handles_disconnect(handles);

  conn_str = connstr;
  r = handles_init(handles, conn_str);

  sqls =
    "drop database if exists foo;"
    "create database if not exists foo;"
    "create stable foo.s1 (ts timestamp, v1 int) tags (info json);";
  r = _execute_batches_of_statements(handles, sqls);
  if (r) return -1;

  if (!ws) {
    sql = "insert into foo.s1_1 using foo.s1 tags (?) values (?, ?)";
    r = INSERT_WITH_VALUES(handles, sql, 2, 3,
      "{\"k1\":\"值1\"}", "{\"k1\":\"值1\"}",
      "2023-05-14 12:13:14.567", "2023-05-14 12:13:14.568",
      "1", "2");
    if (r) return -1;

    sql = "select * from foo.s1";
    r = CHECK_WITH_VALUES(handles, 0, sql, 2, 3,
      "2023-05-14 12:13:14.567", "1", "{\"k1\":\"值1\"}",
      "2023-05-14 12:13:14.568", "2", "{\"k1\":\"值1\"}");
    if (r) return -1;

    sql = "select info->'k1' from foo.s1";
    r = CHECK_WITH_VALUES(handles, 0, sql, 2, 1,
      "\"值1\"",
      "\"值1\"");
    if (r) return -1;

    sql = "select v1 from foo.s1 where info contains 'k1'";
    r = CHECK_WITH_VALUES(handles, 0, sql, 2, 1,
      "1",
      "2");
    if (r) return -1;

    sql = "select v1 from foo.s1 where info->'k1' match '值1'";
    r = CHECK_WITH_VALUES(handles, 0, sql, 2, 1,
      "1",
      "2");
    if (r) return -1;
  }

  return 0;
}

static int test_pool_stmt(handles_t *handles)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLExecDirect(handles->hstmt, (SQLCHAR*)"select * from information_schema.ins_tables", SQL_NTS);
  if (FAILED(sr)) return -1;

  CALL_SQLCloseCursor(handles->hstmt);
  return 0;
}

#ifdef HAVE_TAOSWS               /* { */
static int test_taosws_conn(handles_t *handles, const char *conn_str, int ws)
{
  (void)handles;
  (void)ws;

  int r = 0;
  const char *connstr = conn_str; // "DSN=TAOS_ODBC_DSN;URL={taos://127.0.0.1:6041}";
  const char *sqls = NULL;

  sqls = NULL;
  r = execute_batches_of_statements(connstr, sqls);
  if (r) return -1;

  return 0;
}
#endif                           /* }*/

static int test_pool(handles_t *handles, const char *connstr, int ws)
{
  (void)ws;

  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  const char *conn_str = NULL;

  if (0) sr = CALL_SQLSetEnvAttr(SQL_NULL_HANDLE, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_DRIVER, 0);
  if (FAILED(sr)) return -1;

  handles_disconnect(handles);

  conn_str = connstr;
  r = handles_init(handles, conn_str);
  if (r) return -1;

  r = test_pool_stmt(handles);
  if (r) return -1;

  handles_disconnect(handles);

  r = handles_init(handles, conn_str);
  if (r) return -1;

  r = test_pool_stmt(handles);
  if (r) return -1;

  handles_disconnect(handles);

  return 0;
}

typedef struct case_s              case_t;
struct case_s {
  const char               *name;
  int (*routine)(handles_t *handles, const char *conn_str, int ws);
};

static case_t* find_case(case_t *cases, size_t nr_cases, const char *name)
{
  for (size_t i=0; i<nr_cases; ++i) {
    case_t *p = cases + i;
    if (strcmp(p->name, name)==0) return p;
  }
  return NULL;
}

static int running_case(handles_t *handles, case_t *_case)
{
  int r = 0;
  r = _case->routine(handles, "DSN=TAOS_ODBC_DSN", 0);
  handles_disconnect(handles);
  if (r) return -1;
  r = _case->routine(handles, "DSN=TAOS_ODBC_WS_DSN", 1);
  handles_disconnect(handles);
  return r;
}

static void usage(const char *arg0)
{
  fprintf(stderr, "%s -h\n"
                  "  show this help page\n"
                  "%s [--pooling] [name]...\n"
                  "  running test case `name`\n"
                  "%s -l\n"
                  "  list all test cases\n",
                  arg0, arg0, arg0);
}

static void list_cases(case_t *cases, size_t nr_cases)
{
  fprintf(stderr, "supported test cases:\n");
  for (size_t i=0; i<nr_cases; ++i) {
    fprintf(stderr, "  %s\n", cases[i].name);
  }
}

static int running_with_args(int argc, char *argv[], handles_t *handles, case_t *_cases, size_t _nr_cases)
{
  int r = 0;

  size_t nr_cases = 0;

  for (int i=1; i<argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "-h")==0) {
      usage(argv[0]);
      return 0;
    }
    if (strcmp(arg, "-l")==0) {
      list_cases(_cases, _nr_cases);
      return 0;
    }
    if (strcmp(arg, "--pooling")==0) {
      SQLRETURN sr = SQL_SUCCESS;
      sr = CALL_SQLSetEnvAttr(SQL_NULL_HANDLE, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_DRIVER, 0);
      if (FAILED(sr)) return -1;
      continue;
    }
    case_t *p = find_case(_cases, _nr_cases, arg);
    if (!p) {
      DUMP("test case `%s`:not found", arg);
      return -1;
    }
    r = running_case(handles, p);
    if (r) return -1;
    ++nr_cases;
  }

  if (nr_cases == 0) {
    for (size_t i=0; i<_nr_cases; ++i) {
      r = running_case(handles, _cases + i);
      if (r) return -1;
    }
  }

  return 0;
}

int main(int argc, char *argv[])
{
  int r = 0;

#define RECORD(x) {#x, x}
  case_t _cases[] = {
    RECORD(test_case0),
    RECORD(test_charsets),
    RECORD(test_charsets_with_col_bind),
    RECORD(test_charsets_with_param_bind),
    RECORD(test_topic),
    RECORD(test_params_with_all_chars),
    RECORD(test_json_tag),
#ifdef HAVE_TAOSWS               /* { */
    RECORD(test_taosws_conn),
#endif                           /* } */
    RECORD(test_pool),  // NOTE: for the test purpose, this must keep in the last!!!
  };
#undef RECORD
  size_t _nr_cases = sizeof(_cases)/sizeof(_cases[0]);

  SQLRETURN sr = SQL_SUCCESS;
  if (0) sr = CALL_SQLSetEnvAttr(SQL_NULL_HANDLE, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_DRIVER, 0);
  if (FAILED(sr)) return -1;

  handles_t handles = {0};
  r = running_with_args(argc, argv, &handles, _cases, _nr_cases);
  handles_release(&handles);
  fprintf(stderr, "==%s==\n", r ? "failure" : "success");
  return !!r;
}

