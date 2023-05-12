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


#define DUMP(fmt, ...)             fprintf(stderr, fmt "\n", ##__VA_ARGS__)
#define DCASE(fmt, ...)            fprintf(stderr, "@%d:%s():@%d:%s():" fmt "\n", __LINE__, __func__, line, func, ##__VA_ARGS__)

typedef struct handles_s                 handles_t;
struct handles_s {
  SQLHANDLE henv;
  SQLHANDLE hdbc;
  SQLHANDLE hstmt;

  uint8_t              connected:1;
};

static void handles_release(handles_t *handles)
{
  if (!handles) return;
  if (handles->hstmt != SQL_NULL_HANDLE) {
    CALL_SQLFreeHandle(SQL_HANDLE_STMT, handles->hstmt);
    handles->hstmt = SQL_NULL_HANDLE;
  }

  if (handles->hdbc != SQL_NULL_HANDLE) {
    if (handles->connected) {
      CALL_SQLDisconnect(handles->hdbc);
      handles->connected = 0;
    }
    CALL_SQLFreeHandle(SQL_HANDLE_DBC, handles->hdbc);
    handles->hdbc = SQL_NULL_HANDLE;
  }

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
  sr = CALL_SQLDriverConnect(hdbc, NULL, (SQLCHAR*)connstr, SQL_NTS, (SQLCHAR*)buf, sizeof(buf), &StringLength, SQL_DRIVER_COMPLETE);
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

  r = _execute_batches_of_statements(&handles, sqls);

  handles_release(&handles);

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

  handles_release(&handles);

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

  handles_release(&handles);

  return r ? -1 : 0;
}

static int test_case0(handles_t *handles)
{
  (void)handles;

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
#endif                     /* } */
  connstr = "DSN=TAOS_ODBC_DSN";
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

  return 0;
}

static int _check_with_values_ap(int line, const char *func, handles_t *handles, const char *sql, size_t nr_rows, size_t nr_cols, va_list ap)
{
  SQLRETURN sr = SQL_SUCCESS;

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

static int _check_with_values(int line, const char *func, handles_t *handles, const char *sql, size_t nr_rows, size_t nr_cols, ...)
{
  va_list ap;
  va_start(ap, nr_cols);
  int r = _check_with_values_ap(line, func, handles, sql, nr_rows, nr_cols, ap);
  va_end(ap);

  return r ? -1 : 0;
}

#define CHECK_WITH_VALUES(...)       _check_with_values(__LINE__, __func__, ##__VA_ARGS__)

static int test_charsets(handles_t *handles)
{
  int r = 0;

  const char *conn_str = NULL;
  const char *sqls = NULL;
  const char *sql = NULL;

  handles_release(handles);

  conn_str = "DSN=TAOS_ODBC_DSN";
  r = handles_init(handles, conn_str);

  sqls =
    "drop database if exists foo;"
    "create database if not exists foo;"
    "create table foo.t (ts timestamp, name varchar(20), mark nchar(20));"
    "insert into foo.t (ts, name, mark) values (now(), 'name', 'mark');"
    "insert into foo.t (ts, name, mark) values (now()+1s, '测试', '检验');";
  r = _execute_batches_of_statements(handles, sqls);
  if (r) return -1;

  sql = "select name from foo.t where name='name'",
  r = CHECK_WITH_VALUES(handles, sql, 1, 1, "name");
  if (r) return -1;

  sql = "select mark from foo.t where mark='mark'",
  r = CHECK_WITH_VALUES(handles, sql, 1, 1, "mark");
  if (r) return -1;

  sql = "select name from foo.t where name='测试'",
  r = CHECK_WITH_VALUES(handles, sql, 1, 1, "测试");
  if (r) return -1;

  sql = "select mark from foo.t where mark='检验'",
  r = CHECK_WITH_VALUES(handles, sql, 1, 1, "检验");
  if (r) return -1;

  return 0;
}

typedef struct case_s              case_t;
struct case_s {
  const char               *name;
  int (*routine)(handles_t *handles);
};

#define RECORD(x) {#x, x}

static int running_case(handles_t *handles, case_t *_case)
{
  return _case->routine(handles);
}

static void usage(const char *arg0)
{
  fprintf(stderr, "%s -h\n"
                  "  show this help page\n"
                  "%s [name]...\n"
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
    for (size_t j=0; j<_nr_cases; ++j) {
      if (strcmp(_cases[j].name, arg)) continue;
      r = running_case(handles, _cases + j);
      if (r) return -1;
      ++nr_cases;
    }
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

  case_t _cases[] = {
    RECORD(test_case0),
    RECORD(test_charsets),
  };
  size_t _nr_cases = sizeof(_cases)/sizeof(_cases[0]);

  handles_t handles = {0};
  r = running_with_args(argc, argv, &handles, _cases, _nr_cases);
  handles_release(&handles);
  fprintf(stderr, "==%s==\n", r ? "failure" : "success");
  return !!r;
}

