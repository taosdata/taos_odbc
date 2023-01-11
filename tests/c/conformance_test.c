/*
 * MIT License
 *
 * Copyright (c) 2022 freemine <freemine@yeah.net>
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

#include "../test_helper.h"

#include <stdarg.h>
#include <stdint.h>


static int _under_taos_mysql_sqlite3 = 0;  // 0: taos; 1: mysql; 2: sqlite3

typedef struct conn_arg_s             conn_arg_t;
struct conn_arg_s {
  const char      *dsn;
  const char      *uid;
  const char      *pwd;
  const char      *connstr;
  unsigned int     non_taos:1;
};

static int _connect(SQLHANDLE hconn, const char *dsn, const char *uid, const char *pwd)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLConnect(hconn, (SQLCHAR*)dsn, SQL_NTS, (SQLCHAR*)uid, SQL_NTS, (SQLCHAR*)pwd, SQL_NTS);
  if (FAILED(sr)) {
    E("connect [dsn:%s,uid:%s,pwd:%s] failed", dsn, uid, pwd);
    return -1;
  }

  return 0;
}

static int _driver_connect(SQLHANDLE hconn, const char *connstr)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLDriverConnect(hconn, NULL, (SQLCHAR*)connstr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
  if (FAILED(sr)) {
    E("driver_connect [connstr:%s] failed", connstr);
    return -1;
  }

  return 0;
}

static int _exec_direct(SQLHANDLE hconn, const char *sql)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (FAILED(sr)) break;
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int _exec_and_check_count(SQLHANDLE hconn, const char *sql, size_t *count)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (FAILED(sr)) break;

    *count = 0;
    while (1) {
      sr = CALL_SQLFetch(hstmt);
      if (sr == SQL_ERROR) break;
      if (sr == SQL_NO_DATA) {
        sr = SQL_SUCCESS;
        break;
      }
      *count += 1;
    }
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int _check_varchar(const int irow, const int icol, SQLLEN ind, const char *varchar, const char *exp)
{
  if (ind == SQL_NULL_DATA) {
    if (exp) {
      E("(r#%d, c#%d) [%s] expected, but got ==%s==", irow+1, icol+1, exp, varchar);
      return -1;
    }
    return 0;
  }
  if(ind < 0) {
    E("not implemented yet");
    return -1;
  }

  if (!exp) {
    E("(r#%d, c#%d) `null` expected, but got ==%s==", irow+1, icol+1, varchar);
    return -1;
  }

  if (strcmp(varchar, exp)) {
    E("(r#%d, c#%d) [%s] expected, but got ==%s==", irow+1, icol+1, exp, varchar);
    return -1;
  }

  return 0;
}

static int _exec_and_check_with_stmt_v(SQLHANDLE hstmt, char *buf, size_t sz, const int cols, const int rows, va_list ap)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLSMALLINT ColumnCount = 0;

  sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
  if (FAILED(sr)) return -1;

  if (cols != ColumnCount) {
    E("#%d cols expected, but got ==%d==", cols, ColumnCount);
    return -1;
  }

  for (int i=0; i<rows; ++i) {
    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_NO_DATA) {
      E("#%d rows expected, but got ==%d==", rows, i);
      return -1;
    }
    if (FAILED(sr)) {
      E("fetching #%d row failed", i+1);
      return -1;
    }

    for (int j=0; j<cols; ++j) {
      if (sz > 0) buf[0] = '\0';

      SQLLEN ind = 0;
      sr = CALL_SQLGetData(hstmt, j+1, SQL_C_CHAR, (SQLPOINTER)buf, sz, &ind);
      if (FAILED(sr)) {
        E("getting (r#%d, c#%d) failed", i+1, j+1);
        return -1;
      }

      if (sr == SQL_SUCCESS) {
        const char *exp = va_arg(ap, const char*);
        r = _check_varchar(i, j, ind, buf, exp);
        if (r) return -1;
      } else if (sr == SQL_SUCCESS_WITH_INFO) {
        E("not implemented yet");
        return -1;
      } else {
        E("internal logic error");
        return -1;
      }
    }
  }

  return 0;
}

static int _exec_and_check(SQLHANDLE hconn, char *buf, size_t sz, const char *sql, const int cols, const int rows, ...)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (FAILED(sr)) break;

    if (buf == (char*)-1) break;

    va_list ap;
    va_start(ap, rows);
    r = _exec_and_check_with_stmt_v(hstmt, buf, sz, cols, rows, ap);
    va_end(ap);
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int _exec_and_bind_check_with_stmt_v(SQLHANDLE hstmt, char *buf, size_t sz, const int cols, const int rows, va_list ap)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLSMALLINT ColumnCount = 0;

  sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
  if (FAILED(sr)) return -1;

  if (cols != ColumnCount) {
    E("#%d cols expected, but got ==%d==", cols, ColumnCount);
    return -1;
  }

  const size_t sz_num_rows_fetched = sizeof(SQLULEN);
  const size_t sz_row_status_array = sizeof(SQLUSMALLINT) * rows;
  const size_t sz_ind_array        = sizeof(SQLLEN) * rows;
  const size_t sz_ind_arrays       = sz_ind_array * cols;
  if (sz < sz_num_rows_fetched + sz_row_status_array + sz_ind_arrays) {
    W("buffer too small");
    r = -1;
    return -1;
  }
  const size_t sz_data_arrays = sz - sz_num_rows_fetched - sz_row_status_array - sz_ind_arrays;
  const size_t sz_data_array  = sz_data_arrays / cols;
  const size_t sz_data        = sz_data_array / rows;

  const char *p = buf;
  const char *num_rows_fetched = p;           p += sz_num_rows_fetched;
  const char *row_status_array = p;           p += sz_row_status_array;
  const char *ind_arrays       = p;           p += sz_ind_arrays;
  const char *data_arrays      = p;

  int count_rows = 0;
  while (1) {
    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_NO_DATA) {
      if (count_rows != rows) {
        E("#%d rows expected, but got ==%d==", rows, count_rows);
        return -1;
      }
      break;
    }
    if (FAILED(sr)) {
      E("fetching #%d row failed", count_rows);
      return -1;
    }

    const SQLULEN rows_fetched = *(const SQLULEN*)num_rows_fetched;
    for (size_t i=0; i<rows_fetched; ++i) {
      const SQLUSMALLINT row_status = ((const SQLUSMALLINT*)row_status_array)[i];
      if (row_status == SQL_SUCCESS) {
        for (int j=0; j<cols; ++j) {
          const char *ind_array  = ind_arrays + sz_ind_array * j;
          const char *data_array = data_arrays + sz_data_array * j;

          const SQLLEN *ind   = (const SQLLEN*)(ind_array + sizeof(SQLLEN) * i);
          const char *data  = data_array + sz_data * i;

          if (sr == SQL_SUCCESS) {
            const char *exp = va_arg(ap, const char*);
            r = _check_varchar((int)i, j, *ind, data, exp);
            if (r) return -1;
          } else if (sr == SQL_SUCCESS_WITH_INFO) {
            E("not implemented yet");
            return -1;
          } else {
            E("internal logic error");
            return -1;
          }
        }
        count_rows += 1;
      } else {
        E("internal logic error");
        return -1;
      }
    }
  }

  return 0;
}

static int _exec_and_bind_check(SQLHANDLE hconn, char *buf, size_t sz, const char *sql, const int cols, const int rows, ...)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (FAILED(sr)) break;

    if (buf == (char*)-1) break;

    if (cols && rows) {
      size_t sz_num_rows_fetched = sizeof(SQLULEN);
      size_t sz_row_status_array = sizeof(SQLUSMALLINT) * rows;
      size_t sz_ind_array        = sizeof(SQLLEN) * rows;
      size_t sz_ind_arrays       = sz_ind_array * cols;
      if (sz < sz_num_rows_fetched + sz_row_status_array + sz_ind_arrays) {
        W("buffer too small");
        r = -1;
        break;
      }
      size_t sz_data_arrays = sz - sz_num_rows_fetched - sz_row_status_array - sz_ind_arrays;
      size_t sz_data_array  = sz_data_arrays / cols;
      size_t sz_data        = sz_data_array / rows;

      char *p = buf;
      char *num_rows_fetched = p;           p += sz_num_rows_fetched;
      char *row_status_array = p;           p += sz_row_status_array;
      char *ind_arrays       = p;           p += sz_ind_arrays;
      char *data_arrays      = p;

      sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_BIND_TYPE, SQL_BIND_BY_COLUMN, 0);
      if (FAILED(sr)) { r = -1; break; }
      sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)(uintptr_t)rows, 0);
      if (FAILED(sr)) { r = -1; break; }
      sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_STATUS_PTR, row_status_array, 0);
      if (FAILED(sr)) { r = -1; break; }
      sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_ROWS_FETCHED_PTR, num_rows_fetched, 0);
      if (FAILED(sr)) { r = -1; break; }

      char *ind_array  = ind_arrays;
      char *data_array = data_arrays;
      for (int i=0; i<cols; ++i) {
        sr = CALL_SQLBindCol(hstmt, i+1, SQL_C_CHAR, data_array, sz_data, (SQLLEN*)ind_array);
        if (FAILED(sr)) {
          r = -1;
          break;
        }
        ind_array  += sz_ind_array;
        data_array += sz_data_array;
      }
    }

    va_list ap;
    va_start(ap, rows);
    r = _exec_and_bind_check_with_stmt_v(hstmt, buf, sz, cols, rows, ap);
    va_end(ap);
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int _prepare_dataset_conn(SQLHANDLE hconn, const char **sqls, size_t nr)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  for (size_t i=0; i<nr; ++i) {
    const char *sql = sqls[i];
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (FAILED(sr)) {
      r = -1;
      break;
    }
  }

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return r;
}

static int test_case1(SQLHANDLE hconn)
{
  int r = 0;

  char buf[1024];
  r = _exec_and_check(hconn, buf, sizeof(buf), "drop table if exists t", 0, 0);
  if (r) return -1;

  r = _exec_and_check(hconn, buf, sizeof(buf), "create table t (ts timestamp, bi bigint)", 0, 0);
  if (r) return -1;

  r = _exec_and_check(hconn, buf, sizeof(buf), "insert into t (ts, bi) values ('2022-10-12 13:14:15', 34)", 0, 0);
  if (r) return -1;

  r = _exec_and_check(hconn, buf, sizeof(buf), "select bi from t where ts = '2022-10-12 13:14:15'", 1, 1, "34");
  if (r) return -1;

  return 0;
}

static int test_case2(SQLHANDLE hconn)
{
  int r = 0;

  const char *sqls[] = {
    "drop table if exists t",
    "create table t (ts timestamp, bi bigint)",
  };

  r = _prepare_dataset_conn(hconn, sqls, sizeof(sqls)/sizeof(sqls[0]));
  if (r) return -1;

  const char *fmt = "%Y-%m-%d %H:%M:%S";
  const char *ts = "2022-10-12 13:14:15";
  int64_t bi = 34;
  struct tm tm = {0};
  tod_strptime(ts, fmt, &tm);
  time_t tt = mktime(&tm);

#define COUNT 128

  for (size_t i=0; i<COUNT; ++i) {
    char s[64];
    strftime(s, sizeof(s), fmt, &tm);

    char sql[128];
    snprintf(sql, sizeof(sql), "insert into t (ts, bi) values ('%s', %" PRId64 ")", s, bi);

    r = _exec_direct(hconn, sql);
    if (r) return -1;

    ++tt;
    localtime_r(&tt, &tm);
    ++bi;
  }

  size_t count;
  r = _exec_and_check_count(hconn, "select * from t where ts = '2022-10-12 13:14:15'", &count);
  if (r) return -1;
  if (count != 1) {
    E("1 expected, but got ==%zd==", count);
    return -1;
  }

  r = _exec_and_check_count(hconn, "select * from t", &count);
  if (r) return -1;
  if (count != COUNT) {
    E("%d expected, but got ==%zd==", COUNT, count);
    return -1;
  }

#undef COUNT

  return 0;
}

static int select_count_with_col_bind(SQLHANDLE hconn, const char *sql, size_t *count)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  SQLCHAR ts[128];
  int64_t bi;
  do {
    sr = CALL_SQLBindCol(hstmt, 1, SQL_C_CHAR, &ts, sizeof(ts), NULL);
    if (FAILED(sr)) break;

    sr = CALL_SQLBindCol(hstmt, 2, SQL_C_SBIGINT, &bi, 0, NULL);
    if (FAILED(sr)) break;

    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (FAILED(sr)) break;

    *count = 0;
    while (1) {
      sr = CALL_SQLFetch(hstmt);
      if (sr == SQL_ERROR) break;
      if (sr == SQL_NO_DATA) {
        sr = SQL_SUCCESS;
        break;
      }
      *count += 1;
    }
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int test_case3(SQLHANDLE hconn)
{
  int r = 0;

  const char *sqls[] = {
    "drop table if exists t",
    "create table t (ts timestamp, bi bigint)",
  };

  r = _prepare_dataset_conn(hconn, sqls, sizeof(sqls)/sizeof(sqls[0]));
  if (r) return -1;

  const char *fmt = "%Y-%m-%d %H:%M:%S";
  const char *ts = "2022-10-12 13:14:15";
  int64_t bi = 34;
  struct tm tm = {0};
  tod_strptime(ts, fmt, &tm);
  time_t tt = mktime(&tm);

#define COUNT 3

  for (size_t i=0; i<COUNT; ++i) {
    char s[64];
    strftime(s, sizeof(s), fmt, &tm);

    char sql[128];
    snprintf(sql, sizeof(sql), "insert into t (ts, bi) values ('%s', %" PRId64 ")", s, bi);

    r = _exec_direct(hconn, sql);
    if (r) return -1;

    ++tt;
    localtime_r(&tt, &tm);
    ++bi;
  }

  char buf[1024]; buf[0] = '\0';
  r = _exec_and_bind_check(hconn, buf, sizeof(buf),
      "select bi from t where ts = '2022-10-12 13:14:15'",
      1, 1,
      "34");
  if (r) return -1;

  r = _exec_and_bind_check(hconn, buf, sizeof(buf),
      "select bi from t",
      1, 3,
      "34",
      "35",
      "36");
  if (r) return -1;

  r = _exec_and_bind_check(hconn, buf, sizeof(buf),
      "select bi a, bi+3 b from t",
      2, 3,
      "34", "37",
      "35", "38",
      "36", "39");
  if (r) return -1;

  size_t count;
  r = select_count_with_col_bind(hconn, "select * from t where ts = '2022-10-12 13:14:15'", &count);
  if (r) return -1;
  if (count != 1) {
    E("1 expected, but got ==%zd==", count);
    return -1;
  }

  r = select_count_with_col_bind(hconn, "select * from t", &count);
  if (r) return -1;
  if (count != COUNT) {
    E("%d expected, but got ==%zd==", COUNT, count);
    return -1;
  }

#undef COUNT

  return 0;
}

static int select_count_with_col_bind_array(SQLHANDLE hconn, const char *sql, size_t array_size, size_t *count, size_t *batches)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

#define ARRAY_SIZE 4096
  SQLCHAR ts[ARRAY_SIZE][128];
  SQLLEN ts_ind[ARRAY_SIZE];
  int64_t bi[ARRAY_SIZE];
  SQLLEN bi_ind[ARRAY_SIZE];
  SQLUSMALLINT status[ARRAY_SIZE];
  SQLULEN nr_rows;
  do {
    if (array_size > ARRAY_SIZE) {
      E("array_size[%zd] too large [%d]", array_size, ARRAY_SIZE);
      r = -1;
      break;
    }
    sr = CALL_SQLBindCol(hstmt, 1, SQL_C_CHAR, &ts[0][0], sizeof(ts[0]), ts_ind);
    if (FAILED(sr)) break;

    sr = CALL_SQLBindCol(hstmt, 2, SQL_C_SBIGINT, &bi, 0, bi_ind);
    if (FAILED(sr)) break;

    sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)array_size, 0);
    if (FAILED(sr)) break;
    sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_STATUS_PTR, status, 0);
    if (FAILED(sr)) break;
    sr = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_ROWS_FETCHED_PTR, &nr_rows, 0);
    if (FAILED(sr)) break;

    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (FAILED(sr)) break;

    *batches = 0;
    *count = 0;
    while (1) {
      sr = CALL_SQLFetch(hstmt);
      if (sr == SQL_ERROR) break;
      if (sr == SQL_NO_DATA) {
        sr = SQL_SUCCESS;
        break;
      }
      // D("nr_rows: %d, array_size: %ld", nr_rows, array_size);
      *count += nr_rows;
      ++*batches;
    }
  } while (0);

#undef ARRAY_SIZE

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int test_case4(SQLHANDLE hconn, int non_taos, const size_t dataset, const size_t array_size)
{
  int r = 0;

  r = _exec_direct(hconn, "drop table if exists t");
  if (r) return -1;

  r = _exec_direct(hconn, "create table t (ts timestamp, bi bigint)");
  if (r) return -1;

  const char *fmt = "%Y-%m-%d %H:%M:%S";
  const char *ts = "2022-10-12 13:14:15";
  int64_t bi = 34;
  struct tm tm = {0};
  tod_strptime(ts, fmt, &tm);
  time_t tt = mktime(&tm);

  for (size_t i=0; i<dataset; ++i) {
    char s[64];
    strftime(s, sizeof(s), fmt, &tm);

    char sql[128];
    snprintf(sql, sizeof(sql), "insert into t (ts, bi) values ('%s', %" PRId64 ")", s, bi);

    r = _exec_direct(hconn, sql);
    if (r) return -1;

    ++tt;
    localtime_r(&tt, &tm);
    ++bi;
  }

  size_t count;
  size_t batches;
  r = select_count_with_col_bind_array(hconn, "select * from t", array_size, &count, &batches);
  if (r) return -1;
  if (count != dataset) {
    E("%zd in total expected, but got ==%zd==", dataset, count);
    return -1;
  }
  if (batches != (dataset + array_size - 1) / array_size) {
    // TODO: SQLFetch for taos-odbc is still not fully implemented yet
    // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlfetch-function?view=sql-server-ver16#positioning-the-cursor
    if (non_taos) {
      E("%zd in total, batches[%zd] expected, but got ==%zd==", count, (dataset + array_size - 1) / array_size, batches);
      return -1;
    }
  }

  return 0;
}

static int _dump_rs_to_sql_c_char(SQLHANDLE hstmt, SQLSMALLINT ColumnCount)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  for (SQLSMALLINT i=0; i<ColumnCount; ++i) {
    char buf[1024];
    SQLSMALLINT NameLength;
    SQLSMALLINT DataType;
    SQLULEN ColumnSize;
    SQLSMALLINT DecimalDigits;
    SQLSMALLINT Nullable;
    sr = CALL_SQLDescribeCol(hstmt, i+1, (SQLCHAR*)buf, sizeof(buf), &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
    if (FAILED(sr)) break;
  }
  if (FAILED(sr)) return -1;

  while (1) {
    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_ERROR) break;
    if (sr == SQL_NO_DATA) {
      sr = SQL_SUCCESS;
      break;
    }
    for (SQLSMALLINT i=0; i<ColumnCount; ++i) {
      char buf[1024];
      SQLLEN ind;
      sr = CALL_SQLGetData(hstmt, i+1, SQL_C_CHAR, (SQLPOINTER)buf, sizeof(buf), &ind);
      if (FAILED(sr)) break;
      D("Column[%d]: ==%s==", i+1, ind == SQL_NULL_DATA ? "(null)" : buf);
    }
  }

  return (r || FAILED(sr)) ? -1 : 0;
}

static int test_case5(SQLHANDLE hconn)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  const char *CatalogName;
  const char *SchemaName;
  const char *TableName;
  const char *TableType;

  SQLSMALLINT ColumnCount;

  do {
    CatalogName = "%";
    SchemaName = "";
    TableName = "";
    TableType = "";

    sr = CALL_SQLTables(hstmt,
      (SQLCHAR*)CatalogName, SQL_NTS/*strlen(CatalogName)*/,
      (SQLCHAR*)SchemaName,  SQL_NTS/*strlen(SchemaName)*/,
      (SQLCHAR*)TableName,   SQL_NTS/*strlen(TableName)*/,
      (SQLCHAR*)TableType,   SQL_NTS/*strlen(TableType)*/);
    if (FAILED(sr)) break;

    sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
    if (FAILED(sr)) break;

    if (ColumnCount <= 0) {
      E("result columns expected, but got ==%d==", ColumnCount);
      r = -1;
      break;
    }

    r = _dump_rs_to_sql_c_char(hstmt, ColumnCount);
    if (r) break;

    CatalogName = NULL;
    SchemaName = NULL;
    TableName = NULL;
    TableType = NULL;

    sr = CALL_SQLTables(hstmt,
      (SQLCHAR*)CatalogName, SQL_NTS/*strlen(CatalogName)*/,
      (SQLCHAR*)SchemaName,  SQL_NTS/*strlen(SchemaName)*/,
      (SQLCHAR*)TableName,   SQL_NTS/*strlen(TableName)*/,
      (SQLCHAR*)TableType,   SQL_NTS/*strlen(TableType)*/);
    if (FAILED(sr)) break;

    sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
    if (FAILED(sr)) break;

    if (ColumnCount <= 0) {
      E("result columns expected, but got ==%d==", ColumnCount);
      r = -1;
      break;
    }

    r = _dump_rs_to_sql_c_char(hstmt, ColumnCount);
    if (r) break;

    // CatalogName = "";
    // SchemaName = "%";
    // TableName = "";
    // TableType = "";
    // sr = CALL_SQLTables(hstmt,
    //   (SQLCHAR*)CatalogName, strlen(CatalogName),
    //   (SQLCHAR*)SchemaName,  strlen(SchemaName),
    //   (SQLCHAR*)TableName,   strlen(TableName),
    //   (SQLCHAR*)TableType,   strlen(TableType));
    // if (FAILED(sr)) break;

    // r = _dump_rs_to_sql_c_char(hstmt, ColumnCount);
    // if (r) break;

    CatalogName = "";
    SchemaName = "";
    TableName = "";
    TableType = "%";
    sr = CALL_SQLTables(hstmt,
      (SQLCHAR*)CatalogName, (SQLSMALLINT)strlen(CatalogName),
      (SQLCHAR*)SchemaName,  (SQLSMALLINT)strlen(SchemaName),
      (SQLCHAR*)TableName,   (SQLSMALLINT)strlen(TableName),
      (SQLCHAR*)TableType,   (SQLSMALLINT)strlen(TableType));
    if (FAILED(sr)) break;

    r = _dump_rs_to_sql_c_char(hstmt, ColumnCount);
    if (r) break;

    CatalogName = "";
    SchemaName = "";
    TableName = "";
    TableType = "'TABLE'";
    sr = CALL_SQLTables(hstmt,
      (SQLCHAR*)CatalogName, (SQLSMALLINT)strlen(CatalogName),
      (SQLCHAR*)SchemaName,  (SQLSMALLINT)strlen(SchemaName),
      (SQLCHAR*)TableName,   (SQLSMALLINT)strlen(TableName),
      (SQLCHAR*)TableType,   (SQLSMALLINT)strlen(TableType));
    if (FAILED(sr)) break;

    r = _dump_rs_to_sql_c_char(hstmt, ColumnCount);
    if (r) break;

  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int test_case6(SQLHANDLE hconn)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  r = _exec_direct(hconn, "drop table if exists t");
  if (r) return -1;

  r = _exec_direct(hconn, "create table t (ts timestamp, name varchar(1024))");
  if (r) return -1;

  const char *name = "helloworldfoobargreatwall";
  char sql[1024];
  snprintf(sql, sizeof(sql), "insert into t (ts, name) values ('2022-10-12 13:14:15', '%s')", name);

  r = _exec_direct(hconn, sql);
  if (r) return -1;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)"select name from t", SQL_NTS);
    if (FAILED(sr)) break;

    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_ERROR) break;
    if (sr == SQL_NO_DATA) {
      sr = SQL_SUCCESS;
      break;
    }

    char buf[2048];
    buf[0] = '\0';
    char *p = buf;
    size_t count = 0;
    SQLLEN BufferLength = 2;
    SQLLEN StrLen_or_Ind;

    while (1) {
      StrLen_or_Ind = 0;
      sr = CALL_SQLGetData(hstmt, 1, SQL_C_CHAR, p, BufferLength, &StrLen_or_Ind);
      if (sr == SQL_ERROR) break;
      if (sr == SQL_SUCCESS_WITH_INFO || sr == SQL_SUCCESS) {
        size_t n;
        if (StrLen_or_Ind == SQL_NO_TOTAL) {
          n = strnlen(p, BufferLength);
        } else {
          n = strlen(p);
        }
        if (n == (size_t)BufferLength) {
          E("internal logic error");
          r = -1;
          break;
        }
        p += n;
        count += n;
        continue;
      }
      if (sr == SQL_NO_DATA) {
        if (strncmp(buf, name, strlen(name) + 1)) {
          E("internal logic error: %.*s <> %s", (int)sizeof(buf), buf, name);
          r = -1;
        }
        sr = SQL_SUCCESS;
        break;
      }
      E("internal logic error");
      r = -1;
      break;
    }
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int test_case7(SQLHANDLE hconn)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  r = _exec_direct(hconn, "drop table if exists t");
  if (r) return -1;

  r = _exec_direct(hconn, "create table t (ts timestamp, name varchar(1024))");
  if (r) return -1;

  const char *ts   = "2022-10-12 13:14:15";
  const char *name = "foo";
  char sql[1024];
  snprintf(sql, sizeof(sql), "insert into t (ts, name) values ('%s', '%s')", ts, name);

  r = _exec_direct(hconn, sql);
  if (r) return -1;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)"select ts from t", SQL_NTS);
    if (FAILED(sr)) break;

    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_ERROR) break;
    if (sr == SQL_NO_DATA) {
      sr = SQL_SUCCESS;
      break;
    }

    char buf[2048];
    buf[0] = '\0';
    char *p = buf;
    size_t count = 0;
    SQLLEN BufferLength = 2;
    SQLLEN StrLen_or_Ind;

    while (1) {
      StrLen_or_Ind = 0;
      sr = CALL_SQLGetData(hstmt, 1, SQL_C_CHAR, p, BufferLength, &StrLen_or_Ind);
      if (sr == SQL_ERROR) break;
      if (sr == SQL_SUCCESS_WITH_INFO || sr == SQL_SUCCESS) {
        size_t n;
        if (StrLen_or_Ind == SQL_NO_TOTAL) {
          n = strnlen(p, BufferLength);
        } else {
          n = strlen(p);
        }
        if (n == (size_t)BufferLength) {
          E("internal logic error");
          r = -1;
          break;
        }
        p += n;
        count += n;
        continue;
      }
      if (sr == SQL_NO_DATA) {
        if (strncmp(buf, ts, strlen(ts))) {
          E("internal logic error: %.*s <> %s", (int)sizeof(buf), buf, ts);
          r = -1;
        }
        sr = SQL_SUCCESS;
        break;
      }
      E("internal logic error");
      r = -1;
      break;
    }
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int test_case8(SQLHANDLE hconn)
{
  SQLRETURN sr = SQL_SUCCESS;

  char buf[1024]; buf[0] = '\0';
  SQLINTEGER len;
  sr = CALL_SQLGetConnectAttr(hconn, SQL_CURRENT_QUALIFIER, (SQLPOINTER)buf, sizeof(buf), &len);
  if (sr == SQL_ERROR) return -1;
  if (sr == SQL_SUCCESS) {
    D("SQL_CURRENT_QUALIFIER:%.*s", len, buf);
    return 0;
  }
  E("sr:%s", sql_return_type(sr));
  return -1;
}

static int _vexec_(SQLHANDLE hstmt, const char *fmt, va_list ap)
{
  char buf[1024];
  char *sql = buf;

  va_list aq;
  va_copy(aq, ap);
  int n = vsnprintf(buf, sizeof(buf), fmt, aq);
  if (n < 0) {
    int e = errno;
    E("vsnprintf failed:[%d]%s", e, strerror(e));
    return -1;
  }
  if ((size_t)n >= sizeof(buf)) {
    sql = (char*)malloc(n + 1);
    if (!sql) {
      W("out of memory");
      return -1;
    }
    int m = vsnprintf(sql, n+1, fmt, ap);
    A(m == n, "");
  }
  SQLRETURN sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sql != buf) free(sql);
  va_end(aq);

  if (sr == SQL_ERROR) return -1;
  return 0;
}

static int _exec_impl(SQLHANDLE hstmt, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  int r = _vexec_(hstmt, fmt, ap);
  va_end(ap);

  return r ? -1 : 0;
}


#define _exec_(hstmt, fmt, ...) (0 ? printf(fmt, ##__VA_ARGS__) : _exec_impl(hstmt, fmt, ##__VA_ARGS__))

typedef struct simple_str_s                 simple_str_t;
struct simple_str_s {
  size_t                       cap;
  size_t                       nr;
  char                        *base;
};

static int _simple_str_fmt_impl(simple_str_t *str, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  char *base = str->base ? str->base + str->nr : NULL;
  size_t sz = str->cap - str->nr;
  int n = vsnprintf(base, sz, fmt, ap);
  va_end(ap);

  if (n < 0) {
    int e = errno;
    E("vsnprintf failed:[%d]%s", e, strerror(e));
    return -1;
  }

  if (str->base) {
    if ((size_t)n >= sz) {
      W("buffer too small");
      return -1;
    }
    str->nr += n;
  }

  return 0;
}

#define _simple_str_fmt(str, fmt, ...) (0 ? printf(fmt, ##__VA_ARGS__) : _simple_str_fmt_impl(str, fmt, ##__VA_ARGS__))

typedef struct field_s            field_t;
struct field_s {
  const char       *name;
  const char       *field;
};

static int _gen_table_create_sql(simple_str_t *str, const char *table, const field_t *fields, size_t nr_fields)
{
  int r = 0;
  r = _simple_str_fmt(str, "create table %s", table);
  if (r) return -1;

  for (size_t i=0; i<nr_fields; ++i) {
    const field_t *field = fields + i;
    if (i == 0) {
      r = _simple_str_fmt(str, " (");
    } else {
      r = _simple_str_fmt(str, ",");
    }
    if (r) return -1;
    r = _simple_str_fmt(str, "%s %s", field->name, field->field);
  }
  if (nr_fields > 0) {
    r = _simple_str_fmt(str, ")");
    if (r) return -1;
  }

  return 0;
}

static int _gen_table_param_insert(simple_str_t *str, const char *table, const field_t *fields, size_t nr_fields)
{
  int r = 0;
  r = _simple_str_fmt(str, "insert into %s", table);
  if (r) return -1;

  for (size_t i=0; i<nr_fields; ++i) {
    const field_t *field = fields + i;
    if (i == 0) {
      r = _simple_str_fmt(str, " (");
    } else {
      r = _simple_str_fmt(str, ",");
    }
    if (r) return -1;
    r = _simple_str_fmt(str, "%s", field->name);

    if (i+1 < nr_fields) continue;
    r = _simple_str_fmt(str, ")");
    if (r) return -1;
  }

  for (size_t i=0; i<nr_fields; ++i) {
    if (i == 0) {
      r = _simple_str_fmt(str, " values (");
    } else {
      r = _simple_str_fmt(str, ",");
    }
    if (r) return -1;
    r = _simple_str_fmt(str, "?");

    if (i+1 < nr_fields) continue;
    r = _simple_str_fmt(str, ")");
    if (r) return -1;
  }

  return 0;
}

typedef struct param_s                   param_t;
struct param_s {
  SQLSMALLINT     InputOutputType;
  SQLSMALLINT     ValueType;
  SQLSMALLINT     ParameterType;
  SQLULEN         ColumnSize;
  SQLSMALLINT     DecimalDigits;
  SQLPOINTER      ParameterValuePtr;
  SQLLEN          BufferLength;
  SQLLEN         *StrLen_or_IndPtr;
};

static int test_bind_params_with_stmt(SQLHANDLE hconn, SQLHANDLE hstmt)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;
  char buf[1024];
  simple_str_t str = {
    .base           = buf,
    .cap            = sizeof(buf),
    .nr             = 0,
  };

  const char *database = "foo";

  if (_under_taos_mysql_sqlite3 != 2) {
    r = _exec_(hstmt, "drop database if exists %s", database);
    if (r) return -1;
    r = _exec_(hstmt, "create database if not exists %s", database);
    if (r) return -1;
    r = _exec_(hstmt, "use %s", database);
    if (r) return -1;
  }

  const char *table = "t";
  field_t fields[] = {
    {"ts", "timestamp"},
    {"vname", "varchar(10)"},
    {"wname", "nchar(10)"},
    {"bi", "bigint"},
  };
  if (_under_taos_mysql_sqlite3) {
    fields[0].field = "bigint";
  }

  r = _exec_(hstmt, "drop table if exists %s", table);
  if (r) return -1;

  r = _gen_table_create_sql(&str, table, fields, sizeof(fields)/sizeof(fields[0]));
  if (r) return -1;
  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)buf, SQL_NTS);
  if (sr == SQL_ERROR) return -1;

#define ARRAY_SIZE 10

  SQLUSMALLINT param_status_arr[ARRAY_SIZE] = {0};
  SQLULEN nr_params_processed = 0;
  int64_t ts_arr[ARRAY_SIZE] = {1662861448751};
  SQLLEN  ts_ind[ARRAY_SIZE] = {0};
  char    varchar_arr[ARRAY_SIZE][100];
  SQLLEN  varchar_ind[ARRAY_SIZE] = {0};
  char    nchar_arr[ARRAY_SIZE][100];
  SQLLEN  nchar_ind[ARRAY_SIZE] = {0};
  int64_t i64_arr[ARRAY_SIZE] = {1662861448751};
  SQLLEN  i64_ind[ARRAY_SIZE] = {0};

  const param_t params[] = {
    {SQL_PARAM_INPUT,  SQL_C_SBIGINT,  SQL_TYPE_TIMESTAMP,  23,       3,          ts_arr,           0,   ts_ind},
    {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_VARCHAR,         99,       0,          varchar_arr,      100, varchar_ind},
    {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_WVARCHAR,        99,       0,          nchar_arr,        100, nchar_ind},
    {SQL_PARAM_INPUT,  SQL_C_SBIGINT,  SQL_BIGINT,          99,       0,          i64_arr,          100, i64_ind},
  };

  SQLULEN nr_paramset_size = 4;

  for (int i=0; i<ARRAY_SIZE; ++i) {
    ts_arr[i] = 1662861448751 + i;
    snprintf(varchar_arr[i], 100, "a人%d", i);
    varchar_ind[i] = SQL_NTS;
    snprintf(nchar_arr[i], 100, "b民%d", i);
    nchar_ind[i] = SQL_NTS;
    i64_arr[i] = 54321 + i;
  }

  str.nr = 0;
  r = _gen_table_param_insert(&str, table, fields, sizeof(fields)/sizeof(fields[0]));
  if (r) return -1;

  sr = CALL_SQLPrepare(hstmt, (SQLCHAR*)buf, SQL_NTS);
  if (sr == SQL_ERROR) return -1;

  // Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
  // column-wise binding.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);

  // Specify the number of elements in each parameter array.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)(uintptr_t)nr_paramset_size, 0);

  // Specify an array in which to return the status of each set of
  // parameters.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_STATUS_PTR, param_status_arr, 0);

  // Specify an SQLUINTEGER value in which to return the number of sets of
  // parameters processed.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &nr_params_processed, 0);

  // Bind the parameters in column-wise fashion.
  for (size_t i=0; i<sizeof(params)/sizeof(params[0]); ++i) {
    const param_t *param = params + i;
    sr = CALL_SQLBindParameter(hstmt, (SQLUSMALLINT)i+1,
        param->InputOutputType,
        param->ValueType,
        param->ParameterType,
        param->ColumnSize,
        param->DecimalDigits,
        param->ParameterValuePtr,
        param->BufferLength,
        param->StrLen_or_IndPtr);
    if (sr == SQL_ERROR) return -1;
  }


  // Set part ID, description, and price.

  // Execute the statement.
  sr = CALL_SQLExecute(hstmt);
  if (sr == SQL_ERROR) return -1;

  if (nr_params_processed != nr_paramset_size) {
    W("%zu rows of params to be processed, but only %zu", nr_paramset_size, nr_params_processed);
    return -1;
  }

  // Check to see which sets of parameters were processed successfully.
  for (size_t i = 0; i < nr_params_processed; i++) {
    switch (param_status_arr[i]) {
      case SQL_PARAM_SUCCESS:
        X("param row #%13zd  Success", i + 1);
        break;
      case SQL_PARAM_SUCCESS_WITH_INFO:
        X("param row #%13zd  Success with info", i + 1);
        break;

      case SQL_PARAM_ERROR:
        E("param row #%13zd  Error", i + 1);
        r = -1;
        break;

      case SQL_PARAM_UNUSED:
        E("param row #%13zd  Not processed", i + 1);
        r = -1;
        break;

      case SQL_PARAM_DIAG_UNAVAILABLE:
        E("param row #%13zd  Diag unavailable", i + 1);
        r = -1;
        break;

      default:
        E("internal logic error");
        r = -1;
        break;
    }
  }

  r = _exec_and_bind_check(hconn, buf, sizeof(buf),
      "select wname from t",
      1, 4,
      "b民0", "b民1", "b民2", "b民3");
  if (r) return -1;

  r = _exec_and_bind_check(hconn, buf, sizeof(buf),
      "select vname from t",
      1, 4,
      "a人0", "a人1", "a人2", "a人3");
  if (r) return -1;

  return r ? -1 : 0;
}

static int test_bind_params_with_stmt_status(SQLHANDLE hconn, SQLHANDLE hstmt)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;
  char buf[1024];
  simple_str_t str = {
    .base           = buf,
    .cap            = sizeof(buf),
    .nr             = 0,
  };

  const char *database = "foo";

  if (_under_taos_mysql_sqlite3 != 2) {
    r = _exec_(hstmt, "drop database if exists %s", database);
    if (r) return -1;
    r = _exec_(hstmt, "create database if not exists %s", database);
    if (r) return -1;
    r = _exec_(hstmt, "use %s", database);
    if (r) return -1;
  } else {
    W("not fully test under sqlite3");
    return 0;
  }

  const char *table = "t";
  field_t fields[] = {
    {"ts", "timestamp"},
    {"vname", "varchar(5)"},
    {"wname", "nchar(10)"},
    {"bi", "bigint"},
  };
  if (_under_taos_mysql_sqlite3) {
    fields[0].field = "bigint";
  }

  r = _exec_(hstmt, "drop table if exists %s", table);
  if (r) return -1;

  r = _gen_table_create_sql(&str, table, fields, sizeof(fields)/sizeof(fields[0]));
  if (r) return -1;
  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)buf, SQL_NTS);
  if (sr == SQL_ERROR) return -1;

#define ARRAY_SIZE 10

  SQLUSMALLINT param_status_arr[ARRAY_SIZE] = {0};
  SQLULEN nr_params_processed = 0;
  int64_t ts_arr[ARRAY_SIZE] = {1662861448751};
  SQLLEN  ts_ind[ARRAY_SIZE] = {0};
  char    varchar_arr[ARRAY_SIZE][100];
  SQLLEN  varchar_ind[ARRAY_SIZE] = {0};
  char    nchar_arr[ARRAY_SIZE][100];
  SQLLEN  nchar_ind[ARRAY_SIZE] = {0};
  int64_t i64_arr[ARRAY_SIZE] = {1662861448751};
  SQLLEN  i64_ind[ARRAY_SIZE] = {0};

  const param_t params[] = {
    {SQL_PARAM_INPUT,  SQL_C_SBIGINT,  SQL_TYPE_TIMESTAMP,  23,       3,          ts_arr,           0,   ts_ind},
    {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_VARCHAR,         99,       0,          varchar_arr,      100, varchar_ind},
    {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_WVARCHAR,        99,       0,          nchar_arr,        100, nchar_ind},
    {SQL_PARAM_INPUT,  SQL_C_SBIGINT,  SQL_BIGINT,          99,       0,          i64_arr,          100, i64_ind},
  };

  SQLULEN nr_paramset_size = 4;

  for (int i=0; i<ARRAY_SIZE; ++i) {
    ts_arr[i] = 1662861448751 + i;
    snprintf(varchar_arr[i], 100, "abcd%d", i+9);
    varchar_ind[i] = SQL_NTS;
    snprintf(nchar_arr[i], 100, "b民%d", i);
    nchar_ind[i] = SQL_NTS;
    i64_arr[i] = 54321 + i;
  }

  str.nr = 0;
  r = _gen_table_param_insert(&str, table, fields, sizeof(fields)/sizeof(fields[0]));
  if (r) return -1;

  sr = CALL_SQLPrepare(hstmt, (SQLCHAR*)buf, SQL_NTS);
  if (sr == SQL_ERROR) return -1;

  // Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
  // column-wise binding.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);

  // Specify the number of elements in each parameter array.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)(uintptr_t)nr_paramset_size, 0);

  // Specify an array in which to return the status of each set of
  // parameters.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_STATUS_PTR, param_status_arr, 0);

  // Specify an SQLUINTEGER value in which to return the number of sets of
  // parameters processed.
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &nr_params_processed, 0);

  // Bind the parameters in column-wise fashion.
  for (size_t i=0; i<sizeof(params)/sizeof(params[0]); ++i) {
    const param_t *param = params + i;
    sr = CALL_SQLBindParameter(hstmt, (SQLUSMALLINT)i+1,
        param->InputOutputType,
        param->ValueType,
        param->ParameterType,
        param->ColumnSize,
        param->DecimalDigits,
        param->ParameterValuePtr,
        param->BufferLength,
        param->StrLen_or_IndPtr);
    if (sr == SQL_ERROR) return -1;
  }


  // Set part ID, description, and price.

  // Execute the statement.
  sr = CALL_SQLExecute(hstmt);
  if (sr == SQL_ERROR) return -1;

  if (nr_params_processed != nr_paramset_size) {
    W("%zu rows of params to be processed, but only %zu", nr_paramset_size, nr_params_processed);
  }

  // Check to see which sets of parameters were processed successfully.
  for (size_t i = 0; i < nr_params_processed; i++) {
    switch (param_status_arr[i]) {
      case SQL_PARAM_SUCCESS:
        X("param row #%13zd  Success", i + 1);
        break;
      case SQL_PARAM_SUCCESS_WITH_INFO:
        X("param row #%13zd  Success with info", i + 1);
        break;

      case SQL_PARAM_ERROR:
        E("param row #%13zd  Error", i + 1);
        r = -1;
        break;

      case SQL_PARAM_UNUSED:
        E("param row #%13zd  Not processed", i + 1);
        r = -1;
        break;

      case SQL_PARAM_DIAG_UNAVAILABLE:
        E("param row #%13zd  Diag unavailable", i + 1);
        r = -1;
        break;

      default:
        E("internal logic error");
        r = -1;
        break;
    }
  }

  r = _exec_and_bind_check(hconn, buf, sizeof(buf),
      "select wname from t",
      1, 1,
      "b民0");
  if (r) return -1;

  r = _exec_and_bind_check(hconn, buf, sizeof(buf),
      "select vname from t",
      1, 1,
      "abcd9");
  if (r) return -1;

  return r ? -1 : 0;
}

static int test_bind_params(SQLHANDLE hconn)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    r = test_bind_params_with_stmt(hconn, hstmt);
    if (r) break;

    r = test_bind_params_with_stmt_status(hconn, hstmt);
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

typedef struct prepare_checker_s              prepare_checker_t;
struct prepare_checker_s {
  const char                     *sql;
  SQLSMALLINT                     paramCount;
};

static int test_prepare_with_stmt(SQLHANDLE hstmt)
{
  SQLRETURN sr = SQL_SUCCESS;
  const char *sqls[] = {
    "show databases",
    "drop database if exists foo",
    "create database if not exists foo",
    "use foo",
    "create table t (ts timestamp, v int)",
    "create stable s (ts timestamp, v int) tags (id int)",
  };

  for (size_t i=0; i<sizeof(sqls) / sizeof(sqls[0]); ++i) {
    const char *sql = sqls[i];
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (sr == SQL_ERROR) return SQL_ERROR;
  }

  prepare_checker_t checkers[] = {
    {"insert into ? using s tags (?) values (?, ?)", 4},
    {"insert into suzhou using s tags (?) values (?, ?)", 3},
    {"insert into suzhou using s tags (3) values (?, ?)", 3},
  };

  for (size_t i=0; i<sizeof(checkers)/sizeof(checkers[0]); ++i) {
    prepare_checker_t *checker = checkers + i;
    SQLSMALLINT paramCount = 0;

    sr = CALL_SQLPrepare(hstmt, (SQLCHAR*)checker->sql, SQL_NTS);
    if (sr == SQL_ERROR) return SQL_ERROR;
    sr = CALL_SQLNumParams(hstmt, &paramCount);
    if (sr == SQL_ERROR) return SQL_ERROR;
    if (paramCount != checker->paramCount) {
      E("`%s`:expected %d params, but got ==%d==", checker->sql, checker->paramCount, paramCount);
      return -1;
    }
  }

  return 0;
}

static int test_prepare(SQLHANDLE hconn)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    r = test_prepare_with_stmt(hstmt);
    if (r) break;
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return (r || FAILED(sr)) ? -1 : 0;
}

static int test_cases(SQLHANDLE hconn)
{
  int r = 0;

  _exec_direct(hconn, "create database if not exists foo");
  _exec_direct(hconn, "use foo");

  r = test_prepare(hconn);
  if (r) return r;

  r = test_bind_params(hconn);
  if (r) return r;

  r = test_case1(hconn);
  if (r) return r;;

  r = test_case2(hconn);
  if (r) return r;;

  r = test_case3(hconn);
  if (r) return r;;

  int non_taos = 0;

  r = test_case4(hconn, non_taos, 128, 113);
  if (r) return r;;

  if (0 && !non_taos) {
    r = test_case4(hconn, non_taos, 5000, 4000);
    if (r) return r;;
  }

  r = test_case5(hconn);
  if (r) return r;;

  r = test_case6(hconn);
  if (r) return r;;

  r = test_case7(hconn);
  if (r) return r;;

  r = test_case8(hconn);
  if (r) return r;

  return r;
}

static void check_driver(SQLHANDLE hconn)
{
  char buf[1024]; buf[0] = '\0';
  SQLSMALLINT StringLength = 0;
  SQLRETURN sr = CALL_SQLGetInfo(hconn, SQL_DRIVER_NAME, (SQLCHAR*)buf, sizeof(buf), &StringLength);
  if (FAILED(sr)) {
    W("fall back to taos-odbc");
    _under_taos_mysql_sqlite3 = 0;
    return;
  }
  D("driver_name:%s", buf);
  if (strstr(buf, "taos")) {
    _under_taos_mysql_sqlite3 = 0;
    return;
  }
  if (strstr(buf, "sqlite3")) {
    _under_taos_mysql_sqlite3 = 2;
    return;
  }
  if (strstr(buf, "my")) {
    _under_taos_mysql_sqlite3 = 1;
    return;
  }
  W("fall back to taos-odbc");
  _under_taos_mysql_sqlite3 = 0;
}

static int test_conn(int argc, char *argv[], SQLHANDLE hconn)
{
  conn_arg_t conn_arg = {0};

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "--dsn") == 0) {
      ++i;
      if (i>=argc) break;
      conn_arg.dsn = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--uid") == 0) {
      ++i;
      if (i>=argc) break;
      conn_arg.uid = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--pwd") == 0) {
      ++i;
      if (i>=argc) break;
      conn_arg.pwd = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--connstr") == 0) {
      ++i;
      if (i>=argc) break;
      conn_arg.connstr = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--non-taos") == 0) {
      conn_arg.non_taos = 1;
      continue;
    }
  }

  int r = 0;

  if (conn_arg.connstr) {
    r = _driver_connect(hconn, conn_arg.connstr);
  } else if (conn_arg.dsn) {
    r = _connect(hconn, conn_arg.dsn, conn_arg.uid, conn_arg.pwd);
  } else {
    r = _connect(hconn, "TAOS_ODBC_DSN", NULL, NULL);
  }

  if (r) return r;

  check_driver(hconn);

  if (_under_taos_mysql_sqlite3 == 0) {
    r = _exec_and_check(hconn, (char*)-1, 0, "show apps", 0, 0);
  }

  r = test_cases(hconn);

  CALL_SQLDisconnect(hconn);

  return r;
}

static int test_env(int argc, char *argv[], SQLHANDLE henv)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hconn = SQL_NULL_HANDLE;
  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, henv, &hconn);
  if (FAILED(sr)) return -1;

  r = test_conn(argc, argv, hconn);

  CALL_SQLFreeHandle(SQL_HANDLE_DBC, hconn);

  return r;
}

static int test(int argc, char *argv[])
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE henv  = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  if (FAILED(sr)) return 1;

  do {
    sr = CALL_SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (FAILED(sr)) { r = -1; break; }

    r = test_env(argc, argv, henv);
    if (r) break;
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_ENV, henv);

  return r;
}

int main(int argc, char *argv[])
{
  int r = 0;
  r = test(argc, argv);
  fprintf(stderr, "==%s==\n", r ? "failure" : "success");
  return !!r;
}

