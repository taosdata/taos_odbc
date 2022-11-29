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


static int _under_taos = 1;

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
  SQLUINTEGER nr_rows;
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

static int test_cases(SQLHANDLE hconn)
{
  int r = 0;

  _exec_direct(hconn, "create database if not exists foo");
  _exec_direct(hconn, "use foo");

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

  return r;
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

  r = _exec_and_check(hconn, (char*)-1, 0, "show apps", 0, 0);
  if (r) _under_taos = 0;

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
  D("==%s==", r ? "failure" : "success");
  return !!r;
}

