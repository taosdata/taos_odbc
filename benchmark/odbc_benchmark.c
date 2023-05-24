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

#ifdef _WIN32           /* { */
#include <windows.h>
#endif                  /* } */

#include <stdint.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <sql.h>
#include <sqlext.h>

#ifdef _WIN32           /* { */
static int gettimeofday(struct timeval *tp, void *tzp);
#else                   /* }{ */
#include <sys/time.h>
#endif                  /* } */


#define E(fmt, ...) fprintf(stderr, "@%d:%s():" fmt "\n", __LINE__, __func__, ##__VA_ARGS__)
#define SFREE(x) if (x) { free(x); x = NULL; }

#ifdef _WIN32           /* { */
static int gettimeofday(struct timeval *tp, void *tzp)
{
  static const uint64_t EPOCH = ((uint64_t) 116444736000000000ULL);
  LARGE_INTEGER li;
  FILETIME ft;
  GetSystemTimeAsFileTime(&ft);

  li.QuadPart   = ft.dwHighDateTime;
  li.QuadPart <<= 32;
  li.QuadPart  += ft.dwLowDateTime;
  li.QuadPart  -= EPOCH;

  tp->tv_sec    = (long)(li.QuadPart / 10000000);
  tp->tv_usec   = (li.QuadPart % 10000000) / 10;

  return 0;
}
#endif                  /* } */

static int _timestamp_prepare(void **data, SQLLEN **pn, size_t rows, size_t i_col)
{
  int64_t *p64 = (int64_t*)calloc(rows, sizeof(*p64));
  if (!p64) {
    E("oom");
    return -1;
  }
  data[i_col] = p64;

  SQLLEN *p = (SQLLEN*)calloc(rows, sizeof(*p));
  if (!p) {
    E("oom");
    return -1;
  }
  pn[i_col] = p;

  time_t t0; time(&t0);
  int64_t v = t0 * 1000;

  for (size_t i=0; i<rows; ++i) {
    p64[i] = v + i;
    p[i] = sizeof(p64[i]);
    // E("timestamp:[%zd]", p64[i]);
  }

  return 0;
}

static int _bigint_prepare(void **data, SQLLEN **pn, size_t rows, size_t i_col)
{
  int64_t *p64 = (int64_t*)calloc(rows, sizeof(*p64));
  if (!p64) {
    E("oom");
    return -1;
  }
  data[i_col] = p64;

  SQLLEN *p = (SQLLEN*)calloc(rows, sizeof(*p));
  if (!p) {
    E("oom");
    return -1;
  }
  pn[i_col] = p;

  for (size_t i=0; i<rows; ++i) {
    p64[i] = rand();
    p[i] = sizeof(p64[i]);
    // E("bigint:[%zd]", p64[i]);
  }

  return 0;
}

static int _char_prepare(void **data, SQLLEN **pn, size_t rows, size_t i_col, size_t len)
{
  char *ps = (char*)calloc(rows, len + 1);
  if (!ps) {
    E("oom");
    return -1;
  }
  data[i_col] = ps;

  SQLLEN *p = (SQLLEN*)calloc(rows, sizeof(*p));
  if (!p) {
    E("oom");
    return -1;
  }
  pn[i_col] = p;

  for (size_t i=0; i<rows; ++i) {
    int v = rand();
    int n = snprintf(ps + i * (len + 1), len + 1, "%0*d", (int)len, v);
    p[i] = n;
    // E("char:[%s]", ps + i * (len + 1));
  }

  return 0;
}

static int _prepare_data_v(SQLHANDLE hstmt, size_t rows, size_t cols,
    SQLUSMALLINT **ParamStatusArray, SQLLEN ***StrLen_or_IndPtr, SQLULEN *ParamsProcessed, void ***data, va_list ap)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  int len = 0;

  void **p = (void**)calloc(cols, sizeof(*p));
  if (!p) {
    E("oom");
    return -1;
  }
  *data = p;

  SQLUSMALLINT *pa = (SQLUSMALLINT*)calloc(rows, sizeof(*pa));
  if (!pa) {
    E("oom");
    return -1;
  }
  *ParamStatusArray = pa;

  SQLLEN **pn = (SQLLEN**)calloc(cols, sizeof(*pn));
  if (!pn) {
    E("oom");
    return -1;
  }
  *StrLen_or_IndPtr = pn;

  // Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
  // column-wise binding.
  SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);

  // Specify the number of elements in each parameter array.
  SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)(uintptr_t)(SQLULEN)rows, 0);

  // Specify an array in which to return the status of each set of
  // parameters.
  E("pa:%p", pa);
  SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_STATUS_PTR, pa, 0);

  // Specify an SQLUINTEGER value in which to return the number of sets of
  // parameters processed.
  SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, ParamsProcessed, 0);

  for (size_t i=0; i<cols; ++i) {
    SQLSMALLINT     sqlc_type       = va_arg(ap, int);
    SQLSMALLINT     sql_type        = 0;
    SQLUSMALLINT    ColumnSize      = 0;
    SQLSMALLINT     DecimalDigits   = 0;
    SQLLEN          BufferLength    = 0;
    switch (sqlc_type) {
      case SQL_C_SBIGINT:
        if (i == 0) r = _timestamp_prepare(p, pn, rows, i);
        else        r = _bigint_prepare(p, pn, rows, i);
        if (r) return -1;
        sql_type = SQL_BIGINT;
        break;
      case SQL_C_CHAR:
        len = va_arg(ap, int);
        r = _char_prepare(p, pn, rows, i, len);
        if (r) return -1;
        sql_type = SQL_VARCHAR;
        ColumnSize = len;
        BufferLength = len + 1;
        break;
      default:
        E("SQL_C_xxx[%d/0x%x] not implemented yet", sqlc_type, sqlc_type);
        return -1;
    }

    sr = SQLBindParameter(hstmt, (SQLUSMALLINT)(i+1), SQL_PARAM_INPUT, sqlc_type, sql_type, ColumnSize, DecimalDigits, p[i], BufferLength, pn[i]);
    if (sr != SQL_SUCCESS) return -1;
  }

  return 0;
}

static int _prepare_and_run(SQLHANDLE hstmt, const char *sql)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = SQLPrepare(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  sr = SQLExecute(hstmt);
  if (sr != SQL_SUCCESS) return -1;

  return 0;
}

static int _run_prepare(SQLHANDLE hstmt, const char *sql, size_t rows, size_t cols, void ***data, ...)
{
  int r = 0;

  SQLUSMALLINT *ParamStatusArray = NULL;
  SQLULEN ParamsProcessed = 0;
  SQLLEN **StrLen_or_IndPtr = NULL;

  va_list ap;
  va_start(ap, data);
  r = _prepare_data_v(hstmt, rows, cols, &ParamStatusArray, &StrLen_or_IndPtr, &ParamsProcessed, data, ap);
  va_end(ap);

  if (r) return -1;

  struct timeval tv0 = {0};
  struct timeval tv1 = {0};
  gettimeofday(&tv0, NULL);
  r = _prepare_and_run(hstmt, sql);
  gettimeofday(&tv1, NULL);

  double diff = difftime(tv1.tv_sec, tv0.tv_sec);
  diff += ((double)(tv1.tv_usec - tv0.tv_usec)) / 1000000;

  E("run_with_params(%s), with %zd rows / %zd cols:", sql, rows, cols);
  E("elapsed: %lfsecs", diff);
  E("throughput: %lf rows/secs", rows / diff);

  SFREE(ParamStatusArray);
  for (size_t i=0; i<cols; ++i) {
    SFREE(StrLen_or_IndPtr[i]);
  }
  SFREE(StrLen_or_IndPtr);

  return r ? -1 : 0;
}

static void usage(const char *arg0)
{
  fprintf(stderr, "%s -h\n"
                  "  show this help page\n"
                  "%s --dsn <ip> --usr <usr> --pwd <pwd>\n"
                  "  running benchmark\n"
                  "%s --conn <conn>\n"
                  "  running benchmark\n",
                  arg0, arg0, arg0);
}

typedef struct odbc_conn_cfg_s               odbc_conn_cfg_t;
struct odbc_conn_cfg_s {
  const char                    *dsn;
  const char                    *usr;
  const char                    *pwd;
  const char                    *conn;
};

static int _run(odbc_conn_cfg_t *cfg, SQLHANDLE *penv, SQLHANDLE *pdbc, SQLHANDLE *pstmt)
{
  (void)pstmt;

  SQLRETURN sr = SQL_SUCCESS;

  sr = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, penv);
  if (sr != SQL_SUCCESS) return -1;

  SQLHANDLE henv = *penv;

  sr = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  if (sr != SQL_SUCCESS) return -1;

  sr = SQLAllocHandle(SQL_HANDLE_DBC, henv, pdbc);
  if (sr != SQL_SUCCESS) return -1;

  SQLHANDLE hdbc = *pdbc;

  if (cfg->dsn) {
    sr = SQLConnect(hdbc, (SQLCHAR*)cfg->dsn, SQL_NTS, (SQLCHAR*)cfg->usr, SQL_NTS, (SQLCHAR*)cfg->pwd, SQL_NTS);
    if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;
  } else {
    char buf[1024];
    buf[0] = '\0';
    SQLSMALLINT StringLength = 0;
    sr = SQLDriverConnect(hdbc, NULL, (SQLCHAR*)cfg->conn, SQL_NTS, (SQLCHAR*)buf, sizeof(buf), &StringLength, SQL_DRIVER_COMPLETE);
    if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;
  }

  sr = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, pstmt);
  if (sr != SQL_SUCCESS) return -1;

  SQLHANDLE hstmt = *pstmt;

  const char *sqls =
    "create database if not exists bar;"
    "use bar;"
    "drop table if exists benchmark_case0;"
    "create table benchmark_case0 (ts timestamp, name varchar(20));";

  sr = SQLExecDirect(hstmt, (SQLCHAR*)sqls, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;
  while (1) {
    SQLLEN RowCount = 0;
    sr = SQLRowCount(hstmt, &RowCount);
    if (sr != SQL_SUCCESS) return -1;

    sr = SQLFetch(hstmt);
    if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO && sr != SQL_NO_DATA) return -1;

    sr = SQLMoreResults(hstmt);
    if (sr == SQL_NO_DATA) break;
    if (sr != SQL_SUCCESS) return -1;
  }

  const char *sql = "insert into benchmark_case0 (ts, name) values (?, ?)";
  const size_t nr_cols = 2;
  const size_t nr_rows = INT16_MAX; // 32767

  void **data = NULL;

  int r = _run_prepare(hstmt, sql, nr_rows, nr_cols, &data, SQL_C_SBIGINT, SQL_C_CHAR, 20);

  if (data) {
    for (size_t i=0; i<nr_cols; ++i) {
      SFREE(data[i]);
    }
    SFREE(data);
  }

  return r ? -1 : 0;
}

int main(int argc, char *argv[])
{
  odbc_conn_cfg_t cfg = {0};
  for (int i=1; i<argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "-h") == 0) {
      usage(argv[0]);
      return 0;
    }
    if (strcmp(arg, "--dsn") == 0) {
      ++i;
      if (i>=argc) {
        E("<dsn> is expected after `--dsn`, but got ==null==");
        return -1;
      }
      cfg.dsn = argv[i];
      if (cfg.conn) {
        E("--conn <conn> has been set");
        return -1;
      }
      continue;
    }
    if (strcmp(arg, "--usr") == 0) {
      ++i;
      if (i>=argc) {
        E("<usr> is expected after `--usr`, but got ==null==");
        return -1;
      }
      cfg.usr = argv[i];
      if (!cfg.dsn) {
        E("`--dsn <dsn>` is expected, but got ==null==");
        return -1;
      }
      continue;
    }
    if (strcmp(arg, "--pwd") == 0) {
      ++i;
      if (i>=argc) {
        E("<pwd> is expected after `--pwd`, but got ==null==");
        return -1;
      }
      cfg.pwd = argv[i];
      if (!cfg.dsn) {
        E("`--dsn <dsn>` is expected, but got ==null==");
        return -1;
      }
      continue;
    }
    if (strcmp(arg, "--conn") == 0) {
      ++i;
      if (i>=argc) {
        E("<conn> is expected after `--conn`, but got ==null==");
        return -1;
      }
      cfg.conn = argv[i];
      if (cfg.dsn) {
        E("--dsn <dsn> has been set");
        return -1;
      }
      continue;
    }
  }

  int r = 0;

#ifdef _WIN32                    /* { */
  srand((unsigned int)time(NULL));
#else                            /* }{ */
  srand(time(NULL));
#endif                           /* } */


  if (cfg.conn == NULL && cfg.dsn == NULL) {
    cfg.dsn = "TAOS_ODBC_DSN";
  }

  SQLHANDLE henv = SQL_NULL_HANDLE, hdbc = SQL_NULL_HANDLE, hstmt = SQL_NULL_HANDLE;

  r = _run(&cfg, &henv, &hdbc, &hstmt);

  if (hstmt != SQL_NULL_HANDLE) {
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    hstmt = SQL_NULL_HANDLE;
  }

  if (hdbc != SQL_NULL_HANDLE) {
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    hdbc = SQL_NULL_HANDLE;
  }

  if (henv != SQL_NULL_HANDLE) {
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
    henv = SQL_NULL_HANDLE;
  }

  return r ? 1 : 0;
}

