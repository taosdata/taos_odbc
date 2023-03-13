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

#include "test_config.h"
#include "odbc_helpers.h"

#include "../test_helper.h"

#include <stdarg.h>
#include <stdint.h>

#define TAOS_ODBC              0x01
#define SQLITE3_ODBC           0x02
#define MYSQL_ODBC             0x04

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

static int cmp_real_against_val(SQLHANDLE hstmt, SQLSMALLINT iColumn, const cJSON *val)
{
  SQLRETURN sr = SQL_SUCCESS;

  SQLREAL v = 0;
  SQLLEN StrLen_or_Ind = 0;

  sr = CALL_SQLGetData(hstmt, iColumn+1, SQL_C_FLOAT, &v, sizeof(v), &StrLen_or_Ind);
  if (FAILED(sr)) return -1;
  if (StrLen_or_Ind == SQL_NO_TOTAL) {
    E("not implemented yet");
    return -1;
  }
  if (StrLen_or_Ind == SQL_NULL_DATA) {
    E("not implemented yet");
    return -1;
  }

  cJSON *j = cJSON_CreateNumber(v);
  bool eq = cJSON_Compare(j, val, true);
  cJSON_Delete(j);
  if (eq) return 0;

  if (cJSON_IsNumber(val)) {
    double dl = v;
    double dr = val->valuedouble;
    char lbuf[64]; snprintf(lbuf, sizeof(lbuf), "%lg", dl);
    char rbuf[64]; snprintf(rbuf, sizeof(rbuf), "%lg", dr);
    if (strcmp(lbuf, rbuf) == 0) return 0;
    else D("%s <> %s", lbuf, rbuf);
  } else {
    char *t1 = cJSON_PrintUnformatted(j);
    char *t2 = cJSON_PrintUnformatted(val);
    E("==%s== <> ==%s==", t1, t2);
    free(t1);
    free(t2);
  }

  return -1;
}

static int cmp_double_against_val(SQLHANDLE hstmt, SQLSMALLINT iColumn, const cJSON *val)
{
  SQLRETURN sr = SQL_SUCCESS;

  // https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/c-data-types?view=sql-server-ver16
  // TODO: SQLDOUBLE or SQLFLOAT?
  SQLDOUBLE v = 0;
  SQLLEN StrLen_or_Ind = 0;

  sr = CALL_SQLGetData(hstmt, iColumn+1, SQL_C_DOUBLE, &v, sizeof(v), &StrLen_or_Ind);
  if (FAILED(sr)) return -1;
  if (StrLen_or_Ind == SQL_NO_TOTAL) {
    E("not implemented yet");
    return -1;
  }
  if (StrLen_or_Ind == SQL_NULL_DATA) {
    E("not implemented yet");
    return -1;
  }

  cJSON *j = cJSON_CreateNumber(v);
  bool eq = cJSON_Compare(j, val, true);
  cJSON_Delete(j);
  if (eq) return 0;

  if (cJSON_IsNumber(val)) {
    double dl = v;
    double dr = val->valuedouble;
    char lbuf[64]; snprintf(lbuf, sizeof(lbuf), "%lg", dl);
    char rbuf[64]; snprintf(rbuf, sizeof(rbuf), "%lg", dr);
    if (strcmp(lbuf, rbuf) == 0) return 0;
    else D("%s <> %s", lbuf, rbuf);
  } else {
    char *t1 = cJSON_PrintUnformatted(j);
    char *t2 = cJSON_PrintUnformatted(val);
    E("==%s== <> ==%s==", t1, t2);
    free(t1);
    free(t2);
  }

  return -1;
}

static int cmp_i64_against_val(SQLHANDLE hstmt, SQLSMALLINT iColumn, const cJSON *val)
{
  SQLRETURN sr = SQL_SUCCESS;

  SQLINTEGER v = 0;
  SQLLEN StrLen_or_Ind = 0;

  sr = CALL_SQLGetData(hstmt, iColumn+1, SQL_C_SLONG, &v, sizeof(v), &StrLen_or_Ind);
  if (FAILED(sr)) return -1;
  if (StrLen_or_Ind == SQL_NO_TOTAL) {
    E("not implemented yet");
    return -1;
  }
  if (StrLen_or_Ind == SQL_NULL_DATA) {
    E("not implemented yet");
    return -1;
  }

  cJSON *j = cJSON_CreateNumber(v);
  bool eq = cJSON_Compare(j, val, true);
  cJSON_Delete(j);
  if (eq) return 0;

  if (cJSON_IsNumber(val)) {
    double dl = v;
    double dr = val->valuedouble;
    char lbuf[64]; snprintf(lbuf, sizeof(lbuf), "%lg", dl);
    char rbuf[64]; snprintf(rbuf, sizeof(rbuf), "%lg", dr);
    if (strcmp(lbuf, rbuf) == 0) return 0;
    else D("%s <> %s", lbuf, rbuf);
  } else {
    char *t1 = cJSON_PrintUnformatted(j);
    char *t2 = cJSON_PrintUnformatted(val);
    E("==%s== <> ==%s==", t1, t2);
    free(t1);
    free(t2);
  }

  return -1;
}

static int cmp_i32_against_val(SQLHANDLE hstmt, SQLSMALLINT iColumn, const cJSON *val)
{
  SQLRETURN sr = SQL_SUCCESS;

  SQLINTEGER v = 0;
  SQLLEN StrLen_or_Ind = 0;

  sr = CALL_SQLGetData(hstmt, iColumn+1, SQL_C_SLONG, &v, sizeof(v), &StrLen_or_Ind);
  if (FAILED(sr)) return -1;
  if (StrLen_or_Ind == SQL_NO_TOTAL) {
    E("not implemented yet");
    return -1;
  }
  if (StrLen_or_Ind == SQL_NULL_DATA) {
    E("not implemented yet");
    return -1;
  }

  cJSON *j = cJSON_CreateNumber(v);
  bool eq = cJSON_Compare(j, val, true);
  cJSON_Delete(j);
  if (eq) return 0;

  if (cJSON_IsNumber(val)) {
    double dl = v;
    double dr = val->valuedouble;
    char lbuf[64]; snprintf(lbuf, sizeof(lbuf), "%lg", dl);
    char rbuf[64]; snprintf(rbuf, sizeof(rbuf), "%lg", dr);
    if (strcmp(lbuf, rbuf) == 0) return 0;
    else D("%s <> %s", lbuf, rbuf);
  } else {
    char *t1 = cJSON_PrintUnformatted(j);
    char *t2 = cJSON_PrintUnformatted(val);
    E("==%s== <> ==%s==", t1, t2);
    free(t1);
    free(t2);
  }

  return -1;
}

static int cmp_timestamp_against_val(SQLHANDLE hstmt, SQLSMALLINT iColumn, const cJSON *val)
{
  SQLRETURN sr = SQL_SUCCESS;

  SQLBIGINT v = 0;
  SQLLEN StrLen_or_Ind = 0;

  sr = CALL_SQLGetData(hstmt, iColumn+1, SQL_C_SBIGINT, &v, sizeof(v), &StrLen_or_Ind);
  if (FAILED(sr)) return -1;
  if (StrLen_or_Ind == SQL_NO_TOTAL) {
    E("not implemented yet");
    return -1;
  }
  if (StrLen_or_Ind == SQL_NULL_DATA) {
    E("not implemented yet");
    return -1;
  }

  cJSON *j = cJSON_CreateNumber((double)v);
  bool eq = cJSON_Compare(j, val, true);
  cJSON_Delete(j);
  if (eq) return 0;

  if (cJSON_IsNumber(val)) {
    double dl = (double)v;
    double dr = val->valuedouble;
    char lbuf[64]; snprintf(lbuf, sizeof(lbuf), "%lg", dl);
    char rbuf[64]; snprintf(rbuf, sizeof(rbuf), "%lg", dr);
    if (strcmp(lbuf, rbuf) == 0) return 0;
    else D("%s <> %s", lbuf, rbuf);
  } else {
    char *t1 = cJSON_PrintUnformatted(j);
    char *t2 = cJSON_PrintUnformatted(val);
    E("==%s== <> ==%s==", t1, t2);
    free(t1);
    free(t2);
  }

  return -1;
}

static int cmp_varchar_against_val(SQLHANDLE hstmt, SQLSMALLINT iColumn, SQLULEN ColumnSize, const cJSON *val)
{
  SQLRETURN sr = SQL_SUCCESS;

  SQLCHAR buf[1024]; buf[0] = '\0';
  SQLLEN StrLen_or_Ind = 0;
  if (sizeof(buf) <= ColumnSize) {
    E("buffer is too small to hold data as large as [%zd]", (size_t)ColumnSize);
    return -1;
  }

  sr = CALL_SQLGetData(hstmt, iColumn+1, SQL_C_CHAR, buf, sizeof(buf), &StrLen_or_Ind);
  if (FAILED(sr)) return -1;
  if (StrLen_or_Ind == SQL_NO_TOTAL) {
    E("not implemented yet");
    return -1;
  }
  if (StrLen_or_Ind == SQL_NULL_DATA) {
    if (cJSON_IsNull(val)) return 0;
    char *t1 = cJSON_PrintUnformatted(val);
    E("differ: null <> %s", t1);
    free(t1);
    return -1;
  }
  if ((size_t)StrLen_or_Ind >= sizeof(buf)) {
    E("not implemented yet");
    return -1;
  }
  buf[StrLen_or_Ind] = '\0';

  cJSON *j = cJSON_CreateString((const char*)buf);
  bool eq = cJSON_Compare(j, val, true);
  if (!eq) {
    char *t1 = cJSON_PrintUnformatted(j);
    char *t2 = cJSON_PrintUnformatted(val);
    E("==%s== <> ==%s==", t1, t2);
    free(t1);
    free(t2);
  }
  cJSON_Delete(j);
  if (!eq) return -1;

  return 0;
}

static int cmp_wvarchar_against_val(SQLHANDLE hstmt, SQLSMALLINT iColumn, SQLULEN ColumnSize, const cJSON *val)
{
  SQLRETURN sr = SQL_SUCCESS;

  SQLCHAR buf[1024]; buf[0] = '\0';
  SQLLEN StrLen_or_Ind = 0;
  if (sizeof(buf) <= ColumnSize) {
    E("buffer is too small to hold data as large as [%zd]", (size_t)ColumnSize);
    return -1;
  }

  // TODO: SQL_C_WCHAR to be implemented
  sr = CALL_SQLGetData(hstmt, iColumn+1, SQL_C_CHAR, buf, sizeof(buf), &StrLen_or_Ind);
  if (FAILED(sr)) return -1;
  if (StrLen_or_Ind == SQL_NO_TOTAL) {
    E("not implemented yet");
    return -1;
  }
  if (StrLen_or_Ind == SQL_NULL_DATA) {
    if (cJSON_IsNull(val)) return 0;
    char *t1 = cJSON_PrintUnformatted(val);
    E("differ: null <> %s", t1);
    free(t1);
    return -1;
  }
  if ((size_t)StrLen_or_Ind >= sizeof(buf)) {
    E("not implemented yet");
    return -1;
  }
  buf[StrLen_or_Ind] = '\0';

  cJSON *j = cJSON_CreateString((const char*)buf);
  bool eq = cJSON_Compare(j, val, true);
  cJSON_Delete(j);
  if (eq) return 0;

  return -1;
}

static int record_cmp_row(SQLHANDLE hstmt, SQLSMALLINT ColumnCount, const cJSON *row)
{
  int r = 0;

  SQLRETURN sr = SQL_SUCCESS;

  if (!cJSON_IsArray(row)) {
    char *t1 = cJSON_PrintUnformatted(row);
    E("json array is required but got ==%s==", t1);
    free(t1);
    return -1;
  }

  for (int i=0; i<ColumnCount; ++i) {
    const cJSON *val = cJSON_GetArrayItem(row, i);
    if (!val) {
      E("col #%d: lack of corresponding data", i+1);
      return -1;
    }

    SQLCHAR ColumnName[1024]; ColumnName[0] = '\0';
    SQLSMALLINT NameLength = 0;
    SQLSMALLINT DataType = 0;
    SQLULEN ColumnSize = 0;
    SQLSMALLINT DecimalDigits = 0;
    SQLSMALLINT Nullable = 0;
    sr = CALL_SQLDescribeCol(hstmt, i+1, ColumnName, sizeof(ColumnName), &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
    if (FAILED(sr)) return -1;

    switch (DataType) {
      case SQL_REAL:
        r = cmp_real_against_val(hstmt, i, val);
        if (r) return -1;
        break;
      case SQL_DOUBLE:
        r = cmp_double_against_val(hstmt, i, val);
        if (r) return -1;
        break;
      case SQL_INTEGER:
        r = cmp_i32_against_val(hstmt, i, val);
        if (r) return -1;
        break;
      case SQL_BIGINT:
        r = cmp_i64_against_val(hstmt, i, val);
        if (r) return -1;
        break;
      case SQL_TYPE_TIMESTAMP:
        r = cmp_timestamp_against_val(hstmt, i, val);
        if (r) return -1;
        break;
      case SQL_VARCHAR:
        r = cmp_varchar_against_val(hstmt, i, ColumnSize, val);
        if (r) return -1;
        break;
      case SQL_WVARCHAR:
        r = cmp_wvarchar_against_val(hstmt, i, ColumnSize, val);
        if (r) return -1;
        break;
      default:
        E("[%s] not implemented yet", sql_data_type(DataType));
        return -1;
    }
  }

  return 0;
}

static int res_cmp_rows(SQLSMALLINT ColumnCount, SQLHANDLE hstmt, const cJSON *rows)
{
  int r = 0;

  SQLRETURN sr = SQL_SUCCESS;

  if (!rows) return 0;

  if (!ColumnCount) {
    E("rows not required but provided");
    return -1;
  }

  if (!cJSON_IsArray(rows)) {
    char *t1 = cJSON_PrintUnformatted(rows);
    E("json array is required but got ==%s==", t1);
    free(t1);
    return -1;
  }

  int nr_rows = cJSON_GetArraySize(rows);

  for (int i=0; i<nr_rows; ++i) {
    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_NO_DATA) {
      E("end of record but rows still available");
      return -1;
    }

    const cJSON *row = cJSON_GetArrayItem(rows, i);
    r = record_cmp_row(hstmt, ColumnCount, row);
    if (r) return -1;
  }

  sr = CALL_SQLFetch(hstmt);
  if (sr == SQL_NO_DATA) return 0;

  E("row expected but got NONE");

  return -1;
}


static int test_query_cjson(SQLHANDLE hconn, const char *sql, const cJSON *rows)
{
  int r = 0;

  SQLRETURN sr = SQL_SUCCESS;
  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (FAILED(sr)) return -1;

  do {
    r = -1;
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (FAILED(sr)) break;

    SQLSMALLINT ColumnCount;
    sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
    if (FAILED(sr)) break;

    r = res_cmp_rows(ColumnCount, hstmt, rows);
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return r;
}

static int run_sql_rs(SQLHANDLE hconn, const char *sql, cJSON *rs, int icase, int positive)
{
  int r = 0;

  LOG_CALL("%s case[#%d] [sql:%s]", positive ? "positive" : "negative", icase+1, sql);
  r = test_query_cjson(hconn, sql, rs);

  r = !(!r ^ !positive);
  LOG_FINI(r, "%s case[#%d] [sql:%s]", positive ? "positive" : "negative", icase+1, sql);

  return r;
}

typedef struct params_arrays_s                params_arrays_t;
struct params_arrays_s {
  void                      **arrays;
  SQLLEN                    **strlen_or_inds;

  size_t                      cap;
  size_t                      nr;
};

static void params_arrays_reset(params_arrays_t *arrays)
{
  for (size_t i=0; i<arrays->nr; ++i) {
    free(arrays->arrays[i]);
    arrays->arrays[i] = NULL;
    free(arrays->strlen_or_inds[i]);
    arrays->strlen_or_inds[i] = NULL;
  }
  arrays->nr  = 0;
}

static void params_arrays_release(params_arrays_t *arrays)
{
  params_arrays_reset(arrays);

  free(arrays->arrays);
  free(arrays->strlen_or_inds);
  arrays->cap = 0;
}

static int params_arrays_append(params_arrays_t *arrays, void *array, void *strlen_or_inds)
{
  if (arrays->nr == arrays->cap) {
    size_t cap  = (arrays->cap + 1 + 15) / 16 *16;
    void **p1   = (void **)realloc(arrays->arrays, cap * sizeof(*p1));
    SQLLEN **p2 = (SQLLEN**)realloc(arrays->strlen_or_inds, cap * sizeof(*p2));
    if (!p1 || !p2) {
      E("out of memory");
      free(p1); free(p2);
      return -1;
    }
    arrays->arrays = p1;
    arrays->strlen_or_inds = p2;
    arrays->cap    = cap;
  }

  arrays->arrays[arrays->nr]           = array;
  arrays->strlen_or_inds[arrays->nr++] = strlen_or_inds;

  return 0;
}


typedef struct executes_ctx_s                executes_ctx_t;
struct executes_ctx_s {
  SQLHANDLE   hconn;
  const char *sql;
  SQLHANDLE   hstmt;


  params_arrays_t             params;

  SQLUSMALLINT               *param_status_array;
  SQLULEN                     params_processed;

  // buffers_t   buffers;

  // const char         *subtbl;
  // TAOS_FIELD_E       *tags;
  // int                 nr_tags;
  // TAOS_FIELD_E       *cols;
  // int                 nr_cols;

  // unsigned int        subtbl_required:1;
  // unsigned int        tags_described:1;
  // unsigned int        cols_described:1;
};

static void executes_ctx_release_param_output(executes_ctx_t *ctx)
{
  if (ctx->param_status_array) {
    free(ctx->param_status_array);
    ctx->param_status_array = NULL;
  }

  ctx->params_processed = 0;
}

static void executes_ctx_release(executes_ctx_t *ctx)
{
  params_arrays_release(&ctx->params);
  executes_ctx_release_param_output(ctx);

  if (ctx->hstmt) {
    CALL_SQLFreeHandle(SQL_HANDLE_STMT, ctx->hstmt);
    ctx->hstmt = SQL_NULL_HANDLE;
  }
}

static int executes_ctx_prepare_stmt(executes_ctx_t *ctx)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  if (ctx->hstmt) return 0;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, ctx->hconn, &ctx->hstmt);
  if (FAILED(sr)) return -1;

  sr = CALL_SQLPrepare(ctx->hstmt, (SQLCHAR*)ctx->sql, SQL_NTS);
  if (FAILED(sr)) return -1;

  SQLSMALLINT ParameterCount = 0;
  sr = CALL_SQLNumParams(ctx->hstmt, &ParameterCount);
  if (FAILED(sr)) return -1;

  for (int i=0; i<ParameterCount; ++i) {
    SQLSMALLINT DataType          = 0;
    SQLULEN     ParameterSize     = 0;
    SQLSMALLINT DecimalDigits     = 0;
    SQLSMALLINT Nullable          = 0;
    sr = CALL_SQLDescribeParam(ctx->hstmt, i+1, &DataType, &ParameterSize, &DecimalDigits, &Nullable);
    if (FAILED(sr)) return -1;
  }

  return r;
}

typedef int (*conv_from_json_f)(cJSON *src, char *dst, int bytes, int *nr_required, SQLLEN *strlen_or_ind);

static int _conv_from_json_to_str(cJSON *src, char *dst, int bytes, int *nr_required, SQLLEN *strlen_or_ind)
{
  if (cJSON_IsString(src)) {
    const char *s = cJSON_GetStringValue(src);
    int n = snprintf(dst, bytes, "%s", s);
    if (n<0) {
      E("internal logic error");
      return -1;
    }
    *nr_required = n;
    *strlen_or_ind = SQL_NTS;
    return 0;
  }
  if (cJSON_IsNumber(src)) {
    double d = cJSON_GetNumberValue(src);
    int n = snprintf(dst, bytes, "%g", d);
    if (n<0) {
      E("internal logic error");
      return -1;
    }
    *nr_required = n;
    *strlen_or_ind = SQL_NTS;
    return 0;
  }
  if (cJSON_IsObject(src)) {
    cJSON *j;
    j = cJSON_GetObjectItem(src, "timestamp");
    if (j) {
      if (cJSON_IsNull(j)) {
        *nr_required = sizeof(double);
        *strlen_or_ind = SQL_NULL_DATA;
        return 0;
      }
      return _conv_from_json_to_str(j, dst, bytes, nr_required, strlen_or_ind);
    }
    j = cJSON_GetObjectItem(src, "str");
    if (j) {
      if (cJSON_IsNull(j)) {
        *nr_required = 0;
        *strlen_or_ind = SQL_NULL_DATA;
        return 0;
      }
      return _conv_from_json_to_str(j, dst, bytes, nr_required, strlen_or_ind);
    }
  }
  char *t1 = cJSON_PrintUnformatted(src);
  E("non-determined src type, ==%s==", t1);
  free(t1);
  return -1;
}

static int _conv_from_json_to_double(cJSON *src, char *dst, int bytes, int *nr_required, SQLLEN *strlen_or_ind)
{
  (void)bytes;
  if (cJSON_IsNumber(src)) {
    double d = cJSON_GetNumberValue(src);
    *(double*)dst = d;
    *nr_required = sizeof(double);
    *strlen_or_ind = sizeof(double);
    return 0;
  }
  if (cJSON_IsObject(src)) {
    cJSON *j;
    j = cJSON_GetObjectItem(src, "timestamp");
    if (j) {
      if (cJSON_IsNull(j)) {
        *nr_required = sizeof(double);
        *strlen_or_ind = SQL_NULL_DATA;
        return 0;
      }
      return _conv_from_json_to_double(j, dst, bytes, nr_required, strlen_or_ind);
    }
    j = cJSON_GetObjectItem(src, "str");
    if (j) {
      if (cJSON_IsNull(j)) {
        *nr_required = 0;
        *strlen_or_ind = SQL_NULL_DATA;
        return 0;
      }
      return _conv_from_json_to_str(j, dst, bytes, nr_required, strlen_or_ind);
    }
  }
  char *t1 = cJSON_PrintUnformatted(src);
  E("non-determined src type, ==%s==", t1);
  free(t1);
  return -1;
}

static int _json_sql_c_type(cJSON *json, SQLSMALLINT *ValueType, int *bytes, conv_from_json_f *conv)
{
  if (cJSON_IsString(json)) {
    *ValueType = SQL_C_CHAR;
    const char *s = cJSON_GetStringValue(json);
    *bytes = (int)strlen(s);
    if (conv) *conv = _conv_from_json_to_str;
    return 0;
  }
  if (cJSON_IsNumber(json)) {
    *ValueType = SQL_C_DOUBLE;
    *bytes = sizeof(double);
    if (conv) *conv = _conv_from_json_to_double;
    return 0;
  }
  if (cJSON_IsObject(json)) {
    cJSON *j;
    j = cJSON_GetObjectItem(json, "timestamp");
    if (j) {
      if (cJSON_IsNull(j)) {
        *ValueType = SQL_C_DOUBLE;
        *bytes = sizeof(double);
        if (conv) *conv = _conv_from_json_to_double;
        return 0;
      }
      return _json_sql_c_type(j, ValueType, bytes, conv);
    }
    j = cJSON_GetObjectItem(json, "str");
    if (j) {
      if (cJSON_IsNull(j)) {
        *ValueType = SQL_C_CHAR;
        *bytes = 0;
        if (conv) *conv = _conv_from_json_to_double;
        return 0;
      }
      return _json_sql_c_type(j, ValueType, bytes, conv);
    }
  }
  char *t1 = cJSON_PrintUnformatted(json);
  E("non-determined json type, ==%s==", t1);
  free(t1);
  return -1;
}

static int _prepare_col_array_append(executes_ctx_t *ctx, int rows, int bytes_per_row)
{
  int r = 0;
  char   *p1 = (char*)malloc(bytes_per_row * rows);
  SQLLEN *p2 = (SQLLEN*)malloc(sizeof(*p2) * rows);
  if (p1 && p2) {
    r = params_arrays_append(&ctx->params, p1, p2);
    if (r == 0) return 0;
  }

  free(p1); free(p2);
  E("not implemented yet");
  return -1;
}

static int _store_row_col(executes_ctx_t *ctx, int irow, int icol, int buffer_length, cJSON *row, conv_from_json_f conv)
{
  int r = 0;

  char *data = ctx->params.arrays[icol];
  data += buffer_length * irow;

  cJSON *v = cJSON_GetArrayItem(row, icol);
  if (cJSON_IsNull(v)) {
    ctx->params.strlen_or_inds[icol][irow] = SQL_NULL_DATA;
  } else {
    int nr_required = 0;

    r = conv(v, data, buffer_length, &nr_required, ctx->params.strlen_or_inds[icol] + irow);
    if (r) return -1;
  }

  return 0;
}

static int _run_execute_params_rs(executes_ctx_t *ctx, cJSON *params, cJSON *rs)
{
  (void)rs;

  int r = 0;

  SQLRETURN sr = SQL_SUCCESS;

  cJSON *r0 = cJSON_GetArrayItem(params, 0);
  if (!r0 || !cJSON_IsArray(r0)) {
    r0 = params;
  }

  int rows = 1;
  if (r0 != params) rows = cJSON_GetArraySize(params);

  int cols = cJSON_GetArraySize(r0);

  SQLSMALLINT ParameterCount = 0;
  sr = CALL_SQLNumParams(ctx->hstmt, &ParameterCount);
  if (FAILED(sr)) return -1;

  if (cols != ParameterCount) {
    E("# of parameters differs, %d <> %d", cols, ParameterCount);
    return -1;
  }

  ctx->param_status_array = (SQLUSMALLINT*)calloc(rows, sizeof(*ctx->param_status_array));
  if (!ctx->param_status_array) {
    E("out of memory");
    return -1;
  }

  // Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
  // column-wise binding.
  sr = CALL_SQLSetStmtAttr(ctx->hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);
  if (FAILED(sr)) return -1;

  // Specify the number of elements in each parameter array.
  sr = CALL_SQLSetStmtAttr(ctx->hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)(uintptr_t)rows, 0);
  if (FAILED(sr)) return -1;

  // Specify an array in which to return the status of each set of
  // parameters.
  sr = CALL_SQLSetStmtAttr(ctx->hstmt, SQL_ATTR_PARAM_STATUS_PTR, ctx->param_status_array, 0);
  if (FAILED(sr)) return -1;

  // Specify an SQLUINTEGER value in which to return the number of sets of
  // parameters processed.
  sr = CALL_SQLSetStmtAttr(ctx->hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &ctx->params_processed, 0);
  if (FAILED(sr)) return -1;

  for (int i=0; i<cols; ++i) {
    SQLSMALLINT DataType          = 0;
    SQLULEN     ParameterSize     = 0;
    SQLSMALLINT DecimalDigits     = 0;
    SQLSMALLINT Nullable          = 0;
    sr = CALL_SQLDescribeParam(ctx->hstmt, i+1, &DataType, &ParameterSize, &DecimalDigits, &Nullable);
    if (FAILED(sr)) return -1;

    SQLSMALLINT ValueType = SQL_C_DEFAULT;
    int bytes;
    cJSON *col = cJSON_GetArrayItem(r0, i);
    conv_from_json_f conv = NULL;
    r = _json_sql_c_type(col, &ValueType, &bytes, &conv);
    if (r) return -1;

    for (int j=1; j<rows; ++j) {
      cJSON *row = cJSON_GetArrayItem(params, j);
      if (!cJSON_IsArray(row)) {
        char *t1 = cJSON_PrintUnformatted(row);
        E("json array expected, but got ==%s==", t1);
        free(t1);
        return -1;
      }
      if (cJSON_GetArraySize(row) != cols) {
        char *t1 = cJSON_PrintUnformatted(row);
        E("# %d items of json array expected, but got ==%s==", cols, t1);
        free(t1);
        return -1;
      }

      cJSON *v = cJSON_GetArrayItem(row, i);
      SQLSMALLINT vt = ValueType;
      int n;
      r = _json_sql_c_type(v, &vt, &n, NULL);
      if (r) return -1;
      if (vt != ValueType) {
        char *t1 = cJSON_PrintUnformatted(v);
        E("[%s] item expected, but got ==%s==", sql_c_data_type(ValueType), t1);
        free(t1);
        return -1;
      }
      if (n > bytes) bytes = n;
    }

    int buffer_length = 0;

    switch (ValueType) {
      case SQL_C_CHAR:
        buffer_length = bytes + 1;
        r = _prepare_col_array_append(ctx, rows, buffer_length);
        if (r) return -1;
        break;
      case SQL_C_DOUBLE:
        buffer_length = sizeof(double);
        r = _prepare_col_array_append(ctx, rows, buffer_length);
        break;
      default:
        E("[%s] not implemented yet", sql_c_data_type(ValueType));
        return -1;
    }

    r = _store_row_col(ctx, 0, i, buffer_length, r0, conv);
    if (r) return -1;

    for (int j=1; j<rows; ++j) {
      cJSON *row = cJSON_GetArrayItem(params, j);
      r = _store_row_col(ctx, j, i, buffer_length, row, conv);
      if (r) return -1;
    }

    switch (ValueType) {
      case SQL_C_CHAR:
        sr = CALL_SQLBindParameter(ctx->hstmt, i+1, SQL_PARAM_INPUT, ValueType,
            DataType, ParameterSize, DecimalDigits, ctx->params.arrays[i], buffer_length, ctx->params.strlen_or_inds[i]);
        if (FAILED(sr)) return -1;
        break;
      case SQL_C_DOUBLE:
        sr = CALL_SQLBindParameter(ctx->hstmt, i+1, SQL_PARAM_INPUT, ValueType,
            DataType, ParameterSize, DecimalDigits, ctx->params.arrays[i], buffer_length, ctx->params.strlen_or_inds[i]);
        if (FAILED(sr)) return -1;
        break;
      default:
        E("[%s] not implemented yet", sql_c_data_type(ValueType));
        return -1;
    }
  }

  sr = CALL_SQLExecute(ctx->hstmt);
  if (FAILED(sr)) return -1;

  SQLSMALLINT ColumnCount;
  sr = CALL_SQLNumResultCols(ctx->hstmt, &ColumnCount);
  if (FAILED(sr)) return -1;

  r = res_cmp_rows(ColumnCount, ctx->hstmt, rs);
  if (r) return -1;
  if (FAILED(sr)) return -1;

  return 0;
}

static int run_execute_params_rs(executes_ctx_t *ctx, cJSON *params, cJSON *rs)
{
  int r = 0;

  params_arrays_reset(&ctx->params);
  executes_ctx_release_param_output(ctx);

  r = executes_ctx_prepare_stmt(ctx);
  if (r) return -1;

  if (!cJSON_IsArray(params)) {
    char *t1 = cJSON_PrintUnformatted(params);
    W("expect `params` to be json array but got ==%s==", t1);
    free(t1);
    return -1;
  }

  return _run_execute_params_rs(ctx, params, rs);
}

static int _run_executes_by_ctx(executes_ctx_t *ctx, cJSON *executes)
{
  int r = 0;

  do {
    int nr = cJSON_GetArraySize(executes);
    for (int i=0; i<nr; ++i) {
      cJSON *execute = cJSON_GetArrayItem(executes, i);

      int positive = 1; {
        cJSON *pj = NULL;
        json_get_by_path(execute, "positive", &pj);
        if (pj && !cJSON_IsTrue(pj)) positive = 0;
      }

      cJSON *params = NULL;
      cJSON *rs = NULL;
      json_get_by_path(execute, "params", &params);
      json_get_by_path(execute, "rs", &rs);

      LOG_CALL("%s case[#%d] [%s]", positive ? "positive" : "negative", i+1, ctx->sql);

      if (!params) {
        LOG_CALL("%s case[#%d] [%s]", positive ? "positive" : "negative", i+1, ctx->sql);
        r = test_query_cjson(ctx->hconn, ctx->sql, rs);
        LOG_FINI(r, "%s case[#%d] [%s]", positive ? "positive" : "negative", i+1, ctx->sql);
      } else {
        LOG_CALL("%s case[#%d]", positive ? "positive" : "negative", i+1);
        r = run_execute_params_rs(ctx, params, rs);
        LOG_FINI(r, "%s case[#%d]", positive ? "positive" : "negative", i+1);
      }

      r = !(!r ^ !positive);

      if (r) break;
    }
  } while (0);

  return r;
}

static int _run_executes(SQLHANDLE hconn, const char *sql, cJSON *executes)
{
  int r = 0;

  if (!cJSON_IsArray(executes)) {
    char *t1 = cJSON_PrintUnformatted(executes);
    W("expect `executes` to be json array but got ==%s==", t1);
    free(t1);
    return -1;
  }

  executes_ctx_t ctx = {0};
  ctx.hconn    = hconn;
  ctx.sql      = sql;

  r = _run_executes_by_ctx(&ctx, executes);

  executes_ctx_release(&ctx);

  return r;
}

static int run_executes(SQLHANDLE hconn, const char *sql, cJSON *executes, int icase, int positive)
{
  (void)hconn;
  (void)executes;

  int r = 0;

  LOG_CALL("%s case[#%d] [%s]", positive ? "positive" : "negative", icase+1, sql);
  r = _run_executes(hconn, sql, executes);

  r = !(!r ^ !positive);
  LOG_FINI(r, "%s case[#%d] [%s]", positive ? "positive" : "negative", icase+1, sql);

  return r;
}

static int run_sql(SQLHANDLE hconn, cJSON *jsql, int icase)
{
  int r = 0;

  if (cJSON_IsString(jsql)) {
    const char *sql = cJSON_GetStringValue(jsql);
    LOG_CALL("run_sql_rs(hconn:%p, sql:%s, rs:%p, icase:%d, positive:%d", hconn, sql, NULL, icase, 1);
    r = run_sql_rs(hconn, sql, NULL, icase, 1);
    LOG_FINI(r, "run_sql_rs(hconn:%p, sql:%s, rs:%p, icase:%d, positive:%d", hconn, sql, NULL, icase, 1);
  } else if (cJSON_IsObject(jsql)) {
    const char *sql = json_object_get_string(jsql, "sql");
    if (!sql) return 0;

    int positive = 1; {
      cJSON *pj = NULL;
      json_get_by_path(jsql, "positive", &pj);
      if (pj && !cJSON_IsTrue(pj)) positive = 0;
    }

    cJSON *executes = NULL;
    json_get_by_path(jsql, "executes", &executes);
    if (executes) {
      r = run_executes(hconn, sql, executes, icase, positive);
    } else {
      cJSON *rs = NULL;
      json_get_by_path(jsql, "rs", &rs);

      r = run_sql_rs(hconn, sql, rs, icase, positive);
    }
  } else {
    char *t1 = cJSON_PrintUnformatted(jsql);
    W("non-string-non-object, just ignore. ==%s==", t1);
    free(t1);
  }

  return r;
}

static int run_case_under_conn(SQLHANDLE hconn, cJSON *json)
{
  int r = 0;
  cJSON *sqls = json_object_get_array(json, "sqls");
  if (!sqls) return 0;
  for (int i=0; i>=0; ++i) {
    cJSON *sql = cJSON_GetArrayItem(sqls, i);
    if (!sql) break;
    r = run_sql(hconn, sql, i);
    if (r) break;
  }
  return r;
}

static int run_case(SQLHANDLE hconn, cJSON *json, conn_arg_t *conn_arg, int *bypassed)
{
  (void)hconn;

  *bypassed = 0;

  int r = 0;

  const char *dsn       = json_object_get_string(json, "conn/dsn");
  const char *uid       = json_object_get_string(json, "conn/uid");
  const char *pwd       = json_object_get_string(json, "conn/pwd");
  const char *connstr   = json_object_get_string(json, "conn/connstr");
  double v = 0;
  json_object_get_number(json, "conn/non_taos", &v);
  int non_taos = (int)v;

  if (!!conn_arg->non_taos != !!non_taos) {
    W("test case bypassed because of `non_taos` does not match");
    *bypassed = 1;
    return 0;
  }

  if (!dsn && !connstr) {
    dsn = conn_arg->dsn;
    uid = conn_arg->uid;
    pwd = conn_arg->pwd;
    connstr = conn_arg->connstr;
  }

  if (dsn) {
    r = _connect(hconn, dsn, uid, pwd);
    if (r) return -1;
  } else if (connstr) {
    r = _driver_connect(hconn, connstr);
    if (r) return -1;
  } else {
    char *t1 = cJSON_PrintUnformatted(json);
    W("lack conn/dsn or conn/driver in json, ==%s==", t1);
    free(t1);
    return -1;
  }

  r = run_case_under_conn(hconn, json);

  CALL_SQLDisconnect(hconn);

  return r;
}

static int run_json_file(SQLHANDLE hconn, cJSON *json, conn_arg_t *conn_arg)
{
  int r = 0;

  if (!cJSON_IsArray(json)) {
    char *t1 = cJSON_PrintUnformatted(json);
    W("json array is required but got ==%s==", t1);
    free(t1);
    return -1;
  }

  for (int i=0; i>=0; ++i) {
    cJSON *json_case = NULL;
    if (json_get_by_item(json, i, &json_case)) break;
    if (!json_case) break;
    int positive = 1; {
      cJSON *pj = NULL;
      json_get_by_path(json_case, "positive", &pj);
      if (pj && !cJSON_IsTrue(pj)) positive = 0;
    }

    LOG_CALL("%s case[#%d]", positive ? "positive" : "negative", i+1);

    int bypass = 0;
    r = run_case(hconn, json_case, conn_arg, &bypass);

    if (bypass) {
      LOG_FINI(r, "%s case[#%d]: bypassed", positive ? "positive" : "negative", i+1);
      r = 0;
    } else {
      r = !(!r ^ !positive);
      LOG_FINI(r, "%s case[#%d]", positive ? "positive" : "negative", i+1);
    }

    if (r) break;
  }

  return r;
}

static int try_and_run_file(SQLHANDLE hconn, const char *file, conn_arg_t *conn_arg)
{
  int r = 0;
  char buf[PATH_MAX+1];
  char *p = tod_basename(file, buf, sizeof(buf));
  if (!p) return -1;

  const char *fromcode = "UTF-8";
  const char *tocode = "UTF-8";
#ifdef _WIN32
  tocode = "GB18030";
#endif

  cJSON *json = load_json_file(file, NULL, 0, fromcode, tocode);
  if (!json) return -1;

  const char *base = p;

  LOG_CALL("case %s", base);
  r = run_json_file(hconn, json, conn_arg);
  LOG_FINI(r, "case %s", base);

  cJSON_Delete(json);
  return r;
}

static int try_and_run(SQLHANDLE hconn, cJSON *json_test_case, const char *path, conn_arg_t *conn_arg)
{
  const char *s = json_to_string(json_test_case);
  if (!s) {
    char *t1 = cJSON_PrintUnformatted(json_test_case);
    W("json_test_case string expected but got ==%s==", t1);
    free(t1);
    return -1;
  }

  char buf[PATH_MAX+1];
  int n = snprintf(buf, sizeof(buf), "%s/%s.json", path, s);
  if (n<0 || (size_t)n>=sizeof(buf)) {
    W("buffer too small:%d", n);
    return -1;
  }

  return try_and_run_file(hconn, buf, conn_arg);
}

static int load_and_run(SQLHANDLE hconn, const char *json_test_cases_file, conn_arg_t *conn_arg)
{
  int r = 0;

  const char *fromcode = "UTF-8";
  const char *tocode = "UTF-8";
#ifdef _WIN32
  tocode = "GB18030";
#endif

  char path[PATH_MAX+1];
  cJSON *json_test_cases = load_json_file(json_test_cases_file, path, sizeof(path), fromcode, tocode);
  if (!json_test_cases) return -1;

  do {
    if (!cJSON_IsArray(json_test_cases)) {
      char *t1 = cJSON_PrintUnformatted(json_test_cases);
      W("json_test_cases array expected but got ==%s==", t1);
      free(t1);
      r = -1;
      break;
    }
    cJSON *guess = cJSON_GetArrayItem(json_test_cases, 0);
    if (!cJSON_IsString(guess)) {
      r = try_and_run_file(hconn, json_test_cases_file, conn_arg);
      break;
    }
    for (int i=0; i>=0; ++i) {
      cJSON *json_test_case = NULL;
      if (json_get_by_item(json_test_cases, i, &json_test_case)) break;
      if (!json_test_case) break;
      r = try_and_run(hconn, json_test_case, path, conn_arg);
      if (r) break;
    }
  } while (0);

  cJSON_Delete(json_test_cases);

  return r;
}

static int process_by_args_conn(int argc, char *argv[], SQLHANDLE hconn)
{
  (void)argc;
  (void)argv;

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
  const char *json_test_cases_file = getenv("ODBC_TEST_CASES");
  if (!json_test_cases_file) {
    W("set environment `ODBC_TEST_CASES` to the test cases file");
    return -1;
  }

  LOG_CALL("load_and_run(%s)", json_test_cases_file);
  r = load_and_run(hconn, json_test_cases_file, &conn_arg);
  LOG_FINI(r, "load_and_run(%s)", json_test_cases_file);
  return r;
}

static int process_by_args_env(int argc, char *argv[], SQLHANDLE henv)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hconn  = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, henv, &hconn);
  if (FAILED(sr)) return -1;

  r = process_by_args_conn(argc, argv, hconn);

  CALL_SQLFreeHandle(SQL_HANDLE_DBC, hconn);

  return r;
}

static int test_exec_direct(SQLHANDLE hconn, const char *sql)
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

static int select_count(SQLHANDLE hconn, const char *sql, size_t *count)
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

static int test_case1(SQLHANDLE hconn)
{
  int r = 0;

  r = test_exec_direct(hconn, "drop table if exists t");
  if (r) return -1;

  r = test_exec_direct(hconn, "create table t (ts timestamp, bi bigint)");
  if (r) return -1;

  r = test_exec_direct(hconn, "insert into t (ts, bi) values ('2022-10-12 13:14:15', 34)");
  if (r) return -1;

  size_t count;
  r = select_count(hconn, "select * from t where ts = '2022-10-12 13:14:15'", &count);
  if (r) return -1;
  if (count != 1) {
    E("1 expected, but got ==%zd==", count);
    return -1;
  }

  r = select_count(hconn, "select * from t where bi = 34", &count);
  if (r) return -1;
  if (count != 1) {
    E("1 expected, but got ==%zd==", count);
    return -1;
  }

  return 0;
}

static int test_case2(SQLHANDLE hconn)
{
  int r = 0;

  r = test_exec_direct(hconn, "drop table if exists t");
  if (r) return -1;

  r = test_exec_direct(hconn, "create table t (ts timestamp, bi bigint)");
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

    r = test_exec_direct(hconn, sql);
    if (r) return -1;

    ++tt;
    localtime_r(&tt, &tm);
    ++bi;
  }

  size_t count;
  r = select_count(hconn, "select * from t where ts = '2022-10-12 13:14:15'", &count);
  if (r) return -1;
  if (count != 1) {
    E("1 expected, but got ==%zd==", count);
    return -1;
  }

  r = select_count(hconn, "select * from t", &count);
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

  r = test_exec_direct(hconn, "drop table if exists t");
  if (r) return -1;

  r = test_exec_direct(hconn, "create table t (ts timestamp, bi bigint)");
  if (r) return -1;

  const char *fmt = "%Y-%m-%d %H:%M:%S";
  const char *ts = "2022-10-12 13:14:15";
  int64_t bi = 34;
  struct tm tm = {0};
  tod_strptime(ts, fmt, &tm);
  time_t tt = mktime(&tm);

#define COUNT 20

  for (size_t i=0; i<COUNT; ++i) {
    char s[64];
    strftime(s, sizeof(s), fmt, &tm);

    char sql[128];
    snprintf(sql, sizeof(sql), "insert into t (ts, bi) values ('%s', %" PRId64 ")", s, bi);

    r = test_exec_direct(hconn, sql);
    if (r) return -1;

    ++tt;
    localtime_r(&tt, &tm);
    ++bi;
  }

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
      // D("nr_rows: %d, array_size: %zd", nr_rows, array_size);
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

  r = test_exec_direct(hconn, "drop table if exists t");
  if (r) return -1;

  r = test_exec_direct(hconn, "create table t (ts timestamp, bi bigint)");
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

    r = test_exec_direct(hconn, sql);
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

  r = test_exec_direct(hconn, "drop table if exists t");
  if (r) return -1;

  r = test_exec_direct(hconn, "create table t (ts timestamp, name varchar(1024))");
  if (r) return -1;

  const char *name = "helloworldfoobargreatwall";
  char sql[1024];
  snprintf(sql, sizeof(sql), "insert into t (ts, name) values ('2022-10-12 13:14:15', '%s')", name);

  r = test_exec_direct(hconn, sql);
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
    (void)count;
    // TODO: remove this restriction
    SQLLEN BufferLength = 4;
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

  r = test_exec_direct(hconn, "drop table if exists t");
  if (r) return -1;

  r = test_exec_direct(hconn, "create table t (ts timestamp, name varchar(1024))");
  if (r) return -1;

  const char *ts   = "2022-10-12 13:14:15";
  const char *name = "foo";
  char sql[1024];
  snprintf(sql, sizeof(sql), "insert into t (ts, name) values ('%s', '%s')", ts, name);

  r = test_exec_direct(hconn, sql);
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
    (void)count;
    // TODO: remove this restriction
    SQLLEN BufferLength = 4;
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

static int test_hard_coded(SQLHANDLE henv, const char *dsn, const char *uid, const char *pwd, const char *connstr, int non_taos)
{
  (void)non_taos;

  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hconn = SQL_NULL_HANDLE;
  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, henv, &hconn);
  if (FAILED(sr)) return -1;

  do {
    if (dsn) {
      r = _connect(hconn, dsn, uid, pwd);
      if (r) break;
    } else {
      r = _driver_connect(hconn, connstr);
      if (r) break;
    }

    do {
      test_exec_direct(hconn, "create database if not exists foo");
      test_exec_direct(hconn, "use foo");

      r = test_case1(hconn);
      if (r) break;

      r = test_case2(hconn);
      if (r) break;

      r = test_case3(hconn);
      if (r) break;

      r = test_case4(hconn, non_taos, 128, 113);
      if (r) break;

      if (0 && !non_taos) {
        r = test_case4(hconn, non_taos, 5000, 4000);
        if (r) break;
      }

      if (1) r = test_case5(hconn);
      if (r) break;

      r = test_case6(hconn);
      if (r) break;

      r = test_case7(hconn);
      if (r) break;
    } while (0);

    CALL_SQLDisconnect(hconn);
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_DBC, hconn);

  return (r || FAILED(sr)) ? -1 : 0;
}

static void _print_(const char *buf, size_t maxlen)
{
  fprintf(stderr, "0x");
  maxlen = maxlen - 1;
  for (size_t i=0; i < maxlen; ++i) {
    const unsigned char c1 = (const unsigned char)buf[i];
    if (c1 == 0) break;
    fprintf(stderr, "%02x", c1);
  }
  fprintf(stderr, "\n");
}

static SQLRETURN _test_case_get_char_partial(SQLHANDLE hstmt, const char *name)
{
  SQLRETURN sr = SQL_SUCCESS;
  // TODO: remove this restriction
  char buf[4];
  const char *p = name;

  while (1) {
    SQLLEN StrLen_or_Ind;
    memset(buf, 'X', sizeof(buf));
    sr = CALL_SQLGetData(hstmt, 1, SQL_C_CHAR, (SQLPOINTER)buf, sizeof(buf), &StrLen_or_Ind);
    if (sr == SQL_ERROR) return SQL_ERROR;
    if (sr == SQL_NO_DATA) return SQL_SUCCESS;
    if (sr == SQL_SUCCESS) {
      if (StrLen_or_Ind == SQL_NULL_DATA) {
        if (name) {
          E("failed:`%s` expected, but got null", name);
          return SQL_ERROR;
        }
        continue;
      }
      if (StrLen_or_Ind != SQL_NO_TOTAL && StrLen_or_Ind != 0) {
        _print_((const char*)buf, sizeof(buf));
        if (buf[StrLen_or_Ind]) {
          E("failed: `0x00` expected, but got `0x%02x`", (unsigned int)buf[StrLen_or_Ind]);
          return SQL_ERROR;
        }
        if (strcmp(buf, p) == 0) continue;
        E("failed: `%s` expected, but got `%s`", name, buf);
        return SQL_ERROR;
      }
      E("failed:sr[%s],StrLen_or_Ind:[%" PRId64 "/0x%" PRIu64 "]", sql_return_type(sr), (int64_t)StrLen_or_Ind, (uint64_t)StrLen_or_Ind);
      return SQL_ERROR;
    }
    if (sr == SQL_SUCCESS_WITH_INFO) {
      if (StrLen_or_Ind == SQL_NO_TOTAL || StrLen_or_Ind >= 0) {
        const size_t len = strlen(buf);
        if (len == 0) {
          E("failed:expected to be greater than 0, but got =%zd", len);
          return SQL_ERROR;
        }
        _print_((const char*)buf, sizeof(buf));
        if (strncmp(buf, p, len)) {
          E("failed: `%.*s` expected, but got `%s`", (int)len, p, buf);
          return SQL_ERROR;
        }
        p += len;
        continue;
      }
    }
    E("failed:sr[%s],StrLen_or_Ind:[%" PRId64 "/0x%" PRIu64 "]", sql_return_type(sr), (int64_t)StrLen_or_Ind, (uint64_t)StrLen_or_Ind);
    return SQL_ERROR;
  }

  return SQL_ERROR;
}

static SQLRETURN _test_case_get_char(SQLHANDLE hstmt, const char *sql, const char *name)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr == SQL_SUCCESS) {
    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_SUCCESS) {
      sr = _test_case_get_char_partial(hstmt, name);
    }
  }
  return sr;
}

static void _print_ucs_2le(const char *buf, size_t maxlen)
{
  fprintf(stderr, "0x");
  maxlen = maxlen / 2 * 2 - 2;
  for (size_t i=0; i+1 < maxlen; i+=2) {
    const unsigned char c1 = (const unsigned char)buf[i];
    const unsigned char c2 = (const unsigned char)buf[i+1];
    if (c1 == 0 && c2 == 0) break;
    fprintf(stderr, "%02x", c1);
    fprintf(stderr, "%02x", c2);
  }
  fprintf(stderr, "\n");
}

static SQLRETURN _test_case_get_wchar_partial(SQLHANDLE hstmt, const char *name)
{
  SQLRETURN sr = SQL_SUCCESS;
  SQLWCHAR buf[2];

  while (1) {
    SQLLEN StrLen_or_Ind;
    memset(buf, 'X', sizeof(buf));
    sr = CALL_SQLGetData(hstmt, 1, SQL_C_WCHAR, (SQLPOINTER)buf, sizeof(buf), &StrLen_or_Ind);
    if (sr == SQL_ERROR) return SQL_ERROR;
    if (sr == SQL_NO_DATA) return SQL_SUCCESS;
    if (sr == SQL_SUCCESS) {
      if (StrLen_or_Ind == SQL_NULL_DATA) {
        if (name) {
          E("failed:`%s` expected, but got null", name);
          return SQL_ERROR;
        }
        continue;
      }
      if (StrLen_or_Ind != SQL_NO_TOTAL && StrLen_or_Ind != 0) {
        _print_ucs_2le((const char*)buf, sizeof(buf));
        continue;
        // if (strcmp(buf, p) == 0) continue;
        // E("failed: `%s` expected, but got `%s`", name, buf);
        // return SQL_ERROR;
      }
      E("failed:sr[%s],StrLen_or_Ind:[%" PRId64 "]", sql_return_type(sr), (int64_t)StrLen_or_Ind);
      return SQL_ERROR;
    }
    if (sr == SQL_SUCCESS_WITH_INFO) {
      if (StrLen_or_Ind == SQL_NO_TOTAL || StrLen_or_Ind >= 0) {
        _print_ucs_2le((const char*)buf, sizeof(buf));
        // const size_t len = strlen(buf);
        // if (len == 0) {
        //   E("failed:expected to be greater than 0, but got =%" PRId64 "", len);
        //   return SQL_ERROR;
        // }
        // if (strncmp(buf, p, len)) {
        //   E("failed: `%.*s` expected, but got `%s`", (int)len, p, buf);
        //   return SQL_ERROR;
        // }
        // p += len;
        continue;
      }
    }
    E("failed:sr[%s],StrLen_or_Ind:[%" PRId64 "/0x%" PRIu64 "]", sql_return_type(sr), (int64_t)StrLen_or_Ind, (uint64_t)StrLen_or_Ind);
    return SQL_ERROR;
  }

  return SQL_ERROR;
}

static SQLRETURN _test_case_get_wchar(SQLHANDLE hstmt, const char *sql, const char *name)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr == SQL_SUCCESS) {
    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_SUCCESS) {
      sr = _test_case_get_wchar_partial(hstmt, name);
    }
  }
  return sr;
}

static int test_case_get_data(SQLHANDLE henv, const char *conn_str, int odbc)
{
  SQLRETURN sr = SQL_SUCCESS;

#define LOCAL_NAME             "我shi4中国人"

  struct {
    const char               *sql;
    int                       odbc;
  } sqls[] = {
    {"drop database if exists foo", TAOS_ODBC|MYSQL_ODBC},
    {"create database if not exists foo", TAOS_ODBC|MYSQL_ODBC},
    {"use foo", TAOS_ODBC|MYSQL_ODBC},
    {"drop table if exists t", 0xFF},

    {"create table if not exists t (name varchar(20))", MYSQL_ODBC|SQLITE3_ODBC},
    {"insert into t(name) values('" LOCAL_NAME "')", MYSQL_ODBC|SQLITE3_ODBC},

    {"create table if not exists t (ts timestamp, name varchar(20))", TAOS_ODBC},
    {"insert into t(ts, name) values(now(), '" LOCAL_NAME "')", TAOS_ODBC},
  };

  SQLHANDLE hconn = SQL_NULL_HANDLE;
  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, henv, &hconn);
  if (sr == SQL_SUCCESS) {
    sr = CALL_SQLDriverConnect(hconn, NULL, (SQLCHAR*)conn_str, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    if (sr == SQL_SUCCESS) {
      sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
      if (sr == SQL_SUCCESS) {
        if (odbc) {
          for (size_t i=0; i<sizeof(sqls)/sizeof(sqls[0]); ++i) {
            const char *sql = sqls[i].sql;
            if (odbc & sqls[i].odbc) {
              sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
              if (sr != SQL_SUCCESS) break;
            }
          }
        }
        if (sr == SQL_SUCCESS) {
          sr = _test_case_get_char(hstmt, "select name from t", LOCAL_NAME);
          if (sr == SQL_SUCCESS) {
            CALL_SQLCloseCursor(hstmt);
            sr = _test_case_get_wchar(hstmt, "select name from t", LOCAL_NAME);
          }
        }
        CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      }
      CALL_SQLDisconnect(hconn);
    }
    CALL_SQLFreeHandle(SQL_HANDLE_DBC, hconn);
  }

#undef LOCAL_NAME

  return (sr == SQL_SUCCESS) ? 0 : -1;
}

static int test_cases_get_data(SQLHANDLE henv)
{
  struct {
    const char           *conn_str;
    int                   odbc;
    int                   line;
  } cases[] = {
#ifdef ENABLE_SQLITE3_TEST     /* { */
    {"Driver={SQLite3}",  SQLITE3_ODBC, __LINE__},
#endif                         /* } */
    {"DSN=TAOS_ODBC_DSN", TAOS_ODBC, __LINE__},
  };

  for (size_t i=0; i<sizeof(cases)/sizeof(cases[0]); ++i) {
    const char *conn_str  = cases[i].conn_str;
    int         odbc      = cases[i].odbc;
    int         line      = cases[i].line;
    int r = test_case_get_data(henv, conn_str, odbc);
    if (r) {
      E("case[%zd]@%d:conn_str[%s];odbc[%x]", i+1, line, conn_str, odbc);
      return -1;
    }
  }

  return 0;
}

static SQLRETURN _test_case_prepare(SQLHANDLE hstmt, const char *sql, const char *name)
{
  SQLRETURN sr = SQL_SUCCESS;

  SQLUSMALLINT    ParameterNumber         = 1;
  SQLSMALLINT     InputOutputType         = SQL_PARAM_INPUT;
  SQLSMALLINT     ValueType               = SQL_C_CHAR;
  SQLSMALLINT     ParameterType           = SQL_VARCHAR;
  SQLULEN         ColumnSize;
  SQLSMALLINT     DecimalDigits;
  SQLPOINTER      ParameterValuePtr       = (SQLPOINTER)name;
  SQLLEN          BufferLength            = strlen(name);
  SQLLEN          StrLen_or_Ind           = SQL_NTS;

  SQLSMALLINT     Nullable = SQL_NULLABLE_UNKNOWN;

  sr = CALL_SQLPrepare(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  sr = CALL_SQLDescribeParam(hstmt,
      ParameterNumber, &ParameterType, &ColumnSize, &DecimalDigits, &Nullable);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  sr = CALL_SQLBindParameter(hstmt,
      ParameterNumber, InputOutputType, ValueType,
      ParameterType, ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength,
      &StrLen_or_Ind);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  sr = CALL_SQLExecute(hstmt);

  return sr;
}

static int test_case_prepare(SQLHANDLE henv, const char *conn_str, int odbc)
{
  SQLRETURN sr = SQL_SUCCESS;

#define LOCAL_NAME             "我shi4中国人"

  struct {
    const char               *sql;
    int                       odbc;
  } sqls[] = {
    {"drop database if exists foo", TAOS_ODBC|MYSQL_ODBC},
    {"create database if not exists foo", TAOS_ODBC|MYSQL_ODBC},
    {"use foo", TAOS_ODBC|MYSQL_ODBC},
    {"drop table if exists t", 0xFF},

    {"create table if not exists t (name varchar(20))", MYSQL_ODBC|SQLITE3_ODBC},

    {"create table if not exists t (ts timestamp, name varchar(20))", TAOS_ODBC},
  };

  SQLHANDLE hconn = SQL_NULL_HANDLE;
  SQLHANDLE hstmt = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, henv, &hconn);
  if (sr == SQL_SUCCESS) {
    sr = CALL_SQLDriverConnect(hconn, NULL, (SQLCHAR*)conn_str, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    if (sr == SQL_SUCCESS) {
      sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
      if (sr == SQL_SUCCESS) {
        if (odbc) {
          for (size_t i=0; i<sizeof(sqls)/sizeof(sqls[0]); ++i) {
            const char *sql = sqls[i].sql;
            if (odbc & sqls[i].odbc) {
              sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
              if (sr != SQL_SUCCESS) break;
              CALL_SQLCloseCursor(hstmt);
            }
          }
        }
        if (sr == SQL_SUCCESS) {
          sr = _test_case_prepare(hstmt, "insert into t(name) values(?)", LOCAL_NAME);
        }
        CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      }
      CALL_SQLDisconnect(hconn);
    }
    CALL_SQLFreeHandle(SQL_HANDLE_DBC, hconn);
  }

#undef LOCAL_NAME

  return (sr == SQL_SUCCESS) ? 0 : -1;
}

static int test_cases_prepare(SQLHANDLE henv)
{
  struct {
    const char           *conn_str;
    int                   odbc;
    int                   line;
  } cases[] = {
#ifdef ENABLE_SQLITE3_TEST     /* { */
    // {"Driver={SQLite3}",  SQLITE3_ODBC, __LINE__},
#endif                         /* } */
#ifdef ENABLE_MYSQL_TEST       /* { */
    // {"DSN=MYSQL_ODBC_DSN",  MYSQL_ODBC, __LINE__},
#endif                         /* } */
    {"DSN=TAOS_ODBC_DSN", TAOS_ODBC, __LINE__},
  };

  for (size_t i=0; i<sizeof(cases)/sizeof(cases[0]); ++i) {
    const char *conn_str  = cases[i].conn_str;
    int         odbc      = cases[i].odbc;
    int         line      = cases[i].line;
    int r = test_case_prepare(henv, conn_str, odbc);
    if (r) {
      E("case[%zd]@%d:conn_str[%s];odbc[%x]", i+1, line, conn_str, odbc);
      return -1;
    }
  }

  return 0;
}

static int test_hard_coded_cases(SQLHANDLE henv)
{
  int r = 0;

#ifdef ENABLE_MYSQL_TEST         /* { */
  r = test_hard_coded(henv, "MYSQL_ODBC_DSN", "root", "taosdata", NULL, 1);
  if (r) return -1;
#endif                           /* } */

#ifdef ENABLE_SQLITE3_TEST       /* { */
  r = test_hard_coded(henv, NULL, NULL, NULL, "Driver={SQLite3};Database=/tmp/foo.sqlite3", 1);
  if (r) return -1;
#endif                           /* } */

  r = test_hard_coded(henv, "TAOS_ODBC_DSN", NULL, NULL, NULL, 0);
  if (r) return -1;

  return 0;
}

int main(int argc, char *argv[])
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE henv  = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  if (FAILED(sr)) return 1;

  do {
    sr = CALL_SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (FAILED(sr)) { r = -1; break; }

    if (0) r = test_cases_prepare(henv);
    if (r) break;

    if (1) r = test_cases_get_data(henv);
    if (r) break;

    // hard_coded_test_cases
    if (1) r = test_hard_coded_cases(henv);
    if (r) break;

    r = process_by_args_env(argc, argv, henv);
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_ENV, henv);

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");

  return !!r;
}

