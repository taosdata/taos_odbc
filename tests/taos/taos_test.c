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

#include "taos_helpers.h"

#include "../test_helper.h"

#include <float.h>
#include <math.h>

typedef struct buffers_s              buffers_t;
struct buffers_s {
  char                     **arr;
  size_t                     cap;
  size_t                     nr;
};

static void buffers_reset(buffers_t *buffers)
{
  for (size_t i=0; i<buffers->nr; ++i) {
    free(buffers->arr[i]);
    buffers->arr[i] = NULL;
  }
  buffers->nr = 0;
}

static void buffers_release(buffers_t *buffers)
{
  buffers_reset(buffers);
  free(buffers->arr);
  buffers->arr     = NULL;
  buffers->cap     = 0;
}

static int buffers_append(buffers_t *buffers, char *buf)
{
  if (buffers->nr + 1 > buffers->cap) {
    size_t cap = (buffers->cap + 16) / 16 * 16;
    char **arr = (char**)realloc(buffers->arr, cap * sizeof(*arr));
    if (!arr) {
      E("out of memroy");
      return -1;
    }
    buffers->arr       = arr;
    buffers->cap       = cap;
  }

  buffers->arr[buffers->nr++] = buf;
  return 0;
}

static int cjson_cmp_cjson(int i_col, const cJSON *lv, const cJSON *rv)
{
  int r = 0;

  char *s1 = cJSON_PrintUnformatted(lv);
  char *s2 = cJSON_PrintUnformatted(rv);

  do {
    if (cJSON_Compare(lv, rv, true)) break;
    E("col #%d differs: [%s] <> [%s]", i_col+1, s1, s2);
    r = -1;
  } while (0);
  if (s1) free(s1);
  if (s2) free(s2);

  return r;
}

static int cjson_cmp_tsdb_null(int i_col, const cJSON *val)
{
  if (!cJSON_IsNull(val)) {
    char *t1 = cJSON_PrintUnformatted(val);
    E("col #%d expected null but got ==%s==", i_col, t1);
    free(t1);
    return -1;
  }
  return 0;
}

static int cjson_cmp_str(int i_col, const cJSON *val, const char *s)
{
  cJSON *cjson = cJSON_CreateString(s);
  if (!cjson) {
    E("out of memory");
    return -1;
  }

  int r = cjson_cmp_cjson(i_col, val, cjson);
  cJSON_Delete(cjson);

  return r;
}

static int cjson_cmp_tsdb_str_len(int i_col, const cJSON *val, const char *base, size_t len)
{
  char *p = strndup(base, len);
  if (!p) {
    E("out of memory");
    return -1;
  }

  int r = cjson_cmp_str(i_col, val, p);
  free(p);

  return r;
}

static int cjson_cmp_i64(int i_col, const cJSON *val, int64_t v)
{
  cJSON *cjson = cJSON_CreateNumber((double)v);
  if (!cjson) {
    E("out of memory");
    return -1;
  }

  int r = cjson_cmp_cjson(i_col, val, cjson);
  cJSON_Delete(cjson);

  return r;
}

static int cjson_cmp_tsdb_timestamp(int i_col, const cJSON *val, const char *base)
{
  int64_t v = *(int64_t*)base;

  int r = cjson_cmp_i64(i_col, val, v);

  return r;
}

static int cjson_cmp_i32(int i_col, const cJSON *val, int32_t v)
{
  cJSON *cjson = cJSON_CreateNumber((double)v);
  if (!cjson) {
    E("out of memory");
    return -1;
  }

  int r = cjson_cmp_cjson(i_col, val, cjson);
  cJSON_Delete(cjson);

  return r;
}

static int cjson_cmp_tsdb_int(int i_col, const cJSON *val, const char *base)
{
  int32_t v = *(int32_t*)base;

  int r = cjson_cmp_i32(i_col, val, v);

  return r;
}

static int cjson_cmp_tsdb_bigint(int i_col, const cJSON *val, const char *base)
{
  int64_t v = *(int64_t*)base;

  int r = cjson_cmp_i64(i_col, val, v);

  return r;
}

static int cjson_cmp_float(int i_col, const cJSON *val, float v)
{
  cJSON *cjson = cJSON_CreateNumber(v);
  if (!cjson) {
    E("out of memory");
    return -1;
  }

  int r = cjson_cmp_cjson(i_col, val, cjson);
  cJSON_Delete(cjson);

  if (r) {
    if (cJSON_IsNumber(val)) {
      double dl = v;
      double dr = val->valuedouble;
      char lbuf[64]; snprintf(lbuf, sizeof(lbuf), "%lg", dl);
      char rbuf[64]; snprintf(rbuf, sizeof(rbuf), "%lg", dr);
      if (strcmp(lbuf, rbuf) == 0) r = 0;
      else D("%s =?= %s", lbuf, rbuf);
    }
  }

  return r;
}

static int cjson_cmp_tsdb_float(int i_col, const cJSON *val, const char *base)
{
  float v = *(float*)base;

  int r = cjson_cmp_float(i_col, val, v);

  return r;
}

static int cjson_cmp_double(int i_col, const cJSON *val, double v)
{
  cJSON *cjson = cJSON_CreateNumber(v);
  if (!cjson) {
    E("out of memory");
    return -1;
  }

  int r = cjson_cmp_cjson(i_col, val, cjson);
  cJSON_Delete(cjson);

  return r;
}

static int cjson_cmp_tsdb_double(int i_col, const cJSON *val, const char *base)
{
  double v = *(double*)base;

  int r = cjson_cmp_double(i_col, val, v);

  return r;
}

static int field_cmp_val(int i_field, TAOS_FIELD *field, char *col, const cJSON *val)
{
  if (!col) {
    return cjson_cmp_tsdb_null(i_field, val);
  }
  int type  = field->type;
  int16_t bytes;
  switch (type) {
    case TSDB_DATA_TYPE_VARCHAR:
      bytes = *(int16_t*)(col - sizeof(int16_t));
      return cjson_cmp_tsdb_str_len(i_field, val, col, bytes);
      break;
    case TSDB_DATA_TYPE_NCHAR:
      bytes = *(int16_t*)(col - sizeof(int16_t));
      return cjson_cmp_tsdb_str_len(i_field, val, col, bytes);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      return cjson_cmp_tsdb_timestamp(i_field, val, col);
    case TSDB_DATA_TYPE_INT:
      return cjson_cmp_tsdb_int(i_field, val, col);
    case TSDB_DATA_TYPE_BIGINT:
      return cjson_cmp_tsdb_bigint(i_field, val, col);
    case TSDB_DATA_TYPE_FLOAT:
      return cjson_cmp_tsdb_float(i_field, val, col);
    case TSDB_DATA_TYPE_DOUBLE:
      return cjson_cmp_tsdb_double(i_field, val, col);
    default:
      E("col #%d [%d/0x%x]%s not implemented yet", i_field+1, type, type, CALL_taos_data_type(type));
      return -1;
  }

  return 0;
}

static int record_cmp_row(int nr_fields, TAOS_FIELD *fields, TAOS_ROW record, const cJSON *row, TAOS_RES *res)
{
  if (!cJSON_IsArray(row)) {
    char *t1 = cJSON_PrintUnformatted(row);
    E("json array is required but got ==%s==", t1);
    free(t1);
    return -1;
  }

  for (int i=0; i<nr_fields; ++i) {
    const cJSON *val = cJSON_GetArrayItem(row, i);
    if (!val) {
      E("col #%d: lack of corresponding data", i+1);
      return -1;
    }

    int *offsets = CALL_taos_get_column_data_offset(res, i);

    char *col = record[i];
    if (col) col += offsets ? *offsets : 0;

    TAOS_FIELD *field = fields + i;
    int r = field_cmp_val(i, field, col, val);
    if (r) return -1;
  }

  return 0;
}

static int res_cmp_rows(int nr_fields, TAOS_RES *res, const cJSON *rows)
{
  if (!res) return -1;
  if (CALL_taos_errno(res)) return -1;

  if (!rows) return 0;

  if (!nr_fields) {
    E("rows not required but provided");
    return -1;
  }

  if (!cJSON_IsArray(rows)) {
    char *t1 = cJSON_PrintUnformatted(rows);
    E("json array is required but got ==%s==", t1);
    free(t1);
    return -1;
  }

  TAOS_FIELD *fields = CALL_taos_fetch_fields(res);
  if (!fields) {
    E("no fields returned");
    return -1;
  }

  int nr_rows = cJSON_GetArraySize(rows);
  TAOS_ROW record = CALL_taos_fetch_row(res);
  if (!record) {
    if (nr_rows) {
      E("no result returned");
      return -1;
    }
    return 0;
  }
  for (int i=0; i<nr_rows; ++i) {
    if (!record) {
      E("end of record but rows still available");
      return -1;
    }
    const cJSON *row = cJSON_GetArrayItem(rows, i);
    int r = record_cmp_row(nr_fields, fields, record, row, res);
    if (r) return -1;
    record = CALL_taos_fetch_row(res);
  }

  if (record) {
    E("row expected but got NONE");
    return -1;
  }

  return 0;
}

static int test_query_cjson(TAOS *taos, const char *sql, const cJSON *rows)
{
  int r = 0;

  TAOS_RES *res = CALL_taos_query(taos, sql);
  if (!res) {
    if (rows) {
      E("no result is returned");
      return -1;
    }
    return 0;
  }

  do {
    if (CALL_taos_errno(res)) {
      r = -1;
      break;
    }

    int nr_fields = CALL_taos_field_count(res);
    r = res_cmp_rows(nr_fields, res, rows);
  } while (0);

  CALL_taos_free_result(res);
  return r;
}

static int run_sql_rs(TAOS *taos, const char *sql, cJSON *rs, int icase, int positive)
{
  int r = 0;

  LOG_CALL("%s case[#%d] [sql:%s]", positive ? "positive" : "negative", icase+1, sql);
  r = test_query_cjson(taos, sql, rs);

  r = !(!r ^ !positive);
  LOG_FINI(r, "%s case[#%d] [sql:%s]", positive ? "positive" : "negative", icase+1, sql);

  return r;
}

typedef struct type_bytes_s                  type_bytes_t;
struct type_bytes_s {
  int                  tsdb_type;
  int                  tsdb_bytes;
};

typedef struct executes_ctx_s                executes_ctx_t;
struct executes_ctx_s {
  TAOS       *taos;
  const char *sql;
  TAOS_STMT  *stmt;
  int         insert;
  int         nums;

  buffers_t   buffers;

  const char         *subtbl;
  TAOS_FIELD_E       *tags;
  int                 nr_tags;
  TAOS_FIELD_E       *cols;
  int                 nr_cols;

  unsigned int        subtbl_required:1;
  unsigned int        tags_described:1;
  unsigned int        cols_described:1;
};

static void executes_ctx_release_tags(executes_ctx_t *ctx)
{
  if (ctx->tags) {
    CALL_taos_stmt_reclaim_fields(ctx->stmt, ctx->tags);
    ctx->tags = NULL;
  }
  ctx->nr_tags = 0;
  ctx->tags_described = 0;
}

static void executes_ctx_release_cols(executes_ctx_t *ctx)
{
  if (ctx->cols) {
    CALL_taos_stmt_reclaim_fields(ctx->stmt, ctx->cols);
    ctx->cols = NULL;
  }
  ctx->nr_cols = 0;
  ctx->cols_described = 0;
}

static void executes_ctx_release(executes_ctx_t *ctx)
{
  if (ctx->stmt) {
    CALL_taos_stmt_close(ctx->stmt);
    ctx->stmt = NULL;
  }
  buffers_release(&ctx->buffers);
  executes_ctx_release_tags(ctx);
  executes_ctx_release_cols(ctx);
}

static int _prepare_mb_as_tsdb_timestamp(executes_ctx_t *ctx, TAOS_MULTI_BIND *col_mb, int rows)
{
  int64_t *buffer      = (int64_t*)malloc(rows * sizeof(*buffer));
  int32_t *length      = (int32_t*)malloc(rows * sizeof(*length));
  char    *is_null     = (char*)malloc(rows * sizeof(*is_null));

  if (!buffer || buffers_append(&ctx->buffers, (char*)buffer)) {
    free(buffer);
    free(length);
    free(is_null);
    E("out of memory");
    return -1;
  }

  if (!length || buffers_append(&ctx->buffers, (char*)length)) {
    free(length);
    free(is_null);
    E("out of memory");
    return -1;
  }

  if (!is_null || buffers_append(&ctx->buffers, (char*)is_null)) {
    free(is_null);
    E("out of memory");
    return -1;
  }

  col_mb->buffer_type            = TSDB_DATA_TYPE_TIMESTAMP;
  col_mb->buffer                 = buffer;
  col_mb->buffer_length          = sizeof(int64_t);
  col_mb->length                 = length; // correct me, fixed-length, necessary?
  col_mb->is_null                = is_null;
  col_mb->num                    = rows;

  return 0;
}

static int _prepare_mb_as_tsdb_int(executes_ctx_t *ctx, TAOS_MULTI_BIND *col_mb, int rows)
{
  int32_t *buffer      = (int32_t*)malloc(rows * sizeof(*buffer));
  int32_t *length      = (int32_t*)malloc(rows * sizeof(*length));
  char    *is_null     = (char*)malloc(rows * sizeof(*is_null));

  if (!buffer || buffers_append(&ctx->buffers, (char*)buffer)) {
    free(buffer);
    free(length);
    free(is_null);
    E("out of memory");
    return -1;
  }

  if (!length || buffers_append(&ctx->buffers, (char*)length)) {
    free(length);
    free(is_null);
    E("out of memory");
    return -1;
  }

  if (!is_null || buffers_append(&ctx->buffers, (char*)is_null)) {
    free(is_null);
    E("out of memory");
    return -1;
  }

  col_mb->buffer_type            = TSDB_DATA_TYPE_INT;
  col_mb->buffer                 = buffer;
  col_mb->buffer_length          = sizeof(int32_t);
  col_mb->length                 = length; // correct me, fixed-length, necessary?
  col_mb->is_null                = is_null;
  col_mb->num                    = rows;

  return 0;
}

static int _prepare_mb_as_tsdb_varchar(executes_ctx_t *ctx, TAOS_FIELD_E *col, TAOS_MULTI_BIND *col_mb, int rows)
{
  int32_t *buffer      = (int32_t*)malloc(rows * col->bytes);
  int32_t *length      = (int32_t*)malloc(rows * sizeof(*length));
  char    *is_null     = (char*)malloc(rows * sizeof(*is_null));

  if (!buffer || buffers_append(&ctx->buffers, (char*)buffer)) {
    free(buffer);
    free(length);
    free(is_null);
    E("out of memory");
    return -1;
  }

  if (!length || buffers_append(&ctx->buffers, (char*)length)) {
    free(length);
    free(is_null);
    E("out of memory");
    return -1;
  }

  if (!is_null || buffers_append(&ctx->buffers, (char*)is_null)) {
    free(is_null);
    E("out of memory");
    return -1;
  }

  col_mb->buffer_type            = TSDB_DATA_TYPE_VARCHAR;
  col_mb->buffer                 = buffer;
  col_mb->buffer_length          = col->bytes;
  col_mb->length                 = length;
  col_mb->is_null                = is_null;
  col_mb->num                    = rows;

  return 0;
}

static int _prepare_mb_as_tsdb_double(executes_ctx_t *ctx, TAOS_MULTI_BIND *col_mb, int rows)
{
  double *buffer       = (double*)malloc(rows * sizeof(*buffer));
  int32_t *length      = (int32_t*)malloc(rows * sizeof(*length));
  char    *is_null     = (char*)malloc(rows * sizeof(*is_null));

  if (!buffer || buffers_append(&ctx->buffers, (char*)buffer)) {
    free(buffer);
    free(length);
    free(is_null);
    E("out of memory");
    return -1;
  }

  if (!length || buffers_append(&ctx->buffers, (char*)length)) {
    free(length);
    free(is_null);
    E("out of memory");
    return -1;
  }

  if (!is_null || buffers_append(&ctx->buffers, (char*)is_null)) {
    free(is_null);
    E("out of memory");
    return -1;
  }

  col_mb->buffer_type            = TSDB_DATA_TYPE_DOUBLE;
  col_mb->buffer                 = buffer;
  col_mb->buffer_length          = sizeof(double);
  col_mb->length                 = length; // correct me, fixed-length, necessary?
  col_mb->is_null                = is_null;
  col_mb->num                    = rows;

  return 0;
}

static int _prepare_mb(executes_ctx_t *ctx, int iparam, TAOS_FIELD_E *col, TAOS_MULTI_BIND *col_mb, int rows)
{
  switch (col->type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      return _prepare_mb_as_tsdb_timestamp(ctx, col_mb, rows);
    case TSDB_DATA_TYPE_INT:
      return _prepare_mb_as_tsdb_int(ctx, col_mb, rows);
    case TSDB_DATA_TYPE_VARCHAR:
      return _prepare_mb_as_tsdb_varchar(ctx, col, col_mb, rows);
    case TSDB_DATA_TYPE_DOUBLE:
      return _prepare_mb_as_tsdb_double(ctx, col_mb, rows);
    default:
      E("#%d parameter marker of [%d]%s, but not implemented yet", iparam+1, col->type, CALL_taos_data_type(col->type));
      return -1;
  }
}

static int _store_param_val_by_mb_as_tsdb_timestamp(executes_ctx_t *ctx, cJSON *param, int iparam, TAOS_MULTI_BIND *mb, int irow)
{
  (void)ctx;
  double d;
  int r = json_object_get_number(param, "timestamp", &d);
  if (r) r = json_object_get_number(param, "bigint", &d);
  if (r) r = json_get_number(param, &d);
  if (r) {
    char *t1 = cJSON_PrintUnformatted(param);
    E("#(%d,%d) parameter marker of [%d]%s, but got ==%s==", irow+1, iparam+1, mb->buffer_type, CALL_taos_data_type(mb->buffer_type), t1);
    free(t1);
    return -1;
  }

  if (d < INT64_MIN || d > INT64_MAX) {
    char *t1 = cJSON_PrintUnformatted(param);
    E("#(%d,%d) parameter marker of [%d]%s, but over/underflow, ==%s==", irow+1, iparam+1, mb->buffer_type, CALL_taos_data_type(mb->buffer_type), t1);
    free(t1);
    return -1;
  }

  int64_t *buffer   = (int64_t*)mb->buffer;
  int32_t *length   = (int32_t*)mb->length;
  char    *is_null  = (char*)mb->is_null;
  buffer     += irow;
  length     += irow;
  is_null    += irow;

  *buffer    = (int64_t)d;
  *length    = sizeof(int64_t);
  *is_null   = '\0';

  return 0;
}

static int _store_param_val_by_mb_as_tsdb_int(executes_ctx_t *ctx, cJSON *param, int iparam, TAOS_MULTI_BIND *mb, int irow)
{
  (void)ctx;

  int r = 0;

  double d;
  if (cJSON_IsObject(param)) {
    r = json_object_get_number(param, "int", &d);
    if (r) {
      char *t1 = cJSON_PrintUnformatted(param);
      E("#(%d,%d) parameter marker of [%d]%s, but got ==%s==", irow+1, iparam+1, mb->buffer_type, CALL_taos_data_type(mb->buffer_type), t1);
      free(t1);
      return -1;
    }
  } else if (cJSON_IsNumber(param)) {
    d = cJSON_GetNumberValue(param);
  } else {
    char *t1 = cJSON_PrintUnformatted(param);
    E("#(%d,%d) parameter marker of [%d]%s, but got ==%s==", irow+1, iparam+1, mb->buffer_type, CALL_taos_data_type(mb->buffer_type), t1);
    free(t1);
    return -1;
  }

  if (d < INT32_MIN || d > INT32_MAX) {
    char *t1 = cJSON_PrintUnformatted(param);
    E("#(%d,%d) parameter marker of [%d]%s, but over/underflow, ==%s==", irow+1, iparam+1, mb->buffer_type, CALL_taos_data_type(mb->buffer_type), t1);
    free(t1);
    return -1;
  }

  int32_t *buffer   = (int32_t*)mb->buffer;
  int32_t *length   = (int32_t*)mb->length;
  char    *is_null  = (char*)mb->is_null;
  buffer     += irow;
  length     += irow;
  is_null    += irow;

  *buffer    = (int32_t)d;
  *length    = sizeof(int32_t);
  *is_null   = '\0';

  return 0;
}

static int _store_param_val_by_mb_as_tsdb_varchar(executes_ctx_t *ctx, cJSON *param, int iparam, TAOS_MULTI_BIND *mb, int irow)
{
  (void)ctx;

  if (!cJSON_IsString(param)) {
    char *t1 = cJSON_PrintUnformatted(param);
    E("#(%d,%d) parameter marker of [%d]%s, but got ==%s==", irow+1, iparam+1, mb->buffer_type, CALL_taos_data_type(mb->buffer_type), t1);
    free(t1);
    return -1;
  }

  const char *s = cJSON_GetStringValue(param);
  size_t n = strlen(s);
  if (n > mb->buffer_length - 2) { // hard-coded
    char *t1 = cJSON_PrintUnformatted(param);
    E("#(%d,%d) parameter marker of [%d]%s, would be truncated, ==%s==", irow+1, iparam+1, mb->buffer_type, CALL_taos_data_type(mb->buffer_type), t1);
    free(t1);
    return -1;
  }

  char    *buffer   = (char*)mb->buffer;
  int32_t *length   = (int32_t*)mb->length;
  char    *is_null  = (char*)mb->is_null;
  buffer     += irow * mb->buffer_length;
  length     += irow;
  is_null    += irow;

  strncpy(buffer, s, n);
  buffer[n] = '\0';
  buffer[n+1] = '\0';
  *length    = (int32_t)n;       // plus 2 ?
  *is_null   = '\0';

  return 0;
}

static int _store_param_val_by_mb_as_tsdb_double(executes_ctx_t *ctx, cJSON *param, int iparam, TAOS_MULTI_BIND *mb, int irow)
{
  (void)ctx;

  int r = 0;

  double d;
  if (cJSON_IsObject(param)) {
    r = json_object_get_number(param, "double", &d);
    if (r) {
      char *t1 = cJSON_PrintUnformatted(param);
      E("#(%d,%d) parameter marker of [%d]%s, but got ==%s==", irow+1, iparam+1, mb->buffer_type, CALL_taos_data_type(mb->buffer_type), t1);
      free(t1);
      return -1;
    }
  } else if (cJSON_IsNumber(param)) {
    d = cJSON_GetNumberValue(param);
  } else {
    char *t1 = cJSON_PrintUnformatted(param);
    E("#(%d,%d) parameter marker of [%d]%s, but got ==%s==", irow+1, iparam+1, mb->buffer_type, CALL_taos_data_type(mb->buffer_type), t1);
    free(t1);
    return -1;
  }

  double *buffer    = (double*)mb->buffer;
  int32_t *length   = (int32_t*)mb->length;
  char    *is_null  = (char*)mb->is_null;
  buffer     += irow;
  length     += irow;
  is_null    += irow;

  *buffer    = (double)d;
  *length    = sizeof(double);
  *is_null   = '\0';

  return 0;
}

static int _store_param_val_by_mb(executes_ctx_t *ctx, cJSON *param, int irow, int iparam, TAOS_MULTI_BIND *mb)
{
  if (cJSON_IsNull(param)) {
    char    *is_null  = (char*)mb->is_null;
    is_null    += irow;

    *is_null   = 1;
    return 0;
  }

  switch (mb->buffer_type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      return _store_param_val_by_mb_as_tsdb_timestamp(ctx, param, iparam, mb, irow);
    case TSDB_DATA_TYPE_INT:
      return _store_param_val_by_mb_as_tsdb_int(ctx, param, iparam, mb, irow);
    case TSDB_DATA_TYPE_VARCHAR:
      return _store_param_val_by_mb_as_tsdb_varchar(ctx, param, iparam, mb, irow);
    case TSDB_DATA_TYPE_DOUBLE:
      return _store_param_val_by_mb_as_tsdb_double(ctx, param, iparam, mb, irow);
    default:
      E("#%d parameter marker of [%d]%s, but not implemented yet", iparam+1, mb->buffer_type, CALL_taos_data_type(mb->buffer_type));
      return -1;
  }
}

static int _bind_tags(executes_ctx_t *ctx, cJSON *start, int istart, int iend, cJSON *params)
{
  int r = 0;

  if (ctx->nr_tags <= 0) return 0;

  TAOS_MULTI_BIND *tag_mbs = (TAOS_MULTI_BIND*)calloc(ctx->nr_tags, sizeof(*tag_mbs));
  if (!tag_mbs) {
    E("out of memory");
    return -1;
  }

  for (int i=0; i<ctx->nr_tags; ++i) {
    TAOS_FIELD_E *tag = ctx->tags + i;
    TAOS_MULTI_BIND *tag_mb = tag_mbs + i;

    int iparam = !!ctx->subtbl + i;
    cJSON *param = cJSON_GetArrayItem(start, iparam);

    // all tag values in the same column shall be identical
    for (int i=istart+1; i<iend; ++i) {
      cJSON *row = cJSON_GetArrayItem(params, i);
      cJSON *col = cJSON_GetArrayItem(row, iparam);
      if (!cJSON_Compare(col, param, true)) {
        char *t1 = cJSON_PrintUnformatted(param);
        char *t2 = cJSON_PrintUnformatted(col);
        E("tag values differs, ==%s== <> ==%s==", t1, t2);
        free(t1);
        free(t2);
        r = -1;
        break;
      }
    }
    if (r) break;

    r = _prepare_mb(ctx, iparam, tag, tag_mb, 1);
    if (r) break;

    r = _store_param_val_by_mb(ctx, param, 0, iparam, tag_mb);
    if (r) break;
  }

  if (r == 0) {
    r = CALL_taos_stmt_set_tags(ctx->stmt, tag_mbs);
  }

  if (tag_mbs) free(tag_mbs);

  return r;
}

static int _bind_cols(executes_ctx_t *ctx, cJSON *start, int istart, int iend, cJSON *params)
{
  int r = 0;

  if (ctx->nr_cols <= 0) return 0;

  TAOS_MULTI_BIND *col_mbs = (TAOS_MULTI_BIND*)calloc(ctx->nr_cols, sizeof(*col_mbs));
  if (!col_mbs) {
    E("out of memory");
    return -1;
  }

  for (int i=0; i<ctx->nr_cols; ++i) {
    TAOS_FIELD_E *col = ctx->cols + i;
    TAOS_MULTI_BIND *mb = col_mbs + i;

    int iparam = !!ctx->subtbl + ctx->nr_tags + i;
    cJSON *param = cJSON_GetArrayItem(start, iparam);

    r = _prepare_mb(ctx, iparam, col, mb, iend - istart);
    if (r) break;

    r = _store_param_val_by_mb(ctx, param, 0, iparam, mb);
    if (r) break;

    for (int i=istart+1; i<iend; ++i) {
      cJSON *row = cJSON_GetArrayItem(params, i);
      cJSON *col = cJSON_GetArrayItem(row, iparam);
      r = _store_param_val_by_mb(ctx, col, i-istart, iparam, mb);
      if (r) break;
    }
    if (r) break;
  }

  if (r == 0) {
    r = CALL_taos_stmt_bind_param_batch(ctx->stmt, col_mbs);
  }

  if (r == 0) {
    r = CALL_taos_stmt_add_batch(ctx->stmt);
  }

  if (col_mbs) free(col_mbs);

  return r;
}

static int _bind_mb_by_param_double(executes_ctx_t *ctx, double param, int istart, int iparam, TAOS_MULTI_BIND *mb)
{
  (void)ctx;
  (void)istart;
  (void)iparam;

  double *v = (double*)malloc(sizeof(double));
  if (!v) {
    E("out of memory");
    return -1;
  }
  *v = param;

  mb->buffer_type          = TSDB_DATA_TYPE_DOUBLE;
  mb->buffer               = v;
  mb->buffer_length        = sizeof(double);
  mb->length               = NULL;
  mb->is_null              = NULL;
  mb->num                  = 1;

  return 0;
}

static int _bind_mb_by_param_str(executes_ctx_t *ctx, const char *param, int istart, int iparam, TAOS_MULTI_BIND *mb)
{
  (void)ctx;
  (void)istart;
  (void)iparam;

  char *buffer = strdup(param);

  int32_t *length = (int32_t*)malloc(sizeof(*length));
  if (!length) {
    E("out of memory");
    return -1;
  }
  *length = (int32_t)strlen(buffer);

  mb->buffer_type          = TSDB_DATA_TYPE_VARCHAR;
  mb->buffer               = buffer;
  mb->buffer_length        = *length;
  mb->length               = length;
  mb->is_null              = NULL;
  mb->num                  = 1;

  return 0;
}

static int _bind_mb_by_param(executes_ctx_t *ctx, cJSON *param, int istart, int iparam, TAOS_MULTI_BIND *mb)
{
  (void)ctx;
  (void)mb;
  if (cJSON_IsNumber(param)) {
    return _bind_mb_by_param_double(ctx, cJSON_GetNumberValue(param), istart, iparam, mb);
  }
  if (cJSON_IsString(param)) {
    return _bind_mb_by_param_str(ctx, cJSON_GetStringValue(param), istart, iparam, mb);
  }
  char *t1 = cJSON_PrintUnformatted(param);
  E("#(%d,%d) parameter not implemented yet, ==%s==", istart+1, iparam+1, t1);
  free(t1);
  return -1;
}

static int _run_execute_row_rs_mbs(executes_ctx_t *ctx, cJSON *start, int istart, cJSON *rs, TAOS_MULTI_BIND *mbs)
{
  (void)rs;

  int r = 0;
  for (int i=0; i<ctx->nums; ++i) {
    TAOS_MULTI_BIND *mb = mbs + i;
    cJSON *param = cJSON_GetArrayItem(start, i);
    r = _bind_mb_by_param(ctx, param, istart, i, mb);
    if (r) break;
  }
  if (r) return -1;

  r = CALL_taos_stmt_bind_param_batch(ctx->stmt, mbs);
  if (r) return -1;
  r = CALL_taos_stmt_add_batch(ctx->stmt);
  if (r) return -1;

  r = CALL_taos_stmt_execute(ctx->stmt);
  if (r) return -1;

  if (rs == NULL) return 0;

  if (!cJSON_IsArray(rs)) {
    char *t1 = cJSON_PrintUnformatted(rs);
    E("json array is expected, but got, ==%s==", t1);
    free(t1);
    return -1;
  }

  cJSON *curr_rs = cJSON_GetArrayItem(rs, 0);

  if (!curr_rs) return 0;

  TAOS_RES *res = CALL_taos_stmt_use_result(ctx->stmt);
  if (!res) {
    E("no result is returned");
    return -1;
  }

  int nr_fields = CALL_taos_field_count(res);
  r = res_cmp_rows(nr_fields, res, curr_rs);

  return r;
}

static int _run_execute_row_rs(executes_ctx_t *ctx, cJSON *start, int istart, cJSON *rs)
{
  int r = 0;

  if (!cJSON_IsArray(start)) {
    char *t1 = cJSON_PrintUnformatted(start);
    E("#%d parameters is not json array, but got ==%s==", istart, t1);
    free(t1);
    return -1;
  }

  TAOS_MULTI_BIND *mbs = (TAOS_MULTI_BIND*)calloc(ctx->nums, sizeof(*mbs));
  if (!mbs) {
    E("out of memory");
    return -1;
  }

  r = _run_execute_row_rs_mbs(ctx, start, istart, rs, mbs);

  for (int i=0; i<ctx->nums; ++i) {
    TAOS_MULTI_BIND *mb = mbs + i;
    if (mb->buffer)  free(mb->buffer);
    if (mb->length)  free(mb->length);
    if (mb->is_null) free(mb->is_null);
  }
  free(mbs);

  return r;
}

static int _run_execute_block_params_rs(executes_ctx_t *ctx, cJSON *start, int istart, int iend, cJSON *params, cJSON *rs)
{
  int r = 0;

  if (ctx->subtbl) {
    r = CALL_taos_stmt_set_tbname(ctx->stmt, ctx->subtbl);
    if (r) return -1;
  }

  if (ctx->insert) {
    if (ctx->subtbl_required && !ctx->tags_described) {
      int fieldNum = 0;
      TAOS_FIELD_E *fields = NULL;
      r = CALL_taos_stmt_get_tag_fields(ctx->stmt, &fieldNum, &fields);
      if (r == 0) {
        for (int i=0; i<fieldNum; ++i) {
          TAOS_FIELD_E *field = fields + i;
          D("field[#%d]: %s[%s, precision:%d, scale:%d, bytes:%d]", i+1, field->name, CALL_taos_data_type(field->type), field->precision, field->scale, field->bytes);
        }
        ctx->tags_described = 1;
        ctx->nr_tags = fieldNum;
        ctx->tags    = fields;

        r = _bind_tags(ctx, start, istart, iend, params);
        if (r) return -1;
      }
    }

    if (!ctx->cols_described) {
      int colNum = 0;
      TAOS_FIELD_E *cols = NULL;
      r = CALL_taos_stmt_get_col_fields(ctx->stmt, &colNum, &cols);
      if (r) return -1;
      for (int i=0; i<colNum; ++i) {
        TAOS_FIELD_E *col = cols + i;
        D("co[#%d]: %s[%s, precision:%d, scale:%d, bytes:%d]", i+1, col->name, CALL_taos_data_type(col->type), col->precision, col->scale, col->bytes);
      }
      ctx->cols_described = 1;
      ctx->nr_cols = colNum;
      ctx->cols    = cols;

      r = _bind_cols(ctx, start, istart, iend, params);
      if (r) return -1;
    }

    return CALL_taos_stmt_execute(ctx->stmt);
  }

  // TODO: taosc: currently taos_stmt_num_params does not apply to non-insert-statement
  r = CALL_taos_stmt_num_params(ctx->stmt, &ctx->nums);
  if (r) {
    // TODO: taosc: taos_stmt_num_params fails if it's non-insert-non-parameterized-statement
    //       but `params` is provided
    //       thus we fail the function out
    E("it's a non-insert-non-parameterized-statement, no parameter is required");
    return -1;
  } else if (ctx->nums == 0) {
    E("it's a non-parameterized-statement, no parameter is required");
    return -1;
  }

  // taosc: non-insert-parameterized-statement, especially select, batch-execution does NOT work

  // finish the first row of parameters
  r = _run_execute_row_rs(ctx, start, istart, rs);
  if (r) return -1;

  // finish the remaining rows of parameters
  for (int i=istart+1; i<iend; ++i) {
    cJSON *row = cJSON_GetArrayItem(params, i);
    r = _run_execute_row_rs(ctx, row, i, rs);
    if (r) break;
  }

  return r;
}

static int _run_execute_params_rs(executes_ctx_t *ctx, cJSON *params, cJSON *rs)
{
  int r = 0;

  cJSON *r0 = cJSON_GetArrayItem(params, 0);
  if (!r0 || !cJSON_IsArray(r0)) {
    r0 = params;
  }

  int rows = 1;
  if (r0 != params) rows = cJSON_GetArraySize(params);

  int irow0 = 0;
  cJSON *row0 = r0;
  cJSON *row = r0;
  if (ctx->insert && ctx->subtbl_required) {
    // it's an subtbl insert statement
    // we have to split the rows of parameters according to subtbl name
    for (int i=0; i<rows; ++i) {
      if (i) row = cJSON_GetArrayItem(params, i);
      cJSON *x = cJSON_GetArrayItem(row, 0);
      if (!x || !cJSON_IsString(x)) {
        char *t1 = cJSON_PrintUnformatted(row);
        E("lack of `subtbl` parameter, ==%s==", t1);
        free(t1);
        return -1;
      }

      const char *s = cJSON_GetStringValue(x);
      if (!ctx->subtbl) {
        ctx->subtbl = s;
        continue;
      }
      if (strcmp(ctx->subtbl, s)==0) continue;

      // execute with previous block of parameters
      LOG_CALL("block insert [%d -> %d]", irow0, i);
      r = _run_execute_block_params_rs(ctx, row0, irow0, i, params, rs);
      LOG_FINI(r, "block insert [%d -> %d]", irow0, i);
      if (r) return -1;

      executes_ctx_release_tags(ctx);
      executes_ctx_release_cols(ctx);

      ctx->subtbl = s;
      irow0 = i;
      row0 = row;
    }
  }
  if (r) return -1;

  // finish the last block of parameters
  LOG_CALL("block insert [%d -> %d]", irow0, rows);
  r = _run_execute_block_params_rs(ctx, row0, irow0, rows, params, rs);
  LOG_FINI(r, "block insert [%d -> %d]", irow0, rows);

  return r;
}

static int executes_ctx_prepare_stmt(executes_ctx_t *ctx)
{
  int r = 0;
  if (ctx->stmt) return 0;

  ctx->stmt = CALL_taos_stmt_init(ctx->taos);
  if (!ctx->stmt) return -1;

  r = CALL_taos_stmt_prepare(ctx->stmt, ctx->sql, (unsigned long)strlen(ctx->sql));
  if (r) return -1;

  r = CALL_taos_stmt_is_insert(ctx->stmt, &ctx->insert);
  if (r) return -1;

  if (!ctx->insert) {
    if (1) return 0;
    int nr_params = 0;
    r = CALL_taos_stmt_num_params(ctx->stmt, &nr_params);
    if (r) return -1;
    // [-2147474431/0x80002401]invalid catalog input parameters
    for (int i=0; i<nr_params; ++i) {
      int type  = 0;
      int bytes = 0;
      r = CALL_taos_stmt_get_param(ctx->stmt, i, &type, &bytes);
    }
    return 0;
  }

  int fieldNum = 0;
  TAOS_FIELD_E *fields = NULL;
  r = CALL_taos_stmt_get_tag_fields(ctx->stmt, &fieldNum, &fields);
  if (r) {
    int e = CALL_taos_errno(NULL);
    if (e == TSDB_CODE_TSC_STMT_TBNAME_ERROR) {
      // insert into ? ... will result in TSDB_CODE_TSC_STMT_TBNAME_ERROR
      ctx->subtbl_required = 1;
      W("this is believed an subtbl insert statement");
      r = 0;
    } else if (e == TSDB_CODE_TSC_STMT_API_ERROR) {
      // insert into t ... and t is normal tablename, will result in TSDB_CODE_TSC_STMT_API_ERROR
      ctx->subtbl_required = 0;
      W("this is believed an non-subtbl insert statement");
      r = 0;
    }
  }
  CALL_taos_stmt_reclaim_fields(ctx->stmt, fields);

  return 0;
}

static int run_execute_params_rs(executes_ctx_t *ctx, cJSON *params, cJSON *rs)
{
  int r = 0;

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
        r = test_query_cjson(ctx->taos, ctx->sql, rs);
        LOG_FINI(r, "%s case[#%d] [%s]", positive ? "positive" : "negative", i+1, ctx->sql);
      } else {
        LOG_CALL("%s case[#%d]", positive ? "positive" : "negative", i+1);
        r = run_execute_params_rs(ctx, params, rs);
        LOG_FINI(r, "%s case[#%d]", positive ? "positive" : "negative", i+1);
        executes_ctx_release_tags(ctx);
        executes_ctx_release_cols(ctx);
        ctx->subtbl = NULL;
      }

      r = !(!r ^ !positive);

      if (r) break;
    }
  } while (0);

  return r;
}

static int _run_executes(TAOS *taos, const char *sql, cJSON *executes)
{
  int r = 0;

  if (!cJSON_IsArray(executes)) {
    char *t1 = cJSON_PrintUnformatted(executes);
    W("expect `executes` to be json array but got ==%s==", t1);
    free(t1);
    return -1;
  }

  executes_ctx_t ctx = {0};
  ctx.taos     = taos;
  ctx.sql      = sql;

  r = _run_executes_by_ctx(&ctx, executes);

  executes_ctx_release(&ctx);

  return r;
}

static int run_executes(TAOS *taos, const char *sql, cJSON *executes, int icase, int positive)
{
  int r = 0;

  LOG_CALL("%s case[#%d] [%s]", positive ? "positive" : "negative", icase+1, sql);
  r = _run_executes(taos, sql, executes);

  r = !(!r ^ !positive);
  LOG_FINI(r, "%s case[#%d] [%s]", positive ? "positive" : "negative", icase+1, sql);

  return r;
}

static int run_sql(TAOS *taos, cJSON *jsql, int icase)
{
  int r = 0;

  if (cJSON_IsString(jsql)) {
    const char *sql = cJSON_GetStringValue(jsql);
    LOG_CALL("run_sql_rs(taos:%p, sql:%s, rs:%p, icase:%d, positive:%d", taos, sql, NULL, icase, 1);
    r = run_sql_rs(taos, sql, NULL, icase, 1);
    LOG_FINI(r, "run_sql_rs(taos:%p, sql:%s, rs:%p, icase:%d, positive:%d", taos, sql, NULL, icase, 1);
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
      r = run_executes(taos, sql, executes, icase, positive);
    } else {
      cJSON *rs = NULL;
      json_get_by_path(jsql, "rs", &rs);

      r = run_sql_rs(taos, sql, rs, icase, positive);
    }
  } else {
    char *t1 = cJSON_PrintUnformatted(jsql);
    W("non-string-non-object, just ignore. ==%s==", t1);
    free(t1);
  }

  return r;
}

static int run_case_under_taos(TAOS *taos, cJSON *json)
{
  int r = 0;
  cJSON *sqls = json_object_get_array(json, "sqls");
  if (!sqls) return 0;
  for (int i=0; i>=0; ++i) {
    cJSON *sql = cJSON_GetArrayItem(sqls, i);
    if (!sql) break;
    r = run_sql(taos, sql, i);
    if (r) break;
  }
  return r;
}

static int run_case(cJSON *json)
{
  const char *ip    = json_object_get_string(json, "conn/ip");
  const char *uid   = json_object_get_string(json, "conn/uid");
  const char *pwd   = json_object_get_string(json, "conn/pwd");
  const char *db    = json_object_get_string(json, "conn/db");
  uint16_t    port  = 0; {
    double d;
    if (!json_object_get_number(json, "conn/port", &d)) {
      if (d > 0) port = (uint16_t)d;
    }
  }

  TAOS *taos = CALL_taos_connect(ip, uid, pwd, db, port);
  if (!taos) return -1;

  int r = 0;
  r = run_case_under_taos(taos, json);

  CALL_taos_close(taos);

  return r;
}

static int run_json_file(cJSON *json)
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

    r = run_case(json_case);

    r = !(!r ^ !positive);
    LOG_FINI(r, "%s case[#%d]", positive ? "positive" : "negative", i+1);

    if (r) break;
  }

  return r;
}

static int try_and_run_file(const char *file)
{
  int r = 0;
  char buf[PATH_MAX+1];
  char *p = tod_basename(file, buf, sizeof(buf));
  if (!p) return -1;

  cJSON *json = load_json_file(file, NULL, 0, "UTF-8", "UTF-8");
  if (!json) return -1;

  const char *base = p;

  LOG_CALL("case %s", base);
  r = run_json_file(json);
  LOG_FINI(r, "case %s", base);

  cJSON_Delete(json);
  return r;
}

static int try_and_run(cJSON *json_test_case, const char *path)
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

  return try_and_run_file(buf);
}

static int load_and_run(const char *json_test_cases_file)
{
  int r = 0;

  char path[PATH_MAX+1];
  cJSON *json_test_cases = load_json_file(json_test_cases_file, path, sizeof(path), "UTF-8", "UTF-8");
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
      r = try_and_run_file(json_test_cases_file);
      break;
    }
    for (int i=0; i>=0; ++i) {
      cJSON *json_test_case = NULL;
      if (json_get_by_item(json_test_cases, i, &json_test_case)) break;
      if (!json_test_case) break;
      r = try_and_run(json_test_case, path);
      if (r) break;
    }
  } while (0);

  cJSON_Delete(json_test_cases);

  return r;
}

static int process_by_args(int argc, char *argv[])
{
  (void)argc;
  (void)argv;

  int r = 0;
  const char *json_test_cases_file = getenv("TAOS_TEST_CASES");
  if (!json_test_cases_file) {
    W("set environment `TAOS_TEST_CASES` to the test cases file");
    return -1;
  }

  LOG_CALL("load_and_run(%s)", json_test_cases_file);
  r = load_and_run(json_test_cases_file);
  LOG_FINI(r, "load_and_run(%s)", json_test_cases_file);
  return r;
}

static int flaw_case_under_stmt(TAOS_STMT *stmt, int *tagNum, TAOS_FIELD_E **tags, int *colNum, TAOS_FIELD_E **cols)
{
  int r = 0;
  int insert = 0;
  const char *insert_statement = "insert into ? using st tags (?) values (?, ?)";
  TAOS_MULTI_BIND tagbinds[1] = {0}; // one tag-placeholder in parameterised-statement as above
  TAOS_MULTI_BIND colbinds[2] = {0}; // two col-placeholder in parameterised-statement as above
  const char *tag_value = "hello";
  int32_t tag_value_length = (int32_t)strlen(tag_value);
  int64_t ts_value = 1662961478755;
  int32_t age_value = 20;

  r = CALL_taos_stmt_prepare(stmt, insert_statement, (unsigned long)strlen(insert_statement));
  if (r) return -1;

  r = CALL_taos_stmt_is_insert(stmt, &insert);
  if (r) return -1;

  if (1) {
    // guess if it's parameterised-subtbl-insert-statement

    // don't forget, is's application's duty to free
    if (*tags) { free(*tags); *tags = NULL; }
    r = CALL_taos_stmt_get_tag_fields(stmt, tagNum, tags);
    if (r == 0) {
      // this is not expected to happend because of the parameterised-statement used above
      fprintf(stderr, "%s[%d]:%s(): internal logic error\n", __FILE__, __LINE__, __func__);
      return -1;
    }

    int e = CALL_taos_errno(NULL);
    if (e != TSDB_CODE_TSC_STMT_TBNAME_ERROR) {
      // this is not expected to happend because of the parameterised-statement used above
      fprintf(stderr, "%s[%d]:%s(): internal logic error\n", __FILE__, __LINE__, __func__);
      return -1;
    }

    // ok, this is expected to happen because of the parameterised-statement used above
    if (*tags || *tagNum>0) {
      // this shall not happen!!!
      fprintf(stderr, "%s[%d]:%s(): internal logic error\n", __FILE__, __LINE__, __func__);
      return -1;
    }
  }

  // let the parser know where to find tag/col fields meta info from
  r = CALL_taos_stmt_set_tbname(stmt, "suzhou");
  if (r) return -1;

  // don't forget, is's application's duty to free
  if (*tags) { free(*tags); *tags = NULL; }
  r = CALL_taos_stmt_get_tag_fields(stmt, tagNum, tags);
  if (r) return -1;
  if (*tagNum != 1 && !*tags) {
    // this shall not happen!!!
    fprintf(stderr, "%s[%d]:%s(): internal logic error\n", __FILE__, __LINE__, __func__);
    return -1;
  }

  // don't forget, is's application's duty to free
  if (*cols) { free(*cols); *cols = NULL; }
  r = CALL_taos_stmt_get_col_fields(stmt, colNum, cols);
  if (r) return -1;
  if (*colNum != 1 && !*cols) {
    // this shall not happen!!!
    fprintf(stderr, "%s[%d]:%s(): internal logic error\n", __FILE__, __LINE__, __func__);
    return -1;
  }

  if ((*tags)[0].type != TSDB_DATA_TYPE_VARCHAR) {
    // this shall not happen!!!
    fprintf(stderr, "%s[%d]:%s(): internal logic error\n", __FILE__, __LINE__, __func__);
    return -1;
  }
  tagbinds[0].buffer_type    = (*tags)[0].type;
  tagbinds[0].buffer         = "hello";
  tagbinds[0].buffer_length  = (*tags)[0].bytes;               // correct me if i am wrong here
  tag_value_length           = (int32_t)strlen("hello");       // correct me if i am wrong here
  tagbinds[0].length         = &tag_value_length;              // correct me if i am wrong here
  tagbinds[0].is_null        = NULL;                           // correct me if i am wrong here
  tagbinds[0].num            = 1;
  r = CALL_taos_stmt_set_tags(stmt, tagbinds);
  if (r) return -1;

  if ((*cols)[0].type != TSDB_DATA_TYPE_TIMESTAMP) {
    // this shall not happen!!!
    fprintf(stderr, "%s[%d]:%s(): internal logic error\n", __FILE__, __LINE__, __func__);
    return -1;
  }
  colbinds[0].buffer_type    = (*cols)[0].type;
  colbinds[0].buffer         = &ts_value;
  colbinds[0].buffer_length  = sizeof(int64_t);       // correct me if i am wrong here
  colbinds[0].length         = NULL;                  // correct me if i am wrong here
  colbinds[0].is_null        = NULL;                  // correct me if i am wrong here
  colbinds[0].num            = 1;

  if ((*cols)[1].type != TSDB_DATA_TYPE_INT) {
    // this shall not happen!!!
    fprintf(stderr, "%s[%d]:%s(): internal logic error\n", __FILE__, __LINE__, __func__);
    return -1;
  }
  colbinds[1].buffer_type    = (*cols)[1].type;
  colbinds[1].buffer         = &age_value;
  colbinds[1].buffer_length  = sizeof(int32_t);       // correct me if i am wrong here
  colbinds[1].length         = NULL;                  // correct me if i am wrong here
  colbinds[1].is_null        = NULL;                  // correct me if i am wrong here
  colbinds[1].num            = 1;

  r = CALL_taos_stmt_bind_param_batch(stmt, colbinds);
  if (r) return -1;
  r = CALL_taos_stmt_add_batch(stmt);
  if (r) return -1;

  return CALL_taos_stmt_execute(stmt);
}

static int flaw_case_prepare_database(TAOS *taos)
{
  TAOS_RES   *res  = NULL;

  res = CALL_taos_query(taos, "drop database if exists foo");
  if (!res) return -1;
  if (res) { CALL_taos_free_result(res); res = NULL; }

  res = CALL_taos_query(taos, "create database foo");
  if (!res) return -1;
  if (res) { CALL_taos_free_result(res); res = NULL; }

  res = CALL_taos_query(taos, "use foo");
  if (!res) return -1;
  if (res) { CALL_taos_free_result(res); res = NULL; }

  res = CALL_taos_query(taos, "create table st (ts timestamp, age int) tags (name varchar(20))");
  if (!res) return -1;
  if (res) { CALL_taos_free_result(res); res = NULL; }

  return 0;
}

static int flaw_case_under_taos(TAOS *taos)
{
  int r = 0;
  TAOS_STMT  *stmt = NULL;

  stmt = CALL_taos_stmt_init(taos);
  if (!stmt) return -1;

  int tagNum = 0;
  TAOS_FIELD_E *tags = NULL;
  int colNum = 0;
  TAOS_FIELD_E *cols = NULL;

  r = flaw_case_under_stmt(stmt, &tagNum, &tags, &colNum, &cols);

  // don't forget, is's application's duty to free
  if (tags) { free(tags); tags = NULL; }
  if (cols) { free(cols); cols = NULL; }

  CALL_taos_stmt_close(stmt);
  /* ignore return code */

  return r;
}

static int flaw_case_inited(void)
{
  int   r    = 0;
  TAOS *taos = NULL;

  taos = CALL_taos_connect(NULL, NULL, NULL, NULL, 0);
  if (!taos) return -1;

  do {
    r = flaw_case_prepare_database(taos);
    if (r) break;

    r = flaw_case_under_taos(taos);
  } while (0);

  CALL_taos_close(taos);

  return r;
}

static int flaw_case(void)
{
  int r = 0;

  int n = 1;
  for (int i=0; i<n; ++i) {
    r = flaw_case_inited();
    if (r) break;
  }

  return r;
}

static int _flaw_case1_step3(TAOS_STMT *stmt)
{
  int r = 0;
  const char *sql = "select table_name, db_name from information_schema.ins_tables t where t.db_name like ?";
  r = CALL_taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  if (r) return -1;

  int is_insert = 0;
  r = CALL_taos_stmt_is_insert(stmt, &is_insert);
  if (r) return -1;
  if (is_insert) return -1;

  int nums = 0;
  r = CALL_taos_stmt_num_params(stmt, &nums);
  if (r) return -1;
  if (nums != 1) return -1;

  const char *db_name = "foo";
  int32_t length = (int32_t)strlen(db_name);
  char is_null = 0;

  TAOS_MULTI_BIND mb = {0};
  mb.buffer_type             = TSDB_DATA_TYPE_VARCHAR;
  mb.buffer                  = (char*)db_name;
  mb.buffer_length           = length;
  mb.length                  = &length;
  mb.is_null                 = &is_null;
  mb.num                     = 1;

  r = CALL_taos_stmt_bind_param_batch(stmt, &mb);
  if (r) return -1;

  r = CALL_taos_stmt_add_batch(stmt);
  if (r) return -1;

  r = CALL_taos_stmt_execute(stmt);
  if (r) return -1;

  return 0;
}

static int _flaw_case1_step2(TAOS *taos)
{
  int r = 0;

  const char *sqls[] = {
    "drop database if exists foo",
    "create database if not exists foo",
    "drop table if exists foo.t",
    "create table foo.t(ts timestamp,name varchar(20))",
  };
  for (size_t i=0; i<sizeof(sqls)/sizeof(sqls[0]); ++i) {
    const char *sql = sqls[i];
    TAOS_RES *res = CALL_taos_query(taos,sql);
    if (!res) return -1;
    CALL_taos_free_result(res);
  }

  TAOS_STMT *stmt = NULL;

  stmt = CALL_taos_stmt_init(taos);
  if (!stmt) return -1;

  r = _flaw_case1_step3(stmt);

  CALL_taos_stmt_close(stmt);

  return r;
}

static int _flaw_case1_step1(void)
{
  const char *ip = NULL;
  const char *user = NULL;
  const char *pass = NULL;
  const char *db = NULL;
  uint16_t port = 0;
  TAOS *taos = CALL_taos_connect(ip,user,pass,db,port);
  if (!taos) return -1;

  int r = 0;
  r = _flaw_case1_step2(taos);
  if (r == 0) {
    r = _flaw_case1_step2(taos);
  }

  CALL_taos_close(taos);

  return r;
}

static int flaw_case1(void)
{
  int r = 0;

  r = _flaw_case1_step1();

  return r;
}

static int test_charset_step2(TAOS_RES *res)
{
  int numOfRows                = 0;
  TAOS_ROW rows                = NULL;
  TAOS_FIELD *fields           = NULL;
  int nr_fields                = 0;

  int r = 0;
  nr_fields = CALL_taos_field_count(res);
  if (nr_fields == -1) return -1;
  A(nr_fields == 2, "internal logic error");

  fields = CALL_taos_fetch_fields(res);
  if (!fields) return -1;

  r = CALL_taos_fetch_block_s(res, &numOfRows, &rows);
  if (r) return -1;
  A(numOfRows == 1, "internal logic error");

  TAOS_FIELD *field;
  int col;

  col = 0;
  field = fields + col;
  const char *name = NULL;
  int name_len = 0;
  r = helper_get_data_len(res, field, rows, 0, col, &name, &name_len);
  if (r) return -1;

  col = 1;
  field = fields + col;
  const char *wname = NULL;
  int wname_len = 0;
  r = helper_get_data_len(res, field, rows, 0, col, &wname, &wname_len);
  if (r) return -1;

  D("name:%.*s", name_len, name);
  D("wname:%.*s", wname_len, wname);

  return 0;
}

static int test_charset_step1(TAOS *taos)
{
  const char *sql = "select name, wname from wall.t";
  TAOS_RES *res = CALL_taos_query(taos,sql);
  if (!res) return -1;

  int r = test_charset_step2(res);

  CALL_taos_free_result(res);

  return r;
}

static int test_charset(void)
{
  const char *ip = NULL;
  const char *user = NULL;
  const char *pass = NULL;
  const char *db = NULL;
  uint16_t port = 0;
  TAOS *taos = CALL_taos_connect(ip,user,pass,db,port);
  if (!taos) return -1;

  int r = 0;
  r = test_charset_step1(taos);

  CALL_taos_close(taos);

  return r;
}

static int _flaw_case2_step3(TAOS_STMT *stmt)
{
  int r = 0;
  const char *sql = "insert into t (ts, name) values (?, ?)";
  r = CALL_taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  if (r) return -1;

  int is_insert = 0;
  r = CALL_taos_stmt_is_insert(stmt, &is_insert);
  if (r) return -1;
  if (!is_insert) return -1;

  int fieldNum = 0;
  TAOS_FIELD_E *fields = NULL;
  r = CALL_taos_stmt_get_tag_fields(stmt, &fieldNum, &fields);
  if (r == 0) return -1;
  if (fieldNum > 0) return -1;
  if (fields) return -1;

  int colNum = 0;
  TAOS_FIELD_E *cols = NULL;
  r = CALL_taos_stmt_get_col_fields(stmt, &colNum, &cols);
  if (r) return -1;
  if (colNum != 2) return -1;
  if (cols == NULL) return -1;

  CALL_taos_stmt_reclaim_fields(stmt, cols);

  return 0;
}

static int _flaw_case2_step2(TAOS *taos)
{
  int r = 0;

  const char *sqls[] = {
    "drop database if exists foo",
    "create database if not exists foo",
    "use foo",
    "drop table if exists t",
    "create table t(ts timestamp,name varchar(20))",
  };
  for (size_t i=0; i<sizeof(sqls)/sizeof(sqls[0]); ++i) {
    const char *sql = sqls[i];
    TAOS_RES *res = CALL_taos_query(taos,sql);
    if (!res) return -1;
    CALL_taos_free_result(res);
  }

  TAOS_STMT *stmt = NULL;

  stmt = CALL_taos_stmt_init(taos);
  if (!stmt) return -1;

  r = _flaw_case2_step3(stmt);

  CALL_taos_stmt_close(stmt);

  return r;
}

static int _flaw_case2_step1(void)
{
  const char *ip = NULL;
  const char *user = NULL;
  const char *pass = NULL;
  const char *db = NULL;
  uint16_t port = 0;
  TAOS *taos = CALL_taos_connect(ip,user,pass,db,port);
  if (!taos) return -1;

  int r = 0;
  r = _flaw_case2_step2(taos);

  CALL_taos_close(taos);

  return r;
}

static int flaw_case2(void)
{
  int r = 0;

  r = _flaw_case2_step1();

  return r;
}

static int conformance_taos_query_with_question_mark(TAOS *taos, const char *sql)
{
  TAOS_RES *res = CALL_taos_query(taos, sql);
  int e = CALL_taos_errno(res);

  if (res) {
    CALL_taos_free_result(res);
    res = NULL;
  }

  if (e == 0) return 0;

  return -1;
}

static int conformance_taos_stmt_prepare_step1(TAOS_STMT *stmt, const char *sql)
{
  int r = 0;
  r = CALL_taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  if (r) return -1;

  int insert = 0;
  r = CALL_taos_stmt_is_insert(stmt, &insert);
  if (r) return -1;

  if (!insert) {
    int nr_params = 0;
    r = CALL_taos_stmt_num_params(stmt, &nr_params);
    if (r) return -1;
    // [-2147474431/0x80002401]invalid catalog input parameters
    for (int i=0; i<nr_params; ++i) {
      int type  = 0;
      int bytes = 0;
      r = CALL_taos_stmt_get_param(stmt, i, &type, &bytes);
      if (r) return -1;
    }
    return 0;
  }

  return 0;
}

static int conformance_taos_stmt_prepare_without_question_mark(TAOS *taos, const char *sql)
{
  int r = 0;
  TAOS_STMT *stmt = CALL_taos_stmt_init(taos);
  if (!stmt) return -1;

  r = conformance_taos_stmt_prepare_step1(stmt, sql);

  CALL_taos_stmt_close(stmt);

  return r;
}

static void tmq_commit_cb_print(tmq_t* tmq, int32_t code, void* param) {
  fprintf(stderr, "tmq_commit_cb_print() code: %d, tmq: %p, param: %p\n", code, tmq, param);
}

static tmq_t* build_consumer() {
  tmq_conf_res_t code;
  tmq_conf_t*    conf = tmq_conf_new();

  code = tmq_conf_set(conf, "enable.auto.commit", "true");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "auto.commit.interval.ms", "100");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "group.id", "cgrpName");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "client.id", "user defined name");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "td.connect.user", "root");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "td.connect.pass", "taosdata");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "auto.offset.reset", "earliest");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "experimental.snapshot.enable", "false");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);

  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  tmq_conf_destroy(conf);
  return tmq;
}

static tmq_list_t* build_topic_list() {
  tmq_list_t* topicList = tmq_list_new();
  int32_t     code = tmq_list_append(topicList, "topicname");
  if (code) {
    return NULL;
  }
  return topicList;
}

static int running = 1;

static int32_t msg_process(TAOS_RES* msg) {
  char    buf[1024];
  int32_t rows = 0;

  const char* topicName = tmq_get_topic_name(msg);
  const char* dbName = tmq_get_db_name(msg);
  int32_t     vgroupId = tmq_get_vgroup_id(msg);

  fprintf(stderr, "topic: %s\n", topicName);
  fprintf(stderr, "db: %s\n", dbName);
  fprintf(stderr, "vgroup id: %d\n", vgroupId);

  while (1) {
    TAOS_ROW row = taos_fetch_row(msg);
    if (row == NULL) break;

    TAOS_FIELD* fields = taos_fetch_fields(msg);
    int32_t     numOfFields = taos_field_count(msg);
    // int32_t*    length = taos_fetch_lengths(msg);
    // int32_t     precision = taos_result_precision(msg);
    rows++;
    taos_print_row(buf, row, fields, numOfFields);
    fprintf(stderr, "row content: %s\n", buf);
  }

  return rows;
}

static void basic_consume_loop(tmq_t* tmq) {
  int32_t totalRows = 0;
  int32_t msgCnt = 0;
  int32_t timeout = 100;
  while (running) {
    fprintf(stderr, "polling timeout:%d...\n", timeout);
    TAOS_RES* tmqmsg = tmq_consumer_poll(tmq, timeout);
    fprintf(stderr, "polled tmqmsg:%p\n", tmqmsg);
    if (tmqmsg) {
      msgCnt++;
      totalRows += msg_process(tmqmsg);
      taos_free_result(tmqmsg);
    } else {
      break;
    }
  }

  fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
}

static int conformance_mq_run(void)
{
  int r = 0;

  tmq_t* tmq = build_consumer();
  if (NULL == tmq) {
    fprintf(stderr, "%% build_consumer() fail!\n");
    return -1;
  }

  tmq_list_t* topic_list = build_topic_list();
  if (NULL == topic_list) {
    tmq_consumer_close(tmq);
    return -1;
  }

  r = tmq_subscribe(tmq, topic_list);
  if (r) {
    fprintf(stderr, "%% Failed to tmq_subscribe(): %s\n", tmq_err2str(r));
  }
  tmq_list_destroy(topic_list);

  if (r == 0) {
    basic_consume_loop(tmq);
  }

  tmq_unsubscribe(tmq);
  int rr = tmq_consumer_close(tmq);
  if (rr) {
    fprintf(stderr, "%% Failed to close consumer: %s\n", tmq_err2str(rr));
    r = rr;
  }

  return r ? -1 : 0;
}

static int conformance_mq(TAOS *taos)
{
  int r = 0;

  const char *sqls[] = {
    "drop topic if exists topicname",
    "drop database if exists tmqdb",
    "create database tmqdb",
    "create table tmqdb.stb (ts timestamp, c1 int, c2 float, c3 varchar(16)) tags(t1 int, t3 varchar(16))",
    "create table tmqdb.ctb0 using tmqdb.stb tags(0, 'subtable0')",
    "create table tmqdb.ctb1 using tmqdb.stb tags(1, 'subtable1')",
    "create table tmqdb.ctb2 using tmqdb.stb tags(2, 'subtable2')",
    "create table tmqdb.ctb3 using tmqdb.stb tags(3, 'subtable3')",
    "insert into tmqdb.ctb0 values(now, 0, 0, 'a0')(now+1s, 0,   0, 'a00')(now+2s, 0,   0, 'a00')",
    "insert into tmqdb.ctb1 values(now, 1, 1, 'a1')(now+1s, 11, 11, 'a11')(now+2s, 11, 11, 'a11')",
    "insert into tmqdb.ctb2 values(now, 2, 2, 'a1')(now+1s, 22, 22, 'a22')(now+2s, 22, 22, 'a22')",
    "insert into tmqdb.ctb3 values(now, 3, 3, 'a1')(now+1s, 33, 33, 'a33')(now+2s, 33, 33, 'a33')",
    "insert into tmqdb.ctb0 values(now, 4, 4, 'a4')(now+1s, 44, 44, 'a44')(now+2s, 44, 44, 'a44')",
    "insert into tmqdb.ctb1 values(now, 5, 5, 'a5')(now+1s, 55, 55, 'a55')(now+2s, 55, 55, 'a55')",
    "insert into tmqdb.ctb2 values(now, 6, 6, 'a6')(now+1s, 66, 66, 'a66')(now+2s, 66, 66, 'a66')",
    "insert into tmqdb.ctb3 values(now, 7, 7, 'a7')(now+1s, 77, 77, 'a77')(now+2s, 77, 77, 'a77')",
    "use tmqdb",
    "create topic topicname as select ts, c1, c2, c3, tbname from tmqdb.stb where c1 > 1",
  };
  for (size_t i=0; i<sizeof(sqls)/sizeof(sqls[0]); ++i) {
    const char *sql = sqls[i];
    TAOS_RES *res = CALL_taos_query(taos,sql);
    if (!res) return -1;
    CALL_taos_free_result(res);
  }

  for (int i=0; i<1; ++i) {
    r = conformance_mq_run();
    if (r) break;
  }

  return r;
}

typedef struct prepare_checker_s           prepare_checker_t;
struct prepare_checker_s {
  const char                   *sql;
  int                           tbname_required;
  int                           nr_tags;
  int                           nr_cols;
  int                           nr_params;
};

static int conformance_prepare_check(prepare_checker_t *checker, int tbname_required, int nr_tags, int nr_cols, int nr_params)
{
  if (checker->tbname_required != tbname_required) {
    E("`%s`: expect `tbname_required` to be (%d), but got ==(%d)==", checker->sql, checker->tbname_required, tbname_required);
    return -1;
  }
  if (checker->nr_tags != nr_tags) {
    E("`%s`: expect `nr_tags` to be (%d), but got ==(%d)==", checker->sql, checker->nr_tags, nr_tags);
    return -1;
  }
  if (checker->nr_cols != nr_cols) {
    E("`%s`: expect `nr_cols` to be (%d), but got ==(%d)==", checker->sql, checker->nr_cols, nr_cols);
    return -1;
  }
  if (checker->nr_params != nr_params) {
    E("`%s`: expect `nr_params` to be (%d), but got ==(%d)==", checker->sql, checker->nr_params, nr_params);
    return -1;
  }

  return 0;
}

static int conformance_prepare_non_insert(TAOS_STMT *stmt, prepare_checker_t *checker)
{
  int r = 0;
  int nr_params = 0;
  r = CALL_taos_stmt_num_params(stmt, &nr_params);
  if (r) return -1;

  return conformance_prepare_check(checker, 0, 0, 0, nr_params);
}

static int conformance_prepare_insert_tags_cols_tbname_required(TAOS_STMT *stmt, prepare_checker_t *checker,
    int *nr_tags, TAOS_FIELD_E **tags, int *nr_cols, TAOS_FIELD_E **cols)
{
  int r = 0;
  r = CALL_taos_stmt_set_tbname(stmt, "__hard_coded_fake_name__");
  if (r) return -1;

  A(*nr_tags == 0, "");
  A(*tags == NULL, "");

  r = CALL_taos_stmt_get_tag_fields(stmt, nr_tags, tags);
  if (r) return -1;
  r = CALL_taos_stmt_get_col_fields(stmt, nr_cols, cols);
  if (r) return -1;

  return conformance_prepare_check(checker, 1, *nr_tags, *nr_cols, 0);
}

static int conformance_prepare_insert_tags_cols_no_tbname_required(TAOS_STMT *stmt, prepare_checker_t *checker,
    int *nr_tags, TAOS_FIELD_E **tags, int *nr_cols, TAOS_FIELD_E **cols)
{
  (void)tags;
  int r = 0;

  r = CALL_taos_stmt_get_col_fields(stmt, nr_cols, cols);
  if (r) return -1;

  return conformance_prepare_check(checker, 0, *nr_tags, *nr_cols, 0);
}

static int conformance_prepare_insert_tags_cols(TAOS_STMT *stmt, prepare_checker_t *checker,
    int *nr_tags, TAOS_FIELD_E **tags, int *nr_cols, TAOS_FIELD_E **cols)
{
  int r = 0;
  r = CALL_taos_stmt_get_tag_fields(stmt, nr_tags, tags);
  if (r == 0) {
    r = CALL_taos_stmt_get_col_fields(stmt, nr_cols, cols);
    if (r) return -1;

    return conformance_prepare_check(checker, 0, *nr_tags, *nr_cols, 0);
  }

  int e = CALL_taos_errno(NULL);
  if (e == TSDB_CODE_TSC_STMT_TBNAME_ERROR) {
    return conformance_prepare_insert_tags_cols_tbname_required(stmt, checker, nr_tags, tags, nr_cols, cols);
  } else if (e == TSDB_CODE_TSC_STMT_API_ERROR) {
    return conformance_prepare_insert_tags_cols_no_tbname_required(stmt, checker, nr_tags, tags, nr_cols, cols);
  } else {
    return -1;
  }
}

static int conformance_prepare_insert(TAOS_STMT *stmt, prepare_checker_t *checker)
{
  int r = 0;

  int nr_tags = 0;
  TAOS_FIELD_E *tags = NULL;
  int nr_cols = 0;
  TAOS_FIELD_E *cols = NULL;

  r = conformance_prepare_insert_tags_cols(stmt, checker, &nr_tags, &tags, &nr_cols, &cols);

  if (tags) {
    CALL_taos_stmt_reclaim_fields(stmt, tags);
    tags = NULL;
  }

  if (cols) {
    CALL_taos_stmt_reclaim_fields(stmt, cols);
    cols = NULL;
  }

  return r;
}

static int conformance_prepare_with_stmt(TAOS_STMT *stmt, prepare_checker_t *checker)
{
  int r = 0;

  r = CALL_taos_stmt_prepare(stmt, checker->sql, (unsigned long)strlen(checker->sql));
  if (r) return -1;

  int is_insert = 0;
  r = CALL_taos_stmt_is_insert(stmt, &is_insert);
  if (r) return -1;

  if (!is_insert) {
    return conformance_prepare_non_insert(stmt, checker);
  }
  return conformance_prepare_insert(stmt, checker);
}

static int conformance_prepare(TAOS *taos)
{
  int r = 0;
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
    TAOS_RES *res = CALL_taos_query(taos, sql);
    int e = CALL_taos_errno(NULL);
    if (res) CALL_taos_free_result(res);
    if (e) {
      r = -1;
      break;
    }
  }
  if (r) return -1;

  prepare_checker_t checkers[] = {
    // subtable insert
    {"insert into suzhou using s tags (?) values (?, ?)", 0, 1, 2, 0},
    // NOTE: taosc specific behavior
    {"insert into suzhou using s tags (3) values (?, ?)", 0, 1, 2, 0},
    // NOTE: taosc specific behavior
    {"insert into suzhou using s tags (3) values (now(), 4)", 0, 1, 2, 0},
    // subtable insert
    {"insert into suzhou using s tags (?) (ts) values (?)", 0, 1, 1, 0},
    // normal table insert
    {"insert into t (ts, v) values (?, ?)", 0, 0, 2, 0},
    // subtable insert with subtable-marker
    {"insert into ? using s tags (?) values (?, ?)", 1, 1, 2, 0},
    // normal table select
    {"select * from t where ts > ? and v = ?", 0, 0, 0, 2},
    // NOTE: taosc flaw, shall report `x` is not valid colname
    {"select * from t where x = ?", 0, 0, 0, 1},
  };

  TAOS_STMT *stmt = CALL_taos_stmt_init(taos);
  if (!stmt) return -1;

  for (size_t i=0; i<sizeof(checkers)/sizeof(checkers[0]); ++i) {
    prepare_checker_t *checker = checkers + i;
    r = conformance_prepare_with_stmt(stmt, checker);
    if (r) break;
  }

  CALL_taos_stmt_close(stmt);

  return r;
}

static int conformance_db(TAOS *taos)
{
  // NOTE: bad taste for taos_get_current_db
  char buf[4096] = "hello";
  int r = 0;

  int len;
  TAOS_RES *res;
  int e;

  CALL_taos_reset_current_db(taos);

  strcpy(buf, "hello");
  len = 0;
  r = CALL_taos_get_current_db(taos, buf, sizeof(buf), &len);
  if (r) return -1;
  if (strcmp(buf, "")) {
    E("`` is expected, but got ==%s==", buf);
    return -1;
  }

  res = CALL_taos_query(taos, "use information_schema");
  e = taos_errno(res);
  if (e) E("no error expected, but got ==%d==%s==", e, taos_errstr(res));
  if (res) CALL_taos_free_result(res);
  if (e) return -1;

  strcpy(buf, "hello");
  len = 0;
  r = CALL_taos_get_current_db(taos, buf, 2, &len);
  if (r == 0) {
    E("failed is expected, but got ==%d==", r);
    return -1;
  }
  if (len != 19) {
    E("len is expected to be `19`, but got ==%d==", len);
    return -1;
  }
  if (strcmp(buf, "i")) {
    E("`i` is expected, but got ==%s==", buf);
    return -1;
  }

  strcpy(buf, "hello");
  len = 123;
  r = CALL_taos_get_current_db(taos, buf, sizeof(buf), &len);
  if (r) return -1;
  if (strcmp(buf, "information_schema")) {
    E("`information_schema` is expected, but got ==%s==", buf);
    return -1;
  }

  CALL_taos_reset_current_db(taos);

  strcpy(buf, "hello");
  len = 789;
  r = CALL_taos_get_current_db(taos, buf, sizeof(buf), &len);
  if (r) return -1;
  if (strcmp(buf, "")) {
    E("`` is expected, but got ==%s==", buf);
    return -1;
  }

  return 0;
}

static int conformance_tests_with_taos(TAOS *taos)
{
  int r = 0;

  r = conformance_db(taos);
  if (r) return -1;

  if (0) {
    // memory leakage
    for (int i=0; i<1; ++i) {
      r = conformance_mq(taos);
      if (r) break;
    }
    if (r) return -1;
  }

  r = conformance_prepare(taos);
  if (r) return -1;

  r = conformance_taos_query_with_question_mark(taos, "select * from information_schema.ins_configs where name = ?");
  if (r == 0) {
    E("expect fail, but success");
    return -1;
  }

  r = conformance_taos_stmt_prepare_without_question_mark(taos, "select * from information_schema.ins_configs where name = 'not_exists'");
  if (r == 0) {
    E("expect fail, but success");
    return -1;
  }

  return 0;
}

static int conformance_tests(void)
{
  const char *ip = NULL;
  const char *user = NULL;
  const char *pass = NULL;
  const char *db = NULL;
  uint16_t port = 0;
  TAOS *taos = CALL_taos_connect(ip,user,pass,db,port);
  if (!taos) return -1;

  int r = 0;
  r = conformance_tests_with_taos(taos);

  CALL_taos_close(taos);

  return r;
}

static int conformance_ts_with_taos(TAOS *taos)
{
  const char *sqls[] = {
    "drop database if exists bar",
    "create database bar",
    "create table bar.foo (ts timestamp, name varchar(20))",
    "insert into bar.foo (ts, name) values (1662861448752, 'hello')",
  };
  for (size_t i=0; i<sizeof(sqls)/sizeof(sqls[0]); ++i) {
    const char *sql = sqls[i];
    TAOS_RES *res = CALL_taos_query(taos, sql);
    if (!res) {
      E("res expected, but got ==null==");
      return -1;
    }
    CALL_taos_free_result(res);
    res = NULL;
  }

  const char *sql = "select ts from bar.foo";
  TAOS_RES *res = CALL_taos_query(taos, sql);
  if (!res) {
    E("res expected, but got ==null==");
    return -1;
  }

  do {
    TAOS_FIELD *fields = CALL_taos_fetch_fields(res);
    if (!fields) {
      E("fields expected, but got ==null==");
      break;
    }

    TAOS_ROW record = CALL_taos_fetch_row(res);
    if (!record) {
      E("record expected, but got ==null==");
      break;
    }

    int *offsets = CALL_taos_get_column_data_offset(res, 0);
    if (offsets) {
      E("non offset expected, but got ==%p==", offsets);
      break;
    }

    char *col = record[0];

    TAOS_FIELD *field = fields + 0;
    if (field->type != TSDB_DATA_TYPE_TIMESTAMP) {
      E("TSDB_DATA_TYPE_TIMESTAMP expected, but got ==%s==", taos_data_type(field->type));
      break;
    }
    int64_t v = *(int64_t*)col;
    if (v != 1662861448752) {
      E("1662861448752 expected, but got ==%" PRId64 "==", v);
      break;
    }

    CALL_taos_free_result(res);
    return 0;
  } while (0);

  CALL_taos_free_result(res);

  return -1;
}

static int conformance_ts(void)
{
  const char *ip = NULL;
  const char *user = NULL;
  const char *pass = NULL;
  const char *db = NULL;
  uint16_t port = 0;
  TAOS *taos = CALL_taos_connect(ip,user,pass,db,port);
  if (!taos) return -1;

  int r = 0;
  r = conformance_ts_with_taos(taos);

  CALL_taos_close(taos);

  return r;
}

static int tests(int argc, char *argv[])
{
  int r = 0;

  if (0) {
    // taosc: 4bc0d33db31401cab51317d4962b3df2870bab01
    //        see memleak result from premature-abort-after-taos_stmt_set_tbname
    r = flaw_case();
    if (r) {
      fprintf(stderr, "==failure==\n");
      return 1;
    } else {
      fprintf(stderr, "==success==\n");
      return 0;
    }
  }
  if (0) {
    r = flaw_case1();
    if (r == 0) {
      fprintf(stderr, "seems like the flaw is corrected\n");
    }
    return !r;
  }
  if (0) {
    r = test_charset();
    D("==%s==", r ? "failure" : "success");
    if (1) return 1;
    return !!r;
  }
  if (0) {
    r = flaw_case2();
    if (r == 0) {
      fprintf(stderr, "seems like the flaw is corrected\n");
    }
    return !r;
  }

  r = conformance_ts();
  if (r) return -1;

  r = conformance_tests();
  if (r) return -1;

  r = process_by_args(argc, argv);

  return r;
}

int main(int argc, char *argv[])
{
  int r;
  r = CALL_taos_init();
  if (r == 0) {
    r = tests(argc, argv);
    CALL_taos_cleanup();
  }

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");

  return !!r;
}

