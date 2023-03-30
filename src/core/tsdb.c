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

#include "internal.h"

#include "tsdb.h"

#include "desc.h"
#include "errs.h"
#include "log.h"
#include "stmt.h"
#include "taos_helpers.h"

#include <errno.h>

static int _tsdb_timestamp_to_tm_local(int64_t val, int time_precision, struct tm *tm, int32_t *ms, int *w)
{
  time_t  tt;
  int32_t xms = 0;
  int xw;
  switch (time_precision) {
    case 2:
      tt = (time_t)(val / 1000000000);
      xms = val % 1000000000;
      xw = 9;
      break;
    case 1:
      tt = (time_t)(val / 1000000);
      xms = val % 1000000;
      xw = 6;
      break;
    case 0:
      tt = (time_t)(val / 1000);
      xms = val % 1000;
      xw = 3;
      break;
    default:
      OA_ILE(0);
      break;
  }

  if (tt <= 0 && xms < 0) {
    OA_NIY(0);
  }

  struct tm *p = localtime_r(&tt, tm);
  if (p != tm) return -1;
  if (ms) *ms = xms;
  if (w)  *w  = xw;
  return 0;
}

int tsdb_timestamp_to_SQL_C_TYPE_TIMESTAMP(int64_t val, int time_precision, SQL_TIMESTAMP_STRUCT *ts)
{
  int32_t ms = 0;
  int w;
  struct tm ptm = {0};
  int r = _tsdb_timestamp_to_tm_local(val, time_precision, &ptm, &ms, &w);
  if (r) return -1;
  ts->year       = ptm.tm_year + 1900;
  ts->month      = ptm.tm_mon + 1;
  ts->day        = ptm.tm_mday;
  ts->hour       = ptm.tm_hour;
  ts->minute     = ptm.tm_min;
  ts->second     = ptm.tm_sec;
  ts->fraction   = ms;
  return 0;
}

int tsdb_timestamp_to_string(int64_t val, int time_precision, char *buf, size_t len)
{
  int n;
  int32_t ms = 0;
  int w;
  struct tm ptm = {0};
  int r = _tsdb_timestamp_to_tm_local(val, time_precision, &ptm, &ms, &w);
  if (r) return -1;

  n = snprintf(buf, len,
      "%04d-%02d-%02d %02d:%02d:%02d.%0*d",
      ptm.tm_year + 1900, ptm.tm_mon + 1, ptm.tm_mday,
      ptm.tm_hour, ptm.tm_min, ptm.tm_sec,
      w, ms);

  OA_ILE(n > 0);

  return n;
}

void tsdb_stmt_reset(tsdb_stmt_t *stmt)
{
  if (!stmt) return;
  tsdb_stmt_close_result(stmt);
  if (stmt->stmt) {
    int r = CALL_taos_stmt_close(stmt->stmt);
    OA_NIY(r == 0);
    stmt->stmt = NULL;
  }
}

void tsdb_stmt_release(tsdb_stmt_t *stmt)
{
  if (!stmt) return;
  tsdb_stmt_reset(stmt);

  tsdb_res_release(&stmt->res);
  tsdb_params_release(&stmt->params);
  stmt->owner = NULL;
}

void tsdb_params_reset_tag_fields(tsdb_params_t *params)
{
  if (params->tag_fields) {
    CALL_taos_stmt_reclaim_fields(params->owner->stmt, params->tag_fields);
    params->tag_fields = NULL;
  }
  params->nr_tag_fields = 0;
}

void tsdb_params_reset_col_fields(tsdb_params_t *params)
{
  if (params->col_fields) {
    if (params->is_insert_stmt) {
      CALL_taos_stmt_reclaim_fields(params->owner->stmt, params->col_fields);
    } else {
      mem_reset(&params->mem);
    }
    params->col_fields = NULL;
  }
  params->nr_col_fields = 0;
}

void tsdb_params_reset(tsdb_params_t *params)
{
  tsdb_params_reset_tag_fields(params);
  tsdb_params_reset_col_fields(params);

  TOD_SAFE_FREE(params->subtbl);
  params->prepared = 0;
  params->is_insert_stmt = 0;
  params->subtbl_required = 0;
}

void tsdb_params_release(tsdb_params_t *params)
{
  tsdb_params_reset(params);

  mem_release(&params->mem);
  params->prepared = 0;
  params->is_insert_stmt = 0;
  params->subtbl_required = 0;
  params->owner = NULL;
}

void tsdb_binds_reset(tsdb_binds_t *tsdb_binds)
{
  tsdb_binds->nr = 0;
}

void tsdb_binds_release(tsdb_binds_t *tsdb_binds)
{
  tsdb_binds_reset(tsdb_binds);

  TOD_SAFE_FREE(tsdb_binds->mbs);
  tsdb_binds->cap = 0;
}


void tsdb_res_reset(tsdb_res_t *res)
{
  if (!res) return;
  tsdb_rows_block_reset(&res->rows_block);
  tsdb_fields_reset(&res->fields);
  if (res->res) {
    if (res->res_is_from_taos_query) {
      CALL_taos_free_result(res->res);
      res->res_is_from_taos_query = 0;
    }
    res->res = NULL;
  }
  res->affected_row_count = 0;
  res->time_precision     = 0;
}

void tsdb_res_release(tsdb_res_t *res)
{
  if (!res) return;
  tsdb_res_reset(res);

  tsdb_rows_block_release(&res->rows_block);
  tsdb_fields_release(&res->fields);
}

void tsdb_fields_reset(tsdb_fields_t *fields)
{
  if (!fields) return;
  fields->fields         = NULL;
  fields->nr             = 0;
}

void tsdb_fields_release(tsdb_fields_t *fields)
{
  if (!fields) return;
  tsdb_fields_reset(fields);
}

void tsdb_rows_block_reset(tsdb_rows_block_t *rows_block)
{
  if (!rows_block) return;
  rows_block->rows                 = NULL;
  rows_block->nr                   = 0;
  rows_block->pos                  = 0;
}

void tsdb_rows_block_release(tsdb_rows_block_t *rows_block)
{
  if (!rows_block) return;
  tsdb_rows_block_reset(rows_block);
}

int tsdb_binds_keep(tsdb_binds_t *tsdb_binds, int nr_params)
{
  if (nr_params > tsdb_binds->cap) {
    int cap = (nr_params + 15) / 16 * 16;
    TAOS_MULTI_BIND *mbs = (TAOS_MULTI_BIND*)realloc(tsdb_binds->mbs, sizeof(*mbs) * cap);
    if (!mbs) return -1;
    tsdb_binds->mbs = mbs;
    tsdb_binds->cap = cap;
  }
  tsdb_binds->nr = nr_params;
  return 0;
}

static SQLRETURN _stmt_post_query(tsdb_stmt_t *stmt)
{
  int e;
  const char *estr;

  tsdb_res_t          *res         = &stmt->res;
  tsdb_fields_t       *fields      = &res->fields;

  e = CALL_taos_errno(res->res);
  estr = CALL_taos_errstr(res->res);

  if (e) {
    stmt_append_err_format(stmt->owner, "HY000", e, "General error:[taosc]%s", estr);
    return SQL_ERROR;
  } else if (res->res) {
    res->time_precision = CALL_taos_result_precision(res->res);
    if (res->time_precision < 0 || res->time_precision > 2) {
      stmt_append_err_format(stmt->owner, "HY000", e, "General error:time_precision [%d] out of range", res->time_precision);
      return SQL_ERROR;
    }
    res->affected_row_count = CALL_taos_affected_rows(res->res);
    fields->nr = CALL_taos_field_count(res->res);
    if (fields->nr > 0) {
      fields->fields = CALL_taos_fetch_fields(res->res);
    }
  }

  return SQL_SUCCESS;
}

static SQLRETURN _query(stmt_base_t *base, const char *sql)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;

  tsdb_res_t          *res         = &stmt->res;
  tsdb_res_reset(res);
  res->res = CALL_taos_query(stmt->owner->conn->taos, sql);
  res->res_is_from_taos_query = res->res ? 1 : 0;

  return _stmt_post_query(stmt);
}

static SQLRETURN _execute(stmt_base_t *base)
{
  int r = 0;

  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;

  tsdb_res_t          *res         = &stmt->res;
  tsdb_res_reset(res);

  r = CALL_taos_stmt_execute(stmt->stmt);
  if (r) {
    stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
    return SQL_ERROR;
  }

  res->res = CALL_taos_stmt_use_result(stmt->stmt);
  res->res_is_from_taos_query = 0;

  return _stmt_post_query(stmt);
}

static SQLRETURN _get_fields(stmt_base_t *base, TAOS_FIELD **fields, size_t *nr)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;
  tsdb_res_t           *res          = &stmt->res;

  *fields = res->fields.fields;
  *nr     = res->fields.nr;

  return SQL_SUCCESS;
}

static SQLRETURN _fetch_row(stmt_base_t *base)
{
  SQLRETURN sr = SQL_SUCCESS;

  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;
  tsdb_res_t           *res          = &stmt->res;
  tsdb_rows_block_t    *rows_block   = &res->rows_block;

again:
  // TODO: before and after
  if (rows_block->pos >= rows_block->nr) {
    sr = tsdb_stmt_fetch_rows_block(stmt);
    if (sr == SQL_NO_DATA) return SQL_NO_DATA;
    if (sr != SQL_SUCCESS) return SQL_ERROR;
    goto again;
  }
  ++rows_block->pos;
  return SQL_SUCCESS;
}

static SQLRETURN _describe_param(stmt_base_t *base,
      SQLUSMALLINT    ParameterNumber,
      SQLSMALLINT    *DataTypePtr,
      SQLULEN        *ParameterSizePtr,
      SQLSMALLINT    *DecimalDigitsPtr,
      SQLSMALLINT    *NullablePtr)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;
  return tsdb_stmt_describe_param(stmt, ParameterNumber, DataTypePtr, ParameterSizePtr, DecimalDigitsPtr, NullablePtr);
}

static SQLRETURN _get_num_params(stmt_base_t *base, SQLSMALLINT *ParameterCountPtr)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;
  SQLSMALLINT n = tsdb_stmt_get_count_of_tsdb_params(stmt);
  if (ParameterCountPtr) *ParameterCountPtr = n;
  return SQL_SUCCESS;
}

static SQLRETURN _check_params(stmt_base_t *base)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;
  return tsdb_stmt_check_parameters(stmt);
}

static SQLRETURN _tsdb_field_by_param(stmt_base_t *base, int i_param, TAOS_FIELD_E **field)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;
  TAOS_FIELD_E *p = tsdb_stmt_get_tsdb_field_by_tsdb_params(stmt, i_param);
  if (p == NULL) {
    stmt_append_err_format(stmt->owner, "HY000", 0,
        "General error:Parameter[%d] out of range",
        i_param + 1);
    return SQL_ERROR;
  }
  *field = p;
  return SQL_SUCCESS;
}

static SQLRETURN _row_count(stmt_base_t *base, SQLLEN *row_count_ptr)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;
  tsdb_res_t           *res          = &stmt->res;

  if (row_count_ptr) *row_count_ptr = res->affected_row_count;
  return SQL_SUCCESS;
}

static SQLRETURN _get_num_cols(stmt_base_t *base, SQLSMALLINT *ColumnCountPtr)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;

  tsdb_res_t        *res        = &stmt->res;
  tsdb_fields_t     *fields     = &res->fields;

  *ColumnCountPtr = (SQLSMALLINT)fields->nr;

  return SQL_SUCCESS;
}

static SQLRETURN _get_data(stmt_base_t *base, SQLUSMALLINT Col_or_Param_Num, tsdb_data_t *tsdb)
{
  tsdb_stmt_t *stmt = (tsdb_stmt_t*)base;

  tsdb_res_t          *res         = &stmt->res;
  tsdb_fields_t       *fields      = &res->fields;
  tsdb_rows_block_t   *rows_block  = &res->rows_block;

  int          i_row      = (int)rows_block->pos - 1;
  int          i_col      = Col_or_Param_Num - 1;
  TAOS_ROW     rows       = rows_block->rows;

  char buf[4096];
  int r = helper_get_tsdb(res->res, fields->fields, res->time_precision, rows, i_row, i_col, tsdb, buf, sizeof(buf));
  if (r) {
    stmt_append_err_format(stmt->owner, "HY000", 0, "General error:%.*s", (int)strlen(buf), buf);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

void tsdb_stmt_init(tsdb_stmt_t *stmt, stmt_t *owner)
{
  stmt->base.query                   = _query;
  stmt->base.execute                 = _execute;
  stmt->base.get_fields              = _get_fields;
  stmt->base.fetch_row               = _fetch_row;
  stmt->base.describe_param          = _describe_param;
  stmt->base.get_num_params          = _get_num_params;
  stmt->base.check_params            = _check_params;
  stmt->base.tsdb_field_by_param     = _tsdb_field_by_param;
  stmt->base.row_count               = _row_count;
  stmt->base.get_num_cols            = _get_num_cols;
  stmt->base.get_data                = _get_data;

  stmt->owner = owner;
  stmt->params.owner = stmt;
}

void tsdb_stmt_unprepare(tsdb_stmt_t *stmt)
{
  tsdb_params_reset(&stmt->params);
  stmt->prepared = 0;
}

void tsdb_stmt_close_result(tsdb_stmt_t *stmt)
{
  tsdb_res_reset(&stmt->res);
}

SQLRETURN tsdb_stmt_query(tsdb_stmt_t *stmt, const char *sql)
{
  return stmt->base.query(&stmt->base, sql);
}

static SQLRETURN _tsdb_stmt_describe_tags(tsdb_stmt_t *stmt)
{
  int r = 0;

  int tagNum = 0;
  TAOS_FIELD_E *tags = NULL;
  r = CALL_taos_stmt_get_tag_fields(stmt->stmt, &tagNum, &tags);
  if (r) {
    stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosc]%s", CALL_taos_errstr(NULL));
    return SQL_ERROR;
  }
  stmt->params.nr_tag_fields = tagNum;
  stmt->params.tag_fields    = tags;

  return SQL_SUCCESS;
}

static SQLRETURN _tsdb_stmt_describe_cols(tsdb_stmt_t *stmt)
{
  int r = 0;

  int colNum = 0;
  TAOS_FIELD_E *cols = NULL;
  r = CALL_taos_stmt_get_col_fields(stmt->stmt, &colNum, &cols);
  if (r) {
    stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosc]%s", CALL_taos_errstr(NULL));
    return SQL_ERROR;
  }
  stmt->params.nr_col_fields = colNum;
  stmt->params.col_fields    = cols;

  return SQL_SUCCESS;
}

static SQLRETURN _tsdb_stmt_get_taos_tags_cols_for_subtbled_insert(tsdb_stmt_t *stmt, int e)
{
  // fake subtbl name to get tags/cols params-info
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  r = CALL_taos_stmt_set_tbname(stmt->stmt, "__hard_coded_fake_name__");
  if (r) {
    stmt_append_err_format(stmt->owner, "HY000", e, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
    return SQL_ERROR;
  }

  sr = _tsdb_stmt_describe_tags(stmt);
  if (sr == SQL_ERROR) return SQL_ERROR;

  sr = _tsdb_stmt_describe_cols(stmt);
  if (sr == SQL_ERROR) return SQL_ERROR;

  // insert into ? ... will result in TSDB_CODE_TSC_STMT_TBNAME_ERROR
  stmt->params.subtbl_required = 1;
  return SQL_SUCCESS;
}

static SQLRETURN _tsdb_stmt_get_taos_tags_cols_for_normal_insert(tsdb_stmt_t *stmt, int e)
{
  // insert into t ... and t is normal tablename, will result in TSDB_CODE_TSC_STMT_API_ERROR
  SQLRETURN sr = SQL_SUCCESS;

  stmt->params.subtbl_required = 0;
  stmt_append_err(stmt->owner, "HY000", e, "General error:this is believed an non-subtbl insert statement");
  sr = _tsdb_stmt_describe_cols(stmt);

  return sr;
}

static SQLRETURN _tsdb_stmt_get_taos_tags_cols_for_insert(tsdb_stmt_t *stmt)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;
  int tagNum = 0;
  TAOS_FIELD_E *tag_fields = NULL;
  r = CALL_taos_stmt_get_tag_fields(stmt->stmt, &tagNum, &tag_fields);
  if (r) {
    int e = CALL_taos_errno(NULL);
    if (e == TSDB_CODE_TSC_STMT_TBNAME_ERROR) {
      sr = _tsdb_stmt_get_taos_tags_cols_for_subtbled_insert(stmt, r);
    } else if (e == TSDB_CODE_TSC_STMT_API_ERROR) {
      sr = _tsdb_stmt_get_taos_tags_cols_for_normal_insert(stmt, r);
    }
    if (tag_fields) {
      CALL_taos_stmt_reclaim_fields(stmt->stmt, tag_fields);
      tag_fields = NULL;
    }
  } else {
    // OA_NIY(tagNum == 0);
    // OA_NIY(tag_fields == NULL);
    OA_NIY(stmt->params.tag_fields == NULL);
    OA_NIY(stmt->params.nr_tag_fields == 0);
    stmt->params.tag_fields = tag_fields;
    stmt->params.nr_tag_fields = tagNum;
    sr = _tsdb_stmt_describe_cols(stmt);
  }
  return sr;
}

static SQLRETURN _tsdb_stmt_get_taos_params_for_non_insert(tsdb_stmt_t *stmt)
{
  int r = 0;

  int nr_params = 0;
  r = CALL_taos_stmt_num_params(stmt->stmt, &nr_params);
  if (r) {
    stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
    tsdb_stmt_release(stmt);

    return SQL_ERROR;
  }

  tsdb_params_t *params = &stmt->params;

  OA_NIY(params->is_insert_stmt == 0);
  OA_NIY(params->col_fields == NULL);
  OA_NIY(params->nr_col_fields == 0);
  int nr = (nr_params + 15) / 16 * 16;
  r = mem_keep(&params->mem, sizeof(*params->col_fields)*nr);
  if (r) {
    stmt_oom(stmt->owner);
    return SQL_ERROR;
  }
  params->nr_col_fields = nr_params;
  params->col_fields = (TAOS_FIELD_E*)params->mem.base;

  memset(params->col_fields, 0, nr * sizeof(*params->col_fields));

  return SQL_SUCCESS;
}

SQLSMALLINT tsdb_stmt_get_count_of_tsdb_params(tsdb_stmt_t *stmt)
{
  SQLSMALLINT n = 0;

  if (stmt->params.is_insert_stmt) {
    n = !!stmt->params.subtbl_required;
    n += stmt->params.nr_tag_fields;
    n += stmt->params.nr_col_fields;
  } else {
    n = stmt->params.nr_col_fields;
  }

  return n;
}

static SQLRETURN _tsdb_stmt_describe_param_by_field(
    tsdb_stmt_t    *stmt,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT    *DataTypePtr,
    SQLULEN        *ParameterSizePtr,
    SQLSMALLINT    *DecimalDigitsPtr,
    SQLSMALLINT    *NullablePtr,
    TAOS_FIELD_E   *field)
{
  int type = field->type;
  int bytes = field->bytes;

  switch (type) {
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_UINT:
      if (DataTypePtr)      *DataTypePtr      = SQL_INTEGER;
      if (ParameterSizePtr) *ParameterSizePtr = 10;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_USMALLINT:
      if (DataTypePtr)      *DataTypePtr      = SQL_SMALLINT;
      if (ParameterSizePtr) *ParameterSizePtr = 5;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_UTINYINT:
      if (DataTypePtr)      *DataTypePtr      = SQL_TINYINT;
      if (ParameterSizePtr) *ParameterSizePtr = 3;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_BOOL:
      if (DataTypePtr)      *DataTypePtr      = SQL_TINYINT; // FIXME: SQL_BIT
      if (ParameterSizePtr) *ParameterSizePtr = 3;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      if (DataTypePtr)      *DataTypePtr      = SQL_BIGINT;
      if (ParameterSizePtr) *ParameterSizePtr = 19;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      if (DataTypePtr)      *DataTypePtr      = SQL_REAL;
      if (ParameterSizePtr) *ParameterSizePtr = 7;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      if (DataTypePtr)      *DataTypePtr      = SQL_DOUBLE;
      if (ParameterSizePtr) *ParameterSizePtr = 15;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      if (DataTypePtr)      *DataTypePtr      = SQL_VARCHAR;
      if (ParameterSizePtr) *ParameterSizePtr = bytes - 2;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      if (DataTypePtr)      *DataTypePtr      = SQL_TYPE_TIMESTAMP;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = (field->precision + 1) * 3;
      if (ParameterSizePtr) *ParameterSizePtr = 20 + *DecimalDigitsPtr;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      if (DataTypePtr)      *DataTypePtr      = SQL_WVARCHAR;
      // /* taos internal storage: sizeof(int16_t) + payload */
      if (ParameterSizePtr) *ParameterSizePtr = (bytes - 2) / 4;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
      if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
      break;
    default:
      stmt_append_err_format(stmt->owner, "HY000", 0,
          "General error:#%d param:`%s[0x%x/%d]` not implemented yet", ParameterNumber, taos_data_type(type), type, type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

SQLRETURN tsdb_stmt_describe_param(
    tsdb_stmt_t    *stmt,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT    *DataTypePtr,
    SQLULEN        *ParameterSizePtr,
    SQLSMALLINT    *DecimalDigitsPtr,
    SQLSMALLINT    *NullablePtr)
{
  int r = 0;
  int idx = ParameterNumber - 1;
  int type = 0;
  int bytes = 0;
  OA_ILE(stmt->prepared);

  if (stmt->params.is_insert_stmt) {
    tsdb_params_t *params = &stmt->params;
    TAOS_FIELD_E *field = NULL;
    if (params->subtbl_required) {
      if (ParameterNumber == 1) {
        *DataTypePtr = SQL_VARCHAR;
        *ParameterSizePtr = 1024; // TODO: check taos-doc for max length of subtable name
        *DecimalDigitsPtr = 0;
        *NullablePtr = SQL_NO_NULLS;
        return SQL_SUCCESS;
      } else if (ParameterNumber <= 1 + params->nr_tag_fields) {
        field = params->tag_fields + ParameterNumber - 1 - 1;
      } else {
        field = params->col_fields + ParameterNumber - 1 - 1 - params->nr_tag_fields;
      }
    } else {
      field = params->col_fields + ParameterNumber - 1;
    }
    return _tsdb_stmt_describe_param_by_field(stmt, ParameterNumber, DataTypePtr, ParameterSizePtr, DecimalDigitsPtr, NullablePtr, field);
  }

  r = CALL_taos_stmt_get_param(stmt->stmt, idx, &type, &bytes);
  if (r) {
    // FIXME: return SQL_VARCHAR and hard-coded parameters for the moment
    if (DataTypePtr)       *DataTypePtr       = SQL_VARCHAR;
    if (ParameterSizePtr)  *ParameterSizePtr  = 1024;
    if (DecimalDigitsPtr)  *DecimalDigitsPtr  = 0;
    if (NullablePtr)       *NullablePtr       = SQL_NULLABLE_UNKNOWN;
    stmt_append_err(stmt->owner, "01000", 0,
        "General warning:Arbitrary `SQL_VARCHAR(1024)` is chosen to return because of taos lacking parm-desc for non-insert-statement");
    return SQL_SUCCESS_WITH_INFO;
  }

  stmt_append_err(stmt->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

TAOS_FIELD_E* tsdb_stmt_get_tsdb_field_by_tsdb_params(tsdb_stmt_t *stmt, int i_param)
{
  tsdb_params_t *params = &stmt->params;
  if (params->is_insert_stmt) {
    if (i_param == 0 && params->subtbl_required) {
      return &params->subtbl_field;
    }
    i_param -= !!params->subtbl_required;
    if (i_param < params->nr_tag_fields)
      return params->tag_fields + i_param;
    i_param -= params->nr_tag_fields;
  }
  OA_NIY(i_param < params->nr_col_fields);
  return params->col_fields + i_param;
}

SQLRETURN tsdb_stmt_prepare(tsdb_stmt_t *stmt, const char *sql)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  tsdb_stmt_reset(stmt);

  stmt->stmt = CALL_taos_stmt_init(stmt->owner->conn->taos);
  if (!stmt->stmt) {
    stmt_append_err_format(stmt->owner, "HY000", CALL_taos_errno(NULL), "General error:[taosc]%s", CALL_taos_errstr(NULL));
    return SQL_ERROR;
  }

  r = CALL_taos_stmt_prepare(stmt->stmt, sql, (unsigned long)strlen(sql));
  if (r) {
    stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosc]%s", CALL_taos_errstr(NULL));
    return SQL_ERROR;
  }

  int32_t isInsert = 0;
  r = CALL_taos_stmt_is_insert(stmt->stmt, &isInsert);
  isInsert = !!isInsert;

  if (r) {
    stmt_append_err_format(stmt->owner, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
    tsdb_stmt_reset(stmt);

    return SQL_ERROR;
  }

  stmt->params.is_insert_stmt = isInsert;

  if (stmt->params.is_insert_stmt) {
    sr = _tsdb_stmt_get_taos_tags_cols_for_insert(stmt);
  } else {
    sr = _tsdb_stmt_get_taos_params_for_non_insert(stmt);
  }
  if (sr == SQL_ERROR) return SQL_ERROR;

  SQLSMALLINT n = tsdb_stmt_get_count_of_tsdb_params(stmt);
  if (n <= 0) {
    stmt_append_err(stmt->owner, "HY000", 0, "General error:statement-without-parameter-placemarker not allowed to be prepared");
    return SQL_ERROR;
  }

  stmt->prepared = 1;

  return SQL_SUCCESS;
}

static SQLRETURN _tsdb_stmt_guess_parameter_for_non_insert(tsdb_stmt_t *stmt, size_t param)
{
  descriptor_t *IPD = stmt_IPD(stmt->owner);
  desc_record_t *IPD_records = IPD->records;
  desc_record_t *IPD_record = IPD_records + param - 1;

  SQLSMALLINT ParameterType = IPD_record->DESC_CONCISE_TYPE;
  SQLULEN     ColumnSize    = IPD_record->DESC_LENGTH;
  SQLSMALLINT DecimalDigits = IPD_record->DESC_PRECISION;
  SQLSMALLINT Type          = IPD_record->DESC_TYPE;

  TAOS_FIELD_E *tsdb_field = tsdb_stmt_get_tsdb_field_by_tsdb_params(stmt, (int)param-1);

  switch (ParameterType) {
    case SQL_VARCHAR:
    case SQL_INTEGER:
    case SQL_BIGINT:
      return SQL_SUCCESS;
    default:
      break;
  }

  stmt_append_err_format(stmt->owner, "HY000", 0,
      "General error:it's not an subtbled-insert-statement and parameter marker #%zd of [0x%x/%d]%s is sepcified,"
      " "
      "but application parameter of [0x%x/%d]%s/%s, ColumnSize[%" PRIu64 "], DecimalDigits[%d]",
      param,
      tsdb_field->type, tsdb_field->type, taos_data_type(tsdb_field->type),
      ParameterType, ParameterType, sql_data_type(ParameterType),
      sql_data_type(Type), (uint64_t)ColumnSize, DecimalDigits);

  return SQL_ERROR;
}

static SQLRETURN _tsdb_stmt_check_parameter(tsdb_stmt_t *stmt, size_t param)
{
  descriptor_t *IPD = stmt_IPD(stmt->owner);
  desc_record_t *IPD_records = IPD->records;
  desc_record_t *IPD_record = IPD_records + param - 1;

  SQLSMALLINT ParameterType = IPD_record->DESC_CONCISE_TYPE;

  TAOS_FIELD_E *tsdb_field = tsdb_stmt_get_tsdb_field_by_tsdb_params(stmt, (int)param-1);

  switch (tsdb_field->type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      if (ParameterType == SQL_TYPE_TIMESTAMP) return SQL_SUCCESS;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      if (ParameterType == SQL_VARCHAR) return SQL_SUCCESS;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      if (ParameterType == SQL_WVARCHAR) return SQL_SUCCESS;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      if (ParameterType == SQL_BIGINT) return SQL_SUCCESS;
      break;
    case TSDB_DATA_TYPE_NULL:
      if (!stmt->params.is_insert_stmt) {
        return _tsdb_stmt_guess_parameter_for_non_insert(stmt, param);
      }

      if (!stmt->params.subtbl_required || param != 1) {
        stmt_append_err_format(stmt->owner, "HY000", 0,
            "General error:it's not an subtbled-insert-statement and parameter marker #%zd of [0x%x/%d]%s is sepcified,"
            " "
            "but application parameter is of [0x%x/%d]%s",
            param,
            tsdb_field->type, tsdb_field->type, taos_data_type(tsdb_field->type),
            ParameterType, ParameterType, sql_data_type(ParameterType));
        return SQL_ERROR;
      }

      if (ParameterType == SQL_VARCHAR) return SQL_SUCCESS;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      if (ParameterType == SQL_DOUBLE) return SQL_SUCCESS;
      break;
    case TSDB_DATA_TYPE_INT:
      if (ParameterType == SQL_INTEGER) return SQL_SUCCESS;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      if (ParameterType == SQL_REAL) return SQL_SUCCESS;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      if (ParameterType == SQL_SMALLINT) return SQL_SUCCESS;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      if (ParameterType == SQL_TINYINT) return SQL_SUCCESS;
      break;
    case TSDB_DATA_TYPE_BOOL:
      if (ParameterType == SQL_TINYINT) return SQL_SUCCESS;
      break;
    default:
      break;
  }

  stmt_append_err_format(stmt->owner, "HY000", 0,
      "General error:parameter marker #%zd of [0x%x/%d]%s is sepcified, but application parameter is of [0x%x/%d]%s",
      param,
      tsdb_field->type, tsdb_field->type, taos_data_type(tsdb_field->type),
      ParameterType, ParameterType, sql_data_type(ParameterType));

  return SQL_ERROR;

#if 0
  switch (IPD_record->DESC_CONCISE_TYPE) {
    case SQL_TYPE_TIMESTAMP:
      if (tsdb_field->type == TSDB_DATA_TYPE_TIMESTAMP) return SQL_SUCCESS;
      break;
    case SQL_VARCHAR:
      if (tsdb_field->type == TSDB_DATA_TYPE_VARCHAR) return SQL_SUCCESS;
      if (tsdb_field->type == TSDB_DATA_TYPE_NULL) {
        if (!stmt->params.is_insert_stmt || !stmt->params.subtbl_required || param > 1) {
          stmt_append_err_format(stmt->owner, "HY000", 0,
            "General error:[taosc conformance]application parameter #%" PRId64 " of [0x%x/%d]%s is specified,"
            " "
            "but it's not the first parameter within a subtbled-insert statement",
            param, IPD_record->DESC_CONCISE_TYPE, IPD_record->DESC_CONCISE_TYPE, sql_data_type(IPD_record->DESC_CONCISE_TYPE));
          return SQL_ERROR;
        }
        stmt_append_err_format(stmt->owner, "HY000", 0,
          "General error:[taosc conformance]application parameter #%" PRId64 " of [0x%x/%d]%s is specified,"
          " "
          "but it's not the first parameter within a subtbled-insert statement",
          param, IPD_record->DESC_CONCISE_TYPE, IPD_record->DESC_CONCISE_TYPE, sql_data_type(IPD_record->DESC_CONCISE_TYPE));
        return SQL_ERROR;
        return SQL_SUCCESS;
      }
      break;
    case SQL_WVARCHAR:
      if (tsdb_field->type == TSDB_DATA_TYPE_NCHAR) return SQL_SUCCESS;
      break;
    case SQL_BIGINT:
      if (tsdb_field->type == TSDB_DATA_TYPE_BIGINT) return SQL_SUCCESS;
      break;
    default:
      break;
  }

  stmt_append_err_format(stmt->owner, "HY000", 0,
      "General error:application parameter #%" PRId64 " of [0x%x/%d]%s is sepcified, but conflicts with [0x%x/%d]%s",
      param, IPD_record->DESC_CONCISE_TYPE, IPD_record->DESC_CONCISE_TYPE, sql_data_type(IPD_record->DESC_CONCISE_TYPE),
      tsdb_field->type, tsdb_field->type, taos_data_type(tsdb_field->type));
  return SQL_ERROR;
#endif
}

SQLRETURN tsdb_stmt_check_parameters(tsdb_stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  descriptor_t *APD = stmt_APD(stmt->owner);
  desc_header_t *APD_header = &APD->header;

  descriptor_t *IPD = stmt_IPD(stmt->owner);
  desc_header_t *IPD_header = &IPD->header;

  OA_NIY(APD_header->DESC_COUNT == IPD_header->DESC_COUNT);

  const SQLSMALLINT n = tsdb_stmt_get_count_of_tsdb_params(stmt);
  if (APD_header->DESC_COUNT < n) {
    stmt_append_err_format(stmt->owner, "07002", 0,
        "COUNT field incorrect:%d parameter markers required, but only %d parameters bound",
        n, APD_header->DESC_COUNT);
    return SQL_ERROR;
  }

  if (APD_header->DESC_COUNT > n) {
    OW("bind more parameters (#%d) than required (#%d) by sql-statement", APD_header->DESC_COUNT, n);
  }

  for (size_t i=0; i<IPD_header->DESC_COUNT; ++i) {
    sr = _tsdb_stmt_check_parameter(stmt, i+1);
    if (sr != SQL_SUCCESS) return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

SQLRETURN tsdb_stmt_fetch_rows_block(tsdb_stmt_t *stmt)
{
  tsdb_res_t           *res          = &stmt->res;
  tsdb_rows_block_t    *rows_block   = &res->rows_block;

  tsdb_rows_block_reset(rows_block);

  TAOS_ROW rows = NULL;
  int nr_rows = CALL_taos_fetch_block(res->res, &rows);
  if (nr_rows == 0) return SQL_NO_DATA;
  rows_block->rows   = rows;
  rows_block->nr     = nr_rows;
  rows_block->pos    = 0;

  return SQL_SUCCESS;
}

