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

#include "internal.h"

#include "desc.h"
#include "stmt.h"

#include <errno.h>
#include <iconv.h>
#include <limits.h>
#include <string.h>
#include <time.h>
#include <wchar.h>

static void col_reset(col_t *col)
{
  if (col->buf) col->buf[0] = '\0';
  col->nr = 0;

  col->field = NULL;
  col->i_col = -1;
  col->data  = NULL;
  col->len   = 0;
}

static void col_release(col_t *col)
{
  col_reset(col);
  TOD_SAFE_FREE(col->buf);
  col->cap = 0;
}

static void rowset_reset(rowset_t *rowset)
{
  rowset->i_row = 0;
}

static void rowset_release(rowset_t *rowset)
{
  rowset_reset(rowset);
}

static void _stmt_release_descriptors(stmt_t *stmt)
{
  descriptor_release(&stmt->APD);
  descriptor_release(&stmt->IPD);
  descriptor_release(&stmt->ARD);
  descriptor_release(&stmt->IRD);
}

static void _stmt_init_descriptors(stmt_t *stmt)
{
  descriptor_init(&stmt->APD);
  descriptor_init(&stmt->IPD);
  descriptor_init(&stmt->ARD);
  descriptor_init(&stmt->IRD);

  stmt->current_APD = &stmt->APD;
  stmt->current_ARD = &stmt->ARD;
}

static void _stmt_init(stmt_t *stmt, conn_t *conn)
{
  stmt->conn = conn_ref(conn);
  int prev = atomic_fetch_add(&conn->stmts, 1);
  OA_ILE(prev >= 0);

  errs_init(&stmt->errs);
  _stmt_init_descriptors(stmt);

  stmt->refc = 1;
}

static void _stmt_release_post_filter(stmt_t *stmt)
{
  post_filter_t *post_filter = &stmt->post_filter;
  if (post_filter->post_filter_destroy) {
    post_filter->post_filter_destroy(stmt, post_filter->ctx);
    post_filter->post_filter_destroy = NULL;
  }
  post_filter->post_filter = NULL;
  post_filter->ctx         = NULL;
}

static void _stmt_release_result(stmt_t *stmt)
{
  _stmt_release_post_filter(stmt);

  if (!stmt->res) return;

  if (stmt->res_is_from_taos_query) CALL_taos_free_result(stmt->res);

  stmt->res_is_from_taos_query = 0;
  stmt->res = NULL;
  stmt->affected_row_count = 0;
  stmt->col_count = 0;
  stmt->fields = NULL;
  stmt->lengths = NULL;
  stmt->time_precision = 0;

  stmt->rows = NULL;
  stmt->nr_rows = 0;
}

static void _stmt_release_stmt(stmt_t *stmt)
{
  if (!stmt->stmt) return;

  int r = CALL_taos_stmt_close(stmt->stmt);
  OA_NIY(r == 0);
  stmt->stmt = NULL;
}

void stmt_dissociate_ARD(stmt_t *stmt)
{
  if (stmt->associated_ARD == NULL) return;

  tod_list_del(&stmt->associated_ARD_node);
  desc_unref(stmt->associated_ARD);
  stmt->associated_ARD = NULL;
  stmt->current_ARD = &stmt->ARD;
}

static SQLRETURN _stmt_associate_ARD(stmt_t *stmt, desc_t *desc)
{
  if (stmt->associated_ARD == desc) return SQL_SUCCESS;

  if (desc->conn != stmt->conn) {
    stmt_append_err(stmt, "HY024", 0, "Invalid attribute value:descriptor not on the same connection as that of the statement");
    return SQL_ERROR;
  }

  // FIXME:
  if (stmt->associated_APD == desc) {
    stmt_append_err(stmt, "HY024", 0, "Invalid attribute value:descriptor already associated as statement's APD");
    return SQL_ERROR;
  }

  stmt_dissociate_ARD(stmt);

  tod_list_add_tail(&stmt->associated_ARD_node, &desc->associated_stmts_as_ARD);
  desc_ref(desc);
  stmt->associated_ARD = desc;
  stmt->current_ARD = &desc->descriptor;

  return SQL_SUCCESS;
}

void stmt_dissociate_APD(stmt_t *stmt)
{
  if (stmt->associated_APD == NULL) return;

  descriptor_reclaim_buffers(&stmt->associated_APD->descriptor);

  tod_list_del(&stmt->associated_APD_node);
  desc_unref(stmt->associated_APD);
  stmt->associated_APD = NULL;
  stmt->current_APD = &stmt->APD;
}

static SQLRETURN _stmt_associate_APD(stmt_t *stmt, desc_t *desc)
{
  if (stmt->associated_APD == desc) return SQL_SUCCESS;

  if (desc->conn != stmt->conn) {
    stmt_append_err(stmt, "HY024", 0, "Invalid attribute value:descriptor not on the same connection as that of the statement");
    return SQL_ERROR;
  }

  // FIXME:
  if (stmt->associated_ARD == desc) {
    stmt_append_err(stmt, "HY024", 0, "Invalid attribute value:descriptor already associated as statement's ARD");
    return SQL_ERROR;
  }

  stmt_dissociate_APD(stmt);

  tod_list_add_tail(&stmt->associated_APD_node, &desc->associated_stmts_as_APD);
  desc_ref(desc);
  stmt->associated_APD = desc;
  stmt->current_APD = &desc->descriptor;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_set_row_desc(stmt_t *stmt, SQLPOINTER ValuePtr)
{
  if (ValuePtr == SQL_NULL_DESC) {
    stmt_dissociate_ARD(stmt);
    return SQL_SUCCESS;
  }

  desc_t *desc = (desc_t*)(SQLHANDLE)ValuePtr;
  return _stmt_associate_ARD(stmt, desc);
}

static SQLRETURN _stmt_set_param_desc(stmt_t *stmt, SQLPOINTER ValuePtr)
{
  if (ValuePtr == SQL_NULL_DESC) {
    stmt_dissociate_APD(stmt);
    return SQL_SUCCESS;
  }

  desc_t *desc = (desc_t*)(SQLHANDLE)ValuePtr;
  return _stmt_associate_APD(stmt, desc);
}

static void _stmt_reset_current_for_get_data(stmt_t *stmt)
{
  col_t *col = &stmt->current_for_get_data;
  col_reset(col);
}

static void _stmt_release_current_for_get_data(stmt_t *stmt)
{
  col_t *col = &stmt->current_for_get_data;
  col_release(col);
}

static void _stmt_release_tag_fields(stmt_t *stmt)
{
  if (stmt->tag_fields) {
    free(stmt->tag_fields);
    stmt->tag_fields = NULL;
  }
  stmt->nr_tag_fields = 0;
}

static void _stmt_release_col_fields(stmt_t *stmt)
{
  if (stmt->col_fields) {
    free(stmt->col_fields);
    stmt->col_fields = NULL;
  }
  stmt->nr_col_fields = 0;
}

static descriptor_t* _stmt_APD(stmt_t *stmt)
{
  return stmt->current_APD;
}

static void _stmt_release_field_arrays(stmt_t *stmt)
{
  descriptor_t *APD = _stmt_APD(stmt);
  descriptor_reclaim_buffers(APD);
}

static void _stmt_release(stmt_t *stmt)
{
  rowset_release(&stmt->rowset);
  _stmt_release_post_filter(stmt);
  _stmt_release_result(stmt);

  _stmt_release_field_arrays(stmt);

  stmt_dissociate_APD(stmt);

  _stmt_release_stmt(stmt);

  stmt_dissociate_ARD(stmt);

  _stmt_release_current_for_get_data(stmt);

  int prev = atomic_fetch_sub(&stmt->conn->stmts, 1);
  OA_ILE(prev >= 1);
  conn_unref(stmt->conn);
  stmt->conn = NULL;

  _stmt_release_descriptors(stmt);

  _stmt_release_tag_fields(stmt);
  _stmt_release_col_fields(stmt);
  TOD_SAFE_FREE(stmt->mbs);
  stmt->cap_mbs = 0;
  stmt->nr_mbs = 0;

  TOD_SAFE_FREE(stmt->subtbl);
  TOD_SAFE_FREE(stmt->sql);

  errs_release(&stmt->errs);

  return;
}

stmt_t* stmt_create(conn_t *conn)
{
  stmt_t *stmt = (stmt_t*)calloc(1, sizeof(*stmt));
  if (!stmt) {
    conn_oom(conn);
    return NULL;
  }

  _stmt_init(stmt, conn);

  return stmt;
}

stmt_t* stmt_ref(stmt_t *stmt)
{
  int prev = atomic_fetch_add(&stmt->refc, 1);
  OA_ILE(prev>0);
  return stmt;
}

stmt_t* stmt_unref(stmt_t *stmt)
{
  int prev = atomic_fetch_sub(&stmt->refc, 1);
  if (prev>1) return stmt;
  OA_ILE(prev==1);

  _stmt_release(stmt);
  free(stmt);

  return NULL;
}

SQLRETURN stmt_free(stmt_t *stmt)
{
  stmt_unref(stmt);
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_post_exec(stmt_t *stmt)
{
  int e;
  const char *estr;
  e = CALL_taos_errno(stmt->res);
  estr = CALL_taos_errstr(stmt->res);

  if (e) {
    stmt_append_err_format(stmt, "HY000", e, "General error:[taosc]%s", estr);
    return SQL_ERROR;
  } else if (stmt->res) {
    stmt->time_precision = CALL_taos_result_precision(stmt->res);
    stmt->affected_row_count = CALL_taos_affected_rows(stmt->res);
    stmt->col_count = CALL_taos_field_count(stmt->res);
    if (stmt->col_count > 0) {
      stmt->fields = CALL_taos_fetch_fields(stmt->res);
    }
    _stmt_reset_current_for_get_data(stmt);
  }

  return SQL_SUCCESS;
}

static descriptor_t* _stmt_IPD(stmt_t *stmt)
{
  return &stmt->IPD;
}

static descriptor_t* _stmt_IRD(stmt_t *stmt)
{
  return &stmt->IRD;
}

static descriptor_t* _stmt_ARD(stmt_t *stmt)
{
  return stmt->current_ARD;
}

static SQLRETURN _stmt_set_rows_fetched_ptr(stmt_t *stmt, SQLULEN *rows_fetched_ptr)
{
  descriptor_t *IRD = _stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;
  IRD_header->DESC_ROWS_PROCESSED_PTR = rows_fetched_ptr;

  return SQL_SUCCESS;
}

static SQLULEN* _stmt_get_rows_fetched_ptr(stmt_t *stmt)
{
  descriptor_t *IRD = _stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;
  return IRD_header->DESC_ROWS_PROCESSED_PTR;
}

static SQLRETURN _stmt_exec_direct_sql(stmt_t *stmt, const char *sql)
{
  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->taos);
  OA_ILE(sql);

  if (_stmt_get_rows_fetched_ptr(stmt)) *_stmt_get_rows_fetched_ptr(stmt) = 0;
  rowset_reset(&stmt->rowset);
  _stmt_release_result(stmt);

  TAOS *taos = stmt->conn->taos;

  if (stmt->stmt) {
    // NOTE: not fully tested yet
    _stmt_release_stmt(stmt);

    _stmt_release_tag_fields(stmt);
    _stmt_release_col_fields(stmt);
    TOD_SAFE_FREE(stmt->mbs);
    stmt->cap_mbs = 0;
    stmt->nr_mbs = 0;

    TOD_SAFE_FREE(stmt->subtbl);
    TOD_SAFE_FREE(stmt->sql);
  }

  // TODO: to support exec_direct parameterized statement

  stmt->res = CALL_taos_query(taos, sql);
  stmt->res_is_from_taos_query = stmt->res ? 1 : 0;

  return _stmt_post_exec(stmt);
}

static SQLRETURN _stmt_exec_direct(stmt_t *stmt, const char *sql, int len)
{
  char buf[1024];
  char *p = (char*)sql;
  if (len == SQL_NTS)
    len = strlen(sql);
  if (p[len]) {
    if ((size_t)len < sizeof(buf)) {
      strncpy(buf, p, len);
      buf[len] = '\0';
      p = buf;
    } else {
      p = strndup(p, len);
      if (!p) {
        stmt_oom(stmt);
        return SQL_ERROR;
      }
    }
  }

  SQLRETURN sr = _stmt_exec_direct_sql(stmt, p);

  if (p != sql && p!= buf) free(p);

  return sr;
}

SQLRETURN stmt_exec_direct(stmt_t *stmt, const char *sql, int len)
{
  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->taos);

  int prev = atomic_fetch_add(&stmt->conn->outstandings, 1);
  OA_ILE(prev >= 0);

  SQLRETURN sr = _stmt_exec_direct(stmt, sql, len);
  prev = atomic_fetch_sub(&stmt->conn->outstandings, 1);
  OA_ILE(prev >= 0);

  return sr;
}

static SQLRETURN _stmt_set_row_array_size(stmt_t *stmt, SQLULEN row_array_size)
{
  if (row_array_size == 0) {
    stmt_append_err(stmt, "01S02", 0, "Option value changed:`0` for `SQL_ATTR_ROW_ARRAY_SIZE` is substituted by current value");
    return SQL_SUCCESS_WITH_INFO;
  }

  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;
  ARD_header->DESC_ARRAY_SIZE = row_array_size;

  return SQL_SUCCESS;
}

static SQLULEN _stmt_get_row_array_size(stmt_t *stmt)
{
  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;
  return ARD_header->DESC_ARRAY_SIZE;
}

static SQLRETURN _stmt_set_paramset_size(stmt_t *stmt, SQLULEN paramset_size)
{
  if (paramset_size == 0) {
    stmt_append_err(stmt, "01S02", 0, "Option value changed:`0` for `SQL_ATTR_PARAMSET_SIZE` is substituted by current value");
    return SQL_SUCCESS_WITH_INFO;
  }

  if (paramset_size != 1) {
    if (stmt->prepared && !stmt->is_insert_stmt) {
      stmt_append_err(stmt, "HY000", 0, "General error:taosc currently does not support batch execution for non-insert-statement");
      return SQL_ERROR;
    }
  }

  descriptor_t *APD = _stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;
  APD_header->DESC_ARRAY_SIZE = paramset_size;

  return SQL_SUCCESS;
}

// static SQLULEN _stmt_get_paramset_size(stmt_t *stmt)
// {
//   descriptor_t *APD = _stmt_APD(stmt);
//   desc_header_t *APD_header = &APD->header;
//   return APD_header->DESC_ARRAY_SIZE;
// }

static SQLRETURN _stmt_set_row_status_ptr(stmt_t *stmt, SQLUSMALLINT *row_status_ptr)
{
  descriptor_t *IRD = _stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;
  IRD_header->DESC_ARRAY_STATUS_PTR = row_status_ptr;
  return SQL_SUCCESS;
}

SQLUSMALLINT* stmt_get_row_status_ptr(stmt_t *stmt)
{
  descriptor_t *IRD = _stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;
  return IRD_header->DESC_ARRAY_STATUS_PTR;
}

static SQLRETURN _stmt_set_param_status_ptr(stmt_t *stmt, SQLUSMALLINT *param_status_ptr)
{
  descriptor_t *IPD = _stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  IPD_header->DESC_ARRAY_STATUS_PTR = param_status_ptr;
  return SQL_SUCCESS;
}

// static SQLUSMALLINT* _stmt_get_param_status_ptr(stmt_t *stmt)
// {
//   descriptor_t *IPD = _stmt_IPD(stmt);
//   desc_header_t *IPD_header = &IPD->header;
//   return IPD_header->DESC_ARRAY_STATUS_PTR;
// }

SQLRETURN stmt_get_row_count(stmt_t *stmt, SQLLEN *row_count_ptr)
{
  if (row_count_ptr) *row_count_ptr = stmt->affected_row_count;
  return SQL_SUCCESS;
}

SQLRETURN stmt_get_col_count(stmt_t *stmt, SQLSMALLINT *col_count_ptr)
{
  if (col_count_ptr) *col_count_ptr = stmt->col_count;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_set_row_bind_type(stmt_t *stmt, SQLULEN row_bind_type)
{
  if (row_bind_type != SQL_BIND_BY_COLUMN) {
    stmt_append_err(stmt, "HY000", 0, "General error:only `SQL_BIND_BY_COLUMN` is supported now");
    return SQL_ERROR;
  }

  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;
  ARD_header->DESC_BIND_TYPE = row_bind_type;

  return SQL_SUCCESS;
}

// static SQLULEN stmt_get_row_bind_type(stmt_t *stmt)
// {
//   descriptor_t *ARD = _stmt_ARD(stmt);
//   desc_header_t *ARD_header = &ARD->header;
//   return ARD_header->DESC_BIND_TYPE;
// }

static SQLRETURN _stmt_set_param_bind_type(stmt_t *stmt, SQLULEN param_bind_type)
{
  if (param_bind_type != SQL_BIND_BY_COLUMN) {
    stmt_append_err(stmt, "HY000", 0, "General error:only `SQL_BIND_BY_COLUMN` is supported now");
    return SQL_ERROR;
  }

  descriptor_t *APD = _stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;
  APD_header->DESC_BIND_TYPE = param_bind_type;

  return SQL_SUCCESS;
}

// static SQLULEN _stmt_get_param_bind_type(stmt_t *stmt)
// {
//   descriptor_t *APD = _stmt_APD(stmt);
//   desc_header_t *APD_header = &APD->header;
//   return APD_header->DESC_BIND_TYPE;
// }

static SQLRETURN _stmt_set_params_processed_ptr(stmt_t *stmt, SQLULEN *params_processed_ptr)
{
  descriptor_t *IPD = _stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  IPD_header->DESC_ROWS_PROCESSED_PTR = params_processed_ptr;

  return SQL_SUCCESS;
}

// static SQLULEN* _stmt_get_params_processed_ptr(stmt_t *stmt)
// {
//   descriptor_t *IPD = _stmt_IPD(stmt);
//   desc_header_t *IPD_header = &IPD->header;
//   return IPD_header->DESC_ROWS_PROCESSED_PTR;
// }

static SQLRETURN _stmt_set_max_length(stmt_t *stmt, SQLULEN max_length)
{
  if (max_length != 0) {
    stmt_append_err(stmt, "01S02", 0, "Option value changed:`%u` for `SQL_ATTR_MAX_LENGTH` is substituted by `0`");
    return SQL_SUCCESS_WITH_INFO;
  }
  // stmt->attrs.ATTR_MAX_LENGTH = max_length;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_set_row_bind_offset_ptr(stmt_t *stmt, SQLULEN *row_bind_offset_ptr)
{
  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;
  ARD_header->DESC_BIND_OFFSET_PTR = row_bind_offset_ptr;

  return SQL_SUCCESS;
}

// static SQLULEN* _stmt_get_row_bind_offset_ptr(stmt_t *stmt)
// {
//   descriptor_t *ARD = _stmt_ARD(stmt);
//   desc_header_t *ARD_header = &ARD->header;
//   return ARD_header->DESC_BIND_OFFSET_PTR;
// }

SQLRETURN stmt_describe_col(stmt_t *stmt,
    SQLUSMALLINT   ColumnNumber,
    SQLCHAR       *ColumnName,
    SQLSMALLINT    BufferLength,
    SQLSMALLINT   *NameLengthPtr,
    SQLSMALLINT   *DataTypePtr,
    SQLULEN       *ColumnSizePtr,
    SQLSMALLINT   *DecimalDigitsPtr,
    SQLSMALLINT   *NullablePtr)
{
  // NOTE: DM to make sure ColumnNumber is valid
  TAOS_FIELD *p = stmt->fields + ColumnNumber - 1;

  if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
  if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;

  int n;
  n = snprintf((char*)ColumnName, BufferLength, "%s", p->name);
  if (NameLengthPtr) *NameLengthPtr = n;

  if (NullablePtr) *NullablePtr = SQL_NULLABLE_UNKNOWN;
  switch (p->type) {
    case TSDB_DATA_TYPE_TINYINT:
      if (DataTypePtr) {
        if (stmt->conn->cfg.unsigned_promotion) {
          *DataTypePtr   = SQL_SMALLINT;
        } else {
          *DataTypePtr   = SQL_TINYINT;
        }
      }
      if (ColumnSizePtr)    *ColumnSizePtr = 3;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      if (DataTypePtr)      *DataTypePtr   = SQL_TINYINT;
      if (ColumnSizePtr)    *ColumnSizePtr = 3;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      if (DataTypePtr)      *DataTypePtr = SQL_SMALLINT;
      if (ColumnSizePtr)    *ColumnSizePtr = 5;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      if (DataTypePtr) {
        if (stmt->conn->cfg.unsigned_promotion) {
          *DataTypePtr   = SQL_INTEGER;
        } else {
          *DataTypePtr   = SQL_SMALLINT;
        }
      }
      if (ColumnSizePtr)    *ColumnSizePtr = 5;
      break;
    case TSDB_DATA_TYPE_INT:
      if (DataTypePtr)      *DataTypePtr = SQL_INTEGER;
      if (ColumnSizePtr)    *ColumnSizePtr = 10;
      break;
    case TSDB_DATA_TYPE_UINT:
      if (DataTypePtr) {
        if (stmt->conn->cfg.unsigned_promotion) {
          *DataTypePtr   = SQL_BIGINT;
        } else {
          *DataTypePtr   = SQL_INTEGER;
        }
      }
      if (ColumnSizePtr)    *ColumnSizePtr = 10;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      if (DataTypePtr)      *DataTypePtr = SQL_BIGINT;
      if (ColumnSizePtr)    *ColumnSizePtr = 19; // signed bigint
      break;
    case TSDB_DATA_TYPE_FLOAT:
      if (DataTypePtr)      *DataTypePtr = SQL_REAL;
      // https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/column-size?view=sql-server-ver16
      if (ColumnSizePtr)    *ColumnSizePtr = 7;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      if (DataTypePtr)      *DataTypePtr = SQL_DOUBLE;
      // https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/column-size?view=sql-server-ver16
      if (ColumnSizePtr)    *ColumnSizePtr = 15;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      if (DataTypePtr)      *DataTypePtr = SQL_VARCHAR;
      if (ColumnSizePtr)    *ColumnSizePtr = p->bytes;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      if (DataTypePtr)      *DataTypePtr = SQL_TYPE_TIMESTAMP;
      if (DecimalDigitsPtr) {
        *DecimalDigitsPtr = (stmt->time_precision + 1) * 3;
      }
      if (ColumnSizePtr) {
        *ColumnSizePtr = 20 + *DecimalDigitsPtr;
      }
      break;
    case TSDB_DATA_TYPE_NCHAR:
      if (DataTypePtr)      *DataTypePtr   = SQL_WVARCHAR;
      if (ColumnSizePtr)    *ColumnSizePtr = p->bytes;
      break;
    case TSDB_DATA_TYPE_BOOL:
      if (DataTypePtr)      *DataTypePtr   = SQL_BIT;
      if (ColumnSizePtr)    *ColumnSizePtr = 1;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0, "General error:`%s[%d]` for Column `%s[#%d]` not implemented yet", taos_data_type(p->type), p->type, p->name, ColumnNumber);
      return SQL_ERROR;
  }

  if (n >= BufferLength) {
    stmt_append_err_format(stmt, "01004", 0, "String data, right truncated:Column `%s[#%d]` truncated to [%s]", p->name, ColumnNumber, ColumnName);
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_calc_bytes(stmt_t *stmt,
    const char *fromcode, const char *s, size_t len,
    const char *tocode, size_t *bytes)
{
  iconv_t cd = iconv_open(tocode, fromcode);
  if ((size_t)cd == (size_t)-1) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:[iconv]No character set conversion found for `%s` to `%s`:[%d]%s",
        fromcode, tocode, errno, strerror(errno));
    return SQL_ERROR;
  }

  SQLRETURN sr = SQL_SUCCESS;

  char * inbuf = (char*)s;
  size_t inbytes = len;

  *bytes = 0;
  while (inbytes>0) {
    char buf[1024];
    char *outbuf = buf;
    size_t outbytes = sizeof(buf);
    size_t sz = iconv(cd, &inbuf, &inbytes, &outbuf, &outbytes);
    *bytes += (sizeof(buf) - outbytes);
    if (sz == (size_t)-1) {
      int e = errno;
      if (e == E2BIG) continue;
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:[iconv]Character set conversion for `%s` to `%s` failed:[%d]%s",
          fromcode, tocode, e, strerror(e));
      sr = SQL_ERROR;
      break;
    }
    if (inbytes == 0) break;
  }

  iconv_close(cd);

  return sr;
}

static SQLRETURN _stmt_encode(stmt_t *stmt,
    const char *fromcode, char **inbuf, size_t *inbytesleft,
    const char *tocode, char **outbuf, size_t *outbytesleft)
{
  iconv_t cd = iconv_open(tocode, fromcode);
  if ((size_t)cd == (size_t)-1) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General:[iconv]No character set conversion found for `%s` to `%s`:[%d]%s",
        fromcode, tocode, errno, strerror(errno));
    return SQL_ERROR;
  }

  size_t inbytes = *inbytesleft;
  size_t outbytes = *outbytesleft;
  size_t sz = iconv(cd, inbuf, inbytesleft, outbuf, outbytesleft);
  int e = errno;
  iconv_close(cd);
  if (*inbytesleft > 0) {
    stmt_append_err_format(stmt, "01004", 0,
        "String data, right truncated:[iconv]Character set conversion for `%s` to `%s`, #%ld out of #%ld bytes consumed, #%ld out of #%ld bytes converted:[%d]%s",
        fromcode, tocode, inbytes - *inbytesleft, inbytes, outbytes - * outbytesleft, outbytes, e, strerror(e));
    return SQL_SUCCESS_WITH_INFO;
  }
  if (sz == (size_t)-1) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:[iconv]Character set conversion for `%s` to `%s` failed:[%d]%s",
        fromcode, tocode, e, strerror(e));
    return SQL_ERROR;
  }
  if (sz > 0) {
    // FIXME: what actually means when sz > 0???
    stmt_append_err_format(stmt, "01000", 0,
        "General warning:[iconv]Character set conversion for `%s` to `%s` succeeded with #%ld of nonreversible characters converted",
        fromcode, tocode, sz);
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static int _tsdb_timestamp_to_string(stmt_t *stmt, int64_t val, char *buf)
{
  int n;
  time_t  tt;
  int32_t ms = 0;
  int w;
  switch (stmt->time_precision) {
    case 2:
      tt = (time_t)(val / 1000000000);
      ms = val % 1000000000;
      w = 9;
      break;
    case 1:
      tt = (time_t)(val / 1000000);
      ms = val % 1000000;
      w = 6;
      break;
    case 0:
      tt = (time_t)(val / 1000);
      ms = val % 1000;
      w = 3;
      break;
    default:
      OA_ILE(0);
      break;
  }

  if (tt <= 0 && ms < 0) {
    OA_NIY(0);
  }

  struct tm ptm = {0};
  struct tm *p = localtime_r(&tt, &ptm);
  OA_ILE(p == &ptm);

  n = sprintf(buf,
      "%04d-%02d-%02d %02d:%02d:%02d.%0*d",
      ptm.tm_year + 1900, ptm.tm_mon + 1, ptm.tm_mday,
      ptm.tm_hour, ptm.tm_min, ptm.tm_sec,
      w, ms);

  OA_ILE(n > 0);

  return n;
}

SQLRETURN stmt_bind_col(stmt_t *stmt,
    SQLUSMALLINT   ColumnNumber,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  if (ColumnNumber == 0) {
    stmt_append_err(stmt, "HY000", 0, "General error:bookmark column not supported yet");
    return SQL_ERROR;
  }

  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;

  if (ColumnNumber >= ARD->cap) {
    size_t cap = (ColumnNumber + 15) / 16 * 16;
    desc_record_t *records = (desc_record_t*)realloc(ARD->records, sizeof(*records) * cap);
    if (!records) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
    for (size_t i = ARD->cap; i<cap; ++i) {
      desc_record_t *record = records + i;
      memset(record, 0, sizeof(*record));
    }
    ARD->records = records;
    ARD->cap     = cap;
  }

  desc_record_t *ARD_record = ARD->records + ColumnNumber - 1;
  memset(ARD_record, 0, sizeof(*ARD_record));

  switch (TargetType) {
    case SQL_C_UTINYINT:
      ARD_record->DESC_LENGTH            = 1;
      ARD_record->DESC_PRECISION         = 0;
      ARD_record->DESC_SCALE             = 0;
      ARD_record->DESC_TYPE              = TargetType;
      ARD_record->DESC_CONCISE_TYPE      = TargetType;
      ARD_record->element_size_in_column_wise = ARD_record->DESC_LENGTH;
      break;
    case SQL_C_SHORT:
      ARD_record->DESC_LENGTH            = 2;
      ARD_record->DESC_PRECISION         = 0;
      ARD_record->DESC_SCALE             = 0;
      ARD_record->DESC_TYPE              = TargetType;
      ARD_record->DESC_CONCISE_TYPE      = TargetType;
      ARD_record->element_size_in_column_wise = ARD_record->DESC_LENGTH;
      break;
    case SQL_C_SLONG:
      ARD_record->DESC_LENGTH            = 4;
      ARD_record->DESC_PRECISION         = 0;
      ARD_record->DESC_SCALE             = 0;
      ARD_record->DESC_TYPE              = TargetType;
      ARD_record->DESC_CONCISE_TYPE      = TargetType;
      ARD_record->element_size_in_column_wise = ARD_record->DESC_LENGTH;
      break;
    case SQL_C_SBIGINT:
      ARD_record->DESC_LENGTH            = 8;
      ARD_record->DESC_PRECISION         = 0;
      ARD_record->DESC_SCALE             = 0;
      ARD_record->DESC_TYPE              = TargetType;
      ARD_record->DESC_CONCISE_TYPE      = TargetType;
      ARD_record->element_size_in_column_wise = ARD_record->DESC_LENGTH;
      break;
    case SQL_C_DOUBLE:
      ARD_record->DESC_LENGTH            = 8;
      ARD_record->DESC_PRECISION         = 0;
      ARD_record->DESC_SCALE             = 0;
      ARD_record->DESC_TYPE              = TargetType;
      ARD_record->DESC_CONCISE_TYPE      = TargetType;
      ARD_record->element_size_in_column_wise = ARD_record->DESC_LENGTH;
      break;
    case SQL_C_CHAR:
      ARD_record->DESC_LENGTH            = 0; // FIXME:
      ARD_record->DESC_PRECISION         = 0;
      ARD_record->DESC_SCALE             = 0;
      ARD_record->DESC_TYPE              = TargetType;
      ARD_record->DESC_CONCISE_TYPE      = TargetType;
      ARD_record->element_size_in_column_wise = BufferLength;
      break;
    case SQL_C_WCHAR:
      ARD_record->DESC_LENGTH            = 0; // FIXME:
      ARD_record->DESC_PRECISION         = 0;
      ARD_record->DESC_SCALE             = 0;
      ARD_record->DESC_TYPE              = TargetType;
      ARD_record->DESC_CONCISE_TYPE      = TargetType;
      ARD_record->element_size_in_column_wise = BufferLength;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:#%d Column converstion to `%s[0x%x/%d]` not implemented yet",
          ColumnNumber, sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
      break;
  }

  ARD_record->DESC_OCTET_LENGTH        = BufferLength;
  ARD_record->DESC_DATA_PTR            = TargetValuePtr;
  ARD_record->DESC_INDICATOR_PTR       = StrLen_or_IndPtr;
  ARD_record->DESC_OCTET_LENGTH_PTR    = StrLen_or_IndPtr;

  if (ColumnNumber > ARD_header->DESC_COUNT) ARD_header->DESC_COUNT = ColumnNumber;

  ARD_record->bound = 1;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_len(stmt_t *stmt, int row, int col, const char **data, int *len)
{
  TAOS_FIELD *field = stmt->fields + col;
  switch(field->type) {
    case TSDB_DATA_TYPE_BOOL:
      if (CALL_taos_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        unsigned char *base = (unsigned char*)stmt->rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_TINYINT:
      if (CALL_taos_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        unsigned char *base = (unsigned char*)stmt->rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_UTINYINT:
      if (CALL_taos_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        unsigned char *base = (unsigned char*)stmt->rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_SMALLINT:
      if (CALL_taos_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        int16_t *base = (int16_t*)stmt->rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_USMALLINT:
      if (CALL_taos_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        uint16_t *base = (uint16_t*)stmt->rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_INT:
      if (CALL_taos_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        int32_t *base = (int32_t*)stmt->rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_UINT:
      if (CALL_taos_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        uint32_t *base = (uint32_t*)stmt->rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_BIGINT:
      if (CALL_taos_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        int64_t *base = (int64_t*)stmt->rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_FLOAT:
      if (CALL_taos_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        float *base = (float*)stmt->rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_DOUBLE:
      if (CALL_taos_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        double *base = (double*)stmt->rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_VARCHAR: {
      int *offsets = CALL_taos_get_column_data_offset(stmt->res, col);
      OA_ILE(offsets);
      if (offsets[row] == -1) {
        *data = NULL;
        *len = 0;
      } else {
        char *base = (char*)(stmt->rows[col]);
        base += offsets[row];
        int16_t length = *(int16_t*)base;
        base += sizeof(int16_t);
        *data = base;
        *len = length;
      }
    } break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      int64_t *base = (int64_t*)stmt->rows[col];
      base += row;
      *data = (const char*)base;
      *len = sizeof(*base);
    } break;
    case TSDB_DATA_TYPE_NCHAR: {
      int *offsets = CALL_taos_get_column_data_offset(stmt->res, col);
      OA_ILE(offsets);
      if (offsets[row] == -1) {
        *data = NULL;
        *len = 0;
      } else {
        char *base = (char*)(stmt->rows[col]);
        base += offsets[row];
        int16_t length = *(int16_t*)base;
        base += sizeof(int16_t);
        *data = base;
        *len = length;
      }
    } break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:#%d Column[%s] conversion from `%s[0x%x/%d]` not implemented yet",
          col+1, field->name, taos_data_type(field->type), field->type, field->type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static int _stmt_bind_conv_tsdb_tinyint_to_sql_c_utinyint(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(int8_t));

  int8_t v = *(int8_t*)data;

  OD("v:[%d]", v);

  size_t outbytes = dlen;
  OA_NIY(outbytes == sizeof(int8_t));

  // FIXME: check signness?
  *(int8_t*)dest = v;

  return outbytes;
}

static int _stmt_bind_conv_tsdb_utinyint_to_sql_c_utinyint(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(uint8_t));

  uint8_t v = *(uint8_t*)data;

  OD("v:[%u]", v);

  size_t outbytes = dlen;
  OA_NIY(outbytes == sizeof(uint8_t));

  // FIXME: check signness?
  *(uint8_t*)dest = v;

  return outbytes;
}

static SQLRETURN _stmt_get_conv_to_sql_c_utinyint(stmt_t *stmt, tsdb_to_sql_c_f *conv, int taos_type)
{
  (void)conv;
  switch (taos_type) {
    case TSDB_DATA_TYPE_TINYINT:
      *conv = _stmt_bind_conv_tsdb_tinyint_to_sql_c_utinyint;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      *conv = _stmt_bind_conv_tsdb_utinyint_to_sql_c_utinyint;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column converstion from `%s[0x%x/%d]` to `SQL_C_UTINYINT` not implemented yet",
          taos_data_type(taos_type), taos_type, taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static int _stmt_bind_conv_tsdb_tinyint_to_sql_c_short(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(int8_t));

  int8_t v = *(int8_t*)data;

  size_t outbytes = dlen;
  OA_NIY(outbytes == sizeof(int16_t));

  *(int16_t*)dest = v;

  return outbytes;
}

static int _stmt_bind_conv_tsdb_smallint_to_sql_c_short(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(int16_t));

  int16_t v = *(int16_t*)data;

  size_t outbytes = dlen;
  OA_NIY(outbytes == sizeof(int16_t));

  *(int16_t*)dest = v;

  return outbytes;
}

static SQLRETURN _stmt_get_conv_to_sql_c_short(stmt_t *stmt, tsdb_to_sql_c_f *conv, int taos_type)
{
  switch (taos_type) {
    case TSDB_DATA_TYPE_TINYINT:
      *conv = _stmt_bind_conv_tsdb_tinyint_to_sql_c_short;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      *conv = _stmt_bind_conv_tsdb_smallint_to_sql_c_short;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column converstion from `%s[0x%x/%d]` to `SQL_C_SHORT` not implemented yet",
          taos_data_type(taos_type), taos_type, taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static int _stmt_bind_conv_tsdb_int_to_sql_c_slong(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(int32_t));

  int32_t v = *(int32_t*)data;

  size_t outbytes = dlen;
  OA_NIY(outbytes == sizeof(int32_t));

  *(int32_t*)dest = v;

  return outbytes;
}

static int _stmt_bind_conv_tsdb_usmallint_to_sql_c_slong(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(uint16_t));

  uint16_t v = *(uint16_t*)data;

  size_t outbytes = dlen;
  OA_NIY(outbytes == sizeof(int32_t));

  *(int32_t*)dest = v;

  return outbytes;
}

static SQLRETURN _stmt_get_conv_to_sql_c_slong(stmt_t *stmt, tsdb_to_sql_c_f *conv, int taos_type)
{
  switch (taos_type) {
    case TSDB_DATA_TYPE_INT:
      *conv = _stmt_bind_conv_tsdb_int_to_sql_c_slong;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      *conv = _stmt_bind_conv_tsdb_usmallint_to_sql_c_slong;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column converstion from `%s[0x%x/%d]` to `SQL_C_SLONG` not implemented yet",
          taos_data_type(taos_type), taos_type, taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static int _stmt_bind_conv_tsdb_bigint_to_sql_c_sbigint(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;
  (void)dlen;

  OA_NIY(len == sizeof(int64_t));

  int64_t v = *(int64_t*)data;

  *(int64_t*)dest = v;

  return len;
}

static int _stmt_bind_conv_tsdb_uint_to_sql_c_sbigint(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(uint32_t));

  uint32_t v = *(uint32_t*)data;

  size_t outbytes = dlen;
  OA_NIY(outbytes == sizeof(int64_t));

  *(int64_t*)dest = v;

  return outbytes;
}

static SQLRETURN _stmt_get_conv_to_sql_c_sbigint(stmt_t *stmt, tsdb_to_sql_c_f *conv, int taos_type)
{
  switch (taos_type) {
    case TSDB_DATA_TYPE_BIGINT:
      *conv = _stmt_bind_conv_tsdb_bigint_to_sql_c_sbigint;
      break;
    case TSDB_DATA_TYPE_UINT:
      *conv = _stmt_bind_conv_tsdb_uint_to_sql_c_sbigint;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:converstion from `%s[0x%x/%d]` to `SQL_C_SBIGINT` not implemented yet",
          taos_data_type(taos_type), taos_type, taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static int _stmt_bind_conv_tsdb_double_to_sql_c_double(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(double));

  double v = *(double*)data;

  OD("v:[%g]", v);

  size_t outbytes = dlen;
  OA_NIY(outbytes == sizeof(double));

  *(double*)dest = v;

  return outbytes;
}

static SQLRETURN _stmt_get_conv_to_sql_c_double(stmt_t *stmt, tsdb_to_sql_c_f *conv, int taos_type)
{
  switch (taos_type) {
    case TSDB_DATA_TYPE_DOUBLE:
      *conv = _stmt_bind_conv_tsdb_double_to_sql_c_double;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column converstion from `%s[0x%x/%d]` to `SQL_C_DOUBLE` not implemented yet",
          taos_data_type(taos_type), taos_type, taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static int _stmt_bind_conv_tsdb_timestamp_to_sql_c_char(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  OA_NIY(len == sizeof(int64_t));

  int64_t v = *(int64_t*)data;

  char buf[64];
  int n = _tsdb_timestamp_to_string(stmt, v, buf);
  OA_ILE(n > 0);

  char *outbuf = (char*)dest;
  size_t outbytes = dlen;
  // OA_NIY(outbytes >= 2);
  // outbytes -= 2;

  OD("[%.*s]", n, buf);

  int nn = snprintf(outbuf, outbytes, "%.*s", n, buf);
  OA_NIY(nn >= 0 && (size_t)nn < outbytes);

  return nn;
}

static int _stmt_bind_conv_tsdb_bool_to_sql_c_char(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(int8_t));

  int8_t v = *(int8_t*)data ? 1 : 0;

  char *outbuf = (char*)dest;
  size_t outbytes = dlen;

  OD("[%d]", v);

  int nn = snprintf(outbuf, outbytes, "%d", v);
  OA_NIY(nn >= 0 && (size_t)nn < outbytes);

  return nn;
}

static int _stmt_bind_conv_tsdb_int_to_sql_c_char(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(int32_t));

  int32_t v = *(int32_t*)data;

  char *outbuf = (char*)dest;
  size_t outbytes = dlen;

  OD("[%d]", v);

  int nn = snprintf(outbuf, outbytes, "%d", v);
  OA_NIY(nn >= 0 && (size_t)nn < outbytes);

  return nn;
}

static int _stmt_bind_conv_tsdb_bigint_to_sql_c_char(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(int64_t));

  int64_t v = *(int64_t*)data;

  char *outbuf = (char*)dest;
  size_t outbytes = dlen;

  OD("[%" PRId64 "]", v);

  int nn = snprintf(outbuf, outbytes, "%" PRId64 "", v);
  OA_NIY(nn >= 0 && (size_t)nn < outbytes);

  return nn;
}

static int _stmt_bind_conv_tsdb_float_to_sql_c_char(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(float));

  // FIXME: memory alignment?
  float v = *(float*)data;

  char *outbuf = (char*)dest;
  size_t outbytes = dlen;

  OD("[%g]", v);

  int nn = snprintf(outbuf, outbytes, "%g", v);
  OA_NIY(nn >= 0 && (size_t)nn < outbytes);

  return nn;
}

static int _stmt_bind_conv_tsdb_varchar_to_sql_c_char(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  char *outbuf = (char*)dest;
  size_t outbytes = dlen;
  // OA_NIY(outbytes >= 2);
  // outbytes -= 2;

  OD("[%.*s]", len, data);

  int nn = snprintf(outbuf, outbytes, "%.*s", len, data);
  OA_NIY(nn >= 0 && (size_t)nn < outbytes);

  return nn;
}

static int _stmt_bind_conv_tsdb_nchar_to_sql_c_char(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  char *outbuf = (char*)dest;
  size_t outbytes = dlen;
  // OA_NIY(outbytes >= 2);
  // outbytes -= 2;

  OD("[%.*s]", len, data);

  int nn = snprintf(outbuf, outbytes, "%.*s", len, data);
  OA_NIY(nn >= 0 && (size_t)nn < outbytes);

  return nn;
}

static SQLRETURN _stmt_get_conv_to_sql_c_char(stmt_t *stmt, tsdb_to_sql_c_f *conv, int taos_type)
{
  switch (taos_type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      *conv = _stmt_bind_conv_tsdb_timestamp_to_sql_c_char;
      break;
    case TSDB_DATA_TYPE_BOOL:
      *conv = _stmt_bind_conv_tsdb_bool_to_sql_c_char;
      break;
    case TSDB_DATA_TYPE_INT:
      *conv = _stmt_bind_conv_tsdb_int_to_sql_c_char;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *conv = _stmt_bind_conv_tsdb_bigint_to_sql_c_char;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      *conv = _stmt_bind_conv_tsdb_float_to_sql_c_char;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      *conv = _stmt_bind_conv_tsdb_varchar_to_sql_c_char;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      *conv = _stmt_bind_conv_tsdb_nchar_to_sql_c_char;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:converstion from `%s[0x%x/%d]` to `SQL_C_CHAR` not implemented yet",
          taos_data_type(taos_type), taos_type, taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static int _stmt_bind_conv_tsdb_timestamp_to_sql_c_wchar(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  OA_NIY(len == sizeof(int64_t));

  int64_t v = *(int64_t*)data;

  char buf[64];
  int n = _tsdb_timestamp_to_string(stmt, v, buf);
  OA_ILE(n > 0);

  char *inbuf = buf;
  size_t inbytes = n;
  char *outbuf = (char*)dest;
  size_t outbytes = dlen;
  OA_NIY(outbytes >= 2);
  outbytes -= 2;

  OD("[%s]", buf);

  SQLRETURN sr = _stmt_encode(stmt, "UTF8", &inbuf, &inbytes, "UCS-2LE", &outbuf, &outbytes);
  OA_NIY(sql_succeeded(sr));
  outbuf[0] = '\0';
  outbuf[1] = '\0';

  return outbuf - dest;
}

static int _stmt_bind_conv_tsdb_int_to_sql_c_wchar(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  OA_NIY(len == sizeof(int32_t));

  int32_t v = *(int32_t*)data;

  char buf[64];
  int n = snprintf(buf, sizeof(buf), "%d", v);
  OA_ILE(n > 0);

  char *inbuf = buf;
  size_t inbytes = n;
  char *outbuf = (char*)dest;
  size_t outbytes = dlen;
  OA_NIY(outbytes >= 2);
  outbytes -= 2;

  OD("[%s]", buf);

  SQLRETURN sr = _stmt_encode(stmt, "UTF8", &inbuf, &inbytes, "UCS-2LE", &outbuf, &outbytes);
  OA_NIY(sql_succeeded(sr));
  outbuf[0] = '\0';
  outbuf[1] = '\0';

  return outbuf - dest;
}

static int _stmt_bind_conv_tsdb_varchar_to_sql_c_wchar(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  char *inbuf = (char*)data;
  size_t inbytes = len;
  char *outbuf = (char*)dest;
  size_t outbytes = dlen;
  OA_NIY(outbytes >= 2);
  outbytes -= 2;

  OD("[%.*s]", len, data);

  SQLRETURN sr = _stmt_encode(stmt, "UTF8", &inbuf, &inbytes, "UCS-2LE", &outbuf, &outbytes);
  OA_NIY(sql_succeeded(sr));
  outbuf[0] = '\0';
  outbuf[1] = '\0';

  return outbuf - dest;
}

static int _stmt_bind_conv_tsdb_nchar_to_sql_c_wchar(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  char *inbuf = (char*)data;
  size_t inbytes = len;
  char *outbuf = (char*)dest;
  size_t outbytes = dlen;
  OA_NIY(outbytes >= 2);
  outbytes -= 2;

  OD("[%.*s]", len, data);

  SQLRETURN sr = _stmt_encode(stmt, "UTF8", &inbuf, &inbytes, "UCS-2LE", &outbuf, &outbytes);
  OA_NIY(sql_succeeded(sr));
  outbuf[0] = '\0';
  outbuf[1] = '\0';

  return outbuf - dest;
}

static SQLRETURN _stmt_get_conv_to_sql_c_wchar(stmt_t *stmt, tsdb_to_sql_c_f *conv, int taos_type)
{
  switch (taos_type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      *conv = _stmt_bind_conv_tsdb_timestamp_to_sql_c_wchar;
      break;
    case TSDB_DATA_TYPE_INT:
      *conv = _stmt_bind_conv_tsdb_int_to_sql_c_wchar;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      *conv = _stmt_bind_conv_tsdb_varchar_to_sql_c_wchar;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      *conv = _stmt_bind_conv_tsdb_nchar_to_sql_c_wchar;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column converstion from `%s[0x%x/%d]` to `SQL_C_WCHAR` not implemented yet",
          taos_data_type(taos_type), taos_type, taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_from_tsdb_to_sql_c(stmt_t *stmt, tsdb_to_sql_c_f *conv, int taos_type, SQLSMALLINT TargetType)
{
  switch (TargetType) {
    case SQL_C_UTINYINT:
      return _stmt_get_conv_to_sql_c_utinyint(stmt, conv, taos_type);
    case SQL_C_SHORT:
      return _stmt_get_conv_to_sql_c_short(stmt, conv, taos_type);
    case SQL_C_SLONG:
      return _stmt_get_conv_to_sql_c_slong(stmt, conv, taos_type);
    case SQL_C_SBIGINT:
      return _stmt_get_conv_to_sql_c_sbigint(stmt, conv, taos_type);
    case SQL_C_DOUBLE:
      return _stmt_get_conv_to_sql_c_double(stmt, conv, taos_type);
    case SQL_C_CHAR:
      return _stmt_get_conv_to_sql_c_char(stmt, conv, taos_type);
    case SQL_C_WCHAR:
      return _stmt_get_conv_to_sql_c_wchar(stmt, conv, taos_type);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column converstion to `%s[0x%x/%d]` not implemented yet",
          sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLPOINTER _stmt_get_address(stmt_t *stmt, SQLPOINTER ptr, SQLULEN element_size_in_column_wise, int i_row, desc_header_t *header)
{
  (void)stmt;
  char *base = (char*)ptr;
  if (header->DESC_BIND_OFFSET_PTR) base += *header->DESC_BIND_OFFSET_PTR;

  char *dest = base + element_size_in_column_wise * i_row;

  return dest;
}

static void _stmt_set_len_or_ind(stmt_t *stmt, int i_row, desc_header_t *header, desc_record_t *record, SQLLEN len_or_ind)
{
  SQLPOINTER ptr;
  if (len_or_ind == SQL_NULL_DATA && record->DESC_INDICATOR_PTR) {
    ptr = record->DESC_INDICATOR_PTR;
  } else if (len_or_ind != SQL_NULL_DATA && record->DESC_OCTET_LENGTH_PTR) {
    ptr = record->DESC_OCTET_LENGTH_PTR;
  } else {
    return;
  }
  SQLPOINTER p = _stmt_get_address(stmt, ptr, sizeof(SQLLEN), i_row, header);
  *(SQLLEN*)p = len_or_ind;
}

static SQLRETURN _stmt_fill_rowset(stmt_t *stmt, int i_row, int i_col)
{
  SQLRETURN sr;

  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;

  if (i_col >= ARD_header->DESC_COUNT) return SQL_SUCCESS;

  desc_record_t *ARD_record = ARD->records + i_col;
  if (ARD_record->DESC_DATA_PTR == NULL) return SQL_SUCCESS;

  char *base = ARD_record->DESC_DATA_PTR;
  if (ARD_header->DESC_BIND_OFFSET_PTR) base += *ARD_header->DESC_BIND_OFFSET_PTR;

  char *dest = base + ARD_record->element_size_in_column_wise * i_row;
  char *ptr = ARD_record->DESC_DATA_PTR;
  dest = _stmt_get_address(stmt, ptr, ARD_record->element_size_in_column_wise, i_row, ARD_header);

  const char *data;
  int len;
  sr = _stmt_get_data_len(stmt, i_row + stmt->rowset.i_row, i_col, &data, &len);
  if (!sql_succeeded(sr)) return SQL_ERROR;

  if (!data) {
    _stmt_set_len_or_ind(stmt, i_row, ARD_header, ARD_record, SQL_NULL_DATA);
    OD("null");
    return SQL_SUCCESS;
  }

  int bytes = ARD_record->conv(stmt, data, len, dest, ARD_record->DESC_OCTET_LENGTH);
  OA_NIY(bytes >= 0);
  if (bytes < 0) return SQL_ERROR;

  _stmt_set_len_or_ind(stmt, i_row, ARD_header, ARD_record, bytes);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_fetch_next_rowset(stmt_t *stmt, TAOS_ROW *rows)
{
  rowset_reset(&stmt->rowset);

  // TODO:
  // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlfetch-function?view=sql-server-ver16#positioning-the-cursor
  int nr_rows = CALL_taos_fetch_block(stmt->res, rows);
  if (nr_rows == 0) return SQL_NO_DATA;
  stmt->rows = *rows;          // column-wise
  stmt->nr_rows = nr_rows;
  stmt->rowset.i_row = 0;

  stmt->lengths = CALL_taos_fetch_lengths(stmt->res);
  OA_NIY(stmt->lengths);

  return SQL_SUCCESS;
}

SQLRETURN _stmt_fetch(stmt_t *stmt)
{
  SQLULEN row_array_size = _stmt_get_row_array_size(stmt);
  OA_NIY(row_array_size > 0);

  SQLRETURN sr = SQL_SUCCESS;

  TAOS_ROW rows = NULL;
  OA_NIY(stmt->res);
  stmt->rowset.i_row += row_array_size;
  if (stmt->rowset.i_row >= stmt->nr_rows) {
    sr = _stmt_fetch_next_rowset(stmt, &rows);
    if (sr == SQL_ERROR) return SQL_ERROR;
    if (sr == SQL_NO_DATA) return SQL_NO_DATA;
  }

  post_filter_t *post_filter = &stmt->post_filter;
  if (post_filter->post_filter) {
    while (1) {
      int filter = 0;
      sr = post_filter->post_filter(stmt, stmt->rowset.i_row, post_filter->ctx, &filter);
      if (sr == SQL_ERROR) return SQL_ERROR;
      if (!filter) break;

      ++stmt->rowset.i_row;
      if (stmt->rowset.i_row >= stmt->nr_rows) {
        sr = _stmt_fetch_next_rowset(stmt, &rows);
        if (sr == SQL_ERROR) return SQL_ERROR;
        if (sr == SQL_NO_DATA) return SQL_NO_DATA;
      }
    }
  }

  _stmt_reset_current_for_get_data(stmt);

  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;

  descriptor_t *IRD = _stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;

  OA_NIY(ARD->cap >= ARD_header->DESC_COUNT);
  for (int i_col = 0; (size_t)i_col < ARD->cap; ++i_col) {
    if (i_col >= ARD_header->DESC_COUNT) continue;
    desc_record_t *record = ARD->records + i_col;
    if (!record->bound) continue;
    if (record->DESC_DATA_PTR == NULL) continue;

    TAOS_FIELD *field = stmt->fields + i_col;
    int taos_type = field->type;

    SQLSMALLINT TargetType = record->DESC_TYPE;

    sr = _stmt_get_conv_from_tsdb_to_sql_c(stmt, &record->conv, taos_type, TargetType);
    if (!sql_succeeded(sr)) return SQL_ERROR;
  }

  if (ARD_header->DESC_BIND_TYPE != SQL_BIND_BY_COLUMN) {
    stmt_append_err(stmt, "HY000", 0, "General error:only `SQL_BIND_BY_COLUMN` is supported now");
    return SQL_ERROR;
  }

  if (IRD_header->DESC_ROWS_PROCESSED_PTR) *IRD_header->DESC_ROWS_PROCESSED_PTR = 0;

  int iRow = 0;
  for (int i_row = 0; (SQLULEN)i_row<row_array_size; ++i_row) {
    if (i_row + stmt->rowset.i_row >= stmt->nr_rows) break;

    if (post_filter->post_filter) {
      int filter = 0;
again:
      sr = post_filter->post_filter(stmt, i_row + stmt->rowset.i_row, post_filter->ctx, &filter);
      if (sr == SQL_ERROR) return SQL_ERROR;
      if (filter) {
        ++i_row;
        if (i_row + stmt->rowset.i_row >= stmt->nr_rows) break;
        goto again;
      }
    }

    for (int i_col = 0; (size_t)i_col < ARD->cap; ++i_col) {
      sr = _stmt_fill_rowset(stmt, i_row, i_col);
      if (sr == SQL_ERROR) return SQL_ERROR;
    }

    if (IRD_header->DESC_ARRAY_STATUS_PTR) IRD_header->DESC_ARRAY_STATUS_PTR[iRow] = SQL_ROW_SUCCESS;
    if (IRD_header->DESC_ROWS_PROCESSED_PTR) *IRD_header->DESC_ROWS_PROCESSED_PTR += 1;
    ++iRow;
  }

  return SQL_SUCCESS;
}

SQLRETURN stmt_fetch_scroll(stmt_t *stmt,
    SQLSMALLINT   FetchOrientation,
    SQLLEN        FetchOffset)
{
  switch (FetchOrientation) {
    case SQL_FETCH_NEXT:
      (void)FetchOffset;
      return _stmt_fetch(stmt);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:[%s] not implemented yet",
          sql_fetch_orientation(FetchOrientation));
      return SQL_ERROR;
  }
}

SQLRETURN stmt_fetch(stmt_t *stmt)
{
  return _stmt_fetch(stmt);
}

static void _stmt_close_cursor(stmt_t *stmt)
{
  (void)stmt;
}

SQLRETURN stmt_get_diag_rec(
    stmt_t         *stmt,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr)
{
  return errs_get_diag_rec(&stmt->errs, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
}

typedef SQLRETURN (*stmt_get_data_fill_f)(
    stmt_t        *stmt,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr);

static SQLRETURN _stmt_get_data_fill_sql_c_char_with_buf(
    stmt_t        *stmt,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  col_t *current = &stmt->current_for_get_data;
  const char *data = current->data;
  int len = current->len;

  int n = snprintf(TargetValuePtr, BufferLength, "%.*s", len, data);
  OA_NIY(n > 0);
  if (TargetValuePtr) {
    if (BufferLength > 0) {
      int sn = strlen(TargetValuePtr);
      current->data += sn;
      current->len  -= sn;
    }
  }
  if (n >= BufferLength) {
    if (StrLen_or_IndPtr) *StrLen_or_IndPtr = n;
    stmt_append_err_format(stmt, "01004", 0, "String data, right truncated:#%d Column_or_Param[%.*s]", current->i_col + 1, len, data);
    return SQL_SUCCESS_WITH_INFO;
  } else {
    if (StrLen_or_IndPtr) *StrLen_or_IndPtr = n;
  }
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_fill_sql_c_char_with_tsdb_varchar(
    stmt_t        *stmt,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  return _stmt_get_data_fill_sql_c_char_with_buf(stmt, TargetValuePtr, BufferLength, StrLen_or_IndPtr);
}

static SQLRETURN _stmt_get_data_fill_sql_c_char_with_tsdb_nchar(
    stmt_t        *stmt,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  return _stmt_get_data_fill_sql_c_char_with_buf(stmt, TargetValuePtr, BufferLength, StrLen_or_IndPtr);
}

static int _col_copy_buf(col_t *col, const char *buf, size_t len)
{
  col->nr = 0;
  if (col->nr + len >= col->cap) {
    size_t cap = (col->nr + len + 15) / 16 * 16;
    char *p = (char*)realloc(col->buf, cap + 1);
    if (!p) return -1;
    col->buf = p;
    col->cap = cap;
  }

  strncpy(col->buf + col->nr, buf, len);
  col->nr += len;
  col->buf[col->nr] = '\0';

  return 0;
}

static int _stmt_get_data_reinit_current_with_buf(stmt_t *stmt, const char *buf, size_t len)
{
  col_t *current = &stmt->current_for_get_data;

  int r = _col_copy_buf(current, buf, len);
  if (r) return -1;

  current->data = current->buf;
  current->len  = current->nr;

  return 0;
}

static SQLRETURN _stmt_get_data_fill_sql_c_char_with_tsdb_timestamp(
    stmt_t        *stmt,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  col_t *current = &stmt->current_for_get_data;
  const char *data = current->data;
  int len = current->len;

  if (data < current->buf || data + len > current->buf + current->nr) {
    int64_t val = *(int64_t*)data;

    char buf[64];
    int n = _tsdb_timestamp_to_string(stmt, val, buf);
    OA_ILE(n > 0);

    int r = _stmt_get_data_reinit_current_with_buf(stmt, buf, n);
    if (r) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
  }

  return _stmt_get_data_fill_sql_c_char_with_buf(stmt, TargetValuePtr, BufferLength, StrLen_or_IndPtr);
}

static SQLRETURN _stmt_get_data_fill_sql_c_char_with_tsdb_int(
    stmt_t        *stmt,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  col_t *current = &stmt->current_for_get_data;
  const char *data = current->data;
  int len = current->len;

  if (data < current->buf || data + len > current->buf + current->nr) {
    int32_t val = *(int32_t*)data;

    char buf[64];
    int n = snprintf(buf, sizeof(buf), "%d", val);
    OA_ILE(n > 0);

    int r = _stmt_get_data_reinit_current_with_buf(stmt, buf, n);
    if (r) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
  }

  return _stmt_get_data_fill_sql_c_char_with_buf(stmt, TargetValuePtr, BufferLength, StrLen_or_IndPtr);
}

static SQLRETURN _stmt_get_data_fill_sql_c_char_with_tsdb_bigint(
    stmt_t        *stmt,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  col_t *current = &stmt->current_for_get_data;
  const char *data = current->data;
  int len = current->len;

  if (data < current->buf || data + len > current->buf + current->nr) {
    int64_t val = *(int64_t*)data;

    char buf[64];
    int n = snprintf(buf, sizeof(buf), "%" PRId64 "", val);
    OA_ILE(n > 0);

    int r = _stmt_get_data_reinit_current_with_buf(stmt, buf, n);
    if (r) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
  }

  return _stmt_get_data_fill_sql_c_char_with_buf(stmt, TargetValuePtr, BufferLength, StrLen_or_IndPtr);
}

static SQLRETURN _stmt_get_data_fill_sql_c_float_with_tsdb_float(
    stmt_t        *stmt,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  (void)BufferLength;

  col_t *current = &stmt->current_for_get_data;
  const char *data = current->data;
  int len = current->len;
  OA_NIY(len == sizeof(float));
  float v = *(float*)data;
  if (StrLen_or_IndPtr) *StrLen_or_IndPtr = sizeof(float);
  *(float*)TargetValuePtr = v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_fill_sql_c_double_with_tsdb_double(
    stmt_t        *stmt,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  (void)BufferLength;

  col_t *current = &stmt->current_for_get_data;
  const char *data = current->data;
  int len = current->len;
  OA_NIY(len == sizeof(double));
  double v = *(double*)data;
  if (StrLen_or_IndPtr) *StrLen_or_IndPtr = sizeof(double);
  *(double*)TargetValuePtr = v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_fill_sql_c_slong_with_tsdb_bigint(
    stmt_t        *stmt,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  (void)BufferLength;

  col_t *current = &stmt->current_for_get_data;
  const char *data = current->data;
  int len = current->len;
  OA_NIY(len == sizeof(int64_t));
  int64_t v = *(int64_t*)data;
  if (StrLen_or_IndPtr) *StrLen_or_IndPtr = sizeof(int32_t);
  *(int32_t*)TargetValuePtr = (int32_t)v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_fill_sql_c_slong_with_tsdb_int(
    stmt_t        *stmt,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  (void)BufferLength;

  col_t *current = &stmt->current_for_get_data;
  const char *data = current->data;
  int len = current->len;
  OA_NIY(len == sizeof(int32_t));
  int32_t v = *(int32_t*)data;
  if (StrLen_or_IndPtr) *StrLen_or_IndPtr = sizeof(int32_t);
  *(int32_t*)TargetValuePtr = v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_fill_sql_c_sbigint_with_tsdb_bigint(
    stmt_t        *stmt,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  (void)BufferLength;

  col_t *current = &stmt->current_for_get_data;
  const char *data = current->data;
  int len = current->len;
  OA_NIY(len == sizeof(int64_t));
  int64_t v = *(int64_t*)data;
  if (StrLen_or_IndPtr) *StrLen_or_IndPtr = sizeof(int64_t);
  *(int64_t*)TargetValuePtr = v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_fill_sql_c_sbigint_with_tsdb_timestamp(
    stmt_t        *stmt,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  (void)BufferLength;

  col_t *current = &stmt->current_for_get_data;
  const char *data = current->data;
  int len = current->len;
  OA_NIY(len == sizeof(int64_t));
  int64_t v = *(int64_t*)data;
  if (StrLen_or_IndPtr) *StrLen_or_IndPtr = sizeof(int64_t);
  *(int64_t*)TargetValuePtr = v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_fill_fn_by_target_sql_c_char(stmt_t *stmt, stmt_get_data_fill_f *fill)
{
  col_t *current = &stmt->current_for_get_data;
  TAOS_FIELD *field = current->field;
  int taos_type = field->type;
  switch (taos_type) {
    case TSDB_DATA_TYPE_VARCHAR:
      *fill = _stmt_get_data_fill_sql_c_char_with_tsdb_varchar;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      *fill = _stmt_get_data_fill_sql_c_char_with_tsdb_nchar;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      *fill = _stmt_get_data_fill_sql_c_char_with_tsdb_timestamp;
      break;
    case TSDB_DATA_TYPE_INT:
      *fill = _stmt_get_data_fill_sql_c_char_with_tsdb_int;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *fill = _stmt_get_data_fill_sql_c_char_with_tsdb_bigint;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column converstion from `%s[0x%x/%d]` to `SQL_C_CHAR` not implemented yet",
          taos_data_type(taos_type), taos_type, taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_fill_fn_by_target_sql_c_float(stmt_t *stmt, stmt_get_data_fill_f *fill)
{
  col_t *current = &stmt->current_for_get_data;
  TAOS_FIELD *field = current->field;
  int taos_type = field->type;
  *fill = NULL;
  switch (taos_type) {
    case TSDB_DATA_TYPE_FLOAT:
      *fill = _stmt_get_data_fill_sql_c_float_with_tsdb_float;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column converstion from `%s[0x%x/%d]` to `SQL_C_FLOAT` not implemented yet",
          taos_data_type(taos_type), taos_type, taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_fill_fn_by_target_sql_c_double(stmt_t *stmt, stmt_get_data_fill_f *fill)
{
  col_t *current = &stmt->current_for_get_data;
  TAOS_FIELD *field = current->field;
  int taos_type = field->type;
  *fill = NULL;
  switch (taos_type) {
    case TSDB_DATA_TYPE_DOUBLE:
      *fill = _stmt_get_data_fill_sql_c_double_with_tsdb_double;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column converstion from `%s[0x%x/%d]` to `SQL_C_DOUBLE` not implemented yet",
          taos_data_type(taos_type), taos_type, taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_fill_fn_by_target_sql_c_slong(stmt_t *stmt, stmt_get_data_fill_f *fill)
{
  col_t *current = &stmt->current_for_get_data;
  TAOS_FIELD *field = current->field;
  int taos_type = field->type;
  *fill = NULL;
  switch (taos_type) {
    case TSDB_DATA_TYPE_INT:
      *fill = _stmt_get_data_fill_sql_c_slong_with_tsdb_int;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *fill = _stmt_get_data_fill_sql_c_slong_with_tsdb_bigint;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column converstion from `%s[0x%x/%d]` to `SQL_C_SLONG` not implemented yet",
          taos_data_type(taos_type), taos_type, taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_fill_fn_by_target_sql_c_sbigint(stmt_t *stmt, stmt_get_data_fill_f *fill)
{
  col_t *current = &stmt->current_for_get_data;
  TAOS_FIELD *field = current->field;
  int taos_type = field->type;
  *fill = NULL;
  switch (taos_type) {
    case TSDB_DATA_TYPE_BIGINT:
      *fill = _stmt_get_data_fill_sql_c_sbigint_with_tsdb_bigint;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      *fill = _stmt_get_data_fill_sql_c_sbigint_with_tsdb_timestamp;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column converstion from `%s[0x%x/%d]` to `SQL_C_SBIGINT` not implemented yet",
          taos_data_type(taos_type), taos_type, taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

SQLRETURN stmt_get_data(
    stmt_t        *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetdata-function?view=sql-server-ver16
  // https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/getting-long-data?view=sql-server-ver16
  OA_NIY(stmt->res);
  OA_NIY(stmt->rows);
  if (StrLen_or_IndPtr) StrLen_or_IndPtr[0] = SQL_NO_TOTAL;

  if (Col_or_Param_Num < 1 || Col_or_Param_Num > stmt->col_count) {
    stmt_append_err_format(stmt, "07009", 0, "Invalid descriptor index:#%d Col_or_Param", Col_or_Param_Num);
    return SQL_ERROR;
  }

  SQLRETURN sr = SQL_SUCCESS;

  col_t *current = &stmt->current_for_get_data;
  if (current->i_col + 1 != Col_or_Param_Num) {
    col_reset(current);
    current->i_col = Col_or_Param_Num - 1;
    current->field = stmt->fields + current->i_col;

    const char *data;
    int len;

    sr = _stmt_get_data_len(stmt, stmt->rowset.i_row, current->i_col, &data, &len);
    if (!sql_succeeded(sr)) return sr;

    current->data = data;
    current->len  = len;

    current->TargetType = TargetType;
  } else {
    if (current->TargetType != TargetType) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:TargetType changes in successive SQLGetData call for #%d Column_or_Param", Col_or_Param_Num);
      return SQL_ERROR;
    }
  }
  if (current->data == NULL) {
    if (StrLen_or_IndPtr) {
      StrLen_or_IndPtr[0/*stmt->rowset.i_row*/] = SQL_NULL_DATA;
      return SQL_SUCCESS;
    }
    stmt_append_err_format(stmt, "22002", 0, "Indicator variable required but not supplied:#%d Column_or_Param", Col_or_Param_Num);
    return SQL_ERROR;
  }

  stmt_get_data_fill_f fill = NULL;

  switch (TargetType) {
    case SQL_C_CHAR:
      if (current->len == 0) {
        if (StrLen_or_IndPtr) *StrLen_or_IndPtr = SQL_NULL_DATA;
        return SQL_SUCCESS;
      }

      sr = _stmt_get_data_fill_fn_by_target_sql_c_char(stmt, &fill);
      break;
    case SQL_C_FLOAT:
      if (current->len == 0) {
        if (StrLen_or_IndPtr) *StrLen_or_IndPtr = SQL_NULL_DATA;
        return SQL_SUCCESS;
      }

      sr = _stmt_get_data_fill_fn_by_target_sql_c_float(stmt, &fill);
      break;
    case SQL_C_DOUBLE:
      if (current->len == 0) {
        if (StrLen_or_IndPtr) *StrLen_or_IndPtr = SQL_NULL_DATA;
        return SQL_SUCCESS;
      }

      sr = _stmt_get_data_fill_fn_by_target_sql_c_double(stmt, &fill);
      break;
    case SQL_C_SLONG:
      if (current->len == 0) {
        if (StrLen_or_IndPtr) *StrLen_or_IndPtr = SQL_NULL_DATA;
        return SQL_SUCCESS;
      }

      sr = _stmt_get_data_fill_fn_by_target_sql_c_slong(stmt, &fill);
      break;
    case SQL_C_SBIGINT:
      if (current->len == 0) {
        if (StrLen_or_IndPtr) *StrLen_or_IndPtr = SQL_NULL_DATA;
        return SQL_SUCCESS;
      }

      sr = _stmt_get_data_fill_fn_by_target_sql_c_sbigint(stmt, &fill);
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column converstion to `%s[0x%x/%d]` not implemented yet",
          sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
  }

  if (sr == SQL_ERROR) return sr;

  return fill(stmt, TargetValuePtr, BufferLength, StrLen_or_IndPtr);
}

static SQLRETURN _stmt_describe_tags(stmt_t *stmt)
{
  int r = 0;

  int tagNum = 0;
  TAOS_FIELD_E *tags = NULL;
  r = CALL_taos_stmt_get_tag_fields(stmt->stmt, &tagNum, &tags);
  if (r) {
    stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_errstr(NULL));
    return SQL_ERROR;
  }
  stmt->nr_tag_fields = tagNum;
  stmt->tag_fields    = tags;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_describe_cols(stmt_t *stmt)
{
  int r = 0;

  int colNum = 0;
  TAOS_FIELD_E *cols = NULL;
  r = CALL_taos_stmt_get_col_fields(stmt->stmt, &colNum, &cols);
  if (r) {
    stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_errstr(NULL));
    return SQL_ERROR;
  }
  stmt->nr_col_fields = colNum;
  stmt->col_fields    = cols;

  return SQL_SUCCESS;
}

static SQLSMALLINT _stmt_get_count_of_tsdb_params(stmt_t *stmt)
{
  SQLSMALLINT n = 0;

  if (stmt->is_insert_stmt) {
    n = !!stmt->subtbl_required;
    n += stmt->nr_tag_fields;
    n += stmt->nr_col_fields;
  } else {
    n = stmt->nr_params;
  }

  return n;
}

SQLRETURN _stmt_prepare(stmt_t *stmt, const char *sql, size_t len)
{
  int r = 0;

  SQLRETURN sr = SQL_SUCCESS;

  stmt->stmt = CALL_taos_stmt_init(stmt->conn->taos);
  if (!stmt->stmt) {
    stmt_append_err_format(stmt, "HY000", CALL_taos_errno(NULL), "General error:[taosc]%s", CALL_taos_errstr(NULL));
    return SQL_ERROR;
  }

  r = CALL_taos_stmt_prepare(stmt->stmt, sql, len);
  if (r) {
    stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_errstr(NULL));
    return SQL_ERROR;
  }

  int32_t isInsert = 0;
  r = CALL_taos_stmt_is_insert(stmt->stmt, &isInsert);
  isInsert = !!isInsert;

  if (r) {
    stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
    _stmt_release_stmt(stmt);

    return SQL_ERROR;
  }

  stmt->is_insert_stmt = isInsert;

  if (stmt->is_insert_stmt) {
    int tagNum = 0;
    TAOS_FIELD_E *tag_fields = NULL;
    r = CALL_taos_stmt_get_tag_fields(stmt->stmt, &tagNum, &tag_fields);
    if (r) {
      int e = CALL_taos_errno(NULL);
      if (e == TSDB_CODE_TSC_STMT_TBNAME_ERROR) {
        // fake subtbl name to get tags/cols meta-info

        r = CALL_taos_stmt_set_tbname(stmt->stmt, "__hard_coded_fake_name__");
        if (r) {
          stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
          return SQL_ERROR;
        }

        sr = _stmt_describe_tags(stmt);
        if (sr == SQL_ERROR) return SQL_ERROR;

        sr = _stmt_describe_cols(stmt);
        if (sr == SQL_ERROR) return SQL_ERROR;

        // insert into ? ... will result in TSDB_CODE_TSC_STMT_TBNAME_ERROR
        stmt->subtbl_required = 1;
        r = 0;
      } else if (e == TSDB_CODE_TSC_STMT_API_ERROR) {
        // insert into t ... and t is normal tablename, will result in TSDB_CODE_TSC_STMT_API_ERROR
        stmt->subtbl_required = 0;
        stmt_append_err(stmt, "HY000", r, "General error:this is believed an non-subtbl insert statement");
        sr = _stmt_describe_cols(stmt);
        if (sr == SQL_ERROR) return SQL_ERROR;
        r = 0;
      }
      free(tag_fields);
    } else {
      stmt->nr_tag_fields = tagNum;
      TOD_SAFE_FREE(stmt->tag_fields);
      stmt->tag_fields    = tag_fields;
      sr = _stmt_describe_cols(stmt);
      if (sr == SQL_ERROR) return SQL_ERROR;
    }
  } else {
    int nr_params = 0;
    r = CALL_taos_stmt_num_params(stmt->stmt, &nr_params);
    if (r) {
      stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
      _stmt_release_stmt(stmt);

      return SQL_ERROR;
    }

    stmt->nr_params = nr_params;
    for (int i=0; i<nr_params; ++i) {
    }
  }
  if (r) return SQL_ERROR;

  SQLSMALLINT n = _stmt_get_count_of_tsdb_params(stmt);
  if (n <= 0) {
    stmt_append_err(stmt, "HY000", 0, "General error:statement-without-parameter-placemarker not allowed to be prepared");
    return SQL_ERROR;
  }

  if ((size_t)n > stmt->cap_mbs) {
    size_t cap = (n + 15) / 16 * 16;
    TAOS_MULTI_BIND *mbs = (TAOS_MULTI_BIND*)realloc(stmt->mbs, sizeof(*mbs) * cap);
    if (!mbs) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
    stmt->mbs = mbs;
    stmt->cap_mbs = cap;
    stmt->nr_mbs = n;

    memset(stmt->mbs, 0, sizeof(*stmt->mbs) * stmt->nr_mbs);
  }

  stmt->prepared = 1;

  return SQL_SUCCESS;
}

static void _stmt_unprepare(stmt_t *stmt)
{
  _stmt_release_tag_fields(stmt);
  _stmt_release_col_fields(stmt);
  stmt->nr_params = 0;
  stmt->is_insert_stmt = 0;
  stmt->subtbl_required = 0;
  TOD_SAFE_FREE(stmt->subtbl);
  stmt->nr_mbs = 0;

  stmt->prepared = 0;
}

static SQLRETURN _stmt_get_tag_or_col_field(stmt_t *stmt, int iparam, TAOS_FIELD_E *field)
{
  TAOS_FIELD_E *p;
  if (stmt->subtbl_required) {
    if (iparam == 0) {
      field->type          = TSDB_DATA_TYPE_VARCHAR;
      field->bytes         = 1024; // TODO: check taos-doc for max length of subtable name
      return SQL_SUCCESS;
    }
    if (iparam < 1 + stmt->nr_tag_fields) {
      p = stmt->tag_fields + iparam - 1;
    } else {
      p = stmt->col_fields + iparam - 1 - stmt->nr_tag_fields;
    }
  } else {
    p = stmt->col_fields + iparam;
  }

  memcpy(field, p, sizeof(*field));

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_tsdb_type(stmt_t *stmt, int iparam, int *tsdb_type, int *tsdb_bytes)
{
  SQLRETURN sr = SQL_SUCCESS;

  TAOS_FIELD_E field = {};

  sr = _stmt_get_tag_or_col_field(stmt, iparam, &field);
  if (sr == SQL_ERROR) return SQL_ERROR;

  *tsdb_type  = field.type;
  *tsdb_bytes = field.bytes;

  return SQL_SUCCESS;
}

SQLRETURN stmt_get_num_params(
    stmt_t         *stmt,
    SQLSMALLINT    *ParameterCountPtr)
{
  SQLSMALLINT n = _stmt_get_count_of_tsdb_params(stmt);

  *ParameterCountPtr = n;

  return SQL_SUCCESS;
}

SQLRETURN _stmt_describe_param_by_field(
    stmt_t         *stmt,
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
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:#%d param:`%s[0x%x/%d]` not implemented yet", ParameterNumber, taos_data_type(type), type, type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

SQLRETURN stmt_describe_param(
    stmt_t         *stmt,
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

  if (ParameterNumber == 0) {
    stmt_append_err(stmt, "HY000", 0, "General error:bookmark column not supported yet");
    return SQL_ERROR;
  }

  if (stmt->is_insert_stmt) {
    TAOS_FIELD_E *field = NULL;
    if (stmt->subtbl_required) {
      // NOTE: DM to make sure ParameterNumber is valid
      // SQLUSMALLINT nr_total = 1 + stmt->nr_tag_fields + stmt->nr_col_fields;
      // if (ParameterNumber > nr_total) {
      //   stmt_append_err_format(stmt, "HY000", 0,
      //       "General error:#%d parameter field out of range [%d parameter markers in total]", ParameterNumber, nr_total);
      //   return SQL_ERROR;
      // }
      if (ParameterNumber == 1) {
        *DataTypePtr = SQL_VARCHAR;
        *ParameterSizePtr = 1024; // TODO: check taos-doc for max length of subtable name
        *DecimalDigitsPtr = 0;
        *NullablePtr = SQL_NO_NULLS;
        return SQL_SUCCESS;
      } else if (ParameterNumber <= 1 + stmt->nr_tag_fields) {
        field = stmt->tag_fields + ParameterNumber - 1 - 1;
      } else {
        field = stmt->col_fields + ParameterNumber - 1 - 1 - stmt->nr_tag_fields;
      }
    } else {
      // NOTE: DM to make sure ParameterNumber is valid
      // if (ParameterNumber > stmt->nr_col_fields) {
      //   stmt_append_err_format(stmt, "HY000", 0,
      //       "General error:#%d param field out of range [%d parameter markers in total]", ParameterNumber, stmt->nr_col_fields);
      //   return SQL_ERROR;
      // }
      field = stmt->col_fields + ParameterNumber - 1;
    }
    return _stmt_describe_param_by_field(stmt, ParameterNumber, DataTypePtr, ParameterSizePtr, DecimalDigitsPtr, NullablePtr, field);
  }

  r = CALL_taos_stmt_get_param(stmt->stmt, idx, &type, &bytes);
  if (r) {
    // FIXME: return SQL_VARCHAR and hard-coded parameters for the moment
    if (DataTypePtr)       *DataTypePtr       = SQL_VARCHAR;
    if (ParameterSizePtr)  *ParameterSizePtr  = 1024;
    if (DecimalDigitsPtr)  *DecimalDigitsPtr  = 0;
    if (NullablePtr)       *NullablePtr       = SQL_NULLABLE_UNKNOWN;
    stmt_append_err(stmt, "01000", 0,
        "General warning:Arbitrary `SQL_VARCHAR(1024)` is chosen to return because of taos lacking parm-desc for non-insert-statement");
    return SQL_SUCCESS_WITH_INFO;
  }

  stmt_append_err(stmt, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _stmt_guess_taos_data_type(
    stmt_t         *stmt,
    SQLSMALLINT     ParameterType,
    SQLLEN          BufferLength,
    int            *taos_type,
    int            *taos_bytes)
{
  switch (ParameterType) {
    case SQL_VARCHAR:
      *taos_type  = TSDB_DATA_TYPE_VARCHAR;
      *taos_bytes = BufferLength;
      break;
    case SQL_BIGINT:
      *taos_type = TSDB_DATA_TYPE_BIGINT;
      *taos_bytes = sizeof(int64_t);
      break;
    case SQL_INTEGER:
      *taos_type = TSDB_DATA_TYPE_INT;
      *taos_bytes = sizeof(int32_t);
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
        "General error:unable to guess taos-data-type for `%s[%d]`",
        sql_data_type(ParameterType), ParameterType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN _stmt_create_tsdb_timestamp_array(stmt_t *stmt, desc_record_t *record, int rows, TAOS_MULTI_BIND *mb)
{
  void *p = buf_realloc(&record->data_buffer, sizeof(int64_t) * rows);
  if (!p) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  mb->buffer = p;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_create_tsdb_int_array(stmt_t *stmt, desc_record_t *record, int rows, TAOS_MULTI_BIND *mb)
{
  void *p = buf_realloc(&record->data_buffer, sizeof(int32_t) * rows);
  if (!p) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  mb->buffer = p;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_create_tsdb_smallint_array(stmt_t *stmt, desc_record_t *record, int rows, TAOS_MULTI_BIND *mb)
{
  void *p = buf_realloc(&record->data_buffer, sizeof(int16_t) * rows);
  if (!p) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  mb->buffer = p;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_create_tsdb_tinyint_array(stmt_t *stmt, desc_record_t *record, int rows, TAOS_MULTI_BIND *mb)
{
  void *p = buf_realloc(&record->data_buffer, sizeof(int8_t) * rows);
  if (!p) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  mb->buffer = p;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_create_tsdb_bool_array(stmt_t *stmt, desc_record_t *record, int rows, TAOS_MULTI_BIND *mb)
{
  void *p = buf_realloc(&record->data_buffer, sizeof(int8_t) * rows);
  if (!p) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  mb->buffer = p;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_create_tsdb_float_array(stmt_t *stmt, desc_record_t *record, int rows, TAOS_MULTI_BIND *mb)
{
  void *p = buf_realloc(&record->data_buffer, sizeof(float) * rows);
  if (!p) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  mb->buffer = p;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_create_tsdb_varchar_array(stmt_t *stmt, desc_record_t *record, int rows, TAOS_MULTI_BIND *mb)
{
  void *p = buf_realloc(&record->data_buffer, mb->buffer_length * rows);
  if (!p) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  mb->buffer = p;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_create_length_array(stmt_t *stmt, desc_record_t *record, int rows, TAOS_MULTI_BIND *mb)
{
  void *p = buf_realloc(&record->len_buffer, sizeof(int32_t) * rows);
  if (!p) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  mb->length = p;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_create_is_null_array(stmt_t *stmt, desc_record_t *record, int rows, TAOS_MULTI_BIND *mb)
{
  void *p = buf_realloc(&record->ind_buffer, sizeof(char) * rows);
  if (!p) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  mb->is_null = p;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_sql_c_double_to_tsdb_float(stmt_t *stmt, sql_c_to_tsdb_meta_t *meta)
{
  (void)stmt;

  double v = *(double*)meta->src_base;
  // TODO: check truncation
  *(float*)meta->dst_base = (float)v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_sql_c_sbigint_to_tsdb_bool(stmt_t *stmt, sql_c_to_tsdb_meta_t *meta)
{
  (void)stmt;

  int64_t v = *(int64_t*)meta->src_base;
  *(int8_t*)meta->dst_base = !!v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_sql_c_sbigint_to_tsdb_tinyint(stmt_t *stmt, sql_c_to_tsdb_meta_t *meta)
{
  int64_t v = *(int64_t*)meta->src_base;
  if (v > SCHAR_MAX || v < SCHAR_MIN) {
    stmt_append_err_format(stmt, "22003", 0, "Numeric value out of range:tinyint is required, but got ==[%" PRId64 "]==", v);
    return SQL_ERROR;
  }
  *(int8_t*)meta->dst_base = v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_sql_c_sbigint_to_tsdb_smallint(stmt_t *stmt, sql_c_to_tsdb_meta_t *meta)
{
  int64_t v = *(int64_t*)meta->src_base;
  if (v > SHRT_MAX || v < SHRT_MIN) {
    stmt_append_err_format(stmt, "22003", 0, "Numeric value out of range:smallint is required, but got ==[%" PRId64 "]==", v);
    return SQL_ERROR;
  }
  *(int16_t*)meta->dst_base = v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_sql_c_char_to_tsdb_timestamp(stmt_t *stmt, const char *s, int64_t *timestamp)
{
  char *end;
  errno = 0;
  long int v = strtol(s, &end, 0);
  int e = errno;
  if (e == ERANGE && (v == LONG_MAX || v == LONG_MIN)) {
    stmt_append_err_format(stmt, "22008", 0,
        "Datetime field overflow:timestamp is required, but got ==[%s]==", s);
    stmt_append_err_format(stmt, "HY000", e,
        "General error:[strtol]%s", strerror(e));
    return SQL_ERROR;
  }
  if (e != 0) {
    stmt_append_err_format(stmt, "22007", 0,
        "Invalid datetime format:timestamp is required, but got ==[%s]==", s);
    stmt_append_err_format(stmt, "HY000", e,
        "General error:[strtol]%s", strerror(e));
    return SQL_ERROR;
  }
  if (end == s) {
    stmt_append_err_format(stmt, "22007", 0,
        "Invalid datetime format:timestamp is required, but got ==[%s]==", s);
    stmt_append_err_format(stmt, "HY000", e,
        "General error:no digits at all");
    return SQL_ERROR;
  }
  if (end && *end) {
    stmt_append_err_format(stmt, "22007", 0,
        "Invalid datetime format:timestamp is required, but got ==[%s]==", s);
    stmt_append_err_format(stmt, "HY000", e,
        "General error:string following digits[%s]",
        end);
    return SQL_ERROR;
  }

  *timestamp = v;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_sql_c_char_to_tsdb_timestamp(stmt_t *stmt, sql_c_to_tsdb_meta_t *meta)
{
  SQLRETURN sr = SQL_SUCCESS;

  char   *src = meta->src_base;
  SQLLEN  len = meta->src_len;

  if (len == SQL_NTS) len = strlen(src);

  int64_t v = 0;

  char *p;
  const char *format = "%Y-%m-%d %H:%M:%S";
  struct tm t = {};
  time_t tt;
  p = tod_strptime(src, format, &t);
  tt = mktime(&t);
  v = tt;
  if (!p) {
    // TODO: precision
    sr = _stmt_sql_c_char_to_tsdb_timestamp(stmt, src, &v);
    if (sr == SQL_ERROR) return SQL_ERROR;
  } else if (*p) {
    if (*p != '.') {
      stmt_append_err_format(stmt, "22007", 0,
          "Invalid datetime format:timestamp is required, but got ==[%.*s]==", (int)len, src);
      return SQL_ERROR;
    }
    int n = len - (p-src);
    if (n == 4) {
      if (meta->field->precision != 0) {
        stmt_append_err_format(stmt, "22007", 0,
            "Invalid datetime format:`ms` timestamp is required, but got ==[%.*s]==", (int)len, src);
        return SQL_ERROR;
      }
      char *end = NULL;
      long int x = strtol(p+1, &end, 10);
      if (end && end-p!=4) {
        stmt_append_err_format(stmt, "22007", 0,
            "Invalid datetime format:`ms` timestamp is required, but got ==[%.*s]==", (int)len, src);
        return SQL_ERROR;
      }
      v *= 1000;
      v += x;
    } else if (n == 7) {
      if (meta->field->precision != 1) {
        stmt_append_err_format(stmt, "22007", 0,
            "Invalid datetime format:`us` timestamp is required, but got ==[%.*s]==", (int)len, src);
        return SQL_ERROR;
      }
      char *end = NULL;
      long int x = strtol(p+1, &end, 10);
      if (end && end-p!=7) {
        stmt_append_err_format(stmt, "22007", 0,
            "Invalid datetime format:`us` timestamp is required, but got ==[%.*s]==", (int)len, src);
        return SQL_ERROR;
      }
      v *= 1000000;
      v += x;
    } else if (n == 10) {
      if (meta->field->precision != 1) {
        stmt_append_err_format(stmt, "22007", 0,
            "Invalid datetime format:`ns` timestamp is required, but got ==[%.*s]==", (int)len, src);
        return SQL_ERROR;
      }
      char *end = NULL;
      long int x = strtol(p+1, &end, 10);
      if (end && end-p!=10) {
        stmt_append_err_format(stmt, "22007", 0,
            "Invalid datetime format:`ns` timestamp is required, but got ==[%.*s]==", (int)len, src);
        return SQL_ERROR;
      }
      v *= 1000000000;
      v += x;
    } else {
      stmt_append_err_format(stmt, "22007", 0,
          "Invalid datetime format:timestamp is required, but got ==[%.*s]==", (int)len, src);
      return SQL_ERROR;
    }
  } else {
    // TODO: precision
    stmt_append_err_format(stmt, "22007", 0,
        "Invalid datetime format:timestamp is required, but got ==[%.*s]==", (int)len, src);
    return SQL_ERROR;
  }

  *(int64_t*)meta->dst_base = v;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_sql_c_double_to_tsdb_timestamp(stmt_t *stmt, sql_c_to_tsdb_meta_t *meta)
{
  double v = *(double*)meta->src_base;
  if (v > INT64_MAX || v < INT64_MIN) {
    stmt_append_err_format(stmt, "22003", 0, "Numeric value out of range:bigint is required, but got ==[%lg]==", v);
    return SQL_ERROR;
  }
  *(int64_t*)meta->dst_base = v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_sql_c_sbigint_to_tsdb_int(stmt_t *stmt, sql_c_to_tsdb_meta_t *meta)
{
  int64_t v = *(int64_t*)meta->src_base;
  if (v > INT_MAX || v < INT_MIN) {
    stmt_append_err_format(stmt, "22003", 0, "Numeric value out of range:int is required, but got ==[%" PRId64 "]==", v);
    return SQL_ERROR;
  }
  *(int32_t*)meta->dst_base = v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_sql_c_sbigint_to_tsdb_varchar(stmt_t *stmt, sql_c_to_tsdb_meta_t *meta)
{
  int64_t v = *(int64_t*)meta->src_base;
  char buf[128];
  int n = snprintf(buf, sizeof(buf), "%" PRId64 "", v);
  if (n<0 || (size_t)n>=sizeof(buf)) {
    stmt_append_err(stmt, "HY000", 0, "General error:internal logic error");
    return SQL_ERROR;
  }

  if ((size_t)n > meta->IPD_record->DESC_LENGTH) {
    stmt_append_err_format(stmt, "22001", 0, "String data, right truncation:[%" PRId64 "] truncated to [%s]", v, buf);
    return SQL_ERROR;
  }

  memcpy(meta->dst_base, buf, n);
  *meta->dst_len = n;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_sql_c_double_to_tsdb_varchar(stmt_t *stmt, sql_c_to_tsdb_meta_t *meta)
{
  double v = *(double*)meta->src_base;
  char buf[128];
  int n = snprintf(buf, sizeof(buf), "%lg", v);
  if (n<0 || (size_t)n>=sizeof(buf)) {
    stmt_append_err(stmt, "HY000", 0, "General error:internal logic error");
    return SQL_ERROR;
  }

  if ((size_t)n > meta->IPD_record->DESC_LENGTH) {
    stmt_append_err_format(stmt, "22001", 0, "String data, right truncation:[%lg] truncated to [%s]", v, buf);
    return SQL_ERROR;
  }

  memcpy(meta->dst_base, buf, n);
  *meta->dst_len = n;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_and_prepare(stmt_t *stmt, SQLUSMALLINT ParameterNumber)
{
  SQLRETURN sr = SQL_SUCCESS;

  descriptor_t *APD = _stmt_APD(stmt);
  desc_record_t *APD_record = APD->records + ParameterNumber - 1;
  SQLSMALLINT ValueType = APD_record->DESC_CONCISE_TYPE;
  SQLLEN BufferLength = APD_record->DESC_OCTET_LENGTH;

  descriptor_t *IPD = _stmt_IPD(stmt);
  desc_record_t *IPD_record = IPD->records + ParameterNumber - 1;
  SQLSMALLINT ParameterType = IPD_record->DESC_CONCISE_TYPE;

  if (!APD_record->bound || !IPD_record->bound) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter not bound yet",
        ParameterNumber);
    return SQL_ERROR;
  }

  int tsdb_type;
  int tsdb_bytes;
  if (stmt->is_insert_stmt) {
    sr = _stmt_get_tsdb_type(stmt, ParameterNumber-1, &tsdb_type, &tsdb_bytes);
    if (sr == SQL_ERROR) return SQL_ERROR;
  } else {
    // non-insert-statement has to `guess`
    sr = _stmt_guess_taos_data_type(stmt, ParameterType, BufferLength, &tsdb_type, &tsdb_bytes);
    if (sr == SQL_ERROR) return SQL_ERROR;
  }
  IPD_record->taos_type  = tsdb_type;
  IPD_record->taos_bytes = tsdb_bytes;

  TAOS_MULTI_BIND *mb = stmt->mbs + ParameterNumber - 1;

  if (stmt->is_insert_stmt) {
    TAOS_FIELD_E field = {};
    sr = _stmt_get_tag_or_col_field(stmt, ParameterNumber-1, &field);
    if (sr == SQL_ERROR) return SQL_ERROR;

    switch (field.type) {
      case TSDB_DATA_TYPE_TIMESTAMP:
        if (ParameterType != SQL_TYPE_TIMESTAMP) {
          stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
              ParameterNumber, CALL_taos_data_type(field.type), sql_data_type(ParameterType));
          return SQL_ERROR;
        }
        if (IPD_record->DESC_TYPE==SQL_DATETIME && IPD_record->DESC_CONCISE_TYPE==SQL_TYPE_TIMESTAMP && IPD_record->DESC_PRECISION != (field.precision + 1) * 3) {
          stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter precision [%d] expected, but got [%d] ",
              ParameterNumber, (field.precision + 1) * 3, IPD_record->DESC_PRECISION);
          return SQL_ERROR;
        }
        mb->buffer_type             = field.type;
        mb->buffer                  = APD_record->DESC_DATA_PTR;
        mb->buffer_length           = sizeof(int64_t);
        mb->length                  = NULL;
        mb->is_null                 = NULL;
        if (ValueType == SQL_C_SBIGINT) break;
        if (ValueType == SQL_C_DOUBLE) {
          APD_record->convf               = _stmt_conv_sql_c_double_to_tsdb_timestamp;
          APD_record->create_buffer_array = _stmt_create_tsdb_timestamp_array;
          break;
        }
        if (ValueType == SQL_C_CHAR) {
          APD_record->convf               = _stmt_conv_sql_c_char_to_tsdb_timestamp;
          APD_record->create_buffer_array = _stmt_create_tsdb_timestamp_array;
          break;
        }
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
            ParameterNumber, CALL_taos_data_type(field.type), sql_c_data_type(ValueType));
        return SQL_ERROR;
        break;
      case TSDB_DATA_TYPE_VARCHAR:
        if (ParameterType != SQL_VARCHAR) {
          stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
              ParameterNumber, CALL_taos_data_type(field.type), sql_data_type(ParameterType));
          return SQL_ERROR;
        }
        mb->buffer_type             = field.type;
        mb->buffer                  = APD_record->DESC_DATA_PTR;
        mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        mb->length                  = NULL;
        mb->is_null                 = NULL;
        if (ValueType == SQL_C_CHAR) {
          APD_record->create_length_array = _stmt_create_length_array;
          break;
        }
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
            ParameterNumber, CALL_taos_data_type(field.type), sql_c_data_type(ValueType));
        return SQL_ERROR;
        break;
      case TSDB_DATA_TYPE_NCHAR:
        if (ParameterType != SQL_WVARCHAR) {
          stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
              ParameterNumber, CALL_taos_data_type(field.type), sql_data_type(ParameterType));
          return SQL_ERROR;
        }
        mb->buffer_type             = field.type;
        mb->buffer                  = APD_record->DESC_DATA_PTR;
        mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        mb->length                  = NULL;
        mb->is_null                 = NULL;
        if (ValueType == SQL_C_CHAR) {
          APD_record->create_length_array = _stmt_create_length_array;
          break;
        }
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
            ParameterNumber, CALL_taos_data_type(field.type), sql_c_data_type(ValueType));
        return SQL_ERROR;
        break;
      case TSDB_DATA_TYPE_INT:
        if (ParameterType != SQL_INTEGER) {
          stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
              ParameterNumber, CALL_taos_data_type(field.type), sql_data_type(ParameterType));
          return SQL_ERROR;
        }
        mb->buffer_type             = field.type;
        mb->buffer                  = APD_record->DESC_DATA_PTR;
        mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        mb->length                  = NULL;
        mb->is_null                 = NULL;
        if (ValueType == SQL_C_SLONG) break;
        if (ValueType == SQL_C_SBIGINT) {
          APD_record->convf               = _stmt_conv_sql_c_sbigint_to_tsdb_int;
          APD_record->create_buffer_array = _stmt_create_tsdb_int_array;
          break;
        }
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
            ParameterNumber, CALL_taos_data_type(field.type), sql_c_data_type(ValueType));
        return SQL_ERROR;
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        if (ParameterType != SQL_SMALLINT) {
          stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
              ParameterNumber, CALL_taos_data_type(field.type), sql_data_type(ParameterType));
          return SQL_ERROR;
        }
        mb->buffer_type             = field.type;
        mb->buffer                  = APD_record->DESC_DATA_PTR;
        mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        mb->length                  = NULL;
        mb->is_null                 = NULL;
        if (ValueType == SQL_C_SBIGINT) {
          APD_record->convf               = _stmt_conv_sql_c_sbigint_to_tsdb_smallint;
          APD_record->create_buffer_array = _stmt_create_tsdb_smallint_array;
          break;
        }
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
            ParameterNumber, CALL_taos_data_type(field.type), sql_c_data_type(ValueType));
        return SQL_ERROR;
        break;
      case TSDB_DATA_TYPE_TINYINT:
        if (ParameterType != SQL_TINYINT) {
          stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
              ParameterNumber, CALL_taos_data_type(field.type), sql_data_type(ParameterType));
          return SQL_ERROR;
        }
        mb->buffer_type             = field.type;
        mb->buffer                  = APD_record->DESC_DATA_PTR;
        mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        mb->length                  = NULL;
        mb->is_null                 = NULL;
        if (ValueType == SQL_C_SBIGINT) {
          APD_record->convf               = _stmt_conv_sql_c_sbigint_to_tsdb_tinyint;
          APD_record->create_buffer_array = _stmt_create_tsdb_tinyint_array;
          break;
        }
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
            ParameterNumber, CALL_taos_data_type(field.type), sql_c_data_type(ValueType));
        return SQL_ERROR;
        break;
      case TSDB_DATA_TYPE_BOOL:
        if (ParameterType != SQL_TINYINT) {
          stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
              ParameterNumber, CALL_taos_data_type(field.type), sql_data_type(ParameterType));
          return SQL_ERROR;
        }
        mb->buffer_type             = field.type;
        mb->buffer                  = APD_record->DESC_DATA_PTR;
        mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        mb->length                  = NULL;
        mb->is_null                 = NULL;
        if (ValueType == SQL_C_SBIGINT) {
          APD_record->convf               = _stmt_conv_sql_c_sbigint_to_tsdb_bool;
          APD_record->create_buffer_array = _stmt_create_tsdb_bool_array;
          break;
        }
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
            ParameterNumber, CALL_taos_data_type(field.type), sql_c_data_type(ValueType));
        return SQL_ERROR;
        break;
      case TSDB_DATA_TYPE_BIGINT:
        if (ParameterType != SQL_BIGINT) {
          stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
              ParameterNumber, CALL_taos_data_type(field.type), sql_data_type(ParameterType));
          return SQL_ERROR;
        }
        mb->buffer_type             = field.type;
        mb->buffer                  = APD_record->DESC_DATA_PTR;
        mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        mb->length                  = NULL;
        mb->is_null                 = NULL;
        if (ValueType == SQL_C_SBIGINT) break;
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
            ParameterNumber, CALL_taos_data_type(field.type), sql_c_data_type(ValueType));
        return SQL_ERROR;
        break;
      case TSDB_DATA_TYPE_FLOAT:
        if (ParameterType != SQL_REAL) {
          stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
              ParameterNumber, CALL_taos_data_type(field.type), sql_data_type(ParameterType));
          return SQL_ERROR;
        }
        mb->buffer_type             = field.type;
        mb->buffer                  = APD_record->DESC_DATA_PTR;
        mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        mb->length                  = NULL;
        mb->is_null                 = NULL;
        if (ValueType == SQL_C_FLOAT) break;
        if (ValueType == SQL_C_DOUBLE) {
          APD_record->convf               = _stmt_conv_sql_c_double_to_tsdb_float;
          APD_record->create_buffer_array = _stmt_create_tsdb_float_array;
          break;
        }
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
            ParameterNumber, CALL_taos_data_type(field.type), sql_c_data_type(ValueType));
        return SQL_ERROR;
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        if (ParameterType != SQL_DOUBLE) {
          stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
              ParameterNumber, CALL_taos_data_type(field.type), sql_data_type(ParameterType));
          return SQL_ERROR;
        }
        mb->buffer_type             = field.type;
        mb->buffer                  = APD_record->DESC_DATA_PTR;
        mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        mb->length                  = NULL;
        mb->is_null                 = NULL;
        if (ValueType == SQL_C_DOUBLE) break;
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
            ParameterNumber, CALL_taos_data_type(field.type), sql_c_data_type(ValueType));
        return SQL_ERROR;
        break;
      default:
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] not implemented yet", ParameterNumber, CALL_taos_data_type(field.type));
        return SQL_ERROR;
    }
  } else {
    switch (ParameterType) {
      case SQL_VARCHAR:
        mb->buffer_type             = TSDB_DATA_TYPE_VARCHAR;
        mb->buffer                  = APD_record->DESC_DATA_PTR;
        mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        mb->length                  = NULL;
        mb->is_null                 = NULL;
        APD_record->create_length_array = _stmt_create_length_array;
        if (ValueType == SQL_C_CHAR) {
          break;
        }
        if (ValueType == SQL_C_SBIGINT) {
          mb->buffer = NULL;
          mb->buffer_length = IPD_record->DESC_LENGTH;
          APD_record->convf               = _stmt_conv_sql_c_sbigint_to_tsdb_varchar;
          APD_record->create_buffer_array = _stmt_create_tsdb_varchar_array;
          break;
        }
        if (ValueType == SQL_C_DOUBLE) {
          mb->buffer = NULL;
          mb->buffer_length = IPD_record->DESC_LENGTH;
          APD_record->convf               = _stmt_conv_sql_c_double_to_tsdb_varchar;
          APD_record->create_buffer_array = _stmt_create_tsdb_varchar_array;
          break;
        }
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
            ParameterNumber, sql_data_type(ParameterType), sql_c_data_type(ValueType));
        return SQL_ERROR;
      case SQL_INTEGER:
        mb->buffer_type             = TSDB_DATA_TYPE_INT;
        mb->buffer                  = APD_record->DESC_DATA_PTR;
        mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        mb->length                  = NULL;
        mb->is_null                 = NULL;
        if (ValueType == SQL_C_SLONG) break;
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
            ParameterNumber, sql_data_type(ParameterType), sql_c_data_type(ValueType));
        return SQL_ERROR;
      case SQL_BIGINT:
        mb->buffer_type             = TSDB_DATA_TYPE_BIGINT;
        mb->buffer                  = APD_record->DESC_DATA_PTR;
        mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        mb->length                  = NULL;
        mb->is_null                 = NULL;
        if (ValueType == SQL_C_SBIGINT) break;
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
            ParameterNumber, sql_data_type(ParameterType), sql_c_data_type(ValueType));
        return SQL_ERROR;
      default:
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] not implemented yet", ParameterNumber, sql_data_type(ParameterType));
        return SQL_ERROR;
    }
  }

  return SQL_SUCCESS;
}

SQLRETURN stmt_prepare(stmt_t *stmt,
    SQLCHAR      *StatementText,
    SQLINTEGER    TextLength)
{
  // OA_NIY(stmt->stmt == NULL);

  rowset_reset(&stmt->rowset);
  _stmt_release_result(stmt);

  _stmt_unprepare(stmt);

  const char *sql = (const char*)StatementText;

  size_t len = TextLength;
  if (TextLength == SQL_NTS) len = strlen(sql);

  const char *sqlx;
  size_t n;
  char *s, *pre;

  if (stmt->conn->cfg.cache_sql) {
    s = strndup(sql, len);
    if (!s) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
    pre = stmt->sql;
    stmt->sql = s;

    sqlx = s;
    n = strlen(s);
  } else {
    sqlx = sql;
    n = len;
  }

  SQLRETURN sr = _stmt_prepare(stmt, sqlx, n);

  if (stmt->conn->cfg.cache_sql) {
    if (sql_succeeded(sr)) {
      TOD_SAFE_FREE(pre);
    } else {
      stmt->sql = pre;
      free(s);
    }
  }

  return sr;
}

SQLRETURN stmt_bind_param(
    stmt_t         *stmt,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ValueType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN         *StrLen_or_IndPtr)
{
  if (ParameterNumber == 0) {
    stmt_append_err(stmt, "HY000", 0, "General error:bookmark column not supported yet");
    return SQL_ERROR;
  }

  descriptor_t *APD = _stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;

  if (0 && ParameterNumber > APD_header->DESC_COUNT+1) {
    stmt_append_err(stmt, "HY000", 0, "General error:not implemented yet");
    return SQL_ERROR;
  }

  switch (InputOutputType) {
    case SQL_PARAM_INPUT:
      break;
    case SQL_PARAM_INPUT_OUTPUT:
      break;
    case SQL_PARAM_OUTPUT:
      stmt_append_err(stmt, "HY000", 0, "SQL_PARAM_OUTPUT not supported yet by taos");
      return SQL_ERROR;
    case SQL_PARAM_INPUT_OUTPUT_STREAM:
      stmt_append_err(stmt, "HY000", 0, "SQL_PARAM_INPUT_OUTPUT_STREAM not supported yet by taos");
      return SQL_ERROR;
    case SQL_PARAM_OUTPUT_STREAM:
      stmt_append_err(stmt, "HY000", 0, "SQL_PARAM_OUTPUT_STREAM not supported yet by taos");
      return SQL_ERROR;
    default:
      stmt_append_err(stmt, "HY000", 0, "unknown InputOutputType for `SQLBindParameter`");
      return SQL_ERROR;
  }

  if (ParameterNumber > APD->cap) {
    size_t cap = (ParameterNumber + 15) / 16 * 16;
    desc_record_t *records = (desc_record_t*)realloc(APD->records, sizeof(*records) * cap);
    if (!records) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
    for (size_t i = APD->cap; i<cap; ++i) {
      desc_record_t *record = records + i;
      memset(record, 0, sizeof(*record));
    }
    APD->records = records;
    APD->cap     = cap;
  }

  desc_record_t *APD_record = APD->records + ParameterNumber - 1;
  APD_record->bound = 0;

  descriptor_t *IPD = _stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  if (ParameterNumber >= IPD->cap) {
    size_t cap = (ParameterNumber + 15) / 16 * 16;
    desc_record_t *records = (desc_record_t*)realloc(IPD->records, sizeof(*records) * cap);
    if (!records) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
    for (size_t i = IPD->cap; i<cap; ++i) {
      desc_record_t *record = records + i;
      memset(record, 0, sizeof(*record));
    }
    IPD->records = records;
    IPD->cap     = cap;
  }

  desc_record_t *IPD_record = IPD->records + ParameterNumber - 1;
  memset(IPD_record, 0, sizeof(*IPD_record));

  APD_record->DESC_TYPE                = ValueType;
  APD_record->DESC_CONCISE_TYPE        = ValueType;
  APD_record->DESC_OCTET_LENGTH        = BufferLength;
  APD_record->DESC_DATA_PTR            = ParameterValuePtr;
  APD_record->DESC_INDICATOR_PTR       = StrLen_or_IndPtr;
  APD_record->DESC_OCTET_LENGTH_PTR    = StrLen_or_IndPtr;

  switch (ValueType) {
    case SQL_C_UTINYINT:
      APD_record->DESC_OCTET_LENGTH         = 1;
      break;
    case SQL_C_SHORT:
      APD_record->DESC_OCTET_LENGTH         = 2;
      break;
    case SQL_C_SLONG:
      APD_record->DESC_OCTET_LENGTH         = 4;
      break;
    case SQL_C_SBIGINT:
      APD_record->DESC_OCTET_LENGTH         = 8;
      break;
    case SQL_C_DOUBLE:
      APD_record->DESC_OCTET_LENGTH         = 8;
      break;
    case SQL_C_CHAR:
      break;
    case SQL_C_WCHAR:
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:#%d Parameter converstion from `%s[0x%x/%d]` not implemented yet",
          ParameterNumber, sql_c_data_type(ValueType), ValueType, ValueType);
      return SQL_ERROR;
  }

  IPD_record->DESC_PARAMETER_TYPE      = InputOutputType;
  IPD_record->DESC_TYPE                = ParameterType;
  IPD_record->DESC_CONCISE_TYPE        = ParameterType;

  switch (ParameterType) {
    case SQL_TYPE_TIMESTAMP:
      if ( (!(ColumnSize == 16 && DecimalDigits == 0)) &&
           (!(ColumnSize == 19 && DecimalDigits == 0)) &&
           (!((DecimalDigits == 3 || DecimalDigits == 6 || DecimalDigits == 9) && (ColumnSize == 20 + (size_t)DecimalDigits))) )
      {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:#%d Parameter[SQL_TYPE_TIMESTAMP(%ld.%d)] not implemented yet",
            ParameterNumber, ColumnSize, DecimalDigits);
        return SQL_ERROR;
      }
      IPD_record->DESC_TYPE                     = SQL_DATETIME;
      IPD_record->DESC_LENGTH                   = ColumnSize;
      IPD_record->DESC_PRECISION                = DecimalDigits;
      break;
    case SQL_REAL:
      IPD_record->DESC_LENGTH                   = 7;
      IPD_record->DESC_PRECISION                = DecimalDigits; // FIXME:
      break;
    case SQL_DOUBLE:
      IPD_record->DESC_LENGTH                   = 15;
      IPD_record->DESC_PRECISION                = DecimalDigits; // FIXME:
      break;
    case SQL_INTEGER:
      IPD_record->DESC_LENGTH                   = 10;
      break;
    case SQL_TINYINT:
      IPD_record->DESC_LENGTH                   = 3;
      break;
    case SQL_SMALLINT:
      IPD_record->DESC_LENGTH                   = 5;
      break;
    case SQL_BIGINT:
      IPD_record->DESC_LENGTH                   = 19; // FIXME: 19 (if signed) or 20 (if unsigned)
      break;
    case SQL_VARCHAR:
      IPD_record->DESC_LENGTH                   = ColumnSize;
      break;
    case SQL_WVARCHAR:
      IPD_record->DESC_LENGTH                   = ColumnSize; // FIXME:
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:#%d Parameter converstion from `%s[0x%x/%d]` not implemented yet",
          ParameterNumber, sql_data_type(ParameterType), ParameterType, ParameterType);
      return SQL_ERROR;
  }

  if (ParameterNumber > APD_header->DESC_COUNT) APD_header->DESC_COUNT = ParameterNumber;
  if (ParameterNumber > IPD_header->DESC_COUNT) IPD_header->DESC_COUNT = ParameterNumber;

  APD_record->bound = 1;
  IPD_record->bound = 1;

  return SQL_SUCCESS;
}

static void _stmt_param_get(stmt_t *stmt, int irow, int i_param, char **base, SQLLEN *length, int *is_null)
{
  descriptor_t *APD = _stmt_APD(stmt);
  desc_record_t *APD_record = APD->records + i_param;

  char *buffer = (char*)APD_record->DESC_DATA_PTR;
  SQLLEN *len_arr = APD_record->DESC_OCTET_LENGTH_PTR;
  SQLLEN *ind_arr = APD_record->DESC_INDICATOR_PTR;

  if (ind_arr && ind_arr[irow] == SQL_NULL_DATA) {
    *base    = NULL;
    *length  = 0;
    *is_null = 1;
    return;
  }

  *base    = buffer ? buffer + APD_record->DESC_OCTET_LENGTH * irow : NULL;
  *length  = len_arr ? len_arr[irow] : 0;
  *is_null = 0;
}

static void _stmt_param_get_dst(stmt_t *stmt, int irow, int i_param, char **base, int32_t **length, char **is_null)
{
  TAOS_MULTI_BIND *mb = stmt->mbs + i_param;

  char *buffer        = (char*)mb->buffer;
  char *is_null_arr   = mb->is_null;
  int32_t *length_arr = mb->length;

  *base    = buffer ? buffer + mb->buffer_length * irow : NULL;
  *length  = length_arr ? length_arr + irow : NULL;
  *is_null = is_null_arr ? is_null_arr + irow : NULL;
}

static SQLRETURN _stmt_param_prepare_subtbl(stmt_t *stmt)
{
  int r = 0;

  descriptor_t *APD = _stmt_APD(stmt);
  desc_record_t *APD_record = APD->records + 0;

  if (APD_record->DESC_TYPE != SQL_C_CHAR) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:the first parameter for subtbl-insert-statement must be `SQL_C_CHAR`, but got ==%s==", 
        sql_c_data_type(APD_record->DESC_TYPE));
    return SQL_ERROR;
  }
  char *base;
  SQLLEN length;
  int is_null;
  _stmt_param_get(stmt, 0, 0, &base, &length, &is_null);

  if (is_null) {
    stmt_append_err(stmt, "HY000", 0, "General error:the first parameter for subtbl-insert-statement must not be null");
    return SQL_ERROR;
  }
  TOD_SAFE_FREE(stmt->subtbl);
  stmt->subtbl = strndup(base, length);
  if (!stmt->subtbl) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  r = CALL_taos_stmt_set_tbname(stmt->stmt, stmt->subtbl);
  if (r) {
    stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
    return SQL_ERROR;
  }
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_check_subtbl_or_tag(stmt_t *stmt, int irow, int i_param)
{
  char *base0;
  SQLLEN length0;
  int is_null0;
  _stmt_param_get(stmt, 0, i_param, &base0, &length0, &is_null0);

  char *base;
  SQLLEN length;
  int is_null;
  _stmt_param_get(stmt, irow, i_param, &base, &length, &is_null);

  if (is_null0 != is_null) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:null-flag differs at (%d,%d)", irow+1, i_param+1);
    return SQL_ERROR;
  }

  descriptor_t *APD = _stmt_APD(stmt);
  desc_record_t *APD_record = APD->records + i_param;

  switch (APD_record->DESC_TYPE) {
    case SQL_C_CHAR:
      if (length0 == SQL_NTS) length0 = strlen(base0);
      if (length == SQL_NTS) length = strlen(base);
      if (length != length0) {
        stmt_append_err_format(stmt, "HY000", 0, "General error:param at (%d,%d) differs, ==%.*s <> %.*s==", irow+1, i_param+1, (int)length, base, (int)length0, base0);
        return SQL_ERROR;
      }
      if (strncmp(base, base0, length)) {
        stmt_append_err_format(stmt, "HY000", 0, "General error:param at (%d,%d) differs, ==%.*s <> %.*s==", irow+1, i_param+1, (int)length, base, (int)length0, base0);
        return SQL_ERROR;
      }
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0, "General error:#%d param [%s] not implemented yet", i_param+1, sql_c_data_type(APD_record->DESC_TYPE));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_process(stmt_t *stmt, int irow, int i_param)
{
  SQLRETURN sr = SQL_SUCCESS;

  descriptor_t *APD = _stmt_APD(stmt);
  descriptor_t *IPD = _stmt_IPD(stmt);

  TAOS_MULTI_BIND *mb = stmt->mbs + i_param;

  desc_record_t *APD_record = APD->records + i_param;
  desc_record_t *IPD_record = IPD->records + i_param;

  char   *src_base;
  SQLLEN  src_len;
  int     src_is_null;
  _stmt_param_get(stmt, irow, i_param, &src_base, &src_len, &src_is_null);

  if (APD_record->DESC_TYPE==SQL_C_CHAR && src_len == SQL_NTS) src_len = strlen(src_base);

  if (irow>0 && i_param < (!!stmt->subtbl_required) + stmt->nr_tag_fields) {
    return _stmt_param_check_subtbl_or_tag(stmt, irow, i_param);
  }

  char     *dst_base;
  int32_t  *dst_len;
  char     *dst_is_null;
  _stmt_param_get_dst(stmt, irow, i_param, &dst_base, &dst_len, &dst_is_null);

  if (src_is_null) {
    *dst_is_null = 1;
    return SQL_SUCCESS;
  }
  *dst_is_null = 0;

  if (APD_record->convf) {
    sql_c_to_tsdb_meta_t meta = {
      .src_base                 = src_base,
      .src_len                  = src_len,
      .IPD_record               = IPD_record,
      .field                    = NULL,
      .dst_base                 = dst_base,
      .dst_len                  = dst_len
    };
    TAOS_FIELD_E field = {};
    if (stmt->is_insert_stmt) {
      sr = _stmt_get_tag_or_col_field(stmt, i_param, &field);
      if (sr == SQL_ERROR) return SQL_ERROR;
      meta.field = &field;
    }
    sr = APD_record->convf(stmt, &meta);
    if (sr == SQL_ERROR) return SQL_ERROR;
    return SQL_SUCCESS;
  }
  if (!dst_len) return SQL_SUCCESS;

  if (APD_record->DESC_TYPE!=SQL_C_CHAR) return SQL_SUCCESS;

  if (stmt->is_insert_stmt) {
    TAOS_FIELD_E field = {};
    sr = _stmt_get_tag_or_col_field(stmt, i_param, &field);
    if (sr == SQL_ERROR) return SQL_ERROR;

    if (mb->buffer_type == TSDB_DATA_TYPE_VARCHAR) {
      if (src_len > field.bytes - 2) {
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d param [%.*s] too long [%d]", i_param+1, (int)src_len, src_base, field.bytes - 2);
        return SQL_ERROR;
      }
    } else if (mb->buffer_type == TSDB_DATA_TYPE_NCHAR) {
      size_t nr_bytes = 0;
      sr = _stmt_calc_bytes(stmt, "UTF8", src_base, src_len, "UCS-4BE", &nr_bytes);
      if (sr == SQL_ERROR) return SQL_ERROR;
      if (nr_bytes > (size_t)field.bytes - 2) {
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d param [%.*s] too long [%d]", i_param+1, (int)src_len, src_base, (field.bytes-2)/4);
        return SQL_ERROR;
      }
    } else {
      stmt_append_err_format(stmt, "HY000", 0, "General error:#%d param, not implemented yet", i_param+1);
      return SQL_ERROR;
    }
  }

  *dst_len = src_len;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_prepare_mb(stmt_t *stmt, int i_param)
{
  SQLRETURN sr = SQL_SUCCESS;

  descriptor_t *APD = _stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;
  TAOS_MULTI_BIND *mb = stmt->mbs + i_param;

  desc_record_t *APD_record = APD->records + i_param;

  if (i_param < (!!stmt->subtbl_required) + stmt->nr_tag_fields) {
    mb->num = 1;
  } else {
    mb->num = APD_header->DESC_ARRAY_SIZE;
  }

  if (APD_record->create_buffer_array) {
    sr = APD_record->create_buffer_array(stmt, APD_record, mb->num, mb);
    if (sr == SQL_ERROR) return SQL_ERROR;
  }
  if (APD_record->create_length_array) {
    sr = APD_record->create_length_array(stmt, APD_record, mb->num, mb);
    if (sr == SQL_ERROR) return SQL_ERROR;
  }
  sr = _stmt_create_is_null_array(stmt, APD_record, mb->num, mb);
  if (sr == SQL_ERROR) return SQL_ERROR;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_pre_exec_prepare_params(stmt_t *stmt)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  descriptor_t *APD = _stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;
  descriptor_t *IPD = _stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  if (APD_header->DESC_COUNT != IPD_header->DESC_COUNT) {
    stmt_append_err(stmt, "HY000", 0, "General error:internal logic error, DESC_COUNT of APD/IPD differs");
    return SQL_ERROR;
  }

  if (APD_header->DESC_ARRAY_SIZE <= 0) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:internal logic error, DESC_ARRAY_SIZE[%ld] invalid", APD_header->DESC_ARRAY_SIZE);
    return SQL_ERROR;
  }

  if (APD_header->DESC_ARRAY_SIZE > 1) {
    if (!stmt->is_insert_stmt) {
      stmt_append_err(stmt, "HY000", 0, "General error:taosc currently does not support batch execution for non-insert-statement");
      return SQL_ERROR;
    }
  }

  if (IPD_header->DESC_ROWS_PROCESSED_PTR) *IPD_header->DESC_ROWS_PROCESSED_PTR = 0;

  if (stmt->is_insert_stmt && stmt->subtbl_required) {
    sr = _stmt_param_prepare_subtbl(stmt);
    if (sr == SQL_ERROR) return sr;
  }

  for (int i_param = 0; i_param < IPD_header->DESC_COUNT; ++i_param) {
    sr = _stmt_param_prepare_mb(stmt, i_param);
    if (sr == SQL_ERROR) return SQL_ERROR;
  }

  for (size_t irow = 0; irow < APD_header->DESC_ARRAY_SIZE; ++irow) {
    if (IPD_header->DESC_ARRAY_STATUS_PTR) IPD_header->DESC_ARRAY_STATUS_PTR[irow] = SQL_PARAM_ERROR;
    for (int i_param = 0; i_param < IPD_header->DESC_COUNT; ++i_param) {
      sr = _stmt_param_process(stmt, irow, i_param);
      if (sr == SQL_ERROR) return SQL_ERROR;
    }
    if (IPD_header->DESC_ARRAY_STATUS_PTR) IPD_header->DESC_ARRAY_STATUS_PTR[irow] = SQL_PARAM_SUCCESS;
    if (IPD_header->DESC_ROWS_PROCESSED_PTR) *IPD_header->DESC_ROWS_PROCESSED_PTR += 1;
  }

  if (stmt->is_insert_stmt && stmt->nr_tag_fields) {
    TAOS_MULTI_BIND *mbs = stmt->mbs + (!!stmt->subtbl_required);
    r = CALL_taos_stmt_set_tags(stmt->stmt, mbs);
    if (r) {
      stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
      return SQL_ERROR;
    }
  }

  r = CALL_taos_stmt_bind_param_batch(stmt->stmt, stmt->mbs + (!!stmt->subtbl_required) + stmt->nr_tag_fields);
  if (r) {
    stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
    return SQL_ERROR;
  }

  r = CALL_taos_stmt_add_batch(stmt->stmt);
  if (r) {
    stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_pre_exec(stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;
  sr = _stmt_pre_exec_prepare_params(stmt);
  if (!sql_succeeded(sr)) return SQL_ERROR;

  return SQL_SUCCESS;
}

SQLRETURN _stmt_execute(stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  descriptor_t *APD = _stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;

  SQLSMALLINT n = _stmt_get_count_of_tsdb_params(stmt);
  if (APD_header->DESC_COUNT < n) {
    stmt_append_err_format(stmt, "07002", 0,
        "COUNT field incorrect:%d parameter markers required, but only %d parameters bound",
        n, APD_header->DESC_COUNT);
    return SQL_ERROR;
  }

  for (int i=0; i<n; ++i) {
    if (i >= APD_header->DESC_COUNT) break;
    SQLUSMALLINT ParameterNumber = i + 1;
    sr = _stmt_param_check_and_prepare(stmt, ParameterNumber);
    if (sr == SQL_ERROR) return SQL_ERROR;
  }

  if (_stmt_get_rows_fetched_ptr(stmt)) *_stmt_get_rows_fetched_ptr(stmt) = 0;
  rowset_reset(&stmt->rowset);
  // column-binds remain valid among executes
  _stmt_release_result(stmt);

  sr = _stmt_pre_exec(stmt);
  if (!sql_succeeded(sr)) return sr;

  int r = 0;
  r = CALL_taos_stmt_execute(stmt->stmt);
  if (r) {
    stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
    return SQL_ERROR;
  }
  stmt->res = CALL_taos_stmt_use_result(stmt->stmt);
  stmt->res_is_from_taos_query = 0;

  return _stmt_post_exec(stmt);
}

SQLRETURN stmt_execute(stmt_t *stmt)
{
  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->taos);
  OA_ILE(stmt->stmt);

  SQLRETURN sr = _stmt_execute(stmt);

  return sr;
}

static void _stmt_unbind_cols(stmt_t *stmt)
{
  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;
  ARD_header->DESC_COUNT = 0;
  memset(ARD->records, 0, sizeof(*ARD->records) * ARD->cap);
}

static void _stmt_reset_params(stmt_t *stmt)
{
  descriptor_t *APD = _stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;
  APD_header->DESC_COUNT = 0;

  descriptor_t *IPD = _stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  IPD_header->DESC_COUNT = 0;
}

static SQLRETURN _stmt_set_cursor_type(stmt_t *stmt, SQLULEN cursor_type)
{
  switch (cursor_type) {
    case SQL_CURSOR_FORWARD_ONLY:
      return SQL_SUCCESS;
    case SQL_CURSOR_STATIC:
      stmt_append_err_format(stmt, "01000", 0,
          "General warning:`SQL_CURSOR_STATIC` not fully implemented yet, so `SQL_CURSOR_FORWARD_ONLY` is used as instead");
      return SQL_SUCCESS_WITH_INFO;
    default:
      stmt_append_err_format(stmt, "HY000", 0, "General error:`%s` for `SQL_ATTR_CURSOR_TYPE` not supported yet", sql_cursor_type(cursor_type));
      return SQL_ERROR;
  }
}

SQLRETURN stmt_set_attr(stmt_t *stmt, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength)
{
  (void)StringLength;

  switch (Attribute) {
    case SQL_ATTR_CURSOR_TYPE:
      return _stmt_set_cursor_type(stmt, (SQLULEN)ValuePtr);
    case SQL_ATTR_ROW_ARRAY_SIZE:
      return _stmt_set_row_array_size(stmt, (SQLULEN)ValuePtr);
    case SQL_ATTR_ROW_STATUS_PTR:
      return _stmt_set_row_status_ptr(stmt, (SQLUSMALLINT*)ValuePtr);
    case SQL_ATTR_ROW_BIND_TYPE:
      return _stmt_set_row_bind_type(stmt, (SQLULEN)ValuePtr);
    case SQL_ATTR_ROWS_FETCHED_PTR:
      return _stmt_set_rows_fetched_ptr(stmt, (SQLULEN*)ValuePtr);
    case SQL_ATTR_MAX_LENGTH:
      return _stmt_set_max_length(stmt, (SQLULEN)ValuePtr);
    case SQL_ATTR_ROW_BIND_OFFSET_PTR:
      return _stmt_set_row_bind_offset_ptr(stmt, (SQLULEN*)ValuePtr);
    case SQL_ATTR_PARAM_BIND_TYPE:
      return _stmt_set_param_bind_type(stmt, (SQLULEN)ValuePtr);
    case SQL_ATTR_PARAMSET_SIZE:
      return _stmt_set_paramset_size(stmt, (SQLULEN)ValuePtr);
    case SQL_ATTR_PARAM_STATUS_PTR:
      return _stmt_set_param_status_ptr(stmt, (SQLUSMALLINT*)ValuePtr);
    case SQL_ATTR_PARAMS_PROCESSED_PTR:
      return _stmt_set_params_processed_ptr(stmt, (SQLULEN*)ValuePtr);
    case SQL_ATTR_APP_ROW_DESC:
      return _stmt_set_row_desc(stmt, ValuePtr);
    case SQL_ATTR_APP_PARAM_DESC:
      return _stmt_set_param_desc(stmt, ValuePtr);
    default:
      stmt_append_err_format(stmt, "HY000", 0, "General error:`%s[0x%x/%d]` not supported yet", sql_stmt_attr(Attribute), Attribute, Attribute);
      return SQL_ERROR;
  }
}

SQLRETURN stmt_free_stmt(stmt_t *stmt, SQLUSMALLINT Option)
{
  switch (Option) {
    case SQL_CLOSE:
      _stmt_close_cursor(stmt);
      return SQL_SUCCESS;
    case SQL_UNBIND:
      _stmt_unbind_cols(stmt);
      return SQL_SUCCESS;
    case SQL_RESET_PARAMS:
      _stmt_reset_params(stmt);
      return SQL_SUCCESS;
    default:
      stmt_append_err_format(stmt, "HY000", 0, "General error:`%s[0x%x/%d]` not supported yet", sql_free_statement_option(Option), Option, Option);
      return SQL_ERROR;
  }
}

void stmt_clr_errs(stmt_t *stmt)
{
  errs_clr(&stmt->errs);
}

static SQLRETURN _wild_post_filter(stmt_t *stmt, int row, void *ctx, int *filter)
{
  SQLRETURN sr = SQL_SUCCESS;

  *filter = 0;

  const char *base;
  int len;
  sr = _stmt_get_data_len(stmt, row, 2, &base, &len);
  if (sr == SQL_ERROR) return SQL_ERROR;
  if (base == NULL) return SQL_SUCCESS;

  wildex_t *wild = (wildex_t*)ctx;
  *filter = wildexec_n(wild, base, len);

  return SQL_SUCCESS;
}

static void _wild_post_filter_destroy(stmt_t *stmt, void *ctx)
{
  (void)stmt;
  wildex_t *wild = (wildex_t*)ctx;
  if (wild) {
    wildfree(wild);
  }
}

SQLRETURN stmt_tables(stmt_t *stmt,
    SQLCHAR       *CatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *SchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *TableName,
    SQLSMALLINT    NameLength3,
    SQLCHAR       *TableType,
    SQLSMALLINT    NameLength4)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  const char *catalog = (const char *)CatalogName;
  const char *schema  = (const char *)SchemaName;
  const char *table   = (const char *)TableName;
  const char *type    = (const char *)TableType;

  if (catalog == NULL) catalog = "";
  if (schema  == NULL) schema  = "";
  if (table   == NULL) table   = "";
  if (type    == NULL) type    = "";

  if (NameLength1 == SQL_NTS) NameLength1 = strlen(catalog);
  if (NameLength2 == SQL_NTS) NameLength2 = strlen(schema);
  if (NameLength3 == SQL_NTS) NameLength3 = strlen(table);
  if (NameLength4 == SQL_NTS) NameLength4 = strlen(type);

  if (schema[NameLength2]) {
    stmt_append_err(stmt, "HY000", 0, "General error: non-null-terminated-string for SchemaName, not supported yet");
    return SQL_ERROR;
  }

  const char *sql = NULL;

  if (strcmp(catalog, SQL_ALL_CATALOGS) == 0 && !*schema && !*table) {
    sql =
      "select name as TABLE_CAT, '' as TABLE_SCHEM, '' as TABLE_NAME,"
      " '' as TABLE_TYPE, '' as REMARKS"
      " from information_schema.ins_databases"
      " where name like ?"
      " order by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME";
    sr = stmt_prepare(stmt, (SQLCHAR*)sql, (SQLINTEGER)strlen(sql));
    if (sr == SQL_ERROR) return SQL_ERROR;

    SQLSMALLINT  InputOutputType       = SQL_PARAM_INPUT;
    SQLSMALLINT  ValueType             = SQL_C_CHAR;
    SQLSMALLINT  ParameterType         = SQL_VARCHAR;
    SQLULEN      ColumnSize            = 1024; // FIXME: hard-coded
    SQLSMALLINT  DecimalDigits         = 0;
    SQLPOINTER   ParameterValuePtr     = (SQLPOINTER)catalog;
    SQLLEN       BufferLength          = strlen(catalog) + 1;
    SQLLEN       StrLen_or_Ind         = SQL_NTS;
    sr = stmt_bind_param(stmt, 1, InputOutputType, ValueType, ParameterType, ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength, &StrLen_or_Ind);
    if (sr == SQL_ERROR) return SQL_ERROR;

    return stmt_execute(stmt);
  } else if (strcmp(schema, SQL_ALL_SCHEMAS) == 0 && !*catalog && !*table) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:`schema:[%.*s]` not supported yet", (int)NameLength2, schema);
    return SQL_ERROR;
  } else if (strcmp(type, SQL_ALL_TABLE_TYPES) == 0 && !*catalog && !*schema && !*table) {
    sql =
      "select '' as TABLE_CAT, '' as TABLE_SCHEM, '' as TABLE_NAME,"
      " 'TABLE' as TABLE_TYPE, '' as REMARKS"
      " union"
      " select '' as TABLE_CAT, '' as TABLE_SCHEM, '' as TABLE_NAME,"
      " 'STABLE' as TABLE_TYPE, '' as REMARKS"
      " order by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME";
    sr = stmt_exec_direct(stmt, sql, strlen(sql));
    if (sr == SQL_ERROR) return SQL_ERROR;
    if (sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO) {
      stmt_append_err(stmt, "HY000", 0, "General warning:`SQLTables` not fully implemented yet");
      return SQL_SUCCESS_WITH_INFO;
    }
    return sr;
  } else if (*schema) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:`schema:[%.*s]` not supported yet", (int)NameLength2, schema);
    return SQL_ERROR;
  } else {
    if (0) {
      // NOTE: taosc seems fail to execute-prepared-statement as follows?
      // https://github.com/taosdata/TDengine/issues/17870
      // https://github.com/taosdata/TDengine/issues/17871
      sql =
        "select db_name as TABLE_CAT, '' as TABLE_SCHEM, table_name as TABLE_NAME,"
        " 'TABLE' as TABLE_TYPE, table_comment as REMARKS"
        " from information_schema.ins_tables where TABLE_TYPE like ?"
        " union"
        " select db_name as TABLE_CAT, '' as TABLE_SCHEM, stable_name as TABLE_NAME,"
        " 'STABLE' as TABLE_TYPE, table_comment as REMARKS"
        " from information_schema.ins_stables where TABLE_TYPE like ?"
        " order by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME";
      sr = stmt_prepare(stmt, (SQLCHAR*)sql, (SQLINTEGER)strlen(sql));
      if (sr == SQL_ERROR) return SQL_ERROR;

      SQLSMALLINT  InputOutputType       = SQL_PARAM_INPUT;
      SQLSMALLINT  ValueType             = SQL_C_CHAR;
      SQLSMALLINT  ParameterType         = SQL_VARCHAR;
      SQLULEN      ColumnSize            = 1024; // FIXME: hard-coded
      SQLSMALLINT  DecimalDigits         = 0;
      SQLPOINTER   ParameterValuePtr     = (SQLPOINTER)type;
      SQLLEN       BufferLength          = strlen(type) + 1;
      SQLLEN       StrLen_or_Ind         = SQL_NTS;
      sr = stmt_bind_param(stmt, 1, InputOutputType, ValueType, ParameterType, ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength, &StrLen_or_Ind);
      if (sr == SQL_ERROR) return SQL_ERROR;

      sr = stmt_bind_param(stmt, 2, InputOutputType, ValueType, ParameterType, ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength, &StrLen_or_Ind);
      if (sr == SQL_ERROR) return SQL_ERROR;

      return stmt_execute(stmt);
    }

    if (0) {
      // https://github.com/taosdata/TDengine/issues/17872
      sql =
        "select * from ("
        " select db_name as TABLE_CAT, '' as TABLE_SCHEM, stable_name as TABLE_NAME,"
        " 'STABLE' as TABLE_TYPE, table_comment as REMARKS"
        " from information_schema.ins_stables"
        " union"
        " select db_name as TABLE_CAT, '' as TABLE_SCHEM, table_name as TABLE_NAME,"
        " 'TABLE' as TABLE_TYPE, table_comment as REMARKS"
        " from information_schema.ins_tables"
        ") where TABLE_TYPE in (?, ?)";
      sr = stmt_prepare(stmt, (SQLCHAR*)sql, (SQLINTEGER)strlen(sql));
      if (sr == SQL_ERROR) return SQL_ERROR;

      SQLSMALLINT  InputOutputType       = SQL_PARAM_INPUT;
      SQLSMALLINT  ValueType             = SQL_C_CHAR;
      SQLSMALLINT  ParameterType         = SQL_VARCHAR;
      SQLULEN      ColumnSize            = 1024; // FIXME: hard-coded
      SQLSMALLINT  DecimalDigits         = 0;
      SQLPOINTER   ParameterValuePtr     = (SQLPOINTER)"TABLE";
      SQLLEN       BufferLength          = strlen("TABLE") + 1;
      SQLLEN       StrLen_or_Ind         = SQL_NTS;

      sr = stmt_bind_param(stmt, 1, InputOutputType, ValueType, ParameterType, ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength, &StrLen_or_Ind);
      if (sr == SQL_ERROR) return SQL_ERROR;

      ParameterValuePtr     = (SQLPOINTER)"STABLE";
      BufferLength          = strlen("STABLE") + 1;

      sr = stmt_bind_param(stmt, 2, InputOutputType, ValueType, ParameterType, ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength, &StrLen_or_Ind);
      if (sr == SQL_ERROR) return SQL_ERROR;
      return stmt_execute(stmt);
    }

    if (0) {
      // TODO: Catalog/Schema/TableName/TableType
      // https://github.com/taosdata/TDengine/issues/17890
      if (!*catalog) catalog = "%";
      if (!*table) table = "%";
      NameLength1 = strlen(catalog);
      NameLength3 = strlen(table);
      int is_table  = 0;
      int is_stable = 0;
      r = table_type_parse(type, &is_table, &is_stable);
      if (r) {
        stmt_append_err_format(stmt, "HY000", 0, "General error:invalid `table_type:[%.*s]`", (int)NameLength4, type);
        return SQL_ERROR;
      }
      string_t str = {};
      do {
        if ((is_table && is_stable) || (!is_table && !is_stable)) {
          sql =
            "select * from ("
            " select db_name as TABLE_CAT, '' as TABLE_SCHEM, stable_name as TABLE_NAME,"
            " 'STABLE' as TABLE_TYPE, table_comment as REMARKS"
            " from information_schema.ins_stables"
            " union"
            " select db_name as TABLE_CAT, '' as TABLE_SCHEM, table_name as TABLE_NAME,"
            " 'TABLE' as TABLE_TYPE, table_comment as REMARKS"
            " from information_schema.ins_tables"
            ") where 1 = 1";
          r = string_concat(&str, sql);
          if (r) { stmt_oom(stmt); break; }
        } else if (is_stable) {
          sql =
            "select * from("
            " select db_name as TABLE_CAT, '' as TABLE_SCHEM, stable_name as TABLE_NAME,"
            " 'STABLE' as TABLE_TYPE, table_comment as REMARKS"
            " from information_schema.ins_stables"
            ") where 1 = 1";
          r = string_concat(&str, sql);
          if (r) { stmt_oom(stmt); break; }
        } else {
          sql =
            "select * from("
            " select db_name as TABLE_CAT, '' as TABLE_SCHEM, table_name as TABLE_NAME,"
            " 'TABLE' as TABLE_TYPE, table_comment as REMARKS"
            " from information_schema.ins_tables"
            ") where 1 = 1";
          r = string_concat(&str, sql);
          if (r) { stmt_oom(stmt); break; }
        }
        if (*catalog) {
          r = string_concat_fmt(&str, " and table_cat like '");
          if (r) { stmt_oom(stmt); break; }
          r = string_concat_replacement_n(&str, catalog, NameLength1);
          if (r) { stmt_oom(stmt); break; }
          r = string_concat_n(&str, "'", 1);
          if (r) { stmt_oom(stmt); break; }
        }
        if (*table) {
          r = string_concat_fmt(&str, " and table_name like '");
          if (r) { stmt_oom(stmt); break; }
          r = string_concat_replacement_n(&str, table, NameLength3);
          if (r) { stmt_oom(stmt); break; }
          r = string_concat_n(&str, "'", 1);
          if (r) { stmt_oom(stmt); break; }
        }
        sql = str.base;
        sr = stmt_exec_direct(stmt, sql, str.nr);
      } while (0);
      string_release(&str);
      if (r) return SQL_ERROR;
      return sr;
    }

    if (1) {
      // TODO: Catalog/Schema/TableName/TableType
      // https://github.com/taosdata/TDengine/issues/17890
      if (!*catalog) catalog = "%";
      if (!*table) table = "%";
      NameLength1 = strlen(catalog);
      NameLength3 = strlen(table);
      int is_table  = 0;
      int is_stable = 0;
      r = table_type_parse(type, &is_table, &is_stable);
      if (r) {
        stmt_append_err_format(stmt, "HY000", 0, "General error:invalid `table_type:[%.*s]`", (int)NameLength4, type);
        return SQL_ERROR;
      }
      string_t str = {};
      do {
        if ((is_table && is_stable) || (!is_table && !is_stable)) {
          sql =
            "select * from ("
            " select db_name as TABLE_CAT, '' as TABLE_SCHEM, stable_name as TABLE_NAME,"
            " 'STABLE' as TABLE_TYPE, table_comment as REMARKS"
            " from information_schema.ins_stables"
            " union"
            " select db_name as TABLE_CAT, '' as TABLE_SCHEM, table_name as TABLE_NAME,"
            " 'TABLE' as TABLE_TYPE, table_comment as REMARKS"
            " from information_schema.ins_tables"
            ") where 1 = 1";
          r = string_concat(&str, sql);
          if (r) { stmt_oom(stmt); break; }
        } else if (is_stable) {
          sql =
            "select * from("
            " select db_name as TABLE_CAT, '' as TABLE_SCHEM, stable_name as TABLE_NAME,"
            " 'STABLE' as TABLE_TYPE, table_comment as REMARKS"
            " from information_schema.ins_stables"
            ") where 1 = 1";
          r = string_concat(&str, sql);
          if (r) { stmt_oom(stmt); break; }
        } else {
          sql =
            "select * from("
            " select db_name as TABLE_CAT, '' as TABLE_SCHEM, table_name as TABLE_NAME,"
            " 'TABLE' as TABLE_TYPE, table_comment as REMARKS"
            " from information_schema.ins_tables"
            ") where 1 = 1";
          r = string_concat(&str, sql);
          if (r) { stmt_oom(stmt); break; }
        }
        if (*catalog) {
          r = string_concat_fmt(&str, " and table_cat like '");
          if (r) { stmt_oom(stmt); break; }
          r = string_concat_replacement_n(&str, catalog, NameLength1);
          if (r) { stmt_oom(stmt); break; }
          r = string_concat_n(&str, "'", 1);
          if (r) { stmt_oom(stmt); break; }
        }

        r = string_concat_fmt(&str, " order by table_type, table_cat, table_schem, table_name");
        if (r) { stmt_oom(stmt); break; }

        wildex_t *wild = NULL;
        if (*table) {
          r = wildcomp(&wild, table);
          if (r) {
            stmt_append_err_format(stmt, "HY000", 0, "General error:`invalid pattern for table_name:[%s]`", table);
            break;
          }
        }

        sql = str.base;
        sr = stmt_exec_direct(stmt, sql, str.nr);
        if (sr == SQL_ERROR) return SQL_ERROR;

        if (*table) {
          stmt->post_filter.ctx                   = wild;
          stmt->post_filter.post_filter           = _wild_post_filter;
          stmt->post_filter.post_filter_destroy   = _wild_post_filter_destroy;
        }
      } while (0);
      string_release(&str);
      if (r) return SQL_ERROR;
      return sr;
    }
  }

  stmt_append_err_format(stmt, "HY000", 0, "General error:`sql:[%s]` not supported yet", sql);
  return SQL_ERROR;
}

