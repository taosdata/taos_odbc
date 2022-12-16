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
// make sure `log.h` is included ahead of `taos_helpers.h`, for the `LOG_IMPL` issue
#include "log.h"
#include "stmt.h"
#include "taos_helpers.h"

#include <errno.h>
#include <iconv.h>
#include <limits.h>
#include <string.h>
#include <time.h>
#include <wchar.h>

static void tsdb_to_sql_c_state_reset(tsdb_to_sql_c_state_t *col)
{
  buffer_reset(&col->cache);
  mem_reset(&col->mem);

  col->field = NULL;
  col->i_col = -1;
  col->data  = NULL;
  col->len   = 0;

  col->state = DATA_GET_INIT;
}

static void tsdb_to_sql_c_state_release(tsdb_to_sql_c_state_t *col)
{
  tsdb_to_sql_c_state_reset(col);
  buffer_release(&col->cache);
  mem_release(&col->mem);
}

static void rowset_reset(rowset_t *rowset)
{
  rowset->i_row   = 0;
  rowset->nr_rows = 0;
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

static void _rs_release(rs_t *rs)
{
  if (!rs->res) return;

  if (rs->res_is_from_taos_query) CALL_taos_free_result(rs->res);

  rs->res_is_from_taos_query = 0;
  rs->res                    = NULL;
  rs->affected_row_count     = 0;
  rs->col_count              = 0;
  rs->fields                 = NULL;
  rs->lengths                = NULL;
  rs->time_precision         = 0;
}

static void _stmt_release_result(stmt_t *stmt)
{
  rowset_release(&stmt->rowset);
  _stmt_release_post_filter(stmt);
  _rs_release(&stmt->rs);
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
  tsdb_to_sql_c_state_t *col = &stmt->current_for_get_data;
  tsdb_to_sql_c_state_reset(col);
}

static void _stmt_release_current_for_get_data(stmt_t *stmt)
{
  tsdb_to_sql_c_state_t *col = &stmt->current_for_get_data;
  tsdb_to_sql_c_state_release(col);
}

static void _tsdb_meta_reset_tag_fields(tsdb_meta_t *tsdb_meta)
{
  if (tsdb_meta->tag_fields) {
#ifdef _WIN32
    // https://github.com/taosdata/TDengine/issues/18804
    // https://learn.microsoft.com/en-us/troubleshoot/developer/visualstudio/cpp/libraries/use-c-run-time
    // https://learn.microsoft.com/en-us/cpp/build/reference/md-mt-ld-use-run-time-library?view=msvc-170
    free(tsdb_meta->tag_fields);
#else
    free(tsdb_meta->tag_fields);
#endif
    tsdb_meta->tag_fields = NULL;
  }
  tsdb_meta->nr_tag_fields = 0;
}

static void _tsdb_meta_reset_col_fields(tsdb_meta_t *tsdb_meta)
{
  if (tsdb_meta->col_fields) {
    if (tsdb_meta->is_insert_stmt) {
#ifdef _WIN32
      // https://github.com/taosdata/TDengine/issues/18804
      // https://learn.microsoft.com/en-us/troubleshoot/developer/visualstudio/cpp/libraries/use-c-run-time
      // https://learn.microsoft.com/en-us/cpp/build/reference/md-mt-ld-use-run-time-library?view=msvc-170
      free(tsdb_meta->col_fields);
#else
      free(tsdb_meta->col_fields);
#endif
    } else {
      mem_reset(&tsdb_meta->mem);
    }
    tsdb_meta->col_fields = NULL;
  }
  tsdb_meta->nr_col_fields = 0;
}

static void _tsdb_meta_reset(tsdb_meta_t *tsdb_meta)
{
  _tsdb_meta_reset_tag_fields(tsdb_meta);
  _tsdb_meta_reset_col_fields(tsdb_meta);

  TOD_SAFE_FREE(tsdb_meta->subtbl);
  tsdb_meta->prepared = 0;
  tsdb_meta->is_insert_stmt = 0;
  tsdb_meta->subtbl_required = 0;
}

static void _tsdb_meta_release(tsdb_meta_t *tsdb_meta)
{
  _tsdb_meta_reset(tsdb_meta);

  mem_release(&tsdb_meta->mem);
  tsdb_meta->prepared = 0;
  tsdb_meta->is_insert_stmt = 0;
  tsdb_meta->subtbl_required = 0;
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

// static void _param_array_reset(param_array_t *pa)
// {
//   mem_reset(&pa->mem);
// }

static void _param_array_release(param_array_t *pa)
{
  mem_release(&pa->mem);
  mem_release(&pa->mem_is_null);
  mem_release(&pa->mem_length);
}

static void _param_set_reset(param_set_t *paramset)
{
  paramset->nr_params  = 0;
}

static void _param_set_release(param_set_t *paramset)
{
  for (int i=0; i<paramset->cap_params; ++i) {
    param_array_t *pa = paramset->params + i;
    _param_array_release(pa);
  }
  paramset->cap_params = 0;
  paramset->nr_params  = 0;
  TOD_SAFE_FREE(paramset->params);
}

static int _param_set_calloc(param_set_t *paramset, int nr_params)
{
  _param_set_reset(paramset);
  if (nr_params > paramset->cap_params) {
    int cap = (nr_params + 15) / 16 * 16;
    param_array_t *pas= (param_array_t*)realloc(paramset->params, sizeof(*pas) * cap);
    if (!pas) return -1;

    memset(pas + paramset->cap_params, 0, sizeof(*pas) * (cap - paramset->cap_params));

    paramset->cap_params = cap;
    paramset->nr_params = nr_params;
    paramset->params = pas;
  }
  paramset->nr_params = nr_params;
  return 0;
}

static void _tsdb_binds_reset(tsdb_binds_t *tsdb_binds)
{
  tsdb_binds->nr = 0;
}

static void _tsdb_binds_release(tsdb_binds_t *tsdb_binds)
{
  _tsdb_binds_reset(tsdb_binds);
  TOD_SAFE_FREE(tsdb_binds->mbs);
  tsdb_binds->cap = 0;
}

static int _tsdb_binds_malloc_tsdb_binds(tsdb_binds_t *tsdb_binds, int nr_params)
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

static void _stmt_release(stmt_t *stmt)
{
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

  _tsdb_meta_release(&stmt->tsdb_meta);

  TOD_SAFE_FREE(stmt->sql);

  errs_release(&stmt->errs);
  mem_release(&stmt->mem);
  _param_set_release(&stmt->paramset);
  _tsdb_binds_release(&stmt->tsdb_binds);

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

static SQLRETURN _stmt_post_exec(stmt_t *stmt, rs_t *rs)
{
  int e;
  const char *estr;

  e = CALL_taos_errno(rs->res);
  estr = CALL_taos_errstr(rs->res);

  if (e) {
    stmt_append_err_format(stmt, "HY000", e, "General error:[taosc]%s", estr);
    return SQL_ERROR;
  } else if (rs->res) {
    rs->time_precision = CALL_taos_result_precision(rs->res);
    rs->affected_row_count = CALL_taos_affected_rows(rs->res);
    rs->col_count = CALL_taos_field_count(rs->res);
    if (rs->col_count > 0) {
      rs->fields = CALL_taos_fetch_fields(rs->res);
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

static SQLRETURN _stmt_conv(stmt_t *stmt, mem_t *mem, charset_conv_t *cnv, const char *buf, size_t len, const char **pdata, size_t *plen)
{
  char *p = NULL;
  int n = 0;
  int r = 0;
  if (!cnv->cnv) {
    r = mem_keep(mem, len + 8);
    if (r) { stmt_oom(stmt); return SQL_ERROR; }
    p = (char*)mem->base;
    n = snprintf(p, mem->cap, "%.*s", (int)len, buf);
    OA_NIY(n >= 0);
  } else {
    r = mem_conv(mem, cnv->cnv, buf, len, cnv->nr_to_terminator);
    if (r) {
      int e = errno;
      if (e == ENOMEM) {
        stmt_oom(stmt);
      } else {
        stmt_append_err_format(stmt, "HY000", 0, "General error:charset conversion [%s] => [%s] failed:[%d]%s",
            cnv->from, cnv->to,
            e, strerror(e));
      }
      return SQL_ERROR;
    }
    p = (char*)mem->base;
    n = (int)mem->nr;
  }
  *pdata = p;
  *plen = n;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_exec_direct_sql(stmt_t *stmt, const char *sql)
{
  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->taos);
  OA_ILE(sql);

  TAOS *taos = stmt->conn->taos;

  if (stmt->stmt) {
    // NOTE: not fully tested yet
    _stmt_release_stmt(stmt);

    _tsdb_meta_reset(&stmt->tsdb_meta);
    _tsdb_binds_reset(&stmt->tsdb_binds);

    TOD_SAFE_FREE(stmt->sql);
  }

  // TODO: to support exec_direct parameterized statement

  charset_conv_t *cnv = &stmt->conn->cnv_sql_c_char_to_tsdb_varchar;
  if (cnv->cnv) {
    mem_t *mem = &stmt->mem;
    size_t nr_from_terminator = cnv->nr_from_terminator;
    SQLRETURN sr = SQL_SUCCESS;
    const char *data = NULL;
    size_t len = 0;
    sr = _stmt_conv(stmt, mem, cnv, sql, strlen(sql) + nr_from_terminator, &data, &len);
    if (sr == SQL_ERROR) return SQL_ERROR;
    sql = data;
  }

  rs_t *rs = &stmt->rs;
  rs->res = CALL_taos_query(taos, sql);
  rs->res_is_from_taos_query = rs->res ? 1 : 0;

  return _stmt_post_exec(stmt, &stmt->rs);
}

static SQLRETURN _stmt_exec_direct(stmt_t *stmt, const char *sql, int len)
{
  char buf[1024];
  char *p = (char*)sql;
  if (len == SQL_NTS)
    len = (int)strlen(sql);
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

static void _stmt_close_cursor(stmt_t *stmt)
{
  _stmt_reset_current_for_get_data(stmt);
  rowset_reset(&stmt->rowset);
  _stmt_release_result(stmt);
  if (_stmt_get_rows_fetched_ptr(stmt)) *_stmt_get_rows_fetched_ptr(stmt) = 0;
}

SQLRETURN stmt_exec_direct(stmt_t *stmt, const char *sql, int len)
{
  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->taos);

  _stmt_close_cursor(stmt);

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
    if (stmt->prepared && !stmt->tsdb_meta.is_insert_stmt) {
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
  rs_t *rs = &stmt->rs;

  if (row_count_ptr) *row_count_ptr = rs->affected_row_count;
  return SQL_SUCCESS;
}

SQLRETURN stmt_get_col_count(stmt_t *stmt, SQLSMALLINT *col_count_ptr)
{
  rs_t *rs = &stmt->rs;

  if (col_count_ptr) *col_count_ptr = rs->col_count;
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
  rs_t *rs = &stmt->rs;
  TAOS_FIELD *p = rs->fields + ColumnNumber - 1;

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
        *DecimalDigitsPtr = (rs->time_precision + 1) * 3;
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
        "General error:[iconv]No character set conversion found for `%s` to `%s`:[%d]%s",
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
        "String data, right truncated:[iconv]Character set conversion for `%s` to `%s`, #%zd out of #%zd bytes consumed, #%zd out of #%zd bytes converted:[%d]%s",
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
        "General warning:[iconv]Character set conversion for `%s` to `%s` succeeded with #%zd of nonreversible characters converted",
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
  rs_t *rs = &stmt->rs;
  switch (rs->time_precision) {
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
    desc_record_t *ARD_records = (desc_record_t*)realloc(ARD->records, sizeof(*ARD_records) * cap);
    if (!ARD_records) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
    for (size_t i = ARD->cap; i<cap; ++i) {
      desc_record_t *ARD_record = ARD_records + i;
      memset(ARD_record, 0, sizeof(*ARD_record));
    }
    ARD->records = ARD_records;
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
      ARD_record->DESC_OCTET_LENGTH      = ARD_record->DESC_LENGTH;
      break;
    case SQL_C_SHORT:
      ARD_record->DESC_LENGTH            = 2;
      ARD_record->DESC_PRECISION         = 0;
      ARD_record->DESC_SCALE             = 0;
      ARD_record->DESC_TYPE              = TargetType;
      ARD_record->DESC_CONCISE_TYPE      = TargetType;
      ARD_record->DESC_OCTET_LENGTH      = ARD_record->DESC_LENGTH;
      break;
    case SQL_C_SLONG:
      ARD_record->DESC_LENGTH            = 4;
      ARD_record->DESC_PRECISION         = 0;
      ARD_record->DESC_SCALE             = 0;
      ARD_record->DESC_TYPE              = TargetType;
      ARD_record->DESC_CONCISE_TYPE      = TargetType;
      ARD_record->DESC_OCTET_LENGTH      = ARD_record->DESC_LENGTH;
      break;
    case SQL_C_SBIGINT:
      ARD_record->DESC_LENGTH            = 8;
      ARD_record->DESC_PRECISION         = 0;
      ARD_record->DESC_SCALE             = 0;
      ARD_record->DESC_TYPE              = TargetType;
      ARD_record->DESC_CONCISE_TYPE      = TargetType;
      ARD_record->DESC_OCTET_LENGTH      = ARD_record->DESC_LENGTH;
      break;
    case SQL_C_DOUBLE:
      ARD_record->DESC_LENGTH            = 8;
      ARD_record->DESC_PRECISION         = 0;
      ARD_record->DESC_SCALE             = 0;
      ARD_record->DESC_TYPE              = TargetType;
      ARD_record->DESC_CONCISE_TYPE      = TargetType;
      ARD_record->DESC_OCTET_LENGTH      = ARD_record->DESC_LENGTH;
      break;
    case SQL_C_CHAR:
      ARD_record->DESC_LENGTH            = 0; // FIXME:
      ARD_record->DESC_PRECISION         = 0;
      ARD_record->DESC_SCALE             = 0;
      ARD_record->DESC_TYPE              = TargetType;
      ARD_record->DESC_CONCISE_TYPE      = TargetType;
      ARD_record->DESC_OCTET_LENGTH      = BufferLength;
      break;
    case SQL_C_WCHAR:
      ARD_record->DESC_LENGTH            = 0; // FIXME:
      ARD_record->DESC_PRECISION         = 0;
      ARD_record->DESC_SCALE             = 0;
      ARD_record->DESC_TYPE              = TargetType;
      ARD_record->DESC_CONCISE_TYPE      = TargetType;
      ARD_record->DESC_OCTET_LENGTH      = BufferLength;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:#%d Column converstion to `%s[0x%x/%d]` not implemented yet",
          ColumnNumber, sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
      break;
  }

  ARD_record->DESC_DATA_PTR            = TargetValuePtr;
  ARD_record->DESC_INDICATOR_PTR       = StrLen_or_IndPtr;
  ARD_record->DESC_OCTET_LENGTH_PTR    = StrLen_or_IndPtr;

  if (ColumnNumber > ARD_header->DESC_COUNT) ARD_header->DESC_COUNT = ColumnNumber;

  ARD_record->bound = 1;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_len(stmt_t *stmt, int row, int col, const char **data, int *len)
{
  rs_t             *rs      = &stmt->rs;
  rowset_t         *rowset  = &stmt->rowset;

  TAOS_RES         *res   = rs->res;
  TAOS_FIELD       *field = rs->fields + col;
  TAOS_ROW          rows  = rowset->rows;
  int r = helper_get_data_len(res, field, rows, row, col, data, len);

  if (r) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:#%d Column[%s] conversion from `%s[0x%x/%d]` not implemented yet",
        col+1, field->name, taos_data_type(field->type), field->type, field->type);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLPOINTER _stmt_get_address(stmt_t *stmt, SQLPOINTER ptr, SQLULEN octet_length, int i_row, desc_header_t *header)
{
  (void)stmt;
  if (ptr == NULL) return ptr;

  char *base = (char*)ptr;
  if (header->DESC_BIND_OFFSET_PTR) base += *header->DESC_BIND_OFFSET_PTR;

  char *dest = base + octet_length * i_row;

  return dest;
}

static SQLRETURN _stmt_fetch_next_rowset(stmt_t *stmt, TAOS_ROW *rows)
{
  rs_t *rs = &stmt->rs;
  rowset_t *rowset = &stmt->rowset;

  rowset_reset(rowset);

  // TODO:
  // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlfetch-function?view=sql-server-ver16#positioning-the-cursor
  int nr_rows = CALL_taos_fetch_block(rs->res, rows);
  if (nr_rows == 0) return SQL_NO_DATA;
  rowset->rows = *rows;          // column-wise
  rowset->nr_rows = nr_rows;
  rowset->i_row   = 0;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_from_tsdb_timestamp_to_sql_c_sbigint(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  (void)stmt;

  int64_t v = *(int64_t*)conv_state->data;

  *(int64_t*)conv_state->TargetValuePtr = v;

  conv_state->data = NULL;
  conv_state->len = 0;

  if (conv_state->StrLen_or_IndPtr) *conv_state->StrLen_or_IndPtr = sizeof(int64_t);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_to_sql_c_char_epilog(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  size_t len = 0;
  len = strnlen(conv_state->TargetValuePtr, conv_state->BufferLength);
  OA_NIY(len > 0 || conv_state->BufferLength <= 1);

  if (conv_state->StrLen_or_IndPtr) *conv_state->StrLen_or_IndPtr = conv_state->len;

  conv_state->data += len;
  conv_state->len  -= (int)len;

  if (conv_state->len == 0) {
    conv_state->data = NULL;
    conv_state->state = DATA_GET_DONE;
    return SQL_SUCCESS;
  }
  stmt_append_err_format(stmt, "01004", 0, "String data, right truncated:Column `%s[#%d]`",
      conv_state->field->name, conv_state->i_col + 1);
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN _stmt_conv_to_sql_c_char_next(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  int nn = snprintf(conv_state->TargetValuePtr, conv_state->BufferLength, "%.*s", (int)conv_state->len, conv_state->data);
  if (nn < 0) {
    stmt_append_err(stmt, "HY000", 0, "General error: internal logic error");
    return SQL_ERROR;
  }

  return _stmt_conv_to_sql_c_char_epilog(stmt, conv_state);
}

static SQLRETURN _stmt_conv_from_tsdb_timestamp_to_sql_c_char(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  if (conv_state->state == DATA_GET_DONE) return SQL_NO_DATA;

  if (conv_state->state == DATA_GET_INIT) {
    int64_t v = *(int64_t*)conv_state->data;

    char buf[64];
    int n = _tsdb_timestamp_to_string(stmt, v, buf);
    OA_ILE(n > 0);

    SQLRETURN sr = SQL_SUCCESS;
    const char *data = NULL;
    size_t len = 0;
    sr = _stmt_conv(stmt, &conv_state->mem, &stmt->conn->cnv_utf8_to_sql_c_char, buf, n, &data, &len);
    if (sr == SQL_ERROR) return SQL_ERROR;
    conv_state->data = data;
    conv_state->len = len;
    conv_state->state = DATA_GET_GETTING;
  }
  return _stmt_conv_to_sql_c_char_next(stmt, conv_state);
}

static SQLRETURN _stmt_conv_to_sql_c_wchar_next(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  if (conv_state->len + 2 <= (size_t)conv_state->BufferLength) {
    char *ptr = (char*)conv_state->TargetValuePtr;
    memcpy(ptr, conv_state->data, conv_state->len);
    ptr += conv_state->len;
    // terminating
    ptr[0] = '\0';
    ptr[1] = '\0';
    if (conv_state->StrLen_or_IndPtr) *conv_state->StrLen_or_IndPtr = conv_state->len;
    conv_state->data = NULL;
    conv_state->len = 0;
    return SQL_SUCCESS;
  }

  if (conv_state->StrLen_or_IndPtr) *conv_state->StrLen_or_IndPtr = conv_state->len;

  size_t len = conv_state->BufferLength / 2 * 2;
  if (len >= 2) {
    len -= 2;
    char *ptr = (char*)conv_state->TargetValuePtr;
    memcpy(ptr, conv_state->data, len);
    ptr += len;
    // terminating
    ptr[0] = '\0';
    ptr[1] = '\0';

    conv_state->data += len;
    conv_state->len  -= (int)len;
  }

  stmt_append_err_format(stmt, "01004", 0, "String data, right truncated:Column `%s[#%d]`",
      conv_state->field->name, conv_state->i_col + 1);
  return SQL_SUCCESS_WITH_INFO;
}

static SQLRETURN _stmt_conv_to_sql_c_wchar(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state, const char *buf, size_t len)
{
  int r = 0;
  size_t n = len;

  char wbuf[512];

  char       *p         = wbuf;

  char       *inbuf     = (char*)buf;
  size_t      inbytes   = n;

  char       *outbuf    = wbuf;
  size_t      outbytes  = sizeof(wbuf);

  SQLRETURN sr = _stmt_encode(stmt, "UTF8", &inbuf, &inbytes, "UCS-2LE", &outbuf, &outbytes);
  if (sr == SQL_ERROR) return SQL_ERROR;
  OA_NIY(sql_succeeded(sr));
  n = (int)(sizeof(wbuf) - outbytes);

  outbuf[0] = '\0';
  outbuf[1] = '\0';

  if (n + 2 <= (size_t)conv_state->BufferLength) {
    char *ptr = (char*)conv_state->TargetValuePtr;
    memcpy(ptr, wbuf, n);
    ptr += n;
    // terminating
    ptr[0] = '\0';
    ptr[1] = '\0';
    if (conv_state->StrLen_or_IndPtr) *conv_state->StrLen_or_IndPtr = n;
    conv_state->data = NULL;
    conv_state->len = 0;
    return SQL_SUCCESS;
  }

  r = buffer_copy_n(&conv_state->cache, (const unsigned char*)p, n);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  conv_state->data = conv_state->cache.base;
  conv_state->len  = (int)conv_state->cache.nr;

  return _stmt_conv_to_sql_c_wchar_next(stmt, conv_state);
}

static SQLRETURN _stmt_conv_from_tsdb_timestamp_to_sql_c_wchar(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  if (conv_state->cache.nr == 0) {
    int64_t v = *(int64_t*)conv_state->data;

    char buf[64];
    int n = _tsdb_timestamp_to_string(stmt, v, buf);
    OA_ILE(n > 0);

    return _stmt_conv_to_sql_c_wchar(stmt, conv_state, buf, n);
  } else {
    return _stmt_conv_to_sql_c_wchar_next(stmt, conv_state);
  }
}

static SQLRETURN _stmt_conv_from_tsdb_bool_to_sql_c_char(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  if (conv_state->state == DATA_GET_DONE) return SQL_NO_DATA;

  if (conv_state->state == DATA_GET_INIT) {
    int8_t v = *(int8_t*)conv_state->data;

    const char *buf = v ? "true" : "false";
    int n = v ? 4 : 5;

    SQLRETURN sr = SQL_SUCCESS;
    const char *data = NULL;
    size_t len = 0;
    sr = _stmt_conv(stmt, &conv_state->mem, &stmt->conn->cnv_utf8_to_sql_c_char, buf, n, &data, &len);
    if (sr == SQL_ERROR) return SQL_ERROR;
    conv_state->data = data;
    conv_state->len = len;
    conv_state->state = DATA_GET_GETTING;
  }
  return _stmt_conv_to_sql_c_char_next(stmt, conv_state);
}

static SQLRETURN _stmt_conv_from_tsdb_tinyint_to_sql_c_short(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  (void)stmt;

  int8_t v = *(int8_t*)conv_state->data;

  *(int16_t*)conv_state->TargetValuePtr = v;

  conv_state->data = NULL;
  conv_state->len = 0;

  if (conv_state->StrLen_or_IndPtr) *conv_state->StrLen_or_IndPtr = sizeof(int16_t);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_from_tsdb_utinyint_to_sql_c_utinyint(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  (void)stmt;

  uint8_t v = *(uint8_t*)conv_state->data;

  *(uint8_t*)conv_state->TargetValuePtr = v;

  conv_state->data = NULL;
  conv_state->len = 0;

  if (conv_state->StrLen_or_IndPtr) *conv_state->StrLen_or_IndPtr = sizeof(uint8_t);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_from_tsdb_smallint_to_sql_c_short(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  (void)stmt;

  int16_t v = *(int16_t*)conv_state->data;

  *(int16_t*)conv_state->TargetValuePtr = v;

  conv_state->data = NULL;
  conv_state->len = 0;

  if (conv_state->StrLen_or_IndPtr) *conv_state->StrLen_or_IndPtr = sizeof(int16_t);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_from_tsdb_usmallint_to_sql_c_slong(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  (void)stmt;

  uint16_t v = *(uint16_t*)conv_state->data;

  *(int32_t*)conv_state->TargetValuePtr = v;

  conv_state->data = NULL;
  conv_state->len = 0;

  if (conv_state->StrLen_or_IndPtr) *conv_state->StrLen_or_IndPtr = sizeof(int32_t);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_from_tsdb_int_to_sql_c_slong(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  (void)stmt;

  int32_t v = *(int32_t*)conv_state->data;

  *(int32_t*)conv_state->TargetValuePtr = v;

  conv_state->data = NULL;
  conv_state->len = 0;

  if (conv_state->StrLen_or_IndPtr) *conv_state->StrLen_or_IndPtr = sizeof(int32_t);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_from_tsdb_uint_to_sql_c_sbigint(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  (void)stmt;

  uint32_t v = *(uint32_t*)conv_state->data;

  *(int64_t*)conv_state->TargetValuePtr = v;

  conv_state->data = NULL;
  conv_state->len = 0;

  if (conv_state->StrLen_or_IndPtr) *conv_state->StrLen_or_IndPtr = sizeof(int64_t);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_from_tsdb_int_to_sql_c_char(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  if (conv_state->state == DATA_GET_DONE) return SQL_NO_DATA;

  if (conv_state->state == DATA_GET_INIT) {
    int32_t v = *(int32_t*)conv_state->data;

    char buf[64];
    int n = snprintf(buf, sizeof(buf), "%d", v);
    OA_ILE(n > 0 && (size_t)n < sizeof(buf));

    SQLRETURN sr = SQL_SUCCESS;
    const char *data = NULL;
    size_t len = 0;
    sr = _stmt_conv(stmt, &conv_state->mem, &stmt->conn->cnv_utf8_to_sql_c_char, buf, n, &data, &len);
    if (sr == SQL_ERROR) return SQL_ERROR;
    conv_state->data = data;
    conv_state->len = len;
    conv_state->state = DATA_GET_GETTING;
  }
  return _stmt_conv_to_sql_c_char_next(stmt, conv_state);
}

static SQLRETURN _stmt_conv_from_tsdb_int_to_sql_c_wchar(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  if (conv_state->cache.nr == 0) {
    int32_t v = *(int32_t*)conv_state->data;

    char buf[64];
    int n = snprintf(buf, sizeof(buf), "%d", v);
    OA_ILE(n > 0 && (size_t)n < sizeof(buf));

    return _stmt_conv_to_sql_c_wchar(stmt, conv_state, buf, n);
  } else {
    return _stmt_conv_to_sql_c_wchar_next(stmt, conv_state);
  }
}

static SQLRETURN _stmt_conv_from_tsdb_bigint_to_sql_c_slong(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  (void)stmt;

  int64_t v = *(int64_t*)conv_state->data;

  *(int32_t*)conv_state->TargetValuePtr = (int32_t)v;

  conv_state->data = NULL;
  conv_state->len = 0;

  if (conv_state->StrLen_or_IndPtr) *conv_state->StrLen_or_IndPtr = sizeof(int32_t);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_from_tsdb_bigint_to_sql_c_sbigint(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  (void)stmt;

  int64_t v = *(int64_t*)conv_state->data;

  *(int64_t*)conv_state->TargetValuePtr = v;

  conv_state->data = NULL;
  conv_state->len = 0;

  if (conv_state->StrLen_or_IndPtr) *conv_state->StrLen_or_IndPtr = sizeof(int64_t);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_from_tsdb_bigint_to_sql_c_char(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  if (conv_state->state == DATA_GET_DONE) return SQL_NO_DATA;

  if (conv_state->state == DATA_GET_INIT) {
    int64_t v = *(int64_t*)conv_state->data;

    char buf[64];
    int n = snprintf(buf, sizeof(buf), "%" PRId64 "", v);
    OA_ILE(n > 0 && (size_t)n < sizeof(buf));

    SQLRETURN sr = SQL_SUCCESS;
    const char *data = NULL;
    size_t len = 0;
    sr = _stmt_conv(stmt, &conv_state->mem, &stmt->conn->cnv_utf8_to_sql_c_char, buf, n, &data, &len);
    if (sr == SQL_ERROR) return SQL_ERROR;
    conv_state->data = data;
    conv_state->len = len;
    conv_state->state = DATA_GET_GETTING;
  }
  return _stmt_conv_to_sql_c_char_next(stmt, conv_state);
}

static SQLRETURN _stmt_conv_from_tsdb_float_to_sql_c_char(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  if (conv_state->state == DATA_GET_DONE) return SQL_NO_DATA;

  if (conv_state->state == DATA_GET_INIT) {
    float v = *(float*)conv_state->data;

    char buf[64];
    int n = snprintf(buf, sizeof(buf), "%g", v);
    OA_ILE(n > 0 && (size_t)n < sizeof(buf));

    SQLRETURN sr = SQL_SUCCESS;
    const char *data = NULL;
    size_t len = 0;
    sr = _stmt_conv(stmt, &conv_state->mem, &stmt->conn->cnv_utf8_to_sql_c_char, buf, n, &data, &len);
    if (sr == SQL_ERROR) return SQL_ERROR;
    conv_state->data = data;
    conv_state->len = len;
    conv_state->state = DATA_GET_GETTING;
  }
  return _stmt_conv_to_sql_c_char_next(stmt, conv_state);
}

static SQLRETURN _stmt_conv_from_tsdb_float_to_sql_c_float(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  (void)stmt;

  float v = *(float*)conv_state->data;

  *(float*)conv_state->TargetValuePtr = v;

  conv_state->data = NULL;
  conv_state->len = 0;

  if (conv_state->StrLen_or_IndPtr) *conv_state->StrLen_or_IndPtr = sizeof(float);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_from_tsdb_double_to_sql_c_double(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  (void)stmt;

  double v = *(double*)conv_state->data;

  *(double*)conv_state->TargetValuePtr = v;

  conv_state->data = NULL;
  conv_state->len = 0;

  if (conv_state->StrLen_or_IndPtr) *conv_state->StrLen_or_IndPtr = sizeof(double);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_from_tsdb_double_to_sql_c_char(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  if (conv_state->state == DATA_GET_DONE) return SQL_NO_DATA;

  if (conv_state->state == DATA_GET_INIT) {
    double v = *(double*)conv_state->data;

    char buf[64];
    int n = snprintf(buf, sizeof(buf), "%g", v);
    OA_ILE(n > 0 && (size_t)n < sizeof(buf));

    SQLRETURN sr = SQL_SUCCESS;
    const char *data = NULL;
    size_t len = 0;
    sr = _stmt_conv(stmt, &conv_state->mem, &stmt->conn->cnv_utf8_to_sql_c_char, buf, n, &data, &len);
    if (sr == SQL_ERROR) return SQL_ERROR;
    conv_state->data = data;
    conv_state->len = len;
    conv_state->state = DATA_GET_GETTING;
  }
  return _stmt_conv_to_sql_c_char_next(stmt, conv_state);
}

static SQLRETURN _stmt_conv_from_tsdb_varchar_to_sql_c_char(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  if (conv_state->state == DATA_GET_DONE) return SQL_NO_DATA;

  if (conv_state->state == DATA_GET_INIT) {
    SQLRETURN sr = SQL_SUCCESS;
    const char *data = NULL;
    size_t len = 0;
    sr = _stmt_conv(stmt, &conv_state->mem, &stmt->conn->cnv_tsdb_varchar_to_sql_c_char, conv_state->data, conv_state->len, &data, &len);
    if (sr == SQL_ERROR) return SQL_ERROR;
    conv_state->data = data;
    conv_state->len = len;
    conv_state->state = DATA_GET_GETTING;
  }

  int n = snprintf(conv_state->TargetValuePtr, conv_state->BufferLength,
      "%.*s", (int)conv_state->len, conv_state->data);
  if (n < 0) {
    stmt_append_err(stmt, "HY000", 0, "General error: internal logic error");
    return SQL_ERROR;
  }
  if (conv_state->StrLen_or_IndPtr) *conv_state->StrLen_or_IndPtr = n;

  if (n < conv_state->BufferLength) {
    conv_state->data = NULL;
    conv_state->len = 0;
    return SQL_SUCCESS;
  }

  return _stmt_conv_to_sql_c_char_epilog(stmt, conv_state);
}

static SQLRETURN _stmt_conv_from_tsdb_varchar_to_sql_c_wchar(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  if (conv_state->cache.nr == 0) {
    return _stmt_conv_to_sql_c_wchar(stmt, conv_state, conv_state->data, conv_state->len);
  } else {
    return _stmt_conv_to_sql_c_wchar_next(stmt, conv_state);
  }
}

static SQLRETURN _stmt_conv_from_tsdb_nchar_to_sql_c_char(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  return _stmt_conv_from_tsdb_varchar_to_sql_c_char(stmt, conv_state);
}

static SQLRETURN _stmt_conv_from_tsdb_nchar_to_sql_c_wchar(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state)
{
  return _stmt_conv_from_tsdb_varchar_to_sql_c_wchar(stmt, conv_state);
}

static SQLRETURN _stmt_get_conv_from_tsdb_timestamp_to_sql_c(stmt_t *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    TAOS_FIELD    *field,
    SQLSMALLINT    TargetType,
    conv_from_tsdb_to_sql_c_f *conv)
{
  *conv = NULL;
  switch (TargetType) {
    case SQL_C_SBIGINT:
      *conv = _stmt_conv_from_tsdb_timestamp_to_sql_c_sbigint;
      break;
    case SQL_C_CHAR:
      *conv = _stmt_conv_from_tsdb_timestamp_to_sql_c_char;
      break;
    case SQL_C_WCHAR:
      *conv = _stmt_conv_from_tsdb_timestamp_to_sql_c_wchar;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column #%d[%s] converstion from `%s[0x%x/%d]` to `%s[0x%x/%d]` not implemented yet",
          Col_or_Param_Num,
          field->name,
          taos_data_type(field->type), field->type, field->type,
          sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_from_tsdb_bool_to_sql_c(stmt_t *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    TAOS_FIELD    *field,
    SQLSMALLINT    TargetType,
    conv_from_tsdb_to_sql_c_f *conv)
{
  *conv = NULL;
  switch (TargetType) {
    case SQL_C_CHAR:
      *conv = _stmt_conv_from_tsdb_bool_to_sql_c_char;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column #%d[%s] converstion from `%s[0x%x/%d]` to `%s[0x%x/%d]` not implemented yet",
          Col_or_Param_Num,
          field->name,
          taos_data_type(field->type), field->type, field->type,
          sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_from_tsdb_tinyint_to_sql_c(stmt_t *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    TAOS_FIELD    *field,
    SQLSMALLINT    TargetType,
    conv_from_tsdb_to_sql_c_f *conv)
{
  *conv = NULL;
  switch (TargetType) {
    case SQL_C_SHORT:
      *conv = _stmt_conv_from_tsdb_tinyint_to_sql_c_short;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column #%d[%s] converstion from `%s[0x%x/%d]` to `%s[0x%x/%d]` not implemented yet",
          Col_or_Param_Num,
          field->name,
          taos_data_type(field->type), field->type, field->type,
          sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_from_tsdb_utinyint_to_sql_c(stmt_t *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    TAOS_FIELD    *field,
    SQLSMALLINT    TargetType,
    conv_from_tsdb_to_sql_c_f *conv)
{
  *conv = NULL;
  switch (TargetType) {
    case SQL_C_UTINYINT:
      *conv = _stmt_conv_from_tsdb_utinyint_to_sql_c_utinyint;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column #%d[%s] converstion from `%s[0x%x/%d]` to `%s[0x%x/%d]` not implemented yet",
          Col_or_Param_Num,
          field->name,
          taos_data_type(field->type), field->type, field->type,
          sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_from_tsdb_smallint_to_sql_c(stmt_t *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    TAOS_FIELD    *field,
    SQLSMALLINT    TargetType,
    conv_from_tsdb_to_sql_c_f *conv)
{
  *conv = NULL;
  switch (TargetType) {
    case SQL_C_SHORT:
      *conv = _stmt_conv_from_tsdb_smallint_to_sql_c_short;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column #%d[%s] converstion from `%s[0x%x/%d]` to `%s[0x%x/%d]` not implemented yet",
          Col_or_Param_Num,
          field->name,
          taos_data_type(field->type), field->type, field->type,
          sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_from_tsdb_usmallint_to_sql_c(stmt_t *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    TAOS_FIELD    *field,
    SQLSMALLINT    TargetType,
    conv_from_tsdb_to_sql_c_f *conv)
{
  *conv = NULL;
  switch (TargetType) {
    case SQL_C_SLONG:
      *conv = _stmt_conv_from_tsdb_usmallint_to_sql_c_slong;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column #%d[%s] converstion from `%s[0x%x/%d]` to `%s[0x%x/%d]` not implemented yet",
          Col_or_Param_Num,
          field->name,
          taos_data_type(field->type), field->type, field->type,
          sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_from_tsdb_int_to_sql_c(stmt_t *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    TAOS_FIELD    *field,
    SQLSMALLINT    TargetType,
    conv_from_tsdb_to_sql_c_f *conv)
{
  *conv = NULL;
  switch (TargetType) {
    case SQL_C_SLONG:
      *conv = _stmt_conv_from_tsdb_int_to_sql_c_slong;
      break;
    case SQL_C_CHAR:
      *conv = _stmt_conv_from_tsdb_int_to_sql_c_char;
      break;
    case SQL_C_WCHAR:
      *conv = _stmt_conv_from_tsdb_int_to_sql_c_wchar;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column #%d[%s] converstion from `%s[0x%x/%d]` to `%s[0x%x/%d]` not implemented yet",
          Col_or_Param_Num,
          field->name,
          taos_data_type(field->type), field->type, field->type,
          sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_from_tsdb_uint_to_sql_c(stmt_t *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    TAOS_FIELD    *field,
    SQLSMALLINT    TargetType,
    conv_from_tsdb_to_sql_c_f *conv)
{
  *conv = NULL;
  switch (TargetType) {
    case SQL_C_SBIGINT:
      *conv = _stmt_conv_from_tsdb_uint_to_sql_c_sbigint;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column #%d[%s] converstion from `%s[0x%x/%d]` to `%s[0x%x/%d]` not implemented yet",
          Col_or_Param_Num,
          field->name,
          taos_data_type(field->type), field->type, field->type,
          sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_from_tsdb_bigint_to_sql_c(stmt_t *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    TAOS_FIELD    *field,
    SQLSMALLINT    TargetType,
    conv_from_tsdb_to_sql_c_f *conv)
{
  *conv = NULL;
  switch (TargetType) {
    case SQL_C_SLONG:
      *conv = _stmt_conv_from_tsdb_bigint_to_sql_c_slong;
      break;
    case SQL_C_SBIGINT:
      *conv = _stmt_conv_from_tsdb_bigint_to_sql_c_sbigint;
      break;
    case SQL_C_CHAR:
      *conv = _stmt_conv_from_tsdb_bigint_to_sql_c_char;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column #%d[%s] converstion from `%s[0x%x/%d]` to `%s[0x%x/%d]` not implemented yet",
          Col_or_Param_Num,
          field->name,
          taos_data_type(field->type), field->type, field->type,
          sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_from_tsdb_float_to_sql_c(stmt_t *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    TAOS_FIELD    *field,
    SQLSMALLINT    TargetType,
    conv_from_tsdb_to_sql_c_f *conv)
{
  *conv = NULL;
  switch (TargetType) {
    case SQL_C_CHAR:
      *conv = _stmt_conv_from_tsdb_float_to_sql_c_char;
      break;
    case SQL_C_FLOAT:
      *conv = _stmt_conv_from_tsdb_float_to_sql_c_float;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column #%d[%s] converstion from `%s[0x%x/%d]` to `%s[0x%x/%d]` not implemented yet",
          Col_or_Param_Num,
          field->name,
          taos_data_type(field->type), field->type, field->type,
          sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_from_tsdb_double_to_sql_c(stmt_t *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    TAOS_FIELD    *field,
    SQLSMALLINT    TargetType,
    conv_from_tsdb_to_sql_c_f *conv)
{
  *conv = NULL;
  switch (TargetType) {
    case SQL_C_DOUBLE:
      *conv = _stmt_conv_from_tsdb_double_to_sql_c_double;
      break;
    case SQL_C_CHAR:
      *conv = _stmt_conv_from_tsdb_double_to_sql_c_char;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column #%d[%s] converstion from `%s[0x%x/%d]` to `%s[0x%x/%d]` not implemented yet",
          Col_or_Param_Num,
          field->name,
          taos_data_type(field->type), field->type, field->type,
          sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_from_tsdb_varchar_to_sql_c(stmt_t *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    TAOS_FIELD    *field,
    SQLSMALLINT    TargetType,
    conv_from_tsdb_to_sql_c_f *conv)
{
  *conv = NULL;
  switch (TargetType) {
    case SQL_C_CHAR:
      *conv = _stmt_conv_from_tsdb_varchar_to_sql_c_char;
      break;
    case SQL_C_WCHAR:
      *conv = _stmt_conv_from_tsdb_varchar_to_sql_c_wchar;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column #%d[%s] converstion from `%s[0x%x/%d]` to `%s[0x%x/%d]` not implemented yet",
          Col_or_Param_Num,
          field->name,
          taos_data_type(field->type), field->type, field->type,
          sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_from_tsdb_nchar_to_sql_c(stmt_t *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    TAOS_FIELD    *field,
    SQLSMALLINT    TargetType,
    conv_from_tsdb_to_sql_c_f *conv)
{
  *conv = NULL;
  switch (TargetType) {
    case SQL_C_CHAR:
      *conv = _stmt_conv_from_tsdb_nchar_to_sql_c_char;
      break;
    case SQL_C_WCHAR:
      *conv = _stmt_conv_from_tsdb_nchar_to_sql_c_wchar;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column #%d[%s] converstion from `%s[0x%x/%d]` to `%s[0x%x/%d]` not implemented yet",
          Col_or_Param_Num,
          field->name,
          taos_data_type(field->type), field->type, field->type,
          sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_from_tsdb_to_sql_c(stmt_t *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    SQLSMALLINT    TargetType,
    conv_from_tsdb_to_sql_c_f *conv)
{
  rs_t *rs = &stmt->rs;
  TAOS_FIELD *field = rs->fields + Col_or_Param_Num - 1;
  switch (field->type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      return _stmt_get_conv_from_tsdb_timestamp_to_sql_c(stmt, Col_or_Param_Num, field, TargetType, conv);
    case TSDB_DATA_TYPE_BOOL:
      return _stmt_get_conv_from_tsdb_bool_to_sql_c(stmt, Col_or_Param_Num, field, TargetType, conv);
    case TSDB_DATA_TYPE_TINYINT:
      return _stmt_get_conv_from_tsdb_tinyint_to_sql_c(stmt, Col_or_Param_Num, field, TargetType, conv);
    case TSDB_DATA_TYPE_UTINYINT:
      return _stmt_get_conv_from_tsdb_utinyint_to_sql_c(stmt, Col_or_Param_Num, field, TargetType, conv);
    case TSDB_DATA_TYPE_SMALLINT:
      return _stmt_get_conv_from_tsdb_smallint_to_sql_c(stmt, Col_or_Param_Num, field, TargetType, conv);
    case TSDB_DATA_TYPE_USMALLINT:
      return _stmt_get_conv_from_tsdb_usmallint_to_sql_c(stmt, Col_or_Param_Num, field, TargetType, conv);
    case TSDB_DATA_TYPE_INT:
      return _stmt_get_conv_from_tsdb_int_to_sql_c(stmt, Col_or_Param_Num, field, TargetType, conv);
    case TSDB_DATA_TYPE_UINT:
      return _stmt_get_conv_from_tsdb_uint_to_sql_c(stmt, Col_or_Param_Num, field, TargetType, conv);
    case TSDB_DATA_TYPE_BIGINT:
      return _stmt_get_conv_from_tsdb_bigint_to_sql_c(stmt, Col_or_Param_Num, field, TargetType, conv);
    case TSDB_DATA_TYPE_FLOAT:
      return _stmt_get_conv_from_tsdb_float_to_sql_c(stmt, Col_or_Param_Num, field, TargetType, conv);
    case TSDB_DATA_TYPE_DOUBLE:
      return _stmt_get_conv_from_tsdb_double_to_sql_c(stmt, Col_or_Param_Num, field, TargetType, conv);
    case TSDB_DATA_TYPE_VARCHAR:
      return _stmt_get_conv_from_tsdb_varchar_to_sql_c(stmt, Col_or_Param_Num, field, TargetType, conv);
    case TSDB_DATA_TYPE_NCHAR:
      return _stmt_get_conv_from_tsdb_nchar_to_sql_c(stmt, Col_or_Param_Num, field, TargetType, conv);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column #%d[%s] converstion from `%s[0x%x/%d]` to `%s[0x%x/%d]` not implemented yet",
          Col_or_Param_Num,
          field->name,
          taos_data_type(field->type), field->type, field->type,
          sql_c_data_type(TargetType), TargetType, TargetType);
      return SQL_ERROR;
  }
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

static SQLRETURN _stmt_get_data(
    stmt_t                    *stmt,
    conv_from_tsdb_to_sql_c_f  conv,
    tsdb_to_sql_c_state_t     *current,
    int                        iRow,
    SQLUSMALLINT               Col_or_Param_Num,
    SQLSMALLINT                TargetType,
    SQLPOINTER                 TargetValuePtr,
    SQLLEN                     BufferLength,
    SQLLEN                    *StrLenPtr,
    SQLLEN                    *IndPtr)
{
  SQLRETURN sr = SQL_SUCCESS;

  rs_t *rs = &stmt->rs;

  if (current->i_col + 1 != Col_or_Param_Num) {
    tsdb_to_sql_c_state_reset(current);
    current->i_col              = Col_or_Param_Num - 1;
    current->field              = rs->fields + current->i_col;

    const char *data;
    int len;

    sr = _stmt_get_data_len(stmt, iRow, current->i_col, &data, &len);
    if (!sql_succeeded(sr)) return sr;

    if (IndPtr) IndPtr[0] = 0;
    if (data == NULL && len == 0) {
      if (IndPtr) {
        IndPtr[0] = SQL_NULL_DATA;
        return SQL_SUCCESS;
      }
      stmt_append_err_format(stmt, "22002", 0, "Indicator variable required but not supplied:#%d Column_or_Param", Col_or_Param_Num);
      return SQL_ERROR;
    }

    current->data = data;
    current->len  = len;

    current->TargetType = TargetType;

    current->i_row              = iRow;
    current->TargetType         = TargetType;
  } else {
    if (current->TargetType != TargetType) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:TargetType changes in successive SQLGetData call for #%d Column_or_Param", Col_or_Param_Num);
      return SQL_ERROR;
    }
    if (current->data == NULL && current->len == 0) return SQL_NO_DATA;
  }

  current->TargetValuePtr     = TargetValuePtr;
  current->BufferLength       = BufferLength;
  current->StrLen_or_IndPtr   = StrLenPtr;

  return conv(stmt, current);
}

SQLRETURN stmt_get_data(
    stmt_t        *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  stmt->get_or_put_or_undef = 1; // TODO:
  // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetdata-function?view=sql-server-ver16
  // https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/getting-long-data?view=sql-server-ver16
  OA_NIY(stmt->rs.res);
  OA_NIY(stmt->rowset.rows);

  if (StrLen_or_IndPtr) StrLen_or_IndPtr[0] = SQL_NO_TOTAL;

  rs_t *rs = &stmt->rs;
  rowset_t *rowset = &stmt->rowset;

  if (Col_or_Param_Num < 1 || Col_or_Param_Num > rs->col_count) {
    stmt_append_err_format(stmt, "07009", 0, "Invalid descriptor index:#%d Col_or_Param", Col_or_Param_Num);
    return SQL_ERROR;
  }

  conv_from_tsdb_to_sql_c_f conv = NULL;
  SQLRETURN sr = _stmt_get_conv_from_tsdb_to_sql_c(stmt, Col_or_Param_Num, TargetType, &conv);
  if (sr == SQL_ERROR) return SQL_ERROR;

  tsdb_to_sql_c_state_t *current = &stmt->current_for_get_data;

  return _stmt_get_data(stmt, conv, current, rowset->i_row, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, StrLen_or_IndPtr, StrLen_or_IndPtr);
}

static SQLRETURN _stmt_fill_cell(stmt_t *stmt, int i_row, int i_col, int iRow)
{
  SQLRETURN sr;

  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;

  if (i_col >= ARD_header->DESC_COUNT) return SQL_SUCCESS;

  desc_record_t *ARD_record = ARD->records + i_col;
  if (ARD_record->DESC_DATA_PTR == NULL) return SQL_SUCCESS;

  char *base = ARD_record->DESC_DATA_PTR;
  if (ARD_header->DESC_BIND_OFFSET_PTR) base += *ARD_header->DESC_BIND_OFFSET_PTR;

  char *dest = base + ARD_record->DESC_OCTET_LENGTH * iRow;
  char *ptr = ARD_record->DESC_DATA_PTR;
  dest = _stmt_get_address(stmt, ptr, ARD_record->DESC_OCTET_LENGTH, iRow, ARD_header);
  SQLLEN *StrLenPtr = _stmt_get_address(stmt, ARD_record->DESC_OCTET_LENGTH_PTR, sizeof(SQLLEN), i_row, ARD_header);
  SQLLEN *IndPtr = _stmt_get_address(stmt, ARD_record->DESC_INDICATOR_PTR, sizeof(SQLLEN), i_row, ARD_header);

  rowset_t *rowset = &stmt->rowset;

  SQLSMALLINT    TargetType       = ARD_record->DESC_CONCISE_TYPE;
  SQLPOINTER     TargetValuePtr   = dest;
  SQLLEN         BufferLength     = ARD_record->DESC_OCTET_LENGTH;

  _stmt_reset_current_for_get_data(stmt);
  tsdb_to_sql_c_state_t *current = &stmt->current_for_get_data;
  sr = _stmt_get_data(stmt, ARD_record->conv, current, rowset->i_row + i_row, i_col+1, TargetType, TargetValuePtr, BufferLength, StrLenPtr, IndPtr);
  _stmt_reset_current_for_get_data(stmt);
  return sr;
}

static SQLRETURN _stmt_fill_rowset(stmt_t *stmt,
  SQLULEN row_array_size,
  post_filter_t *post_filter)
{
  SQLRETURN sr = SQL_SUCCESS;

  descriptor_t *ARD = _stmt_ARD(stmt);

  descriptor_t *IRD = _stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;

  if (IRD_header->DESC_ROWS_PROCESSED_PTR) *IRD_header->DESC_ROWS_PROCESSED_PTR = 0;

  rowset_t *rowset = &stmt->rowset;

  int iRow = 0;
  int with_info = 0;

  for (int i_row = 0; (SQLULEN)i_row<row_array_size; ++i_row) {
    if (i_row + rowset->i_row >= rowset->nr_rows) break;

    if (post_filter->post_filter) {
      int filter = 0;
again:
      sr = post_filter->post_filter(stmt, i_row + rowset->i_row, post_filter->ctx, &filter);
      if (sr == SQL_ERROR) return SQL_ERROR;
      if (filter) {
        ++i_row;
        if (i_row + rowset->i_row >= rowset->nr_rows) break;
        goto again;
      }
    }

    SQLRETURN row_sr = SQL_ROW_SUCCESS;

    for (int i_col = 0; (size_t)i_col < ARD->cap; ++i_col) {
      sr = _stmt_fill_cell(stmt, i_row, i_col, iRow);
      if (sr == SQL_ERROR) {
        row_sr = SQL_ROW_ERROR;
        with_info = 1;
        break;
      }
      if (sr == SQL_SUCCESS_WITH_INFO) {
        row_sr = SQL_ROW_SUCCESS_WITH_INFO;
        with_info = 1;
        continue;
      }
      OA_NIY(sr == SQL_SUCCESS);
    }

    if (IRD_header->DESC_ARRAY_STATUS_PTR) IRD_header->DESC_ARRAY_STATUS_PTR[iRow] = row_sr;
    if (IRD_header->DESC_ROWS_PROCESSED_PTR) *IRD_header->DESC_ROWS_PROCESSED_PTR += 1;
    ++iRow;
  }

  return with_info ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN _stmt_fetch(stmt_t *stmt)
{
  _stmt_reset_current_for_get_data(stmt);

  SQLULEN row_array_size = _stmt_get_row_array_size(stmt);
  OA_NIY(row_array_size > 0);

  SQLRETURN sr = SQL_SUCCESS;

  TAOS_ROW rows = NULL;
  OA_NIY(stmt->rs.res);

  rowset_t *rowset = &stmt->rowset;

  rowset->i_row += (int)row_array_size;
  if (rowset->i_row >= rowset->nr_rows) {
    sr = _stmt_fetch_next_rowset(stmt, &rows);
    if (sr == SQL_ERROR) return SQL_ERROR;
    if (sr == SQL_NO_DATA) return SQL_NO_DATA;
  }

  post_filter_t *post_filter = &stmt->post_filter;
  if (post_filter->post_filter) {
    while (1) {
      int filter = 0;
      sr = post_filter->post_filter(stmt, rowset->i_row, post_filter->ctx, &filter);
      if (sr == SQL_ERROR) return SQL_ERROR;
      if (!filter) break;

      ++rowset->i_row;
      if (rowset->i_row >= rowset->nr_rows) {
        sr = _stmt_fetch_next_rowset(stmt, &rows);
        if (sr == SQL_ERROR) return SQL_ERROR;
        if (sr == SQL_NO_DATA) return SQL_NO_DATA;
      }
    }
  }

  descriptor_t *ARD = _stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;

  OA_NIY(ARD->cap >= ARD_header->DESC_COUNT);

  for (int i_col = 0; (size_t)i_col < ARD->cap; ++i_col) {
    if (i_col >= ARD_header->DESC_COUNT) continue;
    desc_record_t *ARD_record = ARD->records + i_col;
    if (!ARD_record->bound) continue;
    if (ARD_record->DESC_DATA_PTR == NULL) continue;

    SQLSMALLINT TargetType = ARD_record->DESC_CONCISE_TYPE;

    conv_from_tsdb_to_sql_c_f conv = NULL;
    sr = _stmt_get_conv_from_tsdb_to_sql_c(stmt, i_col + 1, TargetType, &conv);
    if (sr == SQL_ERROR) return SQL_ERROR;
    ARD_record->conv = conv;
  }

  if (ARD_header->DESC_BIND_TYPE != SQL_BIND_BY_COLUMN) {
    stmt_append_err(stmt, "HY000", 0, "General error:only `SQL_BIND_BY_COLUMN` is supported now");
    return SQL_ERROR;
  }

  sr = _stmt_fill_rowset(stmt, row_array_size, post_filter);
  return sr;
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
  stmt->tsdb_meta.nr_tag_fields = tagNum;
  stmt->tsdb_meta.tag_fields    = tags;

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
  stmt->tsdb_meta.nr_col_fields = colNum;
  stmt->tsdb_meta.col_fields    = cols;

  return SQL_SUCCESS;
}

static SQLSMALLINT _stmt_get_count_of_tsdb_params(stmt_t *stmt)
{
  SQLSMALLINT n = 0;

  if (stmt->tsdb_meta.is_insert_stmt) {
    n = !!stmt->tsdb_meta.subtbl_required;
    n += stmt->tsdb_meta.nr_tag_fields;
    n += stmt->tsdb_meta.nr_col_fields;
  } else {
    n = stmt->tsdb_meta.nr_col_fields;
  }

  return n;
}

SQLRETURN _stmt_get_taos_tags_cols_for_subtbled_insert(stmt_t *stmt, int e)
{
  // fake subtbl name to get tags/cols meta-info
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  r = CALL_taos_stmt_set_tbname(stmt->stmt, "__hard_coded_fake_name__");
  if (r) {
    stmt_append_err_format(stmt, "HY000", e, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
    return SQL_ERROR;
  }

  sr = _stmt_describe_tags(stmt);
  if (sr == SQL_ERROR) return SQL_ERROR;

  sr = _stmt_describe_cols(stmt);
  if (sr == SQL_ERROR) return SQL_ERROR;

  // insert into ? ... will result in TSDB_CODE_TSC_STMT_TBNAME_ERROR
  stmt->tsdb_meta.subtbl_required = 1;
  return SQL_SUCCESS;
}

SQLRETURN _stmt_get_taos_tags_cols_for_normal_insert(stmt_t *stmt, int e)
{
  // insert into t ... and t is normal tablename, will result in TSDB_CODE_TSC_STMT_API_ERROR
  SQLRETURN sr = SQL_SUCCESS;

  stmt->tsdb_meta.subtbl_required = 0;
  stmt_append_err(stmt, "HY000", e, "General error:this is believed an non-subtbl insert statement");
  sr = _stmt_describe_cols(stmt);

  return sr;
}

SQLRETURN _stmt_get_taos_tags_cols_for_insert(stmt_t *stmt)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;
  int tagNum = 0;
  TAOS_FIELD_E *tag_fields = NULL;
  r = CALL_taos_stmt_get_tag_fields(stmt->stmt, &tagNum, &tag_fields);
  if (r) {
    int e = CALL_taos_errno(NULL);
    if (e == TSDB_CODE_TSC_STMT_TBNAME_ERROR) {
      sr = _stmt_get_taos_tags_cols_for_subtbled_insert(stmt, r);
    } else if (e == TSDB_CODE_TSC_STMT_API_ERROR) {
      sr = _stmt_get_taos_tags_cols_for_normal_insert(stmt, r);
    }
    TOD_SAFE_FREE(tag_fields);
  } else {
    OA_NIY(tagNum == 0);
    OA_NIY(tag_fields == NULL);
    OA_NIY(stmt->tsdb_meta.tag_fields == NULL);
    OA_NIY(stmt->tsdb_meta.nr_tag_fields == 0);
    sr = _stmt_describe_cols(stmt);
  }
  return sr;
}

SQLRETURN _stmt_get_taos_params_for_non_insert(stmt_t *stmt)
{
  int r = 0;

  int nr_params = 0;
  r = CALL_taos_stmt_num_params(stmt->stmt, &nr_params);
  if (r) {
    stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
    _stmt_release_stmt(stmt);

    return SQL_ERROR;
  }

  tsdb_meta_t *tsdb_meta = &stmt->tsdb_meta;
  OA_NIY(tsdb_meta->is_insert_stmt == 0);
  OA_NIY(tsdb_meta->col_fields == NULL);
  OA_NIY(tsdb_meta->nr_col_fields == 0);
  int nr = (nr_params + 15) / 16 * 16;
  r = mem_keep(&tsdb_meta->mem, sizeof(*tsdb_meta->col_fields)*nr);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_meta->nr_col_fields = nr_params;
  tsdb_meta->col_fields = (TAOS_FIELD_E*)tsdb_meta->mem.base;

  return SQL_SUCCESS;
}

SQLRETURN _stmt_prepare(stmt_t *stmt, const char *sql, size_t len)
{
  int r = 0;

  SQLRETURN sr = SQL_SUCCESS;
  OA_NIY(stmt->stmt == NULL);

  stmt->stmt = CALL_taos_stmt_init(stmt->conn->taos);
  if (!stmt->stmt) {
    stmt_append_err_format(stmt, "HY000", CALL_taos_errno(NULL), "General error:[taosc]%s", CALL_taos_errstr(NULL));
    return SQL_ERROR;
  }

  conn_t *conn = stmt->conn;
  mem_t *mem = &stmt->mem;

  const char *dst = NULL;
  size_t dlen = 0;
  sr = _stmt_conv(stmt, mem, &conn->cnv_sql_c_char_to_tsdb_varchar, sql, len, &dst, &dlen);
  if (sr == SQL_ERROR) return SQL_ERROR;

  r = CALL_taos_stmt_prepare(stmt->stmt, dst, (unsigned long)dlen);
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

  stmt->tsdb_meta.is_insert_stmt = isInsert;

  if (stmt->tsdb_meta.is_insert_stmt) {
    sr = _stmt_get_taos_tags_cols_for_insert(stmt);
  } else {
    sr = _stmt_get_taos_params_for_non_insert(stmt);
  }
  if (sr == SQL_ERROR) return SQL_ERROR;

  SQLSMALLINT n = _stmt_get_count_of_tsdb_params(stmt);
  if (n <= 0) {
    stmt_append_err(stmt, "HY000", 0, "General error:statement-without-parameter-placemarker not allowed to be prepared");
    return SQL_ERROR;
  }

  // if ((size_t)n > stmt->cap_mbs) {
  //   size_t cap = (n + 15) / 16 * 16;
  //   TAOS_MULTI_BIND *mbs = (TAOS_MULTI_BIND*)realloc(stmt->mbs, sizeof(*mbs) * cap);
  //   if (!mbs) {
  //     stmt_oom(stmt);
  //     return SQL_ERROR;
  //   }
  //   stmt->mbs = mbs;
  //   stmt->cap_mbs = cap;
  //   stmt->nr_mbs = n;

  //   memset(stmt->mbs, 0, sizeof(*stmt->mbs) * stmt->nr_mbs);
  // }

  stmt->prepared = 1;

  return SQL_SUCCESS;
}

static void _stmt_unprepare(stmt_t *stmt)
{
  _tsdb_meta_reset(&stmt->tsdb_meta);
  _tsdb_binds_reset(&stmt->tsdb_binds);

  stmt->prepared = 0;
}

static SQLRETURN _stmt_get_tag_or_col_field(stmt_t *stmt, int iparam, TAOS_FIELD_E *field)
{
  tsdb_meta_t *tsdb_meta = &stmt->tsdb_meta;
  TAOS_FIELD_E *p;
  if (stmt->tsdb_meta.subtbl_required) {
    if (iparam == 0) {
      field->type          = TSDB_DATA_TYPE_VARCHAR;
      field->bytes         = 1024; // TODO: check taos-doc for max length of subtable name
      return SQL_SUCCESS;
    }
    if (iparam < 1 + tsdb_meta->nr_tag_fields) {
      p = tsdb_meta->tag_fields + iparam - 1;
    } else {
      p = tsdb_meta->col_fields + iparam - 1 - tsdb_meta->nr_tag_fields;
    }
  } else {
    p = tsdb_meta->col_fields + iparam;
  }

  memcpy(field, p, sizeof(*field));

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

  if (stmt->tsdb_meta.is_insert_stmt) {
    tsdb_meta_t *tsdb_meta = &stmt->tsdb_meta;
    TAOS_FIELD_E *field = NULL;
    if (stmt->tsdb_meta.subtbl_required) {
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
      } else if (ParameterNumber <= 1 + tsdb_meta->nr_tag_fields) {
        field = tsdb_meta->tag_fields + ParameterNumber - 1 - 1;
      } else {
        field = tsdb_meta->col_fields + ParameterNumber - 1 - 1 - tsdb_meta->nr_tag_fields;
      }
    } else {
      // NOTE: DM to make sure ParameterNumber is valid
      // if (ParameterNumber > stmt->nr_col_fields) {
      //   stmt_append_err_format(stmt, "HY000", 0,
      //       "General error:#%d param field out of range [%d parameter markers in total]", ParameterNumber, stmt->nr_col_fields);
      //   return SQL_ERROR;
      // }
      field = tsdb_meta->col_fields + ParameterNumber - 1;
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
  *(int8_t*)meta->dst_base = (int8_t)v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_sql_c_sbigint_to_tsdb_smallint(stmt_t *stmt, sql_c_to_tsdb_meta_t *meta)
{
  int64_t v = *(int64_t*)meta->src_base;
  if (v > SHRT_MAX || v < SHRT_MIN) {
    stmt_append_err_format(stmt, "22003", 0, "Numeric value out of range:smallint is required, but got ==[%" PRId64 "]==", v);
    return SQL_ERROR;
  }
  *(int16_t*)meta->dst_base = (int16_t)v;

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

static SQLRETURN _stmt_conv_sql_c_char_to_tsdb_timestamp_x(stmt_t *stmt, const char *src, size_t len, int precision, int64_t *dst)
{
  SQLRETURN sr = SQL_SUCCESS;

  int64_t v = 0;
  W("len:%zd", len);

  const char *p;
  const char *format = "%Y-%m-%d %H:%M:%S";
  struct tm t = {0};
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
    int n = (int)(len - (p-src));
    if (n == 4) {
      if (precision != 0) {
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
      if (precision != 1) {
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
      if (precision != 1) {
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
          "Invalid datetime format:timestamp is required, but got ==[%.*s]==, precision:%d;n:%d", (int)len, src, precision,n);
      return SQL_ERROR;
    }
  } else {
    // TODO: precision
    stmt_append_err_format(stmt, "22007", 0,
        "Invalid datetime format:timestamp is required, but got ==[%.*s]==", (int)len, src);
    return SQL_ERROR;
  }

  *dst = v;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_sql_c_char_to_tsdb_timestamp(stmt_t *stmt, sql_c_to_tsdb_meta_t *meta)
{
  SQLRETURN sr = SQL_SUCCESS;

  char   *src = meta->src_base;
  SQLLEN  len = meta->src_len;
  if (len == SQL_NTS) len = strlen(src);
  else                len = strnlen(src, len);

  sr = _stmt_conv_sql_c_char_to_tsdb_timestamp_x(stmt, src, len, meta->field->precision, (int64_t*)meta->dst_base);
  OA_NIY(0);
  return sr;
}

static SQLRETURN _stmt_conv_sql_c_double_to_tsdb_timestamp(stmt_t *stmt, sql_c_to_tsdb_meta_t *meta)
{
  double v = *(double*)meta->src_base;
  if (v > INT64_MAX || v < INT64_MIN) {
    stmt_append_err_format(stmt, "22003", 0, "Numeric value out of range:bigint is required, but got ==[%lg]==", v);
    return SQL_ERROR;
  }
  *(int64_t*)meta->dst_base = (int64_t)v;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_sql_c_sbigint_to_tsdb_int(stmt_t *stmt, sql_c_to_tsdb_meta_t *meta)
{
  int64_t v = *(int64_t*)meta->src_base;
  if (v > INT_MAX || v < INT_MIN) {
    stmt_append_err_format(stmt, "22003", 0, "Numeric value out of range:int is required, but got ==[%" PRId64 "]==", v);
    return SQL_ERROR;
  }
  *(int32_t*)meta->dst_base = (int32_t)v;

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

  descriptor_t *IPD = _stmt_IPD(stmt);
  desc_record_t *IPD_record = IPD->records + ParameterNumber - 1;
  SQLSMALLINT ParameterType = IPD_record->DESC_CONCISE_TYPE;

  if (!APD_record->bound || !IPD_record->bound) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter not bound yet",
        ParameterNumber);
    return SQL_ERROR;
  }

  TAOS_MULTI_BIND *tsdb_mb = stmt->tsdb_binds.mbs + ParameterNumber - 1;
  OA_NIY(stmt->tsdb_binds.mbs);

  if (stmt->tsdb_meta.is_insert_stmt) {
    TAOS_FIELD_E field = {0};
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
        tsdb_mb->buffer_type             = field.type;
        tsdb_mb->buffer                  = APD_record->DESC_DATA_PTR;
        tsdb_mb->buffer_length           = sizeof(int64_t);
        tsdb_mb->length                  = NULL;
        tsdb_mb->is_null                 = NULL;

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
        tsdb_mb->buffer_type             = field.type;
        tsdb_mb->buffer                  = APD_record->DESC_DATA_PTR;
        tsdb_mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        tsdb_mb->length                  = NULL;
        tsdb_mb->is_null                 = NULL;

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
        tsdb_mb->buffer_type             = field.type;
        tsdb_mb->buffer                  = APD_record->DESC_DATA_PTR;
        tsdb_mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        tsdb_mb->length                  = NULL;
        tsdb_mb->is_null                 = NULL;

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
        tsdb_mb->buffer_type             = field.type;
        tsdb_mb->buffer                  = APD_record->DESC_DATA_PTR;
        tsdb_mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        tsdb_mb->length                  = NULL;
        tsdb_mb->is_null                 = NULL;

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
        tsdb_mb->buffer_type             = field.type;
        tsdb_mb->buffer                  = APD_record->DESC_DATA_PTR;
        tsdb_mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        tsdb_mb->length                  = NULL;
        tsdb_mb->is_null                 = NULL;

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
        tsdb_mb->buffer_type             = field.type;
        tsdb_mb->buffer                  = APD_record->DESC_DATA_PTR;
        tsdb_mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        tsdb_mb->length                  = NULL;
        tsdb_mb->is_null                 = NULL;

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
        tsdb_mb->buffer_type             = field.type;
        tsdb_mb->buffer                  = APD_record->DESC_DATA_PTR;
        tsdb_mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        tsdb_mb->length                  = NULL;
        tsdb_mb->is_null                 = NULL;

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
        tsdb_mb->buffer_type             = field.type;
        tsdb_mb->buffer                  = APD_record->DESC_DATA_PTR;
        tsdb_mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        tsdb_mb->length                  = NULL;
        tsdb_mb->is_null                 = NULL;

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
        tsdb_mb->buffer_type             = field.type;
        tsdb_mb->buffer                  = APD_record->DESC_DATA_PTR;
        tsdb_mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        tsdb_mb->length                  = NULL;
        tsdb_mb->is_null                 = NULL;

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
        tsdb_mb->buffer_type             = field.type;
        tsdb_mb->buffer                  = APD_record->DESC_DATA_PTR;
        tsdb_mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        tsdb_mb->length                  = NULL;
        tsdb_mb->is_null                 = NULL;

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
        tsdb_mb->buffer_type             = TSDB_DATA_TYPE_VARCHAR;
        tsdb_mb->buffer                  = APD_record->DESC_DATA_PTR;
        tsdb_mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        tsdb_mb->length                  = NULL;
        tsdb_mb->is_null                 = NULL;

        APD_record->create_length_array = _stmt_create_length_array;
        if (ValueType == SQL_C_CHAR) {
          break;
        }
        if (ValueType == SQL_C_SBIGINT) {
          tsdb_mb->buffer = NULL;
          tsdb_mb->buffer_length = IPD_record->DESC_LENGTH;

          APD_record->convf               = _stmt_conv_sql_c_sbigint_to_tsdb_varchar;
          APD_record->create_buffer_array = _stmt_create_tsdb_varchar_array;
          break;
        }
        if (ValueType == SQL_C_DOUBLE) {
          tsdb_mb->buffer = NULL;
          tsdb_mb->buffer_length = IPD_record->DESC_LENGTH;

          APD_record->convf               = _stmt_conv_sql_c_double_to_tsdb_varchar;
          APD_record->create_buffer_array = _stmt_create_tsdb_varchar_array;
          break;
        }
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
            ParameterNumber, sql_data_type(ParameterType), sql_c_data_type(ValueType));
        return SQL_ERROR;
      case SQL_INTEGER:
        tsdb_mb->buffer_type             = TSDB_DATA_TYPE_INT;
        tsdb_mb->buffer                  = APD_record->DESC_DATA_PTR;
        tsdb_mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        tsdb_mb->length                  = NULL;
        tsdb_mb->is_null                 = NULL;

        if (ValueType == SQL_C_SLONG) break;
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d parameter [%s] is expected, but got [%s] ",
            ParameterNumber, sql_data_type(ParameterType), sql_c_data_type(ValueType));
        return SQL_ERROR;
      case SQL_BIGINT:
        tsdb_mb->buffer_type             = TSDB_DATA_TYPE_BIGINT;
        tsdb_mb->buffer                  = APD_record->DESC_DATA_PTR;
        tsdb_mb->buffer_length           = APD_record->DESC_OCTET_LENGTH;
        tsdb_mb->length                  = NULL;
        tsdb_mb->is_null                 = NULL;

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
  OA_NIY(stmt->stmt == NULL);

  _stmt_close_cursor(stmt);

  _stmt_unprepare(stmt);

  const char *sql = (const char*)StatementText;

  size_t len = TextLength;
  if (TextLength == SQL_NTS) len = strlen(sql);
  else                       len = strnlen(sql, TextLength);

  const char *sqlx = NULL;
  size_t n = 0;
  char *s = NULL, *pre = NULL;

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

  switch (InputOutputType) {
    case SQL_PARAM_INPUT:
      break;
    case SQL_PARAM_INPUT_OUTPUT:
      break;
    case SQL_PARAM_OUTPUT:
      stmt_append_err(stmt, "HY000", 0, "SQL_PARAM_OUTPUT not supported yet by taos");
      return SQL_ERROR;
#if (ODBCVER >= 0x0380)      /* { */
    case SQL_PARAM_INPUT_OUTPUT_STREAM:
      stmt_append_err(stmt, "HY000", 0, "SQL_PARAM_INPUT_OUTPUT_STREAM not supported yet by taos");
      return SQL_ERROR;
    case SQL_PARAM_OUTPUT_STREAM:
      stmt_append_err(stmt, "HY000", 0, "SQL_PARAM_OUTPUT_STREAM not supported yet by taos");
      return SQL_ERROR;
#endif                       /* } */
    default:
      stmt_append_err(stmt, "HY000", 0, "unknown InputOutputType for `SQLBindParameter`");
      return SQL_ERROR;
  }

  if (ParameterNumber > APD->cap) {
    size_t cap = (ParameterNumber + 15) / 16 * 16;
    desc_record_t *APD_records = (desc_record_t*)realloc(APD->records, sizeof(*APD_records) * cap);
    if (!APD_records) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
    for (size_t i = APD->cap; i<cap; ++i) {
      desc_record_t *APD_record = APD_records + i;
      memset(APD_record, 0, sizeof(*APD_record));
    }
    APD->records = APD_records;
    APD->cap     = cap;
  }

  desc_record_t *APD_record = APD->records + ParameterNumber - 1;
  APD_record->bound = 0;

  descriptor_t *IPD = _stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  if (ParameterNumber >= IPD->cap) {
    size_t cap = (ParameterNumber + 15) / 16 * 16;
    desc_record_t *IPD_records = (desc_record_t*)realloc(IPD->records, sizeof(*IPD_records) * cap);
    if (!IPD_records) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
    for (size_t i = IPD->cap; i<cap; ++i) {
      desc_record_t *IPD_record = IPD_records + i;
      memset(IPD_record, 0, sizeof(*IPD_record));
    }
    IPD->records = IPD_records;
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
            "General error:#%d Parameter[SQL_TYPE_TIMESTAMP(%zd.%d)] not implemented yet",
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
  TAOS_MULTI_BIND *mb = stmt->tsdb_binds.mbs + i_param;

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

  tsdb_meta_t *tsdb_meta = &stmt->tsdb_meta;

  TOD_SAFE_FREE(tsdb_meta->subtbl);
  tsdb_meta->subtbl = strndup(base, length);
  if (!tsdb_meta->subtbl) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  r = CALL_taos_stmt_set_tbname(stmt->stmt, tsdb_meta->subtbl);
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

  TAOS_MULTI_BIND *mb = stmt->tsdb_binds.mbs + i_param;

  desc_record_t *APD_record = APD->records + i_param;
  desc_record_t *IPD_record = IPD->records + i_param;

  char   *src_base;
  SQLLEN  src_len;
  int     src_is_null;
  _stmt_param_get(stmt, irow, i_param, &src_base, &src_len, &src_is_null);

  if (APD_record->DESC_TYPE==SQL_C_CHAR && src_len == SQL_NTS) src_len = strlen(src_base);

  tsdb_meta_t *tsdb_meta = &stmt->tsdb_meta;
  if (irow>0 && i_param < (!!stmt->tsdb_meta.subtbl_required) + tsdb_meta->nr_tag_fields) {
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
    TAOS_FIELD_E field = {0};
    if (stmt->tsdb_meta.is_insert_stmt) {
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

  if (stmt->tsdb_meta.is_insert_stmt) {
    TAOS_FIELD_E field = {0};
    sr = _stmt_get_tag_or_col_field(stmt, i_param, &field);
    if (sr == SQL_ERROR) return SQL_ERROR;

    if (mb->buffer_type == TSDB_DATA_TYPE_VARCHAR) {
      if (src_len > field.bytes - 2) {
        stmt_append_err_format(stmt, "HY000", 0, "General error:#%d param [%.*s] too long [%d]", i_param+1, (int)src_len, src_base, field.bytes - 2);
        return SQL_ERROR;
      }
    } else if (mb->buffer_type == TSDB_DATA_TYPE_NCHAR) {
      size_t nr_bytes = 0;
#ifdef _WIN32
      // TODO: remove hard-coded GB18030
      sr = _stmt_calc_bytes(stmt, "GB18030", src_base, src_len, "UCS-4BE", &nr_bytes);
#else
      sr = _stmt_calc_bytes(stmt, "UTF8", src_base, src_len, "UCS-4BE", &nr_bytes);
#endif
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

  *dst_len = (int32_t)src_len;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_param_prepare_mb(stmt_t *stmt, int i_param)
{
  SQLRETURN sr = SQL_SUCCESS;

  descriptor_t *APD = _stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;
  TAOS_MULTI_BIND *mb = stmt->tsdb_binds.mbs + i_param;

  desc_record_t *APD_record = APD->records + i_param;

  tsdb_meta_t *tsdb_meta = &stmt->tsdb_meta;
  if (i_param < (!!stmt->tsdb_meta.subtbl_required) + tsdb_meta->nr_tag_fields) {
    mb->num = 1;
  } else {
    mb->num = (int)(APD_header->DESC_ARRAY_SIZE);
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
    stmt_append_err_format(stmt, "HY000", 0, "General error:internal logic error, DESC_ARRAY_SIZE[%zd] invalid", (size_t)APD_header->DESC_ARRAY_SIZE);
    return SQL_ERROR;
  }

  if (APD_header->DESC_ARRAY_SIZE > 1) {
    if (!stmt->tsdb_meta.is_insert_stmt) {
      stmt_append_err(stmt, "HY000", 0, "General error:taosc currently does not support batch execution for non-insert-statement");
      return SQL_ERROR;
    }
  }

  if (0) {
    r = _param_set_calloc(&stmt->paramset, APD_header->DESC_COUNT);
    if (r) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
  }

  if (IPD_header->DESC_ROWS_PROCESSED_PTR) *IPD_header->DESC_ROWS_PROCESSED_PTR = 0;

  if (stmt->tsdb_meta.is_insert_stmt && stmt->tsdb_meta.subtbl_required) {
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
      sr = _stmt_param_process(stmt, (int)irow, i_param);
      if (sr == SQL_ERROR) return SQL_ERROR;
    }
    if (IPD_header->DESC_ARRAY_STATUS_PTR) IPD_header->DESC_ARRAY_STATUS_PTR[irow] = SQL_PARAM_SUCCESS;
    if (IPD_header->DESC_ROWS_PROCESSED_PTR) *IPD_header->DESC_ROWS_PROCESSED_PTR += 1;
  }

  tsdb_meta_t *tsdb_meta = &stmt->tsdb_meta;
  if (stmt->tsdb_meta.is_insert_stmt && tsdb_meta->nr_tag_fields) {
    TAOS_MULTI_BIND *mbs = stmt->tsdb_binds.mbs + (!!stmt->tsdb_meta.subtbl_required);
    r = CALL_taos_stmt_set_tags(stmt->stmt, mbs);
    if (r) {
      stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
      return SQL_ERROR;
    }
  }

  r = CALL_taos_stmt_bind_param_batch(stmt->stmt, stmt->tsdb_binds.mbs + (!!stmt->tsdb_meta.subtbl_required) + tsdb_meta->nr_tag_fields);
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

typedef struct param_state_s                param_state_t;
struct param_state_s {
  int                        nr_paramset_size;
  SQLSMALLINT                nr_tsdb_fields;

  int                        i_row;
  int                        i_param;
  desc_record_t             *APD_record;
  desc_record_t             *IPD_record;

  TAOS_FIELD_E              *tsdb_field;

  param_array_t             *param_array;
  TAOS_MULTI_BIND           *tsdb_bind;
};

static SQLRETURN _stmt_guess_tsdb_meta_by_sql_varchar(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                             = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ValueType = APD_record->DESC_CONCISE_TYPE;

  charset_conv_t *cnv = &stmt->conn->cnv_sql_c_char_to_tsdb_varchar;

  tsdb_field->name[0] = '?';
  tsdb_field->name[1] = '\0';
  tsdb_field->precision = 0;
  tsdb_field->scale = 0;

  switch (ValueType) {
    case SQL_C_CHAR:
      if (!cnv) {
        tsdb_field->bytes = (int32_t)APD_record->DESC_OCTET_LENGTH + 2; // +2: consistent with taosc
        if (param_state->nr_paramset_size == 1 && (size_t)IPD_record->DESC_LENGTH > (size_t)APD_record->DESC_OCTET_LENGTH) {
          tsdb_field->bytes = (int32_t)IPD_record->DESC_LENGTH + 2; // +2: consistent with taosc
        }
      } else {
        tsdb_field->bytes = (int32_t)APD_record->DESC_OCTET_LENGTH * 8 + 2;   // *8: hard-coded
        if (param_state->nr_paramset_size == 1 && (size_t)IPD_record->DESC_LENGTH > (size_t)APD_record->DESC_OCTET_LENGTH) {
          tsdb_field->bytes = (int32_t)IPD_record->DESC_LENGTH * 8 + 2; // +2: consistent with taosc
        }
      }
      tsdb_field->type = TSDB_DATA_TYPE_VARCHAR;
      break;
    case SQL_C_SBIGINT:
      tsdb_field->type      = TSDB_DATA_TYPE_BIGINT;
      tsdb_field->bytes     = sizeof(int64_t);
      break;
    case SQL_C_DOUBLE:
      tsdb_field->type      = TSDB_DATA_TYPE_DOUBLE;
      tsdb_field->bytes     = sizeof(double);
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sql_c_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_guess_tsdb_meta_by_sql_integer(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                             = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ValueType = APD_record->DESC_CONCISE_TYPE;

  tsdb_field->name[0] = '?';
  tsdb_field->name[1] = '\0';
  tsdb_field->precision = 0;
  tsdb_field->scale = 0;
  tsdb_field->type = TSDB_DATA_TYPE_INT;

  switch (ValueType) {
    case SQL_C_SLONG:
      tsdb_field->bytes = sizeof(int32_t);
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sql_c_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_guess_tsdb_meta_by_sql_bigint(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                             = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ValueType = APD_record->DESC_CONCISE_TYPE;

  tsdb_field->name[0] = '?';
  tsdb_field->name[1] = '\0';
  tsdb_field->precision = 0;
  tsdb_field->scale = 0;

  switch (ValueType) {
    case SQL_C_SBIGINT:
      tsdb_field->type = TSDB_DATA_TYPE_BIGINT;
      tsdb_field->bytes = sizeof(int64_t);
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sql_c_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_guess_tsdb_meta(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                      = param_state->i_param;
  desc_record_t *IPD_record        = param_state->IPD_record;

  SQLSMALLINT ParameterType = IPD_record->DESC_CONCISE_TYPE;

  switch (ParameterType) {
    case SQL_VARCHAR:
      return _stmt_guess_tsdb_meta_by_sql_varchar(stmt, param_state);
    case SQL_INTEGER:
      return _stmt_guess_tsdb_meta_by_sql_integer(stmt, param_state);
    case SQL_BIGINT:
      return _stmt_guess_tsdb_meta_by_sql_bigint(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sql_data_type(ParameterType));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_check_tsdb_meta_by_sql_timestamp(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                      = param_state->i_param;
  desc_record_t *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E  *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ParameterType = IPD_record->DESC_CONCISE_TYPE;

  switch (tsdb_field->type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      return SQL_SUCCESS;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
        "General error:Parameter#%d conversion from [%s] to [%s] not implemented yet",
        i_param + 1, sql_data_type(ParameterType), taos_data_type(tsdb_field->type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_check_tsdb_meta_by_sql_bigint(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                      = param_state->i_param;
  desc_record_t *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E  *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ParameterType = IPD_record->DESC_CONCISE_TYPE;

  switch (tsdb_field->type) {
    case TSDB_DATA_TYPE_BIGINT:
      return SQL_SUCCESS;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
        "General error:Parameter#%d conversion from [%s] to [%s] not implemented yet",
        i_param + 1, sql_data_type(ParameterType), taos_data_type(tsdb_field->type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_check_tsdb_meta_by_sql_int(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                      = param_state->i_param;
  desc_record_t *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E  *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ParameterType = IPD_record->DESC_CONCISE_TYPE;

  switch (tsdb_field->type) {
    case TSDB_DATA_TYPE_INT:
      return SQL_SUCCESS;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
        "General error:Parameter#%d conversion from [%s] to [%s] not implemented yet",
        i_param + 1, sql_data_type(ParameterType), taos_data_type(tsdb_field->type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_check_tsdb_meta_by_sql_smallint(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                      = param_state->i_param;
  desc_record_t *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E  *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ParameterType = IPD_record->DESC_CONCISE_TYPE;

  switch (tsdb_field->type) {
    case TSDB_DATA_TYPE_SMALLINT:
      return SQL_SUCCESS;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
        "General error:Parameter#%d conversion from [%s] to [%s] not implemented yet",
        i_param + 1, sql_data_type(ParameterType), taos_data_type(tsdb_field->type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_check_tsdb_meta_by_sql_tinyint(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                      = param_state->i_param;
  desc_record_t *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E  *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ParameterType = IPD_record->DESC_CONCISE_TYPE;

  switch (tsdb_field->type) {
    case TSDB_DATA_TYPE_TINYINT:
      return SQL_SUCCESS;
    case TSDB_DATA_TYPE_BOOL:
      return SQL_SUCCESS;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
        "General error:Parameter#%d conversion from [%s] to [%s] not implemented yet",
        i_param + 1, sql_data_type(ParameterType), taos_data_type(tsdb_field->type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_check_tsdb_meta_by_sql_real(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                      = param_state->i_param;
  desc_record_t *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E  *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ParameterType = IPD_record->DESC_CONCISE_TYPE;

  switch (tsdb_field->type) {
    case TSDB_DATA_TYPE_FLOAT:
      return SQL_SUCCESS;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
        "General error:Parameter#%d conversion from [%s] to [%s] not implemented yet",
        i_param + 1, sql_data_type(ParameterType), taos_data_type(tsdb_field->type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_check_tsdb_meta_by_sql_double(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                      = param_state->i_param;
  desc_record_t *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E  *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ParameterType = IPD_record->DESC_CONCISE_TYPE;

  switch (tsdb_field->type) {
    case TSDB_DATA_TYPE_DOUBLE:
      return SQL_SUCCESS;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
        "General error:Parameter#%d conversion from [%s] to [%s] not implemented yet",
        i_param + 1, sql_data_type(ParameterType), taos_data_type(tsdb_field->type));
      return SQL_ERROR;
  }
}

static void _dump_TAOS_FIELD_E(TAOS_FIELD_E *tsdb_field)
{
  OD("TAOS_FIELD_E::bytes:%d", tsdb_field->bytes);
  OD("TAOS_FIELD_E::name:%.*s", (int)sizeof(tsdb_field->name), tsdb_field->name);
  OD("TAOS_FIELD_E::precision:%u", tsdb_field->precision);
  OD("TAOS_FIELD_E::scale:%u", tsdb_field->scale);
  OD("TAOS_FIELD_E::type:%s", taos_data_type(tsdb_field->type));
}

static SQLRETURN _stmt_check_tsdb_meta_by_sql_varchar(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                      = param_state->i_param;
  desc_record_t *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E  *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ParameterType = IPD_record->DESC_CONCISE_TYPE;

  switch (tsdb_field->type) {
    case TSDB_DATA_TYPE_VARCHAR:
      return SQL_SUCCESS;
    case TSDB_DATA_TYPE_NULL:
      if (!stmt->tsdb_meta.is_insert_stmt || !stmt->tsdb_meta.subtbl_required || param_state->i_param) {
        stmt_append_err_format(stmt, "HY000", 0,
          "General error:[taosc conformance]Parameter#%d[%s], but it's not the first parameter within a subtbled-insert statement",
          i_param + 1, taos_data_type(tsdb_field->type));
        return SQL_ERROR;
      }
      return SQL_SUCCESS;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
        "General error:Parameter#%d conversion from [%s] to [%s] not implemented yet",
        i_param + 1, sql_data_type(ParameterType), taos_data_type(tsdb_field->type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_check_tsdb_meta_by_sql_wvarchar(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                      = param_state->i_param;
  desc_record_t *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E  *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ParameterType = IPD_record->DESC_CONCISE_TYPE;

  switch (tsdb_field->type) {
    case TSDB_DATA_TYPE_NCHAR:
      return SQL_SUCCESS;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
        "General error:Parameter#%d conversion from [%s] to [%s] not implemented yet",
        i_param + 1, sql_data_type(ParameterType), taos_data_type(tsdb_field->type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_check_tsdb_meta(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                      = param_state->i_param;
  desc_record_t *IPD_record        = param_state->IPD_record;

  SQLSMALLINT ParameterType = IPD_record->DESC_CONCISE_TYPE;

  switch (ParameterType) {
    case SQL_TYPE_TIMESTAMP:
      return _stmt_check_tsdb_meta_by_sql_timestamp(stmt, param_state);
    case SQL_VARCHAR:
      return _stmt_check_tsdb_meta_by_sql_varchar(stmt, param_state);
    case SQL_WVARCHAR:
      return _stmt_check_tsdb_meta_by_sql_wvarchar(stmt, param_state);
    case SQL_BIGINT:
      return _stmt_check_tsdb_meta_by_sql_bigint(stmt, param_state);
    case SQL_DOUBLE:
      return _stmt_check_tsdb_meta_by_sql_double(stmt, param_state);
    case SQL_INTEGER:
      return _stmt_check_tsdb_meta_by_sql_int(stmt, param_state);
    case SQL_REAL:
      return _stmt_check_tsdb_meta_by_sql_real(stmt, param_state);
    case SQL_SMALLINT:
      return _stmt_check_tsdb_meta_by_sql_smallint(stmt, param_state);
    case SQL_TINYINT:
      return _stmt_check_tsdb_meta_by_sql_tinyint(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sql_data_type(ParameterType));
      return SQL_ERROR;
  }
}

static TAOS_FIELD_E* _stmt_get_tsdb_field_by_tsdb_meta(stmt_t *stmt, int i_param)
{
  tsdb_meta_t *tsdb_meta = &stmt->tsdb_meta;
  if (tsdb_meta->is_insert_stmt) {
    if (i_param == 0 && tsdb_meta->subtbl_required) {
      return &tsdb_meta->subtbl_field;
    }
    i_param -= !!tsdb_meta->subtbl_required;
    if (i_param < tsdb_meta->nr_tag_fields)
      return tsdb_meta->tag_fields + i_param;
    i_param -= tsdb_meta->nr_tag_fields;
  }
  OA_NIY(i_param < tsdb_meta->nr_col_fields);
  return tsdb_meta->col_fields + i_param;
}

static SQLRETURN _stmt_prepare_param_data_array_from_sql_c_char_to_tsdb_varchar(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  param_array_t        *param_array       = param_state->param_array;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  r = mem_keep(&param_array->mem_length, sizeof(*tsdb_bind->length) * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = (int32_t*)param_array->mem_length.base;

  charset_conv_t *cnv = &stmt->conn->cnv_sql_c_char_to_tsdb_varchar;
  if (!cnv) {
    tsdb_bind->buffer_length = tsdb_field->bytes - 2;
    tsdb_bind->buffer = APD_record->DESC_DATA_PTR;
    OA_NIY(0);
    return SQL_SUCCESS;
  }

  tsdb_bind->buffer_length = tsdb_field->bytes - 2;

  r = mem_keep(&param_array->mem, sizeof(char) * tsdb_bind->buffer_length * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_array->mem.base;
  OD("i_param/buffer/buffer_length:%d/%p/%zd", param_state->i_param, tsdb_bind->buffer, tsdb_bind->buffer_length);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_varchar(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;

  SQLSMALLINT ValueType = APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_CHAR:
      return _stmt_prepare_param_data_array_from_sql_c_char_to_tsdb_varchar(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sql_c_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_from_sql_c_char_to_tsdb_nchar(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  param_array_t        *param_array       = param_state->param_array;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  r = mem_keep(&param_array->mem_length, sizeof(*tsdb_bind->length) * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = (int32_t*)param_array->mem_length.base;

  charset_conv_t *cnv = &stmt->conn->cnv_sql_c_char_to_tsdb_varchar;
  if (!cnv) {
    tsdb_bind->buffer_length = APD_record->DESC_OCTET_LENGTH;
    tsdb_bind->buffer = APD_record->DESC_DATA_PTR;
    return SQL_SUCCESS;
  }
  if (stmt->tsdb_meta.is_insert_stmt) {
    tsdb_bind->buffer_length = tsdb_field->bytes - 2 /*+ cnv->nr_to_terminator*/;
  } else {
    tsdb_bind->buffer_length = tsdb_field->bytes + 8;
  }

  r = mem_keep(&param_array->mem, sizeof(char) * tsdb_bind->buffer_length * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_array->mem.base;
  OD("i_param/buffer/buffer_length:%d/%p/%zd", param_state->i_param, tsdb_bind->buffer, tsdb_bind->buffer_length);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_nchar(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                             = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;

  SQLSMALLINT ValueType = APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_CHAR:
      return _stmt_prepare_param_data_array_from_sql_c_char_to_tsdb_nchar(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sql_c_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_timestamp(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType = APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_SBIGINT:
    case SQL_C_DOUBLE:
      tsdb_bind->buffer_type = tsdb_field->type;
      tsdb_bind->length = NULL;
      tsdb_bind->buffer_length = APD_record->DESC_OCTET_LENGTH;
      tsdb_bind->buffer = APD_record->DESC_DATA_PTR;
      break;
    case SQL_C_CHAR:
      tsdb_bind->buffer_type = tsdb_field->type;
      tsdb_bind->length = NULL;
      tsdb_bind->buffer_length = APD_record->DESC_OCTET_LENGTH;
      tsdb_bind->buffer = APD_record->DESC_DATA_PTR;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sql_c_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_bigint(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType = APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_SBIGINT:
    // case SQL_C_DOUBLE:
      tsdb_bind->buffer_type = tsdb_field->type;
      tsdb_bind->length = NULL;
      tsdb_bind->buffer_length = APD_record->DESC_OCTET_LENGTH;
      tsdb_bind->buffer = APD_record->DESC_DATA_PTR;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sql_c_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_double(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType = APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_DOUBLE:
      tsdb_bind->buffer_type = tsdb_field->type;
      tsdb_bind->length = NULL;
      tsdb_bind->buffer_length = APD_record->DESC_OCTET_LENGTH;
      tsdb_bind->buffer = APD_record->DESC_DATA_PTR;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sql_c_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_int(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  param_array_t        *param_array       = param_state->param_array;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int32_t);
  r = mem_keep(&param_array->mem, sizeof(char) * tsdb_bind->buffer_length * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_array->mem.base;
  OD("i_param/buffer/buffer_length:%d/%p/%zd", param_state->i_param, tsdb_bind->buffer, tsdb_bind->buffer_length);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_smallint(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  param_array_t        *param_array       = param_state->param_array;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int16_t);
  r = mem_keep(&param_array->mem, sizeof(char) * tsdb_bind->buffer_length * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_array->mem.base;
  OD("i_param/buffer/buffer_length:%d/%p/%zd", param_state->i_param, tsdb_bind->buffer, tsdb_bind->buffer_length);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_tinyint(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  param_array_t        *param_array       = param_state->param_array;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int8_t);
  r = mem_keep(&param_array->mem, sizeof(char) * tsdb_bind->buffer_length * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_array->mem.base;
  OD("i_param/buffer/buffer_length:%d/%p/%zd", param_state->i_param, tsdb_bind->buffer, tsdb_bind->buffer_length);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_bool(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  param_array_t        *param_array       = param_state->param_array;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int8_t);
  r = mem_keep(&param_array->mem, sizeof(char) * tsdb_bind->buffer_length * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_array->mem.base;
  OD("i_param/buffer/buffer_length:%d/%p/%zd", param_state->i_param, tsdb_bind->buffer, tsdb_bind->buffer_length);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_from_sql_c_double_to_tsdb_float(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  param_array_t        *param_array       = param_state->param_array;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(float);
  r = mem_keep(&param_array->mem, sizeof(char) * tsdb_bind->buffer_length * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_array->mem.base;
  OD("i_param/buffer/buffer_length:%d/%p/%zd", param_state->i_param, tsdb_bind->buffer, tsdb_bind->buffer_length);

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_int(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType = APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_SLONG:
      tsdb_bind->buffer_type = tsdb_field->type;
      tsdb_bind->length = NULL;
      tsdb_bind->buffer_length = APD_record->DESC_OCTET_LENGTH;
      tsdb_bind->buffer = APD_record->DESC_DATA_PTR;
      break;
    case SQL_C_SBIGINT:
      return _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_int(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sql_c_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_smallint(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;

  SQLSMALLINT ValueType = APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_SBIGINT:
      return _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_smallint(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sql_c_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_tinyint(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;

  SQLSMALLINT ValueType = APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_SBIGINT:
      return _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_tinyint(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sql_c_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_bool(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;

  SQLSMALLINT ValueType = APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_SBIGINT:
      return _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_bool(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sql_c_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_float(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;

  SQLSMALLINT ValueType = APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_DOUBLE:
      return _stmt_prepare_param_data_array_from_sql_c_double_to_tsdb_float(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sql_c_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  int i_param                             = param_state->i_param;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  param_array_t        *param_array       = param_state->param_array;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  if (i_param == 0 && stmt->tsdb_meta.is_insert_stmt && stmt->tsdb_meta.subtbl_required) {
    return SQL_SUCCESS;
  }

  int r = 0;

  param_array->conv = NULL;
  r = mem_keep(&param_array->mem_is_null, sizeof(char)*nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->is_null = (char*)param_array->mem_is_null.base;
  tsdb_bind->num = nr_paramset_size;

  switch (tsdb_field->type) {
    case TSDB_DATA_TYPE_VARCHAR:
      return _stmt_prepare_param_data_array_by_tsdb_varchar(stmt, param_state);
    case TSDB_DATA_TYPE_TIMESTAMP:
      return _stmt_prepare_param_data_array_by_tsdb_timestamp(stmt, param_state);
    case TSDB_DATA_TYPE_NCHAR:
      return _stmt_prepare_param_data_array_by_tsdb_nchar(stmt, param_state);
    case TSDB_DATA_TYPE_BIGINT:
      return _stmt_prepare_param_data_array_by_tsdb_bigint(stmt, param_state);
    case TSDB_DATA_TYPE_NULL:
      OA_NIY(stmt->tsdb_meta.is_insert_stmt);
      OA_NIY(stmt->tsdb_meta.subtbl_required);
      OA_NIY(param_state->i_param == 0);
      return _stmt_prepare_param_data_array_by_tsdb_varchar(stmt, param_state);
    case TSDB_DATA_TYPE_DOUBLE:
      return _stmt_prepare_param_data_array_by_tsdb_double(stmt, param_state);
    case TSDB_DATA_TYPE_INT:
      return _stmt_prepare_param_data_array_by_tsdb_int(stmt, param_state);
    case TSDB_DATA_TYPE_FLOAT:
      return _stmt_prepare_param_data_array_by_tsdb_float(stmt, param_state);
    case TSDB_DATA_TYPE_SMALLINT:
      return _stmt_prepare_param_data_array_by_tsdb_smallint(stmt, param_state);
    case TSDB_DATA_TYPE_TINYINT:
      return _stmt_prepare_param_data_array_by_tsdb_tinyint(stmt, param_state);
    case TSDB_DATA_TYPE_BOOL:
      return _stmt_prepare_param_data_array_by_tsdb_bool(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, taos_data_type(tsdb_field->type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_sbigint_to_tsdb_timestamp(stmt_t *stmt, param_state_t *param_state, int64_t i64)
{
  (void)stmt;
  int i_row                               = param_state->i_row;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;
  _dump_TAOS_FIELD_E(tsdb_field);
  int64_t *tsdb_timestamp = (int64_t*)tsdb_bind->buffer;
  tsdb_timestamp[i_row] = i64;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_sbigint_to_tsdb_bigint(stmt_t *stmt, param_state_t *param_state, int64_t i64)
{
  (void)stmt;
  int i_row                               = param_state->i_row;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;
  _dump_TAOS_FIELD_E(tsdb_field);

  if (APD_record->DESC_DATA_PTR == tsdb_bind->buffer) return SQL_SUCCESS;

  int64_t *tsdb_bigint = (int64_t*)tsdb_bind->buffer;
  tsdb_bigint[i_row] = i64;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_sbigint_to_tsdb_int(stmt_t *stmt, param_state_t *param_state, int64_t i64)
{
  (void)stmt;
  int i_row                               = param_state->i_row;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;
  _dump_TAOS_FIELD_E(tsdb_field);
  int32_t *tsdb_bigint = (int32_t*)tsdb_bind->buffer;
  tsdb_bigint[i_row] = (int32_t)i64;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_sbigint_to_tsdb_smallint(stmt_t *stmt, param_state_t *param_state, int64_t i64)
{
  (void)stmt;
  int i_row                               = param_state->i_row;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;
  _dump_TAOS_FIELD_E(tsdb_field);
  int16_t *tsdb_bigint = (int16_t*)tsdb_bind->buffer;
  tsdb_bigint[i_row] = (int16_t)i64;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_sbigint_to_tsdb_tinyint(stmt_t *stmt, param_state_t *param_state, int64_t i64)
{
  (void)stmt;
  int i_row                               = param_state->i_row;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;
  _dump_TAOS_FIELD_E(tsdb_field);
  int8_t *tsdb_bigint = (int8_t*)tsdb_bind->buffer;
  tsdb_bigint[i_row] = (int8_t)i64;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_sbigint_to_tsdb_bool(stmt_t *stmt, param_state_t *param_state, int64_t i64)
{
  (void)stmt;
  int i_row                               = param_state->i_row;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;
  _dump_TAOS_FIELD_E(tsdb_field);
  int8_t *tsdb_bigint = (int8_t*)tsdb_bind->buffer;
  tsdb_bigint[i_row] = (int8_t)i64;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_sbigint(stmt_t *stmt, param_state_t *param_state, int64_t i64)
{
  (void)i64;
  int i_row                               = param_state->i_row;
  int i_param                             = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  switch (tsdb_field->type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      return _stmt_conv_param_data_from_sql_c_sbigint_to_tsdb_timestamp(stmt, param_state, i64);
    case TSDB_DATA_TYPE_BIGINT:
      return _stmt_conv_param_data_from_sql_c_sbigint_to_tsdb_bigint(stmt, param_state, i64);
    case TSDB_DATA_TYPE_INT:
      return _stmt_conv_param_data_from_sql_c_sbigint_to_tsdb_int(stmt, param_state, i64);
    case TSDB_DATA_TYPE_SMALLINT:
      return _stmt_conv_param_data_from_sql_c_sbigint_to_tsdb_smallint(stmt, param_state, i64);
    case TSDB_DATA_TYPE_TINYINT:
      return _stmt_conv_param_data_from_sql_c_sbigint_to_tsdb_tinyint(stmt, param_state, i64);
    case TSDB_DATA_TYPE_BOOL:
      return _stmt_conv_param_data_from_sql_c_sbigint_to_tsdb_bool(stmt, param_state, i64);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sql_c_data_type(APD_record->DESC_CONCISE_TYPE), taos_data_type(tsdb_field->type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_double_to_tsdb_timestamp(stmt_t *stmt, param_state_t *param_state, double d)
{
  (void)stmt;

  int i_row                               = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int64_t *tsdb_timestamp = (int64_t*)tsdb_bind->buffer;
  tsdb_timestamp[i_row] = (int64_t)d;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_double_to_tsdb_double(stmt_t *stmt, param_state_t *param_state, double d)
{
  (void)stmt;

  int i_row                               = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  double *tsdb_double = (double*)tsdb_bind->buffer;
  tsdb_double[i_row] = d;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_double_to_tsdb_float(stmt_t *stmt, param_state_t *param_state, double d)
{
  (void)stmt;

  int i_row                               = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  float *tsdb_float = (float*)tsdb_bind->buffer;
  tsdb_float[i_row] = (float)d;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_slong_to_tsdb_int(stmt_t *stmt, param_state_t *param_state, int32_t i32)
{
  (void)stmt;

  int i_row                               = param_state->i_row;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  if (APD_record->DESC_DATA_PTR == tsdb_bind->buffer) return SQL_SUCCESS;

  int32_t *tsdb_int32 = (int32_t*)tsdb_bind->buffer;
  tsdb_int32[i_row] = i32;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_double(stmt_t *stmt, param_state_t *param_state, double d)
{
  int i_row                               = param_state->i_row;
  int i_param                             = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  switch (tsdb_field->type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      return _stmt_conv_param_data_from_sql_c_double_to_tsdb_timestamp(stmt, param_state, d);
    case TSDB_DATA_TYPE_DOUBLE:
      return _stmt_conv_param_data_from_sql_c_double_to_tsdb_double(stmt, param_state, d);
    case TSDB_DATA_TYPE_FLOAT:
      return _stmt_conv_param_data_from_sql_c_double_to_tsdb_float(stmt, param_state, d);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sql_c_data_type(APD_record->DESC_CONCISE_TYPE), taos_data_type(tsdb_field->type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_slong(stmt_t *stmt, param_state_t *param_state, int32_t i32)
{
  int i_row                               = param_state->i_row;
  int i_param                             = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  switch (tsdb_field->type) {
    case TSDB_DATA_TYPE_INT:
      return _stmt_conv_param_data_from_sql_c_slong_to_tsdb_int(stmt, param_state, i32);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sql_c_data_type(APD_record->DESC_CONCISE_TYPE), taos_data_type(tsdb_field->type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_sql_c_char_to_tsdb_varchar(stmt_t *stmt, param_state_t *param_state, charset_conv_t *cnv,
    const char *src, size_t srclen, char **dst, size_t *dstlen)
{
  (void)param_state;

  const size_t   inbytes             = srclen;
  const size_t   outbytes            = *dstlen;

  char          *inbuf               = (char*)src;
  size_t         inbytesleft         = inbytes;

  size_t n = CALL_iconv(cnv->cnv, &inbuf, &inbytesleft, dst, dstlen);
  int e = errno;
  iconv(cnv->cnv, NULL, NULL, NULL, NULL);
  if (n == (size_t)-1) {
    if (e != E2BIG) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:[iconv]Character set conversion for `%s` to `%s` failed:[%d]%s",
          cnv->from, cnv->to, e, strerror(e));
      return SQL_ERROR;
    }
  }
  if (inbytesleft) {
    stmt_append_err_format(stmt, "01004", 0,
        "String data, right truncated:[iconv]Character set conversion for `%s` to `%s`, #%zd out of #%zd bytes consumed, #%zd out of #%zd bytes converted:[%d]%s",
        cnv->from, cnv->to, inbytes - inbytesleft, inbytes, outbytes - *dstlen, outbytes, e, strerror(e));
    return SQL_SUCCESS_WITH_INFO;
  }
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_char_to_tsdb_varchar(stmt_t *stmt, param_state_t *param_state, const char *s, SQLLEN len)
{
  SQLRETURN sr = SQL_SUCCESS;

  int i_row                               = param_state->i_row;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  _dump_TAOS_FIELD_E(tsdb_field);
  charset_conv_t *cnv = &stmt->conn->cnv_sql_c_char_to_tsdb_varchar;
  if (!cnv->cnv) {
    if (tsdb_bind->length) {
      tsdb_bind->length[i_row] = (int32_t)len;
    }
    OA_NIY(0);
  } else {
    char *tsdb_varchar = tsdb_bind->buffer;
    OD("i_param/buffer/buffer_length:%d/%p/%zd", param_state->i_param, tsdb_bind->buffer, tsdb_bind->buffer_length);
    tsdb_varchar += i_row * tsdb_bind->buffer_length;
    size_t tsdb_varchar_len = tsdb_bind->buffer_length;

    size_t         srclen              = (len == SQL_NTS) ? strlen(s) : (size_t)len;
    char          *dst                 = tsdb_varchar;
    size_t         dstlen              = tsdb_varchar_len;

    sr = _stmt_conv_sql_c_char_to_tsdb_varchar(stmt, param_state, cnv, s, srclen, &dst, &dstlen);
    if (sr == SQL_ERROR) return SQL_ERROR;
    if (tsdb_bind->length) {
      tsdb_bind->length[i_row] = (int32_t)(tsdb_varchar_len - dstlen);
    }
    return sr;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_char_to_tsdb_nchar(stmt_t *stmt, param_state_t *param_state, const char *s, SQLLEN len)
{
  return _stmt_conv_param_data_from_sql_c_char_to_tsdb_varchar(stmt, param_state, s, len);
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_char_to_tsdb_timestamp(stmt_t *stmt, param_state_t *param_state, const char *s, SQLLEN len)
{
  SQLRETURN sr = SQL_SUCCESS;

  int i_row                               = param_state->i_row;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  char *tsdb_timestamp = tsdb_bind->buffer;
  tsdb_timestamp += i_row * tsdb_bind->buffer_length;

  const char    *src                 = s;
  size_t         srclen              = (len == SQL_NTS) ? strlen(s) : (size_t)len;

  _dump_TAOS_FIELD_E(tsdb_field);
  sr = _stmt_conv_sql_c_char_to_tsdb_timestamp_x(stmt, src, srclen, tsdb_field->precision, (int64_t*)tsdb_timestamp);
  return sr;
}

static SQLRETURN _stmt_conv_param_data_from_sql_c_char(stmt_t *stmt, param_state_t *param_state, const char *s, SQLLEN len)
{
  int i_row                               = param_state->i_row;
  int i_param                             = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  switch (tsdb_field->type) {
    case TSDB_DATA_TYPE_VARCHAR:
      return _stmt_conv_param_data_from_sql_c_char_to_tsdb_varchar(stmt, param_state, s, len);
    case TSDB_DATA_TYPE_NCHAR:
      return _stmt_conv_param_data_from_sql_c_char_to_tsdb_nchar(stmt, param_state, s, len);
    case TSDB_DATA_TYPE_TIMESTAMP:
      return _stmt_conv_param_data_from_sql_c_char_to_tsdb_timestamp(stmt, param_state, s, len);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sql_c_data_type(APD_record->DESC_CONCISE_TYPE), taos_data_type(tsdb_field->type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data(stmt_t *stmt, param_state_t *param_state)
{
  int                        i_row                      = param_state->i_row;
  int                        i_param                    = param_state->i_param;
  desc_record_t             *APD_record                 = param_state->APD_record;

  TAOS_MULTI_BIND           *tsdb_bind                  = param_state->tsdb_bind;

  char *sql_c_base;
  SQLLEN sql_c_length;
  int sql_c_is_null;
  _stmt_param_get(stmt, i_row, i_param, &sql_c_base, &sql_c_length, &sql_c_is_null);
  if (sql_c_is_null) {
    if (!tsdb_bind->is_null) {
      stmt_append_err_format(stmt, "22002", 0,
          "Indicator variable required but not supplied:Parameter(#%d,#%d)[%s] is null",
          i_row + 1, i_param + 1, sql_c_data_type(APD_record->DESC_CONCISE_TYPE));
      return SQL_ERROR;
    }
    tsdb_bind->is_null[i_row] = 1;
    return SQL_SUCCESS;
  } else {
    if (tsdb_bind->is_null) tsdb_bind->is_null[i_row] = 0;
  }

  SQLSMALLINT ValueType = APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_SBIGINT: {
      int64_t i64 = *(int64_t*)sql_c_base;
      return _stmt_conv_param_data_from_sql_c_sbigint(stmt, param_state, i64);
    } break;
    case SQL_C_CHAR: {
      const char *s = sql_c_base;
      SQLLEN len = sql_c_length;
      return _stmt_conv_param_data_from_sql_c_char(stmt, param_state, s, len);
    } break;
    case SQL_C_DOUBLE: {
      double d = *(double*)sql_c_base;
      return _stmt_conv_param_data_from_sql_c_double(stmt, param_state, d);
    } break;
    case SQL_C_SLONG: {
      int32_t i32 = *(int32_t*)sql_c_base;
      return _stmt_conv_param_data_from_sql_c_slong(stmt, param_state, i32);
    } break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter(#%d,#%d)[%s] not implemented yet",
          i_row + 1, i_param + 1, sql_c_data_type(APD_record->DESC_CONCISE_TYPE));
      return SQL_ERROR;
  }

  if (tsdb_bind->length && APD_record->DESC_OCTET_LENGTH_PTR) {
    tsdb_bind->length[i_row] = (int32_t)*APD_record->DESC_OCTET_LENGTH_PTR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_execute(stmt_t *stmt)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  // NOTE: no need to check whether it's prepared or not, DM would have already checked

  descriptor_t *APD = _stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;

  descriptor_t *IPD = _stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;

  OA_NIY(APD_header->DESC_COUNT == IPD_header->DESC_COUNT);

  const SQLSMALLINT n = _stmt_get_count_of_tsdb_params(stmt);
  if (APD_header->DESC_COUNT < n) {
    stmt_append_err_format(stmt, "07002", 0,
        "COUNT field incorrect:%d parameter markers required, but only %d parameters bound",
        n, APD_header->DESC_COUNT);
    return SQL_ERROR;
  }
  if (APD_header->DESC_COUNT > n) {
    OW("bind more parameters (#%d) than required (#%d) by sql-statement", APD_header->DESC_COUNT, n);
  }

  r = _tsdb_binds_malloc_tsdb_binds(&stmt->tsdb_binds, n);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  r = _param_set_calloc(&stmt->paramset, n);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  for (int i=0; i<n; ++i) {
    if (i >= APD_header->DESC_COUNT) break;
    SQLUSMALLINT ParameterNumber = i + 1;
    sr = _stmt_param_check_and_prepare(stmt, ParameterNumber);
    if (sr == SQL_ERROR) return SQL_ERROR;
  }

  sr = _stmt_pre_exec(stmt);
  if (!sql_succeeded(sr)) return sr;

  r = CALL_taos_stmt_execute(stmt->stmt);
  if (r) {
    stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
    return SQL_ERROR;
  }

  rs_t *rs = &stmt->rs;

  rs->res = CALL_taos_stmt_use_result(stmt->stmt);
  rs->res_is_from_taos_query = 0;

  return _stmt_post_exec(stmt, rs);
}

static SQLRETURN _stmt_execute_prepare_caches(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;

  descriptor_t *APD = _stmt_APD(stmt);

  descriptor_t *IPD = _stmt_IPD(stmt);

  for (int j=0; j<param_state->nr_tsdb_fields; ++j) {
    param_state->i_param                        = j;
    param_state->APD_record = APD->records + j;
    param_state->IPD_record = IPD->records + j;
    param_state->tsdb_field = _stmt_get_tsdb_field_by_tsdb_meta(stmt, j);
    param_state->param_array = stmt->paramset.params + j;
    param_state->tsdb_bind = stmt->tsdb_binds.mbs + j;
    if (!stmt->tsdb_meta.is_insert_stmt) {
      sr = _stmt_guess_tsdb_meta(stmt, param_state);
      if (sr == SQL_ERROR) return SQL_ERROR;
    } else {
      sr = _stmt_check_tsdb_meta(stmt, param_state);
      if (sr == SQL_ERROR) return SQL_ERROR;
    }
    sr = _stmt_prepare_param_data_array(stmt, param_state);
    if (sr == SQL_ERROR) return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_execute_prepare_params(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;

  descriptor_t *APD = _stmt_APD(stmt);
  // desc_header_t *APD_header = &APD->header;

  descriptor_t *IPD = _stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;

  SQLUSMALLINT *param_status_ptr = IPD_header->DESC_ARRAY_STATUS_PTR;
  SQLULEN *params_processed_ptr = IPD_header->DESC_ROWS_PROCESSED_PTR;
  if (params_processed_ptr) *params_processed_ptr = 0;
  if (param_status_ptr) {
    for (int i=0; i<param_state->nr_paramset_size; ++i) {
      param_status_ptr[i] = SQL_PARAM_UNUSED;
    }
  }

  tsdb_meta_t *tsdb_meta = &stmt->tsdb_meta;
  int with_info = 0;
  for (int i=0; i<param_state->nr_paramset_size; ++i) {
    int row_with_info = 0;
    sr = SQL_SUCCESS;
    param_state->i_row = i;
    for (int j=0; j<param_state->nr_tsdb_fields; ++j) {
      param_state->i_param                        = j;
      param_state->APD_record = APD->records + j;
      param_state->IPD_record = IPD->records + j;
      param_state->tsdb_field = _stmt_get_tsdb_field_by_tsdb_meta(stmt, j);
      param_state->param_array = stmt->paramset.params + j;
      param_state->tsdb_bind = stmt->tsdb_binds.mbs + j;
      if (j == 0 && stmt->tsdb_meta.is_insert_stmt && stmt->tsdb_meta.subtbl_required) {
        continue;
      }
      sr = _stmt_conv_param_data(stmt, param_state);
      switch (sr) {
        case SQL_SUCCESS:
          break;
        case SQL_SUCCESS_WITH_INFO:
          row_with_info = 1;
          break;
        default:
          if (sr != SQL_ERROR) {
            stmt_append_err(stmt, "HY000", 0, "General error:internal logic error when processing paramset");
            sr = SQL_ERROR;
          }
          if (param_status_ptr) param_status_ptr[i] = SQL_PARAM_ERROR;
          if (params_processed_ptr) *params_processed_ptr += 1;
          if (i == 0) return SQL_ERROR;
          if (!param_status_ptr) return SQL_ERROR;
          return SQL_SUCCESS_WITH_INFO;
      }
      if (j == 0 && i == 0 && tsdb_meta->is_insert_stmt && tsdb_meta->subtbl_required) {
        OA_NIY(tsdb_meta->subtbl == NULL);
        OA_NIY(0);
      }
    }
    if (row_with_info) {
      with_info = 1;
      if (!param_status_ptr) return SQL_ERROR;
      param_status_ptr[i] = SQL_PARAM_SUCCESS_WITH_INFO;
    } else {
      if (param_status_ptr) param_status_ptr[i] = SQL_PARAM_SUCCESS;
    }
    if (params_processed_ptr) *params_processed_ptr += 1;
  }

  if (with_info) return SQL_SUCCESS_WITH_INFO;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_execute_x(stmt_t *stmt)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  // NOTE: no need to check whether it's prepared or not, DM would have already checked

  descriptor_t *APD = _stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;

  descriptor_t *IPD = _stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;

  OA_NIY(APD_header->DESC_COUNT == IPD_header->DESC_COUNT);

  const SQLSMALLINT n = _stmt_get_count_of_tsdb_params(stmt);
  if (APD_header->DESC_COUNT < n) {
    stmt_append_err_format(stmt, "07002", 0,
        "COUNT field incorrect:%d parameter markers required, but only %d parameters bound",
        n, APD_header->DESC_COUNT);
    return SQL_ERROR;
  }
  if (APD_header->DESC_COUNT > n) {
    OW("bind more parameters (#%d) than required (#%d) by sql-statement", APD_header->DESC_COUNT, n);
  }

  r = _tsdb_binds_malloc_tsdb_binds(&stmt->tsdb_binds, n);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  r = _param_set_calloc(&stmt->paramset, n);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  param_state_t param_state = {0};
  param_state.nr_paramset_size          = (int)APD_header->DESC_ARRAY_SIZE;
  param_state.nr_tsdb_fields            = n;

  sr = _stmt_execute_prepare_caches(stmt, &param_state);
  if (sr == SQL_ERROR) return SQL_ERROR;

  sr = _stmt_execute_prepare_params(stmt, &param_state);
  if (sr == SQL_ERROR) return SQL_ERROR;
  if (sr == SQL_SUCCESS_WITH_INFO) return SQL_ERROR; // FIXME:
  if (sr == SQL_SUCCESS_WITH_INFO) OA_NIY(0);
  if (sr != SQL_SUCCESS) OA_NIY(0);

  tsdb_meta_t *tsdb_meta = &stmt->tsdb_meta;
  if (tsdb_meta->is_insert_stmt) {
    if (tsdb_meta->subtbl_required) {
      desc_record_t *APD_record = APD->records;
      const char *base = APD_record->DESC_DATA_PTR;
      size_t len = SQL_NTS;
      if (APD_record->DESC_OCTET_LENGTH_PTR == 0) len = *APD_record->DESC_OCTET_LENGTH_PTR;
      if (len == (size_t)SQL_NTS) len = strlen(base);

      TOD_SAFE_FREE(tsdb_meta->subtbl);
      tsdb_meta->subtbl = strndup(base, len);
      if (!tsdb_meta->subtbl) {
        stmt_oom(stmt);
        return SQL_ERROR;
      }

      r = CALL_taos_stmt_set_tbname(stmt->stmt, tsdb_meta->subtbl);
      if (r) {
        stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
        return SQL_ERROR;
      }
    }
    if (tsdb_meta->nr_tag_fields) {
      TAOS_MULTI_BIND *mbs = stmt->tsdb_binds.mbs + (!!stmt->tsdb_meta.subtbl_required);
      r = CALL_taos_stmt_set_tags(stmt->stmt, mbs);
      if (r) {
        stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
        return SQL_ERROR;
      }
    }
  }

  r = CALL_taos_stmt_bind_param_batch(stmt->stmt, stmt->tsdb_binds.mbs + (!!stmt->tsdb_meta.subtbl_required) + stmt->tsdb_meta.nr_tag_fields);
  if (r) {
    stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
    return SQL_ERROR;
  }

  r = CALL_taos_stmt_add_batch(stmt->stmt);
  if (r) {
    stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
    return SQL_ERROR;
  }

  r = CALL_taos_stmt_execute(stmt->stmt);
  if (r) {
    stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->stmt));
    return SQL_ERROR;
  }

  rs_t *rs = &stmt->rs;

  rs->res = CALL_taos_stmt_use_result(stmt->stmt);
  rs->res_is_from_taos_query = 0;

  return _stmt_post_exec(stmt, rs);
}

SQLRETURN stmt_execute(stmt_t *stmt)
{
  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->taos);
  OA_ILE(stmt->stmt);
  OA(stmt->prepared, "seems like DM failed to secure function sequence correctly: SQLExecute before SQLPrepare?");

  // column-binds remain valid among executes
  _stmt_close_cursor(stmt);

  if (1) {
    return _stmt_execute_x(stmt);
  } else {
    return _stmt_execute(stmt);
  }
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

  _param_set_reset(&stmt->paramset);
}

static SQLRETURN _stmt_set_cursor_type(stmt_t *stmt, SQLULEN cursor_type)
{
  switch (cursor_type) {
    case SQL_CURSOR_FORWARD_ONLY:
    case SQL_CURSOR_STATIC:
      return SQL_SUCCESS;
    default:
      OE("General error:`%s` for `SQL_ATTR_CURSOR_TYPE` not supported yet", sql_cursor_type(cursor_type));
      stmt_append_err_format(stmt, "HY000", 0, "General error:`%s` for `SQL_ATTR_CURSOR_TYPE` not supported yet", sql_cursor_type(cursor_type));
      OA_NIY(0);
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
      OE("General error:`%s[0x%x/%d]` not supported yet", sql_stmt_attr(Attribute), Attribute, Attribute);
      stmt_append_err_format(stmt, "HY000", 0, "General error:`%s[0x%x/%d]` not supported yet", sql_stmt_attr(Attribute), Attribute, Attribute);
      OA_NIY(0);
      return SQL_ERROR;
  }
}

SQLRETURN stmt_get_attr(stmt_t *stmt,
           SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
  (void)Value;
  (void)BufferLength;
  (void)StringLength;
  switch (Attribute) {
    case SQL_ATTR_APP_ROW_DESC:
      *(SQLHANDLE*)Value = (SQLHANDLE)(stmt->current_ARD);
      return SQL_SUCCESS;
    case SQL_ATTR_APP_PARAM_DESC:
      *(SQLHANDLE*)Value = (SQLHANDLE)(stmt->current_APD);
      return SQL_SUCCESS;
    case SQL_ATTR_IMP_ROW_DESC:
      *(SQLHANDLE*)Value = (SQLHANDLE)(&stmt->IRD);
      return SQL_SUCCESS;
    case SQL_ATTR_IMP_PARAM_DESC:
      *(SQLHANDLE*)Value = (SQLHANDLE)(&stmt->IPD);
      return SQL_SUCCESS;
    default:
      OE("General error:`%s[0x%x/%d]` not supported yet", sql_stmt_attr(Attribute), Attribute, Attribute);
      stmt_append_err_format(stmt, "HY000", 0, "General error:`%s[0x%x/%d]` not supported yet", sql_stmt_attr(Attribute), Attribute, Attribute);
      OA_NIY(0);
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

  if (NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen(catalog);
  if (NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen(schema);
  if (NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen(table);
  if (NameLength4 == SQL_NTS) NameLength4 = (SQLSMALLINT)strlen(type);

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
    sr = stmt_exec_direct(stmt, sql, (int)strlen(sql));
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

    if (1) {
      // TODO: Catalog/Schema/TableName/TableType
      // https://github.com/taosdata/TDengine/issues/17890
      if (!*catalog) catalog = "%";
      if (!*table) table = "%";
      NameLength1 = (SQLSMALLINT)strlen(catalog);
      NameLength3 = (SQLSMALLINT)strlen(table);
      int is_table  = 0;
      int is_stable = 0;
      r = table_type_parse(type, &is_table, &is_stable);
      if (r) {
        stmt_append_err_format(stmt, "HY000", 0, "General error:invalid `table_type:[%.*s]`", (int)NameLength4, type);
        return SQL_ERROR;
      }
      buffer_t str = {0};
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
          r = buffer_concat(&str, sql);
          if (r) { stmt_oom(stmt); break; }
        } else if (is_stable) {
          sql =
            "select * from("
            " select db_name as TABLE_CAT, '' as TABLE_SCHEM, stable_name as TABLE_NAME,"
            " 'STABLE' as TABLE_TYPE, table_comment as REMARKS"
            " from information_schema.ins_stables"
            ") where 1 = 1";
          r = buffer_concat(&str, sql);
          if (r) { stmt_oom(stmt); break; }
        } else {
          sql =
            "select * from("
            " select db_name as TABLE_CAT, '' as TABLE_SCHEM, table_name as TABLE_NAME,"
            " 'TABLE' as TABLE_TYPE, table_comment as REMARKS"
            " from information_schema.ins_tables"
            ") where 1 = 1";
          r = buffer_concat(&str, sql);
          if (r) { stmt_oom(stmt); break; }
        }
        if (*catalog) {
          r = buffer_concat_fmt(&str, " and table_cat like '");
          if (r) { stmt_oom(stmt); break; }
          r = buffer_concat_replacement_n(&str, catalog, NameLength1);
          if (r) { stmt_oom(stmt); break; }
          r = buffer_concat_n(&str, "'", 1);
          if (r) { stmt_oom(stmt); break; }
        }

        r = buffer_concat_fmt(&str, " order by table_type, table_cat, table_schem, table_name");
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
        sr = stmt_exec_direct(stmt, sql, (int)str.nr);
        if (sr == SQL_ERROR) return SQL_ERROR;

        if (*table) {
          stmt->post_filter.ctx                   = wild;
          stmt->post_filter.post_filter           = _wild_post_filter;
          stmt->post_filter.post_filter_destroy   = _wild_post_filter_destroy;
        }
      } while (0);
      buffer_release(&str);
      if (r) return SQL_ERROR;
      return sr;
    }
  }

  stmt_append_err_format(stmt, "HY000", 0, "General error:`sql:[%s]` not supported yet", sql);
  return SQL_ERROR;
}

static SQLRETURN _stmt_get_diag_field_row_number(
    stmt_t         *stmt,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  (void)DiagIdentifier;
  (void)BufferLength;
  (void)StringLengthPtr;

  if (RecNumber!=1) OA_NIY(0);

  rowset_t *rowset = &stmt->rowset;

  switch (stmt->get_or_put_or_undef) {
    case 0x1: // get
      // FIXME: experimental
      *(SQLLEN*)DiagInfoPtr = (SQLLEN)rowset->i_row + 1;
      return SQL_SUCCESS;
    case 0x2: // put
      OA_NIY(0);
      return SQL_ERROR;
    default: // undef
      OA_NIY(0);
      return SQL_ERROR;
  }
}

SQLRETURN stmt_get_diag_field(
    stmt_t         *stmt,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  switch (DiagIdentifier) {
    case SQL_DIAG_SQLSTATE:
      return errs_get_diag_field_sqlstate(&stmt->errs, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_DIAG_ROW_NUMBER:
      return _stmt_get_diag_field_row_number(stmt, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    default:
      OA(0, "RecNumber:[%d]; DiagIdentifier:[%d]%s", RecNumber, DiagIdentifier, sql_diag_identifier(DiagIdentifier));
      return SQL_ERROR;
  }
}

