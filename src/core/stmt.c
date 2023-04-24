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

#include "charset.h"
#include "columns.h"
#include "conn.h"
#include "desc.h"
#include "errs.h"
#include "log.h"
#include "conn_parser.h"
#include "ext_parser.h"
#include "sqls_parser.h"
#include "primarykeys.h"
#include "stmt.h"
#include "tables.h"
#include "taos_helpers.h"
#include "tls.h"
#include "topic.h"
#include "tsdb.h"
#include "typesinfo.h"

#include <errno.h>
#include <limits.h>
#include <math.h>
#include <string.h>
#include <time.h>
#include <wchar.h>

void sqls_reset(sqls_t *sqls)
{
  if (!sqls) return;
  sqls->nr     = 0;
  sqls->pos    = 0;
  sqls->failed = 0;
}

void sqls_release(sqls_t *sqls)
{
  if (!sqls) return;
  sqls_reset(sqls);
  TOD_SAFE_FREE(sqls->sqls);
  sqls->cap = 0;
}

static void _param_state_reset(param_state_t *param_state)
{
  if (!param_state) return;
  mem_reset(&param_state->sqlc_to_sql);
  mem_reset(&param_state->sql_to_tsdb);

  param_state->nr_paramset_size = 0;
  param_state->i_batch_offset   = 0;

  param_state->nr_tsdb_fields   = 0;

  param_state->i_row            = 0;
  param_state->i_param          = 0;
  param_state->APD_record       = NULL;
  param_state->IPD_record       = NULL;

  param_state->tsdb_field       = NULL;

  param_state->param_column     = NULL;
  param_state->tsdb_bind        = NULL;
}

static void _param_state_release(param_state_t *param_state)
{
  if (!param_state) return;
  _param_state_reset(param_state);
  mem_release(&param_state->sqlc_to_sql);
  mem_release(&param_state->sql_to_tsdb);
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

  INIT_TOD_LIST_HEAD(&stmt->associated_APD_node);
  INIT_TOD_LIST_HEAD(&stmt->associated_ARD_node);
}

static void _stmt_init(stmt_t *stmt, conn_t *conn)
{
  stmt->conn = conn_ref(conn);
  int prev = atomic_fetch_add(&conn->stmts, 1);
  OA_ILE(prev >= 0);

  stmt->strict = 1;

  errs_init(&stmt->errs);
  stmt->errs.connected_conn = conn;

  _stmt_init_descriptors(stmt);

  tsdb_stmt_init(&stmt->tsdb_stmt, stmt);
  tables_init(&stmt->tables, stmt);
  columns_init(&stmt->columns, stmt);
  typesinfo_init(&stmt->typesinfo, stmt);
  primarykeys_init(&stmt->primarykeys, stmt);
  topic_init(&stmt->topic, stmt);

  stmt->base = &stmt->tsdb_stmt.base;

  stmt->refc = 1;
}

static void _sqlc_data_reset(sqlc_data_t *sqlc)
{
  if (!sqlc) return;
  mem_reset(&sqlc->mem);
  sqlc->is_null = 0;
}

static void _sqlc_data_release(sqlc_data_t *sqlc)
{
  if (!sqlc) return;
  mem_release(&sqlc->mem);
}

static void _get_data_ctx_reset(get_data_ctx_t *ctx)
{
  if (!ctx) return;
  _sqlc_data_reset(&ctx->sqlc);

  ctx->buf[0] = '\0';
  mem_reset(&ctx->mem);
  ctx->pos = NULL;
  ctx->nr  = 0;

  // NOTE: we don't support bookmark yet, thus `0` means unset yet
  ctx->Col_or_Param_Num = 0;
}

static void _get_data_ctx_release(get_data_ctx_t *ctx)
{
  if (!ctx) return;
  _get_data_ctx_reset(ctx);
  _sqlc_data_release(&ctx->sqlc);
}

descriptor_t* stmt_APD(stmt_t *stmt)
{
  return stmt->current_APD;
}

descriptor_t* stmt_IPD(stmt_t *stmt)
{
  return &stmt->IPD;
}

descriptor_t* stmt_IRD(stmt_t *stmt)
{
  return &stmt->IRD;
}

descriptor_t* stmt_ARD(stmt_t *stmt)
{
  return stmt->current_ARD;
}

static SQLULEN* _stmt_get_rows_fetched_ptr(stmt_t *stmt)
{
  descriptor_t *IRD = stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;
  return IRD_header->DESC_ROWS_PROCESSED_PTR;
}

static void _stmt_reset_result(stmt_t *stmt)
{
  _get_data_ctx_reset(&stmt->get_data_ctx);

  tsdb_res_reset(&stmt->tsdb_stmt.res);
  tables_reset(&stmt->tables);
  columns_reset(&stmt->columns);
  typesinfo_reset(&stmt->typesinfo);
  primarykeys_reset(&stmt->primarykeys);
  topic_reset(&stmt->topic);

  if (_stmt_get_rows_fetched_ptr(stmt)) *_stmt_get_rows_fetched_ptr(stmt) = 0;
}

static void _stmt_release_result(stmt_t *stmt)
{
  _get_data_ctx_release(&stmt->get_data_ctx);

  tsdb_res_release(&stmt->tsdb_stmt.res);
  tables_release(&stmt->tables);
  columns_release(&stmt->columns);
  typesinfo_release(&stmt->typesinfo);
  primarykeys_release(&stmt->primarykeys);
  topic_release(&stmt->topic);

  if (_stmt_get_rows_fetched_ptr(stmt)) *_stmt_get_rows_fetched_ptr(stmt) = 0;
}

static void _stmt_reset_tables(stmt_t *stmt)
{
  tables_reset(&stmt->tables);
}

static void _stmt_reset_columns(stmt_t *stmt)
{
  columns_reset(&stmt->columns);
}

static void _stmt_reset_typesinfo(stmt_t *stmt)
{
  typesinfo_reset(&stmt->typesinfo);
}

static void _stmt_reset_primarykeys(stmt_t *stmt)
{
  primarykeys_reset(&stmt->primarykeys);
}

static void _stmt_close_result(stmt_t *stmt)
{
  _stmt_reset_result(stmt);
  stmt->base = &stmt->tsdb_stmt.base;
}

static void _stmt_unbind_cols(stmt_t *stmt)
{
  descriptor_t *ARD = stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;
  ARD_header->DESC_COUNT = 0;
  memset(ARD->records, 0, sizeof(*ARD->records) * ARD->cap);
}

static void _stmt_reset_params(stmt_t *stmt)
{
  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;
  APD_header->DESC_COUNT = 0;

  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  IPD_header->DESC_COUNT = 0;

  tsdb_paramset_reset(&stmt->paramset);
}

static void _stmt_release_stmt(stmt_t *stmt)
{
  _stmt_close_result(stmt);
  tsdb_stmt_release(&stmt->tsdb_stmt);
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

static void _stmt_release_field_arrays(stmt_t *stmt)
{
  descriptor_t *APD = stmt_APD(stmt);
  descriptor_reclaim_buffers(APD);
}

// static void _tsdb_param_column_reset(tsdb_param_column_t *pa)
// {
//   mem_reset(&pa->mem);
// }

static void _stmt_release(stmt_t *stmt)
{
  _stmt_release_result(stmt);

  _stmt_release_field_arrays(stmt);

  stmt_dissociate_APD(stmt);

  _stmt_release_stmt(stmt);

  stmt_dissociate_ARD(stmt);

  _stmt_release_descriptors(stmt);

  mem_release(&stmt->raw);
  mem_release(&stmt->tsdb_sql);

  errs_release(&stmt->errs);
  mem_release(&stmt->mem);
  tsdb_paramset_release(&stmt->paramset);
  tsdb_binds_release(&stmt->tsdb_binds);
  sqls_release(&stmt->sqls);
  _param_state_release(&stmt->param_state);

  int prev = atomic_fetch_sub(&stmt->conn->stmts, 1);
  OA_ILE(prev >= 1);
  conn_unref(stmt->conn);
  stmt->conn = NULL;

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

static int _stmt_time_precision(stmt_t *stmt)
{
  int time_precision = 0;
  if (stmt->base == &stmt->tsdb_stmt.base) {
    time_precision = stmt->tsdb_stmt.res.time_precision;
  }
  return time_precision;
}

static SQLRETURN _stmt_col_DESC_TYPE(
    stmt_t               *stmt,
    const TAOS_FIELD     *col,
    SQLLEN               *NumericAttributePtr)
{
  static struct {
    int                 tsdb_type;
    int                 sql_type;
    int                 sql_promoted;
  } _maps[] = {
    {TSDB_DATA_TYPE_TIMESTAMP,              SQL_TYPE_TIMESTAMP,        SQL_DATETIME},
    {TSDB_DATA_TYPE_BOOL,                   SQL_BIT,                   SQL_BIT},
    {TSDB_DATA_TYPE_TINYINT,                SQL_TINYINT,               SQL_SMALLINT},
    {TSDB_DATA_TYPE_SMALLINT,               SQL_SMALLINT,              SQL_SMALLINT},
    {TSDB_DATA_TYPE_INT,                    SQL_INTEGER,               SQL_INTEGER},
    {TSDB_DATA_TYPE_BIGINT,                 SQL_BIGINT,                SQL_BIGINT},
    {TSDB_DATA_TYPE_FLOAT,                  SQL_REAL,                  SQL_REAL},
    {TSDB_DATA_TYPE_DOUBLE,                 SQL_DOUBLE,                SQL_DOUBLE},
    {TSDB_DATA_TYPE_VARCHAR,                SQL_VARCHAR,               SQL_VARCHAR},
    {TSDB_DATA_TYPE_NCHAR,                  SQL_WVARCHAR,              SQL_WVARCHAR},
    {TSDB_DATA_TYPE_UTINYINT,               SQL_TINYINT,               SQL_TINYINT},
    {TSDB_DATA_TYPE_USMALLINT,              SQL_SMALLINT,              SQL_INTEGER},
    {TSDB_DATA_TYPE_UINT,                   SQL_INTEGER,               SQL_BIGINT},
    {TSDB_DATA_TYPE_UBIGINT,                SQL_BIGINT,                SQL_BIGINT},
  };

  for (size_t i=0; i<sizeof(_maps)/sizeof(_maps[0]); ++i) {
    if (col->type != _maps[i].tsdb_type) continue;
    if (col->type == TSDB_DATA_TYPE_TIMESTAMP) {
      if (!stmt->conn->cfg.timestamp_as_is) {
        *NumericAttributePtr = SQL_WVARCHAR;
        return SQL_SUCCESS;
      }
    }
    if (stmt->conn->cfg.unsigned_promotion) {
      *NumericAttributePtr = _maps[i].sql_promoted;
    } else {
      *NumericAttributePtr = _maps[i].sql_type;
    }
    return SQL_SUCCESS;
  }

  return SQL_ERROR;
}

static SQLRETURN _stmt_col_DESC_CONCISE_TYPE(
    stmt_t               *stmt,
    const TAOS_FIELD     *col,
    SQLLEN               *NumericAttributePtr)
{
  static struct {
    int                 tsdb_type;
    int                 sql_type;
    int                 sql_promoted;
  } _maps[] = {
    {TSDB_DATA_TYPE_TIMESTAMP,              SQL_TYPE_TIMESTAMP,        SQL_TYPE_TIMESTAMP},
    {TSDB_DATA_TYPE_BOOL,                   SQL_BIT,                   SQL_BIT},
    {TSDB_DATA_TYPE_TINYINT,                SQL_TINYINT,               SQL_SMALLINT},
    {TSDB_DATA_TYPE_SMALLINT,               SQL_SMALLINT,              SQL_SMALLINT},
    {TSDB_DATA_TYPE_INT,                    SQL_INTEGER,               SQL_INTEGER},
    {TSDB_DATA_TYPE_BIGINT,                 SQL_BIGINT,                SQL_BIGINT},
    {TSDB_DATA_TYPE_FLOAT,                  SQL_REAL,                  SQL_REAL},
    {TSDB_DATA_TYPE_DOUBLE,                 SQL_DOUBLE,                SQL_DOUBLE},
    {TSDB_DATA_TYPE_VARCHAR,                SQL_VARCHAR,               SQL_VARCHAR},
    {TSDB_DATA_TYPE_NCHAR,                  SQL_WVARCHAR,              SQL_WVARCHAR},
    {TSDB_DATA_TYPE_UTINYINT,               SQL_TINYINT,               SQL_TINYINT},
    {TSDB_DATA_TYPE_USMALLINT,              SQL_SMALLINT,              SQL_INTEGER},
    {TSDB_DATA_TYPE_UINT,                   SQL_INTEGER,               SQL_BIGINT},
    {TSDB_DATA_TYPE_UBIGINT,                SQL_BIGINT,                SQL_BIGINT},
  };

  for (size_t i=0; i<sizeof(_maps)/sizeof(_maps[0]); ++i) {
    if (col->type != _maps[i].tsdb_type) continue;
    if (col->type == TSDB_DATA_TYPE_TIMESTAMP) {
      if (!stmt->conn->cfg.timestamp_as_is) {
        *NumericAttributePtr = SQL_WVARCHAR;
        return SQL_SUCCESS;
      }
    }
    if (stmt->conn->cfg.unsigned_promotion) {
      *NumericAttributePtr = _maps[i].sql_promoted;
    } else {
      *NumericAttributePtr = _maps[i].sql_type;
    }
    return SQL_SUCCESS;
  }

  return SQL_ERROR;
}

static SQLRETURN _stmt_col_DESC_OCTET_LENGTH(
    stmt_t               *stmt,
    const TAOS_FIELD     *col,
    SQLLEN               *NumericAttributePtr)
{
  int time_precision = _stmt_time_precision(stmt);

  static struct {
    int                 tsdb_type;
    int                 octet_length;
  } _maps[] = {
    {TSDB_DATA_TYPE_TIMESTAMP,              8},
    {TSDB_DATA_TYPE_BOOL,                   1},
    {TSDB_DATA_TYPE_TINYINT,                1},
    {TSDB_DATA_TYPE_SMALLINT,               2},
    {TSDB_DATA_TYPE_INT,                    4},
    {TSDB_DATA_TYPE_BIGINT,                 8},
    {TSDB_DATA_TYPE_FLOAT,                  4},
    {TSDB_DATA_TYPE_DOUBLE,                 8},
    {TSDB_DATA_TYPE_VARCHAR,                -1},
    {TSDB_DATA_TYPE_NCHAR,                  -2},
    {TSDB_DATA_TYPE_UTINYINT,               1},
    {TSDB_DATA_TYPE_USMALLINT,              2},
    {TSDB_DATA_TYPE_UINT,                   4},
    {TSDB_DATA_TYPE_UBIGINT,                8},
  };

  for (size_t i=0; i<sizeof(_maps)/sizeof(_maps[0]); ++i) {
    if (col->type != _maps[i].tsdb_type) continue;
    if (col->type == TSDB_DATA_TYPE_TIMESTAMP) {
      if (!stmt->conn->cfg.timestamp_as_is) {
        *NumericAttributePtr = (20 + (time_precision + 1) * 3) * 2;
        return SQL_SUCCESS;
      }
    }
    if (_maps[i].octet_length > 0 && _maps[i].octet_length != col->bytes) {
      stmt_append_err_format(stmt, "HY000", 0, "General error:octet length for `%s` is expected to be %d, but got ==%d==",
          taos_data_type(col->type), _maps[i].octet_length, col->bytes);
      return SQL_ERROR;
    }
    if (_maps[i].octet_length < 0) {
      *NumericAttributePtr = 0 - col->bytes * _maps[i].octet_length;
    } else {
      *NumericAttributePtr = _maps[i].octet_length;
    }
    return SQL_SUCCESS;
  }
  return SQL_ERROR;
}

static SQLRETURN _stmt_col_DESC_PRECISION(
    stmt_t               *stmt,
    const TAOS_FIELD     *col,
    SQLLEN               *NumericAttributePtr)
{
  int time_precision = _stmt_time_precision(stmt);

  static struct {
    int                 tsdb_type;
    int                 precision;
  } _maps[] = {
    {TSDB_DATA_TYPE_TIMESTAMP,              0},
    {TSDB_DATA_TYPE_BOOL,                   1},
    {TSDB_DATA_TYPE_TINYINT,                3},
    {TSDB_DATA_TYPE_SMALLINT,               5},
    {TSDB_DATA_TYPE_INT,                    10},
    {TSDB_DATA_TYPE_BIGINT,                 19},
    {TSDB_DATA_TYPE_FLOAT,                  24},
    {TSDB_DATA_TYPE_DOUBLE,                 53},
    {TSDB_DATA_TYPE_VARCHAR,                -1},
    {TSDB_DATA_TYPE_NCHAR,                  -1},
    {TSDB_DATA_TYPE_UTINYINT,               3},
    {TSDB_DATA_TYPE_USMALLINT,              5},
    {TSDB_DATA_TYPE_UINT,                   10},
    {TSDB_DATA_TYPE_UBIGINT,                20},
  };

  for (size_t i=0; i<sizeof(_maps)/sizeof(_maps[0]); ++i) {
    if (col->type != _maps[i].tsdb_type) continue;
    if (col->type == TSDB_DATA_TYPE_TIMESTAMP) {
      if (!stmt->conn->cfg.timestamp_as_is) {
        *NumericAttributePtr = 20 + (time_precision + 1) * 3;
      } else {
        *NumericAttributePtr = (time_precision + 1) * 3;
      }
      return SQL_SUCCESS;
    }
    if (_maps[i].precision == -1) {
      *NumericAttributePtr = col->bytes;
    } else {
      *NumericAttributePtr = _maps[i].precision;
    }
    return SQL_SUCCESS;
  }
  return SQL_ERROR;
}

static SQLRETURN _stmt_col_DESC_SCALE(
    stmt_t               *stmt,
    const TAOS_FIELD     *col,
    SQLLEN               *NumericAttributePtr)
{
  int time_precision = _stmt_time_precision(stmt);

  static struct {
    int                 tsdb_type;
    int                 scale;
  } _maps[] = {
    {TSDB_DATA_TYPE_TIMESTAMP,              0},
    {TSDB_DATA_TYPE_BOOL,                   0},
    {TSDB_DATA_TYPE_TINYINT,                0},
    {TSDB_DATA_TYPE_SMALLINT,               0},
    {TSDB_DATA_TYPE_INT,                    0},
    {TSDB_DATA_TYPE_BIGINT,                 0},
    {TSDB_DATA_TYPE_FLOAT,                  0},
    {TSDB_DATA_TYPE_DOUBLE,                 0},
    {TSDB_DATA_TYPE_VARCHAR,                0},
    {TSDB_DATA_TYPE_NCHAR,                  0},
    {TSDB_DATA_TYPE_UTINYINT,               0},
    {TSDB_DATA_TYPE_USMALLINT,              0},
    {TSDB_DATA_TYPE_UINT,                   0},
    {TSDB_DATA_TYPE_UBIGINT,                0},
  };

  for (size_t i=0; i<sizeof(_maps)/sizeof(_maps[0]); ++i) {
    if (col->type != _maps[i].tsdb_type) continue;
    if (col->type == TSDB_DATA_TYPE_TIMESTAMP) {
      if (!stmt->conn->cfg.timestamp_as_is) {
        *NumericAttributePtr = _maps[i].scale;
      } else {
        *NumericAttributePtr = (time_precision + 1) * 3;
      }
      return SQL_SUCCESS;
    }
    *NumericAttributePtr = _maps[i].scale;
    return SQL_SUCCESS;
  }
  return SQL_ERROR;
}

static SQLRETURN _stmt_col_DESC_DISPLAY_SIZE(
    stmt_t               *stmt,
    const TAOS_FIELD     *col,
    SQLLEN               *NumericAttributePtr)
{
  int time_precision = _stmt_time_precision(stmt);

  static struct {
    int                 tsdb_type;
    int                 display_size;
  } _maps[] = {
    {TSDB_DATA_TYPE_TIMESTAMP,              8},
    {TSDB_DATA_TYPE_BOOL,                   1},
    {TSDB_DATA_TYPE_TINYINT,                4},
    {TSDB_DATA_TYPE_SMALLINT,               6},
    {TSDB_DATA_TYPE_INT,                   11},
    {TSDB_DATA_TYPE_BIGINT,                20},
    {TSDB_DATA_TYPE_FLOAT,                 14},
    {TSDB_DATA_TYPE_DOUBLE,                24},
    {TSDB_DATA_TYPE_VARCHAR,                -1},
    {TSDB_DATA_TYPE_NCHAR,                  -2},
    {TSDB_DATA_TYPE_UTINYINT,               3},
    {TSDB_DATA_TYPE_USMALLINT,              5},
    {TSDB_DATA_TYPE_UINT,                  10},
    {TSDB_DATA_TYPE_UBIGINT,               20},
  };

  for (size_t i=0; i<sizeof(_maps)/sizeof(_maps[0]); ++i) {
    if (col->type != _maps[i].tsdb_type) continue;
    if (col->type == TSDB_DATA_TYPE_TIMESTAMP) {
      if (!stmt->conn->cfg.timestamp_as_is) {
        *NumericAttributePtr = 20 + (time_precision + 1) * 3;
        return SQL_SUCCESS;
      }
    }
    if (_maps[i].display_size < 0) {
      *NumericAttributePtr = 0 - col->bytes * _maps[i].display_size;
    } else {
      *NumericAttributePtr = _maps[i].display_size;
    }
    return SQL_SUCCESS;
  }
  return SQL_ERROR;
}

static SQLRETURN _stmt_col_DESC_SEARCHABLE(
    stmt_t               *stmt,
    const TAOS_FIELD     *col,
    SQLLEN               *NumericAttributePtr)
{
  int time_precision = _stmt_time_precision(stmt);

  static struct {
    int                 tsdb_type;
    int                 display_size;
  } _maps[] = {
    {TSDB_DATA_TYPE_TIMESTAMP,              SQL_SEARCHABLE},
    {TSDB_DATA_TYPE_BOOL,                   SQL_PRED_BASIC},
    {TSDB_DATA_TYPE_TINYINT,                SQL_PRED_BASIC},
    {TSDB_DATA_TYPE_SMALLINT,               SQL_PRED_BASIC},
    {TSDB_DATA_TYPE_INT,                    SQL_PRED_BASIC},
    {TSDB_DATA_TYPE_BIGINT,                 SQL_PRED_BASIC},
    {TSDB_DATA_TYPE_FLOAT,                  SQL_PRED_BASIC},
    {TSDB_DATA_TYPE_DOUBLE,                 SQL_PRED_BASIC},
    {TSDB_DATA_TYPE_VARCHAR,                SQL_SEARCHABLE},
    {TSDB_DATA_TYPE_NCHAR,                  SQL_SEARCHABLE},
    {TSDB_DATA_TYPE_UTINYINT,               SQL_PRED_BASIC},
    {TSDB_DATA_TYPE_USMALLINT,              SQL_PRED_BASIC},
    {TSDB_DATA_TYPE_UINT,                   SQL_PRED_BASIC},
    {TSDB_DATA_TYPE_UBIGINT,                SQL_PRED_BASIC},
  };

  for (size_t i=0; i<sizeof(_maps)/sizeof(_maps[0]); ++i) {
    if (col->type != _maps[i].tsdb_type) continue;
    if (col->type == TSDB_DATA_TYPE_TIMESTAMP) {
      if (!stmt->conn->cfg.timestamp_as_is) {
        *NumericAttributePtr = 20 + (time_precision + 1) * 3;
        return SQL_SUCCESS;
      }
    }
    if (_maps[i].display_size < 0) {
      *NumericAttributePtr = -col->bytes;
    } else {
      *NumericAttributePtr = _maps[i].display_size;
    }
    return SQL_SUCCESS;
  }
  return SQL_ERROR;
}

static SQLRETURN _stmt_col_DESC_NAME(
    stmt_t               *stmt,
    const TAOS_FIELD     *col,
    SQLPOINTER            CharacterAttributePtr,
    SQLSMALLINT           BufferLength,
    SQLSMALLINT          *StringLengthPtr)
{
  int n;
  n = snprintf(CharacterAttributePtr, BufferLength, "%.*s", (int)sizeof(col->name), col->name);
  if (n < 0) {
    int e = errno;
    stmt_append_err_format(stmt, "HY000", 0, "General error:internal logic error:[%d]%s", e, strerror(e));
    return SQL_ERROR;
  }
  if (StringLengthPtr) *StringLengthPtr = n;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_col_DESC_TYPE_NAME(
    stmt_t               *stmt,
    const TAOS_FIELD     *col,
    SQLPOINTER            CharacterAttributePtr,
    SQLSMALLINT           BufferLength,
    SQLSMALLINT          *StringLengthPtr)
{
  static struct {
    int                 tsdb_type;
    const char         *name;
  } _maps[] = {
    {TSDB_DATA_TYPE_TIMESTAMP,              "TIMESTAMP"},
    {TSDB_DATA_TYPE_BOOL,                   "BOOL"},
    {TSDB_DATA_TYPE_TINYINT,                "TINYINT"},
    {TSDB_DATA_TYPE_SMALLINT,               "SMALLINT"},
    {TSDB_DATA_TYPE_INT,                    "INT"},
    {TSDB_DATA_TYPE_BIGINT,                 "BIGINT"},
    {TSDB_DATA_TYPE_FLOAT,                  "FLOAT"},
    {TSDB_DATA_TYPE_DOUBLE,                 "DOUBLE"},
    {TSDB_DATA_TYPE_VARCHAR,                "VARCHAR"},
    {TSDB_DATA_TYPE_NCHAR,                  "NCHAR"},
    {TSDB_DATA_TYPE_UTINYINT,               "TINYINT UNSIGNED"},
    {TSDB_DATA_TYPE_USMALLINT,              "SMALLINT UNSIGNED"},
    {TSDB_DATA_TYPE_UINT,                   "INT UNSIGNED"},
    {TSDB_DATA_TYPE_UBIGINT,                "BIGINT UNSIGNED"},
  };

  for (size_t i=0; i<sizeof(_maps)/sizeof(_maps[0]); ++i) {
    if (col->type != _maps[i].tsdb_type) continue;
    const char *name = _maps[i].name;
    if (col->type == TSDB_DATA_TYPE_TIMESTAMP) {
      if (!stmt->conn->cfg.timestamp_as_is) {
        name = "NCHAR";
      }
    }
    int n = snprintf(CharacterAttributePtr, BufferLength, "%s", name);
    if (n < 0) {
      int e = errno;
      stmt_append_err_format(stmt, "HY000", 0, "General error:internal logic error:[%d]%s", e, strerror(e));
      return SQL_ERROR;
    }
    if (StringLengthPtr) *StringLengthPtr = n;
    return SQL_SUCCESS;
  }
  return SQL_ERROR;
}

static SQLRETURN _stmt_col_DESC_LENGTH(
    stmt_t               *stmt,
    const TAOS_FIELD     *col,
    SQLLEN               *NumericAttributePtr)
{
  int time_precision = _stmt_time_precision(stmt);

  static struct {
    int                 tsdb_type;
    int                 length;
  } _maps[] = {
    {TSDB_DATA_TYPE_TIMESTAMP,              0},
    {TSDB_DATA_TYPE_BOOL,                   1},
    {TSDB_DATA_TYPE_TINYINT,                3},
    {TSDB_DATA_TYPE_SMALLINT,               5},
    {TSDB_DATA_TYPE_INT,                    10},
    {TSDB_DATA_TYPE_BIGINT,                 19},
    {TSDB_DATA_TYPE_FLOAT,                  7},    // FIXME: not 24 as precision
    {TSDB_DATA_TYPE_DOUBLE,                 15},   // FIXME: not 53 as precision
    {TSDB_DATA_TYPE_VARCHAR,                -1},
    {TSDB_DATA_TYPE_NCHAR,                  -1},
    {TSDB_DATA_TYPE_UTINYINT,               3},
    {TSDB_DATA_TYPE_USMALLINT,              5},
    {TSDB_DATA_TYPE_UINT,                   10},
    {TSDB_DATA_TYPE_UBIGINT,                20},
  };

  for (size_t i=0; i<sizeof(_maps)/sizeof(_maps[0]); ++i) {
    if (col->type != _maps[i].tsdb_type) continue;
    if (col->type == TSDB_DATA_TYPE_TIMESTAMP) {
      *NumericAttributePtr = 20 + (time_precision + 1) * 3;
      return SQL_SUCCESS;
    }
    if (_maps[i].length == -1) {
      *NumericAttributePtr = col->bytes;
    } else {
      *NumericAttributePtr = _maps[i].length;
    }
    return SQL_SUCCESS;
  }
  return SQL_ERROR;
}

static SQLRETURN _stmt_col_DESC_NUM_PREC_RADIX(
    stmt_t               *stmt,
    const TAOS_FIELD     *col,
    SQLLEN               *NumericAttributePtr)
{
  (void)stmt;
  static struct {
    int                 tsdb_type;
    int                 radix;
  } _maps[] = {
    {TSDB_DATA_TYPE_TIMESTAMP,              10},
    {TSDB_DATA_TYPE_BOOL,                   10},
    {TSDB_DATA_TYPE_TINYINT,                10},
    {TSDB_DATA_TYPE_SMALLINT,               10},
    {TSDB_DATA_TYPE_INT,                    10},
    {TSDB_DATA_TYPE_BIGINT,                 10},
    {TSDB_DATA_TYPE_FLOAT,                  2},
    {TSDB_DATA_TYPE_DOUBLE,                 2},
    {TSDB_DATA_TYPE_VARCHAR,                10},
    {TSDB_DATA_TYPE_NCHAR,                  10},
    {TSDB_DATA_TYPE_UTINYINT,               10},
    {TSDB_DATA_TYPE_USMALLINT,              10},
    {TSDB_DATA_TYPE_UINT,                   10},
    {TSDB_DATA_TYPE_UBIGINT,                10},
  };

  for (size_t i=0; i<sizeof(_maps)/sizeof(_maps[0]); ++i) {
    if (col->type != _maps[i].tsdb_type) continue;
    *NumericAttributePtr = _maps[i].radix;
    return SQL_SUCCESS;
  }
  return SQL_ERROR;
}

static SQLRETURN _stmt_col_DESC_UNSIGNED(
    stmt_t               *stmt,
    const TAOS_FIELD     *col,
    SQLLEN               *NumericAttributePtr)
{
  (void)stmt;

  static struct {
    int                 tsdb_type;
    int                 unsigned_;
  } _maps[] = {
    {TSDB_DATA_TYPE_TIMESTAMP,              SQL_TRUE},
    {TSDB_DATA_TYPE_BOOL,                   SQL_TRUE},
    {TSDB_DATA_TYPE_TINYINT,                SQL_FALSE},
    {TSDB_DATA_TYPE_SMALLINT,               SQL_FALSE},
    {TSDB_DATA_TYPE_INT,                    SQL_FALSE},
    {TSDB_DATA_TYPE_BIGINT,                 SQL_FALSE},
    {TSDB_DATA_TYPE_FLOAT,                  SQL_FALSE},
    {TSDB_DATA_TYPE_DOUBLE,                 SQL_FALSE},
    {TSDB_DATA_TYPE_VARCHAR,                SQL_TRUE},
    {TSDB_DATA_TYPE_NCHAR,                  SQL_TRUE},
    {TSDB_DATA_TYPE_UTINYINT,               SQL_TRUE},
    {TSDB_DATA_TYPE_USMALLINT,              SQL_TRUE},
    {TSDB_DATA_TYPE_UINT,                   SQL_TRUE},
    {TSDB_DATA_TYPE_UBIGINT,                SQL_TRUE},
  };

  for (size_t i=0; i<sizeof(_maps)/sizeof(_maps[0]); ++i) {
    if (col->type != _maps[i].tsdb_type) continue;
    *NumericAttributePtr = _maps[i].unsigned_;
    return SQL_SUCCESS;
  }
  return SQL_ERROR;
}

static SQLRETURN _stmt_col_DESC_LITERAL_SUFFIX(
    stmt_t               *stmt,
    const TAOS_FIELD     *col,
    SQLPOINTER            CharacterAttributePtr,
    SQLSMALLINT           BufferLength,
    SQLSMALLINT          *StringLengthPtr)
{
  static struct {
    int                 tsdb_type;
    const char         *suffix;
  } _maps[] = {
    {TSDB_DATA_TYPE_TIMESTAMP,              "'"},
    {TSDB_DATA_TYPE_BOOL,                   ""},
    {TSDB_DATA_TYPE_TINYINT,                ""},
    {TSDB_DATA_TYPE_SMALLINT,               ""},
    {TSDB_DATA_TYPE_INT,                    ""},
    {TSDB_DATA_TYPE_BIGINT,                 ""},
    {TSDB_DATA_TYPE_FLOAT,                  ""},
    {TSDB_DATA_TYPE_DOUBLE,                 ""},
    {TSDB_DATA_TYPE_VARCHAR,                "'"},
    {TSDB_DATA_TYPE_NCHAR,                  "'"},
    {TSDB_DATA_TYPE_UTINYINT,               ""},
    {TSDB_DATA_TYPE_USMALLINT,              ""},
    {TSDB_DATA_TYPE_UINT,                   ""},
    {TSDB_DATA_TYPE_UBIGINT,                ""},
  };

  for (size_t i=0; i<sizeof(_maps)/sizeof(_maps[0]); ++i) {
    if (col->type != _maps[i].tsdb_type) continue;
    const char *suffix = _maps[i].suffix;
    int n = snprintf(CharacterAttributePtr, BufferLength, "%s", suffix);
    if (n < 0) {
      int e = errno;
      stmt_append_err_format(stmt, "HY000", 0, "General error:internal logic error:[%d]%s", e, strerror(e));
      return SQL_ERROR;
    }
    if (StringLengthPtr) *StringLengthPtr = n;
    return SQL_SUCCESS;
  }
  return SQL_ERROR;
}

static SQLRETURN _stmt_col_set_empty_string(
    stmt_t               *stmt,
    SQLPOINTER            CharacterAttributePtr,
    SQLSMALLINT           BufferLength,
    SQLSMALLINT          *StringLengthPtr)
{
  int n;
  n = snprintf(CharacterAttributePtr, BufferLength, "%s", "");
  if (n < 0) {
    int e = errno;
    stmt_append_err_format(stmt, "HY000", 0, "General error:internal logic error:[%d]%s", e, strerror(e));
    return SQL_ERROR;
  }
  if (StringLengthPtr) *StringLengthPtr = n;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_col_copy_string(
    stmt_t         *stmt,
    const SQLCHAR  *name,
    size_t          name_bytes,
    SQLPOINTER      CharacterAttributePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  int n = 0;
  n = snprintf((char*)CharacterAttributePtr, BufferLength, "%.*s", (int)name_bytes, name);
  if (n < 0) {
    int e = errno;
    stmt_append_err_format(stmt, "HY000", 0, "General error:internal logic error:[%d]%s", e, strerror(e));
    return SQL_ERROR;
  }
  if (StringLengthPtr) *StringLengthPtr = n;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_fill_IRD(stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  TAOS_FIELD *fields;
  size_t nr;
  sr = stmt->base->get_col_fields(stmt->base, &fields, &nr);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  descriptor_t *IRD = stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;

  sr = descriptor_keep(IRD, stmt, nr);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  IRD_header->DESC_COUNT = (SQLUSMALLINT)nr;

  for (size_t i=0; i<IRD_header->DESC_COUNT; ++i) {
    desc_record_t *IRD_record = IRD->records + i;
    TAOS_FIELD *col = fields + i;

    SQLSMALLINT StringLength = 0;

    IRD_record->tsdb_type = col->type;

    IRD_record->DESC_AUTO_UNIQUE_VALUE = SQL_FALSE;

    sr = _stmt_col_set_empty_string(stmt, IRD_record->DESC_BASE_COLUMN_NAME, sizeof(IRD_record->DESC_BASE_COLUMN_NAME), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_set_empty_string(stmt, IRD_record->DESC_BASE_TABLE_NAME, sizeof(IRD_record->DESC_BASE_TABLE_NAME), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    IRD_record->DESC_CASE_SENSITIVE = SQL_FALSE;

    sr = _stmt_col_set_empty_string(stmt, IRD_record->DESC_CATALOG_NAME, sizeof(IRD_record->DESC_CATALOG_NAME), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_CONCISE_TYPE(stmt, col, &IRD_record->DESC_CONCISE_TYPE);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    IRD_record->DESC_DATA_PTR = NULL;
    IRD_record->DESC_COUNT = IRD_header->DESC_COUNT; // FIXME:

    sr = _stmt_col_DESC_DISPLAY_SIZE(stmt, col, &IRD_record->DESC_DISPLAY_SIZE);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    IRD_record->DESC_FIXED_PREC_SCALE = 0;

    sr = _stmt_col_DESC_NAME(stmt, col, IRD_record->DESC_LABEL, sizeof(IRD_record->DESC_LABEL), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_LENGTH(stmt, col, &IRD_record->DESC_LENGTH);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_LITERAL_SUFFIX(stmt, col, IRD_record->DESC_LITERAL_PREFIX, sizeof(IRD_record->DESC_LITERAL_PREFIX), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_LITERAL_SUFFIX(stmt, col, IRD_record->DESC_LITERAL_SUFFIX, sizeof(IRD_record->DESC_LITERAL_SUFFIX), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_TYPE_NAME(stmt, col, IRD_record->DESC_LOCAL_TYPE_NAME, sizeof(IRD_record->DESC_LOCAL_TYPE_NAME), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_NAME(stmt, col, IRD_record->DESC_NAME, sizeof(IRD_record->DESC_NAME), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    IRD_record->DESC_NULLABLE = SQL_NULLABLE_UNKNOWN;
    if (i == 0 && col->type == TSDB_DATA_TYPE_TIMESTAMP) IRD_record->DESC_NULLABLE = SQL_NO_NULLS;

    sr = _stmt_col_DESC_NUM_PREC_RADIX(stmt, col, &IRD_record->DESC_NUM_PREC_RADIX);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_OCTET_LENGTH(stmt, col, &IRD_record->DESC_OCTET_LENGTH);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_PRECISION(stmt, col, &IRD_record->DESC_PRECISION);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_SCALE(stmt, col, &IRD_record->DESC_SCALE);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_set_empty_string(stmt, IRD_record->DESC_SCHEMA_NAME, sizeof(IRD_record->DESC_SCHEMA_NAME), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_SEARCHABLE(stmt, col, &IRD_record->DESC_SEARCHABLE);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_set_empty_string(stmt, IRD_record->DESC_TABLE_NAME, sizeof(IRD_record->DESC_TABLE_NAME), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_TYPE(stmt, col, &IRD_record->DESC_TYPE);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    sr = _stmt_col_DESC_TYPE_NAME(stmt, col, IRD_record->DESC_TYPE_NAME, sizeof(IRD_record->DESC_TYPE_NAME), &StringLength);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    IRD_record->DESC_UNNAMED = (col->name[0]) ? SQL_NAMED : SQL_UNNAMED;

    sr = _stmt_col_DESC_UNSIGNED(stmt, col, &IRD_record->DESC_UNSIGNED);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    IRD_record->DESC_UPDATABLE = SQL_ATTR_READONLY;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_set_rows_fetched_ptr(stmt_t *stmt, SQLULEN *rows_fetched_ptr)
{
  descriptor_t *IRD = stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;
  IRD_header->DESC_ROWS_PROCESSED_PTR = rows_fetched_ptr;

  return SQL_SUCCESS;
}

static void _stmt_unprepare(stmt_t *stmt)
{
  _stmt_close_result(stmt);
  tsdb_stmt_unprepare(&stmt->tsdb_stmt);
  // _tsdb_binds_reset(&stmt->tsdb_binds);
  _param_state_reset(&stmt->param_state);
}

static SQLULEN _stmt_get_row_array_size(stmt_t *stmt)
{
  descriptor_t *ARD = stmt_ARD(stmt);
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
    if (stmt->tsdb_stmt.prepared && !stmt->tsdb_stmt.is_insert_stmt) {
      stmt_append_err(stmt, "HY000", 0, "General error:taosc currently does not support batch execution for non-insert-statement");
      return SQL_ERROR;
    }
  }

  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;
  APD_header->DESC_ARRAY_SIZE = paramset_size;

  return SQL_SUCCESS;
}

// static SQLULEN _stmt_get_paramset_size(stmt_t *stmt)
// {
//   descriptor_t *APD = stmt_APD(stmt);
//   desc_header_t *APD_header = &APD->header;
//   return APD_header->DESC_ARRAY_SIZE;
// }

static SQLRETURN _stmt_set_row_status_ptr(stmt_t *stmt, SQLUSMALLINT *row_status_ptr)
{
  descriptor_t *IRD = stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;
  IRD_header->DESC_ARRAY_STATUS_PTR = row_status_ptr;
  return SQL_SUCCESS;
}

// static SQLUSMALLINT* stmt_get_row_status_ptr(stmt_t *stmt)
// {
//   descriptor_t *IRD = stmt_IRD(stmt);
//   desc_header_t *IRD_header = &IRD->header;
//   return IRD_header->DESC_ARRAY_STATUS_PTR;
// }

static SQLRETURN _stmt_set_param_status_ptr(stmt_t *stmt, SQLUSMALLINT *param_status_ptr)
{
  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  IPD_header->DESC_ARRAY_STATUS_PTR = param_status_ptr;
  return SQL_SUCCESS;
}

// static SQLUSMALLINT* _stmt_get_param_status_ptr(stmt_t *stmt)
// {
//   descriptor_t *IPD = stmt_IPD(stmt);
//   desc_header_t *IPD_header = &IPD->header;
//   return IPD_header->DESC_ARRAY_STATUS_PTR;
// }

SQLRETURN stmt_get_row_count(stmt_t *stmt, SQLLEN *row_count_ptr)
{
  return stmt->base->row_count(stmt->base, row_count_ptr);
}

SQLRETURN stmt_get_col_count(stmt_t *stmt, SQLSMALLINT *col_count_ptr)
{
  return stmt->base->get_num_cols(stmt->base, col_count_ptr);
}

static SQLRETURN _stmt_set_row_bind_type(stmt_t *stmt, SQLULEN row_bind_type)
{
  if (row_bind_type != SQL_BIND_BY_COLUMN) {
    stmt_append_err(stmt, "HY000", 0, "General error:only `SQL_BIND_BY_COLUMN` is supported now");
    return SQL_ERROR;
  }

  descriptor_t *ARD = stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;
  ARD_header->DESC_BIND_TYPE = row_bind_type;

  return SQL_SUCCESS;
}

// static SQLULEN stmt_get_row_bind_type(stmt_t *stmt)
// {
//   descriptor_t *ARD = stmt_ARD(stmt);
//   desc_header_t *ARD_header = &ARD->header;
//   return ARD_header->DESC_BIND_TYPE;
// }

static SQLRETURN _stmt_set_param_bind_type(stmt_t *stmt, SQLULEN param_bind_type)
{
  if (param_bind_type != SQL_BIND_BY_COLUMN) {
    stmt_append_err(stmt, "HY000", 0, "General error:only `SQL_BIND_BY_COLUMN` is supported now");
    return SQL_ERROR;
  }

  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;
  APD_header->DESC_BIND_TYPE = param_bind_type;

  return SQL_SUCCESS;
}

// static SQLULEN _stmt_get_param_bind_type(stmt_t *stmt)
// {
//   descriptor_t *APD = stmt_APD(stmt);
//   desc_header_t *APD_header = &APD->header;
//   return APD_header->DESC_BIND_TYPE;
// }

static SQLRETURN _stmt_set_params_processed_ptr(stmt_t *stmt, SQLULEN *params_processed_ptr)
{
  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  IPD_header->DESC_ROWS_PROCESSED_PTR = params_processed_ptr;

  return SQL_SUCCESS;
}

// static SQLULEN* _stmt_get_params_processed_ptr(stmt_t *stmt)
// {
//   descriptor_t *IPD = stmt_IPD(stmt);
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
  descriptor_t *ARD = stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;
  ARD_header->DESC_BIND_OFFSET_PTR = row_bind_offset_ptr;

  return SQL_SUCCESS;
}

// static SQLULEN* _stmt_get_row_bind_offset_ptr(stmt_t *stmt)
// {
//   descriptor_t *ARD = stmt_ARD(stmt);
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
  if (ColumnNumber == 0) {
    stmt_append_err(stmt, "HY000", 0, "General error:bookmark column not supported yet");
    return SQL_ERROR;
  }

  SQLRETURN sr = SQL_SUCCESS;

  SQLLEN NumericAttribute;

  descriptor_t *IRD = stmt_IRD(stmt);
  desc_record_t *IRD_record = IRD->records + ColumnNumber - 1;

  sr = _stmt_col_copy_string(stmt, IRD_record->DESC_NAME, sizeof(IRD_record->DESC_NAME), ColumnName, BufferLength, NameLengthPtr);
  if (sr != SQL_SUCCESS) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:`SQL_DESC_NAME` for `%s` not supported yet",
        taos_data_type(IRD_record->tsdb_type));
    return SQL_ERROR;
  }

  if (DataTypePtr) {
    NumericAttribute = IRD_record->DESC_CONCISE_TYPE;
    *DataTypePtr = (SQLSMALLINT)NumericAttribute;
  }

  if (ColumnSizePtr) {
    NumericAttribute = IRD_record->DESC_LENGTH;
    *ColumnSizePtr = (SQLULEN)NumericAttribute;
  }

  if (DecimalDigitsPtr) {
    NumericAttribute = IRD_record->DESC_SCALE;
    *DecimalDigitsPtr = (SQLSMALLINT)NumericAttribute;
  }

  if (NullablePtr) {
    NumericAttribute = IRD_record->DESC_NULLABLE;
    *NullablePtr = (SQLSMALLINT)NumericAttribute;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_bind_col(stmt_t *stmt,
    SQLUSMALLINT   ColumnNumber,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  descriptor_t *ARD = stmt_ARD(stmt);
  return descriptor_bind_col(ARD, stmt, ColumnNumber, TargetType, TargetValuePtr, BufferLength, StrLen_or_IndPtr);
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

  return _stmt_bind_col(stmt, ColumnNumber, TargetType, TargetValuePtr, BufferLength, StrLen_or_IndPtr);
}

static SQLPOINTER _stmt_get_address(stmt_t *stmt, SQLPOINTER ptr, SQLULEN octet_length, size_t i_row, desc_header_t *header)
{
  (void)stmt;
  if (ptr == NULL) return ptr;

  char *base = (char*)ptr;
  if (header->DESC_BIND_OFFSET_PTR) base += *header->DESC_BIND_OFFSET_PTR;

  char *dest = base + octet_length * i_row;

  return dest;
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

static SQLRETURN _stmt_get_data_prepare_ctx(stmt_t *stmt, stmt_get_data_args_t *args)
{
  SQLRETURN sr = SQL_SUCCESS;

  SQLSMALLINT ColumnCount;
  sr = stmt_get_col_count(stmt, &ColumnCount);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  if (args->Col_or_Param_Num < 1 || args->Col_or_Param_Num > ColumnCount) {
    stmt_append_err_format(stmt, "07009", 0, "Invalid descriptor index:#%d Col_or_Param, %d ColumnCount", args->Col_or_Param_Num, ColumnCount);
    return SQL_ERROR;
  }

  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  sr = stmt->base->get_data(stmt->base, args->Col_or_Param_Num, tsdb);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  if (tsdb->is_null) return SQL_SUCCESS;

  int target_is_fix = 1;

  switch (args->TargetType) {
    case SQL_C_CHAR:
    case SQL_C_WCHAR:
    case SQL_C_BINARY:
      target_is_fix = 0;
      break;
    default:
      break;
  }

  if (target_is_fix) return SQL_SUCCESS;

  switch(tsdb->type) {
    case TSDB_DATA_TYPE_BOOL:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%s", tsdb->b ? "true" : "false");
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_TINYINT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%d", tsdb->i8);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_UTINYINT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%u", tsdb->u8);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_SMALLINT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%d", tsdb->i16);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_USMALLINT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%u", tsdb->u16);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_INT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%d", tsdb->i32);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_UINT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%u", tsdb->u32);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_BIGINT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%" PRId64 "", tsdb->i64);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_UBIGINT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%" PRIu64 "", tsdb->u64);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_FLOAT:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%g", (double)tsdb->flt);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_DOUBLE:
      {
        ctx->nr = snprintf(ctx->buf, sizeof(ctx->buf), "%g", tsdb->dbl);
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_VARCHAR:
      ctx->nr = tsdb->str.len;
      ctx->pos = tsdb->str.str;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      {
        ctx->nr = tsdb_timestamp_to_string(tsdb->ts.ts, tsdb->ts.precision, (char*)ctx->buf, sizeof(ctx->buf));
        ctx->pos = ctx->buf;
      } break;
    case TSDB_DATA_TYPE_NCHAR:
      ctx->nr = tsdb->str.len;
      ctx->pos = tsdb->str.str;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` not implemented yet",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;

}

static SQLRETURN _stmt_get_data_copy_buf_to_char(stmt_t *stmt, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;
  if (ctx->pos == (const char*)-1) return SQL_NO_DATA;
  if (args->BufferLength < 4) {
    // TODO: remove this restriction
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:buffer too small,"
        " "
        "currently at least 4 bytes in which case a complete unicode character could be converted and stored",
        args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
        sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
    return SQL_ERROR;
  }

  const char *fromcode = conn_get_tsdb_charset(stmt->conn);
  const char *tocode   = conn_get_sqlc_charset(stmt->conn);
  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  const size_t     inbytes             = ctx->nr;
  const size_t     outbytes            = args->BufferLength - 1;

  char            *inbuf               = (char*)ctx->pos;
  size_t           inbytesleft         = inbytes;
  char            *outbuf              = (char*)args->TargetValuePtr;
  size_t           outbytesleft        = outbytes;

  size_t n = CALL_iconv(cnv->cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
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
  ctx->nr = inbytesleft;
  *(int8_t*)outbuf = 0;

  if (ctx->nr) {
    if (args->IndPtr) *args->IndPtr = SQL_NO_TOTAL;
    if (args->StrLenPtr) *args->StrLenPtr = SQL_NO_TOTAL;
    stmt_append_err_format(stmt, "01004", 0,
        "String data, right truncated:[iconv]Character set conversion for `%s` to `%s`, #%zd out of #%zd bytes consumed, #%zd out of #%zd bytes converted:[%d]%s",
        cnv->from, cnv->to, inbytes - inbytesleft, inbytes, outbytes - outbytesleft, outbytes, e, strerror(e));
    ctx->pos = inbuf;
    return SQL_SUCCESS_WITH_INFO;
  }

  ctx->pos = (const char*)-1;
  if (args->IndPtr) *args->IndPtr = 0; // FIXME:
  if (args->StrLenPtr) *args->StrLenPtr = outbytes - outbytesleft;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy_buf_to_wchar(stmt_t *stmt, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;
  if (ctx->pos == (const char*)-1) return SQL_NO_DATA;
  if (args->BufferLength < 4) {
    // TODO: remove this restriction
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:buffer too small,"
        " "
        "currently at least 4 bytes in which case a complete unicode character could be converted and stored",
        args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
        sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
    return SQL_ERROR;
  }

  const char *fromcode = conn_get_tsdb_charset(stmt->conn);
  const char *tocode   = "UCS-2LE";
  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  const size_t     inbytes             = ctx->nr;
  const size_t     outbytes            = args->BufferLength - 2;

  char            *inbuf               = (char*)ctx->pos;
  size_t           inbytesleft         = inbytes;
  char            *outbuf              = (char*)args->TargetValuePtr;
  size_t           outbytesleft        = outbytes;

  size_t n = CALL_iconv(cnv->cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
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
  ctx->nr = inbytesleft;
  *(int16_t*)outbuf = 0;

  if (ctx->nr) {
    if (args->IndPtr) *args->IndPtr = SQL_NO_TOTAL;
    if (args->StrLenPtr) *args->StrLenPtr = SQL_NO_TOTAL;
    stmt_append_err_format(stmt, "01004", 0,
        "String data, right truncated:[iconv]Character set conversion for `%s` to `%s`, #%zd out of #%zd bytes consumed, #%zd out of #%zd bytes converted:[%d]%s",
        cnv->from, cnv->to, inbytes - inbytesleft, inbytes, outbytes - outbytesleft, outbytes, e, strerror(e));
    ctx->pos = inbuf;
    return SQL_SUCCESS_WITH_INFO;
  }

  if (args->IndPtr) *args->IndPtr = 0; // FIXME:
  if (args->StrLenPtr) *args->StrLenPtr = outbytes - outbytesleft;
  ctx->pos = (const char*)-1;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy_buf_to_binary(stmt_t *stmt, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;
  if (ctx->pos == (const char*)-1) return SQL_NO_DATA;
  if (args->BufferLength < 0) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:buffer too small",
        args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
        sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
    return SQL_ERROR;
  }

  size_t n = ctx->nr - args->BufferLength;
  if (n > 0) n = args->BufferLength;
  else       n = ctx->nr;

  memcpy(args->TargetValuePtr, ctx->pos, n);

  ctx->nr = ctx->nr - n;

  if (ctx->nr) {
    if (args->IndPtr) *args->IndPtr = SQL_NO_TOTAL;
    if (args->StrLenPtr) *args->StrLenPtr = SQL_NO_TOTAL;
    stmt_append_err_format(stmt, "01004", 0,
        "String data, right truncated:Column[%d], #%zd out of #%zd bytes consumed",
        args->Col_or_Param_Num, n, n + ctx->nr);
    ctx->pos += n;
    return SQL_SUCCESS_WITH_INFO;
  }

  if (args->IndPtr) *args->IndPtr = 0; // FIXME:
  if (args->StrLenPtr) *args->StrLenPtr = n;
  ctx->pos = (const char*)-1;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_strtoll(stmt_t *stmt, const char *s, int64_t *v)
{
  char *end = NULL;
  long long ll = strtoll(s, &end, 0);
  int e = errno;
  if (end && *end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:conversion from `%s` to `int64_t` failed:invalid character[0x%02x]",
        s, *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:conversion from `%s` to `int64_t` failed:overflow or underflow occurs",
        s);
    return SQL_ERROR;
  }

  *v = ll;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_varchar_to_int64(stmt_t *stmt, const char *s, size_t nr, int64_t *v, stmt_get_data_args_t *args)
{
  int r = 0;

  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;
  mem_t *tsdb_cache = &ctx->mem;

  if (s[nr]) {
    r = mem_keep(tsdb_cache, nr + 1);
    if (r) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
    memcpy(tsdb_cache->base, s, nr);
    tsdb_cache->base[nr] = '\0';
    s = (const char*)tsdb_cache->base;
  }

  char *end = NULL;
  long long ll = strtoll(s, &end, 0);
  int e = errno;
  if (end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:invalid character[0x%02x]",
        args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
        sqlc_data_type(args->TargetType), args->TargetType, args->TargetType, *end);
    return SQL_ERROR;
  }
  if (e == ERANGE) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
        args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
        sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
    return SQL_ERROR;
  }

  *v = ll;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_varchar_to_uint64(stmt_t *stmt, const char *s, size_t nr, uint64_t *v, stmt_get_data_args_t *args)
{
  SQLRETURN sr = SQL_SUCCESS;

  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  int64_t i64;
  sr = _stmt_varchar_to_int64(stmt, s, nr, &i64, args);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  if (memchr(s, '-', nr)) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:signedness conflict",
        args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
        sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
    return SQL_ERROR;
  }

  *v = (uint64_t)i64;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_varchar_to_float(stmt_t *stmt, const char *s, size_t nr, float *v, stmt_get_data_args_t *args)
{
  int r = 0;

  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;
  mem_t *tsdb_cache = &ctx->mem;

  if (s[nr]) {
    r = mem_keep(tsdb_cache, nr + 1);
    if (r) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
    memcpy(tsdb_cache->base, s, nr);
    tsdb_cache->base[nr] = '\0';
    s = (const char*)tsdb_cache->base;
  }

  char *end = NULL;
  float flt = strtof(s, &end);
  int e = errno;
  if (end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:invalid character[0x%02x]",
        args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
        sqlc_data_type(args->TargetType), args->TargetType, args->TargetType, *end);
    return SQL_ERROR;
  }
  if (fabsf(flt) == HUGE_VALF) {
    if (e == ERANGE) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
    }
  }

  if (fpclassify(flt) == FP_ZERO) {
    if (e == ERANGE) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
    }
  }

  *v = flt;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_strtod(stmt_t *stmt, const char *s, double *v)
{
  char *end = NULL;
  double dbl = strtod(s, &end);
  int e = errno;
  if (end && *end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:conversion from `%s` to `double` failed:invalid character[0x%02x]",
        s, *end);
    return SQL_ERROR;
  }
  if (fabs(dbl) == HUGE_VAL) {
    if (e == ERANGE) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from `%s` to `double` failed:overflow or underflow occurs",
          s);
      return SQL_ERROR;
    }
  }

  if (fpclassify(dbl) == FP_ZERO) {
    if (e == ERANGE) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from `%s` to `double` failed:overflow or underflow occurs",
          s);
      return SQL_ERROR;
    }
  }

  *v = dbl;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_varchar_to_double(stmt_t *stmt, const char *s, size_t nr, double *v, stmt_get_data_args_t *args)
{
  int r = 0;

  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;
  mem_t *tsdb_cache = &ctx->mem;

  if (s[nr]) {
    r = mem_keep(tsdb_cache, nr + 1);
    if (r) {
      stmt_oom(stmt);
      return SQL_ERROR;
    }
    memcpy(tsdb_cache->base, s, nr);
    tsdb_cache->base[nr] = '\0';
    s = (const char*)tsdb_cache->base;
  }

  char *end = NULL;
  double dbl = strtod(s, &end);
  int e = errno;
  if (end) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:invalid character[0x%02x]",
        args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
        sqlc_data_type(args->TargetType), args->TargetType, args->TargetType, *end);
    return SQL_ERROR;
  }
  if (fabs(dbl) == HUGE_VAL) {
    if (e == ERANGE) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
    }
  }

  if (fpclassify(dbl) == FP_ZERO) {
    if (e == ERANGE) {
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
    }
  }

  *v = dbl;
  return SQL_SUCCESS;
}


static SQLRETURN _stmt_get_data_copy_varchar(stmt_t *stmt, const char *s, size_t nr, stmt_get_data_args_t *args)
{
  SQLRETURN sr = SQL_SUCCESS;

  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;
  int64_t i64;
  uint64_t u64;
  float flt;
  double dbl;

  switch (args->TargetType) {
    case SQL_C_BIT:
      sr = _stmt_varchar_to_int64(stmt, s, nr, &i64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      if (i64 != 0 && i64 != 1) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      *(int8_t*)args->TargetValuePtr = !!i64;
      break;
    case SQL_C_STINYINT:
      sr = _stmt_varchar_to_int64(stmt, s, nr, &i64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      if (i64 < INT8_MIN || i64 > INT8_MAX ) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      *(int8_t*)args->TargetValuePtr = (int8_t)i64;
      break;
    case SQL_C_UTINYINT:
      sr = _stmt_varchar_to_uint64(stmt, s, nr, &u64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      if (u64 > UINT8_MAX ) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      *(uint8_t*)args->TargetValuePtr = (uint8_t)u64;
      break;
    case SQL_C_SSHORT:
      sr = _stmt_varchar_to_int64(stmt, s, nr, &i64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      if (i64 < INT16_MIN || i64 > INT16_MAX ) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      *(int16_t*)args->TargetValuePtr = (int16_t)i64;
      break;
    case SQL_C_USHORT:
      sr = _stmt_varchar_to_uint64(stmt, s, nr, &u64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      if (u64 > UINT16_MAX ) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      *(uint16_t*)args->TargetValuePtr = (uint16_t)u64;
      break;
    case SQL_C_SLONG:
      sr = _stmt_varchar_to_int64(stmt, s, nr, &i64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      if (i64 < INT32_MIN || i64 > INT32_MAX ) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      *(int32_t*)args->TargetValuePtr = (int32_t)i64;
      break;
    case SQL_C_ULONG:
      sr = _stmt_varchar_to_uint64(stmt, s, nr, &u64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      if (u64 > UINT32_MAX ) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed:overflow or underflow occurs",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      *(uint32_t*)args->TargetValuePtr = (uint32_t)u64;
      break;
    case SQL_C_SBIGINT:
      sr = _stmt_varchar_to_int64(stmt, s, nr, &i64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      *(int64_t*)args->TargetValuePtr = i64;
      break;
    case SQL_C_UBIGINT:
      sr = _stmt_varchar_to_uint64(stmt, s, nr, &u64, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      *(uint64_t*)args->TargetValuePtr = u64;
      break;
    case SQL_C_FLOAT:
      sr = _stmt_varchar_to_float(stmt, s, nr, &flt, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      *(float*)args->TargetValuePtr = flt;
      break;
    case SQL_C_DOUBLE:
      sr = _stmt_varchar_to_double(stmt, s, nr, &dbl, args);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      *(double*)args->TargetValuePtr = dbl;
      break;
    case SQL_C_CHAR:
      return _stmt_get_data_copy_buf_to_char(stmt, args);
    case SQL_C_WCHAR:
      return _stmt_get_data_copy_buf_to_wchar(stmt, args);
    case SQL_C_BINARY:
      return _stmt_get_data_copy_buf_to_binary(stmt, args);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]`not implemented yet",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy_nchar(stmt_t *stmt, const char *s, size_t nr, stmt_get_data_args_t *args)
{
  return _stmt_get_data_copy_varchar(stmt, s, nr, args);
}

static SQLRETURN _stmt_get_data_copy_timestamp(stmt_t *stmt, int64_t v, int precision, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  switch (args->TargetType) {
    case SQL_C_BIT:
      *(uint8_t*)args->TargetValuePtr = !!v;
      break;
    case SQL_C_STINYINT:
      *(int8_t*)args->TargetValuePtr = (int8_t)v;
      break;
    case SQL_C_UTINYINT:
      *(uint8_t*)args->TargetValuePtr = (uint8_t)v;
      break;
    case SQL_C_SSHORT:
      *(int16_t*)args->TargetValuePtr = (int16_t)v;
      break;
    case SQL_C_USHORT:
      *(uint16_t*)args->TargetValuePtr = (uint16_t)v;
      break;
    case SQL_C_SLONG:
      *(int32_t*)args->TargetValuePtr = (int32_t)v;
      break;
    case SQL_C_ULONG:
      *(uint32_t*)args->TargetValuePtr = (uint32_t)v;
      break;
    case SQL_C_SBIGINT:
      *(int64_t*)args->TargetValuePtr = v;
      break;
    case SQL_C_UBIGINT:
      *(uint64_t*)args->TargetValuePtr = v;
      break;
    case SQL_C_SHORT:
      *(int16_t*)args->TargetValuePtr = (int16_t)v;
      break;
    case SQL_C_CHAR:
      return _stmt_get_data_copy_buf_to_char(stmt, args);
    case SQL_C_WCHAR:
      return _stmt_get_data_copy_buf_to_wchar(stmt, args);
    case SQL_C_BINARY:
      return _stmt_get_data_copy_buf_to_binary(stmt, args);
    case SQL_C_TYPE_TIMESTAMP:
      if (tsdb_timestamp_to_SQL_C_TYPE_TIMESTAMP(v, precision, (SQL_TIMESTAMP_STRUCT*)args->TargetValuePtr)) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      return SQL_SUCCESS;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]`not implemented yet",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy_int64(stmt_t *stmt, int64_t v, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  switch (args->TargetType) {
    case SQL_C_BIT:
      *(uint8_t*)args->TargetValuePtr = !!v;
      break;
    case SQL_C_STINYINT:
      *(int8_t*)args->TargetValuePtr = (int8_t)v;
      break;
    case SQL_C_UTINYINT:
      *(uint8_t*)args->TargetValuePtr = (uint8_t)v;
      break;
    case SQL_C_SSHORT:
      *(int16_t*)args->TargetValuePtr = (int16_t)v;
      break;
    case SQL_C_USHORT:
      *(uint16_t*)args->TargetValuePtr = (uint16_t)v;
      break;
    case SQL_C_SLONG:
      *(int32_t*)args->TargetValuePtr = (int32_t)v;
      break;
    case SQL_C_ULONG:
      *(uint32_t*)args->TargetValuePtr = (uint32_t)v;
      break;
    case SQL_C_SBIGINT:
      *(int64_t*)args->TargetValuePtr = v;
      break;
    case SQL_C_UBIGINT:
      *(uint64_t*)args->TargetValuePtr = v;
      break;
    case SQL_C_SHORT:
      *(int16_t*)args->TargetValuePtr = (int16_t)v;
      break;
    case SQL_C_CHAR:
      return _stmt_get_data_copy_buf_to_char(stmt, args);
    case SQL_C_WCHAR:
      return _stmt_get_data_copy_buf_to_wchar(stmt, args);
    case SQL_C_BINARY:
      return _stmt_get_data_copy_buf_to_binary(stmt, args);
    case SQL_C_TYPE_TIMESTAMP:
      if (tsdb_timestamp_to_SQL_C_TYPE_TIMESTAMP(v, 0, (SQL_TIMESTAMP_STRUCT*)args->TargetValuePtr)) {
        stmt_append_err_format(stmt, "HY000", 0,
            "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]` failed",
            args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
            sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
        return SQL_ERROR;
      }
      return SQL_SUCCESS;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]`not implemented yet",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy_uint64(stmt_t *stmt, uint64_t v, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  switch (args->TargetType) {
    case SQL_C_BIT:
      *(uint8_t*)args->TargetValuePtr = !!v;
      break;
    case SQL_C_STINYINT:
      *(int8_t*)args->TargetValuePtr = (int8_t)v;
      break;
    case SQL_C_UTINYINT:
      *(uint8_t*)args->TargetValuePtr = (uint8_t)v;
      break;
    case SQL_C_SHORT:
    case SQL_C_SSHORT:
      *(int16_t*)args->TargetValuePtr = (int16_t)v;
      break;
    case SQL_C_USHORT:
      *(uint16_t*)args->TargetValuePtr = (uint16_t)v;
      break;
    case SQL_C_SLONG:
      *(int32_t*)args->TargetValuePtr = (int32_t)v;
      break;
    case SQL_C_ULONG:
      *(uint32_t*)args->TargetValuePtr = (uint32_t)v;
      break;
    case SQL_C_SBIGINT:
      *(int64_t*)args->TargetValuePtr = v;
      break;
    case SQL_C_UBIGINT:
      *(uint64_t*)args->TargetValuePtr = v;
      break;
    case SQL_C_CHAR:
      return _stmt_get_data_copy_buf_to_char(stmt, args);
    case SQL_C_WCHAR:
      return _stmt_get_data_copy_buf_to_wchar(stmt, args);
    case SQL_C_BINARY:
      return _stmt_get_data_copy_buf_to_binary(stmt, args);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]`not implemented yet",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy_double(stmt_t *stmt, double v, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  switch (args->TargetType) {
    case SQL_C_BIT:
      *(uint8_t*)args->TargetValuePtr = !!(uint8_t)v;
      break;
    case SQL_C_STINYINT:
      *(int8_t*)args->TargetValuePtr = (int8_t)v;
      break;
    case SQL_C_UTINYINT:
      *(uint8_t*)args->TargetValuePtr = (uint8_t)v;
      break;
    case SQL_C_SSHORT:
      *(int16_t*)args->TargetValuePtr = (int16_t)v;
      break;
    case SQL_C_USHORT:
      *(uint16_t*)args->TargetValuePtr = (uint16_t)v;
      break;
    case SQL_C_SLONG:
      *(int32_t*)args->TargetValuePtr = (int32_t)v;
      break;
    case SQL_C_ULONG:
      *(uint32_t*)args->TargetValuePtr = (uint32_t)v;
      break;
    case SQL_C_SBIGINT:
      *(int64_t*)args->TargetValuePtr = (int64_t)v;
      break;
    case SQL_C_UBIGINT:
      *(uint64_t*)args->TargetValuePtr = (uint64_t)v;
      break;
    case SQL_C_FLOAT:
      *(float*)args->TargetValuePtr = (float)v;
      break;
    case SQL_C_DOUBLE:
      *(double*)args->TargetValuePtr = v;
      break;
    case SQL_C_CHAR:
      return _stmt_get_data_copy_buf_to_char(stmt, args);
    case SQL_C_WCHAR:
      return _stmt_get_data_copy_buf_to_wchar(stmt, args);
    case SQL_C_BINARY:
      return _stmt_get_data_copy_buf_to_binary(stmt, args);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]`not implemented yet",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_copy(stmt_t *stmt, stmt_get_data_args_t *args)
{
  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  tsdb_data_t *tsdb = &ctx->tsdb;

  if (tsdb->is_null) {
    if (args->IndPtr) {
      *args->IndPtr = SQL_NULL_DATA;
      return SQL_SUCCESS;
    }
    stmt_append_err_format(stmt, "22002", 0, "Indicator variable required but not supplied:#%d Column_or_Param", args->Col_or_Param_Num);
    return SQL_ERROR;
  }

  switch(tsdb->type) {
    case TSDB_DATA_TYPE_BOOL:
      return _stmt_get_data_copy_uint64(stmt, tsdb->b, args);
    case TSDB_DATA_TYPE_TINYINT:
      return _stmt_get_data_copy_int64(stmt, tsdb->i8, args);
    case TSDB_DATA_TYPE_UTINYINT:
      return _stmt_get_data_copy_uint64(stmt, tsdb->u8, args);
    case TSDB_DATA_TYPE_SMALLINT:
      return _stmt_get_data_copy_int64(stmt, tsdb->i16, args);
    case TSDB_DATA_TYPE_USMALLINT:
      return _stmt_get_data_copy_uint64(stmt, tsdb->u16, args);
    case TSDB_DATA_TYPE_INT:
      return _stmt_get_data_copy_int64(stmt, tsdb->i32, args);
    case TSDB_DATA_TYPE_UINT:
      return _stmt_get_data_copy_uint64(stmt, tsdb->u32, args);
    case TSDB_DATA_TYPE_BIGINT:
      return _stmt_get_data_copy_int64(stmt, tsdb->i64, args);
    case TSDB_DATA_TYPE_UBIGINT:
      return _stmt_get_data_copy_uint64(stmt, tsdb->u64, args);
    case TSDB_DATA_TYPE_FLOAT:
      return _stmt_get_data_copy_double(stmt, tsdb->flt, args);
    case TSDB_DATA_TYPE_DOUBLE:
      return _stmt_get_data_copy_double(stmt, tsdb->dbl, args);
    case TSDB_DATA_TYPE_VARCHAR:
      return _stmt_get_data_copy_varchar(stmt, tsdb->str.str, tsdb->str.len, args);
    case TSDB_DATA_TYPE_TIMESTAMP:
      return _stmt_get_data_copy_timestamp(stmt, tsdb->ts.ts, tsdb->ts.precision, args);
    case TSDB_DATA_TYPE_NCHAR:
      return _stmt_get_data_copy_nchar(stmt, tsdb->str.str, tsdb->str.len, args);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Column[%d] conversion from `%s[0x%x/%d]` to `%s[0x%x/%d]`not implemented yet",
          args->Col_or_Param_Num, taos_data_type(tsdb->type), tsdb->type, tsdb->type,
          sqlc_data_type(args->TargetType), args->TargetType, args->TargetType);
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_get_data_x(stmt_t *stmt, stmt_get_data_args_t *args)
{
  SQLRETURN sr = SQL_SUCCESS;

  get_data_ctx_t *ctx = &stmt->get_data_ctx;
  if (ctx->Col_or_Param_Num != args->Col_or_Param_Num) {
    sr = _stmt_get_data_prepare_ctx(stmt, args);
    if (sr != SQL_SUCCESS) return SQL_ERROR;
    ctx->Col_or_Param_Num = args->Col_or_Param_Num;
    ctx->TargetType       = args->TargetType;
  }

  if (ctx->TargetType != args->TargetType) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:TargetType changes in successive SQLGetData call for #%d Column_or_Param", args->Col_or_Param_Num);
    return SQL_ERROR;
  }

  return _stmt_get_data_copy(stmt, args);
}

SQLRETURN stmt_get_data(
    stmt_t        *stmt,
    SQLUSMALLINT   Col_or_Param_Num,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  SQLRETURN sr = SQL_SUCCESS;

  if (Col_or_Param_Num == 0) {
    stmt_append_err(stmt, "HY000", 0, "General error:bookmark column not supported yet");
    return SQL_ERROR;
  }

  size_t row_array_size = _stmt_get_row_array_size(stmt);
  if (row_array_size > 1) {
    stmt_append_err(stmt, "HY109", 0, "Invalid cursor position:The cursor was a forward-only cursor, and the rowset size was greater than one");
    return SQL_ERROR;
  }

  stmt_get_data_args_t args = {
    .Col_or_Param_Num           = Col_or_Param_Num,
    .TargetType                 = TargetType,
    .TargetValuePtr             = TargetValuePtr,
    .BufferLength               = BufferLength,
    .StrLenPtr                  = StrLen_or_IndPtr,
    .IndPtr                     = StrLen_or_IndPtr,
  };

  sr = _stmt_get_data_x(stmt, &args);
  return sr;
}

static SQLRETURN _stmt_fetch_row(stmt_t *stmt)
{
  return stmt->base->fetch_row(stmt->base);
}

static SQLRETURN _stmt_fill_col(stmt_t *stmt, size_t i_row, size_t i_col)
{
  descriptor_t *ARD = stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;

  if (i_col >= ARD_header->DESC_COUNT) return SQL_SUCCESS;

  desc_record_t *ARD_record = ARD->records + i_col;
  if (ARD_record->DESC_DATA_PTR == NULL) return SQL_SUCCESS;

  char *dest = _stmt_get_address(stmt, ARD_record->DESC_DATA_PTR, ARD_record->DESC_OCTET_LENGTH, i_row, ARD_header);
  SQLLEN *StrLenPtr = _stmt_get_address(stmt, ARD_record->DESC_OCTET_LENGTH_PTR, sizeof(SQLLEN), i_row, ARD_header);
  SQLLEN *IndPtr = _stmt_get_address(stmt, ARD_record->DESC_INDICATOR_PTR, sizeof(SQLLEN), i_row, ARD_header);

  SQLSMALLINT    TargetType       = (SQLSMALLINT)ARD_record->DESC_CONCISE_TYPE;
  SQLPOINTER     TargetValuePtr   = dest;
  SQLLEN         BufferLength     = ARD_record->DESC_OCTET_LENGTH;

  stmt_get_data_args_t args = {
    .Col_or_Param_Num           = (SQLUSMALLINT)i_col + 1,
    .TargetType                 = TargetType,
    .TargetValuePtr             = TargetValuePtr,
    .BufferLength               = BufferLength,
    .StrLenPtr                  = StrLenPtr,
    .IndPtr                     = IndPtr,
  };

  return _stmt_get_data_x(stmt, &args);
}

static SQLRETURN _stmt_fill_row(stmt_t *stmt, size_t i_row)
{
  SQLRETURN sr = SQL_SUCCESS;

  descriptor_t *ARD = stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;

  int with_info = 0;

  for (int i_col = 0; (size_t)i_col < ARD->cap; ++i_col) {
    if (i_col >= ARD_header->DESC_COUNT) continue;
    desc_record_t *ARD_record = ARD->records + i_col;
    if (!ARD_record->bound) continue;
    if (ARD_record->DESC_DATA_PTR == NULL) continue;

    sr = _stmt_fill_col(stmt, i_row, i_col);

    _get_data_ctx_reset(&stmt->get_data_ctx);

    if (sr == SQL_SUCCESS) continue;
    with_info = 1;
    if (sr == SQL_SUCCESS_WITH_INFO) continue;
    return SQL_ERROR;
  }

  return with_info ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

static SQLRETURN _stmt_fetch_rows(stmt_t *stmt, const size_t row_array_size, size_t *nr_rows)
{
  SQLRETURN sr = SQL_SUCCESS;
  SQLRETURN sr_row = SQL_ROW_SUCCESS;

  descriptor_t *IRD = stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;

  size_t i_row = 0;

  *nr_rows = 0;

again:

  sr = _stmt_fetch_row(stmt);
  if (sr == SQL_NO_DATA) {
    if (*nr_rows == 0) return SQL_NO_DATA;
    return SQL_SUCCESS;
  }
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sr = _stmt_fill_row(stmt, i_row);
  switch (sr) {
    case SQL_SUCCESS:
      sr_row = SQL_ROW_SUCCESS;
      break;
    case SQL_SUCCESS_WITH_INFO:
      sr_row = SQL_ROW_SUCCESS_WITH_INFO;
      break;
    default:
      sr_row = SQL_ROW_ERROR;
      break;
  }

  if (IRD_header->DESC_ARRAY_STATUS_PTR) {
    IRD_header->DESC_ARRAY_STATUS_PTR[i_row] = sr_row;
  }

  *nr_rows = ++i_row;

  if (i_row < row_array_size) goto again;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_fetch_x(stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  _get_data_ctx_reset(&stmt->get_data_ctx);

  descriptor_t *IRD = stmt_IRD(stmt);
  desc_header_t *IRD_header = &IRD->header;

  size_t row_array_size = _stmt_get_row_array_size(stmt);
  if (row_array_size == 0) row_array_size = 1;

  size_t nr_rows = 0;
  sr = _stmt_fetch_rows(stmt, row_array_size, &nr_rows);

  if (IRD_header->DESC_ROWS_PROCESSED_PTR) *IRD_header->DESC_ROWS_PROCESSED_PTR = nr_rows;

  if (IRD_header->DESC_ARRAY_STATUS_PTR) {
    for (size_t i=nr_rows; i<row_array_size; ++i) {
      IRD_header->DESC_ARRAY_STATUS_PTR[i] = SQL_ROW_NOROW;
    }
  }

  if (sr == SQL_NO_DATA) return SQL_NO_DATA;
  if (sr == SQL_SUCCESS) return SQL_SUCCESS;
  if (sr == SQL_SUCCESS_WITH_INFO) return SQL_SUCCESS_WITH_INFO;
  return SQL_ERROR;
}

static SQLRETURN _stmt_fetch(stmt_t *stmt)
{
  _get_data_ctx_reset(&stmt->get_data_ctx);

  descriptor_t *ARD = stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;

  if (ARD_header->DESC_BIND_TYPE != SQL_BIND_BY_COLUMN) {
    stmt_append_err(stmt, "HY000", 0, "General error:only `SQL_BIND_BY_COLUMN` is supported now");
    return SQL_ERROR;
  }

  return _stmt_fetch_x(stmt);
}

SQLRETURN stmt_fetch_scroll(stmt_t *stmt,
    SQLSMALLINT   FetchOrientation,
    SQLLEN        FetchOffset)
{
  _get_data_ctx_reset(&stmt->get_data_ctx);

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
  return stmt_fetch_scroll(stmt, SQL_FETCH_NEXT, 0);
}

static SQLRETURN _stmt_prepare(stmt_t *stmt)
{
  return stmt->base->prepare(stmt->base, &stmt->current_sql);
}

static SQLRETURN _stmt_get_num_params(
    stmt_t         *stmt,
    SQLSMALLINT    *ParameterCountPtr)
{
  return stmt->base->get_num_params(stmt->base, ParameterCountPtr);
}

SQLRETURN stmt_get_num_params(
    stmt_t         *stmt,
    SQLSMALLINT    *ParameterCountPtr)
{
  return _stmt_get_num_params(stmt, ParameterCountPtr);
}

static SQLRETURN _stmt_describe_param(
    stmt_t         *stmt,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT    *DataTypePtr,
    SQLULEN        *ParameterSizePtr,
    SQLSMALLINT    *DecimalDigitsPtr,
    SQLSMALLINT    *NullablePtr)
{
  return stmt->base->describe_param(stmt->base, ParameterNumber, DataTypePtr, ParameterSizePtr, DecimalDigitsPtr, NullablePtr);
}

SQLRETURN stmt_describe_param(
    stmt_t         *stmt,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT    *DataTypePtr,
    SQLULEN        *ParameterSizePtr,
    SQLSMALLINT    *DecimalDigitsPtr,
    SQLSMALLINT    *NullablePtr)
{
  if (ParameterNumber == 0) {
    stmt_append_err(stmt, "HY000", 0, "General error:bookmark column not supported yet");
    return SQL_ERROR;
  }

  return _stmt_describe_param(stmt, ParameterNumber, DataTypePtr, ParameterSizePtr, DecimalDigitsPtr, NullablePtr);
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

static int _stmt_sql_found(sqls_parser_param_t *param, size_t start, size_t end, int32_t qms, void *arg)
{
  (void)param;

  stmt_t *stmt = (stmt_t*)arg;
  sqls_t *sqls = &stmt->sqls;
  --end;

  if (sqls->nr >= sqls->cap) {
    size_t cap = sqls->cap + 16;
    parser_nterm_t *nterms = (parser_nterm_t*)realloc(sqls->sqls, sizeof(*nterms) * cap);
    if (!nterms) {
      stmt_oom(stmt);
      return -1;
    }
    sqls->sqls = nterms;
    sqls->cap  = cap;
  }

  sqls->sqls[sqls->nr].start  = start;
  sqls->sqls[sqls->nr].end    = end;
  sqls->sqls[sqls->nr].qms    = qms;
  sqls->nr += 1;

  return 0;
}

static SQLRETURN _stmt_cache_and_parse(stmt_t *stmt, sqls_parser_param_t *param, const char *sql, size_t len)
{
  int r = 0;

  mem_reset(&stmt->raw);
  sqls_reset(&stmt->sqls);

  r = mem_keep(&stmt->raw, len + 1);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  memcpy(stmt->raw.base, sql, len);
  stmt->raw.nr = len;
  stmt->raw.base[len] = '\0';

  r = sqls_parser_parse(sql, len, param);
  if (r) {
    E("location:(%d,%d)->(%d,%d)", param->ctx.row0, param->ctx.col0, param->ctx.row1, param->ctx.col1);
    E("failed:%s", param->ctx.err_msg);
    stmt_append_err_format(stmt, "HY000", 0, "General error:parsing:%.*s", (int)len, sql);
    stmt_append_err_format(stmt, "HY000", 0, "General error:location:(%d,%d)->(%d,%d)", param->ctx.row0, param->ctx.col0, param->ctx.row1, param->ctx.col1);
    stmt_append_err_format(stmt, "HY000", 0, "General error:failed:%.*s", (int)strlen(param->ctx.err_msg), param->ctx.err_msg);

    return SQL_ERROR;
  } else if (stmt->sqls.failed) {
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_next_sql(stmt_t *stmt)
{
  int r = 0;

  sqlc_tsdb_t *sqlc_tsdb = &stmt->current_sql;

  memset(&stmt->current_sql, 0, sizeof(stmt->current_sql));

  sqls_t *sqls = &stmt->sqls;
  if (sqls->pos + 1 > sqls->nr) return SQL_NO_DATA;

  parser_nterm_t *nterms = stmt->sqls.sqls + stmt->sqls.pos;
  sqlc_tsdb->sqlc        = (const char*)stmt->raw.base + nterms->start;
  sqlc_tsdb->sqlc_bytes  = nterms->end - nterms->start;
  sqlc_tsdb->qms         = nterms->qms;

  const char *fromcode = conn_get_sqlc_charset(stmt->conn);
  const char *tocode   = conn_get_tsdb_charset(stmt->conn);
  str_t src = {
    .charset             = fromcode,
    .str                 = sqlc_tsdb->sqlc,
    .bytes               = sqlc_tsdb->sqlc_bytes,
  };
  mem_reset(&stmt->tsdb_sql);
  r = mem_conv_ex(&stmt->tsdb_sql, &src, tocode);
  if (r) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory or conversion failed", fromcode, tocode);
    memset(&stmt->current_sql, 0, sizeof(stmt->current_sql));
    return SQL_ERROR;
  }

  sqlc_tsdb->tsdb        = (const char*)stmt->tsdb_sql.base;
  sqlc_tsdb->tsdb_bytes  = stmt->tsdb_sql.nr;

  ++sqls->pos;

  stmt->tsdb_stmt.params.qms = sqlc_tsdb->qms;

  const char *start = sqlc_tsdb->tsdb;
  const char *end   = sqlc_tsdb->tsdb + sqlc_tsdb->tsdb_bytes;
  if (end > start && start[0] == '!') {
    stmt->tsdb_stmt.is_topic = 1;
  } else {
    stmt->tsdb_stmt.is_topic = 0;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_bind_param(
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
  SQLRETURN sr = SQL_SUCCESS;

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

  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;

  sr = descriptor_keep(APD, stmt, ParameterNumber);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  desc_record_t *APD_record = APD->records + ParameterNumber - 1;
  APD_record->bound = 0;

  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  sr = descriptor_keep(IPD, stmt, ParameterNumber);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  desc_record_t *IPD_record = IPD->records + ParameterNumber - 1;
  // memset(IPD_record, 0, sizeof(*IPD_record));

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
          ParameterNumber, sqlc_data_type(ValueType), ValueType, ValueType);
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

  return _stmt_bind_param(stmt, ParameterNumber, InputOutputType, ValueType, ParameterType, ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength, StrLen_or_IndPtr);
}

static void _stmt_param_get(stmt_t *stmt, int irow, int i_param, char **base, SQLLEN *length, int *is_null)
{
  descriptor_t *APD = stmt_APD(stmt);
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

static SQLRETURN _stmt_guess_tsdb_params_for_sql_c_char(stmt_t *stmt, param_state_t *param_state)
{
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  const char *fromcode = conn_get_sqlc_charset(stmt->conn);
  const char *tocode   = conn_get_tsdb_charset(stmt->conn);
  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  tsdb_field->name[0] = '?';
  tsdb_field->name[1] = '\0';
  tsdb_field->precision = 0;
  tsdb_field->scale = 0;

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

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_guess_tsdb_params_by_sql_varchar(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                             = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ValueType = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;

  tsdb_field->name[0] = '?';
  tsdb_field->name[1] = '\0';
  tsdb_field->precision = 0;
  tsdb_field->scale = 0;

  switch (ValueType) {
    case SQL_C_CHAR:
      return _stmt_guess_tsdb_params_for_sql_c_char(stmt, param_state);
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
          i_param + 1, sqlc_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_guess_tsdb_params_by_sql_integer(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                             = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ValueType = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;

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
          i_param + 1, sqlc_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_guess_tsdb_params_by_sql_bigint(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                             = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ValueType = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;

  tsdb_field->name[0] = '?';
  tsdb_field->name[1] = '\0';
  tsdb_field->precision = 0;
  tsdb_field->scale = 0;

  switch (ValueType) {
    case SQL_C_SBIGINT:
      tsdb_field->type = TSDB_DATA_TYPE_BIGINT;
      tsdb_field->bytes = sizeof(int64_t);
      break;
    case SQL_C_CHAR:
      return _stmt_guess_tsdb_params_for_sql_c_char(stmt, param_state);
    case SQL_C_DOUBLE:
      tsdb_field->type = TSDB_DATA_TYPE_DOUBLE;
      tsdb_field->bytes = sizeof(double);
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sqlc_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_guess_tsdb_params_by_sql_double(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                             = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;

  SQLSMALLINT ValueType = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;

  tsdb_field->name[0] = '?';
  tsdb_field->name[1] = '\0';
  tsdb_field->precision = 0;
  tsdb_field->scale = 0;
  tsdb_field->type = TSDB_DATA_TYPE_INT;

  switch (ValueType) {
    case SQL_C_CHAR:
      return _stmt_guess_tsdb_params_for_sql_c_char(stmt, param_state);
    case SQL_C_DOUBLE:
      tsdb_field->type = TSDB_DATA_TYPE_DOUBLE;
      tsdb_field->bytes = sizeof(double);
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sqlc_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_guess_tsdb_params(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                      = param_state->i_param;
  desc_record_t *IPD_record        = param_state->IPD_record;

  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;

  switch (ParameterType) {
    case SQL_VARCHAR:
      return _stmt_guess_tsdb_params_by_sql_varchar(stmt, param_state);
    case SQL_INTEGER:
      return _stmt_guess_tsdb_params_by_sql_integer(stmt, param_state);
    case SQL_BIGINT:
      return _stmt_guess_tsdb_params_by_sql_bigint(stmt, param_state);
    case SQL_DOUBLE:
      return _stmt_guess_tsdb_params_by_sql_double(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sql_data_type(ParameterType));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_prepare_param_data_array_from_sql_c_char_to_tsdb_varchar(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  r = mem_keep(&param_column->mem_length, sizeof(*tsdb_bind->length) * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = (int32_t*)param_column->mem_length.base;

  tsdb_bind->buffer_length = tsdb_field->bytes - 2;

  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_varchar(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;

  SQLSMALLINT ValueType = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_CHAR:
      return _stmt_prepare_param_data_array_from_sql_c_char_to_tsdb_varchar(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sqlc_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_from_sql_c_char_to_tsdb_nchar(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  r = mem_keep(&param_column->mem_length, sizeof(*tsdb_bind->length) * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = (int32_t*)param_column->mem_length.base;

  const char *fromcode = conn_get_sqlc_charset(stmt->conn);
  const char *tocode   = conn_get_tsdb_charset(stmt->conn);
  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  if (stmt->tsdb_stmt.is_insert_stmt) {
    tsdb_bind->buffer_length = tsdb_field->bytes - 2 /*+ cnv->nr_to_terminator*/;
  } else {
    tsdb_bind->buffer_length = tsdb_field->bytes + 8;
  }

  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_nchar(stmt_t *stmt, param_state_t *param_state)
{
  int i_param                             = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;

  SQLSMALLINT ValueType = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_CHAR:
      return _stmt_prepare_param_data_array_from_sql_c_char_to_tsdb_nchar(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sqlc_data_type(ValueType));
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

  SQLSMALLINT ValueType = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
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
          i_param + 1, sqlc_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_bigint(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int64_t);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_bigint(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;

  SQLSMALLINT ValueType = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_SBIGINT:
      return _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_bigint(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sqlc_data_type(ValueType));
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

  SQLSMALLINT ValueType = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
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
          i_param + 1, sqlc_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_int(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int32_t);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_smallint(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int16_t);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_tinyint(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int8_t);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_bool(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int8_t);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_from_sql_c_double_to_tsdb_float(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(float);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_from_sql_c_slong_to_tsdb_int(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int r = 0;

  tsdb_bind->buffer_type = tsdb_field->type;
  tsdb_bind->length = NULL;
  tsdb_bind->buffer_length = sizeof(int32_t);
  r = mem_keep(&param_column->mem, sizeof(char) * tsdb_bind->buffer_length * nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->buffer = param_column->mem.base;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_int(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;

  SQLSMALLINT ValueType = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_SLONG:
      return _stmt_prepare_param_data_array_from_sql_c_slong_to_tsdb_int(stmt, param_state);
    case SQL_C_SBIGINT:
      return _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_int(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sqlc_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_smallint(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;

  SQLSMALLINT ValueType = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_SBIGINT:
      return _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_smallint(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sqlc_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_tinyint(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;

  SQLSMALLINT ValueType = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_SBIGINT:
      return _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_tinyint(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sqlc_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_bool(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;

  SQLSMALLINT ValueType = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_SBIGINT:
      return _stmt_prepare_param_data_array_from_sql_c_sbigint_to_tsdb_bool(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sqlc_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array_by_tsdb_float(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;

  SQLSMALLINT ValueType = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_DOUBLE:
      return _stmt_prepare_param_data_array_from_sql_c_double_to_tsdb_float(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter#%d[%s] not implemented yet",
          i_param + 1, sqlc_data_type(ValueType));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_param_data_array(stmt_t *stmt, param_state_t *param_state)
{
  int nr_paramset_size                    = param_state->nr_paramset_size;
  int i_param                             = param_state->i_param;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  tsdb_param_column_t  *param_column      = param_state->param_column;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  if (i_param == 0 && stmt->tsdb_stmt.is_insert_stmt && stmt->tsdb_stmt.params.subtbl_required) {
    return SQL_SUCCESS;
  }

  int r = 0;

  r = mem_keep(&param_column->mem_is_null, sizeof(char)*nr_paramset_size);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  tsdb_bind->is_null = (char*)param_column->mem_is_null.base;
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

static SQLRETURN _stmt_conv_param_data_from_sqlc_sbigint_sql_timestamp_tsdb_timestamp(stmt_t *stmt, param_state_t *param_state, int64_t i64)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int64_t *tsdb_timestamp = (int64_t*)tsdb_bind->buffer;
  tsdb_timestamp[i_row - param_state->i_batch_offset] = i64;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_sbigint_sql_timestamp(stmt_t *stmt, param_state_t *param_state, int64_t i64)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      return _stmt_conv_param_data_from_sqlc_sbigint_sql_timestamp_tsdb_timestamp(stmt, param_state, i64);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_sbigint_tsdb_bigint(stmt_t *stmt, param_state_t *param_state, int64_t i64)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int64_t *tsdb_bigint = (int64_t*)tsdb_bind->buffer;
  tsdb_bigint[i_row - param_state->i_batch_offset] = i64;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_sbigint_sql_bigint(stmt_t *stmt, param_state_t *param_state, int64_t i64)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_BIGINT:
       return _stmt_conv_param_data_from_sqlc_sbigint_tsdb_bigint(stmt, param_state, i64);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_sbigint_tsdb_int(stmt_t *stmt, param_state_t *param_state, int32_t i32)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int32_t *v = (int32_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = i32;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_sbigint_sql_int(stmt_t *stmt, param_state_t *param_state, int64_t i64)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  if (i64 > INT_MAX || i64 < INT_MIN) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:parameter(#%d,#%d)[%s] to [%s] to [%s]",
        i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
    return SQL_ERROR;
  }

  int32_t i32 = (int32_t)i64;

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_INT:
       return _stmt_conv_param_data_from_sqlc_sbigint_tsdb_int(stmt, param_state, i32);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_sbigint_sql_varchar(stmt_t *stmt, param_state_t *param_state, int64_t i64)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  SQLULEN ColumnSize = IPD_record->DESC_LENGTH;

  char buf[128];
  snprintf(buf, sizeof(buf), "%" PRId64 "", i64);
  size_t len = strlen(buf);

  if (len > ColumnSize) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:parameter(#%d,#%d) too long",
        i_row + 1, i_param + 1);
    return SQL_ERROR;
  }

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_BIGINT:
       return _stmt_conv_param_data_from_sqlc_sbigint_tsdb_bigint(stmt, param_state, i64);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_sbigint_tsdb_smallint(stmt_t *stmt, param_state_t *param_state, int16_t i16)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int16_t *v = (int16_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = i16;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_sbigint_sql_smallint(stmt_t *stmt, param_state_t *param_state, int64_t i64)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  if (i64 > INT16_MAX || i64 < INT16_MIN) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:parameter(#%d,#%d)[%s] to [%s] to [%s]",
        i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
    return SQL_ERROR;
  }

  int16_t i16 = (int16_t)i64;

  (void)i16;

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_SMALLINT:
       return _stmt_conv_param_data_from_sqlc_sbigint_tsdb_smallint(stmt, param_state, i16);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_sbigint_tsdb_tinyint(stmt_t *stmt, param_state_t *param_state, int8_t i8)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int8_t *v = (int8_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = i8;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_sbigint_tsdb_double(stmt_t *stmt, param_state_t *param_state, double dbl)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  double *v = (double*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = dbl;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_sbigint_tsdb_bool(stmt_t *stmt, param_state_t *param_state, int8_t b)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int8_t *v = (int8_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = !!b;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_sbigint_sql_tinyint(stmt_t *stmt, param_state_t *param_state, int64_t i64)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  if (i64 > INT8_MAX || i64 < INT8_MIN) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:parameter(#%d,#%d)[%s] to [%s] to [%s]",
        i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
    return SQL_ERROR;
  }

  int8_t i8 = (int8_t)i64;

  (void)i8;

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_TINYINT:
      return _stmt_conv_param_data_from_sqlc_sbigint_tsdb_tinyint(stmt, param_state, i8);
    case TSDB_DATA_TYPE_DOUBLE:
      return _stmt_conv_param_data_from_sqlc_sbigint_tsdb_double(stmt, param_state, (double)i8);
    case TSDB_DATA_TYPE_BOOL:
      return _stmt_conv_param_data_from_sqlc_sbigint_tsdb_bool(stmt, param_state, !!i8);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_sbigint(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;

  int64_t i64 = *(int64_t*)param_state->sql_c_base;

  switch (ParameterType) {
    case SQL_TYPE_TIMESTAMP:
      return _stmt_conv_param_data_from_sqlc_sbigint_sql_timestamp(stmt, param_state, i64);
    case SQL_BIGINT:
      return _stmt_conv_param_data_from_sqlc_sbigint_sql_bigint(stmt, param_state, i64);
    case SQL_INTEGER:
      return _stmt_conv_param_data_from_sqlc_sbigint_sql_int(stmt, param_state, i64);
    case SQL_VARCHAR:
      return _stmt_conv_param_data_from_sqlc_sbigint_sql_varchar(stmt, param_state, i64);
    case SQL_SMALLINT:
      return _stmt_conv_param_data_from_sqlc_sbigint_sql_smallint(stmt, param_state, i64);
    case SQL_TINYINT:
      return _stmt_conv_param_data_from_sqlc_sbigint_sql_tinyint(stmt, param_state, i64);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sql_varchar_tsdb_varchar(stmt_t *stmt, param_state_t *param_state, const char *s, size_t len)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  const char *fromcode = conn_get_sqlc_charset(stmt->conn);
  const char *tocode   = conn_get_tsdb_charset(stmt->conn);
  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  char *tsdb_varchar = tsdb_bind->buffer;
  tsdb_varchar += (i_row - param_state->i_batch_offset) * tsdb_bind->buffer_length;
  size_t tsdb_varchar_len = tsdb_bind->buffer_length;

  size_t         inbytes             = len;
  size_t         outbytes            = tsdb_varchar_len;

  size_t         inbytesleft         = inbytes;
  size_t         outbytesleft        = outbytes;

  char          *inbuf               = (char*)s;
  char          *outbuf              = (char*)tsdb_varchar;

  size_t n = CALL_iconv(cnv->cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
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
    stmt_append_err_format(stmt, "22001", 0,
        "String data, right truncated:[iconv]Character set conversion for `%s` to `%s`, #%zd out of #%zd bytes consumed, #%zd out of #%zd bytes converted:[%d]%s",
        cnv->from, cnv->to, inbytes - inbytesleft, inbytes, outbytes - outbytesleft, outbytes, e, strerror(e));
    return SQL_SUCCESS_WITH_INFO;
  }

  if (tsdb_bind->length) {
    tsdb_bind->length[i_row - param_state->i_batch_offset] = (int32_t)(outbytes - outbytesleft);
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_char_sql_varchar(stmt_t *stmt, param_state_t *param_state, const char *s, size_t len)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  SQLULEN ColumnSize = IPD_record->DESC_LENGTH;
  if (len > ColumnSize) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:parameter(#%d,#%d) too long",
        i_row + 1, i_param + 1);
    return SQL_ERROR;
  }

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_VARCHAR:
      return _stmt_conv_param_data_from_sql_varchar_tsdb_varchar(stmt, param_state, s, len);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sql_wvarchar_tsdb_nchar(stmt_t *stmt, param_state_t *param_state, const unsigned char *ucs2le, size_t len)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  const char *fromcode = "UCS-2LE";
  const char *tocode   = conn_get_tsdb_charset(stmt->conn);
  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  char *tsdb_varchar = tsdb_bind->buffer;
  tsdb_varchar += (i_row - param_state->i_batch_offset) * tsdb_bind->buffer_length;
  size_t tsdb_varchar_len = tsdb_bind->buffer_length;

  size_t         inbytes             = len;
  size_t         outbytes            = tsdb_varchar_len;

  size_t         inbytesleft         = inbytes;
  size_t         outbytesleft        = outbytes;

  char          *inbuf               = (char*)ucs2le;
  char          *outbuf              = (char*)tsdb_varchar;

  size_t n = CALL_iconv(cnv->cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
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
    stmt_append_err_format(stmt, "22001", 0,
        "String data, right truncated:[iconv]Character set conversion for `%s` to `%s`, #%zd out of #%zd bytes consumed, #%zd out of #%zd bytes converted:[%d]%s",
        cnv->from, cnv->to, inbytes - inbytesleft, inbytes, outbytes - outbytesleft, outbytes, e, strerror(e));
    return SQL_SUCCESS_WITH_INFO;
  }

  if (tsdb_bind->length) {
    tsdb_bind->length[i_row - param_state->i_batch_offset] = (int32_t)(outbytes - outbytesleft);
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_char_sql_wvarchar(stmt_t *stmt, param_state_t *param_state, const char *s, size_t len)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  SQLULEN ColumnSize = IPD_record->DESC_LENGTH;
  mem_t *sqlc_to_sql = &param_state->sqlc_to_sql;
  mem_reset(sqlc_to_sql);

  const char *fromcode = conn_get_sqlc_charset(stmt->conn);
  const char *tocode   = "UCS-2LE";
  charset_conv_t *cnv  = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:conversion for `%s` to `%s` not found or out of memory", fromcode, tocode);
    return SQL_ERROR;
  }

  int r = mem_conv(sqlc_to_sql, cnv->cnv, s, len);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }
  if (sqlc_to_sql->nr / 2 > ColumnSize) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:parameter(#%d,#%d) too long",
        i_row + 1, i_param + 1);
    return SQL_ERROR;
  }

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_NCHAR:
      return _stmt_conv_param_data_from_sql_wvarchar_tsdb_nchar(stmt, param_state, sqlc_to_sql->base, sqlc_to_sql->nr);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_char_tsdb_timestamp(stmt_t *stmt, param_state_t *param_state, const char *s, size_t len)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;


  SQLRETURN sr = SQL_SUCCESS;

  char *tsdb_timestamp = tsdb_bind->buffer;
  tsdb_timestamp += (i_row - param_state->i_batch_offset) * tsdb_bind->buffer_length;

  sr = _stmt_conv_sql_c_char_to_tsdb_timestamp_x(stmt, s, len, tsdb_field->precision, (int64_t*)tsdb_timestamp);
  return sr;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_char_sql_timestamp(stmt_t *stmt, param_state_t *param_state, const char *s, size_t len)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  (void)s; (void)len;

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      return _stmt_conv_param_data_from_sqlc_char_tsdb_timestamp(stmt, param_state, s, len);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sql_bigint_tsdb_varchar(stmt_t *stmt, param_state_t *param_state, int64_t i64)
{
  (void)stmt;

  char buf[128];
  snprintf(buf, sizeof(buf), "%" PRId64 "", i64);
  size_t len = strlen(buf);

  return _stmt_conv_param_data_from_sql_varchar_tsdb_varchar(stmt, param_state, buf, len);
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_char_sql_bigint(stmt_t *stmt, param_state_t *param_state, const char *s, size_t len)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  mem_t *sqlc_to_sql = &param_state->sqlc_to_sql;
  mem_reset(sqlc_to_sql);
  int r = mem_keep(sqlc_to_sql, len + 1);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  r = mem_copy_str(sqlc_to_sql, s, len);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  int64_t i64 = 0;

  SQLRETURN sr = SQL_SUCCESS;

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_VARCHAR:
      if (stmt->tsdb_stmt.is_insert_stmt) {
        sr = _stmt_strtoll(stmt, (const char*)sqlc_to_sql->base, &i64);
        if (sr != SQL_SUCCESS) return SQL_ERROR;
        return _stmt_conv_param_data_from_sql_bigint_tsdb_varchar(stmt, param_state, i64);
      } else {
        return _stmt_conv_param_data_from_sql_varchar_tsdb_varchar(stmt, param_state, s, len);
      }
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sql_double_tsdb_varchar(stmt_t *stmt, param_state_t *param_state, double dbl)
{
  (void)stmt;

  char buf[128];
  snprintf(buf, sizeof(buf), "%lg", dbl);
  size_t len = strlen(buf);

  return _stmt_conv_param_data_from_sql_varchar_tsdb_varchar(stmt, param_state, buf, len);
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_char_sql_double(stmt_t *stmt, param_state_t *param_state, const char *s, size_t len)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  mem_t *sqlc_to_sql = &param_state->sqlc_to_sql;
  mem_reset(sqlc_to_sql);
  int r = mem_keep(sqlc_to_sql, len + 1);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  r = mem_copy_str(sqlc_to_sql, s, len);
  if (r) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  double dbl = 0;

  SQLRETURN sr = SQL_SUCCESS;

  sr = _stmt_strtod(stmt, (const char*)sqlc_to_sql->base, &dbl);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_VARCHAR:
      return _stmt_conv_param_data_from_sql_double_tsdb_varchar(stmt, param_state, dbl);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_char(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;

  const char *s = (const char*)param_state->sql_c_base;
  size_t len = param_state->sql_c_length;
  if (len == (size_t)SQL_NTS) len = strlen(s);

  switch (ParameterType) {
    case SQL_VARCHAR:
      return _stmt_conv_param_data_from_sqlc_char_sql_varchar(stmt, param_state, s, len);
    case SQL_WVARCHAR:
      return _stmt_conv_param_data_from_sqlc_char_sql_wvarchar(stmt, param_state, s, len);
    case SQL_TYPE_TIMESTAMP:
      return _stmt_conv_param_data_from_sqlc_char_sql_timestamp(stmt, param_state, s, len);
    case SQL_BIGINT:
      return _stmt_conv_param_data_from_sqlc_char_sql_bigint(stmt, param_state, s, len);
    case SQL_DOUBLE:
      return _stmt_conv_param_data_from_sqlc_char_sql_double(stmt, param_state, s, len);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_double_sql_timestamp_tsdb_timestamp(stmt_t *stmt, param_state_t *param_state, double dbl)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int64_t *tsdb_timestamp = (int64_t*)tsdb_bind->buffer;
  tsdb_timestamp[i_row - param_state->i_batch_offset] = dbl;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_double_sql_timestamp(stmt_t *stmt, param_state_t *param_state, double dbl)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      return _stmt_conv_param_data_from_sqlc_double_sql_timestamp_tsdb_timestamp(stmt, param_state, dbl);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_double_sql_double_tsdb_double(stmt_t *stmt, param_state_t *param_state, double dbl)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  double *v = (double*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = dbl;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_double_sql_double(stmt_t *stmt, param_state_t *param_state, double dbl)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_DOUBLE:
      return _stmt_conv_param_data_from_sqlc_double_sql_double_tsdb_double(stmt, param_state, dbl);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_double_sql_varchar(stmt_t *stmt, param_state_t *param_state, double dbl)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  SQLULEN ColumnSize = IPD_record->DESC_LENGTH;
  int tsdb_type = tsdb_field->type;

  char buf[256];
  snprintf(buf, sizeof(buf), "%lg", dbl);
  size_t len = strlen(buf);
  if (len > ColumnSize) {
    stmt_append_err_format(stmt, "HY000", 0,
        "General error:parameter(#%d,#%d) too long",
        i_row + 1, i_param + 1);
    return SQL_ERROR;
  }

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_DOUBLE:
      return _stmt_conv_param_data_from_sqlc_double_sql_double_tsdb_double(stmt, param_state, dbl);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_double_sql_double_tsdb_float(stmt_t *stmt, param_state_t *param_state, double dbl)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  float *v = (float*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = (float)dbl;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_double_sql_real(stmt_t *stmt, param_state_t *param_state, double dbl)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  (void)dbl;

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_FLOAT:
      return _stmt_conv_param_data_from_sqlc_double_sql_double_tsdb_float(stmt, param_state, dbl);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_double_sql_bigint(stmt_t *stmt, param_state_t *param_state, double dbl)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  if (dbl > INT64_MAX || dbl < INT64_MIN) {
    stmt_append_err_format(stmt, "22003", 0,
        "Numeric value out of range:parameter(#%d,#%d)[%s] to [%s] to [%s]",
        i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
    return SQL_ERROR;
  }

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_DOUBLE:
      return _stmt_conv_param_data_from_sqlc_double_sql_double_tsdb_double(stmt, param_state, dbl);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_double(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;

  double dbl = *(double*)param_state->sql_c_base;

  switch (ParameterType) {
    case SQL_TYPE_TIMESTAMP:
      return _stmt_conv_param_data_from_sqlc_double_sql_timestamp(stmt, param_state, dbl);
    case SQL_DOUBLE:
      return _stmt_conv_param_data_from_sqlc_double_sql_double(stmt, param_state, dbl);
    case SQL_VARCHAR:
      return _stmt_conv_param_data_from_sqlc_double_sql_varchar(stmt, param_state, dbl);
    case SQL_REAL:
      return _stmt_conv_param_data_from_sqlc_double_sql_real(stmt, param_state, dbl);
    case SQL_BIGINT:
      return _stmt_conv_param_data_from_sqlc_double_sql_bigint(stmt, param_state, dbl);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_slong_tsdb_int(stmt_t *stmt, param_state_t *param_state, int32_t i32)
{
  (void)stmt;

  int                   i_row             = param_state->i_row;
  TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  int32_t *v = (int32_t*)tsdb_bind->buffer;
  v[i_row - param_state->i_batch_offset] = i32;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_slong_sql_int(stmt_t *stmt, param_state_t *param_state, int32_t i32)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;
  TAOS_FIELD_E         *tsdb_field        = param_state->tsdb_field;
  // TAOS_MULTI_BIND      *tsdb_bind         = param_state->tsdb_bind;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;
  int tsdb_type = tsdb_field->type;

  (void)i32;

  switch (tsdb_type) {
    case TSDB_DATA_TYPE_INT:
      return _stmt_conv_param_data_from_sqlc_slong_tsdb_int(stmt, param_state, i32);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType), taos_data_type(tsdb_type));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_slong(stmt_t *stmt, param_state_t *param_state)
{
  int                   i_row             = param_state->i_row;
  int                   i_param           = param_state->i_param;
  desc_record_t        *APD_record        = param_state->APD_record;
  desc_record_t        *IPD_record        = param_state->IPD_record;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  SQLSMALLINT ParameterType = (SQLSMALLINT)IPD_record->DESC_CONCISE_TYPE;

  int32_t i32 = *(int32_t*)param_state->sql_c_base;

  switch (ParameterType) {
    // case SQL_TYPE_TIMESTAMP:
    //   return _stmt_conv_param_data_from_sqlc_sbigint_sql_timestamp(stmt, param_state, i64);
    // case SQL_BIGINT:
    //   return _stmt_conv_param_data_from_sqlc_sbigint_sql_bigint(stmt, param_state, i64);
    case SQL_INTEGER:
      return _stmt_conv_param_data_from_sqlc_slong_sql_int(stmt, param_state, i32);
    // case SQL_VARCHAR:
    //   return _stmt_conv_param_data_from_sqlc_sbigint_sql_varchar(stmt, param_state, i64);
    // case SQL_SMALLINT:
    //   return _stmt_conv_param_data_from_sqlc_sbigint_sql_smallint(stmt, param_state, i64);
    // case SQL_TINYINT:
    //   return _stmt_conv_param_data_from_sqlc_sbigint_sql_tinyint(stmt, param_state, i64);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:conversion from parameter(#%d,#%d)[%s] to [%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType), sql_data_type(ParameterType));
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_param_data_from_sqlc_to_sql_to_tsdb(stmt_t *stmt, param_state_t *param_state)
{
  int                        i_row             = param_state->i_row;
  int                        i_param           = param_state->i_param;
  desc_record_t             *APD_record        = param_state->APD_record;

  SQLSMALLINT ValueType     = (SQLSMALLINT)APD_record->DESC_CONCISE_TYPE;
  switch (ValueType) {
    case SQL_C_SBIGINT:
      return _stmt_conv_param_data_from_sqlc_sbigint(stmt, param_state);
    case SQL_C_CHAR:
      return _stmt_conv_param_data_from_sqlc_char(stmt, param_state);
    case SQL_C_DOUBLE:
      return _stmt_conv_param_data_from_sqlc_double(stmt, param_state);
    case SQL_C_SLONG:
      return _stmt_conv_param_data_from_sqlc_slong(stmt, param_state);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "General error:Parameter(#%d,#%d)[%s] not implemented yet",
          i_row + 1, i_param + 1, sqlc_data_type(ValueType));
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
          i_row + 1, i_param + 1, sqlc_data_type(APD_record->DESC_CONCISE_TYPE));
      return SQL_ERROR;
    }
    tsdb_bind->is_null[i_row - param_state->i_batch_offset] = 1;
    return SQL_SUCCESS;
  } else {
    if (tsdb_bind->is_null) {
      tsdb_bind->is_null[i_row - param_state->i_batch_offset] = 0;
    }
  }

  param_state->sql_c_base     = sql_c_base;
  param_state->sql_c_length   = sql_c_length;
  param_state->sql_c_is_null  = sql_c_is_null;

  return _stmt_conv_param_data_from_sqlc_to_sql_to_tsdb(stmt, param_state);
}

static SQLRETURN _stmt_execute_prepare_caches(stmt_t *stmt, param_state_t *param_state)
{
  SQLRETURN sr = SQL_SUCCESS;

  descriptor_t *APD = stmt_APD(stmt);

  descriptor_t *IPD = stmt_IPD(stmt);

  for (int j=0; j<param_state->nr_tsdb_fields; ++j) {
    param_state->i_param                        = j;
    param_state->APD_record = APD->records + j;
    param_state->IPD_record = IPD->records + j;
    param_state->tsdb_field = &stmt->paramset.params[j].tsdb_field;
    param_state->param_column = stmt->paramset.params + j;
    param_state->tsdb_bind = stmt->tsdb_binds.mbs + j;
    if (!stmt->tsdb_stmt.is_insert_stmt) {
      sr = _stmt_guess_tsdb_params(stmt, param_state);
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

  descriptor_t *APD = stmt_APD(stmt);
  // desc_header_t *APD_header = &APD->header;

  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;

  SQLUSMALLINT *param_status_ptr = IPD_header->DESC_ARRAY_STATUS_PTR;
  SQLULEN *params_processed_ptr = IPD_header->DESC_ROWS_PROCESSED_PTR;
  if (params_processed_ptr) *params_processed_ptr = 0;
  if (param_status_ptr) {
    for (int i=0; i<param_state->nr_paramset_size; ++i) {
      param_status_ptr[i + param_state->i_batch_offset] = SQL_PARAM_UNUSED;
    }
  }

  tsdb_params_t *tsdb_params = &stmt->tsdb_stmt.params;
  int with_info = 0;
  for (int i=0; i<param_state->nr_paramset_size; ++i) {
    int row_with_info = 0;
    sr = SQL_SUCCESS;
    param_state->i_row = i + param_state->i_batch_offset;
    for (int j=0; j<param_state->nr_tsdb_fields; ++j) {
      param_state->i_param                        = j;
      param_state->APD_record = APD->records + j;
      param_state->IPD_record = IPD->records + j;
      param_state->tsdb_field = &stmt->paramset.params[j].tsdb_field;
      param_state->param_column = stmt->paramset.params + j;
      param_state->tsdb_bind = stmt->tsdb_binds.mbs + j;
      if (j == 0 && stmt->tsdb_stmt.is_insert_stmt && stmt->tsdb_stmt.params.subtbl_required) {
        continue;
      }
      sr = _stmt_conv_param_data(stmt, param_state);
      switch (sr) {
        case SQL_SUCCESS:
          break;
        case SQL_SUCCESS_WITH_INFO:
          if (stmt->strict) {
            sr = SQL_ERROR;
            break;
          }
          row_with_info = 1;
          break;
        default:
          if (sr != SQL_ERROR) {
            stmt_append_err(stmt, "HY000", 0, "General error:internal logic error when processing paramset");
            sr = SQL_ERROR;
          }
          break;
      }
      if (sr == SQL_ERROR) break;
      if (j == 0 && i == 0 && stmt->tsdb_stmt.is_insert_stmt && tsdb_params->subtbl_required) {
        OA_NIY(tsdb_params->subtbl == NULL);
        OA_NIY(0);
      }
    }

    if (params_processed_ptr) *params_processed_ptr += 1;

    if (sr == SQL_ERROR) {
      if (param_status_ptr) param_status_ptr[i + param_state->i_batch_offset] = SQL_PARAM_ERROR;
      return SQL_ERROR;
    }

    if (row_with_info) {
      with_info = 1;
      if (!param_status_ptr) return SQL_ERROR;
      param_status_ptr[i + param_state->i_batch_offset] = SQL_PARAM_SUCCESS_WITH_INFO;
    } else {
      if (param_status_ptr) param_status_ptr[i + param_state->i_batch_offset] = SQL_PARAM_SUCCESS;
    }
  }

  if (with_info) return SQL_SUCCESS_WITH_INFO;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_prepare_params(stmt_t *stmt, param_state_t *param_state)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLSMALLINT n = 0;
  sr = _stmt_get_num_params(stmt, &n);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;

  sr = _stmt_execute_prepare_caches(stmt, param_state);
  if (sr == SQL_ERROR) return SQL_ERROR;

  sr = _stmt_execute_prepare_params(stmt, param_state);

  if (sr == SQL_ERROR) {
    SQLULEN params_processed = 0;
    SQLULEN *params_processed_ptr = IPD_header->DESC_ROWS_PROCESSED_PTR;
    if (params_processed_ptr) params_processed = *params_processed_ptr;

    if (params_processed<=1) return SQL_ERROR;

    int num = (int)(params_processed - 1);
    for (int i=0; i<n; ++i) {
      TAOS_MULTI_BIND *mbs = stmt->tsdb_binds.mbs + i;
      mbs->num = num;
    }
  }

  tsdb_params_t *tsdb_params = &stmt->tsdb_stmt.params;
  if (stmt->tsdb_stmt.is_insert_stmt) {
    if (tsdb_params->nr_tag_fields) {
      TAOS_MULTI_BIND *mbs = stmt->tsdb_binds.mbs + (!!stmt->tsdb_stmt.params.subtbl_required);
      r = CALL_taos_stmt_set_tags(stmt->tsdb_stmt.stmt, mbs);
      if (r) {
        stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->tsdb_stmt.stmt));
        return SQL_ERROR;
      }
    }
  }

  r = CALL_taos_stmt_bind_param_batch(stmt->tsdb_stmt.stmt, stmt->tsdb_binds.mbs + (!!stmt->tsdb_stmt.params.subtbl_required) + stmt->tsdb_stmt.params.nr_tag_fields);
  if (r) {
    stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->tsdb_stmt.stmt));
    return SQL_ERROR;
  }

  r = CALL_taos_stmt_add_batch(stmt->tsdb_stmt.stmt);
  if (r) {
    stmt_append_err_format(stmt, "HY000", r, "General error:[taosc]%s", CALL_taos_stmt_errstr(stmt->tsdb_stmt.stmt));
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_execute_re_bind_subtbl(stmt_t *stmt, const char *subtbl, size_t subtbl_len)
{
  tsdb_params_t *tsdb_params = &stmt->tsdb_stmt.params;

  TOD_SAFE_FREE(tsdb_params->subtbl);
  tsdb_params->subtbl = strndup(subtbl, subtbl_len);
  if (!tsdb_params->subtbl) {
    stmt_oom(stmt);
    return SQL_ERROR;
  }

  return tsdb_stmt_re_bind_subtbl(&stmt->tsdb_stmt);
}

static SQLRETURN _stmt_execute_with_params(stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;

  size_t nr_paramset_size = APD_header->DESC_ARRAY_SIZE;

  SQLSMALLINT n = 0;
  sr = _stmt_get_num_params(stmt, &n);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  if (n <= 0) {
    stmt_niy(stmt);
    return SQL_ERROR;
  }

  const char *subtbl = NULL;
  size_t subtbl_len  = 0;
  size_t i_offset = 0;

  param_state_t *param_state = &stmt->param_state;
  _param_state_reset(param_state);
  // param_state->nr_paramset_size          = (int)APD_header->DESC_ARRAY_SIZE;
  param_state->nr_tsdb_fields            = n;
  param_state->i_batch_offset            = 0;

  if (stmt->tsdb_stmt.is_insert_stmt && stmt->tsdb_stmt.params.subtbl_required) {
    desc_record_t *APD_record = APD->records + 0;
    SQLSMALLINT ValueType = APD_record->DESC_CONCISE_TYPE;
    if (ValueType != SQL_C_CHAR) {
      stmt_append_err_format(stmt, "HY000", 0, "General error:subtbl is required as `SQL_C_CHAR` type, but got ==[%s]==", sqlc_data_type(ValueType));
      return SQL_ERROR;
    }
    for (size_t i=0; i<nr_paramset_size; ++i) {
      char *sql_c_base;
      SQLLEN sql_c_length;
      int sql_c_is_null;
      _stmt_param_get(stmt, i, 0, &sql_c_base, &sql_c_length, &sql_c_is_null);
      if (sql_c_is_null) {
        stmt_append_err(stmt, "HY000", 0, "General error:subtbl is required, but got ==null==");
        return SQL_ERROR;
      }

      const char *base = sql_c_base;
      size_t len = sql_c_length;
      if (len == (size_t)SQL_NTS) len = strlen(base);

      if (subtbl == NULL) {
        subtbl     = base;
        subtbl_len = len;
        continue;
      }

      if (subtbl_len == len && strncmp(subtbl, base, len) == 0) continue;

      sr = _stmt_execute_re_bind_subtbl(stmt, subtbl, subtbl_len);
      if (sr != SQL_SUCCESS) return SQL_ERROR;

      param_state->i_batch_offset   = i_offset;
      param_state->nr_paramset_size = i - i_offset;

      sr = _stmt_prepare_params(stmt, param_state);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
      sr = stmt->base->execute(stmt->base);
      if (sr != SQL_SUCCESS) return SQL_ERROR;

      subtbl     = base;
      subtbl_len = len;
      i_offset   = i;
    }

    sr = _stmt_execute_re_bind_subtbl(stmt, subtbl, subtbl_len);
    if (sr != SQL_SUCCESS) return SQL_ERROR;
  }

  param_state->i_batch_offset = i_offset;
  param_state->nr_paramset_size = nr_paramset_size - i_offset;

  sr = _stmt_prepare_params(stmt, param_state);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  return stmt->base->execute(stmt->base);
}

static SQLRETURN _stmt_execute(stmt_t *stmt)
{
  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;

  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;

  if (APD_header->DESC_COUNT != IPD_header->DESC_COUNT) {
    stmt_append_err(stmt, "HY000", 0, "General error:internal logic error");
    return SQL_ERROR;
  }

  if (APD_header->DESC_COUNT > 0) {
    return _stmt_execute_with_params(stmt);
  }

  return stmt->base->execute(stmt->base);
}

static SQLRETURN _stmt_exec_direct(stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->taos);

  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;

  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;

  if (APD_header->DESC_COUNT != IPD_header->DESC_COUNT) {
    stmt_append_err(stmt, "HY000", 0, "General error:internal logic error");
    return SQL_ERROR;
  }

  sr = _stmt_prepare(stmt);
  if (sr == SQL_ERROR) return SQL_ERROR;

  sr = _stmt_execute(stmt);
  if (sr == SQL_ERROR) return SQL_ERROR;

  return _stmt_fill_IRD(stmt);
}

static SQLRETURN _stmt_exec_direct_sql(stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  _stmt_close_result(stmt);

  int prev = atomic_fetch_add(&stmt->conn->outstandings, 1);
  OA_ILE(prev >= 0);

  sr = _stmt_exec_direct(stmt);

  prev = atomic_fetch_sub(&stmt->conn->outstandings, 1);
  OA_ILE(prev >= 0);

  return sr;
}

static SQLRETURN _stmt_exec_direct_with_simple_sql(stmt_t *stmt)
{
  const sqlc_tsdb_t *sqlc_tsdb = &stmt->current_sql;

  const char *start = sqlc_tsdb->tsdb;
  const char *end   = sqlc_tsdb->tsdb + sqlc_tsdb->tsdb_bytes;
  if (end > start && start[0] == '!') {
    stmt_niy(stmt);
    return SQL_ERROR;
  }

  return _stmt_exec_direct_sql(stmt);
}

static SQLRETURN _stmt_prepare_topic(stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;
  sqlc_tsdb_t *sqlc_tsdb = &stmt->current_sql;

  const char *start = sqlc_tsdb->tsdb;
  const char *end   = sqlc_tsdb->tsdb + sqlc_tsdb->tsdb_bytes;

  if (sqlc_tsdb->qms) {
    stmt_append_err(stmt, "HY000", 0, "General error:parameterized-topic-consumer-statement not supported yet");
    return SQL_ERROR;
  }
  ext_parser_param_t param = {0};
  // param.ctx.debug_flex = 1;
  // param.ctx.debug_bison = 1;
  r = ext_parser_parse(start, end-start, &param);
  if (r) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:parsing:%.*s", (int)(end-start), start);
    stmt_append_err_format(stmt, "HY000", 0, "General error:location:(%d,%d)->(%d,%d)", param.ctx.row0, param.ctx.col0, param.ctx.row1, param.ctx.col1);
    stmt_append_err_format(stmt, "HY000", 0, "General error:failed:%.*s", (int)strlen(param.ctx.err_msg), param.ctx.err_msg);
    stmt_append_err(stmt, "HY000", 0, "General error:taos_odbc_extended syntax for `topic`: !topic [name]+ [{[key[=val];]*}]?");

    ext_parser_param_release(&param);
    return SQL_ERROR;
  }

  sr = topic_open(&stmt->topic, sqlc_tsdb, &param.topic_cfg);
  ext_parser_param_release(&param);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  stmt->base = &stmt->topic.base;

  return SQL_SUCCESS;
}

SQLRETURN stmt_prepare(stmt_t *stmt,
    SQLCHAR      *StatementText,
    SQLINTEGER    TextLength)
{
  SQLRETURN sr = SQL_SUCCESS;

  _stmt_unprepare(stmt);

  const char *sql = (const char*)StatementText;

  size_t len = TextLength;
  if (TextLength == SQL_NTS) len = strlen(sql);
  else                       len = strnlen(sql, TextLength);

  sqls_parser_param_t param = {0};
  // param.ctx.debug_flex = 1;
  // param.ctx.debug_bison = 1;
  param.sql_found = _stmt_sql_found;
  param.arg       = stmt;

  sr = _stmt_cache_and_parse(stmt, &param, sql, len);
  sqls_parser_param_release(&param);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sqls_t *sqls = &stmt->sqls;
  if (sqls->nr > 1) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:multiple statements in a single SQLPrepare not supported yet");
    return SQL_ERROR;
  }

  sr = _stmt_get_next_sql(stmt);
  if (sr == SQL_NO_DATA) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:empty sql statement");
    return SQL_ERROR;
  }

  if (stmt->tsdb_stmt.is_topic) {
    return _stmt_prepare_topic(stmt);
  }

  return _stmt_prepare(stmt);
}

SQLRETURN stmt_exec_direct(stmt_t *stmt, SQLCHAR *StatementText, SQLINTEGER TextLength)
{
  SQLRETURN sr = SQL_SUCCESS;

  // column-binds remain valid among executes
  _stmt_close_result(stmt);

  const char *sql = (const char*)StatementText;

  size_t len = TextLength;
  if (TextLength == SQL_NTS) len = strlen(sql);
  else                       len = strnlen(sql, TextLength);

  sqls_parser_param_t param = {0};
  // param.ctx.debug_flex = 1;
  // param.ctx.debug_bison = 1;
  param.sql_found = _stmt_sql_found;
  param.arg       = stmt;

  sr = _stmt_cache_and_parse(stmt, &param, sql, len);
  sqls_parser_param_release(&param);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sr = _stmt_get_next_sql(stmt);
  if (sr == SQL_NO_DATA) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:empty sql statement");
    return SQL_ERROR;
  }

  if (stmt->tsdb_stmt.is_topic) {
    sr = _stmt_prepare_topic(stmt);
    if (sr != SQL_SUCCESS) return SQL_ERROR;
    sr = _stmt_execute(stmt);
    if (sr == SQL_ERROR) return SQL_ERROR;
    return _stmt_fill_IRD(stmt);
  }

  return _stmt_exec_direct_with_simple_sql(stmt);
}

static SQLRETURN _stmt_set_row_array_size(stmt_t *stmt, SQLULEN row_array_size)
{
  if (row_array_size == 0) {
    stmt_append_err(stmt, "01S02", 0, "Option value changed:`0` for `SQL_ATTR_ROW_ARRAY_SIZE` is substituted by current value");
    return SQL_SUCCESS_WITH_INFO;
  }

  descriptor_t *ARD = stmt_ARD(stmt);
  desc_header_t *ARD_header = &ARD->header;
  ARD_header->DESC_ARRAY_SIZE = row_array_size;

  return SQL_SUCCESS;
}

SQLRETURN stmt_execute(stmt_t *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->taos);

  // NOTE: no need to check whether it's prepared or not, DM would have already checked

  sr = _stmt_execute(stmt);
  if (sr == SQL_ERROR) return SQL_ERROR;
  return _stmt_fill_IRD(stmt);
}

static SQLRETURN _stmt_set_cursor_type(stmt_t *stmt, SQLULEN cursor_type)
{
  switch (cursor_type) {
    case SQL_CURSOR_FORWARD_ONLY:
    case SQL_CURSOR_STATIC:
      return SQL_SUCCESS;
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
    case SQL_ATTR_QUERY_TIMEOUT:
      if ((SQLULEN)(uintptr_t)ValuePtr == 0) return SQL_SUCCESS;
      stmt_append_err_format(stmt, "01S02", 0, "Option value changed:`%zd` for `SQL_ATTR_QUERY_TIMEOUT` is substituted by `0`", (SQLULEN)(uintptr_t)ValuePtr);
      return SQL_SUCCESS_WITH_INFO;
    case SQL_ATTR_USE_BOOKMARKS:
      if ((SQLULEN)(uintptr_t)ValuePtr == SQL_UB_OFF) return SQL_SUCCESS;
      stmt_append_err_format(stmt, "HY000", 0, "General error:`%zd/%s` for `SQL_ATTR_USE_BOOKMARKS` is not supported yet",
          (SQLULEN)(uintptr_t)ValuePtr, sql_stmt_attr((SQLINTEGER)(uintptr_t)ValuePtr));
      return SQL_ERROR;
    default:
      stmt_append_err_format(stmt, "HYC00", 0, "Optional feature not implemented:`%s[0x%x/%d]` not supported yet", sql_stmt_attr(Attribute), Attribute, Attribute);
      return SQL_ERROR;
  }
}

#if (ODBCVER >= 0x0300)          /* { */
SQLRETURN stmt_get_attr(stmt_t *stmt,
           SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
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
      stmt_append_err_format(stmt, "HY000", 0, "General error:`%s[0x%x/%d]` not supported yet", sql_stmt_attr(Attribute), Attribute, Attribute);
      return SQL_ERROR;
  }
}
#endif                           /* } */

SQLRETURN stmt_free_stmt(stmt_t *stmt, SQLUSMALLINT Option)
{
  switch (Option) {
    case SQL_CLOSE:
      // TODO:
      // stmt_append_err_format(stmt, "01000", 0, "General warning:`%s[0x%x/%d]` not supported yet", sql_free_statement_option(Option), Option, Option);
      // return SQL_SUCCESS_WITH_INFO;
      _stmt_close_result(stmt);
      return SQL_SUCCESS;
    case SQL_UNBIND:
      _stmt_unbind_cols(stmt);
      return SQL_SUCCESS;
    case SQL_RESET_PARAMS:
      _stmt_reset_params(stmt);
      return SQL_SUCCESS;
    case SQL_DROP:
      // NOTE: don't know why windows odbc-driver-manager does not map SQL_DROP to SQLFreeHandle
      return stmt_free(stmt);
    default:
      stmt_append_err_format(stmt, "HY000", 0, "General error:`%s[0x%x/%d]` not supported yet", sql_free_statement_option(Option), Option, Option);
      return SQL_ERROR;
  }
}

SQLRETURN stmt_close_cursor(stmt_t *stmt)
{
  // TODO:
  // stmt_append_err(stmt, "24000", 0, "Invalid cursor state:no cursor is open");
  // return SQL_SUCCESS_WITH_INFO;
  _stmt_close_result(stmt);
  return SQL_SUCCESS;
}

void stmt_clr_errs(stmt_t *stmt)
{
  errs_clr(&stmt->errs);
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
  SQLRETURN sr = SQL_SUCCESS;

  _stmt_close_result(stmt);
  _stmt_reset_params(stmt);
  mem_reset(&stmt->raw);
  mem_reset(&stmt->tsdb_sql);

  sr = tables_open(&stmt->tables, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3, TableType, NameLength4);
  if (sr != SQL_SUCCESS) {
    _stmt_reset_tables(stmt);
    return SQL_ERROR;
  }

  stmt->base = &stmt->tables.base;

  return _stmt_fill_IRD(stmt);
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
  if (1) {
    *(SQLLEN*)DiagInfoPtr = SQL_ROW_NUMBER_UNKNOWN;
    return SQL_SUCCESS;
  }

  // FIXME: get or put?
  tsdb_res_t           *res          = &stmt->tsdb_stmt.res;
  tsdb_rows_block_t    *rows_block   = &res->rows_block;

  *(SQLLEN*)DiagInfoPtr = (SQLLEN)rows_block->pos;

  return SQL_SUCCESS;
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

SQLRETURN stmt_col_attribute(
    stmt_t         *stmt,
    SQLUSMALLINT    ColumnNumber,
    SQLUSMALLINT    FieldIdentifier,
    SQLPOINTER      CharacterAttributePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr,
    SQLLEN         *NumericAttributePtr)
{
  if (ColumnNumber == 0) {
    stmt_append_err_format(stmt, "HY000", 0, "General error:ColumnNumber #%d not supported yet", ColumnNumber);
    return SQL_ERROR;
  }

  descriptor_t *IRD = stmt_IRD(stmt);
  desc_record_t *IRD_record = IRD->records + ColumnNumber - 1;

  // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolattribute-function?view=sql-server-ver16#backward-compatibility
  switch(FieldIdentifier) {
    case SQL_DESC_TYPE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_TYPE;
      return SQL_SUCCESS;
    case SQL_DESC_CONCISE_TYPE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_CONCISE_TYPE;
      return SQL_SUCCESS;
    case SQL_DESC_OCTET_LENGTH:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_OCTET_LENGTH;
      return SQL_SUCCESS;
    // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolattribute-function?view=sql-server-ver16#backward-compatibility
    case SQL_DESC_LENGTH:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_LENGTH;
      return SQL_SUCCESS;
    case SQL_DESC_PRECISION:
    case SQL_COLUMN_PRECISION:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_PRECISION;
      return SQL_SUCCESS;
    // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolattribute-function?view=sql-server-ver16#backward-compatibility
    case SQL_DESC_SCALE:
    case SQL_COLUMN_SCALE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_SCALE;
      return SQL_SUCCESS;
    case SQL_DESC_AUTO_UNIQUE_VALUE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_AUTO_UNIQUE_VALUE;
      return SQL_SUCCESS;
    case SQL_DESC_UPDATABLE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_UPDATABLE;
      return SQL_SUCCESS;
    case SQL_DESC_NULLABLE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_NULLABLE;
      return SQL_SUCCESS;
    case SQL_DESC_CASE_SENSITIVE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_CASE_SENSITIVE;
      return SQL_SUCCESS;
    case SQL_DESC_FIXED_PREC_SCALE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_FIXED_PREC_SCALE;
      return SQL_SUCCESS;
    case SQL_DESC_COUNT:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_COUNT;
      return SQL_SUCCESS;
    case SQL_DESC_UNNAMED:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_UNNAMED;
      return SQL_SUCCESS;
    case SQL_DESC_DISPLAY_SIZE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_DISPLAY_SIZE;
      return SQL_SUCCESS;
    case SQL_DESC_SEARCHABLE:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_SEARCHABLE;
      return SQL_SUCCESS;
    case SQL_DESC_NAME:
      return _stmt_col_copy_string(stmt, IRD_record->DESC_NAME, sizeof(IRD_record->DESC_NAME), CharacterAttributePtr, BufferLength, StringLengthPtr);
    case SQL_DESC_LABEL: // FIXME: share the same result?
      return _stmt_col_copy_string(stmt, IRD_record->DESC_LABEL, sizeof(IRD_record->DESC_LABEL), CharacterAttributePtr, BufferLength, StringLengthPtr);
      return SQL_SUCCESS;
    case SQL_DESC_TYPE_NAME:
      return _stmt_col_copy_string(stmt, IRD_record->DESC_TYPE_NAME, sizeof(IRD_record->DESC_TYPE_NAME), CharacterAttributePtr, BufferLength, StringLengthPtr);
    case SQL_DESC_LOCAL_TYPE_NAME: // FIXME: share the same result?
      return _stmt_col_copy_string(stmt, IRD_record->DESC_LOCAL_TYPE_NAME, sizeof(IRD_record->DESC_LOCAL_TYPE_NAME), CharacterAttributePtr, BufferLength, StringLengthPtr);
    case SQL_DESC_NUM_PREC_RADIX:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_NUM_PREC_RADIX;
      return SQL_SUCCESS;
    case SQL_DESC_UNSIGNED:
      if (NumericAttributePtr) *NumericAttributePtr = IRD_record->DESC_UNSIGNED;
      return SQL_SUCCESS;
    case SQL_DESC_CATALOG_NAME:
      return _stmt_col_copy_string(stmt, IRD_record->DESC_CATALOG_NAME, sizeof(IRD_record->DESC_CATALOG_NAME), CharacterAttributePtr, BufferLength, StringLengthPtr);
    case SQL_DESC_SCHEMA_NAME:
      return _stmt_col_copy_string(stmt, IRD_record->DESC_SCHEMA_NAME, sizeof(IRD_record->DESC_SCHEMA_NAME), CharacterAttributePtr, BufferLength, StringLengthPtr);
    case SQL_DESC_TABLE_NAME:
      return _stmt_col_copy_string(stmt, IRD_record->DESC_TABLE_NAME, sizeof(IRD_record->DESC_TABLE_NAME), CharacterAttributePtr, BufferLength, StringLengthPtr);
    case SQL_DESC_BASE_COLUMN_NAME:
    case SQL_DESC_BASE_TABLE_NAME:
      if (_stmt_col_set_empty_string(stmt, CharacterAttributePtr, BufferLength, StringLengthPtr) != SQL_SUCCESS) break;
      return SQL_SUCCESS;
    case SQL_DESC_LITERAL_PREFIX:
      return _stmt_col_copy_string(stmt, IRD_record->DESC_LITERAL_PREFIX, sizeof(IRD_record->DESC_LITERAL_PREFIX), CharacterAttributePtr, BufferLength, StringLengthPtr);
    case SQL_DESC_LITERAL_SUFFIX:
      return _stmt_col_copy_string(stmt, IRD_record->DESC_LITERAL_SUFFIX, sizeof(IRD_record->DESC_LITERAL_SUFFIX), CharacterAttributePtr, BufferLength, StringLengthPtr);
    default:
      stmt_append_err_format(stmt, "HY000", 0, "General error:`%s[%d/0x%x]` not supported yet", sql_col_attribute(FieldIdentifier), FieldIdentifier, FieldIdentifier);
      return SQL_ERROR;
  }

  stmt_append_err_format(stmt, "HY000", 0, "General error:`%s[%d/0x%x]` for `%s` not supported yet",
      sql_col_attribute(FieldIdentifier), FieldIdentifier, FieldIdentifier,
      taos_data_type(IRD_record->tsdb_type));
  return SQL_ERROR;
}

SQLRETURN stmt_more_results(
    stmt_t         *stmt)
{
  SQLRETURN sr = SQL_SUCCESS;

  if (stmt->base == &stmt->topic.base) {
    sr = stmt->base->more_results(stmt->base);
    if (sr == SQL_NO_DATA) return SQL_NO_DATA;
    if (sr != SQL_SUCCESS) return SQL_ERROR;
    return _stmt_fill_IRD(stmt);
  }

  if (stmt->base == &stmt->tsdb_stmt.base) {
    sr = _stmt_get_next_sql(stmt);
    if (sr == SQL_NO_DATA) return SQL_NO_DATA;

    return _stmt_exec_direct_with_simple_sql(stmt);
  }

  return SQL_NO_DATA;
}

SQLRETURN stmt_columns(
    stmt_t         *stmt,
    SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
    SQLCHAR *TableName, SQLSMALLINT NameLength3,
    SQLCHAR *ColumnName, SQLSMALLINT NameLength4)
{
  SQLRETURN sr = SQL_SUCCESS;

  _stmt_close_result(stmt);
  _stmt_reset_params(stmt);
  mem_reset(&stmt->raw);
  mem_reset(&stmt->tsdb_sql);

  if (CatalogName && NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen((const char*)CatalogName);
  if (SchemaName && NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen((const char*)SchemaName);
  if (TableName && NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen((const char*)TableName);
  if (ColumnName && NameLength4 == SQL_NTS) NameLength4 = (SQLSMALLINT)strlen((const char*)ColumnName);

  sr = columns_open(&stmt->columns, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3, ColumnName, NameLength4);
  if (sr != SQL_SUCCESS) {
    _stmt_reset_columns(stmt);
    return SQL_ERROR;
  }

  stmt->base = &stmt->columns.base;

  return _stmt_fill_IRD(stmt);
}

#if (ODBCVER >= 0x0300)       /* { */
SQLRETURN stmt_bulk_operations(
    stmt_t             *stmt,
    SQLSMALLINT         Operation)
{
  stmt_append_err_format(stmt, "HY000", 0, "General error:Operation `%s[%d/0x%x]` not supported yet", sql_bulk_operation(Operation), Operation, Operation);
  return SQL_ERROR;
}

#endif                        /* } */

SQLRETURN stmt_column_privileges(
    stmt_t       *stmt,
    SQLCHAR      *CatalogName,
    SQLSMALLINT   NameLength1,
    SQLCHAR      *SchemaName,
    SQLSMALLINT   NameLength2,
    SQLCHAR      *TableName,
    SQLSMALLINT   NameLength3,
    SQLCHAR      *ColumnName,
    SQLSMALLINT   NameLength4)
{
  const char *catalog = (const char *)CatalogName;
  const char *schema  = (const char *)SchemaName;
  const char *table   = (const char *)TableName;
  const char *column  = (const char *)ColumnName;

  if (catalog == NULL) catalog = "";
  if (schema  == NULL) schema  = "";
  if (table   == NULL) table   = "";
  if (column  == NULL) column  = "";

  if (NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen(catalog);
  if (NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen(schema);
  if (NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen(table);
  if (NameLength4 == SQL_NTS) NameLength4 = (SQLSMALLINT)strlen(column);

  stmt_append_err(stmt, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN stmt_extended_fetch(
    stmt_t          *stmt,
    SQLUSMALLINT     FetchOrientation,
    SQLLEN           FetchOffset,
    SQLULEN         *RowCountPtr,
    SQLUSMALLINT    *RowStatusArray)
{
  (void)RowCountPtr;
  (void)RowStatusArray;
  stmt_append_err_format(stmt, "HY000", 0, "General error:FetchOrientation[%d/0x%x],FetchOffset[%zd/0x%zx] not supported yet",
      FetchOrientation, FetchOrientation, FetchOffset, FetchOffset);
  return SQL_ERROR;
}

SQLRETURN stmt_foreign_keys(
    stmt_t        *stmt,
    SQLCHAR       *PKCatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *PKSchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *PKTableName,
    SQLSMALLINT    NameLength3,
    SQLCHAR       *FKCatalogName,
    SQLSMALLINT    NameLength4,
    SQLCHAR       *FKSchemaName,
    SQLSMALLINT    NameLength5,
    SQLCHAR       *FKTableName,
    SQLSMALLINT    NameLength6)
{
  (void)stmt;

  const char *pkcatalog            = (const char*)PKCatalogName;
  const char *pkschema             = (const char*)PKSchemaName;
  const char *pktable              = (const char*)PKTableName;
  const char *fkcatalog            = (const char*)FKCatalogName;
  const char *fkschema             = (const char*)FKSchemaName;
  const char *fktable              = (const char*)FKTableName;

  if (!pkcatalog)             pkcatalog = "";
  if (!pkschema)              pkschema  = "";
  if (!pktable)               pktable   = "";
  if (!fkcatalog)             fkcatalog = "";
  if (!fkschema)              fkschema  = "";
  if (!fktable)               fktable   = "";

  if (NameLength1 == SQL_NTS)       NameLength1 = (SQLSMALLINT)strlen(pkcatalog);
  if (NameLength2 == SQL_NTS)       NameLength2 = (SQLSMALLINT)strlen(pkschema);
  if (NameLength3 == SQL_NTS)       NameLength3 = (SQLSMALLINT)strlen(pktable);
  if (NameLength4 == SQL_NTS)       NameLength4 = (SQLSMALLINT)strlen(fkcatalog);
  if (NameLength5 == SQL_NTS)       NameLength5 = (SQLSMALLINT)strlen(fkschema);
  if (NameLength6 == SQL_NTS)       NameLength6 = (SQLSMALLINT)strlen(fktable);

  return SQL_NO_DATA;
}

SQLRETURN stmt_get_cursor_name(
    stmt_t       *stmt,
    SQLCHAR      *CursorName,
    SQLSMALLINT   BufferLength,
    SQLSMALLINT  *NameLengthPtr)
{
  (void)CursorName;
  (void)BufferLength;
  (void)NameLengthPtr;
  stmt_append_err(stmt, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN stmt_get_type_info(
    stmt_t       *stmt,
    SQLSMALLINT   DataType)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = typesinfo_open(&stmt->typesinfo, DataType);
  if (sr != SQL_SUCCESS) {
    _stmt_reset_typesinfo(stmt);
    return SQL_ERROR;
  }

  stmt->base = &stmt->typesinfo.base;

  return _stmt_fill_IRD(stmt);
}

SQLRETURN stmt_param_data(
    stmt_t       *stmt,
    SQLPOINTER   *Value)
{
  (void)Value;
  stmt_append_err(stmt, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN stmt_primary_keys(
    stmt_t        *stmt,
    SQLCHAR       *CatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *SchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *TableName,
    SQLSMALLINT    NameLength3)
{
  SQLRETURN sr = SQL_SUCCESS;

  _stmt_close_result(stmt);
  _stmt_reset_params(stmt);
  mem_reset(&stmt->raw);
  mem_reset(&stmt->tsdb_sql);

  if (CatalogName && NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen((const char*)CatalogName);
  if (SchemaName && NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen((const char*)SchemaName);
  if (TableName && NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen((const char*)TableName);

  sr = primarykeys_open(&stmt->primarykeys, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3);
  if (sr != SQL_SUCCESS) {
    _stmt_reset_primarykeys(stmt);
    return SQL_ERROR;
  }

  stmt->base = &stmt->primarykeys.base;

  return _stmt_fill_IRD(stmt);
}

SQLRETURN stmt_procedure_columns(
    stmt_t       *stmt,
    SQLCHAR      *CatalogName,
    SQLSMALLINT   NameLength1,
    SQLCHAR      *SchemaName,
    SQLSMALLINT   NameLength2,
    SQLCHAR      *ProcName,
    SQLSMALLINT   NameLength3,
    SQLCHAR      *ColumnName,
    SQLSMALLINT   NameLength4)
{
  const char *catalog = (const char *)CatalogName;
  const char *schema  = (const char *)SchemaName;
  const char *proc    = (const char *)ProcName;
  const char *column  = (const char *)ColumnName;

  if (catalog == NULL) catalog = "";
  if (schema  == NULL) schema  = "";
  if (proc    == NULL) proc    = "";
  if (column  == NULL) column  = "";

  if (NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen(catalog);
  if (NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen(schema);
  if (NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen(proc);
  if (NameLength4 == SQL_NTS) NameLength4 = (SQLSMALLINT)strlen(column);

  stmt_append_err(stmt, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN stmt_procedures(
    stmt_t         *stmt,
    SQLCHAR        *CatalogName,
    SQLSMALLINT     NameLength1,
    SQLCHAR        *SchemaName,
    SQLSMALLINT     NameLength2,
    SQLCHAR        *ProcName,
    SQLSMALLINT     NameLength3)
{
  const char *catalog = (const char *)CatalogName;
  const char *schema  = (const char *)SchemaName;
  const char *proc    = (const char *)ProcName;

  if (catalog == NULL) catalog = "";
  if (schema  == NULL) schema  = "";
  if (proc    == NULL) proc    = "";

  if (NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen(catalog);
  if (NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen(schema);
  if (NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen(proc);

  stmt_append_err(stmt, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN stmt_put_data(
    stmt_t         *stmt,
    SQLPOINTER      Data,
    SQLLEN          StrLen_or_Ind)
{
  (void)Data;
  (void)StrLen_or_Ind;
  stmt_append_err(stmt, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN stmt_set_cursor_name(
    stmt_t         *stmt,
    SQLCHAR        *CursorName,
    SQLSMALLINT     NameLength)
{
  const char *cursor         = (const char*)CursorName;
  if (!cursor) cursor = "";
  if (NameLength == SQL_NTS) NameLength = (SQLSMALLINT)strlen(cursor);
  stmt_append_err_format(stmt, "HY000", 0, "General error:CursorName[%.*s] not supported yet", NameLength, cursor);
  return SQL_ERROR;
}

SQLRETURN stmt_set_pos(
    stmt_t         *stmt,
    SQLSETPOSIROW   RowNumber,
    SQLUSMALLINT    Operation,
    SQLUSMALLINT    LockType)
{
  stmt_append_err_format(stmt, "HY000", 0, "General error:RowNumber[%zd],Operation `%s[%d/0x%x]`,LockType `%s[%d/0x%x]`",
      RowNumber, sql_pos_operation(Operation), Operation, Operation, sql_pos_locktype(LockType), LockType, LockType);
  return SQL_ERROR;
}

SQLRETURN stmt_special_columns(
    stmt_t         *stmt,
    SQLUSMALLINT    IdentifierType,
    SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
    SQLCHAR *TableName, SQLSMALLINT NameLength3,
    SQLUSMALLINT Scope, SQLUSMALLINT Nullable)
{
  const char *catalog = (const char *)CatalogName;
  const char *schema  = (const char *)SchemaName;
  const char *table   = (const char *)TableName;

  if (catalog == NULL) catalog = "";
  if (schema  == NULL) schema  = "";
  if (table   == NULL) table   = "";

  if (NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen(catalog);
  if (NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen(schema);
  if (NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen(table);

  stmt_append_err_format(stmt, "HY000", 0, "General error:Identifier `%s[%d/0x%x]`,Scope `%s[%d/0x%x]`,Nullable `%s[%d/0x%x]` not supported yet",
      sql_special_columns_identifier(IdentifierType), IdentifierType, IdentifierType,
      sql_scope(Scope), Scope, Scope,
      sql_nullable(Nullable), Nullable, Nullable);
  return SQL_ERROR;
}

SQLRETURN stmt_statistics(
    stmt_t  *stmt,
    SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
    SQLCHAR *TableName, SQLSMALLINT NameLength3,
    SQLUSMALLINT Unique, SQLUSMALLINT Reserved)
{
  const char *catalog = (const char *)CatalogName;
  const char *schema  = (const char *)SchemaName;
  const char *table   = (const char *)TableName;

  if (catalog == NULL) catalog = "";
  if (schema  == NULL) schema  = "";
  if (table   == NULL) table   = "";

  if (NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen(catalog);
  if (NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen(schema);
  if (NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen(table);

  stmt_append_err_format(stmt, "HY000", 0, "General error:Unique `%s[%d/0x%x]`,Reserved `%s[%d/0x%x]` not supported yet",
      sql_index(Unique), Unique, Unique,
      sql_statistics_reserved(Reserved), Reserved, Reserved);
  return SQL_ERROR;
}

SQLRETURN stmt_table_privileges(
    stmt_t  *stmt,
    SQLCHAR *CatalogName,
    SQLSMALLINT NameLength1,
    SQLCHAR *SchemaName,
    SQLSMALLINT NameLength2,
    SQLCHAR *TableName,
    SQLSMALLINT NameLength3)
{
  const char *catalog = (const char *)CatalogName;
  const char *schema  = (const char *)SchemaName;
  const char *table   = (const char *)TableName;

  if (catalog == NULL) catalog = "";
  if (schema  == NULL) schema  = "";
  if (table   == NULL) table   = "";

  if (NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen(catalog);
  if (NameLength2 == SQL_NTS) NameLength2 = (SQLSMALLINT)strlen(schema);
  if (NameLength3 == SQL_NTS) NameLength3 = (SQLSMALLINT)strlen(table);

  stmt_append_err(stmt, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN stmt_complete_async(
    stmt_t      *stmt,
    RETCODE     *AsyncRetCodePtr)
{
  (void)AsyncRetCodePtr;
  stmt_append_err(stmt, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

