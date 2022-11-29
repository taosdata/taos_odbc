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

#ifndef _internal_h_
#define _internal_h_

#include "os_port.h"
#include "enums.h"

#include "conn.h"
#include "desc.h"
#include "errs.h"
#include "env.h"
#include "list.h"
#include "stmt.h"

// make sure `log.h` is included ahead of `taos_helpers.h`, for the `LOG_IMPL` issue
#include "log.h"
#include "taos_helpers.h"

#include <taos.h>

EXTERN_C_BEGIN

// <TDengine/ include/util/tdef.h
// ...
// #define TSDB_NODE_NAME_LEN  64
// #define TSDB_TABLE_NAME_LEN 193  // it is a null-terminated string
// #define TSDB_TOPIC_NAME_LEN 193  // it is a null-terminated string
// #define TSDB_CGROUP_LEN     193  // it is a null-terminated string
// #define TSDB_DB_NAME_LEN    65
// #define TSDB_DB_FNAME_LEN   (TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN)
// ...
// #define TSDB_COL_NAME_LEN        65
// ...

#define MAX_CATALOG_NAME_LEN        64
#define MAX_SCHEMA_NAME_LEN         64
#define MAX_TABLE_NAME_LEN         192
#define MAX_COLUMN_NAME_LEN         64

typedef struct err_s             err_t;

struct err_s {
  int                         err;
  const char                 *estr;
  SQLCHAR                     sql_state[6];

  char                        buf[1024];

  struct tod_list_head        node;
};

struct errs_s {
  struct tod_list_head        errs;
  struct tod_list_head        frees;
};

#define conn_data_source(_conn) _conn->cfg.dsn ? _conn->cfg.dsn : (_conn->cfg.driver ? _conn->cfg.driver : "")

#define env_append_err(_env, _sql_state, _e, _estr) errs_append(&_env->errs, "", _sql_state, _e, _estr)

#define env_append_err_format(_env, _sql_state, _e, _fmt, ...) errs_append_format(&_env->errs, "", _sql_state, _e, _fmt, ##__VA_ARGS__)

#define env_oom(_env)                                 \
  do {                                                \
    env_t *__env = _env;                              \
    errs_oom(&__env->errs, "");                       \
  } while(0)

#define conn_append_err(_conn, _sql_state, _e, _estr)                                \
  do {                                                                               \
    conn_t *__conn = _conn;                                                          \
    errs_append(&__conn->errs, conn_data_source(__conn), _sql_state, _e, _estr);     \
  } while(0)

#define conn_append_err_format(_conn, _sql_state, _e, _fmt, ...)                                      \
  do {                                                                                                \
    conn_t *__conn = _conn;                                                                           \
    errs_append_format(&__conn->errs, conn_data_source(__conn), _sql_state, _e, _fmt, ##__VA_ARGS__); \
  } while (0)

#define conn_oom(_conn)                                  \
  do {                                                   \
    conn_t *__conn = _conn;                              \
    errs_oom(&__conn->errs, conn_data_source(__conn));   \
  } while (0)

#define conn_niy(_conn)                                        \
  do {                                                         \
    conn_t *__conn = _conn;                                    \
    errs_niy(&__conn->errs, conn_data_source(__conn));         \
  } while (0)

#define conn_nsy(_conn)                                        \
  do {                                                         \
    conn_t *__conn = _conn;                                    \
    errs_nsy(&__conn->errs, conn_data_source(__conn));         \
  } while (0)

#define stmt_append_err(_stmt, _sql_state, _e, _estr)                                \
  do {                                                                               \
    stmt_t *__stmt = _stmt;                                                          \
    conn_t *__conn = __stmt->conn;                                                   \
    errs_append(&__stmt->errs, conn_data_source(__conn), _sql_state, _e, _estr);     \
  } while (0)

#define stmt_append_err_format(_stmt, _sql_state, _e, _fmt, ...)                                      \
  do {                                                                                                \
    stmt_t *__stmt = _stmt;                                                                           \
    conn_t *__conn = __stmt->conn;                                                                    \
    errs_append_format(&__stmt->errs, conn_data_source(__conn), _sql_state, _e, _fmt, ##__VA_ARGS__); \
  } while (0)

#define stmt_oom(_stmt)                                        \
  do {                                                         \
    stmt_t *__stmt = _stmt;                                    \
    errs_oom(&__stmt->errs, conn_data_source(__stmt->conn));   \
  } while (0)

#define stmt_niy(_stmt)                                        \
  do {                                                         \
    stmt_t *__stmt = _stmt;                                    \
    errs_niy(&__stmt->errs, conn_data_source(__stmt->conn));   \
  } while (0)

#define stmt_nsy(_stmt)                                        \
  do {                                                         \
    stmt_t *__stmt = _stmt;                                    \
    errs_nsy(&__stmt->errs, conn_data_source(__stmt->conn));   \
  } while (0)



static inline int sql_succeeded(SQLRETURN sr)
{
  return sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO;
}


struct env_s {
  atomic_int          refc;

  atomic_int          conns;

  errs_t              errs;

  unsigned int        debug:1;
  unsigned int        debug_flex:1;
  unsigned int        debug_bison:1;
};

typedef struct desc_header_s                  desc_header_t;
struct desc_header_s {
  // header fields settable by SQLSetStmtAttr
  SQLULEN             DESC_ARRAY_SIZE;
  SQLUSMALLINT       *DESC_ARRAY_STATUS_PTR;
  SQLULEN            *DESC_BIND_OFFSET_PTR;
  SQLULEN             DESC_BIND_TYPE;
  SQLULEN            *DESC_ROWS_PROCESSED_PTR;

  // header fields else
  SQLUSMALLINT        DESC_COUNT;
};

typedef struct tsdb_to_sql_c_state_s      tsdb_to_sql_c_state_t;
struct tsdb_to_sql_c_state_s {
  TAOS_FIELD          *field;
  int                  i_col;
  int                  i_row;

  SQLSMALLINT          TargetType;
  SQLPOINTER           TargetValuePtr;
  SQLLEN               BufferLength;
  SQLLEN              *StrLen_or_IndPtr;

  const char          *data;
  int                  len;

  buffer_t             cache;
};

typedef SQLRETURN (*conv_from_tsdb_to_sql_c_f)(stmt_t *stmt, tsdb_to_sql_c_state_t *conv_state);

typedef struct sql_c_to_tsdb_meta_s                sql_c_to_tsdb_meta_t;

struct sql_c_to_tsdb_meta_s {
  char          *src_base;
  SQLLEN         src_len;
  desc_record_t *IPD_record;
  TAOS_FIELD_E  *field;
  char          *dst_base;
  int32_t       *dst_len;
};

struct desc_record_s {
  SQLSMALLINT                   DESC_TYPE;
  SQLSMALLINT                   DESC_CONCISE_TYPE;
  SQLULEN                       DESC_LENGTH;
  SQLSMALLINT                   DESC_PRECISION;
  SQLSMALLINT                   DESC_SCALE;
  SQLLEN                        DESC_OCTET_LENGTH;
  SQLPOINTER                    DESC_DATA_PTR;
  SQLLEN                       *DESC_INDICATOR_PTR;
  SQLLEN                       *DESC_OCTET_LENGTH_PTR;
  SQLSMALLINT                   DESC_PARAMETER_TYPE;

  conv_from_tsdb_to_sql_c_f     conv;

  buf_t                         data_buffer;
  buf_t                         len_buffer;
  buf_t                         ind_buffer;

  SQLRETURN (*create_buffer_array)(stmt_t *stmt, desc_record_t *record, int rows, TAOS_MULTI_BIND *mb);
  SQLRETURN (*create_length_array)(stmt_t *stmt, desc_record_t *record, int rows, TAOS_MULTI_BIND *mb);

  SQLRETURN (*convf)(stmt_t *stmt, sql_c_to_tsdb_meta_t *meta);

  unsigned int                  bound:1;
};

struct descriptor_s {
  desc_header_t                 header;

  desc_record_t                *records;
  size_t                        cap;
};

struct desc_s {
  atomic_int                    refc;

  descriptor_t                  descriptor;

  conn_t                       *conn;

  // https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/types-of-descriptors?view=sql-server-ver16
  // `A row descriptor in one statement can serve as a parameter descriptor in another statement.`
  struct tod_list_head          associated_stmts_as_ARD; // struct stmt_s*
  struct tod_list_head          associated_stmts_as_APD; // struct stmt_s*
};

struct conn_s {
  atomic_int          refc;
  atomic_int          stmts;
  atomic_int          descs;
  atomic_int          outstandings;

  env_t              *env;

  connection_cfg_t    cfg;

  errs_t              errs;

  TAOS               *taos;

  unsigned int        fmt_time:1;
};

typedef struct rowset_s             rowset_t;
struct rowset_s {
  int                 i_row;
};

// NOTE: this exists because of https://github.com/taosdata/TDengine/issues/17890
typedef struct post_filter_s        post_filter_t;
typedef SQLRETURN (*post_filter_f)(stmt_t *stmt, int row, void *ctx, int *filter);
typedef void (*post_filter_destroy_f)(stmt_t *stmt, void *ctx);

struct post_filter_s {
  void                   *ctx;
  post_filter_f           post_filter;
  post_filter_destroy_f   post_filter_destroy;
};

struct stmt_s {
  atomic_int                 refc;

  conn_t                    *conn;

  errs_t                     errs;

  struct tod_list_head       associated_APD_node;
  desc_t                    *associated_APD;

  struct tod_list_head       associated_ARD_node;
  desc_t                    *associated_ARD;

  descriptor_t               APD, IPD;
  descriptor_t               ARD, IRD;

  descriptor_t              *current_APD;
  descriptor_t              *current_ARD;

  tsdb_to_sql_c_state_t      current_for_get_data;
  buffer_t                   cache;

  char                      *sql;

  TAOS_STMT                 *stmt;
  // for non-insert-parameterized-statement
  int                        nr_params;

  // for insert-parameterized-statement
  char                      *subtbl;
  TAOS_FIELD_E              *tag_fields;
  int                        nr_tag_fields;
  TAOS_FIELD_E              *col_fields;
  int                        nr_col_fields;

  TAOS_MULTI_BIND           *mbs;
  size_t                     cap_mbs;
  size_t                     nr_mbs;

  post_filter_t              post_filter;

  TAOS_RES                  *res;
  SQLLEN                     affected_row_count;
  SQLSMALLINT                col_count;
  TAOS_FIELD                *fields;
  int                       *lengths;
  int                        time_precision;

  TAOS_ROW                   rows;
  int                        nr_rows;

  rowset_t                   rowset;

  unsigned int               prepared:1;
  unsigned int               is_insert_stmt:1;
  unsigned int               res_is_from_taos_query:1;
  unsigned int               subtbl_required:1;
};

EXTERN_C_END

#endif // _internal_h_

