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

#include "list.h"
#include "utils.h"

#include "typedefs.h"

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

#define desc_append_err(_desc, _sql_state, _e, _estr)                                      \
  do {                                                                                     \
    desc_t *__desc = _desc;                                                                \
    errs_append(&__desc->errs, conn_data_source(__desc->conn), _sql_state, _e, _estr);     \
  } while(0)

#define desc_append_err_format(_desc, _sql_state, _e, _fmt, ...)                                            \
  do {                                                                                                      \
    desc_t *__desc = _desc;                                                                                 \
    errs_append_format(&__desc->errs, conn_data_source(__desc->conn), _sql_state, _e, _fmt, ##__VA_ARGS__); \
  } while (0)

#define desc_oom(_desc)                                        \
  do {                                                         \
    desc_t *__desc = _desc;                                    \
    errs_oom(&__desc->errs, conn_data_source(__desc->conn));   \
  } while (0)

#define desc_niy(_desc)                                        \
  do {                                                         \
    desc_t *__desc = _desc;                                    \
    errs_niy(&__desc->errs, conn_data_source(__desc->conn));   \
  } while (0)

#define desc_nsy(_desc)                                        \
  do {                                                         \
    desc_t *__desc = _desc;                                    \
    errs_nsy(&__desc->errs, conn_data_source(__desc->conn));   \
  } while (0)



static inline int sql_succeeded(SQLRETURN sr)
{
  return sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO;
}

typedef struct sql_c_data_s             sql_c_data_t;
struct sql_c_data_s {
  // https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/c-data-types?view=sql-server-ver16
  SQLSMALLINT           type;
  union {
    uint8_t             b;
    int8_t              i8;
    uint8_t             u8;
    int16_t             i16;
    uint16_t            u16;
    int32_t             i32;
    uint32_t            u32;
    int64_t             i64;
    uint64_t            u64;
    float               flt;
    double              dbl;
    struct {
      const char *str;
      size_t      len;
    }                   str;
    int64_t             ts;
  };

  mem_t          mem;
  size_t         len;
  size_t         cur;

  uint8_t               is_null:1;
};

struct tsdb_data_s {
  int8_t                type;
  union {
    uint8_t             b;
    int8_t              i8;
    uint8_t             u8;
    int16_t             i16;
    uint16_t            u16;
    int32_t             i32;
    uint32_t            u32;
    int64_t             i64;
    uint64_t            u64;
    float               flt;
    double              dbl;
    struct {
      const char *str;
      size_t      len;
    }                   str;
    struct {
      int64_t     ts;
      int         precision;
    }                   ts;
  };

  uint8_t               is_null:1;
};

typedef enum {
  DATA_TYPE_UNKNOWM,
  DATA_TYPE_INT8,
  DATA_TYPE_UINT8,
  DATA_TYPE_INT16,
  DATA_TYPE_UINT16,
  DATA_TYPE_INT32,
  DATA_TYPE_UINT32,
  DATA_TYPE_INT64,
  DATA_TYPE_UINT64,
  DATA_TYPE_FLOAT,
  DATA_TYPE_DOUBLE,
  DATA_TYPE_STR,
  DATA_TYPE_MAX,
} data_type;

struct data_s {
  data_type             dt;
  union {
    int8_t              i8;
    uint8_t             u8;
    int16_t             i16;
    uint16_t            u16;
    int32_t             i32;
    uint32_t            u32;
    int64_t             i64;
    uint64_t            u64;
    float               flt;
    double              dbl;
    uintptr_t           str[3]; /* str[0]: a pointer; str[1]: a length; str[2]: charset */
  };
  uint8_t               is_null:1;
};

struct env_s {
  atomic_int          refc;

  atomic_int          conns;

  errs_t              errs;

  mem_t               mem;

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

typedef struct get_data_ctx_s            get_data_ctx_t;
struct get_data_ctx_s {
  SQLUSMALLINT   Col_or_Param_Num;
  SQLSMALLINT    TargetType;

  tsdb_data_t    tsdb;
  sql_c_data_t   sqlc;

  //
  char           buf[64];
  mem_t          mem;

  const char    *pos;
  size_t         nr;
};

typedef struct tsdb_param_column_s         tsdb_param_column_t;
struct tsdb_param_column_s {
  mem_t           mem;
  mem_t           mem_length;
  mem_t           mem_is_null;
  SQLRETURN (*conv)(stmt_t *stmt, int i_param);
};

typedef struct tsdb_paramset_s             tsdb_paramset_t;
struct tsdb_paramset_s {
  int                   cap;
  int                   nr;
  tsdb_param_column_t  *params;
};

struct tsdb_binds_s {
  int                        cap;
  int                        nr;

  TAOS_MULTI_BIND           *mbs;
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

  SQLSMALLINT                   DESC_AUTO_UNIQUE_VALUE;
  SQLSMALLINT                   DESC_UPDATABLE;
  SQLSMALLINT                   DESC_NULLABLE;
  SQLSMALLINT                   DESC_UNNAMED;

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

  errs_t                        errs;
};

struct charset_conv_s {
  char         from[64];
  char         to[64];
  iconv_t      cnv;

  struct tod_list_head        node;
};

struct charset_conv_mgr_s {
  struct tod_list_head        convs; // charset_conv_t*
};

struct connection_cfg_s {
  char                  *driver;
  char                  *dsn;
  char                  *uid;
  char                  *pwd;
  char                  *ip;
  char                  *db;
  int                    port;

  // NOTE: 1.this is to hack node.odbc, which maps SQL_TINYINT to SQL_C_UTINYINT
  //       2.node.odbc does not call SQLGetInfo/SQLColAttribute to get signess of integers
  unsigned int           unsigned_promotion:1;
  unsigned int           cache_sql:1;
};

struct parser_token_s {
  const char      *text;
  size_t           leng;
};

struct parser_param_s {
  connection_cfg_t       conn_str;

  int                    row0, col0;
  int                    row1, col1;
  char                  *errmsg;

  unsigned int           debug_flex:1;
  unsigned int           debug_bison:1;
};

struct conn_s {
  atomic_int          refc;
  atomic_int          stmts;
  atomic_int          descs;
  atomic_int          outstandings;

  env_t              *env;

  connection_cfg_t    cfg;

  // server info
  const char         *svr_info;

  // config from information_schema.ins_configs
  char               *s_statusInterval;
  char               *s_timezone;
  char               *s_locale;
  char               *s_charset;

  char               *sql_c_char_charset;
  char               *tsdb_varchar_charset;

  charset_conv_t      cnv_tsdb_varchar_to_sql_c_char;
  charset_conv_t      cnv_tsdb_varchar_to_utf8;
  charset_conv_t      cnv_tsdb_varchar_to_sql_c_wchar;

  charset_conv_t      cnv_sql_c_char_to_tsdb_varchar;
  charset_conv_t      cnv_sql_c_char_to_utf8;
  charset_conv_t      cnv_sql_c_char_to_sql_c_wchar;

  charset_conv_t      cnv_utf8_to_tsdb_varchar;
  charset_conv_t      cnv_utf8_to_sql_c_char;

  errs_t              errs;

  TAOS               *taos;

  unsigned int        fmt_time:1;
};

struct stmt_get_data_args_s {
  SQLUSMALLINT   Col_or_Param_Num;
  SQLSMALLINT    TargetType;
  SQLPOINTER     TargetValuePtr;
  SQLLEN         BufferLength;
  SQLLEN        *StrLenPtr;
  SQLLEN        *IndPtr;
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

struct stmt_base_s {
  SQLRETURN (*query)(stmt_base_t *base, const char *sql);
  SQLRETURN (*execute)(stmt_base_t *base);
  SQLRETURN (*fetch_rowset)(stmt_base_t *base, size_t rowset_size);
  SQLRETURN (*fetch_row)(stmt_base_t *base);
  void (*move_to_first_on_rowset)(stmt_base_t *base);
  SQLRETURN (*describe_param)(stmt_base_t *base,
      SQLUSMALLINT    ParameterNumber,
      SQLSMALLINT    *DataTypePtr,
      SQLULEN        *ParameterSizePtr,
      SQLSMALLINT    *DecimalDigitsPtr,
      SQLSMALLINT    *NullablePtr);
  SQLRETURN (*describe_col)(stmt_base_t *base,
      SQLUSMALLINT   ColumnNumber,
      SQLCHAR       *ColumnName,
      SQLSMALLINT    BufferLength,
      SQLSMALLINT   *NameLengthPtr,
      SQLSMALLINT   *DataTypePtr,
      SQLULEN       *ColumnSizePtr,
      SQLSMALLINT   *DecimalDigitsPtr,
      SQLSMALLINT   *NullablePtr);
  SQLRETURN (*col_attribute)(stmt_base_t *base,
      SQLUSMALLINT    ColumnNumber,
      SQLUSMALLINT    FieldIdentifier,
      SQLPOINTER      CharacterAttributePtr,
      SQLSMALLINT     BufferLength,
      SQLSMALLINT    *StringLengthPtr,
      SQLLEN         *NumericAttributePtr);
  SQLRETURN (*get_num_params)(stmt_base_t *base, SQLSMALLINT *ParameterCountPtr);
  SQLRETURN (*check_params)(stmt_base_t *base);
  SQLRETURN (*tsdb_field_by_param)(stmt_base_t *base, int i_param, TAOS_FIELD_E **field);
  SQLRETURN (*row_count)(stmt_base_t *base, SQLLEN *row_count_ptr);
  SQLRETURN (*get_num_cols)(stmt_base_t *base, SQLSMALLINT *ColumnCountPtr);
  SQLRETURN (*get_data)(stmt_base_t *base, SQLUSMALLINT Col_or_Param_Num, tsdb_data_t *tsdb);
};

struct tsdb_fields_s {
  TAOS_FIELD                *fields;
  size_t                     nr;
};

struct tsdb_rows_block_s {
  TAOS_ROW            rows;
  size_t              nr;
  size_t              rowset_size;
  size_t              block0;        // 0-based
  size_t              pos0;          // 1-based
  size_t              pos;           // 1-based
};

struct tsdb_res_s {
  TAOS_RES                  *res;
  size_t                     affected_row_count;
  int                        time_precision;
  tsdb_fields_t              fields;
  tsdb_rows_block_t          rows_block;

  unsigned int               res_is_from_taos_query:1;
};

struct tsdb_params_s {
  tsdb_stmt_t                        *owner;
  char                               *subtbl;
  TAOS_FIELD_E                        subtbl_field;
  TAOS_FIELD_E                       *tag_fields;
  int                                 nr_tag_fields;
  TAOS_FIELD_E                       *col_fields;
  int                                 nr_col_fields;

  mem_t                               mem;

  unsigned int                        prepared:1;
  unsigned int                        is_insert_stmt:1;
  unsigned int                        subtbl_required:1;
};

struct tsdb_stmt_s {
  stmt_base_t                base;

  stmt_t                    *owner;

  TAOS_STMT                 *stmt;
  // for insert-parameterized-statement
  tsdb_params_t              params;

  tsdb_binds_t               binds;

  tsdb_res_t                 res;

  unsigned int               prepared:1;
};

struct tables_args_s {
  wildex_t        *catalog_pattern;
  wildex_t        *schema_pattern;
  wildex_t        *table_pattern;
  wildex_t        *type_pattern;
};

typedef enum tables_type_e     tables_type_t;

enum tables_type_e {
  TABLES_FOR_GENERIC,
  TABLES_FOR_CATALOGS,
  TABLES_FOR_SCHEMAS,
  TABLES_FOR_TABLETYPES,
};

struct tables_s {
  stmt_base_t                base;
  stmt_t                    *owner;

  tables_args_t              tables_args;

  tsdb_stmt_t                stmt;

  size_t                     rowset_size;
  size_t                     pos; // 1-based
  size_t                     POS; // 1-based in whole set

  const unsigned char       *catalog;
  const unsigned char       *schema;
  const unsigned char       *table;
  const unsigned char       *type;

  mem_t                      catalog_cache;
  mem_t                      schema_cache;
  mem_t                      table_cache;
  mem_t                      type_cache;

  mem_t                      table_types;

  tables_type_t              tables_type;
};

struct columns_args_s {
  wildex_t        *catalog_pattern;
  wildex_t        *schema_pattern;
  wildex_t        *table_pattern;
  wildex_t        *column_pattern;
};

struct column_meta_s {
  const char                 *name;
  const char                 *column_type_name;
  SQLLEN                      DESC_CONCISE_TYPE;
  SQLLEN                      DESC_LENGTH;
  SQLLEN                      DESC_OCTET_LENGTH;
  SQLLEN                      DESC_PRECISION;
  SQLLEN                      DESC_SCALE;
  SQLLEN                      DESC_AUTO_UNIQUE_VALUE;
  SQLLEN                      DESC_UPDATABLE;
  SQLLEN                      DESC_NULLABLE;
  SQLLEN                      DESC_UNSIGNED;
  SQLLEN                      DESC_NUM_PREC_RADIX;
};

struct columns_s {
  stmt_base_t                base;
  stmt_t                    *owner;

  columns_args_t             columns_args;

  tables_t                   tables;

  tsdb_data_t                current_catalog;
  tsdb_data_t                current_schema;
  tsdb_data_t                current_table;
  tsdb_data_t                current_table_type;

  tsdb_data_t                current_col_name;
  tsdb_data_t                current_col_type;
  tsdb_data_t                current_col_length;
  tsdb_data_t                current_col_note;

  tsdb_stmt_t                desc;

  int                        ordinal_order;

  const char                *column;

  mem_t                      column_cache;
};

struct typesinfo_s {
  stmt_base_t                base;
  stmt_t                    *owner;

  SQLSMALLINT                data_type;

  size_t                     rowset_size;
  size_t                     pos; // 1-based
  size_t                     POS; // 1-based in whole set

  size_t                     IDX_of_first_in_rowset; // 0-based
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

  get_data_ctx_t             get_data_ctx;

  mem_t                      sql;

  tsdb_paramset_t                 paramset;

  tsdb_binds_t               tsdb_binds;

  post_filter_t              post_filter;

  tsdb_stmt_t                tsdb_stmt;
  tables_t                   tables;
  columns_t                  columns;
  typesinfo_t                typesinfo;

  mem_t                      mem;

  stmt_base_t               *base;

  unsigned int               strict:1; // 1: param-truncation as failure
};

struct tls_s {
  mem_t                      intermediate;
  charset_conv_mgr_t        *mgr;
};

EXTERN_C_END

#endif // _internal_h_

