#ifndef _internal_h_
#define _internal_h_

#include "enums.h"

#include "desc.h"
#include "env.h"
#include "conn.h"
#include "list.h"
#include "stmt.h"

#include <stdatomic.h>

#include <taos.h>

EXTERN_C_BEGIN

#define D_APPLY0(_func)                                 ({ OD("%s", #_func); _func(); })
#define D_APPLY1(_func, _v1)                            ({ OD("%s", #_func); _func(_v1); })
#define D_APPLY2(_func, _v1, _v2)                       ({ OD("%s", #_func); _func(_v1, _v2); })
#define D_APPLY3(_func, _v1, _v2, _v3)                  ({ OD("%s", #_func); _func(_v1, _v2, _v3); })
#define D_APPLY4(_func, _v1, _v2, _v3, _v4)             ({ OD("%s", #_func); _func(_v1, _v2, _v3, _v4); })
#define D_APPLY5(_func, _v1, _v2, _v3, _v4, _v5)        ({ OD("%s", #_func); _func(_v1, _v2, _v3, _v4, _v5); })

#define TAOS_get_server_info(_taos)                                D_APPLY1(taos_get_server_info, _taos)
#define TAOS_get_client_info(_taos)                                D_APPLY1(taos_get_client_info, _taos)

#define TAOS_init()                                                D_APPLY0(taos_init)
#define TAOS_cleanup()                                             D_APPLY0(taos_cleanup)

#define TAOS_data_type(_taos_type)                                 D_APPLY1(taos_data_type, _taos_type)

#define TAOS_affected_rows(_res)                                   D_APPLY1(taos_affected_rows, _res)
#define TAOS_field_count(_res)                                     D_APPLY1(taos_field_count, _res)
#define TAOS_result_precision(_res)                                D_APPLY1(taos_result_precision, _res)
#define TAOS_is_null(_res, _row, _col)                             D_APPLY3(taos_is_null, _res, _row, _col)
#define TAOS_get_column_data_offset(_res, _col)                    D_APPLY2(taos_get_column_data_offset, _res, _col)
#define TAOS_fetch_block(_res, _rows)                              D_APPLY2(taos_fetch_block, _res, _rows)
#define TAOS_fetch_lengths(_res)                                   D_APPLY1(taos_fetch_lengths, _res)

#define TAOS_num_fields(_result)                                   D_APPLY1(taos_num_fields, _result)
#define TAOS_fetch_fields(_result)                                 D_APPLY1(taos_fetch_fields, _result)
#define TAOS_fetch_row(_result)                                    D_APPLY1(taos_fetch_row, _result)
#define TAOS_print_row(_temp, _row, _fields, _num_fields)          D_APPLY4(taos_print_row, _temp, _row, _fields, _num_fields)
#define TAOS_query(_taos, _sql)     ({const char *__sql = _sql; OD("sql: [%s]", __sql); D_APPLY2(taos_query, _taos, __sql); })
#define TAOS_errno(_result)                                        D_APPLY1(taos_errno, _result)
#define TAOS_errstr(_result)                                       D_APPLY1(taos_errstr, _result)
#define TAOS_free_result(_result)                                  D_APPLY1(taos_free_result, _result)
#define TAOS_stmt_is_insert(_stmt, _isInsert)                      D_APPLY2(taos_stmt_is_insert, _stmt, _isInsert)
#define TAOS_stmt_errstr(_result)                                  D_APPLY1(taos_stmt_errstr, _result)
#define TAOS_stmt_num_params(_stmt, _num)                          D_APPLY2(taos_stmt_num_params, _stmt, _num)
#define TAOS_stmt_affected_rows(_stmt)                             D_APPLY1(taos_stmt_affected_rows, _stmt)
#define TAOS_stmt_affected_rows_once(_stmt)                        D_APPLY1(taos_stmt_affected_rows_once, _stmt)
#define TAOS_stmt_use_result(_stmt)                                D_APPLY1(taos_stmt_use_result, _stmt)
#define TAOS_stmt_get_param(_stmt, _i, _fieldType, _fieldBytes)    D_APPLY4(taos_stmt_get_param, _stmt, _i, _fieldType, _fieldBytes)
#define TAOS_stmt_get_tag_fields(_stmt, _fieldNum, _pFields)       D_APPLY3(taos_stmt_get_tag_fields, _stmt, _fieldNum, _pFields)
#define TAOS_stmt_get_col_fields(_stmt, _fieldNum, _pFields)       D_APPLY3(taos_stmt_get_col_fields, _stmt, _fieldNum, _pFields)
#define TAOS_stmt_bind_param_batch(_stmt, _bind)                   D_APPLY2(taos_stmt_bind_param_batch, _stmt, _bind)
#define TAOS_stmt_bind_single_param_batch(_stmt, _bind, _i)        D_APPLY3(taos_stmt_bind_single_param_batch, _stmt, _bind, _i)
#define TAOS_stmt_bind_param(_stmt, _bind)                         D_APPLY2(taos_stmt_bind_param, _stmt, _bind)
#define TAOS_stmt_set_tbname(_stmt, _tblName)                      D_APPLY2(taos_stmt_set_tbname, _stmt, _tblName)
#define TAOS_stmt_set_tags(_stmt, _pTags)                          D_APPLY2(taos_stmt_set_tags, _stmt, _pTags)
#define TAOS_stmt_set_tbname_tags(_stmt, _tblName, _pTags)         D_APPLY3(taos_stmt_set_tbname_tags, _stmt, _tblName, _pTags)
#define TAOS_stmt_prepare(_stmt, _sql, _len)                 \
  ({                                                         \
   const char *__sql = _sql;                                 \
   int __len = _len;                                         \
   OD("sql: [%.*s]",__len, __sql);                           \
   D_APPLY3(taos_stmt_prepare, _stmt, __sql, __len);         \
   })

#define TAOS_stmt_add_batch(_stmt)                                 D_APPLY1(taos_stmt_add_batch, _stmt)
#define TAOS_stmt_execute(_stmt)                                   D_APPLY1(taos_stmt_execute, _stmt)
#define TAOS_stmt_init(_stmt)                                      D_APPLY1(taos_stmt_init, _stmt)
#define TAOS_stmt_close(_stmt)                                     D_APPLY1(taos_stmt_close, _stmt)
#define TAOS_connect(_host, _uid, _pwd, _db, _port)                D_APPLY5(taos_connect, _host, _uid, _pwd, _db, _port)
#define TAOS_close(_stmt)                                          D_APPLY1(taos_close, _stmt)

typedef struct err_s             err_t;
typedef struct errs_s            errs_t;

struct err_s {
  int                         err;
  const char                 *estr;
  SQLCHAR                     sql_state[6];

  char                        buf[256];

  struct tod_list_head        node;
};

struct errs_s {
  struct tod_list_head        errs;
  struct tod_list_head        frees;
};

void errs_init(errs_t *errs) FA_HIDDEN;
void errs_append_x(errs_t *errs, const char *file, int line, const char *func, const char *data_source, const char *sql_state, int e, const char *estr) FA_HIDDEN;
void errs_clr_x(errs_t *errs) FA_HIDDEN;
void errs_release_x(errs_t *errs) FA_HIDDEN;
SQLRETURN errs_get_diag_rec_x(
    errs_t         *errs,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr) FA_HIDDEN;

#define errs_append(_errs, _data_source, _sql_state, _e, _estr)                              \
  ({                                                                                         \
    errs_append_x(_errs, __FILE__, __LINE__, __func__, _data_source, _sql_state, _e, _estr); \
  })

#define errs_append_format(_errs, _data_source, _sql_state, _e, _fmt, ...)    \
  ({                                                                          \
    char _buf[1024];                                                          \
    snprintf(_buf, sizeof(_buf), "" _fmt "", ##__VA_ARGS__);                  \
    errs_append(_errs, _data_source, _sql_state, _e, _buf);                   \
  })

#define errs_clr(_errs) errs_clr_x(_errs)

#define errs_release(_errs) errs_release_x(_errs)

#define errs_get_diag_rec(_errs, _RecNumber, _SQLSTATE, _NativeErrorPtr, _MessageText, _BufferLength, _TextLengthPtr) \
  errs_get_diag_rec_x(_errs, _RecNumber, _SQLSTATE, _NativeErrorPtr, _MessageText, _BufferLength, _TextLengthPtr)

#define conn_data_source(_conn) _conn->cfg.dsn ? _conn->cfg.dsn : (_conn->cfg.driver ? _conn->cfg.driver : "")

#define env_append_err(_env, _sql_state, _e, _estr)              \
  errs_append(&_env->errs, "", _sql_state, _e, _estr)

#define conn_append_err(_conn, _sql_state, _e, _estr)                                \
  ({                                                                                 \
    conn_t *__conn = _conn;                                                          \
    errs_append(&__conn->errs, conn_data_source(__conn), _sql_state, _e, _estr);     \
  })

#define stmt_append_err(_stmt, _sql_state, _e, _estr)                                \
  ({                                                                                 \
    stmt_t *__stmt = _stmt;                                                          \
    conn_t *__conn = __stmt->conn;                                                   \
    errs_append(&__stmt->errs, conn_data_source(__conn), _sql_state, _e, _estr);     \
  })

#define env_append_err_format(_env, _sql_state, _e, _fmt, ...)                \
  errs_append_format(&_env->errs, "", _sql_state, _e, _fmt, ##__VA_ARGS__)

#define conn_append_err_format(_conn, _sql_state, _e, _fmt, ...)                                      \
  ({                                                                                                  \
    conn_t *__conn = _conn;                                                                           \
    errs_append_format(&__conn->errs, conn_data_source(__conn), _sql_state, _e, _fmt, ##__VA_ARGS__); \
  })

#define stmt_append_err_format(_stmt, _sql_state, _e, _fmt, ...)                                      \
  ({                                                                                                  \
    stmt_t *__stmt = _stmt;                                                                           \
    conn_t *__conn = __stmt->conn;                                                                    \
    errs_append_format(&__stmt->errs, conn_data_source(__conn), _sql_state, _e, _fmt, ##__VA_ARGS__); \
  })


#define sql_succeeded(_sr) ({ SQLRETURN __sr = _sr; (__sr==SQL_SUCCESS || __sr==SQL_SUCCESS_WITH_INFO); })


typedef struct static_pool_s                   static_pool_t;
struct static_pool_s {
  size_t                        cap;
  size_t                        nr;
};

static_pool_t* static_pool_create(size_t cap) FA_HIDDEN;
void static_pool_destroy(static_pool_t *pool) FA_HIDDEN;
void static_pool_reset(static_pool_t *pool) FA_HIDDEN;
unsigned char* static_pool_malloc(static_pool_t *pool, size_t sz) FA_HIDDEN;
unsigned char* static_pool_calloc(static_pool_t *pool, size_t sz) FA_HIDDEN;
unsigned char* static_pool_malloc_align(static_pool_t *pool, size_t sz, size_t align) FA_HIDDEN;
unsigned char* static_pool_calloc_align(static_pool_t *pool, size_t sz, size_t align) FA_HIDDEN;

struct env_s {
  atomic_int          refc;

  atomic_int          conns;

  errs_t              errs;
};

typedef struct desc_header_s                  desc_header_t;
struct desc_header_s {
  // header fields settable by SQLSetStmtAttr
  SQLULEN             DESC_ARRAY_SIZE;
  SQLUSMALLINT       *DESC_STATUS_PTR;
  SQLULEN            *DESC_BIND_OFFSET_PTR;
  SQLULEN             DESC_BIND_TYPE;
  SQLULEN            *DESC_ROWS_FETCHED_PTR;

  // header fields else
  SQLUSMALLINT        DESC_COUNT;
};

typedef int (*tsdb_to_sql_c_f)(stmt_t *stmt, const char *data, int len, char *dest, int dlen);

typedef struct desc_record_s                  desc_record_t;
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

  SQLULEN                       element_size_in_column_wise;
  tsdb_to_sql_c_f               conv;

  int                           taos_type;
  int                           taos_bytes;
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

typedef struct param_value_s       param_value_t;
struct param_value_s {
  int32_t   length;
  char      is_null;
  union {
    int64_t               tsdb_timestamp;
    int32_t               tsdb_int;
    int64_t               tsdb_bigint;
    const char           *tsdb_varchar;
    const char           *tsdb_nchar;
    void                 *ptr;
  };
  unsigned int            inited:1;
  unsigned int            allocated:1;
};

typedef struct col_s                col_t;
struct col_s {
  TAOS_FIELD          *field;
  int                  i_col;
  const char          *data;
  int                  len;

  SQLSMALLINT          TargetType;

  char                *buf;
  size_t               cap;
  size_t               nr;
};

typedef struct rowset_s             rowset_t;
struct rowset_s {
  int                 i_row;
};

typedef struct stmt_attrs_s         stmt_attrs_t;
struct stmt_attrs_s {
  // SQLULEN                    ATTR_MAX_LENGTH;
};

typedef struct params_s                        params_t;
struct params_s {
  TAOS_MULTI_BIND     *mbs;
  param_value_t       *values;

  size_t               cap;
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

  stmt_attrs_t               attrs;
  col_t                      current_for_get_data;

  TAOS_STMT                 *stmt;
  int                        nr_params;
  params_t                   params;

  TAOS_RES                  *res;
  SQLLEN                     affected_row_count;
  SQLSMALLINT                col_count;
  TAOS_FIELD                *fields;
  int                       *lengths;
  int                        time_precision;

  TAOS_ROW                   rows;
  int                        nr_rows;

  rowset_t                   rowset;

  unsigned int               is_insert_stmt:1;
  unsigned int               res_is_from_taos_query:1;
};

EXTERN_C_END

#endif // _internal_h_

