#ifndef _internal_h_
#define _internal_h_

#include "env.h"
#include "conn.h"
#include "stmt.h"

#include <stdatomic.h>

#include <taos.h>

EXTERN_C_BEGIN

typedef struct err_s             err_t;
struct err_s {
  int                 err;
  const char         *estr;
  SQLCHAR             sql_state[6];
};

void err_set(err_t *err, int e, const char *estr, const char *sql_state);

struct env_s {
  atomic_int          refc;

  atomic_int          conns;
};

struct conn_s {
  atomic_int          refc;
  atomic_int          stmts;
  atomic_int          outstandings;

  env_t              *env;

  err_t               err;

  TAOS               *taos;

  unsigned int        fmt_time:1;
};

typedef struct sql_c_data_desc_s   sql_c_data_desc_t;

typedef SQLRETURN (*conv_f)(stmt_t *stmt, const char *data, int len, int row, sql_c_data_desc_t *desc);

struct sql_c_data_desc_s {
  SQLUSMALLINT   ColumnNumber;
  SQLSMALLINT    TargetType;
  SQLPOINTER     TargetValuePtr;
  SQLLEN         BufferLength;
  SQLLEN        *StrLen_or_IndPtr;
};

typedef struct col_bind_s          col_bind_t;

struct col_bind_s {
  sql_c_data_desc_t    desc;

  conv_f               conv;

  unsigned char        bounded:1;
};

typedef struct col_binds_s          col_binds_t;
struct col_binds_s {
  col_bind_t               *binds;
  size_t                    cap;
};

typedef struct rowset_s             rowset_t;
struct rowset_s {
  TAOS_ROW            rows;
  int                 nr_rows;

  int                 cursor;
};

struct stmt_s {
  atomic_int          refc;

  conn_t             *conn;

  err_t               err;

  TAOS_RES           *res;
  SQLLEN              row_count;
  SQLSMALLINT         col_count;
  TAOS_FIELD         *cols;
  int                *lengths;
  int                 time_precision;
  col_binds_t         col_binds;
  rowset_t            rowset;

  SQLULEN             row_array_size;
  SQLUSMALLINT       *row_status_ptr;
  SQLULEN             row_bind_type;
  SQLULEN            *rows_fetched_ptr;
};

EXTERN_C_END

#endif // _internal_h_

