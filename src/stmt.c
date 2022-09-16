#include "internal.h"

#include <errno.h>
#include <iconv.h>
#include <inttypes.h>
#include <limits.h>
#include <string.h>
#include <time.h>
#include <wchar.h>

#include <sqlext.h>

#define _stmt_malloc_fail(_stmt)             \
  err_set(&_stmt->err,                       \
    "HY001",                                 \
    0,                                       \
    "memory allocation failure");

static void rowset_reset(rowset_t *rowset)
{
  rowset->rows = NULL;
  rowset->nr_rows = 0;

  rowset->cursor = 0;
}

static void rowset_release(rowset_t *rowset)
{
  rowset->rows = NULL;
  rowset->nr_rows = 0;
}

static void param_bind_reset(param_bind_t *param_bind)
{
  if (param_bind->value.inited == 0) return;
  if (param_bind->value.allocated == 0) return;
  if (param_bind->value.ptr == NULL) return;
  if (param_bind->value.inited && param_bind->value.allocated && param_bind->value.ptr) {
    free(param_bind->value.ptr);
    param_bind->value.ptr = NULL;
    param_bind->value.allocated = 0;
    param_bind->value.inited = 0;
  }
}

static void param_binds_reset_param_bind(param_binds_t *param_binds)
{
  if (!param_binds->binds) return;
  for (size_t i=0; i<param_binds->cap; ++i) {
    param_bind_t *param_bind = param_binds->binds + i;
    param_bind_reset(param_bind);
  }
}

static void param_binds_reset(param_binds_t *param_binds)
{
  if (!param_binds->binds) return;
  param_binds_reset_param_bind(param_binds);
  memset(param_binds->binds, 0, sizeof(*param_binds->binds) * param_binds->cap);
}

static void param_binds_release(param_binds_t *param_binds)
{
  if (!param_binds->binds) return;
  param_binds_reset(param_binds);
  free(param_binds->binds);
  param_binds->binds = NULL;
  param_binds->cap = 0;
}

static int param_binds_bind_param(param_binds_t *param_binds, param_bind_t *param_bind)
{
  size_t cap = param_binds->cap;

  if (param_bind->ParameterNumber > param_binds->cap) {
    param_binds_reset_param_bind(param_binds);
    int cap = (param_bind->ParameterNumber + 15) / 16 * 16;
    param_bind_t *binds = (param_bind_t*)realloc(param_binds->binds, sizeof(*binds) * cap);
    if (!param_binds) return -1;
    param_binds->binds = binds;
    param_binds->cap = cap;
  }

  for (size_t i=cap; i<param_binds->cap; ++i) {
    param_bind_t *param_bind = param_binds->binds + i;
    param_bind->value.inited = 0;
  }

  param_binds->binds[param_bind->ParameterNumber - 1]         = *param_bind;
  param_binds->binds[param_bind->ParameterNumber - 1].bounded = 1;

  return 0;
}

static void col_binds_reset(col_binds_t *col_binds)
{
  if (!col_binds->binds) return;
  memset(col_binds->binds, 0, sizeof(*col_binds->binds) * col_binds->cap);
}

static void col_binds_release(col_binds_t *col_binds)
{
  if (!col_binds->binds) return;
  free(col_binds->binds);
  col_binds->binds = NULL;
  col_binds->cap = 0;
}

static int col_binds_bind_col(col_binds_t *col_binds, col_bind_t *col_bind)
{
  if (col_bind->ColumnNumber > col_binds->cap) {
    size_t cap = (col_bind->ColumnNumber + 15) / 16 * 16;
    col_bind_t *binds = (col_bind_t*)realloc(col_binds->binds, sizeof(*binds) * cap);
    if (!binds) return -1;
    col_binds->binds = binds;
    for (size_t i=col_binds->nr; i<cap; ++i) {
      col_binds->binds[i].valid = 0;
    }
    col_binds->cap = cap;
  }

  col_binds->binds[col_bind->ColumnNumber - 1]       = *col_bind;
  col_binds->binds[col_bind->ColumnNumber - 1].valid = !!(col_bind->TargetValuePtr);
  if (col_bind->ColumnNumber > col_binds->nr) col_binds->nr = col_bind->ColumnNumber;

  return 0;
}

static int stmt_init(stmt_t *stmt, conn_t *conn)
{
  stmt->conn = conn_ref(conn);
  int prev = atomic_fetch_add(&conn->stmts, 1);
  OA_ILE(prev >= 0);

  stmt->refc = 1;

  return 0;
}

static void stmt_release_result(stmt_t *stmt)
{
  if (stmt->res) {
    TAOS_free_result(stmt->res);
    stmt->res = NULL;
    stmt->row_count = 0;
    stmt->col_count = 0;
    stmt->cols = NULL;
    stmt->lengths = NULL;
    stmt->time_precision = 0;
  }
}

static void stmt_release_stmt(stmt_t *stmt)
{
  if (stmt->stmt) {
    int r = TAOS_stmt_close(stmt->stmt);
    OA_NIY(r == 0);
    stmt->stmt = NULL;
  }
}

static void stmt_release(stmt_t *stmt)
{
  rowset_release(&stmt->rowset);
  stmt_release_result(stmt);

  col_binds_release(&stmt->col_binds);
  param_binds_release(&stmt->param_binds);

  stmt_release_stmt(stmt);

  conn_unref(stmt->conn);
  int prev = atomic_fetch_sub(&stmt->conn->stmts, 1);
  OA_ILE(prev >= 1);
  stmt->conn = NULL;

  return;
}

stmt_t* stmt_create(conn_t *conn)
{
  OA_ILE(conn);

  stmt_t *stmt = (stmt_t*)calloc(1, sizeof(*stmt));
  if (!stmt) return NULL;

  int r = stmt_init(stmt, conn);
  if (r) {
    stmt_release(stmt);
    return NULL;
  }

  return stmt;
}

stmt_t* stmt_ref(stmt_t *stmt)
{
  OA_ILE(stmt);
  int prev = atomic_fetch_add(&stmt->refc, 1);
  OA_ILE(prev>0);
  return stmt;
}

stmt_t* stmt_unref(stmt_t *stmt)
{
  OA_ILE(stmt);
  int prev = atomic_fetch_sub(&stmt->refc, 1);
  if (prev>1) return stmt;
  OA_ILE(prev==1);

  stmt_release(stmt);
  free(stmt);

  return NULL;
}

static int _stmt_exec_direct(stmt_t *stmt, const char *sql, int len)
{
  OA_ILE(stmt);
  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->taos);
  OA_ILE(sql);

  if (stmt->rows_fetched_ptr) *stmt->rows_fetched_ptr = 0;
  rowset_reset(&stmt->rowset);
  stmt_release_result(stmt);
  param_binds_reset(&stmt->param_binds);

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
      if (!p) return -1;
    }
  }

  TAOS *taos = stmt->conn->taos;

  if (!stmt->stmt) {
    stmt->res = TAOS_query(taos, p);
  } else {
    OA_NIY(0);
    int r = TAOS_stmt_execute(stmt->stmt);
    if (r) {
      err_set(&stmt->err, "HY000", r, TAOS_stmt_errstr(stmt->stmt));
      if (p != sql && p!= buf) free(p);
      return -1;
    }
    stmt->res = TAOS_stmt_use_result(stmt->stmt);
  }

  int e;
  const char *estr;
  e = TAOS_errno(stmt->res);
  estr = TAOS_errstr(stmt->res);

  if (e == 0 && stmt->res) {
    stmt->row_count = TAOS_affected_rows(stmt->res);
    stmt->col_count = TAOS_field_count(stmt->res);
    if (stmt->col_count > 0) {
      stmt->cols = TAOS_fetch_fields(stmt->res);
    }
    stmt->time_precision = TAOS_result_precision(stmt->res);
  }

  if (p != sql && p!= buf) free(p);

  if (e) {
    err_set(&stmt->err, "HY000", e, estr);
  }

  return e ? -1 : 0;
}

int stmt_exec_direct(stmt_t *stmt, const char *sql, int len)
{
  OA_ILE(stmt);
  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->taos);
  OA_ILE(sql);

  int prev = atomic_fetch_add(&stmt->conn->outstandings, 1);
  OA_ILE(prev >= 0);

  int r = _stmt_exec_direct(stmt, sql, len);
  prev = atomic_fetch_sub(&stmt->conn->outstandings, 1);
  OA_ILE(prev >= 0);

  return r;
}

int stmt_set_row_array_size(stmt_t *stmt, SQLULEN row_array_size)
{
  stmt->row_array_size = row_array_size;
  return 0;
}

int stmt_set_row_status_ptr(stmt_t *stmt, SQLUSMALLINT *row_status_ptr)
{
  stmt->row_status_ptr = row_status_ptr;
  return 0;
}

int stmt_get_row_count(stmt_t *stmt, SQLLEN *row_count_ptr)
{
  *row_count_ptr = stmt->row_count;
  return 0;
}

int stmt_get_col_count(stmt_t *stmt, SQLSMALLINT *col_count_ptr)
{
  *col_count_ptr = stmt->col_count;
  return 0;
}

int stmt_set_row_bind_type(stmt_t *stmt, SQLULEN row_bind_type)
{
  if (row_bind_type != SQL_BIND_BY_COLUMN) {
    err_set(&stmt->err, "HY000", 0, "only `SQL_BIND_BY_COLUMN` is supported now`");
    return -1;
  }
  stmt->row_bind_type = row_bind_type;
  return 0;
}

int stmt_set_rows_fetched_ptr(stmt_t *stmt, SQLULEN *rows_fetched_ptr)
{
  stmt->rows_fetched_ptr = rows_fetched_ptr;
  return 0;
}

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
  TAOS_FIELD *p = stmt->cols + ColumnNumber - 1;
  int n;
  n = snprintf((char*)ColumnName, BufferLength, "%s", p->name);
  if (NameLengthPtr) {
    *NameLengthPtr = n;
  }
  if (NullablePtr) *NullablePtr = SQL_NULLABLE_UNKNOWN;
  switch (p->type) {
    case TSDB_DATA_TYPE_TINYINT:
      if (DataTypePtr)      *DataTypePtr = SQL_TINYINT;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      if (DataTypePtr)      *DataTypePtr = SQL_SMALLINT;
      break;
    case TSDB_DATA_TYPE_INT:
      if (DataTypePtr)      *DataTypePtr = SQL_INTEGER;
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
      // FIXME: better use WCHAR
      if (DataTypePtr)      *DataTypePtr   = SQL_WVARCHAR;
      // FIXME: plus 1 for null-terminator or NOT????
      //        does this belongs to node/odbc?
      //        https://www.npmjs.com/package/odbc         (2.4.4)
      if (ColumnSizePtr)    *ColumnSizePtr = p->bytes + 1;
      // FIXME: making ColumnSize big enough to help application allocate buffer
      // if (ColumnSizePtr)    *ColumnSizePtr = (p->bytes + 1) * 4;
      OD("taos_bytes:%d; ColumnSize: %ld", p->bytes, *ColumnSizePtr);

      // if (DataTypePtr)         *DataTypePtr   = SQL_VARCHAR;
      // // make application open much room to bind column
      // if (ColumnSizePtr)       *ColumnSizePtr = p->bytes * sizeof(wchar_t) + 1;
      // OA(0, "bytes: %d", p->bytes);
      break;
    default:
      OA(0, "`%s[%d]` for ColumnNumber[#%d] not implemented yet", taos_data_type(p->type), p->type, ColumnNumber);
      err_set_format(&stmt->err, "HY000", 0, "`%s[%d]` for ColumnNumber[#%d] not implemented yet", taos_data_type(p->type), p->type, ColumnNumber);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _conv_tsdb_int_to_sql_c_char(stmt_t *stmt, const char *data, int len, int row, col_bind_t *col_bind)
{
  OA_ILE(data);
  OA_ILE(len = sizeof(int32_t));
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  char *base = (char*)col_bind->TargetValuePtr;
  base += col_bind->BufferLength * row;
  int n = snprintf(base, col_bind->BufferLength, "%d", *(int32_t*)data);
  if (col_bind->StrLen_or_IndPtr) col_bind->StrLen_or_IndPtr[row] = n;
  if (n >= col_bind->BufferLength) {
    err_set(&stmt->err, "01004", 0, "String was truncated");
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _conv_tsdb_float_to_sql_c_char(stmt_t *stmt, const char *data, int len, int row, col_bind_t *col_bind)
{
  OA_ILE(data);
  OA_ILE(len = sizeof(int32_t));
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  char *base = (char*)col_bind->TargetValuePtr;
  base += col_bind->BufferLength * row;


  int n = snprintf(base, col_bind->BufferLength, "%g", *(float*)data);
  if (col_bind->StrLen_or_IndPtr) col_bind->StrLen_or_IndPtr[row] = n;
  if (n >= col_bind->BufferLength) {
    err_set(&stmt->err, "01004", 0, "String was truncated");
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _conv_tsdb_tinyint_to_sql_c_utinyint(stmt_t *stmt, const char *data, int len, int row, col_bind_t *col_bind)
{
  (void)len;
  OA_ILE(data);
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  char *base = (char*)col_bind->TargetValuePtr;
  base += col_bind->BufferLength * row;

  // FIXME: signess
  int8_t tinyint = *(int8_t*)data;
  *(int8_t*)base = tinyint;

  return SQL_SUCCESS;
}

static SQLRETURN _conv_tsdb_smallint_to_sql_c_short(stmt_t *stmt, const char *data, int len, int row, col_bind_t *col_bind)
{
  (void)len;
  OA_ILE(data);
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  char *base = (char*)col_bind->TargetValuePtr;
  base += col_bind->BufferLength * row;

  // FIXME: signess
  int16_t smallint = *(int16_t*)data;
  *(int16_t*)base = smallint;

  return SQL_SUCCESS;
}

static SQLRETURN _conv_tsdb_double_to_sql_c_double(stmt_t *stmt, const char *data, int len, int row, col_bind_t *col_bind)
{
  (void)len;
  OA_ILE(data);
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  char *base = (char*)col_bind->TargetValuePtr;
  base += col_bind->BufferLength * row;

  // FIXME: signess
  double v = *(double*)data;
  *(double*)base = v;

  return SQL_SUCCESS;
}

static SQLRETURN _conv_tsdb_varchar_to_sql_c_char(stmt_t *stmt, const char *data, int len, int row, col_bind_t *col_bind)
{
  OA_ILE(data);
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  char *base = (char*)col_bind->TargetValuePtr;
  base += col_bind->BufferLength * row;
  int n = snprintf(base, col_bind->BufferLength, "%.*s", len, data);
  if (col_bind->StrLen_or_IndPtr) col_bind->StrLen_or_IndPtr[row] = n;
  if (n >= col_bind->BufferLength) {
    err_set(&stmt->err, "01004", 0, "String was truncated");
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _conv_tsdb_nchar_to_sql_c_char(stmt_t *stmt, const char *data, int len, int row, col_bind_t *col_bind)
{
  OA_ILE(data);
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  char *base = (char*)col_bind->TargetValuePtr;
  base += col_bind->BufferLength * row;
  int n = snprintf(base, col_bind->BufferLength, "%.*s", len, data);
  OD("base[%s]; blen[%ld]; len[%d]; n[%d]", base, col_bind->BufferLength, len, n);
  if (col_bind->StrLen_or_IndPtr) col_bind->StrLen_or_IndPtr[row] = n;
  if (n >= col_bind->BufferLength) {
    err_set(&stmt->err, "01004", 0, "String was truncated");
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_encode(stmt_t *stmt,
    const char *fromcode, char **inbuf, size_t *inbytesleft,
    const char *tocode, char **outbuf, size_t *outbytesleft)
{
  iconv_t cd = iconv_open(tocode, fromcode);
  if ((size_t)cd == (size_t)-1) {
    err_set_format(&stmt->err, "HY000", 0,
        "[iconv] No character set conversion found for `%s` to `%s`: [%d] %s",
        fromcode, tocode, errno, strerror(errno));
    return SQL_ERROR;
  }

  size_t inbytes = *inbytesleft;
  size_t outbytes = *outbytesleft;
  size_t sz = iconv(cd, inbuf, inbytesleft, outbuf, outbytesleft);
  int e = errno;
  iconv_close(cd);
  if (*inbytesleft > 0) {
    err_set_format(&stmt->err, "22001", 0,
        "[iconv] Character set conversion for `%s` to `%s` results in string truncation, #%ld out of #%ld bytes consumed, #%ld out of #%ld bytes converted: [%d] %s",
        fromcode, tocode, inbytes - *inbytesleft, inbytes, outbytes - * outbytesleft, outbytes, e, strerror(e));
    return SQL_SUCCESS_WITH_INFO;
  }
  if (sz == (size_t)-1) {
    err_set_format(&stmt->err, "HY000", 0,
        "[iconv] Character set conversion for `%s` to `%s` failed: [%d] %s",
        fromcode, tocode, e, strerror(e));
    return SQL_ERROR;
  }
  if (sz > 0) {
    // FIXME: what actually means when sz > 0???
    err_set_format(&stmt->err, "HY000", 0,
        "[iconv] Character set conversion for `%s` to `%s` succeeded with #[%ld] of nonreversible characters converted",
        fromcode, tocode, sz);
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _conv_tsdb_nchar_to_sql_c_wchar(stmt_t *stmt, const char *data, int len, int row, col_bind_t *col_bind)
{
  OA_ILE(data);
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  char *base = (char*)col_bind->TargetValuePtr;
  base += col_bind->BufferLength * row;

  char *inbuf = (char*)data;
  size_t inbytes = len;
  char *outbuf = (char*)base;
  size_t outbytes = col_bind->BufferLength;

  SQLRETURN sr = _stmt_encode(stmt, "utf8", &inbuf, &inbytes, "ucs2", &outbuf, &outbytes);
  if (sql_successed(sr)) {
    if (col_bind->StrLen_or_IndPtr) col_bind->StrLen_or_IndPtr[row] = col_bind->BufferLength - outbytes;
  }

  return sr;
}

static SQLRETURN _conv_tsdb_varchar_to_sql_c_wchar(stmt_t *stmt, const char *data, int len, int row, col_bind_t *col_bind)
{
  OA_ILE(data);
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  char *base = (char*)col_bind->TargetValuePtr;
  base += col_bind->BufferLength * row;

  char *inbuf = (char*)data;
  size_t inbytes = len;
  char *outbuf = (char*)base;
  size_t outbytes = col_bind->BufferLength;

  SQLRETURN sr = _stmt_encode(stmt, "utf8", &inbuf, &inbytes, "ucs2", &outbuf, &outbytes);
  if (sql_successed(sr)) {
    if (col_bind->StrLen_or_IndPtr) col_bind->StrLen_or_IndPtr[row] = col_bind->BufferLength - outbytes;
  }

  return sr;
}

static SQLRETURN _conv_tsdb_int_to_sql_c_wchar(stmt_t *stmt, const char *data, int len, int row, col_bind_t *col_bind)
{
  (void)len;
  OA_ILE(data);
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  char *base = (char*)col_bind->TargetValuePtr;
  base += col_bind->BufferLength * row;

  char buf[64];
  int n = snprintf(buf, sizeof(buf), "%d", *(int32_t*)data);

  char *inbuf = buf;
  size_t inbytes = n;
  char *outbuf = (char*)base;
  size_t outbytes = col_bind->BufferLength;

  SQLRETURN sr = _stmt_encode(stmt, "utf8", &inbuf, &inbytes, "ucs2", &outbuf, &outbytes);
  if (sql_successed(sr)) {
    if (col_bind->StrLen_or_IndPtr) col_bind->StrLen_or_IndPtr[row] = col_bind->BufferLength - outbytes;
  }

  return sr;
}

static SQLRETURN _conv_tsdb_timestamp_to_sql_c_wchar(stmt_t *stmt, const char *data, int len, int row, col_bind_t *col_bind)
{
  (void)len;
  OA_ILE(data);
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  char *base = (char*)col_bind->TargetValuePtr;
  base += col_bind->BufferLength * row;

  char buf[64];
  int n = snprintf(buf, sizeof(buf), "%ld", *(int64_t*)data);

  char *inbuf = buf;
  size_t inbytes = n;
  char *outbuf = (char*)base;
  size_t outbytes = col_bind->BufferLength;

  SQLRETURN sr = _stmt_encode(stmt, "utf8", &inbuf, &inbytes, "ucs2", &outbuf, &outbytes);
  if (sr == SQL_SUCCESS_WITH_INFO) sr = SQL_ERROR;
  if (sql_successed(sr)) {
    if (col_bind->StrLen_or_IndPtr) col_bind->StrLen_or_IndPtr[row] = col_bind->BufferLength - outbytes;
  }

  return sr;
}

static SQLRETURN _conv_tsdb_timestamp_to_sql_c_char(stmt_t *stmt, const char *data, int len, int row, col_bind_t *col_bind)
{
  OA_ILE(data);
  OA(len == sizeof(int64_t), "");
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  char *base = (char*)col_bind->TargetValuePtr;
  base += col_bind->BufferLength * row;

  int64_t val = *(int64_t*)data;

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

  n = snprintf(base, col_bind->BufferLength,
      "%04d-%02d-%02d %02d:%02d:%02d.%0*d",
      ptm.tm_year + 1900, ptm.tm_mon + 1, ptm.tm_mday,
      ptm.tm_hour, ptm.tm_min, ptm.tm_sec,
      w, ms);

  if (col_bind->StrLen_or_IndPtr) col_bind->StrLen_or_IndPtr[row] = n;
  if (n >= col_bind->BufferLength) {
    OA_NIY(0);
    err_set(&stmt->err, "01004", 0, "String was truncated");
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_to_sql_c_char(stmt_t *stmt, int taos_type, conv_f *conv)
{
  (void)stmt;

  switch (taos_type) {
    case TSDB_DATA_TYPE_INT:
      *conv = _conv_tsdb_int_to_sql_c_char;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      *conv = _conv_tsdb_float_to_sql_c_char;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      *conv = _conv_tsdb_varchar_to_sql_c_char;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      *conv = _conv_tsdb_timestamp_to_sql_c_char;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      *conv = _conv_tsdb_nchar_to_sql_c_char;
      break;
    default:
      err_set_format(&stmt->err, "HY000", 0,
          "converstion from `%s[%d]` to `SQL_C_CHAR` not implemented yet",
          taos_data_type(taos_type), taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_to_sql_c_utinyint(stmt_t *stmt, int taos_type, conv_f *conv)
{
  switch (taos_type) {
    case TSDB_DATA_TYPE_TINYINT:
      *conv = _conv_tsdb_tinyint_to_sql_c_utinyint;
      break;
    default:
      err_set_format(&stmt->err, "HY000", 0,
          "converstion from `%s[%d]` to `SQL_C_CHAR` not implemented yet",
          taos_data_type(taos_type), taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_to_sql_c_short(stmt_t *stmt, int taos_type, conv_f *conv)
{
  switch (taos_type) {
    case TSDB_DATA_TYPE_SMALLINT:
      *conv = _conv_tsdb_smallint_to_sql_c_short;
      break;
    default:
      err_set_format(&stmt->err, "HY000", 0,
          "converstion from `%s[%d]` to `SQL_C_SHORT` not implemented yet",
          taos_data_type(taos_type), taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_to_sql_c_double(stmt_t *stmt, int taos_type, conv_f *conv)
{
  switch (taos_type) {
    case TSDB_DATA_TYPE_DOUBLE:
      *conv = _conv_tsdb_double_to_sql_c_double;
      break;
    default:
      err_set_format(&stmt->err, "HY000", 0,
          "converstion from `%s[%d]` to `SQL_C_SHORT` not implemented yet",
          taos_data_type(taos_type), taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _conv_tsdb_int_to_sql_c_slong(stmt_t *stmt, const char *data, int len, int row, col_bind_t *col_bind)
{
  OA(len == sizeof(int32_t), "");
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  SQLINTEGER *base = (SQLINTEGER*)col_bind->TargetValuePtr;
  base += row;
  *base = *(int32_t*)data;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_to_sql_c_slong(stmt_t *stmt, int taos_type, conv_f *conv)
{
  (void)stmt;

  switch (taos_type) {
    case TSDB_DATA_TYPE_INT:
      *conv = _conv_tsdb_int_to_sql_c_slong;
      break;
    default:
      err_set_format(&stmt->err, "HY000", 0,
          "converstion from `%s[%d]` to `SQL_C_SLONG` not implemented yet",
          taos_data_type(taos_type), taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv_to_sql_c_wchar(stmt_t *stmt, int taos_type, conv_f *conv)
{
  (void)stmt;

  switch (taos_type) {
    case TSDB_DATA_TYPE_NCHAR:
      *conv = _conv_tsdb_nchar_to_sql_c_wchar;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      *conv = _conv_tsdb_varchar_to_sql_c_wchar;
      break;
    case TSDB_DATA_TYPE_INT:
      *conv = _conv_tsdb_int_to_sql_c_wchar;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      *conv = _conv_tsdb_timestamp_to_sql_c_wchar;
      break;
    default:
      err_set_format(&stmt->err, "HY000", 0,
          "converstion from `%s[%d]` to `SQL_C_WCHAR` not implemented yet",
          taos_data_type(taos_type), taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_conv(stmt_t *stmt, SQLSMALLINT TargetType, int taos_type, conv_f *conv)
{
  switch (TargetType) {
    case SQL_C_UTINYINT:
      return _stmt_get_conv_to_sql_c_utinyint(stmt, taos_type, conv);
    case SQL_C_SHORT:
      return _stmt_get_conv_to_sql_c_short(stmt, taos_type, conv);
    case SQL_C_DOUBLE:
      return _stmt_get_conv_to_sql_c_double(stmt, taos_type, conv);
    case SQL_C_CHAR:
      return _stmt_get_conv_to_sql_c_char(stmt, taos_type, conv);
    case SQL_C_SLONG:
      return _stmt_get_conv_to_sql_c_slong(stmt, taos_type, conv);
    case SQL_C_WCHAR:
      return _stmt_get_conv_to_sql_c_wchar(stmt, taos_type, conv);
    default:
      // OA(0, "converstion to `%s[%d]` not implemented yet",
      //     sql_c_data_type_to_str(TargetType), TargetType);
      err_set_format(&stmt->err, "HY000", 0,
          "converstion to `%s[%d]` not implemented yet",
          sql_c_data_type_to_str(TargetType), TargetType);
      return SQL_ERROR;
      break;
  }
}

SQLRETURN stmt_bind_col(stmt_t *stmt,
    SQLUSMALLINT   ColumnNumber,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  col_bind_t col_bind = {};
  col_bind.ColumnNumber       = ColumnNumber;
  col_bind.TargetType         = TargetType;
  col_bind.TargetValuePtr     = TargetValuePtr;
  col_bind.BufferLength       = BufferLength;
  col_bind.StrLen_or_IndPtr   = StrLen_or_IndPtr;

  int r = col_binds_bind_col(&stmt->col_binds, &col_bind);
  if (r) {
    _stmt_malloc_fail(stmt);
    return SQL_ERROR;
  }
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_len(stmt_t *stmt, int row, int col, const char **data, int *len)
{
  TAOS_FIELD *field = stmt->cols + col;
  switch(field->type) {
    case TSDB_DATA_TYPE_TINYINT:
      if (TAOS_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        unsigned char *base = (unsigned char*)stmt->rowset.rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_SMALLINT:
      if (TAOS_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        int16_t *base = (int16_t*)stmt->rowset.rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_INT:
      if (TAOS_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        int32_t *base = (int32_t*)stmt->rowset.rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_FLOAT:
      if (TAOS_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        float *base = (float*)stmt->rowset.rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_DOUBLE:
      if (TAOS_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        double *base = (double*)stmt->rowset.rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_VARCHAR: {
      int *offsets = TAOS_get_column_data_offset(stmt->res, col);
      OA_ILE(offsets);
      if (offsets[row] == -1) {
        *data = NULL;
        *len = 0;
      } else {
        char *base = (char*)(stmt->rowset.rows[col]);
        base += offsets[row];
        int16_t length = *(int16_t*)base;
        base += sizeof(int16_t);
        *data = base;
        *len = length;
      }
    } break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      int64_t *base = (int64_t*)stmt->rowset.rows[col];
      base += row;
      *data = (const char*)base;
      *len = sizeof(*base);
    } break;
    case TSDB_DATA_TYPE_NCHAR: {
      int *offsets = TAOS_get_column_data_offset(stmt->res, col);
      OA_ILE(offsets);
      if (offsets[row] == -1) {
        *data = NULL;
        *len = 0;
      } else {
        char *base = (char*)(stmt->rowset.rows[col]);
        base += offsets[row];
        int16_t length = *(int16_t*)base;
        base += sizeof(int16_t);
        *data = base;
        *len = length;
      }
    } break;
    default:
      err_set_format(&stmt->err, "HY000", 0,
          "`%s[%d]` not implemented yet",
          taos_data_type(field->type), field->type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_fill_col(stmt_t *stmt, int row, col_bind_t *col_bind, conv_f conv)
{
  const char *data = NULL;
  int len = 0;

  int i = col_bind->ColumnNumber - 1;
  SQLRETURN sr = _stmt_get_data_len(stmt, row, i, &data, &len);
  if (!sql_successed(sr)) return sr;

  if (data) {
    return conv(stmt, data, len, row-stmt->rowset.cursor, col_bind);
  } else {
    if (col_bind->StrLen_or_IndPtr) {
      col_bind->StrLen_or_IndPtr[row-stmt->rowset.cursor] = SQL_NULL_DATA;
    }
    return SQL_SUCCESS;
  }
}

SQLRETURN stmt_fetch(stmt_t *stmt)
{
  if (stmt->row_array_size == 0) stmt->row_array_size = 1;

  SQLRETURN sr = SQL_SUCCESS;

  TAOS_ROW rows = NULL;
  OA_NIY(stmt->res);
  if (stmt->rowset.cursor + stmt->row_array_size >= (SQLULEN)stmt->rowset.nr_rows) {
    rowset_reset(&stmt->rowset);

    int nr_rows = TAOS_fetch_block(stmt->res, &rows);
    if (nr_rows == 0) return SQL_NO_DATA;
    OA_NIY(rows);
    stmt->rowset.rows = rows;          // column-wise
    stmt->rowset.nr_rows = nr_rows;
    stmt->rowset.cursor = 0;

    stmt->lengths = TAOS_fetch_lengths(stmt->res);
    OA_NIY(stmt->lengths);
  } else {
    stmt->rowset.cursor += stmt->row_array_size;
  }

  int with_info = 0;
  for (int i = stmt->rowset.cursor; (SQLULEN)i < stmt->rowset.cursor + stmt->row_array_size; i++) {
    if (i >= stmt->rowset.nr_rows) break;
    for (int j=0; j>=0 && (size_t)j<stmt->col_binds.nr; ++j) {
      if (j>=stmt->col_count) {
        err_set_format(&stmt->err,
            "07009",
            0,
            "ColumnNumber[%d] exceeded the maximum number of columns[#%d] in the result set",
            j+1, stmt->col_count);
        return SQL_ERROR;
      }
      col_bind_t *col_bind = stmt->col_binds.binds + j;
      if (!col_bind->valid) continue;
      OA_NIY(col_bind->ColumnNumber == j+1);

      TAOS_FIELD *p = stmt->cols + col_bind->ColumnNumber - 1;
      conv_f conv = NULL;
      sr = _stmt_get_conv(stmt, col_bind->TargetType, p->type, &conv);
      if (!sql_successed(sr)) return sr;
      sr = _stmt_fill_col(stmt, i, col_bind, conv);
      if (!sql_successed(sr)) return sr;
      if (sr == SQL_SUCCESS_WITH_INFO) with_info = 1;
    }
    if (stmt->row_status_ptr) stmt->row_status_ptr[i - stmt->rowset.cursor] = SQL_ROW_SUCCESS;
    if (stmt->rows_fetched_ptr) *stmt->rows_fetched_ptr = i - stmt->rowset.cursor + 1;
  }

  if (sr == SQL_SUCCESS && with_info) return SQL_SUCCESS_WITH_INFO;
  return sr;
}

int stmt_close_cursor(stmt_t *stmt)
{
  (void)stmt;
  return 0;
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
  if (RecNumber > 1) return SQL_NO_DATA;
  if (stmt->err.sql_state[0] == '\0') return SQL_NO_DATA;
  if (NativeErrorPtr) *NativeErrorPtr = stmt->err.err;
  if (SQLState) strncpy((char*)SQLState, (const char*)stmt->err.sql_state, 6);
  int n = snprintf((char*)MessageText, BufferLength, "%s", stmt->err.estr);
  if (TextLengthPtr) *TextLengthPtr = n;

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
  OA_NIY(stmt->res);
  OA_NIY(stmt->rowset.rows);
  if (Col_or_Param_Num < 1 || Col_or_Param_Num > stmt->col_count) {
    err_set(&stmt->err, "07009", 0, "The value specified for the argument `Col_or_Param_Num` is out of range");
    return SQL_ERROR;
  }

  const char *data;
  int len;

  int i = Col_or_Param_Num - 1;
  SQLRETURN sr = _stmt_get_data_len(stmt, stmt->rowset.cursor, i, &data, &len);
  if (!sql_successed(sr)) return sr;

  if (data) {
    TAOS_FIELD *p = stmt->cols + i;
    conv_f conv;
    sr = _stmt_get_conv(stmt, TargetType, p->type, &conv);
    if (!sql_successed(sr)) return sr;

    col_bind_t col_bind = {};
    col_bind.ColumnNumber         = Col_or_Param_Num;
    col_bind.TargetType           = TargetType;
    col_bind.TargetValuePtr       = TargetValuePtr;
    col_bind.BufferLength         = BufferLength;
    col_bind.StrLen_or_IndPtr     = StrLen_or_IndPtr;

    return conv(stmt, data, len, 0/*stmt->rowset.cursor*/, &col_bind);
  } else {
    if (StrLen_or_IndPtr) {
      StrLen_or_IndPtr[0/*stmt->rowset.cursor*/] = SQL_NULL_DATA;
    }
    return SQL_SUCCESS;
  }
}

SQLRETURN stmt_prepare(stmt_t *stmt, const char *sql, size_t len)
{
  OA_NIY(stmt->res == NULL);
  OA_NIY(stmt->stmt == NULL);
  stmt->stmt = TAOS_stmt_init(stmt->conn->taos);
  if (!stmt->stmt) {
    err_set(&stmt->err, "HY000", TAOS_errno(NULL), TAOS_errstr(NULL));
    return SQL_ERROR;
  }

  int r;
  r = TAOS_stmt_prepare(stmt->stmt, sql, len);
  OA_NIY(r == 0);

  int32_t isInsert = 0;
  r = TAOS_stmt_is_insert(stmt->stmt, &isInsert);
  isInsert = !!isInsert;

  if (r) {
    err_set(&stmt->err, "HY000", r, TAOS_stmt_errstr(stmt->stmt));
    stmt_release_stmt(stmt);

    return SQL_ERROR;
  }

  stmt->is_insert_stmt = isInsert;

  int nr_params = 0;
  r = TAOS_stmt_num_params(stmt->stmt, &nr_params);
  if (r) {
    err_set(&stmt->err, "HY000", r, TAOS_stmt_errstr(stmt->stmt));
    stmt_release_stmt(stmt);

    return SQL_ERROR;
  }

  stmt->nr_params = nr_params;

  if (!isInsert) {
  } else {
    int fieldNum = 0;
    TAOS_FIELD_E* pFields = NULL;
    r = TAOS_stmt_get_col_fields(stmt->stmt, &fieldNum, &pFields);
    if (r) {
      err_set_format(&stmt->err, "HY000", r, "prepared statement for `INSERT` failed: %s", taos_stmt_errstr(stmt->stmt));
      stmt_release_stmt(stmt);
      return SQL_ERROR;
    }
  }

  return SQL_SUCCESS;
}

SQLRETURN stmt_get_num_params(
    stmt_t         *stmt,
    SQLSMALLINT    *ParameterCountPtr)
{
  *ParameterCountPtr = stmt->nr_params;
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
  if (NullablePtr) *NullablePtr = SQL_NULLABLE_UNKNOWN;
  int r;
  int idx = ParameterNumber - 1;
  OA_NIY(idx >= 0);
  OA_NIY(idx < stmt->nr_params);
  int type;
  int bytes;
  r = TAOS_stmt_get_param(stmt->stmt, idx, &type, &bytes);
  if (r) {
    if (stmt->is_insert_stmt) {
      err_set(&stmt->err, "HY000", r, TAOS_stmt_errstr(stmt->stmt));
      return SQL_ERROR;
    }
    // FIXME: return SQL_VARCHAR and hard-coded parameters for the moment
    if (DataTypePtr)         *DataTypePtr = SQL_VARCHAR;
    if (ParameterSizePtr)    *ParameterSizePtr = 1024; /* hard-coded */
    OD("ParameterSize: %ld", *ParameterSizePtr);
    err_set(&stmt->err, "HY000", 0, "Arbitrary `SQL_VARCHAR(1024s)` is chosen to return because of taos lacking parm-desc for non-insert-statement");
    return SQL_SUCCESS_WITH_INFO;
  }
  OD("bytes: %d", bytes);

  switch (type) {
    case TSDB_DATA_TYPE_INT:
      if (DataTypePtr)      *DataTypePtr = SQL_INTEGER;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      if (DataTypePtr)         *DataTypePtr = SQL_VARCHAR;
      if (ParameterSizePtr)    *ParameterSizePtr = bytes - 2;
      OD("ParameterSize: %ld", *ParameterSizePtr);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      if (DataTypePtr)      *DataTypePtr = SQL_TYPE_TIMESTAMP;
      if (DecimalDigitsPtr) {
        *DecimalDigitsPtr = (stmt->time_precision + 1) * 3;
      }
      if (ParameterSizePtr) {
        *ParameterSizePtr = 20 + *DecimalDigitsPtr;
      }
      break;
    case TSDB_DATA_TYPE_NCHAR:
      if (DataTypePtr)         *DataTypePtr   = SQL_WVARCHAR;
      /* taos internal storage: sizeof(int16_t) + payload */
      if (ParameterSizePtr)    *ParameterSizePtr = (bytes - 2) / 4;
      OD("taos_bytes: %d; ParameterSize: %ld", bytes, *ParameterSizePtr);
      break;
    default:
      err_set_format(&stmt->err, "HY000", 0, "`%s[%d]` not implemented yet", taos_data_type(type), type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_guess_taos_data_type(
    stmt_t         *stmt,
    SQLSMALLINT     ValueType,
    SQLLEN          BufferLength,
    int            *taos_type,
    int            *taos_bytes)
{
  (void)taos_type;
  (void)taos_bytes;
  switch (ValueType) {
    case SQL_C_CHAR:
      *taos_type = TSDB_DATA_TYPE_VARCHAR;
      *taos_bytes = BufferLength;
      break;
    case SQL_C_SBIGINT:
      *taos_type = TSDB_DATA_TYPE_BIGINT;
      *taos_bytes = sizeof(int64_t);
      break;
    default:
      err_set_format(&stmt->err,
        "HY000",
        0,
        "unable to guess taos-data-type for `%s[%d]`",
        sql_c_data_type_to_str(ValueType), ValueType);
      return SQL_ERROR;
  }

  return SQL_SUCCESS_WITH_INFO;
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
  switch (InputOutputType) {
    case SQL_PARAM_INPUT:
      break;
    case SQL_PARAM_INPUT_OUTPUT:
      break;
    case SQL_PARAM_OUTPUT:
      err_set(&stmt->err, "HY000", 0, "SQL_PARAM_OUTPUT not supported yet by taos");
      return SQL_ERROR;
    case SQL_PARAM_INPUT_OUTPUT_STREAM:
      err_set(&stmt->err, "HY000", 0, "SQL_PARAM_INPUT_OUTPUT_STREAM not supported yet by taos");
      return SQL_ERROR;
    case SQL_PARAM_OUTPUT_STREAM:
      err_set(&stmt->err, "HY000", 0, "SQL_PARAM_OUTPUT_STREAM not supported yet by taos");
      return SQL_ERROR;
    default:
      err_set(&stmt->err, "HY000", 0, "unknown InputOutputType for `SQLBindParameter`");
      return SQL_ERROR;
  }

  param_bind_t param_bind = {};
  param_bind.ParameterNumber          = ParameterNumber;
  param_bind.InputOutputType          = InputOutputType;
  param_bind.ValueType                = ValueType;
  param_bind.ParameterType            = ParameterType;
  param_bind.ColumnSize               = ColumnSize;
  param_bind.DecimalDigits            = DecimalDigits;
  param_bind.ParameterValuePtr        = ParameterValuePtr;
  param_bind.BufferLength             = BufferLength;
  param_bind.StrLen_or_IndPtr         = StrLen_or_IndPtr;
  param_bind.value.inited = 0;

  SQLRETURN sr = SQL_SUCCESS;
  int type;
  int bytes;
  int r = 0;
  r = TAOS_stmt_get_param(stmt->stmt, ParameterNumber-1, &type, &bytes);
  if (r) {
    if (stmt->is_insert_stmt) {
      err_set(&stmt->err, "HY000", r, taos_stmt_errstr(stmt->stmt));
      stmt_release_stmt(stmt);
      return SQL_ERROR;
    }
    sr = _stmt_guess_taos_data_type(stmt, ValueType, BufferLength, &type, &bytes);
    if (!sql_successed(sr)) return sr;
  }

  param_bind.taos_type      = type;
  param_bind.taos_bytes     = bytes;
  OD("ValueType[%d]%s; ParameterType[%d]%s; ColumnSize[%ld]; DecimalDigits[%d]; BufferLength[%ld]; bytes[%d]",
      ValueType, sql_c_data_type_to_str(ValueType),
      ParameterType, sql_data_type_to_str(ParameterType),
      ColumnSize, DecimalDigits, BufferLength, bytes);

  r = param_binds_bind_param(&stmt->param_binds, &param_bind);
  OA(r == 0, "");

  return sr;
}

#define _stmt_conv_bounded_param_fail(_stmt, _param_bind)             \
  OD("ColumnSize[%ld]; DecimalDigits[%d]; BufferLength[%ld]",         \
      _param_bind->ColumnSize,                                        \
      _param_bind->DecimalDigits,                                     \
      _param_bind->BufferLength);                                     \
  err_set_format(&_stmt->err,                                         \
      "HY000",                                                        \
      0,                                                              \
      "`%s` to `%s` for param `%d` not supported yet by taos",        \
      sql_c_data_type_to_str(_param_bind->ValueType),                 \
      TAOS_data_type(_param_bind->taos_type),                         \
      _param_bind->ParameterNumber)

static void _param_bind_reset_default_actual_data(param_bind_t *param_bind)
{
  param_bind_reset(param_bind);

  if (*param_bind->StrLen_or_IndPtr == SQL_NULL_DATA) {
    param_bind->mb.buffer = NULL;
    param_bind->mb.buffer_length = 0;
    param_bind->mb.length = NULL;
    param_bind->value.is_null = 1;
    param_bind->mb.is_null = &param_bind->value.is_null;
  } else {
    // param_bind->mb.buffer;
    // param_bind->mb.buffer_length;
    param_bind->mb.length = &param_bind->value.length;
    param_bind->mb.is_null = NULL;
  }
  // TODO: currently only one row of parameters to bind
  param_bind->mb.num = 1;
  param_bind->value.inited = 1;
}

#define _stmt_param_bind_fail(_stmt, _param_bind, _r)     \
  err_set(&_stmt->err,                                    \
    "HY000",                                              \
    _r,                                                   \
    TAOS_stmt_errstr(_stmt->stmt))

static SQLRETURN _stmt_conv_bounded_param_c_sbigint_to_tsdb_timestamp(stmt_t *stmt, param_bind_t *param_bind)
{
  _param_bind_reset_default_actual_data(param_bind);

  param_bind->mb.buffer_type = TSDB_DATA_TYPE_TIMESTAMP;

  if (*param_bind->StrLen_or_IndPtr != SQL_NULL_DATA) {
    int64_t *base = (int64_t*)param_bind->ParameterValuePtr;
    OD("c_sbigint: %" PRId64 "", *base);

    param_bind->mb.buffer_length = sizeof(int64_t);
    param_bind->mb.buffer = base;
    param_bind->mb.length = NULL;
  }

  int r = TAOS_stmt_bind_single_param_batch(stmt->stmt, &param_bind->mb, param_bind->ParameterNumber - 1);
  if (r) {
    _stmt_param_bind_fail(stmt, param_bind, r);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_bounded_param_c_char_to_tsdb_timestamp(stmt_t *stmt, param_bind_t *param_bind)
{
  _param_bind_reset_default_actual_data(param_bind);

  param_bind->mb.buffer_type = TSDB_DATA_TYPE_TIMESTAMP;

  if (*param_bind->StrLen_or_IndPtr != SQL_NULL_DATA) {
    const char *base = (const char*)param_bind->ParameterValuePtr;
    SQLLEN len = *param_bind->StrLen_or_IndPtr;
    if (len == SQL_NTS) len = strlen(base);

    char *end;
    errno = 0;
    long int v = strtol(base, &end, 0);
    int e = errno;
    if (e == ERANGE && (v == LONG_MAX || v == LONG_MIN)) {
      err_set_format(&stmt->err,
          "HY000",
          0,
          "convertion from `SQL_C_CHAR` to `TSDB_DATA_TYPE_TIMESTAMP` failed: [%d] %s",
          e, strerror(e));
      return SQL_ERROR;
    }
    if (e != 0) {
      err_set_format(&stmt->err,
          "HY000",
          0,
          "convertion from `SQL_C_CHAR` to `TSDB_DATA_TYPE_TIMESTAMP` failed: [%d] %s",
          e, strerror(e));
      return SQL_ERROR;
    }
    if (end == base) {
      err_set(&stmt->err,
          "HY000",
          0,
          "convertion from `SQL_C_CHAR` to `TSDB_DATA_TYPE_TIMESTAMP` failed: no digits at all");
      return SQL_ERROR;
    }
    if (end && *end) {
      err_set_format(&stmt->err,
          "HY000",
          0,
          "convertion from `SQL_C_CHAR` to `TSDB_DATA_TYPE_TIMESTAMP` failed: string following digits[%s]",
          end);
      return SQL_ERROR;
    }

    param_bind->value.tsdb_timestamp = v;

    param_bind->mb.buffer_length = sizeof(int64_t);
    param_bind->mb.buffer = &param_bind->value.tsdb_timestamp;
    param_bind->mb.length = NULL;
  }

  int r = TAOS_stmt_bind_single_param_batch(stmt->stmt, &param_bind->mb, param_bind->ParameterNumber - 1);
  if (r) {
    _stmt_param_bind_fail(stmt, param_bind, r);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_bounded_param_to_tsdb_timestamp(stmt_t *stmt, param_bind_t *param_bind)
{
  switch (param_bind->ValueType) {
    case SQL_C_SBIGINT:
      return _stmt_conv_bounded_param_c_sbigint_to_tsdb_timestamp(stmt, param_bind);
    case SQL_C_CHAR:
      return _stmt_conv_bounded_param_c_char_to_tsdb_timestamp(stmt, param_bind);
    default:
      _stmt_conv_bounded_param_fail(stmt, param_bind);
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_bounded_param_c_char_to_tsdb_varchar(stmt_t *stmt, param_bind_t *param_bind)
{
  _param_bind_reset_default_actual_data(param_bind);

  param_bind->mb.buffer_type = TSDB_DATA_TYPE_VARCHAR;

  if (*param_bind->StrLen_or_IndPtr != SQL_NULL_DATA) {
    const char *base = (const char*)param_bind->ParameterValuePtr;
    size_t len = *param_bind->StrLen_or_IndPtr;
    if (*param_bind->StrLen_or_IndPtr == SQL_NTS) {
      len = strlen(base);
    }
    int n = snprintf(NULL, 0, "%.*s", (int)len, base);
    OA_NIY(n >= 0);

    param_bind->mb.buffer_length = param_bind->taos_bytes;
    param_bind->mb.buffer = (char*)base;
    param_bind->value.length = n;
  }

  int r = TAOS_stmt_bind_single_param_batch(stmt->stmt, &param_bind->mb, param_bind->ParameterNumber - 1);
  if (r) {
    _stmt_param_bind_fail(stmt, param_bind, r);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_bounded_param_c_sbigint_to_tsdb_varchar(stmt_t *stmt, param_bind_t *param_bind)
{
  _param_bind_reset_default_actual_data(param_bind);

  param_bind->mb.buffer_type = TSDB_DATA_TYPE_VARCHAR;

  if (*param_bind->StrLen_or_IndPtr != SQL_NULL_DATA) {
    int64_t *base = (int64_t*)param_bind->ParameterValuePtr;
    char buf[64];
    int n = snprintf(buf, sizeof(buf), "%ld", *base);
    OD("c_sbigint: [%ld]", *base);

    param_bind->value.ptr = strdup(buf);
    param_bind->value.allocated = 1;
    param_bind->value.length = n;

    param_bind->mb.buffer_length = param_bind->taos_bytes;
    param_bind->mb.buffer = param_bind->value.ptr;
  }

  int r = TAOS_stmt_bind_single_param_batch(stmt->stmt, &param_bind->mb, param_bind->ParameterNumber - 1);
  if (r) {
    _stmt_param_bind_fail(stmt, param_bind, r);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_bounded_param_c_default_to_tsdb_varchar(stmt_t *stmt, param_bind_t *param_bind)
{
  _param_bind_reset_default_actual_data(param_bind);

  param_bind->mb.buffer_type = TSDB_DATA_TYPE_VARCHAR;

  OA_NIY(*param_bind->StrLen_or_IndPtr == SQL_NULL_DATA);

  int r = TAOS_stmt_bind_single_param_batch(stmt->stmt, &param_bind->mb, param_bind->ParameterNumber - 1);
  if (r) {
    _stmt_param_bind_fail(stmt, param_bind, r);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_bounded_param_to_tsdb_varchar(stmt_t *stmt, param_bind_t *param_bind)
{
  switch (param_bind->ValueType) {
    case SQL_C_CHAR:
      return _stmt_conv_bounded_param_c_char_to_tsdb_varchar(stmt, param_bind);
    case SQL_C_SBIGINT:
      return _stmt_conv_bounded_param_c_sbigint_to_tsdb_varchar(stmt, param_bind);
    case SQL_C_DEFAULT:
      return _stmt_conv_bounded_param_c_default_to_tsdb_varchar(stmt, param_bind);
    default:
      _stmt_conv_bounded_param_fail(stmt, param_bind);
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_bounded_param_c_sbigint_to_tsdb_int(stmt_t *stmt, param_bind_t *param_bind)
{
  _param_bind_reset_default_actual_data(param_bind);

  param_bind->mb.buffer_type = TSDB_DATA_TYPE_INT;

  if (*param_bind->StrLen_or_IndPtr != SQL_NULL_DATA) {
    int64_t *base = (int64_t*)param_bind->ParameterValuePtr;
    OD("c_sbigint: %" PRId64 "", *base);

    if (*base > INT_MAX || *base < INT_MIN) {
      err_set(&stmt->err, "22003", 0, "Numeric value out of range");
      return SQL_ERROR;
    }

    param_bind->mb.buffer_length = sizeof(int);
    param_bind->mb.buffer = base;
    param_bind->mb.length = NULL;
  }

  int r = TAOS_stmt_bind_single_param_batch(stmt->stmt, &param_bind->mb, param_bind->ParameterNumber - 1);
  if (r) {
    _stmt_param_bind_fail(stmt, param_bind, r);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_bounded_param_c_sbigint_to_tsdb_bigint(stmt_t *stmt, param_bind_t *param_bind)
{
  _param_bind_reset_default_actual_data(param_bind);

  param_bind->mb.buffer_type = TSDB_DATA_TYPE_BIGINT;

  if (*param_bind->StrLen_or_IndPtr != SQL_NULL_DATA) {
    int64_t *base = (int64_t*)param_bind->ParameterValuePtr;
    OD("c_sbigint: %" PRId64 "", *base);

    param_bind->mb.buffer_length = sizeof(int64_t);
    param_bind->mb.buffer = base;
    param_bind->mb.length = NULL;
  }

  int r = TAOS_stmt_bind_single_param_batch(stmt->stmt, &param_bind->mb, param_bind->ParameterNumber - 1);
  if (r) {
    _stmt_param_bind_fail(stmt, param_bind, r);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_bounded_param_c_default_to_tsdb_int(stmt_t *stmt, param_bind_t *param_bind)
{
  _param_bind_reset_default_actual_data(param_bind);

  param_bind->mb.buffer_type = TSDB_DATA_TYPE_INT;

  OA_NIY(*param_bind->StrLen_or_IndPtr == SQL_NULL_DATA);

  int r = TAOS_stmt_bind_single_param_batch(stmt->stmt, &param_bind->mb, param_bind->ParameterNumber - 1);
  if (r) {
    _stmt_param_bind_fail(stmt, param_bind, r);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_bounded_param_to_tsdb_int(stmt_t *stmt, param_bind_t *param_bind)
{
  switch (param_bind->ValueType) {
    case SQL_C_SBIGINT:
      return _stmt_conv_bounded_param_c_sbigint_to_tsdb_int(stmt, param_bind);
    case SQL_C_DEFAULT:
      return _stmt_conv_bounded_param_c_default_to_tsdb_int(stmt, param_bind);
    default:
      _stmt_conv_bounded_param_fail(stmt, param_bind);
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_bounded_param_to_tsdb_bigint(stmt_t *stmt, param_bind_t *param_bind)
{
  switch (param_bind->ValueType) {
    case SQL_C_SBIGINT:
      return _stmt_conv_bounded_param_c_sbigint_to_tsdb_bigint(stmt, param_bind);
    default:
      _stmt_conv_bounded_param_fail(stmt, param_bind);
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_bounded_param_c_char_to_tsdb_nchar(stmt_t *stmt, param_bind_t *param_bind)
{
  _param_bind_reset_default_actual_data(param_bind);

  param_bind->mb.buffer_type = TSDB_DATA_TYPE_NCHAR;

  if (*param_bind->StrLen_or_IndPtr != SQL_NULL_DATA) {
    // taosc: although buffer_type is TSDB_DATA_TYPE_NCHAR
    //        internally, multi-bytes is used rather than UCS-4
    //        FIXME: what'bout locale?, eg. locale is not UTF8?
    const char *base = (const char*)param_bind->ParameterValuePtr;
    int n = snprintf(NULL, 0, "%.*s", (int)param_bind->BufferLength, base);
    OA_NIY(n >= 0);
    OD("blen: %ld; n: %d", param_bind->BufferLength, n);
    OD("c_char: [%.*s]", (int)param_bind->BufferLength, base);

    param_bind->mb.buffer_length = param_bind->taos_bytes;
    param_bind->mb.buffer = (char*)base;
    // taosc: although buffer_type is TSDB_DATA_TYPE_NCHAR
    //        internally, multi-bytes is used rather than UCS-4
    //        FIXME: what'bout locale?, eg. locale is not UTF8?
    param_bind->value.length = n;
  }

  int r = TAOS_stmt_bind_single_param_batch(stmt->stmt, &param_bind->mb, param_bind->ParameterNumber - 1);
  if (r) {
    _stmt_param_bind_fail(stmt, param_bind, r);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_bounded_param_c_default_to_tsdb_nchar(stmt_t *stmt, param_bind_t *param_bind)
{
  _param_bind_reset_default_actual_data(param_bind);

  param_bind->mb.buffer_type = TSDB_DATA_TYPE_NCHAR;

  OA_NIY(*param_bind->StrLen_or_IndPtr == SQL_NULL_DATA);

  int r = TAOS_stmt_bind_single_param_batch(stmt->stmt, &param_bind->mb, param_bind->ParameterNumber - 1);
  if (r) {
    _stmt_param_bind_fail(stmt, param_bind, r);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_conv_bounded_param_to_tsdb_nchar(stmt_t *stmt, param_bind_t *param_bind)
{
  switch (param_bind->ValueType) {
    case SQL_C_CHAR:
      return _stmt_conv_bounded_param_c_char_to_tsdb_nchar(stmt, param_bind);
    case SQL_C_DEFAULT:
      return _stmt_conv_bounded_param_c_default_to_tsdb_nchar(stmt, param_bind);
    default:
      _stmt_conv_bounded_param_fail(stmt, param_bind);
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_conv_bounded_param(stmt_t *stmt, int idx)
{
  param_bind_t *param_bind = stmt->param_binds.binds + idx;
  switch (param_bind->taos_type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      return _stmt_conv_bounded_param_to_tsdb_timestamp(stmt, param_bind);
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      return _stmt_conv_bounded_param_to_tsdb_varchar(stmt, param_bind);
    case TSDB_DATA_TYPE_INT:
      return _stmt_conv_bounded_param_to_tsdb_int(stmt, param_bind);
    case TSDB_DATA_TYPE_NCHAR:
      return _stmt_conv_bounded_param_to_tsdb_nchar(stmt, param_bind);
    case TSDB_DATA_TYPE_BIGINT:
      return _stmt_conv_bounded_param_to_tsdb_bigint(stmt, param_bind);
    default:
      _stmt_conv_bounded_param_fail(stmt, param_bind);
      return SQL_ERROR;
  }
}

SQLRETURN stmt_execute(
    stmt_t         *stmt)
{
  OA_ILE(stmt);
  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->taos);
  OA_ILE(stmt->stmt);

  if (stmt->rows_fetched_ptr) *stmt->rows_fetched_ptr = 0;
  rowset_reset(&stmt->rowset);
  // column-binds remain valid among executes
  stmt_release_result(stmt);

  int r = 0;
  if (stmt->param_binds.binds) {
    for (int j=0; j<stmt->nr_params; ++j) {
      SQLRETURN sr  = _stmt_conv_bounded_param(stmt, j);
      if (!sql_successed(sr)) return sr;
    }
  }

  r = TAOS_stmt_add_batch(stmt->stmt);
  if (r) {
    err_set(&stmt->err, "HY000", r, TAOS_stmt_errstr(stmt->stmt));
    return SQL_ERROR;
  }

  r = TAOS_stmt_execute(stmt->stmt);
  if (r) {
    err_set(&stmt->err, "HY000", r, TAOS_stmt_errstr(stmt->stmt));
    return SQL_ERROR;
  }
  stmt->res = TAOS_stmt_use_result(stmt->stmt);

  int e;
  const char *estr;
  e = TAOS_errno(stmt->res);
  estr = TAOS_errstr(stmt->res);

  if (e == 0 && stmt->res) {
    stmt->row_count = TAOS_affected_rows(stmt->res);
    stmt->col_count = TAOS_field_count(stmt->res);
    if (stmt->col_count > 0) {
      stmt->cols = TAOS_fetch_fields(stmt->res);
    }
    stmt->time_precision = TAOS_result_precision(stmt->res);
  }

  if (e) {
    err_set(&stmt->err, "HY000", e, estr);
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

SQLRETURN stmt_unbind_cols(
    stmt_t         *stmt)
{
  col_binds_reset(&stmt->col_binds);
  return SQL_SUCCESS;
}

SQLRETURN stmt_reset_params(
    stmt_t         *stmt)
{
  param_binds_reset(&stmt->param_binds);
  return SQL_SUCCESS;
}

