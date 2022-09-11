#include "internal.h"

#include <inttypes.h>
#include <string.h>
#include <time.h>

#include <sqlext.h>

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
  if (col_bind->desc.ColumnNumber > col_binds->cap) {
    int cap = (col_bind->desc.ColumnNumber + 15) / 16 * 16;
    col_bind_t *binds = (col_bind_t*)realloc(col_binds->binds, sizeof(*binds) * cap);
    if (!col_binds) return -1;
    col_binds->binds = binds;
    col_binds->cap = cap;
  }

  col_binds->binds[col_bind->desc.ColumnNumber - 1]         = *col_bind;
  col_binds->binds[col_bind->desc.ColumnNumber - 1].bounded = 1;

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
    taos_free_result(stmt->res);
    stmt->res = NULL;
    stmt->row_count = 0;
    stmt->col_count = 0;
    stmt->cols = NULL;
    stmt->lengths = NULL;
    stmt->time_precision = 0;
  }
}

static void stmt_release(stmt_t *stmt)
{
  rowset_release(&stmt->rowset);
  stmt_release_result(stmt);

  col_binds_release(&stmt->col_binds);

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
  col_binds_reset(&stmt->col_binds);
  stmt_release_result(stmt);

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

  stmt->res = taos_query(taos, p);

  int e;
  const char *estr;
  e = taos_errno(stmt->res);
  estr = taos_errstr(stmt->res);

  if (e == 0 && stmt->res) {
    stmt->row_count = taos_affected_rows(stmt->res);
    stmt->col_count = taos_field_count(stmt->res);
    if (stmt->col_count > 0) {
      stmt->cols = taos_fetch_fields(stmt->res);
    }
    stmt->time_precision = taos_result_precision(stmt->res);
  }

  if (p != sql && p!= buf) free(p);

  if (e) {
    err_set(&stmt->err,
        e,
        estr,
        "HY000");
  }

  return stmt->err.err ? -1 : 0;
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
  stmt->row_bind_type = row_bind_type;
  return 0;
}

int stmt_set_rows_fetched_ptr(stmt_t *stmt, SQLULEN *rows_fetched_ptr)
{
  stmt->rows_fetched_ptr = rows_fetched_ptr;
  return 0;
}

int stmt_describe_col(stmt_t *stmt,
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
    case TSDB_DATA_TYPE_BOOL:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_TINYINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_INT:
      if (DataTypePtr)      *DataTypePtr = SQL_INTEGER;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      OA_NIY(0);
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
      if (DataTypePtr)      *DataTypePtr   = SQL_VARCHAR;
      if (ColumnSizePtr)    *ColumnSizePtr = p->bytes;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_UINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_JSON:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_VARBINARY:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_DECIMAL:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_BLOB:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      OA_NIY(0);
      break;
    default:
      OA_NIY(0);
  }

  return 0;
}

static SQLRETURN _conv_tsdb_int_to_sql_c_char(stmt_t *stmt, const char *data, int len, int row, sql_c_data_desc_t *desc)
{
  OA_ILE(data);
  OA_ILE(len = sizeof(int32_t));
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  char *base = (char*)desc->TargetValuePtr;
  base += desc->BufferLength * row;
  int n = snprintf(base, desc->BufferLength, "%d", *(int32_t*)data);
  if (desc->StrLen_or_IndPtr) desc->StrLen_or_IndPtr[row] = n;
  if (n >= desc->BufferLength) {
    err_set(&stmt->err,
        0,
        "String was truncated",
        "01004");
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _conv_tsdb_varchar_to_sql_c_char(stmt_t *stmt, const char *data, int len, int row, sql_c_data_desc_t *desc)
{
  OA_ILE(data);
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  char *base = (char*)desc->TargetValuePtr;
  base += desc->BufferLength * row;
  int n = snprintf(base, desc->BufferLength, "%.*s", len, data);
  if (desc->StrLen_or_IndPtr) desc->StrLen_or_IndPtr[row] = n;
  if (n >= desc->BufferLength) {
    err_set(&stmt->err,
        0,
        "String was truncated",
        "01004");
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _conv_tsdb_nchar_to_sql_c_char(stmt_t *stmt, const char *data, int len, int row, sql_c_data_desc_t *desc)
{
  OA_ILE(data);
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  char *base = (char*)desc->TargetValuePtr;
  base += desc->BufferLength * row;
  int n = snprintf(base, desc->BufferLength, "%.*s", len, data);
  if (desc->StrLen_or_IndPtr) desc->StrLen_or_IndPtr[row] = n;
  if (n >= desc->BufferLength) {
    err_set(&stmt->err,
        0,
        "String was truncated",
        "01004");
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _conv_tsdb_timestamp_to_sql_c_char(stmt_t *stmt, const char *data, int len, int row, sql_c_data_desc_t *desc)
{
  OA_ILE(data);
  OA(len == sizeof(int64_t), "");
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  char *base = (char*)desc->TargetValuePtr;
  base += desc->BufferLength * row;

  int64_t val = *(int64_t*)data;

  int n;
  if (!stmt->conn->fmt_time) {
    n = snprintf(base, desc->BufferLength, "%" PRId64 "", val);
  } else {
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

    n = snprintf(base, desc->BufferLength,
        "%04d-%02d-%02d %02d:%02d:%02d.%0*d",
        ptm.tm_year + 1900, ptm.tm_mon + 1, ptm.tm_mday,
        ptm.tm_hour, ptm.tm_min, ptm.tm_sec,
        w, ms);
  }

  if (desc->StrLen_or_IndPtr) desc->StrLen_or_IndPtr[row] = n;
  if (n >= desc->BufferLength) {
    OA_NIY(0);
    err_set(&stmt->err,
        0,
        "String was truncated",
        "01004");
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static conv_f _stmt_get_conv_to_sql_c_char(stmt_t *stmt, int taos_type)
{
  (void)stmt;

  switch (taos_type) {
    case TSDB_DATA_TYPE_BOOL:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_TINYINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_INT:
      return _conv_tsdb_int_to_sql_c_char;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      return _conv_tsdb_varchar_to_sql_c_char;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      return _conv_tsdb_timestamp_to_sql_c_char;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      return _conv_tsdb_nchar_to_sql_c_char;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_UINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_JSON:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_VARBINARY:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_DECIMAL:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_BLOB:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      OA_NIY(0);
      break;
    default:
      OA_NIY(0);
  }
}

static SQLRETURN _conv_tsdb_int_to_sql_c_slong(stmt_t *stmt, const char *data, int len, int row, sql_c_data_desc_t *desc)
{
  OA(len == sizeof(int32_t), "");
  OA_NIY(stmt->row_bind_type == SQL_BIND_BY_COLUMN);
  SQLINTEGER *base = (SQLINTEGER*)desc->TargetValuePtr;
  base += row;
  *base = *(int32_t*)data;

  return SQL_SUCCESS;
}

static conv_f _stmt_get_conv_to_sql_c_slong(stmt_t *stmt, int taos_type)
{
  (void)stmt;

  switch (taos_type) {
    case TSDB_DATA_TYPE_BOOL:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_TINYINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_INT:
      return _conv_tsdb_int_to_sql_c_slong;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_NCHAR:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_UINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_JSON:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_VARBINARY:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_DECIMAL:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_BLOB:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      OA_NIY(0);
      break;
    default:
      OA_NIY(0);
  }
}

static conv_f _stmt_get_conv(stmt_t *stmt, SQLSMALLINT TargetType, int taos_type)
{
  switch (TargetType) {
    case SQL_C_CHAR:
      return _stmt_get_conv_to_sql_c_char(stmt, taos_type);
      break;
    case SQL_C_SLONG:
      return _stmt_get_conv_to_sql_c_slong(stmt, taos_type);
      break;
    default:
      OA_NIY(0);
      break;
  }
}

int stmt_bind_col(stmt_t *stmt,
    SQLUSMALLINT   ColumnNumber,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  col_bind_t col_bind = {};
  col_bind.desc.ColumnNumber       = ColumnNumber;
  col_bind.desc.TargetType         = TargetType;
  col_bind.desc.TargetValuePtr     = TargetValuePtr;
  col_bind.desc.BufferLength       = BufferLength;
  col_bind.desc.StrLen_or_IndPtr   = StrLen_or_IndPtr;

  TAOS_FIELD *p = stmt->cols + ColumnNumber - 1;
  col_bind.conv = _stmt_get_conv(stmt, TargetType, p->type);
  OA_NIY(col_bind.conv);

  return col_binds_bind_col(&stmt->col_binds, &col_bind);
}

static void _stmt_get_data_len(stmt_t *stmt, int row, int col, const char **data, int *len)
{
  TAOS_FIELD *field = stmt->cols + col;
  switch(field->type) {
    case TSDB_DATA_TYPE_BOOL:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_TINYINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_INT:
      if (taos_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        int32_t *base = (int32_t*)stmt->rowset.rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_BIGINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_VARCHAR: {
      int *offsets = taos_get_column_data_offset(stmt->res, col);
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
      int *offsets = taos_get_column_data_offset(stmt->res, col);
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
    case TSDB_DATA_TYPE_UTINYINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_UINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_JSON:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_VARBINARY:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_DECIMAL:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_BLOB:
      OA_NIY(0);
      break;
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      OA_NIY(0);
      break;
    default:
      OA_NIY(0);
  }
}

static SQLRETURN _stmt_conv_bounded_col(stmt_t *stmt, int row, sql_c_data_desc_t *desc, conv_f conv)
{
  const char *data = NULL;
  int len = 0;

  int i = desc->ColumnNumber - 1;
  _stmt_get_data_len(stmt, row, i, &data, &len);
  if (data) {
    return conv(stmt, data, len, row-stmt->rowset.cursor, desc);
  } else {
    if (desc->StrLen_or_IndPtr) {
      desc->StrLen_or_IndPtr[row-stmt->rowset.cursor] = SQL_NULL_DATA;
    }
    return SQL_SUCCESS;
  }
}

SQLRETURN stmt_fetch(stmt_t *stmt)
{
  if (stmt->row_array_size == 0) stmt->row_array_size = 1;

  SQLRETURN r = SQL_SUCCESS;

  TAOS_ROW rows = NULL;
  OA_NIY(stmt->res);
  if (stmt->rowset.cursor + stmt->row_array_size >= (SQLULEN)stmt->rowset.nr_rows) {
    rowset_reset(&stmt->rowset);

    int nr_rows = taos_fetch_block(stmt->res, &rows);
    if (nr_rows == 0) return SQL_NO_DATA;
    OA_NIY(rows);
    stmt->rowset.rows = rows;          // column-wise
    stmt->rowset.nr_rows = nr_rows;
    stmt->rowset.cursor = 0;

    stmt->lengths = taos_fetch_lengths(stmt->res);
    OA_NIY(stmt->lengths);
  } else {
    stmt->rowset.cursor += stmt->row_array_size;
  }

  for (int i = stmt->rowset.cursor; (SQLULEN)i < stmt->rowset.cursor + stmt->row_array_size; i++) {
    if (i >= stmt->rowset.nr_rows) break;
    for (int j=0; j<stmt->col_count && (size_t)j<stmt->col_binds.cap; ++j) {
      col_bind_t *col_bind = stmt->col_binds.binds + j;
      if (col_bind->bounded) {
        OA_NIY(col_bind->desc.ColumnNumber == j+1);
        r = _stmt_conv_bounded_col(stmt, i, &col_bind->desc, col_bind->conv);
        if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return r;
      }
    }
    if (stmt->row_status_ptr) stmt->row_status_ptr[i - stmt->rowset.cursor] = SQL_ROW_SUCCESS;
    if (stmt->rows_fetched_ptr) *stmt->rows_fetched_ptr = i - stmt->rowset.cursor + 1;
  }

  return r;
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
    err_set(&stmt->err, 0, "The value specified for the argument `Col_or_Param_Num` is out of range", "07009");
    return SQL_ERROR;
  }

  const char *data;
  int len;

  int i = Col_or_Param_Num - 1;
  _stmt_get_data_len(stmt, stmt->rowset.cursor, i, &data, &len);

  if (data) {
    TAOS_FIELD *p = stmt->cols + i;
    conv_f conv = _stmt_get_conv(stmt, TargetType, p->type);
    OA_NIY(conv);

    sql_c_data_desc_t desc = {};
    desc.ColumnNumber         = Col_or_Param_Num;
    desc.TargetType           = TargetType;
    desc.TargetValuePtr       = TargetValuePtr;
    desc.BufferLength         = BufferLength;
    desc.StrLen_or_IndPtr     = StrLen_or_IndPtr;

    return conv(stmt, data, len, 0/*stmt->rowset.cursor*/, &desc);
  } else {
    if (StrLen_or_IndPtr) {
      StrLen_or_IndPtr[0/*stmt->rowset.cursor*/] = SQL_NULL_DATA;
    }
    return SQL_SUCCESS;
  }
}

