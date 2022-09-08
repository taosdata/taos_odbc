#include "internal.h"

#include <string.h>

#include <sqlext.h>

static void rowset_reset(rowset_t *rowset)
{
  rowset->rows = NULL;
  rowset->nr_rows = 0;
}

static void rowset_release(rowset_t *rowset)
{
  rowset->rows = NULL;
  rowset->nr_rows = 0;
}

static void col_binds_reset(col_binds_t *col_binds)
{
  if (!col_binds->binds) return;
  memset(col_binds->binds, 0, sizeof(*col_binds->binds) * col_binds->nr);
  col_binds->nr = 0;
}

static void col_binds_release(col_binds_t *col_binds)
{
  if (!col_binds->binds) return;
  free(col_binds->binds);
  col_binds->binds = NULL;
  col_binds->cap = 0;
  col_binds->nr = 0;
}

static int col_binds_bind_col(col_binds_t *col_binds, col_bind_t *col_bind)
{
  if (col_bind->ColumnNumber > col_binds->cap) {
    col_bind_t *binds = (col_bind_t*)realloc(col_binds->binds, sizeof(*binds) * (col_bind->ColumnNumber + 15) / 16 * 16);
    if (!col_binds) return -1;
    col_binds->binds = binds;
  }

  col_binds->binds[col_bind->ColumnNumber - 1]         = *col_bind;
  col_binds->binds[col_bind->ColumnNumber - 1].bounded = 1;

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

  rowset_reset(&stmt->rowset);
  col_binds_reset(&stmt->col_binds);
  stmt_release_result(stmt);

  char buf[1024];
  char *p = (char*)sql;
  OD("sql: %s", sql);
  if (len == SQL_NTS)
    len = strlen(sql);
  if (p[len]) {
    if ((size_t)len < sizeof(buf)) {
      strncpy(buf, p, len);
      p[len] = '\0';
    } else {
      p = strndup(p, len);
      if (!p) return -1;
    }
  }

  TAOS *taos = stmt->conn->taos;

  stmt->res = taos_query(taos, p);

  stmt->err  = taos_errno(stmt->res);
  stmt->estr = taos_errstr(stmt->res);

  if (stmt->err == 0 && stmt->res) {
    stmt->row_count = taos_affected_rows(stmt->res);
    stmt->col_count = taos_field_count(stmt->res);
    if (stmt->col_count > 0) {
      stmt->cols = taos_fetch_fields(stmt->res);
    }
    stmt->time_precision = taos_result_precision(stmt->res);
  }

  if (p != sql && p!= buf) free(p);

  return stmt->err ? -1 : 0;
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
  (void)ColumnSizePtr;
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

  return 0;
}

int stmt_bind_col_to_sql_c_char(stmt_t *stmt, col_bind_t *col_bind)
{
  TAOS_FIELD *p = stmt->cols + col_bind->ColumnNumber - 1;

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
      OA_NIY(0);
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
      return col_binds_bind_col(&stmt->col_binds, col_bind);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      return col_binds_bind_col(&stmt->col_binds, col_bind);
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

int stmt_bind_col_to_sql_c_slong(stmt_t *stmt, col_bind_t *col_bind)
{
  TAOS_FIELD *p = stmt->cols + col_bind->ColumnNumber - 1;

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
      return col_binds_bind_col(&stmt->col_binds, col_bind);
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

int stmt_bind_col(stmt_t *stmt,
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

  switch (TargetType) {
    case SQL_C_CHAR:
      return stmt_bind_col_to_sql_c_char(stmt, &col_bind);
      break;
    case SQL_C_SLONG:
      return stmt_bind_col_to_sql_c_slong(stmt, &col_bind);
      break;
    default:
      OA_NIY(0);
      break;
  }

  return 0;
}

SQLRETURN stmt_fetch(stmt_t *stmt)
{
  OA_ILE(stmt->row_array_size == 1);
  rowset_reset(&stmt->rowset);
  TAOS_ROW row = taos_fetch_row(stmt->res);
  if (row == NULL) return SQL_NO_DATA;
  stmt->rowset.rows = row;
  stmt->rowset.nr_rows = 1;
  return SQL_SUCCESS;
}

int stmt_close_cursor(stmt_t *stmt)
{
  (void)stmt;
  return 0;
}

