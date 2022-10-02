#include "internal.h"

#include "desc.h"
#include "stmt.h"

#include <errno.h>
#include <iconv.h>
#include <inttypes.h>
#include <limits.h>
#include <string.h>
#include <time.h>
#include <wchar.h>

#define _stmt_malloc_fail(_stmt)             \
  stmt_append_err(_stmt,                     \
    "HY001",                                 \
    0,                                       \
    "memory allocation failure");

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

static void param_value_reset(param_value_t *value)
{
  if (value->inited && value->allocated) {
    TOD_SAFE_FREE(value->ptr);
  }
  value->allocated = 0;
  value->inited = 0;
}

static void params_reset_param_values(params_t *params)
{
  if (!params->values) return;
  for (size_t i=0; i<params->cap; ++i) {
    param_value_t *param_value = params->values + i;
    param_value_reset(param_value);
  }
}

static void params_reset_mbs(params_t *params)
{
  if (!params->mbs) return;
  memset(params->mbs, 0, sizeof(*params->mbs) * params->cap);
}

static void params_reset(params_t *params)
{
  params_reset_param_values(params);
  params_reset_mbs(params);
}

static void params_release(params_t *params)
{
  params_reset(params);
  TOD_SAFE_FREE(params->values);
  TOD_SAFE_FREE(params->mbs);
  params->cap = 0;
}

static int params_realloc(params_t *params, size_t cap)
{
  if (cap > params->cap) {
    params_reset(params);
    size_t n = (cap+ 15) / 16 *16;
    TAOS_MULTI_BIND *mbs        = (TAOS_MULTI_BIND*)realloc(params->mbs, n * sizeof(*mbs));
    param_value_t   *values     = (param_value_t*)realloc(params->values, n * sizeof(*values));
    if (mbs)    params->mbs     = mbs;
    if (values) params->values  = values;
    if (!mbs || !values) return -1;

    params->cap         = n;
  }
  memset(params->mbs,    0, params->cap * sizeof(*params->mbs));
  memset(params->values, 0, params->cap * sizeof(*params->values));
  return 0;
}

static void stmt_release_descriptors(stmt_t *stmt)
{
  descriptor_release(&stmt->APD);
  descriptor_release(&stmt->IPD);
  descriptor_release(&stmt->ARD);
  descriptor_release(&stmt->IRD);
}

static void stmt_init_descriptors(stmt_t *stmt)
{
  descriptor_init(&stmt->APD);
  descriptor_init(&stmt->IPD);
  descriptor_init(&stmt->ARD);
  descriptor_init(&stmt->IRD);

  stmt->current_APD = &stmt->APD;
  stmt->current_ARD = &stmt->ARD;
}

static int stmt_init(stmt_t *stmt, conn_t *conn)
{
  stmt->conn = conn_ref(conn);
  int prev = atomic_fetch_add(&conn->stmts, 1);
  OA_ILE(prev >= 0);

  errs_init(&stmt->errs);
  stmt_init_descriptors(stmt);

  stmt->refc = 1;

  return 0;
}

static void stmt_release_result(stmt_t *stmt)
{
  if (stmt->res) {
    if (stmt->res_is_from_taos_query) TAOS_free_result(stmt->res);

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
}

static void stmt_release_stmt(stmt_t *stmt)
{
  if (stmt->stmt) {
    int r = TAOS_stmt_close(stmt->stmt);
    OA_NIY(r == 0);
    stmt->stmt = NULL;
  }
}

void stmt_dissociate_ARD(stmt_t *stmt)
{
  if (stmt->associated_ARD == NULL) return;

  tod_list_del(&stmt->associated_ARD_node);
  desc_unref(stmt->associated_ARD);
  stmt->associated_ARD = NULL;
  stmt->current_ARD = &stmt->ARD;
}

static SQLRETURN stmt_associate_ARD(stmt_t *stmt, desc_t *desc)
{
  if (stmt->associated_ARD == desc) return SQL_SUCCESS;

  // FIXME:
  if (stmt->associated_APD == desc) {
    stmt_append_err(stmt, "HY024", 0, "descriptor already associated as statement's APD");
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

  tod_list_del(&stmt->associated_APD_node);
  desc_unref(stmt->associated_APD);
  stmt->associated_APD = NULL;
  stmt->current_APD = &stmt->APD;
}

static SQLRETURN stmt_associate_APD(stmt_t *stmt, desc_t *desc)
{
  if (stmt->associated_APD == desc) return SQL_SUCCESS;

  // FIXME:
  if (stmt->associated_ARD == desc) {
    stmt_append_err(stmt, "HY024", 0, "descriptor already associated as statement's ARD");
    return SQL_ERROR;
  }

  stmt_dissociate_APD(stmt);

  tod_list_add_tail(&stmt->associated_APD_node, &desc->associated_stmts_as_APD);
  desc_ref(desc);
  stmt->associated_APD = desc;
  stmt->current_APD = &desc->descriptor;

  return SQL_SUCCESS;
}

SQLRETURN stmt_set_row_desc(stmt_t *stmt, SQLPOINTER ValuePtr)
{
  if (ValuePtr == SQL_NULL_DESC) {
    stmt_dissociate_ARD(stmt);
    return SQL_SUCCESS;
  }

  desc_t *desc = (desc_t*)(SQLHANDLE)ValuePtr;
  if (desc->conn != stmt->conn) {
    stmt_append_err(stmt, "HY024", 0, "descriptor allocated on the connection other than that of the statement which is to be associated with");
    return SQL_ERROR;
  }

  return stmt_associate_ARD(stmt, desc);
}

SQLRETURN stmt_set_param_desc(stmt_t *stmt, SQLPOINTER ValuePtr)
{
  if (ValuePtr == SQL_NULL_DESC) {
    stmt_dissociate_APD(stmt);
    return SQL_SUCCESS;
  }

  desc_t *desc = (desc_t*)(SQLHANDLE)ValuePtr;
  if (desc->conn != stmt->conn) {
    stmt_append_err(stmt, "HY024", 0, "descriptor allocated on the connection other than that of the statement which is to be associated with");
    return SQL_ERROR;
  }

  return stmt_associate_APD(stmt, desc);
}

static void stmt_reset_current_for_get_data(stmt_t *stmt)
{
  col_t *col = &stmt->current_for_get_data;
  col_reset(col);
}

static void stmt_release_current_for_get_data(stmt_t *stmt)
{
  col_t *col = &stmt->current_for_get_data;
  col_release(col);
}

static void stmt_release(stmt_t *stmt)
{
  rowset_release(&stmt->rowset);
  stmt_release_result(stmt);

  stmt_dissociate_APD(stmt);

  params_release(&stmt->params);

  stmt_release_stmt(stmt);

  stmt_dissociate_ARD(stmt);

  stmt_release_current_for_get_data(stmt);

  int prev = atomic_fetch_sub(&stmt->conn->stmts, 1);
  OA_ILE(prev >= 1);
  conn_unref(stmt->conn);
  stmt->conn = NULL;

  stmt_release_descriptors(stmt);

  TOD_SAFE_FREE(stmt->sql);

  errs_release(&stmt->errs);

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

SQLRETURN stmt_free(stmt_t *stmt)
{
  stmt_unref(stmt);
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_post_exec(stmt_t *stmt)
{
  int e;
  const char *estr;
  e = TAOS_errno(stmt->res);
  estr = TAOS_errstr(stmt->res);

  if (e) {
    stmt_append_err(stmt, "HY000", e, estr);
    return SQL_ERROR;
  } else if (stmt->res) {
    stmt->time_precision = TAOS_result_precision(stmt->res);
    stmt->affected_row_count = TAOS_affected_rows(stmt->res);
    stmt->col_count = TAOS_field_count(stmt->res);
    if (stmt->col_count > 0) {
      stmt->fields = TAOS_fetch_fields(stmt->res);
    }
    stmt_reset_current_for_get_data(stmt);
  }

  return SQL_SUCCESS;
}

static descriptor_t* stmt_APD(stmt_t *stmt)
{
  return stmt->current_APD;
}

static descriptor_t* stmt_IPD(stmt_t *stmt)
{
  return &stmt->IPD;
}

static descriptor_t* stmt_ARD(stmt_t *stmt)
{
  return stmt->current_ARD;
}

SQLULEN* stmt_get_rows_fetched_ptr(stmt_t *stmt)
{
  descriptor_t *desc = stmt_ARD(stmt);
  desc_header_t *header = &desc->header;
  return header->DESC_ROWS_FETCHED_PTR;
}

static SQLRETURN _stmt_exec_direct_sql(stmt_t *stmt, const char *sql)
{
  OA_ILE(stmt);
  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->taos);
  OA_ILE(sql);

  if (stmt_get_rows_fetched_ptr(stmt)) *stmt_get_rows_fetched_ptr(stmt) = 0;
  rowset_reset(&stmt->rowset);
  stmt_release_result(stmt);
  params_reset(&stmt->params);

  TAOS *taos = stmt->conn->taos;

  if (!stmt->stmt) {
    stmt->res = TAOS_query(taos, sql);
    stmt->res_is_from_taos_query = stmt->res ? 1 : 0;
  } else {
    OA_NIY(0);
    int r = TAOS_stmt_execute(stmt->stmt);
    if (r) {
      stmt_append_err(stmt, "HY000", r, TAOS_stmt_errstr(stmt->stmt));
      return SQL_ERROR;
    }
    stmt->res = TAOS_stmt_use_result(stmt->stmt);
    stmt->res_is_from_taos_query = 0;
  }

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
        _stmt_malloc_fail(stmt);
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
  OA_ILE(stmt);
  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->taos);
  OA_ILE(sql);

  int prev = atomic_fetch_add(&stmt->conn->outstandings, 1);
  OA_ILE(prev >= 0);

  SQLRETURN sr = _stmt_exec_direct(stmt, sql, len);
  prev = atomic_fetch_sub(&stmt->conn->outstandings, 1);
  OA_ILE(prev >= 0);

  return sr;
}

SQLRETURN stmt_set_row_array_size(stmt_t *stmt, SQLULEN row_array_size)
{
  // if (row_array_size != 1) {
  //   stmt_append_err_format(stmt, "01S02", 0, "`SQL_ATTR_ROW_ARRAY_SIZE[%ld]` other than `1` is not supported yet", row_array_size);
  //   return SQL_SUCCESS_WITH_INFO;
  // }

  descriptor_t *desc = stmt_ARD(stmt);
  desc_header_t *header = &desc->header;
  header->DESC_ARRAY_SIZE = row_array_size;

  return SQL_SUCCESS;
}

SQLULEN stmt_get_row_array_size(stmt_t *stmt)
{
  descriptor_t *desc = stmt_ARD(stmt);
  desc_header_t *header = &desc->header;
  return header->DESC_ARRAY_SIZE;
}

SQLRETURN stmt_set_row_status_ptr(stmt_t *stmt, SQLUSMALLINT *row_status_ptr)
{
  descriptor_t *desc = stmt_ARD(stmt);
  desc_header_t *header = &desc->header;
  header->DESC_STATUS_PTR = row_status_ptr;
  return SQL_SUCCESS;
}

SQLUSMALLINT* stmt_get_row_status_ptr(stmt_t *stmt)
{
  descriptor_t *desc = stmt_ARD(stmt);
  desc_header_t *header = &desc->header;
  return header->DESC_STATUS_PTR;
}

int stmt_get_row_count(stmt_t *stmt, SQLLEN *row_count_ptr)
{
  *row_count_ptr = stmt->affected_row_count;
  return 0;
}

int stmt_get_col_count(stmt_t *stmt, SQLSMALLINT *col_count_ptr)
{
  *col_count_ptr = stmt->col_count;
  return 0;
}

SQLRETURN stmt_set_row_bind_type(stmt_t *stmt, SQLULEN row_bind_type)
{
  if (row_bind_type != SQL_BIND_BY_COLUMN) {
    stmt_append_err(stmt, "HY000", 0, "only `SQL_BIND_BY_COLUMN` is supported now");
    return SQL_ERROR;
  }

  descriptor_t *desc = stmt_ARD(stmt);
  desc_header_t *header = &desc->header;
  header->DESC_BIND_TYPE = row_bind_type;

  return SQL_SUCCESS;
}

SQLULEN stmt_get_row_bind_type(stmt_t *stmt)
{
  descriptor_t *desc = stmt_ARD(stmt);
  desc_header_t *header = &desc->header;
  return header->DESC_BIND_TYPE;
}

SQLRETURN stmt_set_rows_fetched_ptr(stmt_t *stmt, SQLULEN *rows_fetched_ptr)
{
  descriptor_t *desc = stmt_ARD(stmt);
  desc_header_t *header = &desc->header;
  header->DESC_ROWS_FETCHED_PTR = rows_fetched_ptr;

  return SQL_SUCCESS;
}

SQLRETURN stmt_set_max_length(stmt_t *stmt, SQLULEN max_length)
{
  if (max_length != 0) {
    stmt_append_err(stmt, "01S02", 0, "`SQL_ATTR_MAX_LENGTH` of non-zero is not supported");
    return SQL_SUCCESS_WITH_INFO;
  }
  // stmt->attrs.ATTR_MAX_LENGTH = max_length;
  return SQL_SUCCESS;
}

SQLRETURN stmt_set_row_bind_offset_ptr(stmt_t *stmt, SQLULEN *row_bind_offset_ptr)
{
  descriptor_t *desc = stmt_ARD(stmt);
  desc_header_t *header = &desc->header;
  header->DESC_BIND_OFFSET_PTR = row_bind_offset_ptr;

  return SQL_SUCCESS;
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
  TAOS_FIELD *p = stmt->fields + ColumnNumber - 1;
  SQLRETURN sr = SQL_SUCCESS;

  if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
  if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;

  int n;
  n = snprintf((char*)ColumnName, BufferLength, "%s", p->name);
  if (NameLengthPtr) {
    *NameLengthPtr = n;
  }

  if (n >= BufferLength) {
    stmt_append_err_format(stmt, "01004", 0, "String right truncated for Column `%s[#%d]`", p->name, ColumnNumber);
    sr = SQL_SUCCESS_WITH_INFO;
  }

  if (NullablePtr) *NullablePtr = SQL_NULLABLE_UNKNOWN;
  switch (p->type) {
    case TSDB_DATA_TYPE_TINYINT:
      if (DataTypePtr) {
        if (stmt->conn->cfg.tinyint_to_smallint) {
          *DataTypePtr   = SQL_SMALLINT;
        } else {
          *DataTypePtr   = SQL_TINYINT;
        }
      }
      if (ColumnSizePtr)    *ColumnSizePtr = 3;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      if (DataTypePtr)      *DataTypePtr = SQL_SMALLINT;
      if (ColumnSizePtr)    *ColumnSizePtr = 5;
      break;
    case TSDB_DATA_TYPE_INT:
      if (DataTypePtr)      *DataTypePtr = SQL_INTEGER;
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
      OA(0, "`%s[%d]` for Column `%s[#%d]` not implemented yet", taos_data_type(p->type), p->type, p->name, ColumnNumber);
      stmt_append_err_format(stmt, "HY000", 0, "`%s[%d]` for Column `%s[#%d]` not implemented yet", taos_data_type(p->type), p->type, p->name, ColumnNumber);
      return SQL_ERROR;
  }

  return sr;
}

static SQLRETURN _stmt_calc_bytes(stmt_t *stmt,
    const char *fromcode, const char *s, size_t len,
    const char *tocode, size_t *bytes)
{
  iconv_t cd = iconv_open(tocode, fromcode);
  if ((size_t)cd == (size_t)-1) {
    stmt_append_err_format(stmt, "HY000", 0,
        "[iconv] No character set conversion found for `%s` to `%s`: [%d] %s",
        fromcode, tocode, errno, strerror(errno));
    return SQL_ERROR;
  }

  SQLRETURN sr = SQL_SUCCESS;

  char * inbuf = (char*)s;
  size_t inbytes = len;

  *bytes = 0;
  while (inbytes>0) {
    char buf[2];
    char *outbuf = buf;
    size_t outbytes = sizeof(buf);
    size_t sz = iconv(cd, &inbuf, &inbytes, &outbuf, &outbytes);
    *bytes += (sizeof(buf) - outbytes);
    if (sz == (size_t)-1) {
      int e = errno;
      if (e == E2BIG) continue;
      stmt_append_err_format(stmt, "HY000", 0,
          "[iconv] Character set conversion for `%s` to `%s` failed: [%d] %s",
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
    stmt_append_err_format(stmt, "01004", 0,
        "[iconv] Character set conversion for `%s` to `%s` results in string truncation, #%ld out of #%ld bytes consumed, #%ld out of #%ld bytes converted: [%d] %s",
        fromcode, tocode, inbytes - *inbytesleft, inbytes, outbytes - * outbytesleft, outbytes, e, strerror(e));
    return SQL_SUCCESS_WITH_INFO;
  }
  if (sz == (size_t)-1) {
    stmt_append_err_format(stmt, "HY000", 0,
        "[iconv] Character set conversion for `%s` to `%s` failed: [%d] %s",
        fromcode, tocode, e, strerror(e));
    return SQL_ERROR;
  }
  if (sz > 0) {
    // FIXME: what actually means when sz > 0???
    stmt_append_err_format(stmt, "HY000", 0,
        "[iconv] Character set conversion for `%s` to `%s` succeeded with #[%ld] of nonreversible characters converted",
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
  descriptor_t *desc = stmt_ARD(stmt);
  desc_header_t *header = &desc->header;
  if (ColumnNumber >= desc->cap) {
    size_t cap = (ColumnNumber + 15) / 16 * 16;
    desc_record_t *records = (desc_record_t*)realloc(desc->records, sizeof(*records) * cap);
    if (!records) {
      _stmt_malloc_fail(stmt);
      return SQL_ERROR;
    }
    desc->records = records;
    desc->cap     = cap;
  }

  desc_record_t *record = desc->records + ColumnNumber - 1;
  memset(record, 0, sizeof(*record));

  switch (TargetType) {
    case SQL_C_UTINYINT:
      record->DESC_LENGTH            = 1;
      record->DESC_PRECISION         = 0;
      record->DESC_SCALE             = 0;
      record->DESC_TYPE              = TargetType;
      record->DESC_CONCISE_TYPE      = TargetType;
      record->element_size_in_column_wise = record->DESC_LENGTH;
      break;
    case SQL_C_SHORT:
      record->DESC_LENGTH            = 2;
      record->DESC_PRECISION         = 0;
      record->DESC_SCALE             = 0;
      record->DESC_TYPE              = TargetType;
      record->DESC_CONCISE_TYPE      = TargetType;
      record->element_size_in_column_wise = record->DESC_LENGTH;
      break;
    case SQL_C_SLONG:
      record->DESC_LENGTH            = 4;
      record->DESC_PRECISION         = 0;
      record->DESC_SCALE             = 0;
      record->DESC_TYPE              = TargetType;
      record->DESC_CONCISE_TYPE      = TargetType;
      record->element_size_in_column_wise = record->DESC_LENGTH;
      break;
    case SQL_C_SBIGINT:
      record->DESC_LENGTH            = 8;
      record->DESC_PRECISION         = 0;
      record->DESC_SCALE             = 0;
      record->DESC_TYPE              = TargetType;
      record->DESC_CONCISE_TYPE      = TargetType;
      record->element_size_in_column_wise = record->DESC_LENGTH;
      break;
    case SQL_C_DOUBLE:
      record->DESC_LENGTH            = 8;
      record->DESC_PRECISION         = 0;
      record->DESC_SCALE             = 0;
      record->DESC_TYPE              = TargetType;
      record->DESC_CONCISE_TYPE      = TargetType;
      record->element_size_in_column_wise = record->DESC_LENGTH;
      break;
    case SQL_C_CHAR:
      record->DESC_LENGTH            = 0; // FIXME:
      record->DESC_PRECISION         = 0;
      record->DESC_SCALE             = 0;
      record->DESC_TYPE              = TargetType;
      record->DESC_CONCISE_TYPE      = TargetType;
      record->element_size_in_column_wise = BufferLength;
      break;
    case SQL_C_WCHAR:
      record->DESC_LENGTH            = 0; // FIXME:
      record->DESC_PRECISION         = 0;
      record->DESC_SCALE             = 0;
      record->DESC_TYPE              = TargetType;
      record->DESC_CONCISE_TYPE      = TargetType;
      record->element_size_in_column_wise = BufferLength;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "converstion to `%s[%d]` not implemented yet",
          sql_c_data_type(TargetType), TargetType);
      return SQL_ERROR;
      break;
  }

  record->DESC_OCTET_LENGTH        = BufferLength;
  record->DESC_DATA_PTR            = TargetValuePtr;
  record->DESC_INDICATOR_PTR       = StrLen_or_IndPtr;
  record->DESC_OCTET_LENGTH_PTR    = StrLen_or_IndPtr;

  if (ColumnNumber > header->DESC_COUNT) header->DESC_COUNT = ColumnNumber;

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_get_data_len(stmt_t *stmt, int row, int col, const char **data, int *len)
{
  TAOS_FIELD *field = stmt->fields + col;
  switch(field->type) {
    case TSDB_DATA_TYPE_BOOL:
      if (TAOS_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        unsigned char *base = (unsigned char*)stmt->rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_TINYINT:
      if (TAOS_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        unsigned char *base = (unsigned char*)stmt->rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_SMALLINT:
      if (TAOS_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        int16_t *base = (int16_t*)stmt->rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_INT:
      if (TAOS_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        int32_t *base = (int32_t*)stmt->rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_BIGINT:
      if (TAOS_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        int64_t *base = (int64_t*)stmt->rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_FLOAT:
      if (TAOS_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        float *base = (float*)stmt->rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_DOUBLE:
      if (TAOS_is_null(stmt->res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        double *base = (double*)stmt->rows[col];
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
      int *offsets = TAOS_get_column_data_offset(stmt->res, col);
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
          "`%s[%d]` not implemented yet",
          taos_data_type(field->type), field->type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static int _stmt_bind_conv_tsdb_tinyint_to_sql_c_utinyint(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(int8_t));

  int8_t v = *(int8_t*)data;

  OD("v: [%d]", v);

  size_t outbytes = dlen;
  OA_NIY(outbytes == sizeof(int8_t));

  // FIXME: check signness?
  *(int8_t*)dest = v;

  return outbytes;
}

static SQLRETURN _stmt_get_conv_to_sql_c_utinyint(stmt_t *stmt, tsdb_to_sql_c_f *conv, int taos_type)
{
  (void)conv;
  switch (taos_type) {
    case TSDB_DATA_TYPE_TINYINT:
      *conv = _stmt_bind_conv_tsdb_tinyint_to_sql_c_utinyint;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "converstion from `%s[%d]` to `SQL_C_UTINYINT` not implemented yet",
          taos_data_type(taos_type), taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static int _stmt_bind_conv_tsdb_tinyint_to_sql_c_short(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(int8_t));

  int8_t v = *(int8_t*)data;

  OD("v: [%d]", v);

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

  OD("v: [%d]", v);

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
          "converstion from `%s[%d]` to `SQL_C_SHORT` not implemented yet",
          taos_data_type(taos_type), taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static int _stmt_bind_conv_tsdb_int_to_sql_c_slong(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(int32_t));

  int32_t v = *(int32_t*)data;

  OD("v: [%d]", v);

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
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "converstion from `%s[%d]` to `SQL_C_SLONG` not implemented yet",
          taos_data_type(taos_type), taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static int _stmt_bind_conv_tsdb_bigint_to_sql_c_sbigint(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(int64_t));

  int64_t v = *(int64_t*)data;

  OD("v: [%ld]", v);

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
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "converstion from `%s[%d]` to `SQL_C_SBIGINT` not implemented yet",
          taos_data_type(taos_type), taos_type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static int _stmt_bind_conv_tsdb_double_to_sql_c_double(stmt_t *stmt, const char *data, int len, char *dest, int dlen)
{
  (void)stmt;

  OA_NIY(len == sizeof(double));

  double v = *(double*)data;

  OD("v: [%g]", v);

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
          "converstion from `%s[%d]` to `SQL_C_DOUBLE` not implemented yet",
          taos_data_type(taos_type), taos_type);
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

  OD("[%ld]", v);

  int nn = snprintf(outbuf, outbytes, "%ld", v);
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
      OD("converstion from `%s[%d]` to `SQL_C_CHAR` not implemented yet",
          taos_data_type(taos_type), taos_type);
      stmt_append_err_format(stmt, "HY000", 0,
          "converstion from `%s[%d]` to `SQL_C_CHAR` not implemented yet",
          taos_data_type(taos_type), taos_type);
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

  SQLRETURN sr = _stmt_encode(stmt, "utf8", &inbuf, &inbytes, "ucs2", &outbuf, &outbytes);
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

  SQLRETURN sr = _stmt_encode(stmt, "utf8", &inbuf, &inbytes, "ucs2", &outbuf, &outbytes);
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

  SQLRETURN sr = _stmt_encode(stmt, "utf8", &inbuf, &inbytes, "ucs2", &outbuf, &outbytes);
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

  SQLRETURN sr = _stmt_encode(stmt, "utf8", &inbuf, &inbytes, "ucs2", &outbuf, &outbytes);
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
          "converstion from `%s[%d]` to `SQL_C_WCHAR` not implemented yet",
          taos_data_type(taos_type), taos_type);
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
      stmt_append_err_format(stmt, "07006", 0,
          "converstion to `%s[%d]` not implemented yet",
          sql_c_data_type(TargetType), TargetType);
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

  descriptor_t *desc = stmt_ARD(stmt);
  desc_header_t *header = &desc->header;

  if (i_col >= header->DESC_COUNT) return SQL_SUCCESS;

  desc_record_t *record = desc->records + i_col;
  if (record->DESC_DATA_PTR == NULL) return SQL_SUCCESS;

  char *base = record->DESC_DATA_PTR;
  if (header->DESC_BIND_OFFSET_PTR) base += *header->DESC_BIND_OFFSET_PTR;

  char *dest = base + record->element_size_in_column_wise * i_row;
  char *ptr = record->DESC_DATA_PTR;
  dest = _stmt_get_address(stmt, ptr, record->element_size_in_column_wise, i_row, header);

  const char *data;
  int len;
  sr = _stmt_get_data_len(stmt, i_row + stmt->rowset.i_row, i_col, &data, &len);
  if (!sql_succeeded(sr)) return SQL_ERROR;

  if (!data) {
    _stmt_set_len_or_ind(stmt, i_row, header, record, SQL_NULL_DATA);
    OD("null");
    return SQL_SUCCESS;
  }

  int bytes = record->conv(stmt, data, len, dest, record->DESC_OCTET_LENGTH);
  OA_NIY(bytes >= 0);
  if (bytes < 0) return SQL_ERROR;

  _stmt_set_len_or_ind(stmt, i_row, header, record, bytes);

  return SQL_SUCCESS;
}

SQLRETURN stmt_fetch(stmt_t *stmt)
{
  SQLULEN row_array_size = stmt_get_row_array_size(stmt);
  OA_NIY(row_array_size > 0);

  SQLRETURN sr = SQL_SUCCESS;

  TAOS_ROW rows = NULL;
  OA_NIY(stmt->res);
  if (stmt->rowset.i_row + row_array_size >= (SQLULEN)stmt->nr_rows) {
    rowset_reset(&stmt->rowset);

    int nr_rows = TAOS_fetch_block(stmt->res, &rows);
    if (nr_rows == 0) return SQL_NO_DATA;
    stmt->rows = rows;          // column-wise
    stmt->nr_rows = nr_rows;
    stmt->rowset.i_row = 0;

    stmt->lengths = TAOS_fetch_lengths(stmt->res);
    OA_NIY(stmt->lengths);
  } else {
    stmt->rowset.i_row += row_array_size;
  }

  stmt_reset_current_for_get_data(stmt);

  descriptor_t *desc = stmt_ARD(stmt);
  desc_header_t *header = &desc->header;
  OA_NIY(desc->cap >= header->DESC_COUNT);
  for (int i_col = 0; (size_t)i_col < desc->cap; ++i_col) {
    if (i_col >= header->DESC_COUNT) continue;
    desc_record_t *record = desc->records + i_col;
    if (record->DESC_DATA_PTR == NULL) continue;

    TAOS_FIELD *field = stmt->fields + i_col;
    int taos_type = field->type;

    SQLSMALLINT TargetType = record->DESC_TYPE;

    sr = _stmt_get_conv_from_tsdb_to_sql_c(stmt, &record->conv, taos_type, TargetType);
    if (!sql_succeeded(sr)) return SQL_ERROR;
  }

  if (header->DESC_BIND_TYPE != SQL_BIND_BY_COLUMN) {
    stmt_append_err(stmt, "HY000", 0, "only `SQL_BIND_BY_COLUMN` is supported now");
    return SQL_ERROR;
  }

  if (header->DESC_ROWS_FETCHED_PTR) *header->DESC_ROWS_FETCHED_PTR = 0;

  for (int i_row = 0; (SQLULEN)i_row<row_array_size; ++i_row) {
    if (i_row + stmt->rowset.i_row >= stmt->nr_rows) break;

    for (int i_col = 0; (size_t)i_col < desc->cap; ++i_col) {
      sr = _stmt_fill_rowset(stmt, i_row, i_col);
      if (sr == SQL_ERROR) return SQL_ERROR;
    }

    if (header->DESC_STATUS_PTR) header->DESC_STATUS_PTR[i_row] = SQL_ROW_SUCCESS;
    if (header->DESC_ROWS_FETCHED_PTR) *header->DESC_ROWS_FETCHED_PTR += 1;
  }

  return SQL_SUCCESS;
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
    if (BufferLength == 0) {
      stmt_append_err_format(stmt, "01004", 0, "Data buffer supplied is too small to hold the null-termination character for Column_or_Param[#%d]", current->i_col + 1);
      return SQL_SUCCESS_WITH_INFO;
    }
    if (StrLen_or_IndPtr) *StrLen_or_IndPtr = n;
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
      _stmt_malloc_fail(stmt);
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
      _stmt_malloc_fail(stmt);
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
      _stmt_malloc_fail(stmt);
      return SQL_ERROR;
    }
  }

  return _stmt_get_data_fill_sql_c_char_with_buf(stmt, TargetValuePtr, BufferLength, StrLen_or_IndPtr);
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
          "converstion from `%s[%d]` to `SQL_C_CHAR` not implemented yet",
          taos_data_type(taos_type), taos_type);
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
    stmt_append_err(stmt, "07009", 0, "The value specified for the argument `Col_or_Param_Num` is out of range");
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
      stmt_append_err_format(stmt, "HY000", 0, "TargetType changes in successive SQLGetData call for Column_or_Param[#%d]", Col_or_Param_Num);
      return SQL_ERROR;
    }
  }
  if (current->data == NULL) {
    if (StrLen_or_IndPtr) {
      StrLen_or_IndPtr[0/*stmt->rowset.i_row*/] = SQL_NULL_DATA;
      return SQL_SUCCESS;
    }
    stmt_append_err_format(stmt, "22002", 0, "Indicator variable required but not supplied for Column_or_Param[#%d]", Col_or_Param_Num);
    return SQL_ERROR;
  }

  stmt_get_data_fill_f fill = NULL;

  switch (TargetType) {
    case SQL_C_CHAR:
      if (current->len == 0) {
        if (StrLen_or_IndPtr) *StrLen_or_IndPtr = 0;
        return SQL_NO_DATA;
      }

      sr = _stmt_get_data_fill_fn_by_target_sql_c_char(stmt, &fill);
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "converstion to `%s[%d]` not implemented yet",
          sql_c_data_type(TargetType), TargetType);
      return SQL_ERROR;
  }

  if (sr == SQL_ERROR) return sr;

  return fill(stmt, TargetValuePtr, BufferLength, StrLen_or_IndPtr);
}

SQLRETURN _stmt_prepare(stmt_t *stmt, const char *sql, size_t len)
{
  stmt->stmt = TAOS_stmt_init(stmt->conn->taos);
  if (!stmt->stmt) {
    stmt_append_err(stmt, "HY000", TAOS_errno(NULL), TAOS_errstr(NULL));
    return SQL_ERROR;
  }

  int r;
  r = TAOS_stmt_prepare(stmt->stmt, sql, len);
  OA_NIY(r == 0);

  int32_t isInsert = 0;
  r = TAOS_stmt_is_insert(stmt->stmt, &isInsert);
  isInsert = !!isInsert;

  if (r) {
    stmt_append_err(stmt, "HY000", r, TAOS_stmt_errstr(stmt->stmt));
    stmt_release_stmt(stmt);

    return SQL_ERROR;
  }

  stmt->is_insert_stmt = isInsert;

  int nr_params = 0;
  r = TAOS_stmt_num_params(stmt->stmt, &nr_params);
  if (r) {
    stmt_append_err(stmt, "HY000", r, TAOS_stmt_errstr(stmt->stmt));
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
      stmt_append_err_format(stmt, "HY000", r, "prepared statement for `INSERT` failed: %s", taos_stmt_errstr(stmt->stmt));
      stmt_release_stmt(stmt);
      return SQL_ERROR;
    }
  }

  return SQL_SUCCESS;
}

SQLRETURN stmt_prepare(stmt_t *stmt, const char *sql, size_t len)
{
  OA_NIY(stmt->res == NULL);
  OA_NIY(stmt->stmt == NULL);

  const char *sqlx;
  size_t n;
  char *s, *pre;

  if (stmt->conn->cfg.cache_sql) {
    s = strndup(sql, len);
    if (!s) {
      _stmt_malloc_fail(stmt);
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
  if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
  if (NullablePtr)      *NullablePtr      = SQL_NULLABLE_UNKNOWN;
  int r;
  int idx = ParameterNumber - 1;
  OA_NIY(idx >= 0);
  OA_NIY(idx < stmt->nr_params);
  int type;
  int bytes;
  r = TAOS_stmt_get_param(stmt->stmt, idx, &type, &bytes);
  if (r) {
    if (stmt->is_insert_stmt) {
      stmt_append_err(stmt, "HY000", r, TAOS_stmt_errstr(stmt->stmt));
      return SQL_ERROR;
    }
    // FIXME: return SQL_VARCHAR and hard-coded parameters for the moment
    if (DataTypePtr)         *DataTypePtr = SQL_VARCHAR;
    if (ParameterSizePtr)    *ParameterSizePtr = 1024; /* hard-coded */
    stmt_append_err(stmt, "HY000", 0, "Arbitrary `SQL_VARCHAR(1024s)` is chosen to return because of taos lacking parm-desc for non-insert-statement");
    return SQL_SUCCESS_WITH_INFO;
  }

  switch (type) {
    case TSDB_DATA_TYPE_INT:
      if (DataTypePtr)         *DataTypePtr      = SQL_INTEGER;
      if (ParameterSizePtr)    *ParameterSizePtr = 10;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      if (DataTypePtr)         *DataTypePtr      = SQL_VARCHAR;
      if (ParameterSizePtr)    *ParameterSizePtr = bytes - 2;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      if (DataTypePtr)      *DataTypePtr      = SQL_TYPE_TIMESTAMP;
      if (DecimalDigitsPtr) *DecimalDigitsPtr = (stmt->time_precision + 1) * 3;
      if (ParameterSizePtr) *ParameterSizePtr = 20 + *DecimalDigitsPtr;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      if (DataTypePtr)         *DataTypePtr      = SQL_WVARCHAR;
      /* taos internal storage: sizeof(int16_t) + payload */
      if (ParameterSizePtr)    *ParameterSizePtr = (bytes - 2) / 4;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0, "`%s[%d]` not implemented yet", taos_data_type(type), type);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
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
      stmt_append_err_format(stmt,
        "HY000",
        0,
        "unable to guess taos-data-type for `%s[%d]`",
        sql_data_type(ParameterType), ParameterType);
      return SQL_ERROR;
  }

  *taos_bytes = 0;

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

  int type;
  int bytes;
  int r = 0;
  r = TAOS_stmt_get_param(stmt->stmt, ParameterNumber-1, &type, &bytes);
  if (r) {
    if (stmt->is_insert_stmt) {
      stmt_append_err(stmt, "HY000", r, taos_stmt_errstr(stmt->stmt));
      stmt_release_stmt(stmt);
      return SQL_ERROR;
    }
    SQLRETURN sr = _stmt_guess_taos_data_type(stmt, ParameterType, BufferLength, &type, &bytes);
    if (!sql_succeeded(sr)) return sr;
  }

  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;
  if (ParameterNumber > APD->cap) {
    size_t cap = (ParameterNumber + 15) / 16 * 16;
    desc_record_t *records = (desc_record_t*)realloc(APD->records, sizeof(*records) * cap);
    if (!records) {
      _stmt_malloc_fail(stmt);
      return SQL_ERROR;
    }
    APD->records = records;
    APD->cap     = cap;
  }

  desc_record_t *APD_record = APD->records + ParameterNumber - 1;
  memset(APD_record, 0, sizeof(*APD_record));

  SQLSMALLINT vt = ValueType;

  if (ValueType == SQL_C_DEFAULT) {
    switch (ParameterType) {
      case SQL_INTEGER:
        vt = SQL_C_SLONG;
        break;
      case SQL_BIGINT:
        vt = SQL_C_SBIGINT;
        break;
      case SQL_VARCHAR:
        vt = SQL_C_CHAR;
        break;
      case SQL_WVARCHAR:
        vt = SQL_C_WCHAR;
        break;
      default:
        {
          stmt_append_err_format(stmt, "HY000", 0,
              "no default sql_c_data type for `%s[%d]`",
              sql_data_type(ParameterType), ParameterType);
          return SQL_ERROR;
        }
    }
  }

  switch (vt) {
    case SQL_C_UTINYINT:
      APD_record->DESC_LENGTH            = 1;
      APD_record->DESC_PRECISION         = 0;
      APD_record->DESC_SCALE             = 0;
      APD_record->DESC_TYPE              = vt;
      APD_record->DESC_CONCISE_TYPE      = vt;
      APD_record->element_size_in_column_wise = APD_record->DESC_LENGTH;
      break;
    case SQL_C_SHORT:
      APD_record->DESC_LENGTH            = 2;
      APD_record->DESC_PRECISION         = 0;
      APD_record->DESC_SCALE             = 0;
      APD_record->DESC_TYPE              = vt;
      APD_record->DESC_CONCISE_TYPE      = vt;
      APD_record->element_size_in_column_wise = APD_record->DESC_LENGTH;
      break;
    case SQL_C_SLONG:
      APD_record->DESC_LENGTH            = 4;
      APD_record->DESC_PRECISION         = 0;
      APD_record->DESC_SCALE             = 0;
      APD_record->DESC_TYPE              = vt;
      APD_record->DESC_CONCISE_TYPE      = vt;
      APD_record->element_size_in_column_wise = APD_record->DESC_LENGTH;
      break;
    case SQL_C_SBIGINT:
      APD_record->DESC_LENGTH            = 8;
      APD_record->DESC_PRECISION         = 0;
      APD_record->DESC_SCALE             = 0;
      APD_record->DESC_TYPE              = vt;
      APD_record->DESC_CONCISE_TYPE      = vt;
      APD_record->element_size_in_column_wise = APD_record->DESC_LENGTH;
      break;
    case SQL_C_DOUBLE:
      APD_record->DESC_LENGTH            = 8;
      APD_record->DESC_PRECISION         = 0;
      APD_record->DESC_SCALE             = 0;
      APD_record->DESC_TYPE              = vt;
      APD_record->DESC_CONCISE_TYPE      = vt;
      APD_record->element_size_in_column_wise = APD_record->DESC_LENGTH;
      break;
    case SQL_C_CHAR:
      APD_record->DESC_LENGTH            = 0; // FIXME:
      APD_record->DESC_PRECISION         = 0;
      APD_record->DESC_SCALE             = 0;
      APD_record->DESC_TYPE              = vt;
      APD_record->DESC_CONCISE_TYPE      = vt;
      APD_record->element_size_in_column_wise = BufferLength;
      break;
    case SQL_C_WCHAR:
      APD_record->DESC_LENGTH            = 0; // FIXME:
      APD_record->DESC_PRECISION         = 0;
      APD_record->DESC_SCALE             = 0;
      APD_record->DESC_TYPE              = vt;
      APD_record->DESC_CONCISE_TYPE      = vt;
      APD_record->element_size_in_column_wise = BufferLength;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "converstion from `%s[%d]` not implemented yet",
          sql_c_data_type(vt), vt);
      return SQL_ERROR;
  }

  APD_record->DESC_OCTET_LENGTH        = BufferLength;
  APD_record->DESC_DATA_PTR            = ParameterValuePtr;
  APD_record->DESC_INDICATOR_PTR       = StrLen_or_IndPtr;
  APD_record->DESC_OCTET_LENGTH_PTR    = StrLen_or_IndPtr;

  ////////////////
  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  if (ParameterNumber >= IPD->cap) {
    size_t cap = (ParameterNumber + 15) / 16 * 16;
    desc_record_t *records = (desc_record_t*)realloc(IPD->records, sizeof(*records) * cap);
    if (!records) {
      _stmt_malloc_fail(stmt);
      return SQL_ERROR;
    }
    IPD->records = records;
    IPD->cap     = cap;
  }

  desc_record_t *IPD_record = IPD->records + ParameterNumber - 1;
  memset(IPD_record, 0, sizeof(*IPD_record));

  switch (ParameterType) {
    case SQL_TYPE_TIMESTAMP:
      IPD_record->DESC_PARAMETER_TYPE           = InputOutputType;
      IPD_record->DESC_TYPE                     = SQL_DATETIME;
      IPD_record->DESC_CONCISE_TYPE             = ParameterType;
      IPD_record->DESC_LENGTH                   = ColumnSize;
      IPD_record->DESC_PRECISION                = DecimalDigits;
      break;
    case SQL_INTEGER:
      IPD_record->DESC_PARAMETER_TYPE           = InputOutputType;
      IPD_record->DESC_TYPE                     = ParameterType;
      IPD_record->DESC_CONCISE_TYPE             = ParameterType;
      IPD_record->DESC_LENGTH                   = 0;
      IPD_record->DESC_PRECISION                = 0;
      break;
    case SQL_BIGINT:
      IPD_record->DESC_PARAMETER_TYPE           = InputOutputType;
      IPD_record->DESC_TYPE                     = ParameterType;
      IPD_record->DESC_CONCISE_TYPE             = ParameterType;
      IPD_record->DESC_LENGTH                   = 0;
      IPD_record->DESC_PRECISION                = 0;
      break;
    case SQL_VARCHAR:
      IPD_record->DESC_PARAMETER_TYPE           = InputOutputType;
      IPD_record->DESC_TYPE                     = ParameterType;
      IPD_record->DESC_CONCISE_TYPE             = ParameterType;
      IPD_record->DESC_LENGTH                   = ColumnSize;
      IPD_record->DESC_PRECISION                = 0;
      break;
    case SQL_WVARCHAR:
      IPD_record->DESC_PARAMETER_TYPE           = InputOutputType;
      IPD_record->DESC_TYPE                     = ParameterType;
      IPD_record->DESC_CONCISE_TYPE             = ParameterType;
      IPD_record->DESC_LENGTH                   = ColumnSize;
      IPD_record->DESC_PRECISION                = 0;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "converstion from `%s[%d]` not implemented yet",
          sql_data_type(ParameterType), ParameterType);
      return SQL_ERROR;
  }

  IPD_record->taos_type  = type;
  IPD_record->taos_bytes = bytes;

  if (ParameterNumber > APD_header->DESC_COUNT) APD_header->DESC_COUNT = ParameterNumber;
  if (ParameterNumber > IPD_header->DESC_COUNT) IPD_header->DESC_COUNT = ParameterNumber;

  return SQL_SUCCESS;
}

#define _stmt_conv_bounded_param_fail(_stmt, _param_bind)             \
  OD("ColumnSize[%ld]; DecimalDigits[%d]; BufferLength[%ld]",         \
      _param_bind->ColumnSize,                                        \
      _param_bind->DecimalDigits,                                     \
      _param_bind->BufferLength);                                     \
  stmt_append_err_format(_stmt,                                       \
      "HY000",                                                        \
      0,                                                              \
      "`%s` to `%s` for param `%d` not supported yet by taos",        \
      sql_c_data_type(_param_bind->ValueType),                        \
      TAOS_data_type(_param_bind->taos_type),                         \
      _param_bind->ParameterNumber)

#define _stmt_param_bind_fail(_stmt, _param_bind, _r)     \
  stmt_append_err(_stmt,                                  \
    "HY000",                                              \
    _r,                                                   \
    TAOS_stmt_errstr(_stmt->stmt))

static SQLRETURN _stmt_sql_c_char_to_tsdb_timestamp(stmt_t *stmt, const char *s, int64_t *timestamp)
{
  char *end;
  errno = 0;
  long int v = strtol(s, &end, 0);
  int e = errno;
  if (e == ERANGE && (v == LONG_MAX || v == LONG_MIN)) {
    stmt_append_err_format(stmt,
        "HY000",
        0,
        "convertion from `SQL_C_CHAR` to `TSDB_DATA_TYPE_TIMESTAMP` failed: [%d] %s",
        e, strerror(e));
    return SQL_ERROR;
  }
  if (e != 0) {
    stmt_append_err_format(stmt,
        "HY000",
        0,
        "convertion from `SQL_C_CHAR` to `TSDB_DATA_TYPE_TIMESTAMP` failed: [%d] %s",
        e, strerror(e));
    return SQL_ERROR;
  }
  if (end == s) {
    stmt_append_err(stmt,
        "HY000",
        0,
        "convertion from `SQL_C_CHAR` to `TSDB_DATA_TYPE_TIMESTAMP` failed: no digits at all");
    return SQL_ERROR;
  }
  if (end && *end) {
    stmt_append_err_format(stmt,
        "HY000",
        0,
        "convertion from `SQL_C_CHAR` to `TSDB_DATA_TYPE_TIMESTAMP` failed: string following digits[%s]",
        end);
    return SQL_ERROR;
  }

  *timestamp = v;
  return SQL_SUCCESS;
}

static SQLRETURN _stmt_bind_param_tsdb(stmt_t *stmt, int i_param)
{
  TAOS_MULTI_BIND *mb        = stmt->params.mbs    + i_param;
  param_value_t   *value     = stmt->params.values + i_param;

  mb->buffer_length = value->length;
  mb->length        = &value->length;
  mb->is_null       = &value->is_null;
  // TODO: currently only one row of parameters to bind
  mb->num = 1;

  int r = TAOS_stmt_bind_single_param_batch(stmt->stmt, mb, i_param);
  if (r) {
    stmt_append_err_format(stmt, "HY000", r, "Param[#%d,%s] for [%s]: %s", i_param+1, taos_data_type(mb->buffer_type), stmt->sql, TAOS_stmt_errstr(stmt->stmt));
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_bind_param_with_sql_c_null(stmt_t *stmt, int i_param)
{
  descriptor_t *APD = stmt_APD(stmt);
  descriptor_t *IPD = stmt_IPD(stmt);

  desc_record_t *IPD_record = IPD->records + i_param;
  desc_record_t *APD_record = APD->records + i_param;

  SQLPOINTER    data_base = APD_record->DESC_DATA_PTR;
  SQLLEN       *len_base  = APD_record->DESC_OCTET_LENGTH_PTR;
  (void)data_base;
  (void)len_base;

  SQLSMALLINT ParameterType = IPD_record->DESC_TYPE;
  switch (ParameterType) {
    case SQL_DATETIME:
      break;
    case SQL_INTEGER:
      break;
    case SQL_BIGINT:
      break;
    case SQL_VARCHAR:
      break;
    case SQL_WVARCHAR:
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "parameter type of `%s[%d]` not implemented yet",
          sql_data_type(ParameterType), ParameterType);
      return SQL_ERROR;
  }

  TAOS_MULTI_BIND *mb        = stmt->params.mbs    + i_param;
  param_value_t   *value     = stmt->params.values + i_param;

  int taos_type = IPD_record->taos_type;
  switch (taos_type) {
    case TSDB_DATA_TYPE_VARCHAR:
      value->tsdb_varchar        = NULL;
      value->is_null             = 1;
      value->length              = 0;
      value->inited              = 1;
      mb->buffer_type            = taos_type;
      mb->buffer                 = NULL;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      value->tsdb_nchar          = NULL;
      value->is_null             = 1;
      value->length              = 0;
      value->inited              = 1;
      mb->buffer_type            = taos_type;
      mb->buffer                 = NULL;
      break;
    case TSDB_DATA_TYPE_INT:
      value->tsdb_int            = 0;
      value->is_null             = 1;
      value->length              = 0;
      value->inited              = 1;
      mb->buffer_type            = taos_type;
      mb->buffer                 = NULL;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "converstion to `%s[%d]` from `SQL_C_NULL` not implemented yet",
          taos_data_type(taos_type), taos_type);
      return SQL_ERROR;
  }

  return _stmt_bind_param_tsdb(stmt, i_param);
}

static SQLRETURN _stmt_bind_param_with_sql_c_slong(stmt_t *stmt, int i_param)
{
  descriptor_t *APD = stmt_APD(stmt);
  descriptor_t *IPD = stmt_IPD(stmt);

  desc_record_t *IPD_record = IPD->records + i_param;
  desc_record_t *APD_record = APD->records + i_param;

  SQLPOINTER    data_base = APD_record->DESC_DATA_PTR;

  int val = *(int*)data_base;

  SQLSMALLINT ParameterType = IPD_record->DESC_TYPE;
  switch (ParameterType) {
    case SQL_INTEGER:
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "parameter type of `%s[%d]` not implemented yet",
          sql_data_type(ParameterType), ParameterType);
      return SQL_ERROR;
  }

  TAOS_MULTI_BIND *mb        = stmt->params.mbs    + i_param;
  param_value_t   *value     = stmt->params.values + i_param;

  int taos_type = IPD_record->taos_type;
  switch (taos_type) {
    case TSDB_DATA_TYPE_INT:
      value->tsdb_int            = val;
      value->is_null             = 0;
      value->length              = sizeof(value->tsdb_int);
      value->inited              = 1;
      mb->buffer_type            = taos_type;
      mb->buffer                 = &value->tsdb_int;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "converstion to `%s[%d]` from `SQL_C_SLONG` not implemented yet",
          taos_data_type(taos_type), taos_type);
      return SQL_ERROR;
  }

  return _stmt_bind_param_tsdb(stmt, i_param);
}

static SQLRETURN _stmt_bind_param_with_sql_c_sbigint(stmt_t *stmt, int i_param)
{
  descriptor_t *APD = stmt_APD(stmt);
  descriptor_t *IPD = stmt_IPD(stmt);

  desc_record_t *IPD_record = IPD->records + i_param;
  desc_record_t *APD_record = APD->records + i_param;

  SQLPOINTER    data_base = APD_record->DESC_DATA_PTR;
  SQLLEN       *len_base  = APD_record->DESC_OCTET_LENGTH_PTR;
  (void)data_base;
  (void)len_base;

  int64_t val = *(int64_t*)data_base;

  SQLSMALLINT ParameterType = IPD_record->DESC_TYPE;
  switch (ParameterType) {
    case SQL_DATETIME:
      if (IPD_record->DESC_CONCISE_TYPE != SQL_TYPE_TIMESTAMP) {
        stmt_append_err_format(stmt, "HY000", 0,
            "parameter type of `%s[%d]` not implemented yet",
            sql_data_type(IPD_record->DESC_CONCISE_TYPE), IPD_record->DESC_CONCISE_TYPE);
        return SQL_ERROR;
      }
      break;
    case SQL_INTEGER:
      {
        int32_t v = val;
        if (v != val) {
          stmt_append_err_format(stmt, "HY000", 0,
              "parameter `%s[%d]` would be loss of precision",
              sql_data_type(IPD_record->DESC_CONCISE_TYPE), IPD_record->DESC_CONCISE_TYPE);
          return SQL_ERROR;
        }
      } break;
    case SQL_BIGINT:
      break;
    case SQL_VARCHAR:
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "parameter type of `%s[%d]` not implemented yet",
          sql_data_type(ParameterType), ParameterType);
      return SQL_ERROR;
  }

  TAOS_MULTI_BIND *mb        = stmt->params.mbs    + i_param;
  param_value_t   *value     = stmt->params.values + i_param;

  char buf[64];
  int n;

  int taos_type = IPD_record->taos_type;
  switch (taos_type) {
    case TSDB_DATA_TYPE_INT:
      value->tsdb_int            = val;
      value->is_null             = 0;
      value->length              = sizeof(value->tsdb_int);
      value->inited              = 1;
      mb->buffer_type            = taos_type;
      mb->buffer                 = &value->tsdb_int;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      value->tsdb_bigint         = val;
      value->is_null             = 0;
      value->length              = sizeof(value->tsdb_bigint);
      value->inited              = 1;
      mb->buffer_type            = taos_type;
      mb->buffer                 = &value->tsdb_bigint;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      value->tsdb_timestamp      = val;
      value->is_null             = 0;
      value->length              = sizeof(value->tsdb_timestamp);
      value->inited              = 1;
      mb->buffer_type            = taos_type;
      mb->buffer                 = &value->tsdb_timestamp;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      n = snprintf(buf, sizeof(buf), "%ld", val);
      OA_NIY(n >= 0 && (size_t)n < sizeof(buf));
      if (value->allocated) TOD_SAFE_FREE(value->ptr);
      value->ptr = strdup(buf);
      if (!value->ptr) {
        _stmt_malloc_fail(stmt);
        return SQL_ERROR;
      }
      value->allocated = 1;
      // check if taosc support data-conversion
      value->is_null             = 0;
      value->length              = n;
      value->inited              = 1;
      mb->buffer_type            = taos_type;
      mb->buffer                 = value->ptr;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "converstion to `%s[%d]` from `SQL_C_SBIGINT` not implemented yet",
          taos_data_type(taos_type), taos_type);
      return SQL_ERROR;
  }

  return _stmt_bind_param_tsdb(stmt, i_param);
}

static SQLRETURN _stmt_bind_param_with_sql_c_double(stmt_t *stmt, int i_param)
{
  descriptor_t *APD = stmt_APD(stmt);
  descriptor_t *IPD = stmt_IPD(stmt);

  desc_record_t *IPD_record = IPD->records + i_param;
  desc_record_t *APD_record = APD->records + i_param;

  SQLPOINTER    data_base = APD_record->DESC_DATA_PTR;
  SQLLEN       *len_base  = APD_record->DESC_OCTET_LENGTH_PTR;
  (void)data_base;
  (void)len_base;

  double val = *(double*)data_base;

  SQLSMALLINT ParameterType = IPD_record->DESC_TYPE;
  switch (ParameterType) {
    case SQL_VARCHAR:
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "parameter type of `%s[%d]` not implemented yet",
          sql_data_type(ParameterType), ParameterType);
      return SQL_ERROR;
  }

  TAOS_MULTI_BIND *mb        = stmt->params.mbs    + i_param;
  param_value_t   *value     = stmt->params.values + i_param;

  char buf[64];
  int n;

  int taos_type = IPD_record->taos_type;
  switch (taos_type) {
    case TSDB_DATA_TYPE_VARCHAR:
      n = snprintf(buf, sizeof(buf), "%g", val);
      OA_NIY(n >= 0 && (size_t)n < sizeof(buf));
      if (value->allocated) TOD_SAFE_FREE(value->ptr);
      value->ptr = strdup(buf);
      if (!value->ptr) {
        _stmt_malloc_fail(stmt);
        return SQL_ERROR;
      }
      value->allocated = 1;
      // check if taosc support data-conversion
      value->is_null             = 0;
      value->length              = n;
      value->inited              = 1;
      mb->buffer_type            = taos_type;
      mb->buffer                 = value->ptr;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "converstion to `%s[%d]` from `SQL_C_DOUBLE` not implemented yet",
          taos_data_type(taos_type), taos_type);
      return SQL_ERROR;
  }

  return _stmt_bind_param_tsdb(stmt, i_param);
}

static SQLRETURN _stmt_bind_param_with_sql_c_char(stmt_t *stmt, int i_param)
{
  descriptor_t *APD = stmt_APD(stmt);
  descriptor_t *IPD = stmt_IPD(stmt);

  desc_record_t *IPD_record = IPD->records + i_param;
  desc_record_t *APD_record = APD->records + i_param;

  SQLPOINTER    data_base = APD_record->DESC_DATA_PTR;
  SQLLEN       *len_base  = APD_record->DESC_OCTET_LENGTH_PTR;
  OA_NIY(data_base);
  if (*len_base == SQL_NTS) *len_base = strlen((const char*)data_base);
  const char *s = (const char*)data_base;
  int         n = *len_base;

  SQLSMALLINT ParameterType = IPD_record->DESC_TYPE;
  switch (ParameterType) {
    case SQL_VARCHAR:
      if ((SQLULEN)*len_base > IPD_record->DESC_LENGTH) {
        stmt_append_err_format(stmt, "HY000", 0,
            "parameter of `%s[%d]` truncated",
            sql_data_type(ParameterType), ParameterType);
        return SQL_ERROR;
      }
      break;
    case SQL_WVARCHAR:
      {
        size_t wchars = 0;
        SQLRETURN sr = _stmt_calc_bytes(stmt, "utf8", s, n, "ucs2", &wchars);
        wchars /= 2;
        OA(sql_succeeded(sr), "wchars: %ld", wchars);
        if (wchars > IPD_record->DESC_LENGTH) {
          stmt_append_err_format(stmt, "HY000", 0,
              "parameter of `%s[%d]` truncated, %ld/%ld",
              sql_data_type(ParameterType), ParameterType, wchars, IPD_record->DESC_LENGTH);
          return SQL_ERROR;
        }
      }
      break;
    case SQL_DATETIME:
      if (IPD_record->DESC_CONCISE_TYPE != SQL_TYPE_TIMESTAMP) {
        stmt_append_err_format(stmt, "HY000", 0,
            "parameter type of `%s[%d]` not implemented yet",
            sql_data_type(IPD_record->DESC_CONCISE_TYPE), IPD_record->DESC_CONCISE_TYPE);
        return SQL_ERROR;
      }
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "parameter type of `%s[%d]` not implemented yet",
          sql_data_type(ParameterType), ParameterType);
      return SQL_ERROR;
  }

  SQLRETURN sr = SQL_SUCCESS;

  TAOS_MULTI_BIND *mb        = stmt->params.mbs    + i_param;
  param_value_t   *value     = stmt->params.values + i_param;

  int taos_type = IPD_record->taos_type;
  switch (taos_type) {
    case TSDB_DATA_TYPE_VARCHAR:
      value->tsdb_varchar        = s;
      value->is_null             = 0;
      value->length              = n;
      value->inited              = 1;
      mb->buffer_type            = taos_type;
      mb->buffer                 = (void*)value->tsdb_varchar;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      value->tsdb_nchar          = s;
      value->is_null             = 0;
      value->length              = n;
      value->inited              = 1;
      mb->buffer_type            = taos_type;
      mb->buffer                 = (void*)value->tsdb_varchar;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      sr = _stmt_sql_c_char_to_tsdb_timestamp(stmt, s, &value->tsdb_timestamp);
      if (!sql_succeeded(sr)) return SQL_ERROR;
      value->is_null             = 0;
      value->length              = n;
      value->inited              = 1;
      mb->buffer_type            = taos_type;
      mb->buffer                 = &value->tsdb_timestamp;
      break;
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "converstion to `%s[%d]` from `SQL_C_CHAR` not implemented yet",
          taos_data_type(taos_type), taos_type);
      return SQL_ERROR;
  }

  return _stmt_bind_param_tsdb(stmt, i_param);
}

static SQLRETURN _stmt_bind_param(stmt_t *stmt, int i_param)
{
  descriptor_t *APD = stmt_APD(stmt);

  desc_record_t *APD_record = APD->records + i_param;

  SQLLEN       *ind_base  = APD_record->DESC_INDICATOR_PTR;

  if (ind_base && *ind_base == SQL_NULL_DATA) {
    return _stmt_bind_param_with_sql_c_null(stmt, i_param);
    return SQL_SUCCESS;
  }

  SQLSMALLINT ValueType = APD_record->DESC_TYPE;
  switch (ValueType) {
    case SQL_C_SLONG:
      return _stmt_bind_param_with_sql_c_slong(stmt, i_param);
    case SQL_C_SBIGINT:
      return _stmt_bind_param_with_sql_c_sbigint(stmt, i_param);
    case SQL_C_DOUBLE:
      return _stmt_bind_param_with_sql_c_double(stmt, i_param);
    case SQL_C_CHAR:
      return _stmt_bind_param_with_sql_c_char(stmt, i_param);
    default:
      stmt_append_err_format(stmt, "HY000", 0,
          "value type of `%s[%d]` not implemented yet",
          sql_c_data_type(ValueType), ValueType);
      return SQL_ERROR;
  }
}

static SQLRETURN _stmt_pre_exec_prepare_params(stmt_t *stmt)
{
  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;
  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  if (APD_header->DESC_COUNT != IPD_header->DESC_COUNT) {
    stmt_append_err(stmt, "HY000", 0, "internal logic error, DESC_COUNT of APD/IPD differs");
    OA(0, "");
    return SQL_ERROR;
  }

  if (APD_header->DESC_COUNT > 0) {
    if (params_realloc(&stmt->params, APD_header->DESC_COUNT)) {
      _stmt_malloc_fail(stmt);
      return SQL_ERROR;
    }
  }

  for (int i_param = 0; i_param < IPD_header->DESC_COUNT; ++i_param) {
    SQLRETURN sr = _stmt_bind_param(stmt, i_param);
    if (!sql_succeeded(sr)) return SQL_ERROR;
  }

  int r = TAOS_stmt_add_batch(stmt->stmt);
  if (r) {
    stmt_append_err(stmt, "HY000", r, TAOS_stmt_errstr(stmt->stmt));
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _stmt_pre_exec(stmt_t *stmt)
{
  SQLRETURN sr = _stmt_pre_exec_prepare_params(stmt);
  if (!sql_succeeded(sr)) return SQL_ERROR;

  return SQL_SUCCESS;
}

SQLRETURN stmt_execute(
    stmt_t         *stmt)
{
  OA_ILE(stmt);
  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->taos);
  OA_ILE(stmt->stmt);

  if (stmt_get_rows_fetched_ptr(stmt)) *stmt_get_rows_fetched_ptr(stmt) = 0;
  rowset_reset(&stmt->rowset);
  // column-binds remain valid among executes
  stmt_release_result(stmt);

  SQLRETURN sr = _stmt_pre_exec(stmt);
  if (!sql_succeeded(sr)) return sr;

  int r = 0;
  r = TAOS_stmt_execute(stmt->stmt);
  if (r) {
    stmt_append_err(stmt, "HY000", r, TAOS_stmt_errstr(stmt->stmt));
    return SQL_ERROR;
  }
  stmt->res = TAOS_stmt_use_result(stmt->stmt);
  stmt->res_is_from_taos_query = 0;

  return _stmt_post_exec(stmt);
}

SQLRETURN stmt_unbind_cols(
    stmt_t         *stmt)
{
  descriptor_t *desc = stmt_ARD(stmt);
  desc_header_t *header = &desc->header;
  header->DESC_COUNT = 0;
  memset(desc->records, 0, sizeof(*desc->records) * desc->cap);
  return SQL_SUCCESS;
}

SQLRETURN stmt_reset_params(
    stmt_t         *stmt)
{
  params_reset(&stmt->params);

  descriptor_t *APD = stmt_APD(stmt);
  desc_header_t *APD_header = &APD->header;
  APD_header->DESC_COUNT = 0;

  descriptor_t *IPD = stmt_IPD(stmt);
  desc_header_t *IPD_header = &IPD->header;
  IPD_header->DESC_COUNT = 0;

  return SQL_SUCCESS;
}

