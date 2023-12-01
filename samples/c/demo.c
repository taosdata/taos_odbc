#include "odbc_helpers.h"

#include <stdio.h>
#include <string.h>


#define DUMP(fmt, ...) fprintf(stderr, "[%d]:%s():" fmt "\n", __LINE__, __func__, ##__VA_ARGS__)

#ifdef _WIN32        /* { */
#endif               /* } */

static void usage(const char *app)
{
  DUMP("%s [options] <actiion> <action-options>", app);
  DUMP("  options:");
  DUMP("    --dsn <dsn>");
  DUMP("    --conn_str <conn_str>");
  DUMP("    --user <user>");
  DUMP("    --pass <pass>");
  DUMP("    --");
  DUMP("  actions:");
  DUMP("    conn");
  DUMP("    query");
}

typedef struct handles_s                 handles_t;
struct handles_s {
  SQLHENV            henv;
  SQLHDBC            hdbc;
  SQLHSTMT           hstmt;
};

typedef struct col_bind_s                col_bind_t;
struct col_bind_s {
  SQLCHAR                 name[1024];
  SQLSMALLINT             len;
  SQLLEN                  attr;
  SQLCHAR                 value[1024];
  SQLLEN                  value_ind_len;
};

typedef enum param_type_e                param_type_t;
enum param_type_e {
  param_type_ms,
  param_type_ms_str,
  param_type_str,
  param_type_i32,
};

typedef struct param_bind_s              param_bind_t;
struct param_bind_s {
  size_t          idx;
  param_type_t    param_type;
  SQLSMALLINT     ValueType;
  SQLSMALLINT     ParameterType;
  SQLULEN         ColumnSize;
  SQLSMALLINT     DecimalDigits;
  SQLPOINTER     *ParameterValuePtr;
  SQLLEN          BufferLength;
  SQLLEN         *StrLen_or_IndPtr;
  char           *buf;
  size_t          sz;
  size_t          nr;
};
typedef struct param_binds_s             param_binds_t;
struct param_binds_s {
  param_bind_t        *param_binds;
  size_t               sz;
  size_t               nr;
  SQLUSMALLINT        *ParamStatusPtr;
  SQLULEN              ParamsProcessed;
};

static void param_bind_release(param_bind_t *param_bind)
{
  if (param_bind->buf) {
    free(param_bind->buf);
    param_bind->buf = NULL;
    param_bind->sz  = 0;
    param_bind->nr  = 0;
    param_bind->ParameterValuePtr = NULL;
  }
  if (param_bind->StrLen_or_IndPtr) {
    free(param_bind->StrLen_or_IndPtr);
    param_bind->StrLen_or_IndPtr = NULL;
  }
}

static int bind_param_ms(param_bind_t *param_bind, size_t nr_rows)
{
  size_t sz = sizeof(int64_t);

  param_bind->ValueType              = SQL_C_SBIGINT;
  param_bind->ParameterType          = SQL_BIGINT;
  param_bind->ColumnSize             = sz;
  param_bind->DecimalDigits          = 0;
  param_bind->buf = (char*)malloc(sz * nr_rows);
  if (!param_bind->buf) {
    DUMP("out of memory");
    return -1;
  }
  param_bind->ParameterValuePtr      = (SQLPOINTER*)param_bind->buf;
  param_bind->BufferLength           = sz;
  param_bind->StrLen_or_IndPtr       = (SQLLEN*)malloc(nr_rows * sizeof(*param_bind->StrLen_or_IndPtr));

  return 0;
}

static int bind_param_i32(param_bind_t *param_bind, size_t nr_rows)
{
  size_t sz = sizeof(int32_t);

  param_bind->ValueType              = SQL_C_LONG;
  param_bind->ParameterType          = SQL_INTEGER;
  param_bind->ColumnSize             = sz;
  param_bind->DecimalDigits          = 0;
  param_bind->buf = (char*)malloc(sz * nr_rows);
  if (!param_bind->buf) {
    DUMP("out of memory");
    return -1;
  }
  param_bind->ParameterValuePtr      = (SQLPOINTER*)param_bind->buf;
  param_bind->BufferLength           = sz;
  param_bind->StrLen_or_IndPtr       = (SQLLEN*)malloc(nr_rows * sizeof(*param_bind->StrLen_or_IndPtr));

  return 0;
}

static int bind_param_str(param_bind_t *param_bind, size_t nr_rows)
{
  size_t sz = 64; // TODO: dynamic and configurable

  param_bind->ValueType              = SQL_C_CHAR;
  param_bind->ParameterType          = SQL_VARCHAR;
  param_bind->ColumnSize             = sz;
  param_bind->DecimalDigits          = 0;
  param_bind->buf = (char*)malloc(sz * nr_rows);
  if (!param_bind->buf) {
    DUMP("out of memory");
    return -1;
  }
  param_bind->ParameterValuePtr      = (SQLPOINTER*)param_bind->buf;
  param_bind->BufferLength           = sz;
  param_bind->StrLen_or_IndPtr       = (SQLLEN*)malloc(nr_rows * sizeof(*param_bind->StrLen_or_IndPtr));

  return 0;
}

static int prepare_param_ms(param_bind_t *param_bind, size_t nr_rows)
{
  int64_t i64 = (int64_t)time(0);
  i64 *= 1000;

  int64_t *p = (int64_t*)param_bind->buf;
  SQLLEN *pInd = param_bind->StrLen_or_IndPtr;
  for (size_t i=0; i<nr_rows; ++i) {
    *p = i64;
    *pInd = 0; // NOTE: null
    ++i64;
    ++p;
    ++pInd;
  }

  return 0;
}

static int prepare_param_i32(param_bind_t *param_bind, size_t nr_rows)
{
  int32_t i32 = rand();

  int32_t *p = (int32_t*)param_bind->buf;
  SQLLEN *pInd = param_bind->StrLen_or_IndPtr;
  for (size_t i=0; i<nr_rows; ++i) {
    *p = i32;
    *pInd = 0; // NOTE: null

    i32 = rand();
    ++p;
    ++pInd;
  }

  return 0;
}

static int prepare_param_str(param_bind_t *param_bind, size_t nr_rows)
{
  int32_t i32 = rand();

  char *p = (char*)param_bind->buf;
  SQLLEN *pInd = param_bind->StrLen_or_IndPtr;
  for (size_t i=0; i<nr_rows; ++i) {
    snprintf(p, param_bind->ColumnSize, "%d", i32);
    *pInd = strlen(p);

    i32 = rand();
    p += param_bind->BufferLength;
    ++pInd;
  }

  return 0;
}

static int bind_param(param_bind_t *param_bind, size_t nr_rows)
{
  switch (param_bind->param_type) {
    case param_type_ms:
      return bind_param_ms(param_bind, nr_rows);
    case param_type_i32:
      return bind_param_i32(param_bind, nr_rows);
    case param_type_str:
      return bind_param_str(param_bind, nr_rows);
    default:
      DUMP("unknown param_type:[0x%x]", param_bind->param_type);
      return -1;
  }
}

static int prepare_param(param_bind_t *param_bind, size_t nr_rows)
{
  switch (param_bind->param_type) {
    case param_type_ms:
      return prepare_param_ms(param_bind, nr_rows);
    case param_type_i32:
      return prepare_param_i32(param_bind, nr_rows);
    case param_type_str:
      return prepare_param_str(param_bind, nr_rows);
    default:
      DUMP("unknown param_type:[0x%x]", param_bind->param_type);
      return -1;
  }
}

static void param_binds_release(param_binds_t *param_binds)
{
  if (!param_binds) return;
  for (size_t i=0; i<param_binds->nr; ++i) {
    param_bind_t *param_bind = param_binds->param_binds + i;
    param_bind_release(param_bind);
  }
  if (param_binds->param_binds) {
    free(param_binds->param_binds);
    param_binds->param_binds = NULL;
  }
  param_binds->sz = 0;
  param_binds->nr = 0;
  if (param_binds->ParamStatusPtr) {
    free(param_binds->ParamStatusPtr);
    param_binds->ParamStatusPtr = NULL;
  }
}

static int param_binds_keep(param_binds_t *param_binds, size_t sz)
{
  if (sz < param_binds->sz) return 0;
  sz = (sz + 15) / 16 * 16;
  param_bind_t *p = (param_bind_t*)realloc(param_binds->param_binds, sizeof(*p) * sz);
  if (!p) return -1;
  param_binds->param_binds = p;
  param_binds->sz          = sz;
  return 0;
}

static int param_binds_add_ms(param_binds_t *param_binds)
{
  int r = 0;
  r = param_binds_keep(param_binds, param_binds->nr + 1);
  if (r) return -1;
  param_bind_t *param_bind           = param_binds->param_binds + param_binds->nr;
  param_bind->idx                    = param_binds->nr++;
  param_bind->param_type             = param_type_ms;
  return 0;
}

static int param_binds_add_ms_str(param_binds_t *param_binds)
{
  int r = 0;
  r = param_binds_keep(param_binds, param_binds->nr + 1);
  if (r) return -1;
  param_bind_t *param_bind           = param_binds->param_binds + param_binds->nr;
  param_bind->idx                    = param_binds->nr++;
  param_bind->param_type             = param_type_ms_str;
  param_bind->ValueType              = SQL_C_CHAR;
  param_bind->ParameterType          = SQL_BIGINT;
  param_bind->ColumnSize             = 64; // TODO: dynamic or configable
  param_bind->DecimalDigits          = 0;
  return 0;
}

static int param_binds_add_str(param_binds_t *param_binds)
{
  int r = 0;
  r = param_binds_keep(param_binds, param_binds->nr + 1);
  if (r) return -1;
  param_bind_t *param_bind           = param_binds->param_binds + param_binds->nr;
  param_bind->idx                    = param_binds->nr++;
  param_bind->param_type             = param_type_str;
  return 0;
}

static int param_binds_add_i32(param_binds_t *param_binds)
{
  int r = 0;
  r = param_binds_keep(param_binds, param_binds->nr + 1);
  if (r) return -1;
  param_bind_t *param_bind           = param_binds->param_binds + param_binds->nr;
  param_bind->idx                    = param_binds->nr++;
  param_bind->param_type             = param_type_i32;
  return 0;
}

static int run_query_with_cols(SQLHANDLE hstmt, col_bind_t *cols, SQLSMALLINT nr_cols, int display)
{
  SQLRETURN          sr;

  for (int i=0; i<nr_cols; ++i) {
    col_bind_t *col = cols + i;
    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL, &col->attr);
    if (sr != SQL_SUCCESS) return -1;
    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_NAME, col->name, sizeof(col->name),  &col->len, NULL);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("col[%d]:%s:len[%d]:attr[%" PRId64 "]", i+1, col->name, col->len, col->attr);
    sr = CALL_SQLBindCol(hstmt, i+1, SQL_C_CHAR, col->value, sizeof(col->value), &col->value_ind_len);
    if (sr != SQL_SUCCESS) return -1;
  }

  size_t nr_rows = 0;
  while (sr == SQL_SUCCESS) {
    sr = CALL_SQLFetch(hstmt);
    if (sr == SQL_NO_DATA) return 0;
    if (sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO) {
      if (display) DUMP("row[%zd]:", ++nr_rows);
      for (int i=0; i<nr_cols; ++i) {
        col_bind_t *col = cols + i;
        if (display) {
          if (i) DUMP(",");
          DUMP("%s[%s]", col->name, col->value_ind_len == SQL_NULL_DATA ? "null" : (const char*)col->value);
        }
      }
      if (display) DUMP("");
      sr = SQL_SUCCESS;
      continue;
    }
  }

  return sr == SQL_SUCCESS ? 0 : -1;
}

static int run_query_with_sql(handles_t *handles, const char *query, int display)
{
  SQLRETURN          sr;
  int r = 0;

  if (handles->hstmt == SQL_NULL_HSTMT) {
    sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, handles->hdbc, &handles->hstmt);
    if (sr != SQL_SUCCESS) return -1;
  }

  SQLHANDLE hstmt = handles->hstmt;

  col_bind_t        *cols    = NULL;
  SQLSMALLINT        nr_cols = 0;

  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)query, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  sr = CALL_SQLNumResultCols(hstmt, &nr_cols);
  if (sr != SQL_SUCCESS) return -1;
  if (nr_cols == 0) {
    SQLLEN nr_rows_affected = 0;
    sr = CALL_SQLRowCount(hstmt, &nr_rows_affected);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("%zd row(s) affected", (size_t)nr_rows_affected);
    return 0;
  }

  cols = (col_bind_t*)calloc(nr_cols, sizeof(*cols));
  if (!cols) {
    DUMP("out of memory");
    return -1;
  }

  r = run_query_with_cols(hstmt, cols, nr_cols, display);

  if (cols) {
    free(cols);
    cols = NULL;
  }

  return r;
}
static int run_query(handles_t *handles, int i, int argc, char *argv[])
{
  int r = 0;
  int display = 0;

  for (; i<argc; ++i) {
    const char *arg = argv[i];
    DUMP("arg:%s", arg);
    if (0 == strcasecmp(arg, "--")) break;
    if (0 == strcasecmp(arg, "--display")) {
      display = 1;
      continue;
    }
    r = run_query_with_sql(handles, argv[i], display);
    if (r) return -1;
  }
  return i;
}

static int run_insert_with_options(handles_t *handles, const char *sql, size_t nr_rows, param_binds_t *param_binds)
{
  SQLRETURN          sr;
  int r = 0;

  param_binds->ParamStatusPtr = (SQLUSMALLINT*)calloc(nr_rows, sizeof(*param_binds->ParamStatusPtr));
  if (!param_binds->ParamStatusPtr) {
    DUMP("out of memory");
    return -1;
  }
  param_binds->ParamsProcessed = 0;

  for (size_t i=0; i<param_binds->nr; ++i) {
    param_bind_t *param_bind = param_binds->param_binds + i;
    r = bind_param(param_bind, nr_rows);
    if (r) return -1;
    r = prepare_param(param_bind, nr_rows);
    if (r) return -1;
  }

  if (handles->hstmt == SQL_NULL_HSTMT) {
    sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, handles->hdbc, &handles->hstmt);
    if (sr != SQL_SUCCESS) return -1;
  }

  // Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
  // column-wise binding.
  sr = CALL_SQLSetStmtAttr(handles->hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);
  if (sr != SQL_SUCCESS) return -1;

  // Specify the number of elements in each parameter array.
  sr = CALL_SQLSetStmtAttr(handles->hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)(uintptr_t)nr_rows, 0);
  if (sr != SQL_SUCCESS) return -1;

  // Specify an array in which to return the status of each set of
  // parameters.
  sr = CALL_SQLSetStmtAttr(handles->hstmt, SQL_ATTR_PARAM_STATUS_PTR, param_binds->ParamStatusPtr, 0);
  if (sr != SQL_SUCCESS) return -1;

  // Specify an SQLUINTEGER value in which to return the number of sets of
  // parameters processed.
  sr = CALL_SQLSetStmtAttr(handles->hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &param_binds->ParamsProcessed, 0);
  if (sr != SQL_SUCCESS) return -1;

  SQLHANDLE hstmt = handles->hstmt;
  sr = CALL_SQLPrepare(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  SQLSMALLINT nr_params = 0;
  sr = CALL_SQLNumParams(hstmt, &nr_params);
  if (sr != SQL_SUCCESS) return -1;

  for (SQLSMALLINT i=0; i<nr_params; ++i) {
    SQLSMALLINT     DataType;
    SQLULEN         ParameterSize;
    SQLSMALLINT     DecimalDigits;
    SQLSMALLINT     Nullable;
    sr = CALL_SQLDescribeParam(hstmt, i+1, &DataType, &ParameterSize, &DecimalDigits, &Nullable);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("DataType:[0x%x]%s;ParameterSize:%" PRIu64 ";DecimalDigits:%d;Nullable:%d",
        DataType, sql_data_type(DataType), ParameterSize, DecimalDigits, Nullable);
  }

  for (size_t i=0; i<param_binds->nr; ++i) {
    param_bind_t *param_bind = param_binds->param_binds + i;
    sr = CALL_SQLBindParameter(
        handles->hstmt,
        param_bind->idx+1,
        SQL_PARAM_INPUT,
        param_bind->ValueType,
        param_bind->ParameterType,
        param_bind->ColumnSize,
        param_bind->DecimalDigits,
        param_bind->ParameterValuePtr,
        param_bind->BufferLength,
        param_bind->StrLen_or_IndPtr);
    if (sr != SQL_SUCCESS) return -1;
  }

  sr = CALL_SQLExecute(handles->hstmt);
  if (sr != SQL_SUCCESS) return -1;

  DUMP("processed:%lu", param_binds->ParamsProcessed);

  return 0;
}

static int run_insert_with_param_binds(handles_t *handles, param_binds_t *param_binds, int i, int argc, char *argv[])
{
  int r = 0;

  size_t          nr_rows = 0;
  const char     *sql     = NULL;

  for (; i<argc; ++i) {
    const char *arg = argv[i];
    DUMP("arg:%s", arg);
    if (0 == strcasecmp(arg, "--")) break;
    if (0 == strcasecmp(arg, "--rows")) {
      if (++i >= argc) {
        DUMP("<rows> is expected after --rows");
        return -1;
      }
      nr_rows = strtoll(argv[i], NULL, 0); // TODO: error check
      continue;
    }
    if (0 == strcasecmp(arg, "--sql")) {
      if (++i >= argc) {
        DUMP("<sql> is expected after --sql");
        return -1;
      }
      sql = argv[i];
      continue;
    }
    if (0 == strcasecmp(arg, "--params")) {
      for (++i; i<argc; ++i) {
        arg = argv[i];
        if (0 == strcasecmp(arg, "--")) {
          --i;
          break;
        }
        if (0 == strcasecmp(arg, "ms")) {
          r = param_binds_add_ms(param_binds);
          if (r) return -1;
          continue;
        }
        if (0 == strcasecmp(arg, "ms_str")) {
          r = param_binds_add_ms_str(param_binds);
          if (r) return -1;
          continue;
        }
        if (0 == strcasecmp(arg, "str")) {
          r = param_binds_add_str(param_binds);
          if (r) return -1;
          continue;
        }
        if (0 == strcasecmp(arg, "i32")) {
          r = param_binds_add_i32(param_binds);
          if (r) return -1;
          continue;
        }
        DUMP("unknown argument:%s", arg);
        return -1;
      }
      continue;
    }
    DUMP("unknown argument:%s", arg);
    return -1;
  }

  r = run_insert_with_options(handles, sql, nr_rows, param_binds);
  if (r) return -1;

  return i;
}

static int run_insert(handles_t *handles, int i, int argc, char *argv[])
{
  param_binds_t   param_binds = {0};
  i = run_insert_with_param_binds(handles, &param_binds, i, argc, argv);
  param_binds_release(&param_binds);

  return i;
}

static int run_cmd(handles_t *handles, const char *cmd)
{
  SQLRETURN          sr;
  int r = 0;

  if (handles->hstmt == SQL_NULL_HSTMT) {
    sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, handles->hdbc, &handles->hstmt);
    if (sr != SQL_SUCCESS) return -1;
  }

  SQLHANDLE hstmt = handles->hstmt;

  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)cmd, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  return r;
}

static void handles_release(handles_t *handles)
{
  if (handles->hstmt != SQL_NULL_HSTMT) {
    CALL_SQLFreeHandle(SQL_HANDLE_STMT, handles->hstmt);
    handles->hstmt = SQL_NULL_HSTMT;
  }

  if (handles->hdbc != SQL_NULL_HDBC) {
    CALL_SQLDisconnect(handles->hdbc);
    CALL_SQLFreeHandle(SQL_HANDLE_DBC, handles->hdbc);
    handles->hdbc = SQL_NULL_HDBC;
  }

  if (handles->henv != SQL_NULL_HENV) {
    CALL_SQLFreeHandle(SQL_HANDLE_ENV, handles->henv);
    handles->henv = SQL_NULL_HENV;
  }
}

static void handles_disconnect(handles_t *handles)
{
  if (handles->hstmt != SQL_NULL_HSTMT) {
    CALL_SQLFreeHandle(SQL_HANDLE_STMT, handles->hstmt);
    handles->hstmt = SQL_NULL_HSTMT;
  }

  if (handles->hdbc != SQL_NULL_HDBC) {
    CALL_SQLDisconnect(handles->hdbc);
  }
}

static int handles_connect(handles_t *handles, const char *dsn, const char *user, const char *pass, const char *conn_str)
{
  SQLRETURN          sr;

  if (!dsn && !conn_str) {
    DUMP("either --dsn or --conn_str is required");
    return -1;
  }

  handles_disconnect(handles);
  if (handles->henv == SQL_NULL_HANDLE) {
    sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &handles->henv);
    if (sr != SQL_SUCCESS) return -1;
    sr = CALL_SQLSetEnvAttr(handles->henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER*)SQL_OV_ODBC3, 0);
    if (sr != SQL_SUCCESS) return -1;
  }

  if (handles->hdbc == SQL_NULL_HANDLE) {
    sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, handles->henv, &handles->hdbc);
    if (sr != SQL_SUCCESS) return -1;
  }

  sr = CALL_SQLSetConnectAttr(handles->hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)0, 0);
  if (sr != SQL_SUCCESS) return -1;

  if (dsn) sr = CALL_SQLConnect(handles->hdbc, (SQLCHAR*)dsn, SQL_NTS, (SQLCHAR*)user, SQL_NTS, (SQLCHAR*)pass, SQL_NTS);
  else     sr = CALL_SQLDriverConnect(handles->hdbc, NULL, (SQLCHAR*)conn_str, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
  if (sr != SQL_SUCCESS) return -1;

  return 0;
}

static int process(handles_t *handles, int argc, char *argv[])
{
  int r = 0;
  const char *dsn = NULL;
  const char *conn_str = NULL;
  const char *user = NULL;
  const char *pass = NULL;
  const char *app   = argv[0];
  int conn_changed = 1;
  for (int i=1; i<argc; ++i) {
    const char *arg = argv[i];
    DUMP("arg:%s", arg);
    if (0 == strcasecmp(arg, "-h")) {
      usage(app);
      return 0;
    }
    if (0 == strcasecmp(arg, "--")) continue;
    if (0 == strcasecmp(arg, "--dsn")) {
      if (++i >= argc) {
        DUMP("<dsn> is expected after --dsn");
        return -1;
      }
      conn_changed = 1;
      dsn = argv[i];
      conn_str = NULL;
      continue;
    }
    if (0 == strcasecmp(arg, "--conn_str")) {
      if (++i >= argc) {
        DUMP("<conn_str> is expected after --conn_str");
        return -1;
      }
      conn_changed = 1;
      conn_str = argv[i];
      dsn = NULL;
      user = NULL;
      pass = NULL;
      continue;
    }
    if (0 == strcasecmp(arg, "--user")) {
      if (++i >= argc) {
        DUMP("<user> is expected after --user");
        return -1;
      }
      conn_changed = 1;
      user = argv[i];
      continue;
    }
    if (0 == strcasecmp(arg, "--pass")) {
      if (++i >= argc) {
        DUMP("<pass> is expected after --pass");
        return -1;
      }
      conn_changed = 1;
      pass = argv[i];
      continue;
    }
    if (0 == strcasecmp(arg, "conn")) {
      r = handles_connect(handles, dsn, user, pass, conn_str);
      if (r) return -1;
      conn_changed = 0;
      continue;
    }
    if (0 == strcasecmp(arg, "cmd")) {
      if (++i >= argc) {
        DUMP("<query> is expected after query");
        return -1;
      }
      if (conn_changed) {
        r = handles_connect(handles, dsn, user, pass, conn_str);
        if (r) return -1;
        conn_changed = 0;
      }
      for (; i<argc; ++i) {
        if (0 == strcasecmp(argv[i], "--")) break;
        r = run_cmd(handles, argv[i]);
        if (r) return -1;
      }
      continue;
    }
    if (0 == strcasecmp(arg, "query")) {
      if (++i >= argc) {
        DUMP("<query> is expected after query");
        return -1;
      }
      if (conn_changed) {
        r = handles_connect(handles, dsn, user, pass, conn_str);
        if (r) return -1;
        conn_changed = 0;
      }
      i = run_query(handles, i, argc, argv);
      if (i < 0) return -1;
      continue;
    }
    if (0 == strcasecmp(arg, "insert")) {
      if (conn_changed) {
        r = handles_connect(handles, dsn, user, pass, conn_str);
        if (r) return -1;
        conn_changed = 0;
      }
      i = run_insert(handles, i+1, argc, argv);
      if (i < 0) return -1;
      continue;
    }
    DUMP("unknown argument:%s", arg);
    return -1;
  }

  return 0;
}

int main(int argc, char *argv[])
{
  handles_t handles = {
    .henv    = SQL_NULL_HENV,
    .hdbc    = SQL_NULL_HDBC,
    .hstmt   = SQL_NULL_HSTMT,
  };

  srand(time(0));

  int r = process(&handles, argc, argv);

  handles_release(&handles);

  if (r == 0) DUMP("-=Done=-");

  return !!r;
}

