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

#include "os_port.h"
#include "conn.h"
#include "desc.h"
#include "enums.h"
#include "env.h"
#include "errs.h"
#include "log.h"
#include "stmt.h"
#include "tls.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sql.h>
#include <sqlext.h>
#include <sqlucode.h>

#ifdef _WIN32
#include <odbcinst.h>
#else
#include <syslog.h>
#endif

// NOTE: if you wanna debug in detail, just define DEBUG_OOW to 1
// NOTE: this is performance-hit, please take serious consideration in advance!!!
#define DEBUG_OOW      0
#define OOW(fmt, ...) do {        \
  if (DEBUG_OOW) {                \
    OW(fmt, ##__VA_ARGS__);       \
  }                               \
} while (0)

static atomic_int    _nr_load               = 0;
static unsigned int  _taos_odbc_debug       = 0;
static unsigned int  _taos_odbc_debug_flex  = 0;
static unsigned int  _taos_odbc_debug_bison = 0;

static int check_env_bool(const char *name)
{
  const char *val = tod_getenv(name);
  if (!val) return 0;
  if (!*val) return 1;
  if (tod_strcasecmp(val, "true") == 0) return 1;
  if (tod_strcasecmp(val, "on") == 0) return 1;
  return atoi(val);
}

static void _init_once(void)
{
  _taos_odbc_debug       = check_env_bool("TAOS_ODBC_DEBUG");
  _taos_odbc_debug_flex  = check_env_bool("TAOS_ODBC_DEBUG_FLEX");
  _taos_odbc_debug_bison = check_env_bool("TAOS_ODBC_DEBUG_BISON");
}

int tod_get_debug(void)
{
  return !!_taos_odbc_debug;
}

int tod_get_debug_flex(void)
{
  return !!_taos_odbc_debug_flex;
}

int tod_get_debug_bison(void)
{
  return !!_taos_odbc_debug_bison;
}

static void odbc_log(const char *log)
{
#ifdef _WIN32
  const char *temp = getenv("TEMP");
  if (!temp) temp = getenv("TMP");
  if (!temp) return;

  char fn[MAX_PATH+1]; fn[0] = '\0';
  snprintf(fn, sizeof(fn), "%s\\taos_odbc.log", temp);

  FILE *f = fopen(fn, "a");
  if (!f) return;
  fputs(log, f);
  fclose(f);
#else
  syslog(LOG_DEBUG, "%s", log);
#endif
}

void tod_log(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);

  if (tod_get_debug()) {
    va_list aq;
    va_copy(aq, ap);
    vfprintf(stderr, fmt, aq);
    va_end(aq);
  }

  if (DEBUG_OOW) {
    char buf[1024]; buf[0] = '\0';
    vsnprintf(buf, sizeof(buf), fmt, ap);
    odbc_log(buf);
  }

  va_end(ap);
}

int get_nr_load(void)
{
  return atomic_load(&_nr_load);
}

static void init_global(void)
{
  static int                     inited = 0;
  if (!inited) {
    static pthread_once_t        once;
    pthread_once(&once, _init_once);
    inited = 1;
  }
}

#ifdef _WIN32                  /* { */
static DWORD tls_idx;
tls_t* tls_get(void)
{
  tls_t *tls = TlsGetValue(tls_idx);
  return tls;
}

BOOL WINAPI DllMain(
    HINSTANCE hinstDLL, // handle to DLL module
    DWORD fdwReason,    // reason for calling function
    LPVOID lpvReserved) // reserved
{
  init_global();

  tls_t *tls;

  // Perform actions based on the reason for calling.
  switch (fdwReason)
  {
  case DLL_PROCESS_ATTACH:
    // Initialize once for each new process.
    // Return FALSE to fail DLL load.
    if ((tls_idx = TlsAlloc()) == TLS_OUT_OF_INDEXES) {
      return FALSE;
    }
    tls = (tls_t*)LocalAlloc(LPTR, tls_size());
    if (tls != NULL) TlsSetValue(tls_idx, tls);

    atomic_fetch_add(&_nr_load, 1);
    break;

  case DLL_THREAD_ATTACH:
    // Do thread-specific initialization.
    tls = tls_get();
    if (tls) break;
    tls = (tls_t*)LocalAlloc(LPTR, tls_size());
    if (tls != NULL) TlsSetValue(tls_idx, tls);
    break;

  case DLL_THREAD_DETACH:
    // Do thread-specific cleanup.
    tls = tls_get();
    if (tls != NULL) {
      tls_release(tls);
      LocalFree((HLOCAL)tls);
    }
    break;

  case DLL_PROCESS_DETACH:
    atomic_fetch_sub(&_nr_load, 1);

    tls = tls_get();
    if (tls != NULL) {
      tls_release(tls);
      LocalFree((HLOCAL)tls);
    }

    TlsFree(tls_idx); 

    if (lpvReserved != NULL)
    {
      break; // do not do cleanup if process termination scenario
    }

    // Perform any necessary cleanup.
    break;
  }
  return TRUE; // Successful DLL_PROCESS_ATTACH.
}
#else                          /* }{ */
static pthread_once_t key_once = PTHREAD_ONCE_INIT;
static pthread_key_t tls_key;

static void _tls_destroy(void *val)
{
  tls_t *tls = (tls_t*)val;
  tls_release(tls);
  free(tls);
}

static void _tls_init(void)
{
  int e = pthread_key_create(&tls_key, _tls_destroy);
  if (e) {
    fprintf(stderr, "pthread_key_create failed: [%d]%s\n", e, strerror(e));
    abort();
  }
}

tls_t* tls_get(void)
{
  pthread_once(&key_once, _tls_init);

  tls_t *tls = (tls_t*)pthread_getspecific(tls_key);
  if (tls) return tls;

  tls = (tls_t*)calloc(1, tls_size());
  if (!tls) {
    fprintf(stderr, "out of memory\n");
    abort();
  }
  int e = pthread_setspecific(tls_key, tls);
  if (e) {
    free(tls);
    fprintf(stderr, "pthread_setspecific failed: [%d]%s\n", e, strerror(e));
    abort();
  }

  return tls;
}

__attribute__((constructor)) void _initialize(void)
{
  init_global();
  // yes, no check return value
  atomic_fetch_add(&_nr_load, 1);
}

__attribute__((destructor)) void _deinitialize(void)
{
  // yes, no check return value
  atomic_fetch_sub(&_nr_load, 1);
}
#endif                         /* } */

static SQLRETURN do_alloc_env(
    SQLHANDLE *OutputHandle)
{
  if (!OutputHandle) return SQL_INVALID_HANDLE;

  *OutputHandle = SQL_NULL_HANDLE;

  env_t *env = env_create();
  if (!env) return SQL_ERROR;

  *OutputHandle = (SQLHANDLE)env;
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLAllocHandle(
    SQLSMALLINT HandleType,
    SQLHANDLE   InputHandle,
    SQLHANDLE  *OutputHandle)
{
  OOW("===");
  env_t  *env;
  conn_t *conn;

  switch (HandleType) {
    case SQL_HANDLE_ENV:
      return do_alloc_env(OutputHandle);
    case SQL_HANDLE_DBC:
      if (InputHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;
      if (!OutputHandle)                  return SQL_INVALID_HANDLE;

      env = (env_t*)InputHandle;
      env_clr_errs(env);
      return env_alloc_conn(env, OutputHandle);
    case SQL_HANDLE_STMT:
      if (InputHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;
      if (!OutputHandle)                  return SQL_INVALID_HANDLE;

      conn = (conn_t*)InputHandle;
      conn_clr_errs(conn);
      return conn_alloc_stmt(conn, OutputHandle);
    case SQL_HANDLE_DESC:
      if (InputHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;
      if (!OutputHandle)                  return SQL_INVALID_HANDLE;

      conn = (conn_t*)InputHandle;
      conn_clr_errs(conn);
      return conn_alloc_desc(conn, OutputHandle);
    default:
      if (InputHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

      OA_NIY(0);
      return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLFreeHandle(
    SQLSMALLINT HandleType,
    SQLHANDLE   Handle)
{
  OOW("===");
  if (Handle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  switch (HandleType) {
    case SQL_HANDLE_ENV:
      env_clr_errs((env_t*)Handle);
      return env_free((env_t*)Handle);
    case SQL_HANDLE_DBC:
      conn_clr_errs((conn_t*)Handle);
      return conn_free((conn_t*)Handle);
    case SQL_HANDLE_STMT:
      stmt_clr_errs((stmt_t*)Handle);
      return stmt_free((stmt_t*)Handle);
    case SQL_HANDLE_DESC:
      return desc_free((desc_t*)Handle);
    default:
      OA_NIY(0);
      return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLDriverConnect(
    SQLHDBC         ConnectionHandle,
    SQLHWND         WindowHandle,
    SQLCHAR        *InConnectionString,
    SQLSMALLINT     StringLength1,
    SQLCHAR        *OutConnectionString,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLength2Ptr,
    SQLUSMALLINT    DriverCompletion)
{
  OOW("===");
  if (ConnectionHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  conn_t *conn = (conn_t*)ConnectionHandle;

  conn_clr_errs(conn);

  return conn_driver_connect(conn, WindowHandle, InConnectionString, StringLength1, OutConnectionString, BufferLength, StringLength2Ptr, DriverCompletion);
}

SQLRETURN SQL_API SQLDisconnect(
    SQLHDBC ConnectionHandle)
{
  OOW("===");
  if (ConnectionHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  conn_t *conn = (conn_t*)ConnectionHandle;

  conn_clr_errs(conn);

  conn_disconnect(conn);

  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLExecDirect(
    SQLHSTMT     StatementHandle,
    SQLCHAR     *StatementText,
    SQLINTEGER   TextLength)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_exec_direct(stmt, (const char*)StatementText, TextLength);
}

SQLRETURN SQL_API SQLSetEnvAttr(
    SQLHENV      EnvironmentHandle,
    SQLINTEGER   Attribute,
    SQLPOINTER   ValuePtr,
    SQLINTEGER   StringLength)
{
  OOW("===");
  if (EnvironmentHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  env_t *env = (env_t*)EnvironmentHandle;

  env_clr_errs(env);

  return env_set_attr(env, Attribute, ValuePtr, StringLength);
}

SQLRETURN SQL_API SQLGetInfo(
    SQLHDBC         ConnectionHandle,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  OOW("===");
  if (ConnectionHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  conn_t *conn = (conn_t*)ConnectionHandle;

  conn_clr_errs(conn);

  return conn_get_info(conn, InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
}

SQLRETURN SQL_API SQLEndTran(
    SQLSMALLINT   HandleType,
    SQLHANDLE     Handle,
    SQLSMALLINT   CompletionType)
{
  OOW("===");
  if (Handle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  switch (HandleType) {
    case SQL_HANDLE_ENV:
      env_clr_errs((env_t*)Handle);
      return env_end_tran((env_t*)Handle, CompletionType);
    case SQL_HANDLE_DBC:
      conn_clr_errs((conn_t*)Handle);
      return conn_end_tran((conn_t*)Handle, CompletionType);
    default:
      OA_NIY(0);
      return SQL_ERROR;
  }
}

#if (ODBCVER >= 0x0300)
SQLRETURN SQL_API SQLSetConnectAttr(
    SQLHDBC       ConnectionHandle,
    SQLINTEGER    Attribute,
    SQLPOINTER    ValuePtr,
    SQLINTEGER    StringLength)
{
  OOW("===");
  if (ConnectionHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  conn_t *conn = (conn_t*)ConnectionHandle;

  conn_clr_errs(conn);

  return conn_set_attr(conn, Attribute, ValuePtr, StringLength);
}
#endif /* ODBCVER >= 0x0300 */

SQLRETURN SQL_API SQLSetStmtAttr(
    SQLHSTMT      StatementHandle,
    SQLINTEGER    Attribute,
    SQLPOINTER    ValuePtr,
    SQLINTEGER    StringLength)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);

  return stmt_set_attr(stmt, Attribute, ValuePtr, StringLength);
}

SQLRETURN SQL_API SQLRowCount(
    SQLHSTMT   StatementHandle,
    SQLLEN    *RowCountPtr)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);

  return stmt_get_row_count((stmt_t*)StatementHandle, RowCountPtr);
}

SQLRETURN SQL_API SQLNumResultCols(
     SQLHSTMT        StatementHandle,
     SQLSMALLINT    *ColumnCountPtr)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);

  return stmt_get_col_count((stmt_t*)StatementHandle, ColumnCountPtr);
}

SQLRETURN SQL_API SQLDescribeCol(
    SQLHSTMT       StatementHandle,
    SQLUSMALLINT   ColumnNumber,
    SQLCHAR       *ColumnName,
    SQLSMALLINT    BufferLength,
    SQLSMALLINT   *NameLengthPtr,
    SQLSMALLINT   *DataTypePtr,
    SQLULEN       *ColumnSizePtr,
    SQLSMALLINT   *DecimalDigitsPtr,
    SQLSMALLINT   *NullablePtr)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);

  return stmt_describe_col(stmt,
      ColumnNumber,
      ColumnName,
      BufferLength,
      NameLengthPtr,
      DataTypePtr,
      ColumnSizePtr,
      DecimalDigitsPtr,
      NullablePtr);
}

SQLRETURN SQL_API SQLBindCol(
    SQLHSTMT       StatementHandle,
    SQLUSMALLINT   ColumnNumber,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);

  return stmt_bind_col(stmt,
      ColumnNumber,
      TargetType,
      TargetValuePtr,
      BufferLength,
      StrLen_or_IndPtr);
}

SQLRETURN SQL_API SQLFetch(
    SQLHSTMT     StatementHandle)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);

  return stmt_fetch(stmt);
}

SQLRETURN SQL_API SQLFetchScroll(
    SQLHSTMT      StatementHandle,
    SQLSMALLINT   FetchOrientation,
    SQLLEN        FetchOffset)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);

  return stmt_fetch_scroll(stmt, FetchOrientation, FetchOffset);
}

SQLRETURN SQL_API SQLFreeStmt(
    SQLHSTMT       StatementHandle,
    SQLUSMALLINT   Option)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);

  return stmt_free_stmt(stmt, Option);
}

SQLRETURN SQL_API SQLGetDiagRec(
    SQLSMALLINT     HandleType,
    SQLHANDLE       Handle,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr)
{
  OOW("===");
  OD("HandleType:%s; Handle:%p", sql_handle_type(HandleType), Handle);
  if (Handle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  switch (HandleType) {
    case SQL_HANDLE_ENV:
      return env_get_diag_rec((env_t*)Handle, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
    case SQL_HANDLE_DBC:
      return conn_get_diag_rec((conn_t*)Handle, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
    case SQL_HANDLE_STMT:
      return stmt_get_diag_rec((stmt_t*)Handle, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
    default:
      OE("HandleType[%s] not supported yet", sql_handle_type(HandleType));
      return SQL_ERROR;
  }

  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetDiagField(
    SQLSMALLINT     HandleType,
    SQLHANDLE       Handle,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  OOW("===");
  if (Handle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

#ifdef _WIN32
  const char *p = "";
  switch (HandleType) {
    case SQL_HANDLE_DBC:
      return conn_get_diag_field((conn_t*)Handle, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_HANDLE_STMT:
      return stmt_get_diag_field((stmt_t*)Handle, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_HANDLE_ENV:
      return env_get_diag_field((env_t*)Handle, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    default:
      OW("`%s` not implemented yet", sql_handle_type(HandleType));
      OA_NIY(0);
      return SQL_ERROR;
  }
#endif
  (void)HandleType;
  (void)RecNumber;
  (void)DiagIdentifier;
  (void)DiagInfoPtr;
  (void)BufferLength;
  (void)StringLengthPtr;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetData(
    SQLHSTMT       StatementHandle,
    SQLUSMALLINT   Col_or_Param_Num,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);

  return stmt_get_data(stmt, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, StrLen_or_IndPtr);
}

SQLRETURN SQL_API SQLPrepare(
    SQLHSTMT      StatementHandle,
    SQLCHAR      *StatementText,
    SQLINTEGER    TextLength)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);

  return stmt_prepare(stmt, StatementText, TextLength);
}

SQLRETURN SQL_API SQLNumParams(
    SQLHSTMT        StatementHandle,
    SQLSMALLINT    *ParameterCountPtr)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);

  return stmt_get_num_params(stmt, ParameterCountPtr);
}

SQLRETURN SQL_API SQLDescribeParam(
    SQLHSTMT        StatementHandle,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT    *DataTypePtr,
    SQLULEN        *ParameterSizePtr,
    SQLSMALLINT    *DecimalDigitsPtr,
    SQLSMALLINT    *NullablePtr)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);

  return stmt_describe_param(
      stmt,
      ParameterNumber,
      DataTypePtr,
      ParameterSizePtr,
      DecimalDigitsPtr,
      NullablePtr);
}

SQLRETURN SQL_API SQLBindParameter(
    SQLHSTMT        StatementHandle,
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
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);

  return stmt_bind_param(stmt,
    ParameterNumber,
    InputOutputType,
    ValueType,
    ParameterType,
    ColumnSize,
    DecimalDigits,
    ParameterValuePtr,
    BufferLength,
    StrLen_or_IndPtr);
}

SQLRETURN SQL_API SQLExecute(
    SQLHSTMT     StatementHandle)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);

  return stmt_execute(stmt);
}

SQLRETURN SQL_API SQLConnect(
    SQLHDBC        ConnectionHandle,
    SQLCHAR       *ServerName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *UserName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *Authentication,
    SQLSMALLINT    NameLength3)
{
  OOW("===");
  if (ConnectionHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  conn_t *conn = (conn_t*)ConnectionHandle;

  conn_clr_errs(conn);

  return conn_connect(
      conn,
      ServerName, NameLength1,
      UserName, NameLength2,
      Authentication, NameLength3);
}

SQLRETURN SQL_API SQLColAttribute(
    SQLHSTMT        StatementHandle,
    SQLUSMALLINT    ColumnNumber,
    SQLUSMALLINT    FieldIdentifier,
    SQLPOINTER      CharacterAttributePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr,
    SQLLEN         *NumericAttributePtr)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_col_attribute(stmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, StringLengthPtr, NumericAttributePtr);
}

SQLRETURN SQL_API SQLTables(
    SQLHSTMT       StatementHandle,
    SQLCHAR       *CatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *SchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *TableName,
    SQLSMALLINT    NameLength3,
    SQLCHAR       *TableType,
    SQLSMALLINT    NameLength4)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_tables(stmt,
    CatalogName, NameLength1,
    SchemaName, NameLength2,
    TableName, NameLength3,
    TableType, NameLength4);
}

#ifdef _WIN32                /* { */
#define POST_INSTALLER_ERROR(hwndParent, code, fmt, ...)             \
do {                                                                 \
  char buf[4096];                                                    \
  char name[1024];                                                   \
  tod_basename(__FILE__, name, sizeof(name));                        \
  snprintf(buf, sizeof(buf), "%s[%d]%s():" fmt "",                   \
           name, __LINE__, __func__,                                 \
           ##__VA_ARGS__);                                           \
  SQLPostInstallerError(code, buf);                                  \
  if (hwndParent) {                                                  \
    MessageBox(hwndParent, buf, "Error", MB_OK|MB_ICONEXCLAMATION);  \
  }                                                                  \
} while (0)

typedef struct kv_s           kv_t;
struct kv_s {
  char    *line;
  size_t   val;
};

static BOOL get_driver_dll_path(HWND hwndParent, char *buf, size_t len)
{
  HMODULE hm = NULL;

  if (GetModuleHandleEx(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
          (LPCSTR) &ConfigDSN, &hm) == 0)
  {
      int ret = GetLastError();
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_REQUEST_FAILED, "GetModuleHandle failed, error = %d\n", ret);
      return FALSE;
  }
  if (GetModuleFileName(hm, buf, (DWORD)len) == 0)
  {
      int ret = GetLastError();
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_REQUEST_FAILED, "GetModuleFileName failed, error = %d\n", ret);
      return FALSE;
  }
  return TRUE;
}

static BOOL doDSNAdd(HWND	hwndParent, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  BOOL r = TRUE;

  kv_t *kvs = NULL;

  kv_t dsn = {0};
  char *line = NULL;

  do {
    char driver_dll[MAX_PATH + 1];
    r = get_driver_dll_path(hwndParent, driver_dll, sizeof(driver_dll));
    if (!r) break;

    const char *p = lpszAttributes;
    int ikvs = 0;
    while (p && *p) {
      line = strdup(p);
      if (!line) { r = FALSE; break; }
      char *v = strchr(line, '=');
      if (!v) { r = FALSE; break; }

      if (strstr(line, "DSN")==line) {
        if (dsn.line) {
          free(dsn.line);
          dsn.line = NULL;
          dsn.val  = 0;
        }
        dsn.line = line;
        line = NULL;
      } else {
        kv_t *t = (kv_t*)realloc(kvs, (ikvs+1)*sizeof(*t));
        if (!t) { r = FALSE; free(line); break; }
        t[ikvs].line = line;
        *v = '\0';
        if (v) t[ikvs].val = v - line + 1;
        line = NULL;

        kvs = t;
        ++ikvs;
      }

      p += strlen(p) + 1;
    }

    if (hwndParent) {
      MessageBox(hwndParent, "Please use odbcconf to add DSN for TAOS ODBC Driver", "Warning!", MB_OK|MB_ICONEXCLAMATION);
    }
    if (!r) break;

    char *v = NULL;
    v = strchr(dsn.line, '=');
    if (!v) { r = FALSE; break; }
    *v = '\0';
    dsn.val = v - dsn.line + 1;

    if ((!dsn.line)) {
      if (!r) POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_REQUEST_FAILED, "lack of either DSN or Driver");
    } else {
      if (r) r = SQLWritePrivateProfileString("ODBC Data Sources", dsn.line+dsn.val, lpszDriver, "Odbc.ini");
      if (r) r = SQLWritePrivateProfileString(dsn.line+dsn.val, "Driver", driver_dll, "Odbc.ini");
    }

    for (int i=0; r && i<ikvs; ++i) {
      const char *k = kvs[i].line;
      const char *v = NULL;
      if (kvs[i].val) v = kvs[i].line + kvs[i].val;
      r = SQLWritePrivateProfileString(dsn.line+dsn.val, k, v, "Odbc.ini");
    }
  } while (0);

  if (dsn.line) free(dsn.line);
  if (line) free(line);

  return r;
}

static BOOL doDSNConfig(HWND	hwndParent, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  const char *p = lpszAttributes;
  while (p && *p) {
    p += strlen(p) + 1;
  }
  return FALSE;
}

static BOOL doDSNRemove(HWND	hwndParent, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  BOOL r = TRUE;

  kv_t dsn = {0};
  char *line = NULL;

  do {
    const char *p = lpszAttributes;
    int ikvs = 0;
    while (p && *p) {
      line = strdup(p);
      if (!line) { r = FALSE; break; }
      char *v = strchr(line, '=');
      if (!v) { r = FALSE; break; }
      *v = '\0';

      if (strstr(line, "DSN")==line) {
        if (dsn.line) {
          free(dsn.line);
          dsn.line = NULL;
          dsn.val  = 0;
        }
        dsn.line = line;
        dsn.val = v - line + 1;
        line = NULL;
        break;
      } else {
        free(line);
        line = NULL;
      }

      p += strlen(p) + 1;
    }

    if (!r) break;

    if (!dsn.line) {
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_REQUEST_FAILED, "lack of DSN");
      r = FALSE;
      break;
    }

    r = SQLWritePrivateProfileString("ODBC Data Sources", dsn.line+dsn.val, NULL, "Odbc.ini");
    if (!r) break;

    char buf[8192];
    r = SQLGetPrivateProfileString(dsn.line+dsn.val, NULL, "null", buf, sizeof(buf), "Odbc.ini");
    if (!r) break;

    int n = 0;
    char *s = buf;
    while (s && *s && n++<10) {
      SQLWritePrivateProfileString(dsn.line+dsn.val, s, NULL, "Odbc.ini");
      s += strlen(s) + 1;
    }
  } while (0);

  if (dsn.line) free(dsn.line);
  if (line) free(line);
  return r;
}

static BOOL doConfigDSN(HWND	hwndParent, WORD fRequest, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  BOOL r = FALSE;
  const char *sReq = NULL;
  switch(fRequest) {
    case ODBC_ADD_DSN:    sReq = "ODBC_ADD_DSN";      break;
    case ODBC_CONFIG_DSN: sReq = "ODBC_CONFIG_DSN";   break;
    case ODBC_REMOVE_DSN: sReq = "ODBC_REMOVE_DSN";   break;
    default:              sReq = "UNKNOWN";           break;
  }
  switch(fRequest) {
    case ODBC_ADD_DSN: {
      r = doDSNAdd(hwndParent, lpszDriver, lpszAttributes);
    } break;
    case ODBC_CONFIG_DSN: {
      r = doDSNConfig(hwndParent, lpszDriver, lpszAttributes);
    } break;
    case ODBC_REMOVE_DSN: {
      r = doDSNRemove(hwndParent, lpszDriver, lpszAttributes);
    } break;
    default: {
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_GENERAL_ERR, "not implemented yet");
      r = FALSE;
    } break;
  }
  return r;
}

BOOL INSTAPI ConfigDSN(HWND	hwndParent, WORD fRequest, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  BOOL r;
  r = doConfigDSN(hwndParent, fRequest, lpszDriver, lpszAttributes);
  return r;
}

BOOL INSTAPI ConfigTranslator(HWND hwndParent, DWORD *pvOption)
{
  (void)hwndParent;
  (void)pvOption;
  POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_GENERAL_ERR, "not implemented yet");
  return FALSE;
}

BOOL INSTAPI ConfigDriver(HWND hwndParent, WORD fRequest, LPCSTR lpszDriver, LPCSTR lpszArgs,
                          LPSTR lpszMsg, WORD cbMsgMax, WORD *pcbMsgOut)
{
  (void)hwndParent;
  (void)fRequest;
  (void)lpszDriver;
  (void)lpszArgs;
  (void)lpszMsg;
  (void)cbMsgMax;
  (void)pcbMsgOut;
  POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_GENERAL_ERR, "not implemented yet");
  return FALSE;
}

#endif                       /* } */

#if (ODBCVER >= 0x0300)
SQLRETURN SQL_API SQLBulkOperations(
    SQLHSTMT            StatementHandle,
    SQLSMALLINT         Operation)
{
  OOW("===");
  (void)StatementHandle;
  OD("Operation:%s", sql_bulk_operation(Operation));
  OA_NIY(0);
  return SQL_ERROR;
}
#endif  /* ODBCVER >= 0x0300 */

#if 0
SQLRETURN SQL_API SQLCancel(SQLHSTMT StatementHandle)
{
  OOW("===");
  (void)StatementHandle;
  OA_NIY(0);
}
#endif

#if (ODBCVER >= 0x0300)
#if 0
SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT StatementHandle)
{
  OOW("===");
  (void)StatementHandle;
  // NOTE: some tiny difference in SQLFreeStmt
  // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlclosecursor-function?view=sql-server-ver16
  return SQLFreeStmt(StatementHandle, SQL_CLOSE);
}
#endif
#endif

SQLRETURN SQL_API SQLColumnPrivileges(
    SQLHSTMT           hstmt,
    SQLCHAR           *szCatalogName,
    SQLSMALLINT        cchCatalogName,
    SQLCHAR           *szSchemaName,
    SQLSMALLINT        cchSchemaName,
    SQLCHAR           *szTableName,
    SQLSMALLINT        cchTableName,
    SQLCHAR           *szColumnName,
    SQLSMALLINT        cchColumnName)
{
  OOW("===");
  (void)hstmt;
  (void)szCatalogName;
  (void)cchCatalogName;
  (void)szSchemaName;
  (void)cchSchemaName;
  (void)szTableName;
  (void)cchTableName;
  (void)szColumnName;
  (void)cchColumnName;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLColumns(SQLHSTMT StatementHandle,
          SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
          SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
          SQLCHAR *TableName, SQLSMALLINT NameLength3,
          SQLCHAR *ColumnName, SQLSMALLINT NameLength4)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_columns(stmt, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3, ColumnName, NameLength4);
}

#if (ODBCVER >= 0x0300)
SQLRETURN SQL_API SQLCopyDesc(SQLHDESC SourceDescHandle,
           SQLHDESC TargetDescHandle)
{
  OOW("===");
  (void)SourceDescHandle;
  (void)TargetDescHandle;
  OA_NIY(0);
  return SQL_ERROR;
}
#endif

SQLRETURN SQL_API SQLExtendedFetch(
    SQLHSTMT           hstmt,
    SQLUSMALLINT       fFetchType,
    SQLLEN             irow,
    SQLULEN           *pcrow,
    SQLUSMALLINT      *rgfRowStatus)
{
  OOW("===");
  (void)hstmt;
  (void)fFetchType;
  (void)irow;
  (void)pcrow;
  (void)rgfRowStatus;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLForeignKeys(
    SQLHSTMT           hstmt,
    SQLCHAR           *szPkCatalogName,
    SQLSMALLINT        cchPkCatalogName,
    SQLCHAR           *szPkSchemaName,
    SQLSMALLINT        cchPkSchemaName,
    SQLCHAR           *szPkTableName,
    SQLSMALLINT        cchPkTableName,
    SQLCHAR           *szFkCatalogName,
    SQLSMALLINT        cchFkCatalogName,
    SQLCHAR           *szFkSchemaName,
    SQLSMALLINT        cchFkSchemaName,
    SQLCHAR           *szFkTableName,
    SQLSMALLINT        cchFkTableName)
{
  OOW("===");
  (void)hstmt;
  (void)szPkCatalogName;
  (void)cchPkCatalogName;
  (void)szPkSchemaName;
  (void)cchPkSchemaName;
  (void)szPkTableName;
  (void)cchPkTableName;
  (void)szFkCatalogName;
  (void)cchFkCatalogName;
  (void)szFkSchemaName;
  (void)cchFkSchemaName;
  (void)szFkTableName;
  (void)cchFkTableName;
  OA_NIY(0);
  return SQL_ERROR;
}

#if (ODBCVER >= 0x0300)
SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC ConnectionHandle,
           SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLengthPtr)
{
  OOW("===");
  if (ConnectionHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  conn_t *conn = (conn_t*)ConnectionHandle;

  conn_clr_errs(conn);

  return conn_get_attr(conn, Attribute, Value, BufferLength, StringLengthPtr);
}
#endif

SQLRETURN SQL_API SQLGetCursorName(
    SQLHSTMT StatementHandle,
    SQLCHAR *CursorName,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *NameLengthPtr)
{
  OOW("===");
  (void)StatementHandle;
  (void)CursorName;
  (void)BufferLength;
  (void)NameLengthPtr;
  OA_NIY(0);
  return SQL_ERROR;
}

#if (ODBCVER >= 0x0300)
SQLRETURN SQL_API SQLGetDescField(SQLHDESC DescriptorHandle,
           SQLSMALLINT RecNumber, SQLSMALLINT FieldIdentifier,
           SQLPOINTER Value, SQLINTEGER BufferLength,
           SQLINTEGER *StringLength)
{
  OOW("===");
  (void)DescriptorHandle;
  (void)RecNumber;
  (void)FieldIdentifier;
  (void)Value;
  (void)BufferLength;
  (void)StringLength;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetDescRec(SQLHDESC DescriptorHandle,
           SQLSMALLINT RecNumber, SQLCHAR *Name,
           SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr,
           SQLSMALLINT *TypePtr, SQLSMALLINT *SubTypePtr,
           SQLLEN     *LengthPtr, SQLSMALLINT *PrecisionPtr,
           SQLSMALLINT *ScalePtr, SQLSMALLINT *NullablePtr)
{
  OOW("===");
  (void)DescriptorHandle;
  (void)RecNumber;
  (void)Name;
  (void)BufferLength;
  (void)StringLengthPtr;
  (void)TypePtr;
  (void)SubTypePtr;
  (void)LengthPtr;
  (void)PrecisionPtr;
  (void)ScalePtr;
  (void)NullablePtr;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV EnvironmentHandle,
           SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
  OOW("===");
  (void)EnvironmentHandle;
  (void)Attribute;
  (void)Value;
  (void)BufferLength;
  (void)StringLength;
  if (EnvironmentHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  env_t *env = (env_t*)EnvironmentHandle;

  env_clr_errs(env);

  return env_get_attr(env, Attribute, Value, BufferLength, StringLength);
}
#endif

#if 0
SQLRETURN SQL_API SQLGetFunctions(SQLHDBC ConnectionHandle,
           SQLUSMALLINT FunctionId,
           SQLUSMALLINT *Supported)
{
  OOW("===");
  (void)ConnectionHandle;
  (void)FunctionId;
  (void)Supported;
  OA_NIY(0);
}
#endif

#if (ODBCVER >= 0x0300)
SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT StatementHandle,
           SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
  OOW("===");
  stmt_clr_errs((stmt_t*)StatementHandle);
  return stmt_get_attr((stmt_t*)StatementHandle, Attribute, Value, BufferLength, StringLength);
}
#endif  /* ODBCVER >= 0x0300 */

SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT StatementHandle,
           SQLSMALLINT DataType)
{
  OOW("===");
  (void)StatementHandle;
  (void)DataType;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLMoreResults(
    SQLHSTMT           StatementHandle)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_more_results(stmt);
}

SQLRETURN SQL_API SQLNativeSql(
    SQLHDBC            hdbc,
    SQLCHAR* szSqlStrIn,
    SQLINTEGER  cchSqlStrIn,
    SQLCHAR* szSqlStr,
    SQLINTEGER         cchSqlStrMax,
    SQLINTEGER        *pcbSqlStr)
{
  OOW("===");
  (void)hdbc;
  (void)szSqlStrIn;
  (void)cchSqlStrIn;
  (void)szSqlStr;
  (void)cchSqlStrMax;
  (void)pcbSqlStr;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLParamData(SQLHSTMT StatementHandle,
           SQLPOINTER *Value)
{
  OOW("===");
  (void)StatementHandle;
  (void)Value;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLPrimaryKeys(
    SQLHSTMT           hstmt,
    SQLCHAR           *szCatalogName,
    SQLSMALLINT        cchCatalogName,
    SQLCHAR           *szSchemaName,
    SQLSMALLINT        cchSchemaName,
    SQLCHAR           *szTableName,
    SQLSMALLINT        cchTableName)
{
  OOW("===");
  (void)hstmt;
  (void)szCatalogName;
  (void)cchCatalogName;
  (void)szSchemaName;
  (void)cchSchemaName;
  (void)szTableName;
  (void)cchTableName;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLProcedureColumns(
    SQLHSTMT           hstmt,
    SQLCHAR           *szCatalogName,
    SQLSMALLINT        cchCatalogName,
    SQLCHAR           *szSchemaName,
    SQLSMALLINT        cchSchemaName,
    SQLCHAR           *szProcName,
    SQLSMALLINT        cchProcName,
    SQLCHAR           *szColumnName,
    SQLSMALLINT        cchColumnName)
{
  OOW("===");
  (void)hstmt;
  (void)szCatalogName;
  (void)cchCatalogName;
  (void)szSchemaName;
  (void)cchSchemaName;
  (void)szProcName;
  (void)cchProcName;
  (void)szColumnName;
  (void)cchColumnName;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLProcedures(
    SQLHSTMT           hstmt,
    SQLCHAR           *szCatalogName,
    SQLSMALLINT        cchCatalogName,
    SQLCHAR           *szSchemaName,
    SQLSMALLINT        cchSchemaName,
    SQLCHAR           *szProcName,
    SQLSMALLINT        cchProcName)
{
  OOW("===");
  (void)hstmt;
  (void)szCatalogName;
  (void)cchCatalogName;
  (void)szSchemaName;
  (void)cchSchemaName;
  (void)szProcName;
  (void)cchProcName;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLPutData(SQLHSTMT StatementHandle,
           SQLPOINTER Data, SQLLEN StrLen_or_Ind)
{
  OOW("===");
  (void)StatementHandle;
  (void)Data;
  (void)StrLen_or_Ind;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLSetCursorName(
    SQLHSTMT StatementHandle,
    SQLCHAR* CursorName,
    SQLSMALLINT NameLength)
{
  OOW("===");
  (void)StatementHandle;
  (void)CursorName;
  (void)NameLength;
  OA_NIY(0);
  return SQL_ERROR;
}

#if (ODBCVER >= 0x0300)
SQLRETURN SQL_API SQLSetDescField(SQLHDESC DescriptorHandle,
           SQLSMALLINT RecNumber, SQLSMALLINT FieldIdentifier,
           SQLPOINTER Value, SQLINTEGER BufferLength)
{
  OOW("===");
  (void)DescriptorHandle;
  (void)RecNumber;
  (void)FieldIdentifier;
  (void)Value;
  (void)BufferLength;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLSetDescRec(SQLHDESC DescriptorHandle,
           SQLSMALLINT RecNumber, SQLSMALLINT Type,
           SQLSMALLINT SubType, SQLLEN Length,
           SQLSMALLINT Precision, SQLSMALLINT Scale,
           SQLPOINTER Data, SQLLEN *StringLength,
           SQLLEN *Indicator)
{
  OOW("===");
  (void)DescriptorHandle;
  (void)RecNumber;
  (void)Type;
  (void)SubType;
  (void)Length;
  (void)Precision;
  (void)Scale;
  (void)Data;
  (void)StringLength;
  (void)Indicator;
  OA_NIY(0);
  return SQL_ERROR;
}
#endif /* ODBCVER >= 0x0300 */

SQLRETURN SQL_API SQLSetPos(
    SQLHSTMT           hstmt,
    SQLSETPOSIROW      irow,
    SQLUSMALLINT       fOption,
    SQLUSMALLINT       fLock)
{
  OOW("===");
  (void)hstmt;
  (void)irow;
  (void)fOption;
  (void)fLock;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLSpecialColumns(SQLHSTMT StatementHandle,
           SQLUSMALLINT IdentifierType,
           SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
           SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
           SQLCHAR *TableName, SQLSMALLINT NameLength3,
           SQLUSMALLINT Scope, SQLUSMALLINT Nullable)
{
  OOW("===");
  (void)StatementHandle;
  (void)IdentifierType;
  (void)CatalogName;
  (void)NameLength1;
  (void)SchemaName;
  (void)NameLength2;
  (void)TableName;
  (void)NameLength3;
  (void)Scope;
  (void)Nullable;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLStatistics(SQLHSTMT StatementHandle,
           SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
           SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
           SQLCHAR *TableName, SQLSMALLINT NameLength3,
           SQLUSMALLINT Unique, SQLUSMALLINT Reserved)
{
  OOW("===");
  (void)StatementHandle;
  (void)CatalogName;
  (void)NameLength1;
  (void)SchemaName;
  (void)NameLength2;
  (void)TableName;
  (void)NameLength3;
  (void)Unique;
  (void)Reserved;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLTablePrivileges(
    SQLHSTMT           hstmt,
    SQLCHAR           *szCatalogName,
    SQLSMALLINT        cchCatalogName,
    SQLCHAR           *szSchemaName,
    SQLSMALLINT        cchSchemaName,
    SQLCHAR           *szTableName,
    SQLSMALLINT        cchTableName)
{
  OOW("===");
  (void)hstmt;
  (void)szCatalogName;
  (void)cchCatalogName;
  (void)szSchemaName;
  (void)cchSchemaName;
  (void)szTableName;
  (void)cchTableName;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLBrowseConnect(
    SQLHDBC        ConnectionHandle,
    SQLCHAR       *InConnectionString,
    SQLSMALLINT    StringLength1,
    SQLCHAR       *OutConnectionString,
    SQLSMALLINT    BufferLength,
    SQLSMALLINT   *StringLength2Ptr)
{
  OOW("===");
  (void)ConnectionHandle;
  (void)InConnectionString;
  (void)StringLength1;
  (void)OutConnectionString;
  (void)BufferLength;
  (void)StringLength2Ptr;
  OA_NIY(0);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLCompleteAsync(
    SQLSMALLINT  HandleType,
    SQLHANDLE    Handle,
    RETCODE     *AsyncRetCodePtr)
{
  OOW("===");
  (void)HandleType;
  (void)Handle;
  (void)AsyncRetCodePtr;
  OA_NIY(0);
  return SQL_ERROR;
}
