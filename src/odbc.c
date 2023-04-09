/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
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
#include "setup.h"
#include "stmt.h"
#include "tls.h"

#ifndef _WIN32
#include <locale.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include <sqlucode.h>

// NOTE: if you wanna debug in detail, just define DEBUG_OOW to 1
// NOTE: this is performance-hit, please take serious consideration in advance!!!
#define DEBUG_OOW      0
#define OOW(fmt, ...) do {        \
  if (DEBUG_OOW) {                \
    OW(fmt, ##__VA_ARGS__);       \
  }                               \
} while (0)

#define TRACE_LEAK(fmt, ...) do {                                            \
  if (DEBUG_OOW) {                                                           \
    OE("%zx:%zx:%d[%s]:" fmt,                                                \
        tod_get_current_process_id(), tod_get_current_thread_id(),           \
        tls_idx_state, tls_idx_state_str(tls_idx_state), ##__VA_ARGS__);     \
  }                                                                          \
} while (0)

static int check_env_bool(const char *name)
{
  const char *val = tod_getenv(name);
  if (!val) return 0;
  if (!*val) return 1;
  if (tod_strcasecmp(val, "true") == 0) return 1;
  if (tod_strcasecmp(val, "on") == 0) return 1;
  return atoi(val);
}

typedef struct global_s                  global_t;
struct global_s {
  atomic_int    nr_load;
  unsigned int  taos_odbc_debug_flex;
  unsigned int  taos_odbc_debug_bison;

  char          locale_or_ACP[64];
  char          sqlc_charset[64];
};

static global_t _global;

static void _init_charsets(void)
{
#ifdef _WIN32
  UINT acp = GetACP();
  snprintf(_global.locale_or_ACP, sizeof(_global.locale_or_ACP), "%d", acp);
  switch (acp) {
    case 936:
      snprintf(_global.sqlc_charset, sizeof(_global.sqlc_charset), "GB18030");
      break;
    case 65001:
      snprintf(_global.sqlc_charset, sizeof(_global.sqlc_charset), "UTF-8");
      break;
    default:
      break;
  }
#else
  const char *locale = setlocale(LC_CTYPE, "");
  if (!locale) return;
  snprintf(_global.locale_or_ACP, sizeof(_global.locale_or_ACP), "%s", locale);

  const char *p = strchr(locale, '.');
  p = p ? p + 1 : locale;
  snprintf(_global.sqlc_charset, sizeof(_global.sqlc_charset), "%s", p);
#endif
}

static void _init_once(void)
{
  _global.taos_odbc_debug_flex  = check_env_bool("TAOS_ODBC_DEBUG_FLEX");
  _global.taos_odbc_debug_bison = check_env_bool("TAOS_ODBC_DEBUG_BISON");
  _init_charsets();
}

int tod_get_debug_flex(void)
{
  return !!_global.taos_odbc_debug_flex;
}

int tod_get_debug_bison(void)
{
  return !!_global.taos_odbc_debug_bison;
}

const char* tod_get_sqlc_charset(void)
{
  if (!_global.sqlc_charset[0]) return NULL;
  return _global.sqlc_charset;
}

const char* tod_get_locale_or_ACP(void)
{
  if (!_global.locale_or_ACP[0]) return NULL;
  return _global.locale_or_ACP;
}

int get_nr_load(void)
{
  return atomic_load(&_global.nr_load);
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
static DWORD tls_idx = 0;
static DWORD dwTlsIndex; // address of shared memory
#define TLS_IDX_UNINITIALIZED -1
#define TLS_IDX_INITIALIZED    0
#define TLS_IDX_FINALIZED      1
static int8_t tls_idx_state = TLS_IDX_UNINITIALIZED;

static const char *tls_idx_state_str(uint8_t state)
{
  switch (state) {
    case TLS_IDX_UNINITIALIZED: return "TLS_IDX_INITIALIZED";
    case TLS_IDX_INITIALIZED: return "TLS_IDX_INITIALIZED";
    case TLS_IDX_FINALIZED: return "TLS_IDX_FINALIZED";
    default: return "TLS_IDX_UNKNOWN";
  }
}

tls_t* tls_get(void)
{
  if (tls_idx_state != TLS_IDX_INITIALIZED) {
    TRACE_LEAK("thread:tls_get:failed");
    return NULL;
  }
  tls_t *tls = TlsGetValue(tls_idx);
  if (tls) return tls;

  tls = (tls_t*)LocalAlloc(LPTR, tls_size());
  if (tls && !TlsSetValue(tls_idx, tls)) {
    LocalFree((HLOCAL)tls);
    TRACE_LEAK("thread:tls_get:failed");
    return NULL;
  }
  if (tls) OA(tls == TlsGetValue(tls_idx), "");
  TRACE_LEAK("thread:tls_get#alloc:%p", tls);

  return tls;
}

BOOL WINAPI DllMain(
    HINSTANCE hinstDLL, // handle to DLL module
    DWORD fdwReason,    // reason for calling function
    LPVOID lpvReserved) // reserved
{
  init_global();

  tls_t *tls = NULL;

  // Perform actions based on the reason for calling.
  switch (fdwReason)
  {
  case DLL_PROCESS_ATTACH:
    // Initialize once for each new process.
    // Return FALSE to fail DLL load.

    if (tls_idx_state != TLS_IDX_UNINITIALIZED) {
      TRACE_LEAK("process:attach:failed_check");
      return FALSE;
    }

    if ((tls_idx = TlsAlloc()) == TLS_OUT_OF_INDEXES) {
      TRACE_LEAK("process:attach:failed");
      return FALSE;
    }
    tls_idx_state = TLS_IDX_INITIALIZED;
    TRACE_LEAK("process:attach:ok");

    atomic_fetch_add(&_global.nr_load, 1);

    return setup_init(hinstDLL);

  case DLL_THREAD_ATTACH:
    // TRACE_LEAK("thread attach");
    // Do thread-specific initialization.
    // tls = (tls_t*)LocalAlloc(LPTR, tls_size());
    // if (tls && !TlsSetValue(tls_idx, tls)) {
    //   LocalFree((HLOCAL)tls);
    //   tls = NULL;
    // }
    if (tls_idx_state != TLS_IDX_INITIALIZED) {
      TRACE_LEAK("thread:attach:failed_check");
    } else {
      TRACE_LEAK("thread:attach:ok");
    }

    break;

  case DLL_THREAD_DETACH:
    // TRACE_LEAK("thread detach");
    // Do thread-specific cleanup.
    if (tls_idx_state != TLS_IDX_INITIALIZED) {
      TRACE_LEAK("thread:detach:failed_check");
    }
    tls_t *tls = TlsGetValue(tls_idx);
    TRACE_LEAK("thread:detach:ok");
    if (tls) {
      TRACE_LEAK("thread:tls_release#free:%p", tls);
      tls_release(tls);
      LocalFree((HLOCAL)tls);
      tls = NULL;
      TlsSetValue(tls_idx, NULL);
    }
    break;

  case DLL_PROCESS_DETACH:
    setup_fini();
    atomic_fetch_sub(&_global.nr_load, 1);

    if (tls_idx_state != TLS_IDX_INITIALIZED) {
      TRACE_LEAK("process:detach:failed_check");
    }
    tls = TlsGetValue(tls_idx);
    TRACE_LEAK("process:detach:ok");
    if (tls) {
      TRACE_LEAK("process:tls_release#free:%p", tls);
      tls_release(tls);
      LocalFree((HLOCAL)tls);
      tls = NULL;
      TlsSetValue(tls_idx, NULL);
    }

    TlsFree(tls_idx); 
    tls_idx = 0;
    tls_idx_state = TLS_IDX_FINALIZED;

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
static int key_inited = 0;

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
  key_inited = 1;
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
  atomic_fetch_add(&_global.nr_load, 1);
}

__attribute__((destructor)) void _deinitialize(void)
{
  if (key_inited) {
    tls_t *tls = (tls_t*)pthread_getspecific(tls_key);
    if (tls) {
      _tls_destroy(tls);
    }
    pthread_setspecific(tls_key, NULL);
  }
  // yes, no check return value
  atomic_fetch_sub(&_global.nr_load, 1);
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

  return stmt_exec_direct(stmt, StatementText, TextLength);
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

#if (ODBCVER >= 0x0300)                  /* { */
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
#endif                                   /* } */

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
  if (Handle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  switch (HandleType) {
    case SQL_HANDLE_ENV:
      return env_get_diag_rec((env_t*)Handle, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
    case SQL_HANDLE_DBC:
      return conn_get_diag_rec((conn_t*)Handle, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
    case SQL_HANDLE_STMT:
      return stmt_get_diag_rec((stmt_t*)Handle, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
    case SQL_HANDLE_DESC:
      return desc_get_diag_rec((desc_t*)Handle, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
    default:
      OE("HandleType[%s] not supported yet", sql_handle_type(HandleType));
      return SQL_ERROR;
  }
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

  switch (HandleType) {
    case SQL_HANDLE_DBC:
      return conn_get_diag_field((conn_t*)Handle, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_HANDLE_STMT:
      return stmt_get_diag_field((stmt_t*)Handle, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_HANDLE_ENV:
      return env_get_diag_field((env_t*)Handle, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_HANDLE_DESC:
      return desc_get_diag_field((desc_t*)Handle, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    default:
      OW("`%s` not implemented yet", sql_handle_type(HandleType));
      OA_NIY(0);
      return SQL_ERROR;
  }
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

#if (ODBCVER >= 0x0300)                  /* { */
SQLRETURN SQL_API SQLBulkOperations(
    SQLHSTMT            StatementHandle,
    SQLSMALLINT         Operation)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);

  return stmt_bulk_operations(stmt, Operation);
}
#endif                                   /* } */

#if 0                          /* { */
SQLRETURN SQL_API SQLCancel(SQLHSTMT StatementHandle)
{
  OOW("===");
  (void)StatementHandle;
  OA_NIY(0);
}
#endif                         /* } */

#if (ODBCVER >= 0x0300)                  /* { */
SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT StatementHandle)
{
  OOW("===");
  // NOTE: some tiny difference in SQLFreeStmt
  // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlclosecursor-function?view=sql-server-ver16
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);

  return stmt_close_cursor(stmt);
}
#endif                                   /* } */

SQLRETURN SQL_API SQLColumnPrivileges(
    SQLHSTMT      StatementHandle,
    SQLCHAR      *CatalogName,
    SQLSMALLINT   NameLength1,
    SQLCHAR      *SchemaName,
    SQLSMALLINT   NameLength2,
    SQLCHAR      *TableName,
    SQLSMALLINT   NameLength3,
    SQLCHAR      *ColumnName,
    SQLSMALLINT   NameLength4)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_column_privileges(stmt, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3, ColumnName, NameLength4);
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

#if (ODBCVER >= 0x0300)       /* { */
SQLRETURN SQL_API SQLCopyDesc(SQLHDESC SourceDescHandle,
           SQLHDESC TargetDescHandle)
{
  OOW("===");
  if (SourceDescHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;
  if (TargetDescHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  desc_t *src = (desc_t*)SourceDescHandle;
  desc_t *tgt = (desc_t*)TargetDescHandle;

  desc_clr_errs(src);
  desc_clr_errs(tgt);

  return desc_copy(src, tgt);
}
#endif                        /* } */

SQLRETURN SQL_API SQLExtendedFetch(
    SQLHSTMT         StatementHandle,
    SQLUSMALLINT     FetchOrientation,
    SQLLEN           FetchOffset,
    SQLULEN         *RowCountPtr,
    SQLUSMALLINT    *RowStatusArray)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_extended_fetch(stmt, FetchOrientation, FetchOffset, RowCountPtr, RowStatusArray);
}

SQLRETURN SQL_API SQLForeignKeys(
    SQLHSTMT       StatementHandle,
    SQLCHAR       *PKCatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *PKSchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *PKTableName,
    SQLSMALLINT    NameLength3,
    SQLCHAR       *FKCatalogName,
    SQLSMALLINT    NameLength4,
    SQLCHAR       *FKSchemaName,
    SQLSMALLINT    NameLength5,
    SQLCHAR       *FKTableName,
    SQLSMALLINT    NameLength6)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_foreign_keys(stmt, PKCatalogName, NameLength1, PKSchemaName, NameLength2,
      PKTableName, NameLength3, FKCatalogName, NameLength4, FKSchemaName, NameLength5, FKTableName, NameLength6);
}

#if (ODBCVER >= 0x0300)        /* { */
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
#endif                         /* } */

SQLRETURN SQL_API SQLGetCursorName(
    SQLHSTMT      StatementHandle,
    SQLCHAR      *CursorName,
    SQLSMALLINT   BufferLength,
    SQLSMALLINT  *NameLengthPtr)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_get_cursor_name(stmt, CursorName, BufferLength, NameLengthPtr);
}

#if (ODBCVER >= 0x0300)         /* { */
SQLRETURN SQL_API SQLGetDescField(SQLHDESC DescriptorHandle,
           SQLSMALLINT RecNumber, SQLSMALLINT FieldIdentifier,
           SQLPOINTER Value, SQLINTEGER BufferLength,
           SQLINTEGER *StringLength)
{
  OOW("===");
  if (DescriptorHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  desc_t *desc = (desc_t*)DescriptorHandle;

  desc_clr_errs(desc);
  return desc_get_field(desc, RecNumber, FieldIdentifier, Value, BufferLength, StringLength);
}

SQLRETURN SQL_API SQLGetDescRec(SQLHDESC DescriptorHandle,
           SQLSMALLINT RecNumber, SQLCHAR *Name,
           SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr,
           SQLSMALLINT *TypePtr, SQLSMALLINT *SubTypePtr,
           SQLLEN     *LengthPtr, SQLSMALLINT *PrecisionPtr,
           SQLSMALLINT *ScalePtr, SQLSMALLINT *NullablePtr)
{
  OOW("===");
  if (DescriptorHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  desc_t *desc = (desc_t*)DescriptorHandle;

  desc_clr_errs(desc);
  return desc_get_rec(
      desc,
      RecNumber, Name,
      BufferLength, StringLengthPtr,
      TypePtr, SubTypePtr,
      LengthPtr, PrecisionPtr,
      ScalePtr, NullablePtr);
}

SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV EnvironmentHandle,
           SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
  OOW("===");
  if (EnvironmentHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  env_t *env = (env_t*)EnvironmentHandle;

  env_clr_errs(env);

  return env_get_attr(env, Attribute, Value, BufferLength, StringLength);
}
#endif                          /* } */

#if 0                           /* { */
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
#endif                          /* } */

#if (ODBCVER >= 0x0300)                  /* { */
SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT StatementHandle,
           SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_get_attr(stmt, Attribute, Value, BufferLength, StringLength);
}
#endif                                   /* } */

SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT StatementHandle,
           SQLSMALLINT DataType)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_get_type_info(stmt, DataType);
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
    SQLHDBC        ConnectionHandle,
    SQLCHAR       *InStatementText,
    SQLINTEGER     TextLength1,
    SQLCHAR       *OutStatementText,
    SQLINTEGER     BufferLength,
    SQLINTEGER    *TextLength2Ptr)
{
  OOW("===");
  if (ConnectionHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  conn_t *conn = (conn_t*)ConnectionHandle;

  conn_clr_errs(conn);
  return conn_native_sql((conn_t*)ConnectionHandle, InStatementText, TextLength1, OutStatementText, BufferLength, TextLength2Ptr);
}

SQLRETURN SQL_API SQLParamData(SQLHSTMT StatementHandle,
           SQLPOINTER *Value)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_param_data(stmt, Value);
}

SQLRETURN SQL_API SQLPrimaryKeys(
    SQLHSTMT       StatementHandle,
    SQLCHAR       *CatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *SchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *TableName,
    SQLSMALLINT    NameLength3)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_primary_keys(stmt, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3);
}

SQLRETURN SQL_API SQLProcedureColumns(
    SQLHSTMT      StatementHandle,
    SQLCHAR      *CatalogName,
    SQLSMALLINT   NameLength1,
    SQLCHAR      *SchemaName,
    SQLSMALLINT   NameLength2,
    SQLCHAR      *ProcName,
    SQLSMALLINT   NameLength3,
    SQLCHAR      *ColumnName,
    SQLSMALLINT   NameLength4)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_procedure_columns(stmt, CatalogName, NameLength1, SchemaName, NameLength2, ProcName, NameLength3, ColumnName, NameLength4);
}

SQLRETURN SQL_API SQLProcedures(
    SQLHSTMT        StatementHandle,
    SQLCHAR        *CatalogName,
    SQLSMALLINT     NameLength1,
    SQLCHAR        *SchemaName,
    SQLSMALLINT     NameLength2,
    SQLCHAR        *ProcName,
    SQLSMALLINT     NameLength3)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_procedures(stmt, CatalogName, NameLength1, SchemaName, NameLength2, ProcName, NameLength3);
}

SQLRETURN SQL_API SQLPutData(SQLHSTMT StatementHandle,
           SQLPOINTER Data, SQLLEN StrLen_or_Ind)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_put_data(stmt, Data, StrLen_or_Ind);
}

SQLRETURN SQL_API SQLSetCursorName(
    SQLHSTMT     StatementHandle,
    SQLCHAR     *CursorName,
    SQLSMALLINT  NameLength)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_set_cursor_name(stmt, CursorName, NameLength);
}

#if (ODBCVER >= 0x0300)          /* { */
SQLRETURN SQL_API SQLSetDescField(SQLHDESC DescriptorHandle,
           SQLSMALLINT RecNumber, SQLSMALLINT FieldIdentifier,
           SQLPOINTER Value, SQLINTEGER BufferLength)
{
  OOW("===");
  if (DescriptorHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  desc_t *desc = (desc_t*)DescriptorHandle;

  desc_clr_errs(desc);
  return desc_set_field(desc, RecNumber, FieldIdentifier, Value, BufferLength);
}

SQLRETURN SQL_API SQLSetDescRec(SQLHDESC DescriptorHandle,
           SQLSMALLINT RecNumber, SQLSMALLINT Type,
           SQLSMALLINT SubType, SQLLEN Length,
           SQLSMALLINT Precision, SQLSMALLINT Scale,
           SQLPOINTER Data, SQLLEN *StringLength,
           SQLLEN *Indicator)
{
  OOW("===");
  if (DescriptorHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  desc_t *desc = (desc_t*)DescriptorHandle;

  desc_clr_errs(desc);
  return desc_set_rec(
      desc,
      RecNumber, Type,
      SubType, Length,
      Precision, Scale,
      Data, StringLength,
      Indicator);
}
#endif                           /* } */

SQLRETURN SQL_API SQLSetPos(
    SQLHSTMT        StatementHandle,
    SQLSETPOSIROW   RowNumber,
    SQLUSMALLINT    Operation,
    SQLUSMALLINT    LockType)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_set_pos(stmt, RowNumber, Operation, LockType);
}

SQLRETURN SQL_API SQLSpecialColumns(
    SQLHSTMT StatementHandle,
    SQLUSMALLINT IdentifierType,
    SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
    SQLCHAR *TableName, SQLSMALLINT NameLength3,
    SQLUSMALLINT Scope, SQLUSMALLINT Nullable)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_special_columns(
      stmt, IdentifierType, CatalogName, NameLength1,
      SchemaName, NameLength2, TableName, NameLength3, Scope, Nullable);
}

SQLRETURN SQL_API SQLStatistics(
    SQLHSTMT StatementHandle,
    SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
    SQLCHAR *TableName, SQLSMALLINT NameLength3,
    SQLUSMALLINT Unique, SQLUSMALLINT Reserved)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_statistics(
      stmt,
      CatalogName, NameLength1,
      SchemaName, NameLength2,
      TableName, NameLength3,
      Unique, Reserved);
}

SQLRETURN SQL_API SQLTablePrivileges(
    SQLHSTMT StatementHandle,
    SQLCHAR *CatalogName,
    SQLSMALLINT NameLength1,
    SQLCHAR *SchemaName,
    SQLSMALLINT NameLength2,
    SQLCHAR *TableName,
    SQLSMALLINT NameLength3)
{
  OOW("===");
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  stmt_clr_errs(stmt);
  return stmt_table_privileges(
      stmt,
      CatalogName, NameLength1,
      SchemaName, NameLength2,
      TableName, NameLength3);
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
  if (ConnectionHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  conn_t *conn = (conn_t*)ConnectionHandle;

  conn_clr_errs(conn);
  return conn_browse_connect(
      conn,
      InConnectionString, StringLength1,
      OutConnectionString, BufferLength, StringLength2Ptr);
}

SQLRETURN SQL_API SQLCompleteAsync(
    SQLSMALLINT  HandleType,
    SQLHANDLE    Handle,
    RETCODE     *AsyncRetCodePtr)
{
  OOW("===");
  if (Handle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  switch (HandleType) {
    case SQL_HANDLE_DBC:
      conn_clr_errs((conn_t*)Handle);
      return conn_complete_async((conn_t*)Handle, AsyncRetCodePtr);
    case SQL_HANDLE_STMT:
      stmt_clr_errs((stmt_t*)Handle);
      return stmt_complete_async((stmt_t*)Handle, AsyncRetCodePtr);
    default:
      OA_NIY(0);
      return SQL_ERROR;
  }
}

