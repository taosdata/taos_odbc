#include "conn.h"
#include "desc.h"
#include "env.h"
#include "internal.h"
#include "log.h"
#include "stmt.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sql.h>
#include <sqlext.h>
#include <sqlucode.h>

static atomic_int         nr_load = 0;

int get_nr_load(void)
{
  return atomic_load(&nr_load);
}

__attribute__((constructor)) void _initialize(void)
{
  // yes, no check return value
  atomic_fetch_add(&nr_load, 1);
}

__attribute__((destructor)) void _deinitialize(void)
{
  // yes, no check return value
  atomic_fetch_sub(&nr_load, 1);
}

static SQLRETURN do_alloc_env(
    SQLHANDLE *OutputHandle)
{
  *OutputHandle = SQL_NULL_HANDLE;

  env_t *env = env_create();
  if (!env) return SQL_ERROR;

  *OutputHandle = (SQLHANDLE)env;
  return SQL_SUCCESS;
}

static SQLRETURN do_alloc_conn(
    SQLHANDLE InputHandle,
    SQLHANDLE *OutputHandle)
{
  *OutputHandle = SQL_NULL_HANDLE;

  env_t *env = (env_t*)InputHandle;
  conn_t *conn = conn_create(env);
  if (conn == NULL) return SQL_ERROR;

  *OutputHandle = (SQLHANDLE)conn;
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLAllocHandle(
    SQLSMALLINT HandleType,
    SQLHANDLE   InputHandle,
    SQLHANDLE  *OutputHandle)
{
  switch (HandleType) {
    case SQL_HANDLE_ENV:
      return do_alloc_env(OutputHandle);
    case SQL_HANDLE_DBC:
      if (InputHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

      errs_clr(&((env_t*)InputHandle)->errs);
      if (!OutputHandle) return SQL_INVALID_HANDLE;
      return do_alloc_conn(InputHandle, OutputHandle);
    case SQL_HANDLE_STMT:
      if (InputHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

      errs_clr(&((conn_t*)InputHandle)->errs);

      if (!OutputHandle) return SQL_INVALID_HANDLE;
      return conn_alloc_stmt((conn_t*)InputHandle, OutputHandle);
    case SQL_HANDLE_DESC:
      if (InputHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

      errs_clr(&((conn_t*)InputHandle)->errs);

      if (!OutputHandle) return SQL_INVALID_HANDLE;
      return conn_alloc_desc((conn_t*)InputHandle, OutputHandle);
    default:
      if (InputHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

      return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLFreeHandle(
    SQLSMALLINT HandleType,
    SQLHANDLE   Handle)
{
  if (Handle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  switch (HandleType) {
    case SQL_HANDLE_ENV:
      errs_clr(&((env_t*)Handle)->errs);
      return env_free((env_t*)Handle);
    case SQL_HANDLE_DBC:
      errs_clr(&((conn_t*)Handle)->errs);
      return conn_free((conn_t*)Handle);
    case SQL_HANDLE_STMT:
      errs_clr(&((stmt_t*)Handle)->errs);
      return stmt_free((stmt_t*)Handle);
    case SQL_HANDLE_DESC:
      return desc_free((desc_t*)Handle);
    default:
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
  if (ConnectionHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  errs_clr(&((conn_t*)ConnectionHandle)->errs);

  OA_DM(InConnectionString);

  switch (DriverCompletion) {
    case SQL_DRIVER_NOPROMPT:
      return conn_driver_connect((conn_t*)ConnectionHandle, WindowHandle, InConnectionString, StringLength1, OutConnectionString, BufferLength, StringLength2Ptr);
    default:
      return SQL_ERROR;
  }
}

static SQLRETURN do_disconnect(
    conn_t *conn)
{
  conn_disconnect(conn);

  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLDisconnect(
    SQLHDBC ConnectionHandle)
{
  if (ConnectionHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  errs_clr(&((conn_t*)ConnectionHandle)->errs);

  return do_disconnect((conn_t*)ConnectionHandle);
}

static SQLRETURN do_exec_direct(
    stmt_t      *stmt,
    SQLCHAR     *StatementText,
    SQLINTEGER   TextLength)
{
  return stmt_exec_direct(stmt, (const char*)StatementText, TextLength);
}

SQLRETURN SQL_API SQLExecDirect(
    SQLHSTMT     StatementHandle,
    SQLCHAR     *StatementText,
    SQLINTEGER   TextLength)
{
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  errs_clr(&((stmt_t*)StatementHandle)->errs);

  return do_exec_direct((stmt_t*)StatementHandle, StatementText, TextLength);
}

static SQLRETURN do_env_set_odbc_version(
    env_t       *env,
    SQLINTEGER   odbc_version)
{
  (void)env;
  switch (odbc_version) {
    case SQL_OV_ODBC3:
      return SQL_SUCCESS;
    case SQL_OV_ODBC3_80:
      return SQL_ERROR;
    case SQL_OV_ODBC2:
      return SQL_ERROR;
    default:
      return SQL_ERROR;
  }
}

static SQLRETURN do_env_set_attr(
    SQLHENV      EnvironmentHandle,
    SQLINTEGER   Attribute,
    SQLPOINTER   ValuePtr,
    SQLINTEGER   StringLength)
{
  (void)StringLength;
  env_t *env = (env_t*)EnvironmentHandle;
  switch (Attribute) {
    case SQL_ATTR_CONNECTION_POOLING:
      return SQL_ERROR;

    case SQL_ATTR_CP_MATCH:
      return SQL_ERROR;

    case SQL_ATTR_ODBC_VERSION:
      return do_env_set_odbc_version(env, (SQLINTEGER)(size_t)ValuePtr);

    case SQL_ATTR_OUTPUT_NTS:
      return SQL_ERROR;

    default:
      return SQL_ERROR;
  }
}

SQLRETURN SQLSetEnvAttr(
    SQLHENV      EnvironmentHandle,
    SQLINTEGER   Attribute,
    SQLPOINTER   ValuePtr,
    SQLINTEGER   StringLength)
{
  return do_env_set_attr(EnvironmentHandle, Attribute, ValuePtr, StringLength);
}

static SQLRETURN do_conn_get_info_dbms_name(
    conn_t         *conn,
    char           *buf,
    size_t          sz,
    SQLSMALLINT    *StringLengthPtr)
{
  const char *name;
  int r = conn_get_dbms_name(conn, &name);
  if (r) return SQL_ERROR;

  int n = snprintf(buf, sz, "%s", name);
  *StringLengthPtr = n;

  return SQL_SUCCESS;
}

static SQLRETURN do_conn_get_info_driver_name(
    conn_t         *conn,
    char           *buf,
    size_t          sz,
    SQLSMALLINT    *StringLengthPtr)
{
  const char *name;
  int r = conn_get_driver_name(conn, &name);
  if (r) return SQL_ERROR;

  int n = snprintf(buf, sz, "%s", name);
  *StringLengthPtr = n;

  return SQL_SUCCESS;
}

static SQLRETURN do_conn_get_info(
    conn_t         *conn,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  (void)InfoType;
  (void)InfoValuePtr;
  (void)BufferLength;
  (void)StringLengthPtr;
  switch (InfoType) {
    case SQL_DBMS_NAME:
      return do_conn_get_info_dbms_name(conn, (char*)InfoValuePtr, (size_t)BufferLength, StringLengthPtr);
    case SQL_DRIVER_NAME:
      return do_conn_get_info_driver_name(conn, (char*)InfoValuePtr, (size_t)BufferLength, StringLengthPtr);
    case SQL_CURSOR_COMMIT_BEHAVIOR:
      return SQL_ERROR;
    default:
      return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLGetInfo(
    SQLHDBC         ConnectionHandle,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  if (ConnectionHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  errs_clr(&((conn_t*)ConnectionHandle)->errs);

  return do_conn_get_info((conn_t*)ConnectionHandle, InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
}

static SQLRETURN do_env_end_tran(
    env_t       *env,
    SQLSMALLINT   CompletionType)
{
  switch (CompletionType) {
    case SQL_COMMIT:
      return SQL_SUCCESS;
    case SQL_ROLLBACK:
      if (env_rollback(env)) return SQL_ERROR;
      return SQL_SUCCESS;
    default:
      return SQL_ERROR;
  }
}

static SQLRETURN do_conn_end_tran(
    conn_t       *conn,
    SQLSMALLINT   CompletionType)
{
  switch (CompletionType) {
    case SQL_COMMIT:
      OA_NIY(0);
      return SQL_SUCCESS;
    case SQL_ROLLBACK:
      if (conn_rollback(conn)) return SQL_ERROR;
      return SQL_SUCCESS;
    default:
      return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLEndTran(
    SQLSMALLINT   HandleType,
    SQLHANDLE     Handle,
    SQLSMALLINT   CompletionType)
{
  if (Handle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  switch (HandleType) {
    case SQL_HANDLE_ENV:
      errs_clr(&((env_t*)Handle)->errs);
      return do_env_end_tran((env_t*)Handle, CompletionType);
    case SQL_HANDLE_DBC:
      errs_clr(&((conn_t*)Handle)->errs);
      return do_conn_end_tran((conn_t*)Handle, CompletionType);
    default:
      return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLSetConnectAttr(
    SQLHDBC       ConnectionHandle,
    SQLINTEGER    Attribute,
    SQLPOINTER    ValuePtr,
    SQLINTEGER    StringLength)
{
  if (ConnectionHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  errs_clr(&((conn_t*)ConnectionHandle)->errs);

  (void)ValuePtr;
  (void)StringLength;

  switch (Attribute) {
    case SQL_ATTR_CONNECTION_TIMEOUT:
    case SQL_ATTR_LOGIN_TIMEOUT:
      return SQL_SUCCESS;
    default:
      return SQL_ERROR;
  }
}

static SQLRETURN do_stmt_set_cursor_type(
    stmt_t       *stmt,
    SQLULEN       cursor_type)
{
  (void)stmt;
  switch (cursor_type) {
    case SQL_CURSOR_FORWARD_ONLY:
      return SQL_SUCCESS;
    case SQL_CURSOR_STATIC:
      return SQL_SUCCESS;
    case SQL_CURSOR_KEYSET_DRIVEN:
      OD("");
      return SQL_ERROR;
    case SQL_CURSOR_DYNAMIC:
      OD("");
      return SQL_ERROR;
    default:
      OD("");
      return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLSetStmtAttr(
    SQLHSTMT      StatementHandle,
    SQLINTEGER    Attribute,
    SQLPOINTER    ValuePtr,
    SQLINTEGER    StringLength)
{
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  stmt_t *stmt = (stmt_t*)StatementHandle;

  errs_clr(&stmt->errs);

  (void)StringLength;

  switch (Attribute) {
    case SQL_ATTR_CURSOR_TYPE:
      return do_stmt_set_cursor_type(stmt, (SQLULEN)ValuePtr);
    case SQL_ATTR_ROW_ARRAY_SIZE:
      return stmt_set_row_array_size(stmt, (SQLULEN)ValuePtr);
    case SQL_ATTR_ROW_STATUS_PTR:
      return stmt_set_row_status_ptr(stmt, (SQLUSMALLINT*)ValuePtr);
    case SQL_ATTR_ROW_BIND_TYPE:
      return stmt_set_row_bind_type(stmt, (SQLULEN)ValuePtr);
    case SQL_ATTR_ROWS_FETCHED_PTR:
      return stmt_set_rows_fetched_ptr(stmt, (SQLULEN*)ValuePtr);
    case SQL_ATTR_MAX_LENGTH:
      return stmt_set_max_length(stmt, (SQLULEN)ValuePtr);
    case SQL_ATTR_ROW_BIND_OFFSET_PTR:
      return stmt_set_row_bind_offset_ptr(stmt, (SQLULEN*)ValuePtr);
    case SQL_ATTR_APP_ROW_DESC:
      return stmt_set_row_desc(stmt, ValuePtr);
    case SQL_ATTR_APP_PARAM_DESC:
      return stmt_set_param_desc(stmt, ValuePtr);
    default:
      return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLRowCount(
    SQLHSTMT   StatementHandle,
    SQLLEN    *RowCountPtr)
{
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  errs_clr(&((stmt_t*)StatementHandle)->errs);

  if (stmt_get_row_count((stmt_t*)StatementHandle, RowCountPtr)) return SQL_ERROR;
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLNumResultCols(
     SQLHSTMT        StatementHandle,
     SQLSMALLINT    *ColumnCountPtr)
{
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  errs_clr(&((stmt_t*)StatementHandle)->errs);

  if (stmt_get_col_count((stmt_t*)StatementHandle, ColumnCountPtr)) return SQL_ERROR;
  return SQL_SUCCESS;
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
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  errs_clr(&((stmt_t*)StatementHandle)->errs);

  return stmt_describe_col((stmt_t*)StatementHandle,
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
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  errs_clr(&((stmt_t*)StatementHandle)->errs);

  return stmt_bind_col((stmt_t*)StatementHandle,
      ColumnNumber,
      TargetType,
      TargetValuePtr,
      BufferLength,
      StrLen_or_IndPtr);
}

SQLRETURN SQL_API SQLFetch(
    SQLHSTMT     StatementHandle)
{
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  errs_clr(&((stmt_t*)StatementHandle)->errs);

  return stmt_fetch((stmt_t*)StatementHandle);
}

SQLRETURN SQLFreeStmt(
    SQLHSTMT       StatementHandle,
    SQLUSMALLINT   Option)
{
  switch (Option) {
    case SQL_CLOSE:
      if (stmt_close_cursor((stmt_t*)StatementHandle)) return SQL_ERROR;
      return SQL_SUCCESS;
    case SQL_UNBIND:
      return stmt_unbind_cols((stmt_t*)StatementHandle);
    case SQL_RESET_PARAMS:
      return stmt_reset_params((stmt_t*)StatementHandle);
    default:
      stmt_append_err((stmt_t*)StatementHandle, "HY000", 0, "only `SQL_CLOSE` is supported now");
      return SQL_ERROR;
  }
}

static SQLRETURN do_env_get_diag_rec(
    env_t          *env,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr)
{
  return env_get_diag_rec(env, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
}

static SQLRETURN do_conn_get_diag_rec(
    conn_t         *conn,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr)
{
  (void)conn;
  (void)RecNumber;
  (void)SQLState;
  (void)NativeErrorPtr;
  (void)MessageText;
  (void)BufferLength;
  (void)TextLengthPtr;
  return conn_get_diag_rec(conn, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
}

static SQLRETURN do_stmt_get_diag_rec(
    stmt_t         *stmt,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr)
{
  (void)stmt;
  (void)RecNumber;
  (void)SQLState;
  (void)NativeErrorPtr;
  (void)MessageText;
  (void)BufferLength;
  (void)TextLengthPtr;
  return stmt_get_diag_rec(stmt, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
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
  if (Handle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  switch (HandleType) {
    case SQL_HANDLE_ENV:
      return do_env_get_diag_rec((env_t*)Handle, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
    case SQL_HANDLE_DBC:
      return do_conn_get_diag_rec((conn_t*)Handle, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
    case SQL_HANDLE_STMT:
      return do_stmt_get_diag_rec((stmt_t*)Handle, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
    default:
      OA_NIY(0);
      return SQL_ERROR;
  }

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
  if (Handle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  (void)HandleType;
  (void)RecNumber;
  (void)DiagIdentifier;
  (void)DiagInfoPtr;
  (void)BufferLength;
  (void)StringLengthPtr;
  OA_NIY(0);
}

SQLRETURN SQL_API SQLGetData(
    SQLHSTMT       StatementHandle,
    SQLUSMALLINT   Col_or_Param_Num,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN        *StrLen_or_IndPtr)
{
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  errs_clr(&((stmt_t*)StatementHandle)->errs);

  return stmt_get_data((stmt_t*)StatementHandle, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, StrLen_or_IndPtr);
}

SQLRETURN SQL_API SQLPrepare(
    SQLHSTMT      StatementHandle,
    SQLCHAR      *StatementText,
    SQLINTEGER    TextLength)
{
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  errs_clr(&((stmt_t*)StatementHandle)->errs);

  if (TextLength == SQL_NTS) TextLength = strlen((const char*)StatementText);
  return stmt_prepare((stmt_t*)StatementHandle, (const char*)StatementText, (size_t)TextLength);
}

SQLRETURN SQL_API SQLNumParams(
    SQLHSTMT        StatementHandle,
    SQLSMALLINT    *ParameterCountPtr)
{
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  errs_clr(&((stmt_t*)StatementHandle)->errs);

  return stmt_get_num_params((stmt_t*)StatementHandle, ParameterCountPtr);
}

SQLRETURN SQL_API SQLDescribeParam(
    SQLHSTMT        StatementHandle,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT    *DataTypePtr,
    SQLULEN        *ParameterSizePtr,
    SQLSMALLINT    *DecimalDigitsPtr,
    SQLSMALLINT    *NullablePtr)
{
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  errs_clr(&((stmt_t*)StatementHandle)->errs);

  return stmt_describe_param(
      (stmt_t*)StatementHandle,
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
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  errs_clr(&((stmt_t*)StatementHandle)->errs);

  return stmt_bind_param((stmt_t*)StatementHandle,
    ParameterNumber,
    InputOutputType,
    ValueType,
    ParameterType,
    ColumnSize,
    DecimalDigits,
    ParameterValuePtr,
    BufferLength,
    StrLen_or_IndPtr);
  (void)StatementHandle;
  (void)ParameterNumber;
  (void)InputOutputType;
  (void)ValueType;
  (void)ParameterType;
  (void)ColumnSize;
  (void)DecimalDigits;
  (void)ParameterValuePtr;
  (void)BufferLength;
  (void)StrLen_or_IndPtr;
}

SQLRETURN SQL_API SQLExecute(
    SQLHSTMT     StatementHandle)
{
  if (StatementHandle == SQL_NULL_HANDLE) return SQL_INVALID_HANDLE;

  errs_clr(&((stmt_t*)StatementHandle)->errs);

  return stmt_execute((stmt_t*)StatementHandle);
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
  if (NameLength1 == SQL_NTS) NameLength1 = strlen((const char*)ServerName);
  if (UserName       && NameLength2 == SQL_NTS) NameLength2 = strlen((const char*)UserName);
  if (Authentication && NameLength3 == SQL_NTS) NameLength3 = strlen((const char*)Authentication);

  return conn_connect(
      (conn_t*)ConnectionHandle,
      ServerName, NameLength1,
      UserName, NameLength2,
      Authentication, NameLength3);
}

