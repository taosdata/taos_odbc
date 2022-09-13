#include "conn.h"
#include "env.h"
#include "log.h"
#include "parser.h"
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

static SQLRETURN do_alloc_stmt(
    SQLHANDLE InputHandle,
    SQLHANDLE *OutputHandle)
{
  *OutputHandle = SQL_NULL_HANDLE;

  conn_t *conn = (conn_t*)InputHandle;
  stmt_t *stmt = stmt_create(conn);
  if (stmt == NULL) return SQL_ERROR;

  *OutputHandle = (SQLHANDLE)stmt;
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLAllocHandle(
    SQLSMALLINT HandleType,
    SQLHANDLE   InputHandle,
    SQLHANDLE  *OutputHandle)
{
  OA_DM(OutputHandle);

  switch (HandleType) {
    case SQL_HANDLE_ENV:
      OA_DM(InputHandle == SQL_NULL_HANDLE);
      return do_alloc_env(OutputHandle);
    case SQL_HANDLE_DBC:
      OA_DM(InputHandle);
      return do_alloc_conn(InputHandle, OutputHandle);
    case SQL_HANDLE_STMT:
      OA_DM(InputHandle);
      return do_alloc_stmt(InputHandle, OutputHandle);
    default:
      return SQL_ERROR;
  }
}

static SQLRETURN do_free_env(
    env_t *env)
{
  env_unref(env);
  return SQL_SUCCESS;
}

static SQLRETURN do_free_conn(
    conn_t *conn)
{
  conn_unref(conn);
  return SQL_SUCCESS;
}

static SQLRETURN do_free_stmt(
    stmt_t *stmt)
{
  stmt_unref(stmt);
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLFreeHandle(
    SQLSMALLINT HandleType,
    SQLHANDLE   Handle)
{
  OA_DM(Handle);

  switch (HandleType) {
    case SQL_HANDLE_ENV:
      return do_free_env((env_t*)Handle);
    case SQL_HANDLE_DBC:
      return do_free_conn((conn_t*)Handle);
    case SQL_HANDLE_STMT:
      return do_free_stmt((stmt_t*)Handle);
    default:
      return SQL_ERROR;
  }
}

static SQLRETURN do_driver_connect(
    conn_t         *conn,
    SQLCHAR        *InConnectionString,
    SQLSMALLINT     StringLength1,
    SQLCHAR        *OutConnectionString,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLength2Ptr)
{
  parser_param_t param = {};
  // param.debug_flex = 1;
  // param.debug_bison = 1;

  if (StringLength1 == SQL_NTS) StringLength1 = strlen((const char*)InConnectionString);
  int r = parser_parse((const char*)InConnectionString, StringLength1, &param);
  do {
    if (r) break;

    r = conn_connect(conn, &param.conn_str);

    if (r) break;

    if (OutConnectionString) {
      int n;
      if (StringLength1 > BufferLength) {
        n = BufferLength;
      } else {
        n = StringLength1;
      }

      strncpy((char*)OutConnectionString, (const char*)InConnectionString, n);
      if (n<BufferLength) {
        OutConnectionString[n] = '\0';
      } else if (BufferLength>0) {
        OutConnectionString[BufferLength-1] = '\0';
      }
      if (StringLength2Ptr) {
        *StringLength2Ptr = strlen((const char*)OutConnectionString);
      }
    } else {
      if (StringLength2Ptr) {
        *StringLength2Ptr = 0;
      }
    }

    parser_param_release(&param);
    return SQL_SUCCESS;
  } while (0);

  parser_param_release(&param);
  return SQL_ERROR;
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
  OA_DM(ConnectionHandle);
  OA_DM(InConnectionString);
  OA_NIY(WindowHandle == NULL);
  switch (DriverCompletion) {
    case SQL_DRIVER_NOPROMPT:
      return do_driver_connect((conn_t*)ConnectionHandle, InConnectionString, StringLength1, OutConnectionString, BufferLength, StringLength2Ptr);
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
  OA_DM(ConnectionHandle);
  return do_disconnect((conn_t*)ConnectionHandle);
}

static SQLRETURN do_exec_direct(
    stmt_t      *stmt,
    SQLCHAR     *StatementText,
    SQLINTEGER   TextLength)
{
  int r = stmt_exec_direct(stmt, (const char*)StatementText, TextLength);
  return r ? SQL_ERROR : SQL_SUCCESS;
}

SQLRETURN SQL_API SQLExecDirect(
    SQLHSTMT     StatementHandle,
    SQLCHAR     *StatementText,
    SQLINTEGER   TextLength)
{
  OA_DM(StatementHandle);
  return do_exec_direct((stmt_t*)StatementHandle, StatementText, TextLength);
}

static SQLRETURN do_env_set_odbc_version(
    env_t       *env,
    int32_t      odbc_version)
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
      return do_env_set_odbc_version(env, (int32_t)(size_t)ValuePtr);

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
  switch (HandleType) {
    case SQL_HANDLE_ENV:
      return do_env_end_tran((env_t*)Handle, CompletionType);
    case SQL_HANDLE_DBC:
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
  (void)ConnectionHandle;
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

static SQLRETURN do_stmt_set_row_array_size(
    stmt_t       *stmt,
    SQLULEN       row_array_size)
{
  if (stmt_set_row_array_size(stmt, row_array_size)) return SQL_ERROR;
  return SQL_SUCCESS;
}

static SQLRETURN do_stmt_set_row_status_ptr(
    stmt_t       *stmt,
    SQLUSMALLINT *row_status_ptr)
{
  if (stmt_set_row_status_ptr(stmt, row_status_ptr)) return SQL_ERROR;
  return SQL_SUCCESS;
}

static SQLRETURN do_stmt_set_row_bind_type(
    stmt_t       *stmt,
    SQLULEN       row_bind_type)
{
  OA_NIY(row_bind_type == SQL_BIND_BY_COLUMN);
  if (stmt_set_row_bind_type(stmt, row_bind_type)) return SQL_ERROR;
  return SQL_SUCCESS;
}

static SQLRETURN do_stmt_set_rows_fetched_ptr(
    stmt_t       *stmt,
    SQLULEN      *rows_fetched_ptr)
{
  if (stmt_set_rows_fetched_ptr(stmt, rows_fetched_ptr)) return SQL_ERROR;
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetStmtAttr(
    SQLHSTMT      StatementHandle,
    SQLINTEGER    Attribute,
    SQLPOINTER    ValuePtr,
    SQLINTEGER    StringLength)
{
  (void)StringLength;
  switch (Attribute) {
    case SQL_ATTR_CURSOR_TYPE:
      return do_stmt_set_cursor_type((stmt_t*)StatementHandle, (SQLULEN)ValuePtr);
    case SQL_ATTR_ROW_ARRAY_SIZE:
      return do_stmt_set_row_array_size((stmt_t*)StatementHandle, (SQLULEN)ValuePtr);
    case SQL_ATTR_ROW_STATUS_PTR:
      return do_stmt_set_row_status_ptr((stmt_t*)StatementHandle, (SQLUSMALLINT*)ValuePtr);
    case SQL_ATTR_ROW_BIND_TYPE:
      return do_stmt_set_row_bind_type((stmt_t*)StatementHandle, (SQLULEN)ValuePtr);
    case SQL_ATTR_ROWS_FETCHED_PTR:
      return do_stmt_set_rows_fetched_ptr((stmt_t*)StatementHandle, (SQLULEN*)ValuePtr);
    default:
      return SQL_ERROR;
  }
}

SQLRETURN SQL_API SQLRowCount(
    SQLHSTMT   StatementHandle,
    SQLLEN    *RowCountPtr)
{
  if (stmt_get_row_count((stmt_t*)StatementHandle, RowCountPtr)) return SQL_ERROR;
  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLNumResultCols(
     SQLHSTMT        StatementHandle,
     SQLSMALLINT    *ColumnCountPtr)
{
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
    default:
      OA_NIY(0);
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
  (void)env;
  (void)RecNumber;
  (void)SQLState;
  (void)NativeErrorPtr;
  (void)MessageText;
  (void)BufferLength;
  (void)TextLengthPtr;
  OA_NIY(0);
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
  (void)HandleType;
  (void)Handle;
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
  return stmt_get_data((stmt_t*)StatementHandle, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, StrLen_or_IndPtr);
}

SQLRETURN SQL_API SQLPrepare(
    SQLHSTMT      StatementHandle,
    SQLCHAR      *StatementText,
    SQLINTEGER    TextLength)
{
  if (TextLength == SQL_NTS) TextLength = strlen((const char*)StatementText);
  return stmt_prepare((stmt_t*)StatementHandle, (const char*)StatementText, (size_t)TextLength);
}

SQLRETURN SQL_API SQLNumParams(
    SQLHSTMT        StatementHandle,
    SQLSMALLINT    *ParameterCountPtr)
{
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
  return stmt_execute((stmt_t*)StatementHandle);
}

