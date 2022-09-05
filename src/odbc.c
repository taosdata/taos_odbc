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
    SQLHANDLE Handle)
{
  env_t *env = (env_t*)Handle;
  env_unref(env);
  return SQL_SUCCESS;
}

static SQLRETURN do_free_conn(
    SQLHANDLE Handle)
{
  conn_t *conn = (conn_t*)Handle;
  conn_unref(conn);
  return SQL_SUCCESS;
}

static SQLRETURN do_free_stmt(
    SQLHANDLE Handle)
{
  stmt_t *stmt = (stmt_t*)Handle;
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
      return do_free_env(Handle);
    case SQL_HANDLE_DBC:
      return do_free_conn(Handle);
    case SQL_HANDLE_STMT:
      return do_free_stmt(Handle);
    default:
      return SQL_ERROR;
  }
}

static SQLRETURN do_driver_connect(
    SQLHDBC         ConnectionHandle,
    SQLCHAR        *InConnectionString,
    SQLSMALLINT     StringLength1,
    SQLCHAR        *OutConnectionString,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLength2Ptr)
{
  parser_param_t param = {};
  // param.debug_flex = 1;
  // param.debug_bison = 1;

  int r = parser_parse((const char*)InConnectionString, StringLength1, &param);
  do {
    if (r) break;

    conn_t *conn = (conn_t*)ConnectionHandle;
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
      return do_driver_connect(ConnectionHandle, InConnectionString, StringLength1, OutConnectionString, BufferLength, StringLength2Ptr);
    default:
      return SQL_ERROR;
  }
}

static SQLRETURN do_disconnect(
    SQLHDBC ConnectionHandle)
{
  conn_t *conn = (conn_t*)ConnectionHandle;
  conn_disconnect(conn);

  return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLDisconnect(
    SQLHDBC ConnectionHandle)
{
  OA_DM(ConnectionHandle);
  return do_disconnect(ConnectionHandle);
}

static SQLRETURN do_exec_direct(
    SQLHSTMT     StatementHandle,
    SQLCHAR     *StatementText,
    SQLINTEGER   TextLength)
{
  stmt_t *stmt = (stmt_t*)StatementHandle;
  int r = stmt_exec_direct(stmt, (const char*)StatementText, TextLength);
  return r ? SQL_ERROR : SQL_SUCCESS;
}

SQLRETURN SQL_API SQLExecDirect(
    SQLHSTMT     StatementHandle,
    SQLCHAR     *StatementText,
    SQLINTEGER   TextLength)
{
  OA_DM(StatementHandle);
  return do_exec_direct(StatementHandle, StatementText, TextLength);
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

  return SQL_SUCCESS;
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
    SQLHDBC         ConnectionHandle,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  conn_t *conn = (conn_t*)ConnectionHandle;
  (void)InfoType;
  (void)InfoValuePtr;
  (void)BufferLength;
  (void)StringLengthPtr;
  switch (InfoType) {
    case SQL_DBMS_NAME:
      return do_conn_get_info_dbms_name(conn, (char*)InfoValuePtr, (size_t)BufferLength, StringLengthPtr);
    case SQL_DRIVER_NAME:
      return do_conn_get_info_driver_name(conn, (char*)InfoValuePtr, (size_t)BufferLength, StringLengthPtr);
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
  return do_conn_get_info(ConnectionHandle, InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
}

