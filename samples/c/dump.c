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

#include "odbc_helpers.h"

#include <stdarg.h>
#include <stdint.h>


#define DUMP(fmt, ...)          printf(fmt "\n", ##__VA_ARGS__)

typedef int (*dump_f)(SQLSMALLINT HandleType, SQLHANDLE Handle);

static int execute_sqls(SQLHANDLE hstmt, const char **sqls, size_t nr)
{
  SQLRETURN sr = SQL_SUCCESS;

  for (size_t i=0; i<nr; ++i) {
    const char *sql = sqls[i];
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;
    CALL_SQLCloseCursor(hstmt);
  }

  return 0;
}

static int _dump_col_info(SQLHANDLE hstmt, SQLUSMALLINT ColumnNumber)
{
  SQLRETURN sr = SQL_SUCCESS;

  SQLUSMALLINT    FieldIdentifier;
  SQLPOINTER      CharacterAttributePtr;
  SQLSMALLINT     BufferLength;
  SQLSMALLINT     StringLength;
  SQLLEN          NumericAttribute;

  char buf[4096];

  FieldIdentifier             = SQL_DESC_AUTO_UNIQUE_VALUE;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), !!NumericAttribute ? "SQL_TRUE" : "SQL_FALSE");

  FieldIdentifier             = SQL_DESC_BASE_COLUMN_NAME;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), buf);

  FieldIdentifier             = SQL_DESC_BASE_TABLE_NAME;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), buf);

  FieldIdentifier             = SQL_DESC_CASE_SENSITIVE;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), !!NumericAttribute ? "SQL_TRUE" : "SQL_FALSE");

  FieldIdentifier             = SQL_DESC_CATALOG_NAME;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), buf);

  FieldIdentifier             = SQL_DESC_CONCISE_TYPE;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), sql_data_type((SQLSMALLINT)NumericAttribute));

  FieldIdentifier             = SQL_DESC_COUNT;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%" PRId64 "",ColumnNumber, sql_col_attribute(FieldIdentifier), (int64_t)NumericAttribute);

  FieldIdentifier             = SQL_DESC_DISPLAY_SIZE;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%" PRId64 "",ColumnNumber, sql_col_attribute(FieldIdentifier), (int64_t)NumericAttribute);

  FieldIdentifier             = SQL_DESC_FIXED_PREC_SCALE;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%" PRId64 "",ColumnNumber, sql_col_attribute(FieldIdentifier), (int64_t)NumericAttribute);

  FieldIdentifier             = SQL_DESC_LABEL;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), buf);

  FieldIdentifier             = SQL_DESC_LENGTH;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%" PRId64 "",ColumnNumber, sql_col_attribute(FieldIdentifier), (int64_t)NumericAttribute);

  FieldIdentifier             = SQL_DESC_LITERAL_PREFIX;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), buf);

  FieldIdentifier             = SQL_DESC_LITERAL_SUFFIX;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), buf);

  FieldIdentifier             = SQL_DESC_LOCAL_TYPE_NAME;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), buf);

  FieldIdentifier             = SQL_DESC_NAME;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), buf);

  FieldIdentifier             = SQL_DESC_NULLABLE;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), sql_nullable((SQLSMALLINT)NumericAttribute));

  FieldIdentifier             = SQL_DESC_NUM_PREC_RADIX;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%" PRId64 "",ColumnNumber, sql_col_attribute(FieldIdentifier), (int64_t)NumericAttribute);

  FieldIdentifier             = SQL_DESC_OCTET_LENGTH;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%" PRId64 "",ColumnNumber, sql_col_attribute(FieldIdentifier), (int64_t)NumericAttribute);

  FieldIdentifier             = SQL_DESC_PRECISION;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%" PRId64 "",ColumnNumber, sql_col_attribute(FieldIdentifier), (int64_t)NumericAttribute);

  FieldIdentifier             = SQL_DESC_SCALE;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%" PRId64 "",ColumnNumber, sql_col_attribute(FieldIdentifier), (int64_t)NumericAttribute);

  FieldIdentifier             = SQL_DESC_SCHEMA_NAME;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), buf);

  FieldIdentifier             = SQL_DESC_SEARCHABLE;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), sql_searchable((SQLSMALLINT)NumericAttribute));

  FieldIdentifier             = SQL_DESC_TABLE_NAME;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), buf);

  FieldIdentifier             = SQL_DESC_TYPE;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), sql_data_type((SQLSMALLINT)NumericAttribute));

  FieldIdentifier             = SQL_DESC_TYPE_NAME;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), buf);

  FieldIdentifier             = SQL_DESC_UNNAMED;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%" PRId64 "",ColumnNumber, sql_col_attribute(FieldIdentifier), (int64_t)NumericAttribute);

  FieldIdentifier             = SQL_DESC_UNSIGNED;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), !!NumericAttribute ? "SQL_TRUE" : "SQL_FALSE");

  FieldIdentifier             = SQL_DESC_UPDATABLE;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), sql_updatable((SQLSMALLINT)NumericAttribute));

  return 0;
}

static int _dump_stmt_col_info(SQLSMALLINT HandleType, SQLHANDLE Handle)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  if (HandleType != SQL_HANDLE_STMT) return 0;

  DUMP("");
  DUMP("%s:", __func__);

  SQLHANDLE hstmt = Handle;

  const char *sqls[] = {
    "drop database if exists bar",
    "create database bar",
    "use bar",
    "create table x (ts timestamp, f real, d float)",
  };

  r = execute_sqls(hstmt, sqls, sizeof(sqls)/sizeof(sqls[0]));
  if (r) return -1;

  const char *sql = "select * from x";
  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  SQLSMALLINT ColumnCount;
  sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
  if (sr != SQL_SUCCESS) return -1;

  for (SQLSMALLINT i = 0; i<ColumnCount; ++i) {
    r = _dump_col_info(hstmt, (SQLUSMALLINT)i+1);
    if (r) return -1;
  }

  return 0;
}

static const dump_f _dumps[] = {
  _dump_stmt_col_info,
};

typedef struct conn_arg_s             conn_arg_t;
struct conn_arg_s {
  const char      *dsn;
  const char      *uid;
  const char      *pwd;
  const char      *connstr;
};

static int _connect(SQLHANDLE hconn, const char *dsn, const char *uid, const char *pwd)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLConnect(hconn, (SQLCHAR*)dsn, SQL_NTS, (SQLCHAR*)uid, SQL_NTS, (SQLCHAR*)pwd, SQL_NTS);
  if (FAILED(sr)) {
    E("connect [dsn:%s,uid:%s,pwd:%s] failed", dsn, uid, pwd);
    return -1;
  }

  return 0;
}

static int _driver_connect(SQLHANDLE hconn, const char *connstr)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLDriverConnect(hconn, NULL, (SQLCHAR*)connstr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
  if (FAILED(sr)) {
    E("driver_connect [connstr:%s] failed", connstr);
    return -1;
  }

  return 0;
}

static int dump_with_stmt(SQLHANDLE hstmt)
{
  int r = 0;

  for (size_t i=0; i<sizeof(_dumps)/sizeof(_dumps[0]); ++i) {
    r = _dumps[i](SQL_HANDLE_STMT, hstmt);
    if (r) return -1;
  }

  return 0;
}

static int dump_with_connected_conn(SQLHANDLE hconn)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  for (size_t i=0; i<sizeof(_dumps)/sizeof(_dumps[0]); ++i) {
    r = _dumps[i](SQL_HANDLE_DBC, hconn);
    if (r) return -1;
  }

  SQLHANDLE hstmt;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  r = dump_with_stmt(hstmt);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return r ? -1 : 0;
}

static int dump_with_conn(int argc, char *argv[], SQLHANDLE hconn)
{
  conn_arg_t conn_arg = {0};

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "--dsn") == 0) {
      ++i;
      if (i>=argc) break;
      conn_arg.dsn = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--uid") == 0) {
      ++i;
      if (i>=argc) break;
      conn_arg.uid = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--pwd") == 0) {
      ++i;
      if (i>=argc) break;
      conn_arg.pwd = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--connstr") == 0) {
      ++i;
      if (i>=argc) break;
      conn_arg.connstr = argv[i];
      continue;
    }
  }

  int r = 0;

  if (conn_arg.connstr) {
    r = _driver_connect(hconn, conn_arg.connstr);
    if (r) return -1;
  } else {
    r = _connect(hconn, conn_arg.dsn, conn_arg.uid, conn_arg.pwd);
    if (r) return -1;
  }

  r = dump_with_connected_conn(hconn);

  CALL_SQLDisconnect(hconn);

  return r ? -1 : 0;
}

static int dump_with_env(int argc, char *argv[], SQLHANDLE henv)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hconn = SQL_NULL_HANDLE;
  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, henv, &hconn);
  if (FAILED(sr)) return -1;

  r = dump_with_conn(argc, argv, hconn);

  CALL_SQLFreeHandle(SQL_HANDLE_DBC, hconn);

  return r;
}

static int dump(int argc, char *argv[])
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE henv  = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  if (FAILED(sr)) return 1;

  do {
    sr = CALL_SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (FAILED(sr)) { r = -1; break; }

    r = dump_with_env(argc, argv, henv);
    if (r) break;
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_ENV, henv);

  return r;
}

int main(int argc, char *argv[])
{
  int r = 0;
  r = dump(argc, argv);
  fprintf(stderr, "==%s==\n", r ? "failure" : "success");
  return !!r;
}
