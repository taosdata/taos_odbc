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

#include "odbc_helpers.h"

#include <errno.h>
#include <stdarg.h>
#include <stdint.h>


#define DUMP(fmt, ...)          printf(fmt "\n", ##__VA_ARGS__)

typedef struct arg_s             arg_t;

struct arg_s {
  odbc_conn_arg_t  conn_arg;

  const char      *name;
};

static int _dummy(odbc_case_t *odbc_case, odbc_stage_t stage, odbc_handles_t *handles)
{
  (void)odbc_case;
  (void)stage;
  (void)handles;
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

  FieldIdentifier             = SQL_DESC_NAME;
  CharacterAttributePtr       = buf;
  BufferLength                = sizeof(buf);
  sr = CALL_SQLColAttribute(hstmt, ColumnNumber, FieldIdentifier, CharacterAttributePtr, BufferLength, &StringLength, &NumericAttribute);
  if (sr != SQL_SUCCESS) return -1;
  DUMP("Column%d.%s=%s",ColumnNumber, sql_col_attribute(FieldIdentifier), buf);

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

static int _dump_stmt_col_info(odbc_case_t *odbc_case, odbc_stage_t stage, odbc_handles_t *handles)
{
  (void)odbc_case;

  SQLHANDLE hstmt = handles->hstmt;

  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  if (stage != ODBC_STMT) return 0;

  DUMP("%s:", __func__);

  const char *env = "SAMPLE_DUMP_COL_SQL";
  const char *sql = getenv(env);
  if (!sql) {
    DUMP("env `%s` not set yet", env);
    return 0;
  }
  DUMP("env`%s`:%s", env, sql);

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

static int _dump_col(SQLHANDLE hstmt, SQLUSMALLINT ColumnNumber)
{
  SQLRETURN sr = SQL_SUCCESS;

  char buf[4096];

  SQLUSMALLINT   Col_or_Param_Num = ColumnNumber;
  SQLSMALLINT    TargetType       = SQL_C_CHAR;
  SQLPOINTER     TargetValuePtr   = buf;
  SQLLEN         BufferLength     = sizeof(buf);
  SQLLEN         StrLen_or_Ind;

  sr = CALL_SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, &StrLen_or_Ind);
  if (sr != SQL_SUCCESS) return -1;
  if (StrLen_or_Ind == SQL_NULL_DATA) {
    DUMP("Column%d:[null]", Col_or_Param_Num);
  } else {
    DUMP("Column%d:%.*s", Col_or_Param_Num, (int)StrLen_or_Ind, buf);
  }

  return 0;
}

static int _dump_stmt_tables(odbc_case_t *odbc_case, odbc_stage_t stage, odbc_handles_t *handles)
{
  (void)odbc_case;

  SQLHANDLE hstmt = handles->hstmt;

  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  if (stage != ODBC_STMT) return 0;

  int row = 0;

  DUMP("%s:", __func__);

  const char *CatalogName = NULL;
  const char *SchemaName  = NULL;
  const char *TableName   = NULL;
  const char *TableType   = NULL;

  const char *env;

  env = "CATALOG";
  CatalogName = getenv(env);
  DUMP("env `%s`:%s", env, CatalogName);

  env = "SCHEMA";
  SchemaName = getenv(env);
  DUMP("env `%s`:%s", env, SchemaName);

  env = "TABLE";
  TableName = getenv(env);
  DUMP("env `%s`:%s", env, TableName);

  env = "TYPE";
  TableType = getenv(env);
  DUMP("env `%s`:%s", env, TableType);

  sr = CALL_SQLTables(hstmt,
    (SQLCHAR*)CatalogName, CatalogName ? (SQLSMALLINT)strlen(CatalogName) : 0,
    (SQLCHAR*)SchemaName,  SchemaName ? (SQLSMALLINT)strlen(SchemaName) : 0,
    (SQLCHAR*)TableName,   TableName ? (SQLSMALLINT)strlen(TableName) : 0,
    (SQLCHAR*)TableType,   TableType ? (SQLSMALLINT)strlen(TableType) : 0);
  DUMP("sr: %s", sql_return_type(sr));
  if (FAILED(sr)) return -1;

again:
  sr = CALL_SQLFetch(hstmt);
  if (sr == SQL_NO_DATA) return 0;
  if (sr != SQL_SUCCESS) return -1;

  ++row;
  DUMP("row:%d", row);

  SQLSMALLINT ColumnCount;
  sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
  if (sr != SQL_SUCCESS) return -1;

  for (SQLSMALLINT i = 0; i<ColumnCount; ++i) {
    r = _dump_col(hstmt, (SQLUSMALLINT)i+1);
    if (r) return -1;
  }

  goto again;

  return 0;
}

static int _dump_stmt_describe_col(odbc_case_t *odbc_case, odbc_stage_t stage, odbc_handles_t *handles)
{
  (void)odbc_case;

  SQLHANDLE hstmt = handles->hstmt;

  SQLRETURN sr = SQL_SUCCESS;

  if (stage != ODBC_STMT) return 0;

  DUMP("%s:", __func__);

  const char *env = "SAMPLE_DUMP_COL_SQL";
  const char *sql = getenv(env);
  if (!sql) {
    DUMP("env `%s` not set yet", env);
    return 0;
  }
  DUMP("env`%s`:%s", env, sql);

  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  SQLSMALLINT ColumnCount;
  sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
  if (sr != SQL_SUCCESS) return -1;

  for (SQLSMALLINT i = 0; i<ColumnCount; ++i) {
    char buf[4096];
    SQLSMALLINT    NameLength;
    SQLSMALLINT    DataType;
    SQLULEN        ColumnSize;
    SQLSMALLINT    DecimalDigits;
    SQLSMALLINT    Nullable;
    SQLLEN         NumericAttribute;

    sr = CALL_SQLDescribeCol(hstmt, i+1, (SQLCHAR*)buf, sizeof(buf), &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("Column%d Name:%s, DataType:%s, ColumnSize:%" PRIu64 ", DecimalDigits:%d, Nullable:%s",
      i+1, buf, sql_data_type(DataType), (uint64_t)ColumnSize, DecimalDigits, sql_nullable(Nullable));

    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_NAME, (SQLCHAR*)buf, sizeof(buf), &NameLength, &NumericAttribute);
    if (sr != SQL_SUCCESS) return -1;

    SQLLEN         DESC_LENGTH;
    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_LENGTH, NULL, 0, NULL, &DESC_LENGTH);
    if (sr != SQL_SUCCESS) return -1;

    SQLLEN         DESC_PRECISION;
    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_PRECISION, NULL, 0, NULL, &DESC_PRECISION);
    if (sr != SQL_SUCCESS) return -1;

    SQLLEN         DESC_SCALE;
    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_SCALE, NULL, 0, NULL, &DESC_SCALE);
    if (sr != SQL_SUCCESS) return -1;

    SQLLEN         DESC_OCTET_LENGTH;
    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_OCTET_LENGTH, NULL, 0, NULL, &DESC_OCTET_LENGTH);
    if (sr != SQL_SUCCESS) return -1;

    DUMP("Column%d Name:%s, DESC_LENGTH:%" PRId64 ", DESC_PRECISION:%" PRId64 ", DESC_SCALE:%" PRId64 ", DESC_OCTET_LENGTH:%" PRId64 "",
      i+1, buf, (int64_t)DESC_LENGTH, (int64_t)DESC_PRECISION, (int64_t)DESC_SCALE, (int64_t)DESC_OCTET_LENGTH);
  }

  return 0;
}

static int _dump_stmt_desc_bind_desc_col(odbc_case_t *odbc_case, odbc_stage_t stage, odbc_handles_t *handles)
{
  (void)odbc_case;

  SQLHANDLE hstmt = handles->hstmt;

  SQLRETURN sr = SQL_SUCCESS;

  if (stage != ODBC_STMT) return 0;

  DUMP("%s:", __func__);

  const char *env = "SAMPLE_DUMP_COL_SQL";
  const char *sql = getenv(env);
  if (!sql) {
    DUMP("env `%s` not set yet", env);
    return 0;
  }
  DUMP("env`%s`:%s", env, sql);

  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  SQLSMALLINT ColumnCount;
  sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
  if (sr != SQL_SUCCESS) return -1;

  for (SQLSMALLINT i = 0; i<ColumnCount; ++i) {
    char buf[4096];
    SQLSMALLINT    NameLength;
    SQLSMALLINT    DataType;
    SQLULEN        ColumnSize;
    SQLSMALLINT    DecimalDigits;
    SQLSMALLINT    Nullable;
    SQLLEN         NumericAttribute;

    sr = CALL_SQLDescribeCol(hstmt, i+1, (SQLCHAR*)buf, sizeof(buf), &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("Column%d Name:%s, DataType:%s, ColumnSize:%" PRIu64 ", DecimalDigits:%d, Nullable:%s",
      i+1, buf, sql_data_type(DataType), (uint64_t)ColumnSize, DecimalDigits, sql_nullable(Nullable));

    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_NAME, (SQLCHAR*)buf, sizeof(buf), &NameLength, &NumericAttribute);
    if (sr != SQL_SUCCESS) return -1;

    SQLLEN         DESC_LENGTH;
    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_LENGTH, NULL, 0, NULL, &DESC_LENGTH);
    if (sr != SQL_SUCCESS) return -1;

    SQLLEN         DESC_PRECISION;
    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_PRECISION, NULL, 0, NULL, &DESC_PRECISION);
    if (sr != SQL_SUCCESS) return -1;

    SQLLEN         DESC_SCALE;
    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_SCALE, NULL, 0, NULL, &DESC_SCALE);
    if (sr != SQL_SUCCESS) return -1;

    SQLLEN         DESC_OCTET_LENGTH;
    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_OCTET_LENGTH, NULL, 0, NULL, &DESC_OCTET_LENGTH);
    if (sr != SQL_SUCCESS) return -1;

    DUMP("Column%d Name:%s, DESC_LENGTH:%" PRId64 ", DESC_PRECISION:%" PRId64 ", DESC_SCALE:%" PRId64 ", DESC_OCTET_LENGTH:%" PRId64 "",
      i+1, buf, (int64_t)DESC_LENGTH, (int64_t)DESC_PRECISION, (int64_t)DESC_SCALE, (int64_t)DESC_OCTET_LENGTH);
  }

  for (SQLSMALLINT i = 0; i<ColumnCount; ++i) {
    char buf[4096];
    SQLSMALLINT    TargetType       = SQL_C_CHAR;
    SQLPOINTER     TargetValuePtr   = buf;
    SQLLEN         BufferLength     = sizeof(buf);
    SQLLEN         StrLen_or_Ind    = 0;
    sr = CALL_SQLBindCol(hstmt, i+1, TargetType, TargetValuePtr, BufferLength, &StrLen_or_Ind);
    if (sr != SQL_SUCCESS) return -1;
  }

  DUMP("after binding...");
  for (SQLSMALLINT i = 0; i<ColumnCount; ++i) {
    char buf[4096];
    SQLSMALLINT    NameLength;
    SQLSMALLINT    DataType;
    SQLULEN        ColumnSize;
    SQLSMALLINT    DecimalDigits;
    SQLSMALLINT    Nullable;
    SQLLEN         NumericAttribute;

    sr = CALL_SQLDescribeCol(hstmt, i+1, (SQLCHAR*)buf, sizeof(buf), &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("Column%d Name:%s, DataType:%s, ColumnSize:%" PRIu64 ", DecimalDigits:%d, Nullable:%s",
      i+1, buf, sql_data_type(DataType), (uint64_t)ColumnSize, DecimalDigits, sql_nullable(Nullable));

    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_NAME, (SQLCHAR*)buf, sizeof(buf), &NameLength, &NumericAttribute);
    if (sr != SQL_SUCCESS) return -1;

    SQLLEN         DESC_LENGTH;
    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_LENGTH, NULL, 0, NULL, &DESC_LENGTH);
    if (sr != SQL_SUCCESS) return -1;

    SQLLEN         DESC_PRECISION;
    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_PRECISION, NULL, 0, NULL, &DESC_PRECISION);
    if (sr != SQL_SUCCESS) return -1;

    SQLLEN         DESC_SCALE;
    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_SCALE, NULL, 0, NULL, &DESC_SCALE);
    if (sr != SQL_SUCCESS) return -1;

    SQLLEN         DESC_OCTET_LENGTH;
    sr = CALL_SQLColAttribute(hstmt, i+1, SQL_DESC_OCTET_LENGTH, NULL, 0, NULL, &DESC_OCTET_LENGTH);
    if (sr != SQL_SUCCESS) return -1;

    DUMP("Column%d Name:%s, DESC_LENGTH:%" PRId64 ", DESC_PRECISION:%" PRId64 ", DESC_SCALE:%" PRId64 ", DESC_OCTET_LENGTH:%" PRId64 "",
      i+1, buf, (int64_t)DESC_LENGTH, (int64_t)DESC_PRECISION, (int64_t)DESC_SCALE, (int64_t)DESC_OCTET_LENGTH);
  }

  return 0;
}

static int _dump_stmt_describe_param(odbc_case_t *odbc_case, odbc_stage_t stage, odbc_handles_t *handles)
{
  (void)odbc_case;

  SQLHANDLE hstmt = handles->hstmt;

  SQLRETURN sr = SQL_SUCCESS;

  if (stage != ODBC_STMT) return 0;

  DUMP("%s:", __func__);

  const char *env = "SAMPLE_DUMP_PARAM_SQL";
  const char *sql = getenv(env);
  if (!sql) {
    DUMP("env `%s` not set yet", env);
    return 0;
  }
  DUMP("env`%s`:%s", env, sql);

  sr = CALL_SQLPrepare(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  SQLSMALLINT ParameterCount;
  sr = CALL_SQLNumParams(hstmt, &ParameterCount);
  if (sr != SQL_SUCCESS) return -1;

  for (SQLSMALLINT i = 0; i<ParameterCount; ++i) {
    SQLSMALLINT     DataType        = 0;
    SQLULEN         ParameterSize   = 0;
    SQLSMALLINT     DecimalDigits   = 0;
    SQLSMALLINT     Nullable        = 0;

    sr = CALL_SQLDescribeParam(hstmt, i+1, &DataType, &ParameterSize, &DecimalDigits, &Nullable);
    if (FAILED(sr)) return -1;
    DUMP("Parameter%d, DataType:%s, ParameterSize:%" PRIu64 ", DecimalDigits:%d, Nullable:%s",
        i+1, sql_data_type(DataType), (uint64_t)ParameterSize, DecimalDigits, sql_nullable(Nullable));
  }

  char buf[64][4096+1];

  for (SQLSMALLINT i = 0; i<ParameterCount; ++i) {
    SQLSMALLINT     InputOutputType               = SQL_PARAM_INPUT;
    SQLSMALLINT     ValueType                     = SQL_C_CHAR;
    SQLSMALLINT     ParameterType                 = SQL_VARCHAR;
    SQLULEN         ColumnSize                    = 8;
    SQLSMALLINT     DecimalDigits                 = 0;
    SQLPOINTER      ParameterValuePtr             = buf[i];
    SQLLEN          BufferLength                  = sizeof(buf[i]);
    SQLLEN          StrLen_or_Ind                 = SQL_NTS;

    sr = CALL_SQLBindParameter(hstmt, i+1, InputOutputType, ValueType, ParameterType, ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength, &StrLen_or_Ind);
    if (sr != SQL_SUCCESS) return -1;
  }

  for (SQLSMALLINT i = 0; i<ParameterCount; ++i) {
    SQLSMALLINT     DataType        = 0;
    SQLULEN         ParameterSize   = 0;
    SQLSMALLINT     DecimalDigits   = 0;
    SQLSMALLINT     Nullable        = 0;

    sr = CALL_SQLDescribeParam(hstmt, i+1, &DataType, &ParameterSize, &DecimalDigits, &Nullable);
    if (FAILED(sr)) return -1;
    DUMP("Parameter%d, DataType:%s, ParameterSize:%" PRIu64 ", DecimalDigits:%d, Nullable:%s",
        i+1, sql_data_type(DataType), (uint64_t)ParameterSize, DecimalDigits, sql_nullable(Nullable));
  }

  return 0;
}

static int _dump_stmt_desc_bind_desc_param(odbc_case_t *odbc_case, odbc_stage_t stage, odbc_handles_t *handles)
{
  (void)odbc_case;

  SQLHANDLE hstmt = handles->hstmt;

  SQLRETURN sr = SQL_SUCCESS;

  if (stage != ODBC_STMT) return 0;

  DUMP("%s:", __func__);

  const char *env = "SAMPLE_DUMP_PARAM_SQL";
  const char *sql = getenv(env);
  if (!sql) {
    DUMP("env `%s` not set yet", env);
    return 0;
  }
  DUMP("env`%s`:%s", env, sql);

  sr = CALL_SQLPrepare(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  SQLSMALLINT ParameterCount;
  sr = CALL_SQLNumParams(hstmt, &ParameterCount);
  if (sr != SQL_SUCCESS) return -1;

  for (SQLSMALLINT i = 0; i<ParameterCount; ++i) {
    SQLSMALLINT     DataType        = 0;
    SQLULEN         ParameterSize   = 0;
    SQLSMALLINT     DecimalDigits   = 0;
    SQLSMALLINT     Nullable        = 0;

    sr = CALL_SQLDescribeParam(hstmt, i+1, &DataType, &ParameterSize, &DecimalDigits, &Nullable);
    if (FAILED(sr)) return -1;
    DUMP("Parameter%d, DataType:%s, ParameterSize:%" PRIu64 ", DecimalDigits:%d, Nullable:%s",
        i+1, sql_data_type(DataType), (uint64_t)ParameterSize, DecimalDigits, sql_nullable(Nullable));
  }

  char buf[64][10+1];

  for (SQLSMALLINT i = 0; i<ParameterCount; ++i) {
    SQLSMALLINT     InputOutputType               = SQL_PARAM_INPUT;
    SQLSMALLINT     ValueType                     = SQL_C_CHAR;
    SQLSMALLINT     ParameterType                 = SQL_VARCHAR;
    SQLULEN         ColumnSize                    = sizeof(buf[i]) - 1;
    SQLSMALLINT     DecimalDigits                 = 0;
    SQLPOINTER      ParameterValuePtr             = buf[i];
    SQLLEN          BufferLength                  = sizeof(buf[i]);
    SQLLEN          StrLen_or_Ind                 = SQL_NTS;

    sr = CALL_SQLBindParameter(hstmt, i+1, InputOutputType, ValueType, ParameterType, ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength, &StrLen_or_Ind);
    if (sr != SQL_SUCCESS) return -1;
  }

  DUMP("after binding...");

  for (SQLSMALLINT i = 0; i<ParameterCount; ++i) {
    SQLSMALLINT     DataType        = 0;
    SQLULEN         ParameterSize   = 0;
    SQLSMALLINT     DecimalDigits   = 0;
    SQLSMALLINT     Nullable        = 0;

    sr = CALL_SQLDescribeParam(hstmt, i+1, &DataType, &ParameterSize, &DecimalDigits, &Nullable);
    if (FAILED(sr)) return -1;
    DUMP("Parameter%d, DataType:%s, ParameterSize:%" PRIu64 ", DecimalDigits:%d, Nullable:%s",
        i+1, sql_data_type(DataType), (uint64_t)ParameterSize, DecimalDigits, sql_nullable(Nullable));
  }

  return 0;
}

static int _prepare_bind_param_execute(odbc_case_t *odbc_case, odbc_stage_t stage, odbc_handles_t *handles)
{
  (void)odbc_case;

  SQLHANDLE hstmt = handles->hstmt;

  SQLRETURN sr = SQL_SUCCESS;

  if (stage != ODBC_STMT) return 0;

  DUMP("%s:", __func__);

  const char *sql = "insert into x (i8) values (?)";
  DUMP("sql:%s", sql);

  sr = CALL_SQLPrepare(hstmt, (SQLCHAR*)sql, SQL_NTS);
  if (sr != SQL_SUCCESS) return -1;

  SQLSMALLINT ParameterCount;
  sr = CALL_SQLNumParams(hstmt, &ParameterCount);
  if (sr != SQL_SUCCESS) return -1;

  for (SQLSMALLINT i = 0; i<ParameterCount; ++i) {
    SQLSMALLINT     DataType        = 0;
    SQLULEN         ParameterSize   = 0;
    SQLSMALLINT     DecimalDigits   = 0;
    SQLSMALLINT     Nullable        = 0;

    sr = CALL_SQLDescribeParam(hstmt, i+1, &DataType, &ParameterSize, &DecimalDigits, &Nullable);
    if (FAILED(sr)) return -1;
    DUMP("Parameter%d, DataType:%s, ParameterSize:%" PRIu64 ", DecimalDigits:%d, Nullable:%s",
        i+1, sql_data_type(DataType), (uint64_t)ParameterSize, DecimalDigits, sql_nullable(Nullable));
  }

  int8_t v[20];
  SQLLEN v_ind[20];

  memset(v, 0, sizeof(v));
  memset(v_ind, 0, sizeof(v_ind));

  SQLULEN ParamsProcessed = 0;
  SQLUSMALLINT ParamStatusArray[sizeof(v)/sizeof(v[0])];
  memset(ParamStatusArray, 0, sizeof(ParamStatusArray));
  SQLULEN paramset_size = sizeof(v)/sizeof(v[0]);
  paramset_size = 2;

  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)paramset_size, 0);
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_STATUS_PTR, ParamStatusArray, 0);
  CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &ParamsProcessed, 0);

  {
    SQLSMALLINT     InputOutputType               = SQL_PARAM_INPUT;
    SQLSMALLINT     ValueType                     = SQL_C_TINYINT;
    SQLSMALLINT     ParameterType                 = SQL_VARCHAR;
    SQLULEN         ColumnSize                    = 2;
    SQLSMALLINT     DecimalDigits                 = 0;
    SQLPOINTER      ParameterValuePtr             = v;
    SQLLEN          BufferLength                  = sizeof(v[0]);

    sr = CALL_SQLBindParameter(hstmt, 1, InputOutputType, ValueType, ParameterType, ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength, v_ind);
    if (sr != SQL_SUCCESS) return -1;
  }

  DUMP("after binding...");

  for (SQLSMALLINT i = 0; i<ParameterCount; ++i) {
    SQLSMALLINT     DataType        = 0;
    SQLULEN         ParameterSize   = 0;
    SQLSMALLINT     DecimalDigits   = 0;
    SQLSMALLINT     Nullable        = 0;

    sr = CALL_SQLDescribeParam(hstmt, i+1, &DataType, &ParameterSize, &DecimalDigits, &Nullable);
    if (FAILED(sr)) return -1;
    DUMP("Parameter%d, DataType:%s, ParameterSize:%" PRIu64 ", DecimalDigits:%d, Nullable:%s",
        i+1, sql_data_type(DataType), (uint64_t)ParameterSize, DecimalDigits, sql_nullable(Nullable));
  }

  v[0] = 1;
  v[1] = 123;

  sr = CALL_SQLExecute(hstmt);
  if (sr != SQL_SUCCESS) return -1;

  for (SQLULEN i = 0; i<ParamsProcessed; ++i) {
    DUMP("%zd:%d", i+1, ParamStatusArray[i]);
  }

  return 0;
}

static int _execute_file_impl(SQLHANDLE hstmt, const char *env, const char *fn, FILE *f)
{
  SQLRETURN sr = SQL_SUCCESS;

  char buf[4096];
  size_t nr;

  buf[0] = '\0';
  nr = fread(buf, 1, sizeof(buf)-1, f);
  if (ferror(f)) {
    DUMP("reading file `%s` set by env `%s` failed", fn, env);
    return -1;
  }
  if (!feof(f)) {
    DUMP("buffer too small to hold content of file `%s` set by env `%s`", fn, env);
    return -1;
  }
  buf[nr] = '\0';

  sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)buf, SQL_NTS);
  if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;

  SQLSMALLINT ColumnCount;
  SQLLEN RowCount;
  int i_row = 0;
  char row_buf[1024*64];
  const char *end = row_buf + sizeof(row_buf);
  char *p;
  char name[193];
  char value[1024];
  SQLSMALLINT    NameLength;
  SQLSMALLINT    DataType;
  SQLULEN        ColumnSize;
  SQLSMALLINT    DecimalDigits;
  SQLSMALLINT    Nullable;

describe:

  sr = CALL_SQLNumResultCols(hstmt, &ColumnCount);
  if (sr != SQL_SUCCESS) return -1;

fetch:

  if (ColumnCount > 0) {
    DUMP("dumping result set:");
    sr = CALL_SQLFetch(hstmt);
  } else {
    sr = CALL_SQLRowCount(hstmt, &RowCount);
    if (sr != SQL_SUCCESS) return -1;
    DUMP("rows affected:%zd", (size_t)RowCount);
    sr = SQL_NO_DATA;
  }

  if (sr == SQL_NO_DATA) {
    sr = CALL_SQLMoreResults(hstmt);
    if (sr == SQL_NO_DATA) return 0;
    if (sr == SQL_SUCCESS) goto describe;
  }
  if (sr != SQL_SUCCESS) return -1;

  row_buf[0] = '\0';
  p = row_buf;
  for (int i=0; i<ColumnCount; ++i) {
    sr = CALL_SQLDescribeCol(hstmt, i+1, (SQLCHAR*)name, sizeof(name), &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
    if (sr != SQL_SUCCESS) return -1;

    SQLLEN Len_or_Ind;
    sr = CALL_SQLGetData(hstmt, i+1, SQL_C_CHAR, value, sizeof(value), &Len_or_Ind);
    if (sr != SQL_SUCCESS) return -1;

    int n = snprintf(p, end - p, "%s%s:[%s]",
        i ? ";" : "",
        name,
        Len_or_Ind == SQL_NULL_DATA ? "null" : value);
    if (n < 0 || n >= end - p) {
      E("buffer too small");
      return -1;
    }
    p += n;
  }

  DUMP("%-4d:%s", ++i_row, row_buf);

  goto fetch;
}

static int _execute_file(odbc_case_t *odbc_case, odbc_stage_t stage, odbc_handles_t *handles)
{
  (void)odbc_case;

  SQLHANDLE hstmt = handles->hstmt;

  int r = 0;

  if (stage != ODBC_STMT) return 0;

  DUMP("%s:", __func__);

  const char *env = "SAMPLE_DUMP_SQL_FILE";
  const char *fn = getenv(env);
  if (!fn) {
    DUMP("env `%s` not set yet", env);
    return -1;
  }
  DUMP("env`%s`:%s", env, fn);

  FILE *f;

  f = fopen(fn, "r");
  if (!f) {
    DUMP("open file `%s` set by env `%s` failed:[%d]%s", fn, env, errno, strerror(errno));
    return -1;
  }

  r = _execute_file_impl(hstmt, env, fn, f);
  fclose(f);

  return r ? -1 : 0;
}

static odbc_case_t odbc_cases[] = {
  ODBC_CASE(_dummy),
  ODBC_CASE(_dump_stmt_col_info),
  ODBC_CASE(_dump_stmt_tables),
  ODBC_CASE(_dump_stmt_describe_col),
  ODBC_CASE(_dump_stmt_desc_bind_desc_col),
  ODBC_CASE(_dump_stmt_describe_param),
  ODBC_CASE(_dump_stmt_desc_bind_desc_param),
  ODBC_CASE(_prepare_bind_param_execute),
  ODBC_CASE(_execute_file),
};

static int _dumping(arg_t *arg)
{
  odbc_conn_arg_t conn_arg = arg->conn_arg;
  return run_odbc_cases(arg->name, &conn_arg, odbc_cases, sizeof(odbc_cases)/sizeof(odbc_cases[0]));
}

static void usage(const char *arg0)
{
  DUMP("usage:");
  DUMP("  %s -h", arg0);
  DUMP("  %s --dsn <DSN> [--uid <UID>] [--pwd <PWD> [name]", arg0);
  DUMP("  %s --connstr <connstr> [name]", arg0);
  DUMP("");
  DUMP("supported dump names:");
  for (size_t i=0; i<sizeof(odbc_cases)/sizeof(odbc_cases[0]); ++i) {
    DUMP("  %s", odbc_cases[i].name);
  }
}

static int dumping(int argc, char *argv[])
{
  int r = 0;

  arg_t arg = {0};

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-h") == 0) {
      usage(argv[0]);
      return 0;
    }
    if (strcmp(argv[i], "--dsn") == 0) {
      ++i;
      if (i>=argc) break;
      arg.conn_arg.dsn = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--uid") == 0) {
      ++i;
      if (i>=argc) break;
      arg.conn_arg.uid = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--pwd") == 0) {
      ++i;
      if (i>=argc) break;
      arg.conn_arg.pwd = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--connstr") == 0) {
      ++i;
      if (i>=argc) break;
      arg.conn_arg.connstr = argv[i];
      continue;
    }

    arg.name = argv[i];
    r = _dumping(&arg);
    if (r) return -1;
  }

  if (arg.name) return 0;

  return _dumping(&arg);
}

int main(int argc, char *argv[])
{
  int r = 0;
  r = dumping(argc, argv);
  fprintf(stderr, "==%s==\n", r ? "failure" : "success");
  return !!r;
}

