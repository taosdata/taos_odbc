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

#include "internal.h"

#include "typesinfo.h"

#include "errs.h"
#include "log.h"
#include "taos_helpers.h"
#include "tsdb.h"

#include <errno.h>

static const column_meta_t _typesinfo_meta[] = {
  {
    /* 1 */
    /* name                            */ "TYPE_NAME",
    /* column_type_name                */ "VARCHAR",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_VARCHAR,
    /* SQL_DESC_LENGTH                 */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_OCTET_LENGTH           */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 1024,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_TRUE,
    /* SQL_DESC_NUM_PREC_RADIX         */ 0,
  },{
    /* 2 */
    /* name                            */ "DATA_TYPE",
    /* column_type_name                */ "SMALLINT",         /* FIXME: what to fill? */
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_LENGTH                 */ 5,                  /* FIXME: what to fill? */
    /* SQL_DESC_OCTET_LENGTH           */ 2,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 5,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 10,                 /* NOTE: check it later */
  },{
    /* 3 */
    /* name                            */ "COLUMN_SIZE",
    /* column_type_name                */ "INT",              /* FIXME: what to fill? */
    /* SQL_DESC_CONCISE_TYPE           */ SQL_INTEGER,
    /* SQL_DESC_LENGTH                 */ 10,                 /* FIXME: what to fill? */
    /* SQL_DESC_OCTET_LENGTH           */ 4,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 10,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 10,                 /* NOTE: check it later */
  },{
    /* 4 */
    /* name                            */ "LITERAL_PREFIX",
    /* column_type_name                */ "VARCHAR",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_VARCHAR,
    /* SQL_DESC_LENGTH                 */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_OCTET_LENGTH           */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 1024,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_TRUE,
    /* SQL_DESC_NUM_PREC_RADIX         */ 0,
  },{
    /* 5 */
    /* name                            */ "LITERAL_SUFFIX",
    /* column_type_name                */ "VARCHAR",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_VARCHAR,
    /* SQL_DESC_LENGTH                 */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_OCTET_LENGTH           */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 1024,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_TRUE,
    /* SQL_DESC_NUM_PREC_RADIX         */ 0,
  },{
    /* 6 */
    /* name                            */ "CREATE_PARAMS",
    /* column_type_name                */ "VARCHAR",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_VARCHAR,
    /* SQL_DESC_LENGTH                 */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_OCTET_LENGTH           */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 1024,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_TRUE,           /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 0,
  },{
    /* 7 */
    /* name                            */ "NULLABLE",
    /* column_type_name                */ "SMALLINT",         /* FIXME: what to fill? */
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_LENGTH                 */ 5,                  /* FIXME: what to fill? */
    /* SQL_DESC_OCTET_LENGTH           */ 2,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 5,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 10,                 /* NOTE: check it later */
  },{
    /* 8 */
    /* name                            */ "CASE_SENSITIVE",
    /* column_type_name                */ "SMALLINT",         /* FIXME: what to fill? */
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_LENGTH                 */ 5,                  /* FIXME: what to fill? */
    /* SQL_DESC_OCTET_LENGTH           */ 2,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 5,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 10,                 /* NOTE: check it later */
  },{
    /* 9 */
    /* name                            */ "SEARCHABLE",
    /* column_type_name                */ "SMALLINT",         /* FIXME: what to fill? */
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_LENGTH                 */ 5,                  /* FIXME: what to fill? */
    /* SQL_DESC_OCTET_LENGTH           */ 2,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 5,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 10,                 /* NOTE: check it later */
  },{
    /* 10 */
    /* name                            */ "UNSIGNED_ATTRIBUTE",
    /* column_type_name                */ "SMALLINT",         /* FIXME: what to fill? */
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_LENGTH                 */ 5,                  /* FIXME: what to fill? */
    /* SQL_DESC_OCTET_LENGTH           */ 2,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 5,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,  /* FIXME: SQL_ATTR_READWRITE_UNKNOWN */
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 10,                 /* NOTE: check it later */
  },{
    /* 11 */
    /* name                            */ "FIXED_PREC_SCALE",
    /* column_type_name                */ "SMALLINT",         /* FIXME: what to fill? */
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_LENGTH                 */ 5,                  /* FIXME: what to fill? */
    /* SQL_DESC_OCTET_LENGTH           */ 2,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 5,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,  /* FIXME: SQL_ATTR_READWRITE_UNKNOWN */
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 10,                 /* NOTE: check it later */
  },{
    /* 12 */
    /* name                            */ "AUTO_UNIQUE_VALUE",
    /* column_type_name                */ "SMALLINT",         /* FIXME: what to fill? */
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_LENGTH                 */ 5,                  /* FIXME: what to fill? */
    /* SQL_DESC_OCTET_LENGTH           */ 2,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 5,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,  /* FIXME: SQL_ATTR_READWRITE_UNKNOWN */
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 10,                 /* NOTE: check it later */
  },{
    /* 13 */
    /* name                            */ "LOCAL_TYPE_NAME",
    /* column_type_name                */ "VARCHAR",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_VARCHAR,
    /* SQL_DESC_LENGTH                 */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_OCTET_LENGTH           */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 1024,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_TRUE,           /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 0,
  },{
    /* 14 */
    /* name                            */ "MINIMUM_SCALE",
    /* column_type_name                */ "SMALLINT",         /* FIXME: what to fill? */
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_LENGTH                 */ 5,                  /* FIXME: what to fill? */
    /* SQL_DESC_OCTET_LENGTH           */ 2,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 5,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 10,                 /* NOTE: check it later */
  },{
    /* 15 */
    /* name                            */ "MAXIMUM_SCALE",
    /* column_type_name                */ "SMALLINT",         /* FIXME: what to fill? */
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_LENGTH                 */ 5,                  /* FIXME: what to fill? */
    /* SQL_DESC_OCTET_LENGTH           */ 2,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 5,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 10,                 /* NOTE: check it later */
  },{
    /* 16 */
    /* name                            */ "SQL_DATA_TYPE",
    /* column_type_name                */ "SMALLINT",         /* FIXME: what to fill? */
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_LENGTH                 */ 5,                  /* FIXME: what to fill? */
    /* SQL_DESC_OCTET_LENGTH           */ 2,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 5,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 10,                 /* NOTE: check it later */
  },{
    /* 17 */
    /* name                            */ "SQL_DATETIME_SUB",
    /* column_type_name                */ "SMALLINT",         /* FIXME: what to fill? */
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_LENGTH                 */ 5,                  /* FIXME: what to fill? */
    /* SQL_DESC_OCTET_LENGTH           */ 2,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 5,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 10,                 /* NOTE: check it later */
  },{
    /* 18 */
    /* name                            */ "NUM_PREC_RADIX",
    /* column_type_name                */ "INT",              /* FIXME: what to fill? */
    /* SQL_DESC_CONCISE_TYPE           */ SQL_INTEGER,
    /* SQL_DESC_LENGTH                 */ 10,                 /* FIXME: what to fill? */
    /* SQL_DESC_OCTET_LENGTH           */ 4,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 10,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 10,                 /* NOTE: check it later */
  },{
    /* 19 */
    /* name                            */ "INTERVAL_PRECISION",
    /* column_type_name                */ "SMALLINT",         /* FIXME: what to fill? */
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_LENGTH                 */ 5,                  /* FIXME: what to fill? */
    /* SQL_DESC_OCTET_LENGTH           */ 2,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 5,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 10,                 /* NOTE: check it later */
  },
};

typedef struct type_info_s                  type_info_t;
struct type_info_s {
  /*  1 */ const char                *TYPE_NAME;
  /*  2 */ int16_t                    DATA_TYPE;
  /*  3 */ int32_t                    COLUMN_SIZE;
  /*  4 */ const char                *LITERAL_PREFIX;
  /*  5 */ const char                *LITERAL_SUFFIX;
  /*  6 */ const char                *CREATE_PARAMS;
  /*  7 */ int16_t                    NULLABLE;
  /*  8 */ int16_t                    CASE_SENSITIVE;
  /*  9 */ int16_t                    SEARCHABLE;
  /* 10 */ int16_t                    UNSIGNED_ATTRIBUTE;
  /* 11 */ int16_t                    FIXED_PREC_SCALE;
  /* 12 */ int16_t                    AUTO_UNIQUE_VALUE;
  /* 13 */ const char                *LOCAL_TYPE_NAME;
  /* 14 */ int16_t                    MINIMUM_SCALE;
  /* 15 */ int16_t                    MAXIMUM_SCALE;
  /* 16 */ int16_t                    SQL_DATA_TYPE;
  /* 17 */ int16_t                    SQL_DATETIME_SUB;
  /* 18 */ int32_t                    NUM_PREC_RADIX;
  /* 19 */ int16_t                    INTERVAL_PRECISION;
};

static const type_info_t _typesinfo[] = {
  {0}
};

SQLSMALLINT typesinfo_get_count_of_col_meta(void)
{
  SQLSMALLINT nr = (SQLSMALLINT)(sizeof(_typesinfo_meta) / sizeof(_typesinfo_meta[0]));
  return nr;
}

const column_meta_t* typesinfo_get_col_meta(int i_col)
{
  int nr = (int)(sizeof(_typesinfo_meta) / sizeof(_typesinfo_meta[0]));
  if (i_col < 0) return NULL;
  if (i_col >= nr) return NULL;
  return _typesinfo_meta + i_col;
}

void typesinfo_reset(typesinfo_t *typesinfo)
{
  if (!typesinfo) return;
}

void typesinfo_release(typesinfo_t *typesinfo)
{
  if (!typesinfo) return;
  typesinfo_reset(typesinfo);

  typesinfo->owner = NULL;
}

static SQLRETURN _query(stmt_base_t *base, const char *sql)
{
  (void)sql;

  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;
  stmt_append_err(typesinfo->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _execute(stmt_base_t *base)
{
  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;
  stmt_append_err(typesinfo->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _fetch_rowset(stmt_base_t *base, size_t rowset_size)
{
  typesinfo_t *typesinfo = (typesinfo_t*)base;
  typesinfo->rowset_size = rowset_size;
  if (typesinfo->idx < sizeof(_typesinfo)/sizeof(_typesinfo[0])) return SQL_SUCCESS;
  return SQL_NO_DATA;
}

static SQLRETURN _fetch_row(stmt_base_t *base)
{
  typesinfo_t *typesinfo = (typesinfo_t*)base;

  stmt_append_err(typesinfo->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static void _move_to_first_on_rowset(stmt_base_t *base)
{
  typesinfo_t *typesinfo = (typesinfo_t*)base;
  typesinfo->pos = 1;
}

static SQLRETURN _describe_param(stmt_base_t *base,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT    *DataTypePtr,
    SQLULEN        *ParameterSizePtr,
    SQLSMALLINT    *DecimalDigitsPtr,
    SQLSMALLINT    *NullablePtr)
{
  (void)ParameterNumber;
  (void)DataTypePtr;
  (void)ParameterSizePtr;
  (void)DecimalDigitsPtr;
  (void)NullablePtr;

  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;
  stmt_append_err(typesinfo->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _describe_col(stmt_base_t *base,
    SQLUSMALLINT   ColumnNumber,
    SQLCHAR       *ColumnName,
    SQLSMALLINT    BufferLength,
    SQLSMALLINT   *NameLengthPtr,
    SQLSMALLINT   *DataTypePtr,
    SQLULEN       *typesinfoizePtr,
    SQLSMALLINT   *DecimalDigitsPtr,
    SQLSMALLINT   *NullablePtr)
{
  typesinfo_t *typesinfo = (typesinfo_t*)base;

  const column_meta_t *col_meta = typesinfo_get_col_meta(ColumnNumber - 1);

  *NameLengthPtr    = (SQLSMALLINT)strlen(col_meta->name);
  *DataTypePtr      = (SQLSMALLINT)col_meta->DESC_CONCISE_TYPE;
  *typesinfoizePtr    = col_meta->DESC_OCTET_LENGTH;
  *DecimalDigitsPtr = 0;
  *NullablePtr      = (SQLSMALLINT)col_meta->DESC_NULLABLE;

  int n = snprintf((char*)ColumnName, BufferLength, "%s", col_meta->name);
  if (n < 0 || n >= BufferLength) {
    stmt_append_err(typesinfo->owner, "01004", 0, "String data, right truncated");
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _col_attribute(stmt_base_t *base,
    SQLUSMALLINT    ColumnNumber,
    SQLUSMALLINT    FieldIdentifier,
    SQLPOINTER      CharacterAttributePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr,
    SQLLEN         *NumericAttributePtr)
{
  (void)ColumnNumber;
  (void)FieldIdentifier;
  (void)CharacterAttributePtr;
  (void)BufferLength;
  (void)StringLengthPtr;
  (void)NumericAttributePtr;
  int n = 0;
  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;
  const column_meta_t *col_meta = typesinfo_get_col_meta(ColumnNumber - 1);
  if (!col_meta) {
    stmt_append_err_format(typesinfo->owner, "HY000", 0, "General error:column[%d] out of range", ColumnNumber);
    return SQL_ERROR;
  }

  switch(FieldIdentifier) {
    case SQL_DESC_CONCISE_TYPE:
      *NumericAttributePtr = col_meta->DESC_CONCISE_TYPE;
      OW("Column%d:[SQL_DESC_CONCISE_TYPE]:[%d]%s", ColumnNumber, (int)*NumericAttributePtr, sql_data_type((SQLSMALLINT)*NumericAttributePtr));
      return SQL_SUCCESS;
    case SQL_DESC_OCTET_LENGTH:
      *NumericAttributePtr = col_meta->DESC_OCTET_LENGTH;
      OW("Column%d:[SQL_DESC_OCTET_LENGTH]:%d", ColumnNumber, (int)*NumericAttributePtr);
      return SQL_SUCCESS;
    // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolattribute-function?view=sql-server-ver16#backward-compatibility
    case SQL_DESC_PRECISION:
    case SQL_COLUMN_PRECISION:
      *NumericAttributePtr = col_meta->DESC_PRECISION;
      OW("Column%d:[SQL_DESC_PRECISION]:%d", ColumnNumber, (int)*NumericAttributePtr);
      return SQL_SUCCESS;
    // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolattribute-function?view=sql-server-ver16#backward-compatibility
    case SQL_DESC_SCALE:
    case SQL_COLUMN_SCALE:
      *NumericAttributePtr = col_meta->DESC_SCALE;
      OW("Column%d:[SQL_DESC_SCALE]:%d", ColumnNumber, (int)*NumericAttributePtr);
      return SQL_SUCCESS;
    case SQL_DESC_AUTO_UNIQUE_VALUE:
      *NumericAttributePtr = col_meta->DESC_AUTO_UNIQUE_VALUE;
      OW("Column%d:[SQL_DESC_AUTO_UNIQUE_VALUE]:%d", ColumnNumber, (int)*NumericAttributePtr);
      return SQL_SUCCESS;
    case SQL_DESC_UPDATABLE:
      *NumericAttributePtr = col_meta->DESC_UPDATABLE;
      OW("Column%d:[SQL_DESC_UPDATABLE]:[%d]%s", ColumnNumber, (int)*NumericAttributePtr, sql_updatable(*NumericAttributePtr));
      return SQL_SUCCESS;
    case SQL_DESC_NULLABLE:
      *NumericAttributePtr = col_meta->DESC_NULLABLE;
      OW("Column%d:[SQL_DESC_NULLABLE]:[%d]%s", ColumnNumber, (int)*NumericAttributePtr, sql_nullable((SQLSMALLINT)*NumericAttributePtr));
      return SQL_SUCCESS;
    case SQL_DESC_NAME:
      n = snprintf(CharacterAttributePtr, BufferLength, "%s", col_meta->name);
      if (n < 0) {
        int e = errno;
        stmt_append_err_format(typesinfo->owner, "HY000", 0, "General error:internal logic error:[%d]%s", e, strerror(e));
        return SQL_ERROR;
      }
      if (StringLengthPtr) *StringLengthPtr = n;
      OW("Column%d:[SQL_DESC_NAME]:%.*s", ColumnNumber, n, (const char*)CharacterAttributePtr);
      return SQL_SUCCESS;
    case SQL_COLUMN_TYPE_NAME:
      n = snprintf(CharacterAttributePtr, BufferLength, "%s", col_meta->column_type_name);
      if (n < 0) {
        int e = errno;
        stmt_append_err_format(typesinfo->owner, "HY000", 0, "General error:internal logic error:[%d]%s", e, strerror(e));
        return SQL_ERROR;
      }
      if (StringLengthPtr) *StringLengthPtr = n;
      OW("Column%d:[SQL_COLUMN_TYPE_NAME]:%.*s", ColumnNumber, n, (const char*)CharacterAttributePtr);
      return SQL_SUCCESS;
    case SQL_DESC_LENGTH:
      *NumericAttributePtr = col_meta->DESC_LENGTH;
      OW("Column%d:[SQL_DESC_LENGTH]:%d", ColumnNumber, (int)*NumericAttributePtr);
      return SQL_SUCCESS;
    case SQL_DESC_NUM_PREC_RADIX:
      *NumericAttributePtr = col_meta->DESC_NUM_PREC_RADIX;
      OW("Column%d:[SQL_DESC_NUM_PREC_RADIX]:%d", ColumnNumber, (int)*NumericAttributePtr);
      return SQL_SUCCESS;
    case SQL_DESC_UNSIGNED:
      *NumericAttributePtr = col_meta->DESC_UNSIGNED;
      OW("Column%d:[SQL_DESC_UNSIGNED]:%s", ColumnNumber, ((int)*NumericAttributePtr) ? "SQL_TRUE" : "SQL_FALSE");
      return SQL_SUCCESS;
    default:
      stmt_append_err_format(typesinfo->owner, "HY000", 0, "General error:`%s[%d/0x%x]` not supported yet", sql_col_attribute(FieldIdentifier), FieldIdentifier, FieldIdentifier);
      return SQL_ERROR;
  }

  stmt_append_err(typesinfo->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _get_num_params(stmt_base_t *base, SQLSMALLINT *ParameterCountPtr)
{
  (void)ParameterCountPtr;

  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;
  stmt_append_err(typesinfo->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _check_params(stmt_base_t *base)
{
  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;
  stmt_append_err(typesinfo->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _tsdb_field_by_param(stmt_base_t *base, int i_param, TAOS_FIELD_E **field)
{
  (void)i_param;
  (void)field;

  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;
  stmt_append_err(typesinfo->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _row_count(stmt_base_t *base, SQLLEN *row_count_ptr)
{
  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;

  if (row_count_ptr) *row_count_ptr = 0;

  return SQL_SUCCESS;
}

static SQLRETURN _get_num_cols(stmt_base_t *base, SQLSMALLINT *ColumnCountPtr)
{
  (void)base;

  *ColumnCountPtr = typesinfo_get_count_of_col_meta();

  return SQL_SUCCESS;
}

static SQLRETURN _get_data(stmt_base_t *base, SQLUSMALLINT Col_or_Param_Num, tsdb_data_t *tsdb)
{
  (void)Col_or_Param_Num;
  (void)tsdb;
  typesinfo_t *typesinfo = (typesinfo_t*)base;
  (void)typesinfo;

  tsdb->is_null = 0;

  stmt_append_err(typesinfo->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

void typesinfo_init(typesinfo_t *typesinfo, stmt_t *stmt)
{
  typesinfo->owner = stmt;
  typesinfo->base.query                        = _query;
  typesinfo->base.execute                      = _execute;
  typesinfo->base.fetch_rowset                 = _fetch_rowset;
  typesinfo->base.fetch_row                    = _fetch_row;
  typesinfo->base.move_to_first_on_rowset      = _move_to_first_on_rowset;
  typesinfo->base.describe_param               = _describe_param;
  typesinfo->base.describe_col                 = _describe_col;
  typesinfo->base.col_attribute                = _col_attribute;
  typesinfo->base.get_num_params               = _get_num_params;
  typesinfo->base.check_params                 = _check_params;
  typesinfo->base.tsdb_field_by_param          = _tsdb_field_by_param;
  typesinfo->base.row_count                    = _row_count;
  typesinfo->base.get_num_cols                 = _get_num_cols;
  typesinfo->base.get_data                     = _get_data;
}

SQLRETURN typesinfo_open(
    typesinfo_t     *typesinfo,
    SQLSMALLINT      DataType)
{
  SQLRETURN sr = SQL_SUCCESS;

  typesinfo_reset(typesinfo);

  typesinfo->data_type = DataType;

  return sr;
}
