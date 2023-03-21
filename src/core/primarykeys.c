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

#include "primarykeys.h"
#include "tables.h"

#include "errs.h"
#include "log.h"
#include "taos_helpers.h"
#include "tsdb.h"

#include <errno.h>

void primarykeys_args_reset(primarykeys_args_t *args)
{
  if (!args) return;

  WILD_SAFE_FREE(args->catalog_pattern);
  WILD_SAFE_FREE(args->schema_pattern);
  WILD_SAFE_FREE(args->table_pattern);
}

void primarykeys_args_release(primarykeys_args_t *args)
{
  if (!args) return;
  primarykeys_args_reset(args);
}

static column_meta_t _primarykeys_meta[] = {
  {
    /* 1 */
    /* name                            */ "TABLE_CAT",
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
    /* 2 */
    /* name                            */ "TABLE_SCHEM",
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
    /* 3 */
    /* name                            */ "TABLE_NAME",
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
    /* 4 */
    /* name                            */ "COLUMN_NAME",
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
    /* 5 */
    /* name                            */ "KEY_SEQ",
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
    /* 6 */
    /* name                            */ "PK_NAME",
    /* column_type_name                */ "VARCHAR",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_VARCHAR,
    /* SQL_DESC_LENGTH                 */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_OCTET_LENGTH           */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 1024,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_TRUE,           /* NOTE: check it later */
    /* SQL_DESC_NUM_PREC_RADIX         */ 0,
  },
};

SQLSMALLINT primarykeys_get_count_of_col_meta(void)
{
  SQLSMALLINT nr = (SQLSMALLINT)(sizeof(_primarykeys_meta) / sizeof(_primarykeys_meta[0]));
  return nr;
}

const column_meta_t* primarykeys_get_col_meta(int i_col)
{
  int nr = (int)(sizeof(_primarykeys_meta) / sizeof(_primarykeys_meta[0]));
  if (i_col < 0) return NULL;
  if (i_col >= nr) return NULL;
  return _primarykeys_meta + i_col;
}

void primarykeys_reset(primarykeys_t *primarykeys)
{
  if (!primarykeys) return;

  tsdb_stmt_reset(&primarykeys->desc);
  tables_reset(&primarykeys->tables);

  primarykeys_args_reset(&primarykeys->primarykeys_args);

  primarykeys->ordinal_order = 0;
}

void primarykeys_release(primarykeys_t *primarykeys)
{
  if (!primarykeys) return;
  primarykeys_reset(primarykeys);

  tsdb_stmt_release(&primarykeys->desc);
  tables_release(&primarykeys->tables);

  primarykeys_args_release(&primarykeys->primarykeys_args);

  primarykeys->owner = NULL;
}

static SQLRETURN _query(stmt_base_t *base, const char *sql)
{
  (void)sql;

  primarykeys_t *primarykeys = (primarykeys_t*)base;
  (void)primarykeys;
  stmt_append_err(primarykeys->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _execute(stmt_base_t *base)
{
  primarykeys_t *primarykeys = (primarykeys_t*)base;
  (void)primarykeys;
  stmt_append_err(primarykeys->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _fetch_and_desc_next_table(primarykeys_t *primarykeys)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = primarykeys->tables.base.fetch_row(&primarykeys->tables.base);
  if (sr == SQL_NO_DATA) return SQL_NO_DATA;
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sr = primarykeys->tables.base.get_data(&primarykeys->tables.base, 1, &primarykeys->current_catalog);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sr = primarykeys->tables.base.get_data(&primarykeys->tables.base, 2, &primarykeys->current_schema);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sr = primarykeys->tables.base.get_data(&primarykeys->tables.base, 3, &primarykeys->current_table);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  char sql[4096];
  int n = snprintf(sql, sizeof(sql), "desc `%.*s`.`%.*s`",
      (int)primarykeys->current_catalog.str.len, primarykeys->current_catalog.str.str,
      (int)primarykeys->current_table.str.len, primarykeys->current_table.str.str);
  if (n < 0 || (size_t)n >= sizeof(sql)) {
    stmt_append_err(primarykeys->owner, "HY000", 0, "General error:internal logic error or buffer too small");
    return SQL_ERROR;
  }

  tsdb_stmt_reset(&primarykeys->desc);
  tsdb_stmt_init(&primarykeys->desc, primarykeys->owner);

  sr = tsdb_stmt_query(&primarykeys->desc, sql);
  if (sr != SQL_SUCCESS) {
    stmt_append_err(primarykeys->owner, "HY000", 0, "General error:internal logic error or buffer too small");
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _fetch_row_with_tsdb(stmt_base_t *base, tsdb_data_t *tsdb)
{
  (void)tsdb;

  SQLRETURN sr = SQL_SUCCESS;

  primarykeys_t *primarykeys = (primarykeys_t*)base;

again:

  sr = primarykeys->desc.base.fetch_row(&primarykeys->desc.base);
  if (sr == SQL_NO_DATA) {
    sr = _fetch_and_desc_next_table(primarykeys);
    if (sr == SQL_NO_DATA) return SQL_NO_DATA;
    if (sr != SQL_SUCCESS) return SQL_ERROR;
    goto again;
  }

  if (sr != SQL_SUCCESS) return SQL_ERROR;

  return SQL_SUCCESS;
}

static SQLRETURN _fetch_row(stmt_base_t *base)
{
  SQLRETURN sr = SQL_SUCCESS;

  primarykeys_t *primarykeys = (primarykeys_t*)base;

  tsdb_data_t tsdb = {0};

  if (primarykeys->ordinal_order == 1) return SQL_NO_DATA;

  sr = _fetch_row_with_tsdb(base, &tsdb);
  if (sr == SQL_NO_DATA) return SQL_NO_DATA;
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  tsdb_data_t *col_type        = &primarykeys->current_col_type;

  sr = primarykeys->desc.base.get_data(&primarykeys->desc.base, 2, col_type);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  if (col_type->is_null) {
    stmt_append_err(primarykeys->owner, "HY000", 0, "General error:internal logic error");
    return SQL_ERROR;
  }
  if (col_type->type != TSDB_DATA_TYPE_VARCHAR) {
    stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:internal logic error:`%s`", taos_data_type(col_type->type));
    return SQL_ERROR;
  }

  ++primarykeys->ordinal_order;

  if (col_type->str.len != 9 || strncmp(col_type->str.str, "TIMESTAMP", 9)) return SQL_NO_DATA;

  return SQL_SUCCESS;
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

  primarykeys_t *primarykeys = (primarykeys_t*)base;
  (void)primarykeys;
  stmt_append_err(primarykeys->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _describe_col(stmt_base_t *base,
    SQLUSMALLINT   ColumnNumber,
    SQLCHAR       *ColumnName,
    SQLSMALLINT    BufferLength,
    SQLSMALLINT   *NameLengthPtr,
    SQLSMALLINT   *DataTypePtr,
    SQLULEN       *primarykeysizePtr,
    SQLSMALLINT   *DecimalDigitsPtr,
    SQLSMALLINT   *NullablePtr)
{
  primarykeys_t *primarykeys = (primarykeys_t*)base;

  const column_meta_t *col_meta = primarykeys_get_col_meta(ColumnNumber - 1);

  *NameLengthPtr    = (SQLSMALLINT)strlen(col_meta->name);
  *DataTypePtr      = (SQLSMALLINT)col_meta->DESC_CONCISE_TYPE;
  *primarykeysizePtr    = col_meta->DESC_OCTET_LENGTH;
  *DecimalDigitsPtr = 0;
  *NullablePtr      = (SQLSMALLINT)col_meta->DESC_NULLABLE;

  int n = snprintf((char*)ColumnName, BufferLength, "%s", col_meta->name);
  if (n < 0 || n >= BufferLength) {
    stmt_append_err(primarykeys->owner, "01004", 0, "String data, right truncated");
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
  primarykeys_t *primarykeys = (primarykeys_t*)base;
  (void)primarykeys;
  const column_meta_t *col_meta = primarykeys_get_col_meta(ColumnNumber - 1);
  if (!col_meta) {
    stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:column[%d] out of range", ColumnNumber);
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
        stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:internal logic error:[%d]%s", e, strerror(e));
        return SQL_ERROR;
      }
      if (StringLengthPtr) *StringLengthPtr = n;
      OW("Column%d:[SQL_DESC_NAME]:%.*s", ColumnNumber, n, (const char*)CharacterAttributePtr);
      return SQL_SUCCESS;
    case SQL_COLUMN_TYPE_NAME:
      n = snprintf(CharacterAttributePtr, BufferLength, "%s", col_meta->column_type_name);
      if (n < 0) {
        int e = errno;
        stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:internal logic error:[%d]%s", e, strerror(e));
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
      stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:`%s[%d/0x%x]` not supported yet", sql_col_attribute(FieldIdentifier), FieldIdentifier, FieldIdentifier);
      return SQL_ERROR;
  }

  stmt_append_err(primarykeys->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _get_num_params(stmt_base_t *base, SQLSMALLINT *ParameterCountPtr)
{
  (void)ParameterCountPtr;

  primarykeys_t *primarykeys = (primarykeys_t*)base;
  (void)primarykeys;
  stmt_append_err(primarykeys->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _check_params(stmt_base_t *base)
{
  primarykeys_t *primarykeys = (primarykeys_t*)base;
  (void)primarykeys;
  stmt_append_err(primarykeys->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _tsdb_field_by_param(stmt_base_t *base, int i_param, TAOS_FIELD_E **field)
{
  (void)i_param;
  (void)field;

  primarykeys_t *primarykeys = (primarykeys_t*)base;
  (void)primarykeys;
  stmt_append_err(primarykeys->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _row_count(stmt_base_t *base, SQLLEN *row_count_ptr)
{
  primarykeys_t *primarykeys = (primarykeys_t*)base;
  (void)primarykeys;

  if (row_count_ptr) *row_count_ptr = 0;

  return SQL_SUCCESS;
}

static SQLRETURN _get_num_cols(stmt_base_t *base, SQLSMALLINT *ColumnCountPtr)
{
  (void)base;

  *ColumnCountPtr = primarykeys_get_count_of_col_meta();

  return SQL_SUCCESS;
}

static SQLRETURN _get_data(stmt_base_t *base, SQLUSMALLINT Col_or_Param_Num, tsdb_data_t *tsdb)
{
  SQLRETURN sr = SQL_SUCCESS;

  primarykeys_t *primarykeys = (primarykeys_t*)base;
  tsdb_data_t *col_name        = &primarykeys->current_col_name;
  tsdb_data_t *col_type        = &primarykeys->current_col_type;
  tsdb_data_t *col_length      = &primarykeys->current_col_length;
  tsdb_data_t *col_note        = &primarykeys->current_col_note;

  sr = primarykeys->desc.base.get_data(&primarykeys->desc.base, 1, col_name);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  if (col_name->type != TSDB_DATA_TYPE_VARCHAR) {
    stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:internal logic error:`%s`", taos_data_type(col_name->type));
    return SQL_ERROR;
  }
  sr = primarykeys->desc.base.get_data(&primarykeys->desc.base, 2, col_type);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  if (col_type->type != TSDB_DATA_TYPE_VARCHAR) {
    stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:internal logic error:`%s`", taos_data_type(col_type->type));
    return SQL_ERROR;
  }
  sr = primarykeys->desc.base.get_data(&primarykeys->desc.base, 3, col_length);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  if (col_length->type != TSDB_DATA_TYPE_INT) {
    stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:internal logic error:`%s`", taos_data_type(col_length->type));
    return SQL_ERROR;
  }
  sr = primarykeys->desc.base.get_data(&primarykeys->desc.base, 4, col_note);
  if (sr != SQL_SUCCESS) return SQL_ERROR;
  if (col_note->type != TSDB_DATA_TYPE_VARCHAR) {
    stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:internal logic error:`%s`", taos_data_type(col_note->type));
    return SQL_ERROR;
  }

  TAOS_FIELD fake = {0};
  int n = snprintf(fake.name, sizeof(fake.name), "%.*s", (int)col_name->str.len, col_name->str.str);
  if (n < 0 || (size_t)n >= sizeof(fake.name)) {
    stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:buffer too small to hold `%.*s`", (int)col_name->str.len, col_name->str.str);
    return SQL_ERROR;
  }

  if (col_type->str.len == 9 && strncmp(col_type->str.str, "TIMESTAMP", 9) == 0) {
    fake.type = TSDB_DATA_TYPE_TIMESTAMP;
    fake.bytes = col_length->i32;
    if (fake.bytes != 8) {
      stmt_append_err(primarykeys->owner, "HY000", 0, "General error:internal logic error");
      return SQL_ERROR;
    }
  } else if (col_type->str.len == 3 && strncmp(col_type->str.str, "INT", 3) == 0) {
    fake.type = TSDB_DATA_TYPE_INT;
    fake.bytes = col_length->i32;
    if (fake.bytes != 4) {
      stmt_append_err(primarykeys->owner, "HY000", 0, "General error:internal logic error");
      return SQL_ERROR;
    }
  } else if (col_type->str.len == 7 && strncmp(col_type->str.str, "VARCHAR", 7) == 0) {
    fake.type = TSDB_DATA_TYPE_VARCHAR;
    fake.bytes = col_length->i32;
  } else if (col_type->str.len == 5 && strncmp(col_type->str.str, "NCHAR", 5) == 0) {
    fake.type = TSDB_DATA_TYPE_VARCHAR;
    fake.bytes = col_length->i32;
  } else {
    stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:not implemented yet for column[%d] `%.*s`",
        Col_or_Param_Num, (int)col_type->str.len, col_type->str.str);
    return SQL_ERROR;
  }

  tsdb->is_null = 0;

  switch (Col_or_Param_Num) {
    case 1: // TABLE_CAT
      // better approach?
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str  = primarykeys->current_catalog.str;
      OW("TABLE_CAT:[%.*s]", (int)tsdb->str.len, tsdb->str.str);
      break;
    case 2: // TABLE_SCHEM
      // better approach?
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str  = primarykeys->current_schema.str;
      OW("TABLE_SCHEM:[%.*s]", (int)tsdb->str.len, tsdb->str.str);
      break;
    case 3: // TABLE_NAME
      // better approach?
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str  = primarykeys->current_table.str;
      OW("TABLE_NAME:[%.*s]", (int)tsdb->str.len, tsdb->str.str);
      break;
    case 4: // COLUMN_NAME
      // better approach?
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str  = col_name->str;
      OW("COLUMN_NAME:[%.*s]", (int)tsdb->str.len, tsdb->str.str);
      break;
    case 5: // KEY_SEQ
      tsdb->type = TSDB_DATA_TYPE_INT;
      tsdb->i32  = primarykeys->ordinal_order;
      OW("KEY_SEQ:[%d]", tsdb->i32);
      break;
    case 6: // PK_NAME
      // better approach?
      tsdb->type = TSDB_DATA_TYPE_VARCHAR;
      tsdb->str  = col_name->str;
      OW("PK_NAME:[%.*s]", (int)tsdb->str.len, tsdb->str.str);
      break;
    default:
      stmt_append_err_format(primarykeys->owner, "HY000", 0, "General error:not implemented yet for column[%d]", Col_or_Param_Num);
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

void primarykeys_init(primarykeys_t *primarykeys, stmt_t *stmt)
{
  primarykeys->owner = stmt;
  tables_init(&primarykeys->tables, stmt);
  tsdb_stmt_init(&primarykeys->desc, stmt);
  primarykeys->base.query                        = _query;
  primarykeys->base.execute                      = _execute;
  primarykeys->base.fetch_row                    = _fetch_row;
  primarykeys->base.describe_param               = _describe_param;
  primarykeys->base.describe_col                 = _describe_col;
  primarykeys->base.col_attribute                = _col_attribute;
  primarykeys->base.get_num_params               = _get_num_params;
  primarykeys->base.check_params                 = _check_params;
  primarykeys->base.tsdb_field_by_param          = _tsdb_field_by_param;
  primarykeys->base.row_count                    = _row_count;
  primarykeys->base.get_num_cols                 = _get_num_cols;
  primarykeys->base.get_data                     = _get_data;
}

SQLRETURN primarykeys_open(
    primarykeys_t      *primarykeys,
    SQLCHAR       *CatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *SchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *TableName,
    SQLSMALLINT    NameLength3)
{
  SQLRETURN sr = SQL_SUCCESS;

  primarykeys_reset(primarykeys);

  if (CatalogName && NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen((const char*)CatalogName);
  if (SchemaName && NameLength2 == SQL_NTS)  NameLength2 = (SQLSMALLINT)strlen((const char*)SchemaName);
  if (TableName && NameLength3 == SQL_NTS)   NameLength3 = (SQLSMALLINT)strlen((const char*)TableName);

  OW("CatalogName:%p,%.*s", CatalogName, (int)NameLength1, CatalogName);
  OW("SchemaName:%p,%.*s", SchemaName, (int)NameLength2, SchemaName);
  OW("TableName:%p,%.*s", TableName, (int)NameLength3, TableName);

  sr = tables_open(&primarykeys->tables, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3, (SQLCHAR*)"TABLE", SQL_NTS);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  return _fetch_and_desc_next_table(primarykeys);
}
