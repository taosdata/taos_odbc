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

#include "columns.h"

#include "errs.h"
#include "log.h"
#include "taos_helpers.h"
#include "tsdb.h"

void columns_args_reset(columns_args_t *args)
{
  if (!args) return;

  WILD_SAFE_FREE(args->catalog_pattern);
  WILD_SAFE_FREE(args->schema_pattern);
  WILD_SAFE_FREE(args->table_pattern);
  WILD_SAFE_FREE(args->column_pattern);
}

void columns_args_release(columns_args_t *args)
{
  if (!args) return;
  columns_args_reset(args);
}

static columns_col_meta_t _columns_meta[] = {
  {
    /* name                            */ "TABLE_CAT",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_VARCHAR,
    /* SQL_DESC_OCTET_LENGTH           */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,
  },{
    /* name                            */ "TABLE_SCHEM",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_VARCHAR,
    /* SQL_DESC_OCTET_LENGTH           */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,
  },{
    /* name                            */ "TABLE_NAME",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_VARCHAR,
    /* SQL_DESC_OCTET_LENGTH           */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,
  },{
    /* name                            */ "COLUMN_NAME",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_VARCHAR,
    /* SQL_DESC_OCTET_LENGTH           */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,
  },{
    /* name                            */ "DATA_TYPE",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_OCTET_LENGTH           */ 5,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
  },{
    /* name                            */ "TYPE_NAME",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_VARCHAR,
    /* SQL_DESC_OCTET_LENGTH           */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
  },{
    /* name                            */ "COLUMN_SIZE",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_INTEGER,
    /* SQL_DESC_OCTET_LENGTH           */ 20,                 /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
  },{
    /* name                            */ "BUFFER_LENGTH",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_INTEGER,
    /* SQL_DESC_OCTET_LENGTH           */ 20,                 /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
  },{
    /* name                            */ "DECIMAL_DIGITS",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_OCTET_LENGTH           */ 5,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
  },{
    /* name                            */ "NUM_PREC_RADIX",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_OCTET_LENGTH           */ 5,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
  },{
    /* name                            */ "NULLABLE",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_OCTET_LENGTH           */ 5,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
  },{
    /* name                            */ "REMARKS",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_VARCHAR,
    /* SQL_DESC_OCTET_LENGTH           */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
  },{
    /* name                            */ "COLUMN_DEF",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_VARCHAR,
    /* SQL_DESC_OCTET_LENGTH           */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
  },{
    /* name                            */ "SQL_DATA_TYPE",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_OCTET_LENGTH           */ 5,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
  },{
    /* name                            */ "SQL_DATETIME_SUB",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_SMALLINT,
    /* SQL_DESC_OCTET_LENGTH           */ 5,                  /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
  },{
    /* name                            */ "CHAR_OCTET_LENGTH",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_INTEGER,
    /* SQL_DESC_OCTET_LENGTH           */ 20,                 /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NULLABLE,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
  },{
    /* name                            */ "ORDINAL_POSITION",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_INTEGER,
    /* SQL_DESC_OCTET_LENGTH           */ 20,                 /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
  },{
    /* name                            */ "IS_NULLABLE",
    /* SQL_DESC_CONCISE_TYPE           */ SQL_VARCHAR,
    /* SQL_DESC_OCTET_LENGTH           */ 1024,               /* hard-coded, big enough */
    /* SQL_DESC_PRECISION              */ 0,
    /* SQL_DESC_SCALE                  */ 0,
    /* SQL_DESC_AUTO_UNIQUE_VALUE      */ SQL_FALSE,
    /* SQL_DESC_UPDATABLE              */ SQL_ATTR_READONLY,
    /* SQL_DESC_NULLABLE               */ SQL_NO_NULLS,
    /* SQL_DESC_UNSIGNED               */ SQL_FALSE,          /* NOTE: check it later */
  },
};

SQLSMALLINT columns_get_count_of_col_meta(void)
{
  SQLSMALLINT nr = (SQLSMALLINT)(sizeof(_columns_meta) / sizeof(_columns_meta[0]));
  return nr;
}

columns_col_meta_t* columns_get_col_meta(int i_col)
{
  int nr = (int)(sizeof(_columns_meta) / sizeof(_columns_meta[0]));
  if (i_col < 0) return NULL;
  if (i_col >= nr) return NULL;
  return _columns_meta + i_col;
}

void columns_reset(columns_t *columns)
{
  if (!columns) return;

  tsdb_stmt_reset(&columns->desc);
  tsdb_stmt_reset(&columns->stmt);
  mem_reset(&columns->catalog_cache);
  mem_reset(&columns->schema_cache);
  mem_reset(&columns->table_cache);
  mem_reset(&columns->column_cache);

  columns_args_reset(&columns->columns_args);

  columns->catalog = NULL;
  columns->schema  = NULL;
  columns->table   = NULL;
  columns->column  = NULL;
}

void columns_release(columns_t *columns)
{
  if (!columns) return;
  columns_reset(columns);

  mem_release(&columns->catalog_cache);
  mem_release(&columns->schema_cache);
  mem_release(&columns->table_cache);
  mem_release(&columns->column_cache);

  columns_args_release(&columns->columns_args);

  columns->owner = NULL;
}

static SQLRETURN _query(stmt_base_t *base, const char *sql)
{
  (void)sql;

  columns_t *columns = (columns_t*)base;
  (void)columns;
  stmt_append_err(columns->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _execute(stmt_base_t *base)
{
  columns_t *columns = (columns_t*)base;
  (void)columns;
  stmt_append_err(columns->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _fetch_rowset(stmt_base_t *base, size_t rowset_size)
{
  columns_t *columns = (columns_t*)base;
  return columns->desc.base.fetch_rowset(&columns->desc.base, rowset_size);
}

static SQLRETURN _fetch_row_with_tsdb(stmt_base_t *base, tsdb_data_t *tsdb)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  columns_t *columns = (columns_t*)base;

  charset_conv_t *cnv = &columns->owner->conn->cnv_tsdb_varchar_to_sql_c_wchar;

again:

  sr = columns->desc.base.fetch_row(&columns->desc.base);
  if (sr == SQL_NO_DATA) return SQL_NO_DATA;
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  if (columns->columns_args.column_pattern) {
    tsdb_data_reset(tsdb);
    sr = columns->desc.base.get_data(&columns->desc.base, 1, tsdb);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    r = wildexec_n_ex(columns->columns_args.column_pattern, cnv->from, tsdb->str.str, tsdb->str.len);

    if (r) {
      // FIXME: out of memory
      goto again;
    }
  }

  return SQL_SUCCESS;
}

static SQLRETURN _fetch_row(stmt_base_t *base)
{
  SQLRETURN sr = SQL_SUCCESS;

  tsdb_data_t tsdb = {0};

  sr = _fetch_row_with_tsdb(base, &tsdb);

  tsdb_data_release(&tsdb);

  return sr;
}

static void _move_to_first_on_rowset(stmt_base_t *base)
{
  columns_t *columns = (columns_t*)base;
  columns->desc.base.move_to_first_on_rowset(&columns->desc.base);
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

  columns_t *columns = (columns_t*)base;
  (void)columns;
  stmt_append_err(columns->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _describe_col(stmt_base_t *base,
    SQLUSMALLINT   ColumnNumber,
    SQLCHAR       *ColumnName,
    SQLSMALLINT    BufferLength,
    SQLSMALLINT   *NameLengthPtr,
    SQLSMALLINT   *DataTypePtr,
    SQLULEN       *ColumnSizePtr,
    SQLSMALLINT   *DecimalDigitsPtr,
    SQLSMALLINT   *NullablePtr)
{
  columns_t *columns = (columns_t*)base;

  columns_col_meta_t *col_meta = columns_get_col_meta(ColumnNumber - 1);

  *NameLengthPtr    = (SQLSMALLINT)strlen(col_meta->name);
  *DataTypePtr      = (SQLSMALLINT)col_meta->DESC_CONCISE_TYPE;
  *ColumnSizePtr    = col_meta->DESC_OCTET_LENGTH;
  *DecimalDigitsPtr = 0;
  *NullablePtr      = (SQLSMALLINT)col_meta->DESC_NULLABLE;

  int n = snprintf((char*)ColumnName, BufferLength, "%s", col_meta->name);
  if (n < 0 || n >= BufferLength) {
    stmt_append_err(columns->owner, "01004", 0, "String data, right truncated");
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _get_num_params(stmt_base_t *base, SQLSMALLINT *ParameterCountPtr)
{
  (void)ParameterCountPtr;

  columns_t *columns = (columns_t*)base;
  (void)columns;
  stmt_append_err(columns->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _check_params(stmt_base_t *base)
{
  columns_t *columns = (columns_t*)base;
  (void)columns;
  stmt_append_err(columns->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _tsdb_field_by_param(stmt_base_t *base, int i_param, TAOS_FIELD_E **field)
{
  (void)i_param;
  (void)field;

  columns_t *columns = (columns_t*)base;
  (void)columns;
  stmt_append_err(columns->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _get_num_cols(stmt_base_t *base, SQLSMALLINT *ColumnCountPtr)
{
  (void)base;

  *ColumnCountPtr = columns_get_count_of_col_meta();

  return SQL_SUCCESS;
}

static SQLRETURN _get_data(stmt_base_t *base, SQLUSMALLINT Col_or_Param_Num, tsdb_data_t *tsdb)
{
  columns_t *columns = (columns_t*)base;
  switch (Col_or_Param_Num) {
    case 4:
      return columns->desc.base.get_data(&columns->desc.base, 1, tsdb);
    default:
      break;
  }
  stmt_append_err(columns->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

void columns_init(columns_t *columns, stmt_t *stmt)
{
  columns->owner = stmt;
  tsdb_stmt_init(&columns->stmt, stmt);
  tsdb_stmt_init(&columns->desc, stmt);
  columns->base.query                        = _query;
  columns->base.execute                      = _execute;
  columns->base.fetch_rowset                 = _fetch_rowset;
  columns->base.fetch_row                    = _fetch_row;
  columns->base.move_to_first_on_rowset      = _move_to_first_on_rowset;
  columns->base.describe_param               = _describe_param;
  columns->base.describe_col                 = _describe_col;
  columns->base.get_num_params               = _get_num_params;
  columns->base.check_params                 = _check_params;
  columns->base.tsdb_field_by_param          = _tsdb_field_by_param;
  columns->base.get_num_cols                 = _get_num_cols;
  columns->base.get_data                     = _get_data;
}

SQLRETURN columns_open(
    columns_t      *columns,
    SQLCHAR       *CatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *SchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *TableName,
    SQLSMALLINT    NameLength3,
    SQLCHAR       *ColumnName,
    SQLSMALLINT    NameLength4)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  columns_reset(columns);

  OW("owner:%p", columns->owner);
  OW("conn:%p", columns->owner->conn);

  charset_conv_t *cnv = &columns->owner->conn->cnv_sql_c_char_to_sql_c_wchar;
  if (CatalogName) {
    if (mem_conv(&columns->catalog_cache, cnv->cnv, (const char*)CatalogName, NameLength1)) {
      stmt_oom(columns->owner);
      return SQL_ERROR;
    }
    columns->catalog = columns->catalog_cache.base;
    if (wildcomp_n_ex(&columns->columns_args.catalog_pattern, cnv->from, (const char*)CatalogName, NameLength1)) {
      stmt_append_err_format(columns->owner, "HY000", 0,
          "General error:wild compile failed for CatalogName[%.*s]", (int)NameLength1, (const char*)CatalogName);
      return SQL_ERROR;
    }
  }
  if (SchemaName) {
    if (mem_conv(&columns->schema_cache, cnv->cnv, (const char*)SchemaName, NameLength2)) {
      stmt_oom(columns->owner);
      return SQL_ERROR;
    }
    columns->schema = columns->schema_cache.base;
    if (wildcomp_n_ex(&columns->columns_args.schema_pattern, cnv->from, (const char*)SchemaName, NameLength2)) {
      stmt_append_err_format(columns->owner, "HY000", 0,
          "General error:wild compile failed for SchemaName[%.*s]", (int)NameLength2, (const char*)SchemaName);
      return SQL_ERROR;
    }
  }
  if (TableName) {
    if (mem_conv(&columns->table_cache, cnv->cnv, (const char*)TableName, NameLength3)) {
      stmt_oom(columns->owner);
      return SQL_ERROR;
    }
    columns->table = columns->table_cache.base;
    if (wildcomp_n_ex(&columns->columns_args.table_pattern, cnv->from, (const char*)TableName, NameLength3)) {
      stmt_append_err_format(columns->owner, "HY000", 0,
          "General error:wild compile failed for TableName[%.*s]", (int)NameLength3, (const char*)TableName);
      return SQL_ERROR;
    }
  }
  if (ColumnName) {
    if (mem_conv(&columns->column_cache, cnv->cnv, (const char*)ColumnName, NameLength4)) {
      stmt_oom(columns->owner);
      return SQL_ERROR;
    }
    columns->column = columns->column_cache.base;
    if (wildcomp_n_ex(&columns->columns_args.column_pattern, cnv->from, (const char*)ColumnName, NameLength4)) {
      stmt_append_err_format(columns->owner, "HY000", 0,
          "General error:wild compile failed for ColumnName[%.*s]", (int)NameLength4, (const char*)ColumnName);
      return SQL_ERROR;
    }
  }

  const char *sql =
    "select db_name `TABLE_CAT`, '' `TABLE_SCHEM`, stable_name `TABLE_NAME`, table_comment `REMARKS` from information_schema.ins_stables"
    " "
    "union all"
    " "
    "select db_name `TABLE_CAT`, '' `TABLE_SCHEM`, table_name `TABLE_NAME`, table_comment `REMARKS` from information_schema.ins_tables"
    " "
    "order by `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`";

  sr = tsdb_stmt_query(&columns->stmt, sql);
  if (sr != SQL_SUCCESS) return SQL_ERROR;


  tsdb_data_t cat = {0};
  tsdb_data_t sch = {0};
  tsdb_data_t tbl = {0};

again:

  sr = columns->stmt.base.fetch_rowset(&columns->stmt.base, 1);
  if (sr == SQL_NO_DATA) return SQL_NO_DATA;
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  sr = columns->stmt.base.fetch_row(&columns->stmt.base);
  if (sr == SQL_NO_DATA) goto again;
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  do {
    tsdb_data_reset(&cat);
    sr = columns->stmt.base.get_data(&columns->stmt.base, 1, &cat);
    if (sr != SQL_SUCCESS) break;

    if (columns->columns_args.catalog_pattern) {
      r = wildexec_n_ex(columns->columns_args.catalog_pattern, cnv->from, cat.str.str, cat.str.len);

      if (r) {
        // FIXME: out of memory
        goto again;
      }
    }

    tsdb_data_reset(&sch);
    sr = columns->stmt.base.get_data(&columns->stmt.base, 2, &sch);
    if (sr != SQL_SUCCESS) break;

    if (columns->columns_args.schema_pattern) {
      r = wildexec_n_ex(columns->columns_args.schema_pattern, cnv->from, sch.str.str, sch.str.len);

      if (r) {
        // FIXME: out of memory
        goto again;
      }
    }

    tsdb_data_reset(&tbl);
    sr = columns->stmt.base.get_data(&columns->stmt.base, 3, &tbl);
    if (sr != SQL_SUCCESS) break;

    if (columns->columns_args.table_pattern) {
      r = wildexec_n_ex(columns->columns_args.table_pattern, cnv->from, tbl.str.str, tbl.str.len);

      if (r) {
        // FIXME: out of memory
        goto again;
      }
    }

    char sql[4096];
    int n = snprintf(sql, sizeof(sql), "desc `%.*s`.`%.*s`", (int)cat.str.len, cat.str.str, (int)tbl.str.len, tbl.str.str);
    if (n < 0 || (size_t)n >= sizeof(sql)) {
      stmt_append_err(columns->owner, "HY000", 0, "General error:internal logic error or buffer too small");
      sr = SQL_ERROR;
      break;
    }

    tsdb_stmt_reset(&columns->desc);
    tsdb_stmt_init(&columns->desc, columns->owner);

    sr = tsdb_stmt_query(&columns->desc, sql);
    if (sr != SQL_SUCCESS) {
      stmt_append_err(columns->owner, "HY000", 0, "General error:internal logic error or buffer too small");
      sr = SQL_ERROR;
      break;
    }
  } while (0);

  tsdb_data_release(&cat);
  tsdb_data_release(&sch);
  tsdb_data_release(&tbl);

  return sr;
}

