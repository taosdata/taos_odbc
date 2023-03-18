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

#include "tables.h"

#include "charset.h"
#include "errs.h"
#include "log.h"
#include "taos_helpers.h"
#include "tsdb.h"

#include <ctype.h>

void tables_args_reset(tables_args_t *args)
{
  if (!args) return;

  WILD_SAFE_FREE(args->catalog_pattern);
  WILD_SAFE_FREE(args->schema_pattern);
  WILD_SAFE_FREE(args->table_pattern);
  WILD_SAFE_FREE(args->type_pattern);
}

void tables_args_release(tables_args_t *args)
{
  if (!args) return;
  tables_args_reset(args);
}

static void table_types_reset(tables_t *tables)
{
  mem_reset(&tables->table_types);
}

static void table_types_release(tables_t *tables)
{
  if (!tables) return;
  table_types_reset(tables);

  mem_release(&tables->table_types);
}

void tables_reset(tables_t *tables)
{
  if (!tables) return;

  tsdb_stmt_reset(&tables->stmt);
  table_types_reset(tables);
  mem_reset(&tables->catalog_cache);
  mem_reset(&tables->schema_cache);
  mem_reset(&tables->table_cache);
  mem_reset(&tables->type_cache);
  
  tables_args_reset(&tables->tables_args);

  tables->catalog = NULL;
  tables->schema  = NULL;
  tables->table   = NULL;
  tables->type    = NULL;
}

void tables_release(tables_t *tables)
{
  if (!tables) return;
  tables_reset(tables);

  table_types_release(tables);

  mem_release(&tables->catalog_cache);
  mem_release(&tables->schema_cache);
  mem_release(&tables->table_cache);
  mem_release(&tables->type_cache);

  tables_args_release(&tables->tables_args);

  tables->owner = NULL;
  tables->rowset_size = 0;
}

static SQLRETURN _query(stmt_base_t *base, const char *sql)
{
  (void)sql;

  tables_t *tables = (tables_t*)base;
  (void)tables;
  stmt_append_err(tables->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _execute(stmt_base_t *base)
{
  tables_t *tables = (tables_t*)base;
  (void)tables;
  stmt_append_err(tables->owner, "HY000", 0, "General error:internal logic error");
  return SQL_ERROR;
}

static SQLRETURN _fetch_rowset(stmt_base_t *base, size_t rowset_size)
{
  tables_t *tables = (tables_t*)base;
  tables->rowset_size = rowset_size;
  return tables->stmt.base.fetch_rowset(&tables->stmt.base, rowset_size);
}

static void _match(tables_t *tables, tsdb_data_t *tsdb, int *matched)
{
  const char *begin = (const char*)tables->table_types.base;
  const char *end   = begin + tables->table_types.nr;

  const char *p = begin;
  while (p < end) {
    // NOTE: non-unicode stored in table_types
    size_t n = strlen(p);
    if (tsdb->str.len == n && strncmp(tsdb->str.str, p, n) == 0) {
      *matched = 1;
      return;
    }
    p += n + 1;
    continue;
  }

  *matched = 0;

  return;
}

static SQLRETURN _fetch_row_with_tsdb(stmt_base_t *base, tsdb_data_t *tsdb)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  tables_t *tables = (tables_t*)base;

again:

  sr = tables->stmt.base.fetch_row(&tables->stmt.base);
  if (sr == SQL_NO_DATA) {
    sr = _fetch_rowset(base, tables->rowset_size);
    if (sr == SQL_NO_DATA) return SQL_NO_DATA;
    if (sr != SQL_SUCCESS) return SQL_ERROR;
    goto again;
  }
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  if (sr != TABLES_FOR_GENERIC) return SQL_SUCCESS;

  if (sr != SQL_SUCCESS) return SQL_ERROR;

  charset_conv_t *cnv = &tables->owner->conn->cnv_tsdb_varchar_to_sql_c_wchar;

  if (tables->tables_args.catalog_pattern) {
    tsdb_data_reset(tsdb);
    sr = tables->stmt.base.get_data(&tables->stmt.base, 1, tsdb);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    r = wildexec_n_ex(tables->tables_args.catalog_pattern, cnv->from, tsdb->str.str, tsdb->str.len);

    if (r) {
      // FIXME: out of memory
      goto again;
    }
  }

  if (tables->tables_args.schema_pattern) {
    tsdb_data_reset(tsdb);
    sr = tables->stmt.base.get_data(&tables->stmt.base, 2, tsdb);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    r = wildexec_n_ex(tables->tables_args.schema_pattern, cnv->from, tsdb->str.str, tsdb->str.len);

    if (r) {
      // FIXME: out of memory
      goto again;
    }
  }

  if (tables->tables_args.table_pattern) {
    tsdb_data_reset(tsdb);
    sr = tables->stmt.base.get_data(&tables->stmt.base, 3, tsdb);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    r = wildexec_n_ex(tables->tables_args.table_pattern, cnv->from, tsdb->str.str, tsdb->str.len);

    if (r) {
      // FIXME: out of memory
      goto again;
    }
  }

  if (tables->table_types.nr > 0) {
    tsdb_data_reset(tsdb);
    sr = tables->stmt.base.get_data(&tables->stmt.base, 4, tsdb);
    if (sr != SQL_SUCCESS) return SQL_ERROR;

    int matched = 0;
    _match(tables, tsdb, &matched);
    OW("tsdb:%.*s:%s", (int)tsdb->str.len, tsdb->str.str, matched ? "matched" : "not matched");

    tsdb_data_reset(tsdb);
    sr = tables->stmt.base.get_data(&tables->stmt.base, 3, tsdb);
    if (sr == SQL_SUCCESS) {
      OW("table:%.*s:%s", (int)tsdb->str.len, tsdb->str.str, matched ? "matched" : "not matched");
    }

    if (matched) return SQL_SUCCESS;

    goto again;
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
  tables_t *tables = (tables_t*)base;
  tables->stmt.base.move_to_first_on_rowset(&tables->stmt.base);
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

  tables_t *tables = (tables_t*)base;
  (void)tables;
  stmt_append_err(tables->owner, "HY000", 0, "General error:not implemented yet");
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
  tables_t *tables = (tables_t*)base;
  return tables->stmt.base.describe_col(&tables->stmt.base,
      ColumnNumber, ColumnName, BufferLength, NameLengthPtr, DataTypePtr, ColumnSizePtr, DecimalDigitsPtr, NullablePtr);
}

static SQLRETURN _col_attribute(stmt_base_t *base,
    SQLUSMALLINT    ColumnNumber,
    SQLUSMALLINT    FieldIdentifier,
    SQLPOINTER      CharacterAttributePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr,
    SQLLEN         *NumericAttributePtr)
{
  tables_t *tables = (tables_t*)base;
  (void)tables;
  SQLRETURN sr = tables->stmt.base.col_attribute(&tables->stmt.base,
      ColumnNumber,
      FieldIdentifier,
      CharacterAttributePtr,
      BufferLength,
      StringLengthPtr,
      NumericAttributePtr);
  OA(sr == SQL_SUCCESS, "%s:ColumnNumber:%d;FieldIdentifier:%s", sql_return_type(sr), ColumnNumber, sql_col_attribute(FieldIdentifier));

  return sr;
}

static SQLRETURN _get_num_params(stmt_base_t *base, SQLSMALLINT *ParameterCountPtr)
{
  (void)ParameterCountPtr;

  tables_t *tables = (tables_t*)base;
  (void)tables;
  stmt_append_err(tables->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _check_params(stmt_base_t *base)
{
  tables_t *tables = (tables_t*)base;
  (void)tables;
  stmt_append_err(tables->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _tsdb_field_by_param(stmt_base_t *base, int i_param, TAOS_FIELD_E **field)
{
  (void)i_param;
  (void)field;

  tables_t *tables = (tables_t*)base;
  (void)tables;
  stmt_append_err(tables->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _row_count(stmt_base_t *base, SQLLEN *row_count_ptr)
{
  tables_t *tables = (tables_t*)base;
  (void)tables;

  return tables->stmt.base.row_count(&tables->stmt.base, row_count_ptr);
}

static SQLRETURN _get_num_cols(stmt_base_t *base, SQLSMALLINT *ColumnCountPtr)
{
  tables_t *tables = (tables_t*)base;
  return tables->stmt.base.get_num_cols(&tables->stmt.base, ColumnCountPtr);
}

static SQLRETURN _get_data(stmt_base_t *base, SQLUSMALLINT Col_or_Param_Num, tsdb_data_t *tsdb)
{
  tables_t *tables = (tables_t*)base;
  return tables->stmt.base.get_data(&tables->stmt.base, Col_or_Param_Num, tsdb);
}

void tables_init(tables_t *tables, stmt_t *stmt)
{
  tables->owner = stmt;
  tsdb_stmt_init(&tables->stmt, stmt);
  tables->base.query                        = _query;
  tables->base.execute                      = _execute;
  tables->base.fetch_rowset                 = _fetch_rowset;
  tables->base.fetch_row                    = _fetch_row;
  tables->base.move_to_first_on_rowset      = _move_to_first_on_rowset;
  tables->base.describe_param               = _describe_param;
  tables->base.describe_col                 = _describe_col;
  tables->base.col_attribute                = _col_attribute;
  tables->base.get_num_params               = _get_num_params;
  tables->base.check_params                 = _check_params;
  tables->base.tsdb_field_by_param          = _tsdb_field_by_param;
  tables->base.row_count                    = _row_count;
  tables->base.get_num_cols                 = _get_num_cols;
  tables->base.get_data                     = _get_data;
}

static SQLRETURN _tables_open_catalogs(tables_t *tables)
{
  SQLRETURN sr = SQL_SUCCESS;

  const char *sql =
    "select name `TABLE_CAT`, null `TABLE_SCHEM`, null `TABLE_NAME`, null `TABLE_TYPE`, null `REMARKS` from information_schema.ins_databases order by `TABLE_CAT`";

  sr = tsdb_stmt_query(&tables->stmt, sql);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  tables->tables_type = TABLES_FOR_CATALOGS;

  return SQL_SUCCESS;
}

static SQLRETURN _tables_open_schemas(tables_t *tables)
{
  SQLRETURN sr = SQL_SUCCESS;

  const char *sql =
    "select null `TABLE_CAT`, null `TABLE_SCHEM`, null `TABLE_NAME`, null `TABLE_TYPE`, null `REMARKS` where 1=2";

  sr = tsdb_stmt_query(&tables->stmt, sql);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  tables->tables_type = TABLES_FOR_SCHEMAS;

  return SQL_SUCCESS;
}

static SQLRETURN _tables_open_tabletypes_with_buffer(tables_t *tables, buffer_t *buf)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  const char *table_types[] = {
    "CHILD TABLE",
    "SUPER TABLE",
    "SYSTEM TABLE",
    "TABLE",
    "VIEW",
  };

  for (size_t i=0; i<sizeof(table_types)/sizeof(table_types[0]); ++i) {
    const char *table_type = table_types[i];
    if (i) {
      r = buffer_concat_fmt(buf, " union ");
      if (r) {
        stmt_oom(tables->owner);
        return SQL_ERROR;
      }
    }
    r = buffer_concat_fmt(buf, "select null `TABLE_CAT`, null `TABLE_SCHEM`, null `TABLE_NAME`, '%s' `TABLE_TYPE`, null `REMARKS`", table_type);
    if (r) {
      stmt_oom(tables->owner);
      return SQL_ERROR;
    }
  }

  const char *sql = buf->base;
  sr = tsdb_stmt_query(&tables->stmt, sql);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  tables->tables_type = TABLES_FOR_TABLETYPES;

  return SQL_SUCCESS;
}

static SQLRETURN _tables_open_tabletypes(tables_t *tables)
{
  SQLRETURN sr = SQL_SUCCESS;

  buffer_t buf = {0};

  sr = _tables_open_tabletypes_with_buffer(tables, &buf);

  buffer_release(&buf);

  return sr;
}

static SQLRETURN _tables_add_table_type(tables_t *tables, SQLCHAR *TableType, SQLSMALLINT NameLength4, const char *begin, const char *p)
{
  charset_conv_t *cnv = &tables->owner->conn->cnv_sql_c_char_to_tsdb_varchar;

  char *t = (char*)tables->table_types.base + tables->table_types.nr;
  char       *inbuf                 = (char*)begin;
  size_t      inbytesleft           = p-begin;
  char       *outbuf                = t;
  size_t      outbytesleft          = tables->table_types.cap - tables->table_types.nr;
  size_t n = CALL_iconv(cnv->cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
  if (n != 0) {
    stmt_append_err_format(tables->owner, "HY000", 0, "convert [%.*s] from %s to %s failed or non-reversible characters found therein",
        (int)NameLength4, (const char*)TableType, cnv->from, cnv->to);
    return SQL_ERROR;
  }

  // NOTE: non-unicode for tsdb_varchar, thus '\0' is a string terminator
  memset(outbuf, 0, 1);

  tables->table_types.nr += outbuf - t + 1;

  return SQL_SUCCESS;
}

static SQLRETURN _tables_parse_table_type(tables_t *tables, SQLCHAR *TableType, SQLSMALLINT NameLength4)
{
  SQLRETURN sr = SQL_SUCCESS;

  OW("TableType:%.*s", (int)NameLength4, TableType);

  const char *begin = (const char*)TableType;
  const char *end   = begin + NameLength4;

  size_t cap = (NameLength4 + 1) * 6;
  int r = mem_keep(&tables->table_types, cap);
  if (r) {
    stmt_oom(tables->owner);
    return SQL_ERROR;
  }

  const char *p = begin;
  begin = NULL;

  while (p < end) {
    if (!begin) {
      if (isspace(*p)) {
        ++p;
        continue;
      }
      begin = p;
      continue;
    }

    if (*p != ',') {
      ++p;
      continue;
    }

    if (p > begin) {
      sr = _tables_add_table_type(tables, TableType, NameLength4, begin, p);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
    }
    begin = NULL;
    ++p;
  }

  sr = _tables_add_table_type(tables, TableType, NameLength4, begin, p);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  return SQL_SUCCESS;
}

SQLRETURN tables_open(
    tables_t      *tables,
    SQLCHAR       *CatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *SchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *TableName,
    SQLSMALLINT    NameLength3,
    SQLCHAR       *TableType,
    SQLSMALLINT    NameLength4)
{
  SQLRETURN sr = SQL_SUCCESS;

  tables_reset(tables);

  if (CatalogName && NameLength1 == SQL_NTS) NameLength1 = (SQLSMALLINT)strlen((const char*)CatalogName);
  if (SchemaName && NameLength2 == SQL_NTS)  NameLength2 = (SQLSMALLINT)strlen((const char*)SchemaName);
  if (TableName && NameLength3 == SQL_NTS)   NameLength3 = (SQLSMALLINT)strlen((const char*)TableName);
  if (TableType && NameLength4 == SQL_NTS)   NameLength4 = (SQLSMALLINT)strlen((const char*)TableType);

  OW("CatalogName:%p,%.*s", CatalogName, (int)NameLength1, CatalogName);
  OW("SchemaName:%p,%.*s", SchemaName, (int)NameLength2, SchemaName);
  OW("TableName:%p,%.*s", TableName, (int)NameLength3, TableName);
  OW("TableType:%p,%.*s", TableType, (int)NameLength4, TableType);

  if (CatalogName && strncmp((const char*)CatalogName, SQL_ALL_CATALOGS, 1) == 0) {
    if ((!SchemaName || !*SchemaName) && (!TableName || !*TableName)) {
      return _tables_open_catalogs(tables);
    }
  }
  if (SchemaName && strncmp((const char*)SchemaName, SQL_ALL_SCHEMAS, 1) == 0) {
    if ((!CatalogName || !*CatalogName) && (!TableName || !*TableName)) {
      return _tables_open_schemas(tables);
    }
  }
  if (TableType && strncmp((const char*)TableType, SQL_ALL_TABLE_TYPES, 1) == 0) {
    if ((!CatalogName || !*CatalogName) && (!SchemaName || !*SchemaName) && (!TableName || !*TableName)) {
      return _tables_open_tabletypes(tables);
    }
  }

  charset_conv_t *cnv = &tables->owner->conn->cnv_sql_c_char_to_sql_c_wchar;
  if (CatalogName) {
    if (mem_conv(&tables->catalog_cache, cnv->cnv, (const char*)CatalogName, NameLength1)) {
      stmt_oom(tables->owner);
      return SQL_ERROR;
    }
    tables->catalog = tables->catalog_cache.base;
    if (wildcomp_n_ex(&tables->tables_args.catalog_pattern, cnv->from, (const char*)CatalogName, NameLength1)) {
      stmt_append_err_format(tables->owner, "HY000", 0,
          "General error:wild compile failed for CatalogName[%.*s]", (int)NameLength1, (const char*)CatalogName);
      return SQL_ERROR;
    }
  }
  if (SchemaName) {
    if (mem_conv(&tables->schema_cache, cnv->cnv, (const char*)SchemaName, NameLength2)) {
      stmt_oom(tables->owner);
      return SQL_ERROR;
    }
    tables->schema = tables->schema_cache.base;
    if (wildcomp_n_ex(&tables->tables_args.schema_pattern, cnv->from, (const char*)SchemaName, NameLength2)) {
      stmt_append_err_format(tables->owner, "HY000", 0,
          "General error:wild compile failed for SchemaName[%.*s]", (int)NameLength2, (const char*)SchemaName);
      return SQL_ERROR;
    }
  }
  if (TableName) {
    if (mem_conv(&tables->table_cache, cnv->cnv, (const char*)TableName, NameLength3)) {
      stmt_oom(tables->owner);
      return SQL_ERROR;
    }
    tables->table = tables->table_cache.base;
    if (wildcomp_n_ex(&tables->tables_args.table_pattern, cnv->from, (const char*)TableName, NameLength3)) {
      stmt_append_err_format(tables->owner, "HY000", 0,
          "General error:wild compile failed for TableName[%.*s]", (int)NameLength3, (const char*)TableName);
      return SQL_ERROR;
    }
  }
  if (TableType) {
    if (mem_conv(&tables->type_cache, cnv->cnv, (const char*)TableType, NameLength4)) {
      stmt_oom(tables->owner);
      return SQL_ERROR;
    }
    tables->type = tables->type_cache.base;
    if (wildcomp_n_ex(&tables->tables_args.type_pattern, cnv->from, (const char*)TableType, NameLength4)) {
      stmt_append_err_format(tables->owner, "HY000", 0,
          "General error:wild compile failed for TypeName[%.*s]", (int)NameLength4, (const char*)TableType);
      return SQL_ERROR;
    }
    if (*TableType) {
      sr = _tables_parse_table_type(tables, TableType, NameLength4);
      if (sr != SQL_SUCCESS) return SQL_ERROR;
    }
  }

  const char *sql =
    "select db_name `TABLE_CAT`, '' `TABLE_SCHEM`, stable_name `TABLE_NAME`, 'TABLE' `TABLE_TYPE`, table_comment `REMARKS` from information_schema.ins_stables"
    " "
    "union all"
    " "
    "select db_name `TABLE_CAT`, '' `TABLE_SCHEM`, table_name `TABLE_NAME`,"
    "  case when `type`='SYSTEM_TABLE' then 'SYSTEM TABLE'"
    "       when `type`='NORMAL_TABLE' then 'TABLE'"
    "       when `type`='CHILD_TABLE' then 'TABLE'"
    "       else 'UNKNOWN'"
    "  end `TABLE_TYPE`, table_comment `REMARKS` from information_schema.ins_tables"
    " "
    "order by `TABLE_TYPE`, `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`";

  sr = tsdb_stmt_query(&tables->stmt, sql);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  tables->tables_type = TABLES_FOR_GENERIC;
  return SQL_SUCCESS;
}
