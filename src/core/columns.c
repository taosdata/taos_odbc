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

#include "taos_helpers.h"

void columns_args_release(columns_args_t *args)
{
  if (!args) return;
  WILD_SAFE_FREE(args->catalog_pattern);
  WILD_SAFE_FREE(args->schema_pattern);
  WILD_SAFE_FREE(args->table_pattern);
  WILD_SAFE_FREE(args->column_pattern);
}

void columns_ctx_release(columns_ctx_t *ctx)
{
  if (!ctx) return;
  columns_args_release(&ctx->columns_args);
  if (ctx->table) {
    CALL_taos_free_result(ctx->table);
    ctx->table = NULL;
  }
  ctx->table_fields       = NULL;
  ctx->table_rowset       = NULL;
  ctx->table_rowset_num   = 0;
  ctx->table_rowset_idx   = 0;

  if (ctx->tables) {
    CALL_taos_free_result(ctx->tables);
    ctx->tables = NULL;
  }
  ctx->tables_fields       = NULL;
  ctx->tables_rowset       = NULL;
  ctx->tables_rowset_num   = 0;
  ctx->tables_rowset_idx   = 0;

  ctx->active   = 0;
  ctx->fetching = 0;
  ctx->ending   = 0;
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
