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

#include "internal.h"

#include "ds.h"
#include "log.h"

int ds_res_get_result_precision(ds_res_t *ds_res)
{
  OA_NIY(ds_res->conn);

#ifdef HAVE_TAOSWS           /* { */
  if (ds_res->conn->cfg.url) {
    return ws_result_precision((WS_RES*)ds_res->res);
  }
#endif                       /* } */

  return CALL_taos_result_precision((TAOS_RES*)ds_res->res);
}

int ds_res_field_count(ds_res_t *ds_res)
{
  OA_NIY(ds_res->conn);

#ifdef HAVE_TAOSWS           /* { */
  if (ds_res->conn->cfg.url) {
    return ws_field_count((WS_RES*)ds_res->res);
  }
#endif                       /* } */

  return CALL_taos_field_count((TAOS_RES*)ds_res->res);
}

int ds_res_errno(ds_res_t *ds_res)
{
  OA_NIY(ds_res->conn);

#ifdef HAVE_TAOSWS           /* { */
  if (ds_res->conn->cfg.url) {
    return ws_errno((WS_RES*)ds_res->res);
  }
#endif                       /* } */

  return taos_errno((TAOS_RES*)ds_res->res);
}

const char* ds_res_errstr(ds_res_t *ds_res)
{
  OA_NIY(ds_res->conn);

#ifdef HAVE_TAOSWS           /* { */
  if (ds_res->conn->cfg.url) {
    return ws_errstr((WS_RES*)ds_res->res);
  }
#endif                       /* } */

  return taos_errstr((TAOS_RES*)ds_res->res);
}

ds_fields_t* ds_res_fetch_fields(ds_res_t *ds_res, ds_fields_t *fields)
{
  OA_NIY(ds_res->conn);

  fields->conn = ds_res->conn;

#ifdef HAVE_TAOSWS           /* { */
  if (ds_res->conn->cfg.url) {
    fields->fields = ws_fetch_fields((WS_RES*)ds_res->res);
    return fields;
  }
#endif                       /* } */

  fields->fields = CALL_taos_fetch_fields((TAOS_RES*)ds_res->res);
  return fields;
}

int8_t ds_fields_type(ds_fields_t *ds_fields, int i_col)
{
  OA_NIY(ds_fields->conn);

#ifdef HAVE_TAOSWS           /* { */
  if (ds_fields->conn->cfg.url) {
    const WS_FIELD *fields = (const WS_FIELD*)ds_fields->fields;
    return (int8_t)fields[i_col].type;
  }
#endif                       /* } */

  const TAOS_FIELD *fields = (const TAOS_FIELD*)ds_fields->fields;
  return fields[i_col].type;
}

int ds_res_fetch_block(ds_res_t *ds_res, ds_block_t *ds_block)
{
  int r = 0;

  OA_NIY(ds_res->conn);

  ds_block->conn = ds_res->conn;

#ifdef HAVE_TAOSWS           /* { */
  if (ds_res->conn->cfg.url) {
    const void *block = NULL;
    int32_t rows_in_block = 0;
    r = ws_fetch_block((WS_RES*)ds_res->res, &block, &rows_in_block);
    if (r == 0) {
      ds_block->nr_rows_in_block = rows_in_block;
      ds_block->block            = block;
    }
    return r;
  }
#endif                       /* } */

  int rows_in_block = 0;
  TAOS_ROW rows = NULL;
  r = CALL_taos_fetch_block_s((TAOS_RES*)ds_res->res, &rows_in_block, &rows);
  if (r == 0) {
    ds_block->nr_rows_in_block = rows_in_block;
    ds_block->block            = rows;
  }

  return r;
}

