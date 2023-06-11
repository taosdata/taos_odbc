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

void ds_conn_query(ds_conn_t *ds_conn, const char *sql, ds_res_t *ds_res)
{
  OA_NIY(ds_conn->conn);

  ds_res->conn        = ds_conn->conn;
  ds_res->fields.conn = ds_conn->conn;

#ifdef HAVE_TAOSWS           /* { */
  if (ds_conn->conn->cfg.url) {
    WS_TAOS *ws_taos = (WS_TAOS*)ds_conn->taos;
    WS_RES  *res     = ws_query(ws_taos, sql);

    ds_res->res = res;
    if (ws_errno(res)) return;
    if (!res) return;
    ds_res->result_precision = ws_result_precision(res);
    if (ws_errno(res)) return;
    ds_res->fields.nr_fields = ws_field_count(res);
    if (ws_errno(res)) return;
    ds_res->fields.fields = ws_fetch_fields(res);

    return;
  }
#endif                       /* } */

  TAOS *taos    = (TAOS*)ds_conn->taos;
  TAOS_RES *res = CALL_taos_query(taos, sql);

  ds_res->res = res;
  if (taos_errno(res)) return;
  if (!res) return;
  ds_res->result_precision = CALL_taos_result_precision(res);
  if (taos_errno(res)) return;
  ds_res->fields.nr_fields = CALL_taos_field_count(res);
  if (taos_errno(res)) return;
  ds_res->fields.fields = CALL_taos_fetch_fields(res);

  return;
}

const char* ds_conn_get_server_info(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);

#ifdef HAVE_TAOSWS           /* { */
  if (ds_conn->conn->cfg.url) {
    return ws_get_server_info((WS_TAOS*)ds_conn->taos);
  }
#endif                       /* } */

  return CALL_taos_get_server_info((TAOS*)ds_conn->taos);
}

const char* ds_conn_get_client_info(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);

#ifdef HAVE_TAOSWS           /* { */
  if (ds_conn->conn->cfg.url) {
    // FIXME:
    return "client_info-undefined-under-taosws-for-the-moment";
  }
#endif                       /* } */

  return CALL_taos_get_client_info();
}

int ds_conn_get_current_db(ds_conn_t *ds_conn, char *db, size_t len, int *e, const char **errstr)
{
  int r = 0;

  OA_NIY(ds_conn->conn);

  conn_t *conn = ds_conn->conn;

#ifdef HAVE_TAOSWS           /* { */
  if (conn->cfg.url) {
    *e = 0;
    *errstr = "websocket backend not implemented yet";
    return -1;
  }
#endif                       /* } */

  int required = 0; // FIXME: what usage?
  r = CALL_taos_get_current_db((TAOS*)ds_conn->taos, db, (int)len, &required);
  if (r) {
    *e = taos_errno(NULL);
    *errstr = taos_errstr(NULL);
    return -1;
  }

  return 0;
}

void ds_conn_close(ds_conn_t *ds_conn)
{
  if (!ds_conn->conn || !ds_conn->taos) return;

#ifdef HAVE_TAOSWS           /* { */
  if (ds_conn->conn->cfg.url) {
    ws_close((WS_RES*)ds_conn->taos);
    ds_conn->taos = NULL;
    return;
  }
#endif                       /* } */

  CALL_taos_close((TAOS*)ds_conn->taos);
  ds_conn->taos = NULL;
}


void ds_res_close(ds_res_t *ds_res)
{
  OA_NIY(ds_res->conn);

  if (!ds_res->res) return;

#ifdef HAVE_TAOSWS           /* { */
  if (ds_res->conn->cfg.url) {
    ws_free_result((WS_RES*)ds_res->res);
    ds_res->res = NULL;
    return;
  }
#endif                       /* } */

  CALL_taos_free_result((TAOS_RES*)ds_res->res);
  return;
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

int ds_res_block_get_into_tsdb(ds_res_t *ds_res, ds_block_t *ds_block, int i_row, int i_col, tsdb_data_t *tsdb, char *buf, size_t len)
{
#ifdef HAVE_TAOSWS           /* { */
  if (ds_res->conn->cfg.url) {
    WS_RES   *res        = (WS_RES*)ds_res->res;
    WS_FIELD *fields     = (WS_FIELD*)ds_res->fields.fields;
    int result_precision = ds_res->result_precision;

    uint8_t     col_type = 0;
    const void *col_data = NULL;
    uint32_t    col_len  = 0;

    col_data = ws_get_value_in_block(res, i_row, i_col, &col_type, &col_len);

    const WS_FIELD *field = fields + i_col;

    return helper_get_tsdb_impl(result_precision, field->name, col_type, col_data, col_len, i_row, i_col, tsdb, buf, len);
  }
#endif                       /* } */

  int block_mode = 1;
  TAOS_RES     *res    = (TAOS_RES*)ds_res->res;
  TAOS_FIELD   *fields = (TAOS_FIELD*)ds_res->fields.fields;
  TAOS_ROW      block  = (TAOS_ROW)ds_block->block;
  int result_precision = ds_res->result_precision;

  return helper_get_tsdb(res, block_mode, fields, result_precision, block, i_row, i_col, tsdb, buf, len);
}

