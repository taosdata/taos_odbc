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

static void _ds_res_setup(ds_res_t *ds_res);
static void _ds_block_setup(ds_block_t *ds_block);
static void _ds_fields_setup(ds_fields_t *ds_fields);

#ifdef HAVE_TAOSWS           /* { */
static void _ds_ws_query(ds_conn_t *ds_conn, const char *sql, ds_res_t *ds_res)
{
  OA_NIY(ds_conn->conn);

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

static const char* _ds_ws_get_server_info(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);

  return ws_get_server_info((WS_TAOS*)ds_conn->taos);
}

static const char* _ds_ws_get_client_info(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);

  // FIXME:
  return "client_info-undefined-under-taosws-for-the-moment";
}

static int _ds_ws_get_current_db(ds_conn_t *ds_conn, char *db, size_t len, int *e, const char **errstr)
{
  (void)db;
  (void)len;

  OA_NIY(ds_conn->conn);

  *e = 0;
  *errstr = "websocket backend not implemented yet";
  return -1;
}

static void _ds_ws_close(ds_conn_t *ds_conn)
{
  if (!ds_conn->conn || !ds_conn->taos) return;

  ws_close((WS_RES*)ds_conn->taos);
  ds_conn->taos = NULL;
}
#endif                       /* } */

static void _ds_tsdb_query(ds_conn_t *ds_conn, const char *sql, ds_res_t *ds_res)
{
  OA_NIY(ds_conn->conn);

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

static const char* _ds_tsdb_get_server_info(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);

  return CALL_taos_get_server_info((TAOS*)ds_conn->taos);
}

static const char* _ds_tsdb_get_client_info(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);

  return CALL_taos_get_client_info();
}

static int _ds_tsdb_get_current_db(ds_conn_t *ds_conn, char *db, size_t len, int *e, const char **errstr)
{
  int r = 0;

  OA_NIY(ds_conn->conn);

  int required = 0; // FIXME: what usage?
  r = CALL_taos_get_current_db((TAOS*)ds_conn->taos, db, (int)len, &required);
  if (r) {
    *e = taos_errno(NULL);
    *errstr = taos_errstr(NULL);
    return -1;
  }

  return 0;
}

static void _ds_tsdb_close(ds_conn_t *ds_conn)
{
  if (!ds_conn->conn || !ds_conn->taos) return;
  CALL_taos_close((TAOS*)ds_conn->taos);
  ds_conn->taos = NULL;
}

void ds_conn_setup(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);

#ifdef HAVE_TAOSWS           /* { */
  if (ds_conn->conn->cfg.url) {
    ds_conn->query               = _ds_ws_query;
    ds_conn->get_server_info     = _ds_ws_get_server_info;
    ds_conn->get_client_info     = _ds_ws_get_client_info;
    ds_conn->get_current_db      = _ds_ws_get_current_db;
    ds_conn->close               = _ds_ws_close;
    return;
  }
#endif                       /* } */

  ds_conn->query               = _ds_tsdb_query;
  ds_conn->get_server_info     = _ds_tsdb_get_server_info;
  ds_conn->get_client_info     = _ds_tsdb_get_client_info;
  ds_conn->get_current_db      = _ds_tsdb_get_current_db;
  ds_conn->close               = _ds_tsdb_close;
  return;
}

void ds_conn_query(ds_conn_t *ds_conn, const char *sql, ds_res_t *ds_res)
{
  OA_NIY(ds_conn->conn);

  ds_res->ds_conn = ds_conn;
  _ds_res_setup(ds_res);

  ds_conn->query(ds_conn, sql, ds_res);
}

const char* ds_conn_get_server_info(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);

  return ds_conn->get_server_info(ds_conn);
}

const char* ds_conn_get_client_info(ds_conn_t *ds_conn)
{
  OA_NIY(ds_conn->conn);

  return ds_conn->get_client_info(ds_conn);
}

int ds_conn_get_current_db(ds_conn_t *ds_conn, char *db, size_t len, int *e, const char **errstr)
{
  OA_NIY(ds_conn->conn);

  return ds_conn->get_current_db(ds_conn, db, len, e, errstr);
}

void ds_conn_close(ds_conn_t *ds_conn)
{
  return ds_conn->close(ds_conn);
}

#ifdef HAVE_TAOSWS           /* { */
static void _ds_res_ws_close(ds_res_t *ds_res)
{
  WS_RES *res = (WS_RES*)ds_res->res;

  ws_free_result(res);
  ds_res->res = NULL;
}

static int _ds_res_ws_errno(ds_res_t *ds_res)
{
  WS_RES *res = (WS_RES*)ds_res->res;
  return ws_errno(res);
}

static const char* _ds_res_ws_errstr(ds_res_t *ds_res)
{
  WS_RES *res = (WS_RES*)ds_res->res;
  return ws_errstr(res);
}

static int _ds_res_ws_fetch_block(ds_res_t *ds_res)
{
  int r = 0;

  WS_RES *res = (WS_RES*)ds_res->res;

  const void *block = NULL;
  int32_t rows_in_block = 0;
  r = ws_fetch_block(res, &block, &rows_in_block);
  if (r == 0) {
    ds_block_t *ds_block = &ds_res->block;
    ds_block->nr_rows_in_block = rows_in_block;
    ds_block->block            = block;
  }

  return r;
}

static int8_t _ds_fields_ws_field_type(ds_fields_t *ds_fields, int i_col)
{
  const WS_FIELD *fields = (const WS_FIELD*)ds_fields->fields;
  return (int8_t)fields[i_col].type;
}

static int _ds_block_ws_get_into_tsdb(ds_block_t *ds_block, int i_row, int i_col, tsdb_data_t *tsdb, char *buf, size_t len)
{
  ds_res_t *ds_res = ds_block->ds_res;

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

static void _ds_res_tsdb_close(ds_res_t *ds_res)
{
  TAOS_RES *res = (TAOS_RES*)ds_res->res;
  CALL_taos_free_result(res);
  ds_res->res = NULL;
}

static int _ds_res_tsdb_errno(ds_res_t *ds_res)
{
  TAOS_RES *res = (TAOS_RES*)ds_res->res;
  return taos_errno(res);
}

static const char* _ds_res_tsdb_errstr(ds_res_t *ds_res)
{
  TAOS_RES *res = (TAOS_RES*)ds_res->res;
  return taos_errstr(res);
}

static int _ds_res_tsdb_fetch_block(ds_res_t *ds_res)
{
  int r = 0;

  TAOS_RES *res = (TAOS_RES*)ds_res->res;

  int rows_in_block = 0;
  TAOS_ROW rows = NULL;
  r = CALL_taos_fetch_block_s(res, &rows_in_block, &rows);
  if (r == 0) {
    ds_block_t *ds_block = &ds_res->block;
    ds_block->nr_rows_in_block = rows_in_block;
    ds_block->block            = rows;
  }

  return r;
}

static int8_t _ds_fields_tsdb_field_type(ds_fields_t *ds_fields, int i_col)
{
  const TAOS_FIELD *fields = (const TAOS_FIELD*)ds_fields->fields;
  return fields[i_col].type;
}

static int _ds_block_tsdb_get_into_tsdb(ds_block_t *ds_block, int i_row, int i_col, tsdb_data_t *tsdb, char *buf, size_t len)
{
  ds_res_t *ds_res = ds_block->ds_res;

  int block_mode = 1;
  TAOS_RES     *res    = (TAOS_RES*)ds_res->res;
  TAOS_FIELD   *fields = (TAOS_FIELD*)ds_res->fields.fields;
  TAOS_ROW      block  = (TAOS_ROW)ds_block->block;
  int result_precision = ds_res->result_precision;

  return helper_get_tsdb(res, block_mode, fields, result_precision, block, i_row, i_col, tsdb, buf, len);
}

static void _ds_res_setup(ds_res_t *ds_res)
{
  OA_NIY(ds_res->ds_conn);
  OA_NIY(ds_res->ds_conn->conn);

  conn_t *conn = ds_res->ds_conn->conn;

  ds_res->fields.ds_res  = ds_res;
  _ds_fields_setup(&ds_res->fields);

  ds_res->block.ds_res = ds_res;
  _ds_block_setup(&ds_res->block);

#ifdef HAVE_TAOSWS           /* { */
  if (conn->cfg.url) {
    ds_res->close             = _ds_res_ws_close;
    ds_res->errno             = _ds_res_ws_errno;
    ds_res->errstr            = _ds_res_ws_errstr;
    ds_res->fetch_block       = _ds_res_ws_fetch_block;
    return;
  }
#endif                       /* } */

  ds_res->close             = _ds_res_tsdb_close;
  ds_res->errno             = _ds_res_tsdb_errno;
  ds_res->errstr            = _ds_res_tsdb_errstr;
  ds_res->fetch_block       = _ds_res_tsdb_fetch_block;
  return;
}

void ds_res_close(ds_res_t *ds_res)
{
  if (!ds_res->res) return;

  ds_res->close(ds_res);
}

int ds_res_errno(ds_res_t *ds_res)
{
  OA_NIY(ds_res->ds_conn);

  return ds_res->errno(ds_res);
}

const char* ds_res_errstr(ds_res_t *ds_res)
{
  OA_NIY(ds_res->ds_conn);

  return ds_res->errstr(ds_res);
}

int ds_res_fetch_block(ds_res_t *ds_res)
{
  OA_NIY(ds_res->ds_conn);
  OA_NIY(ds_res->block.ds_res == ds_res);

  return ds_res->fetch_block(ds_res);
}

static void _ds_fields_setup(ds_fields_t *ds_fields)
{
  OA_NIY(ds_fields->ds_res);

  ds_res_t *ds_res = ds_fields->ds_res;
  OA_NIY(ds_res->ds_conn);
  OA_NIY(ds_res->ds_conn->conn);

#ifdef HAVE_TAOSWS           /* { */
  if (ds_res->ds_conn->conn->cfg.url) {
    ds_fields->field_type = _ds_fields_ws_field_type;
    return;
  }
#endif                       /* } */

  ds_fields->field_type = _ds_fields_tsdb_field_type;
  return;
}

int8_t ds_fields_field_type(ds_fields_t *ds_fields, int i_col)
{
  OA_NIY(ds_fields->ds_res);

  return ds_fields->field_type(ds_fields, i_col);
}

static void _ds_block_setup(ds_block_t *ds_block)
{
  OA_NIY(ds_block->ds_res);

  ds_res_t *ds_res = ds_block->ds_res;
  OA_NIY(ds_res->ds_conn);
  OA_NIY(ds_res->ds_conn->conn);

#ifdef HAVE_TAOSWS           /* { */
  if (ds_res->ds_conn->conn->cfg.url) {
    ds_block->get_into_tsdb   = _ds_block_ws_get_into_tsdb;
    return;
  }
#endif                       /* } */

  ds_block->get_into_tsdb   = _ds_block_tsdb_get_into_tsdb;
  return;
}

int ds_block_get_into_tsdb(ds_block_t *ds_block, int i_row, int i_col, tsdb_data_t *tsdb, char *buf, size_t len)
{
  OA_NIY(ds_block->get_into_tsdb);
  return ds_block->get_into_tsdb(ds_block, i_row, i_col, tsdb, buf, len);
}

