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

#include "charset.h"
#include "conn.h"
#include "desc.h"
#include "env.h"
#include "errs.h"
#include "log.h"
#include "conn_parser.h"
#include "stmt.h"
#include "taos_helpers.h"
#include "taos_odbc_config.h"
#include "tls.h"

#include <odbcinst.h>
#include <string.h>

void conn_cfg_release(conn_cfg_t *conn_cfg)
{
  if (!conn_cfg) return;

  TOD_SAFE_FREE(conn_cfg->driver);
  TOD_SAFE_FREE(conn_cfg->dsn);
  TOD_SAFE_FREE(conn_cfg->uid);
  TOD_SAFE_FREE(conn_cfg->pwd);
  TOD_SAFE_FREE(conn_cfg->ip);
  TOD_SAFE_FREE(conn_cfg->db);

  memset(conn_cfg, 0, sizeof(*conn_cfg));
}

void conn_cfg_transfer(conn_cfg_t *from, conn_cfg_t *to)
{
  if (from == to) return;
  conn_cfg_release(to);
  *to = *from;
  memset(from, 0, sizeof(*from));
}

static void _conn_init(conn_t *conn, env_t *env)
{
  conn->env = env_ref(env);
  int prev = atomic_fetch_add(&env->conns, 1);
  OA_ILE(prev >= 0);

  errs_init(&conn->errs);

  conn->refc = 1;
}

static void _conn_release_information_schema_ins_configs(conn_t *conn)
{
  TOD_SAFE_FREE(conn->s_statusInterval);
  TOD_SAFE_FREE(conn->s_timezone);
  TOD_SAFE_FREE(conn->s_locale);
  TOD_SAFE_FREE(conn->s_charset);
  TOD_SAFE_FREE(conn->sql_c_char_charset);
  TOD_SAFE_FREE(conn->tsdb_varchar_charset);
}

static void _conn_release_iconvs(conn_t *conn)
{
  charset_conv_release(&conn->_cnv_tsdb_varchar_to_sql_c_char);
  charset_conv_release(&conn->_cnv_tsdb_varchar_to_sql_c_wchar);

  charset_conv_release(&conn->_cnv_sql_c_char_to_tsdb_varchar);
  charset_conv_release(&conn->_cnv_sql_c_char_to_sql_c_wchar);
}

static void _conn_release(conn_t *conn)
{
  OA_ILE(conn->taos == NULL);

  int prev = atomic_fetch_sub(&conn->env->conns, 1);
  OA_ILE(prev >= 1);
  int stmts = atomic_load(&conn->stmts);
  OA_ILE(stmts == 0);
  env_unref(conn->env);
  conn->env = NULL;

  conn_cfg_release(&conn->cfg);
  _conn_release_information_schema_ins_configs(conn);
  _conn_release_iconvs(conn);

  errs_release(&conn->errs);

  return;
}

conn_t* conn_create(env_t *env)
{
  conn_t *conn = (conn_t*)calloc(1, sizeof(*conn));
  if (!conn) {
    env_oom(env);
    return NULL;
  }

  _conn_init(conn, env);

  return conn;
}

conn_t* conn_ref(conn_t *conn)
{
  int prev = atomic_fetch_add(&conn->refc, 1);
  OA_ILE(prev>0);
  return conn;
}

conn_t* conn_unref(conn_t *conn)
{
  int prev = atomic_fetch_sub(&conn->refc, 1);
  if (prev>1) return conn;
  OA_ILE(prev==1);

  _conn_release(conn);
  free(conn);

  return NULL;
}

SQLRETURN conn_free(conn_t *conn)
{
  int outstandings = atomic_load(&conn->outstandings);
  if (outstandings) {
    conn_append_err_format(conn, "HY000", 0, "General error:%d outstandings still active", outstandings);
    return SQL_ERROR;
  }

  int stmts = atomic_load(&conn->stmts);
  if (stmts) {
    conn_append_err_format(conn, "HY000", 0, "General error:%d statements are still outstanding", stmts);
    return SQL_ERROR;
  }

  int descs = atomic_load(&conn->descs);
  if (descs) {
    conn_append_err_format(conn, "HY000", 0, "General error:%d descriptors are still outstanding", descs);
    return SQL_ERROR;
  }

  conn_unref(conn);
  return SQL_SUCCESS;
}

static int _conn_save_from_information_schema_ins_configs(conn_t *conn, int name_len, const char *name, int value_len, const char *value)
{
#ifdef _LOCAL_
#error choose another name for local usage
#endif

#define _LOCAL_(x) do {                                                        \
  if (name_len == sizeof(#x) - 1 && strncmp(#x, name, name_len) == 0) {        \
    TOD_SAFE_FREE(conn->s_##x);                                                \
    conn->s_##x = strndup(value, value_len);                                   \
    if (!conn->s_##x) return -1;                                               \
    return 0;                                                                  \
  }                                                                            \
} while (0)

  _LOCAL_(statusInterval);
  _LOCAL_(timezone);
  _LOCAL_(locale);
  _LOCAL_(charset);

#undef _LOCAL_
  return 0;
}

static SQLRETURN _conn_get_configs_from_information_schema_ins_configs_with_res(conn_t *conn, TAOS_RES *res)
{
  int numOfRows                = 0;
  TAOS_ROW rows                = NULL;
  TAOS_FIELD *fields           = NULL;
  int nr_fields                = 0;

  int time_precision = CALL_taos_result_precision(res);

  int r = 0;
  nr_fields = CALL_taos_field_count(res);
  if (nr_fields == -1) {
    int err = taos_errno(res);
    const char *s = taos_errstr(res);
    conn_append_err_format(conn, "HY000", 0,
        "General error:failed to fetch # of fields from `information_schema.ins_configs`:[%d]%s", err, s);
    return SQL_ERROR;
  }
  if (nr_fields != 2) {
    conn_append_err_format(conn, "HY000", 0,
        "General error:# of fields from `information_schema.ins_configs` is expected 2 but got ==%d==", nr_fields);
    return SQL_ERROR;
  }
  fields = CALL_taos_fetch_fields(res);
  if (!fields) {
    int err = taos_errno(res);
    const char *s = taos_errstr(res);
    conn_append_err_format(conn, "HY000", 0,
        "General error:failed to fetch fields from `information_schema.ins_configs`:[%d]%s", err, s);
    return SQL_ERROR;
  }
  for (int i=0; i<nr_fields; ++i) {
    if (fields[i].type != TSDB_DATA_TYPE_VARCHAR) {
      conn_append_err_format(conn, "HY000", 0,
          "General error:field[#%d] from `information_schema.ins_configs` is expected `TSDB_DATA_TYPE_VARCHAR` but got ==%s==",
          i + 1, taos_data_type(fields[0].type));
      return SQL_ERROR;
    }
  }
  r = CALL_taos_fetch_block_s(res, &numOfRows, &rows);
  if (r) {
    int err = taos_errno(res);
    const char *s = taos_errstr(res);
    conn_append_err_format(conn, "HY000", 0,
        "General error:failed to fetch block from `information_schema.ins_configs`:[%d]%s", err, s);
    return SQL_ERROR;
  }
  for (int i=0; i<numOfRows; ++i) {
    char buf[4096];

    tsdb_data_t name = {0};
    r = helper_get_tsdb(res, 1, fields, time_precision, rows, i, 0, &name, buf, sizeof(buf));
    if (r) {
      conn_append_err_format(conn, "HY000", 0, "General error:%.*s", (int)strlen(buf), buf);
      return SQL_ERROR;
    }
    if (name.type != TSDB_DATA_TYPE_VARCHAR || name.is_null) {
      conn_append_err(conn, "HY000", 0, "General error:internal logic error or taosc conformance issue");
      return SQL_ERROR;
    }

    tsdb_data_t value = {0};
    r = helper_get_tsdb(res, 1, fields, time_precision, rows, i, 1, &value, buf, sizeof(buf));
    if (r) {
      conn_append_err_format(conn, "HY000", 0, "General error:%.*s", (int)strlen(buf), buf);
      return SQL_ERROR;
    }
    if (value.type != TSDB_DATA_TYPE_VARCHAR || value.is_null) {
      conn_append_err(conn, "HY000", 0, "General error:internal logic error or taosc conformance issue");
      return SQL_ERROR;
    }

    r = _conn_save_from_information_schema_ins_configs(conn, (int)name.str.len, name.str.str, (int)value.str.len, value.str.str);
    if (r) {
      conn_append_err_format(conn, "HY000", 0,
          "General error: save `%.*s:%.*s` failed", (int)name.str.len, name.str.str, (int)value.str.len, value.str.str);
      return SQL_ERROR;
    }
  }
  return SQL_SUCCESS;
}

static SQLRETURN _conn_get_configs_from_information_schema_ins_configs(conn_t *conn)
{
#ifdef FAKE_TAOS            /* { */
  if (1) return SQL_SUCCESS;
#endif                      /* } */

  TAOS *taos = conn->taos;
  OA_ILE(taos);
  const char *sql = "select * from information_schema.ins_configs";
  TAOS_RES *res = CALL_taos_query(taos, sql);
  if (!res) {
    int err = taos_errno(NULL);
    const char *s = taos_errstr(NULL);
    conn_append_err_format(conn, "HY000", 0,
        "General error:failed to query `information_schema.ins_configs`:[%d]%s", err, s);
    return SQL_ERROR;
  }
  SQLRETURN sr = _conn_get_configs_from_information_schema_ins_configs_with_res(conn, res);
  CALL_taos_free_result(res);
  return sr;
}

static int _conn_setup_iconvs(conn_t *conn)
{
  _conn_release_iconvs(conn);
  // FIXME: we know conn->s_charset is actually server-side config rather than client-side
  const char *tsdb_charset = conn->s_charset;
  const char *sql_c_charset = tod_get_sql_c_charset();
  if (!sql_c_charset) {
    conn_append_err_format(conn, "HY000", 0, "General error:current locale_or_ACP [%s]:not implemented yet", tod_get_locale_or_ACP());
    return -1;
  }

#ifdef FAKE_TAOS            /* { */
  sql_c_charset = "GB18030";
  tsdb_charset = "UTF-8";
#endif                      /* } */
  conn->sql_c_char_charset = strdup(sql_c_charset);
  conn->tsdb_varchar_charset = strdup(tsdb_charset);
  if (!conn->sql_c_char_charset || !conn->tsdb_varchar_charset) {
    conn_oom(conn);
    return -1;
  }

  sql_c_charset = conn->sql_c_char_charset;
  tsdb_charset = conn->tsdb_varchar_charset;

  const char *from, *to;
  charset_conv_t *cnv;
  do {
    do {
      cnv = &conn->_cnv_sql_c_char_to_tsdb_varchar;
      from = sql_c_charset; to = tsdb_charset;
      if (charset_conv_reset(cnv, from, to)) break;

      cnv = &conn->_cnv_sql_c_char_to_sql_c_wchar;
      from = sql_c_charset; to = "UCS-2LE";
      if (charset_conv_reset(cnv, from, to)) break;

      cnv = &conn->_cnv_tsdb_varchar_to_sql_c_char;
      from = tsdb_charset; to = sql_c_charset;
      if (charset_conv_reset(cnv, from, to)) break;

      cnv = &conn->_cnv_tsdb_varchar_to_sql_c_wchar;
      from = tsdb_charset; to = "UCS-2LE";
      if (charset_conv_reset(cnv, from, to)) break;

      return 0;
    } while (0);

    _conn_release_iconvs(conn);
    conn_append_err_format(conn, "HY000", 0,
        "General error:conversion between current locale_or_ACP [%s], charset %s <=> charset [%s] not supported yet",
        tod_get_locale_or_ACP(), sql_c_charset, tsdb_charset);
    return -1;
  } while (0);

  _conn_release_iconvs(conn);
  conn_append_err_format(conn, "HY000", 0,
      "General error:conversion between charsets [%s] <=> [%s] not supported yet",
      from, to);
  return -1;
}

static int _conn_get_timezone_from_res(conn_t *conn, const char *sql, TAOS_RES *res)
{
#ifdef FAKE_TAOS            /* { */
  conn->tz = 800;
  conn->tz_seconds = 28800;
  return 0;
#else                       /* }{ */
  int numOfRows                = 0;
  TAOS_ROW rows                = NULL;
  TAOS_FIELD *fields           = NULL;
  int nr_fields                = 0;

  int time_precision = CALL_taos_result_precision(res);

  int r = 0;
  nr_fields = CALL_taos_field_count(res);
  if (nr_fields == -1) {
    int err = taos_errno(res);
    const char *s = taos_errstr(res);
    conn_append_err_format(conn, "HY000", 0,
        "General error:failed to fetch # of fields from `%s`:[%d]%s", sql, err, s);
    return -1;
  }
  if (nr_fields != 1) {
    conn_append_err_format(conn, "HY000", 0,
        "General error:# of fields from `%s` is expected 2 but got ==%d==", sql, nr_fields);
    return -1;
  }
  fields = CALL_taos_fetch_fields(res);
  if (!fields) {
    int err = taos_errno(res);
    const char *s = taos_errstr(res);
    conn_append_err_format(conn, "HY000", 0,
        "General error:failed to fetch fields from `%s`:[%d]%s", sql, err, s);
    return -1;
  }
  if (fields[0].type != TSDB_DATA_TYPE_VARCHAR) {
    conn_append_err_format(conn, "HY000", 0,
        "General error:field[#%d] from `%s` is expected `TSDB_DATA_TYPE_VARCHAR` but got ==%s==",
        1, sql, taos_data_type(fields[0].type));
    return -1;
  }
  r = CALL_taos_fetch_block_s(res, &numOfRows, &rows);
  if (!rows) {
    int err = taos_errno(res);
    const char *s = taos_errstr(res);
    conn_append_err_format(conn, "HY000", 0,
        "General error:failed to fetch block from `%s`:[%d]%s", sql, err, s);
    return -1;
  }

  char buf[4096];

  tsdb_data_t ts = {0};
  r = helper_get_tsdb(res, 1, fields, time_precision, rows, 0, 0, &ts, buf, sizeof(buf));
  if (r) {
    conn_append_err_format(conn, "HY000", 0, "General error:%.*s", (int)strlen(buf), buf);
    return SQL_ERROR;
  }
  if (ts.type != TSDB_DATA_TYPE_VARCHAR || ts.is_null) {
    conn_append_err(conn, "HY000", 0, "General error:internal logic error or taosc conformance issue");
    return SQL_ERROR;
  }

  if (ts.str.len != 24 || (ts.str.str[19] != '+' && ts.str.str[19] != '-')) {
    conn_append_err_format(conn, "HY000", 0,
        "General error:format for `%.*s` not implemented yet",
        (int)ts.str.len, ts.str.str);
    return -1;
  }

  snprintf(buf, sizeof(buf), "%.*s", 5, ts.str.str + 19);
  int tz = 0;
  int n = sscanf(buf, "%d", &tz);
  if (n!=1) {
    conn_append_err_format(conn, "HY000", 0,
        "General error:format for `%.*s` not implemented yet",
        (int)ts.str.len, ts.str.str);
    return -1;
  }
  buf[0] = '\0';
  snprintf(buf, sizeof(buf), "%+05d", tz);
  if (strncmp(buf, ts.str.str + 19, 5)) {
    conn_append_err_format(conn, "HY000", 0,
        "General error:format for `%.*s` not implemented yet",
        (int)ts.str.len, ts.str.str);
    return -1;
  }

  conn->tz = tz; // +0800 for Asia/Shanghai
  conn->tz_seconds = (tz / 100) * 60 * 60 + (tz % 100) * 60;

  return 0;
#endif                      /* } */
}

static int _conn_get_timezone(conn_t *conn)
{
  const char *sql = "select to_iso8601(0) as ts";
  TAOS_RES *res = CALL_taos_query(conn->taos, sql);
  if (!res) {
    int err = taos_errno(NULL);
    const char *s = taos_errstr(NULL);
    conn_append_err_format(conn, "HY000", 0,
        "General error:failed to query `%s`:[%d]%s", sql, err, s);
    return -1;
  }
  int r = _conn_get_timezone_from_res(conn, sql, res);
  CALL_taos_free_result(res);
  return r;
}

static SQLRETURN _do_conn_connect(conn_t *conn)
{
  const conn_cfg_t *cfg = &conn->cfg;

  conn->taos = CALL_taos_connect(cfg->ip, cfg->uid, cfg->pwd, cfg->db, cfg->port);
  if (!conn->taos) {
    char buf[1024];
    fixed_buf_t buffer = {0};
    buffer.buf = buf;
    buffer.cap = sizeof(buf);
    buffer.nr  = 0;
    int n = 0;
    fixed_buf_sprintf(n, &buffer, "taos_odbc://");
    if (cfg->uid) fixed_buf_sprintf(n, &buffer, "%s:*@", cfg->uid);
    if (cfg->ip) {
      if (cfg->port) {
        fixed_buf_sprintf(n, &buffer, "%s:%d", cfg->ip, cfg->port);
      } else {
        fixed_buf_sprintf(n, &buffer, "%s", cfg->ip);
      }
    } else {
      fixed_buf_sprintf(n, &buffer, "localhost");
    }
    if (cfg->db) fixed_buf_sprintf(n, &buffer, "/%s", cfg->db);

    conn_append_err_format(conn, "08001", CALL_taos_errno(NULL), "Client unable to establish connection:[%s][%s]", buffer.buf, CALL_taos_errstr(NULL));
    return SQL_ERROR;
  }

  conn->svr_info = CALL_taos_get_server_info(conn->taos);
  do {
    SQLRETURN sr;
    int r;
    sr = _conn_get_configs_from_information_schema_ins_configs(conn);
    if (sr == SQL_ERROR) break;
    r = _conn_setup_iconvs(conn);
    if (r) break;
    r = _conn_get_timezone(conn);
    if (r) break;
    if (0) {
      r = tls_leakage_potential();
      if (r) {
        env_oom(conn->env);
        break;
      }
    }
    if (0) {
      const char *from = "UTF-8";
      const char *to   = "GB18030";
      charset_conv_t *cnv = tls_get_charset_conv(from, to);
      if (!cnv) {
        env_oom(conn->env);
        break;
      }
    }
    return SQL_SUCCESS;
  } while (0);
  conn_disconnect(conn);
  return SQL_ERROR;
}

static void _conn_fill_out_connection_str(
    conn_t         *conn,
    SQLCHAR        *OutConnectionString,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLength2Ptr)
{
  char *p = (char*)OutConnectionString;
  if (BufferLength > 0) *p = '\0';
  size_t count = 0;
  int n = 0;

  fixed_buf_t buffer = {0};
  buffer.buf = p;
  buffer.cap = BufferLength;
  buffer.nr = 0;
  if (conn->cfg.driver) {
    fixed_buf_sprintf(n, &buffer, "Driver={%s};", conn->cfg.driver);
  } else {
    fixed_buf_sprintf(n, &buffer, "DSN=%s;", conn->cfg.dsn);
  }
  if (n>0) count += n;

  if (conn->cfg.uid) {
    fixed_buf_sprintf(n, &buffer, "UID=%s;", conn->cfg.uid);
  } else {
    fixed_buf_sprintf(n, &buffer, "UID=;");
  }
  if (n>0) count += n;

  // if (conn->cfg.pwd) {
  //   fixed_buf_sprintf(n, &buffer, "PWD=*;");
  // } else {
  //   fixed_buf_sprintf(n, &buffer, "PWD=;");
  // }
  // if (n>0) count += n;

  if (conn->cfg.ip) {
    if (conn->cfg.port) {
      fixed_buf_sprintf(n, &buffer, "Server=%s:%d;", conn->cfg.ip, conn->cfg.port);
    } else {
      fixed_buf_sprintf(n, &buffer, "Server=%s;", conn->cfg.ip);
    }
  } else {
    fixed_buf_sprintf(n, &buffer, "Server=;");
  }
  if (n>0) count += n;

  if (conn->cfg.db) {
    fixed_buf_sprintf(n, &buffer, "DB=%s;", conn->cfg.db);
  } else {
    fixed_buf_sprintf(n, &buffer, "DB=;");
  }
  if (n>0) count += n;

  if (conn->cfg.unsigned_promotion) {
    fixed_buf_sprintf(n, &buffer, "UNSIGNED_PROMOTION=1;");
  } else {
    fixed_buf_sprintf(n, &buffer, "UNSIGNED_PROMOTION=0;");
  }
  if (n>0) count += n;

  if (conn->cfg.timestamp_as_is) {
    fixed_buf_sprintf(n, &buffer, "TIMESTAMP_AS_IS=1;");
  } else {
    fixed_buf_sprintf(n, &buffer, "TIMESTAMP_AS_IS=0;");
  }
  if (n>0) count += n;

  if (buffer.nr+1 == buffer.cap) {
    char *x = buffer.buf + buffer.nr;
    for (int i=0; i<3 && x>buffer.buf; ++i, --x) x[-1] = '.';
  }

  if (StringLength2Ptr) {
    *StringLength2Ptr = (SQLSMALLINT)count;
  }
}

int conn_cfg_init_other_fields(conn_cfg_t *cfg)
{
  char buf[1024];
  buf[0] = '\0';

  if (!cfg->dsn) return 0;

  int r = 0;
  if (!cfg->unsigned_promotion_set) {
    buf[0] = '\0';
    r = SQLGetPrivateProfileString((LPCSTR)cfg->dsn, "UNSIGNED_PROMOTION", (LPCSTR)"0", (LPSTR)buf, sizeof(buf), "Odbc.ini");
    if (r == 1) cfg->unsigned_promotion = !!atoi(buf);
  }

  r = 0;
  if (!cfg->timestamp_as_is_set) {
    buf[0] = '\0';
    r = SQLGetPrivateProfileString((LPCSTR)cfg->dsn, "TIMESTAMP_AS_IS", (LPCSTR)"0", (LPSTR)buf, sizeof(buf), "Odbc.ini");
    if (r == 1) cfg->timestamp_as_is = !!atoi(buf);
  }

  if (!cfg->pwd) {
    buf[0] = '\0';
    r = SQLGetPrivateProfileString((LPCSTR)cfg->dsn, "PWD", (LPCSTR)"", (LPSTR)buf, sizeof(buf), "Odbc.ini");
    if (buf[0]) {
      cfg->pwd = strdup(buf);
      if (!cfg->pwd) return -1;
    }
  }

  if (!cfg->uid) {
    buf[0] = '\0';
    r = SQLGetPrivateProfileString((LPCSTR)cfg->dsn, "UID", (LPCSTR)"", (LPSTR)buf, sizeof(buf), "Odbc.ini");
    if (buf[0]) {
      cfg->uid = strdup(buf);
      if (!cfg->uid) return -1;
    }
  }

  if (!cfg->db) {
    buf[0] = '\0';
    r = SQLGetPrivateProfileString((LPCSTR)cfg->dsn, "DB", (LPCSTR)"", (LPSTR)buf, sizeof(buf), "Odbc.ini");
    if (buf[0]) {
      cfg->db = strdup(buf);
      if (!cfg->db) return -1;
    }
  }

  return 0;
}

SQLRETURN conn_driver_connect(
    conn_t         *conn,
    SQLHWND         WindowHandle,
    SQLCHAR        *InConnectionString,
    SQLSMALLINT     StringLength1,
    SQLCHAR        *OutConnectionString,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLength2Ptr,
    SQLUSMALLINT    DriverCompletion)
{
  (void)WindowHandle;
  SQLRETURN sr = SQL_SUCCESS;

  if (StringLength1 == SQL_NTS) StringLength1 = (SQLSMALLINT)strlen((const char*)InConnectionString);

  switch (DriverCompletion) {
    case SQL_DRIVER_NOPROMPT:
      break;
    case SQL_DRIVER_COMPLETE:
      break;
    default:
      conn_append_err_format(conn, "HY000", 0,
          "General error:`%s[%d/0x%x]` not supported yet",
          sql_driver_completion(DriverCompletion), DriverCompletion, DriverCompletion);
      return SQL_ERROR;
  }

  conn_parser_param_t param = {0};
  param.ctx.debug_flex  = env_get_debug_flex(conn->env);
  param.ctx.debug_bison = env_get_debug_bison(conn->env);

  int r = conn_parser_parse((const char*)InConnectionString, StringLength1, &param);

  do {
    if (r) {
      conn_append_err_format(conn, "HY000", 0, "General error:parsing:%.*s", StringLength1, (const char*)InConnectionString);
      conn_append_err_format(conn, "HY000", 0, "General error:location:(%d,%d)->(%d,%d)", param.ctx.row0, param.ctx.col0, param.ctx.row1, param.ctx.col1);
      conn_append_err_format(conn, "HY000", 0, "General error:failed:%.*s", (int)strlen(param.ctx.err_msg), param.ctx.err_msg);
      conn_append_err(conn, "HY000", 0, "General error:supported connection string syntax:[<key[=<val>]>]+");
      break;
    }

    conn_cfg_transfer(&param.conn_cfg, &conn->cfg);

    r = conn_cfg_init_other_fields(&conn->cfg);
    if (r) {
      conn_oom(conn);
      break;
    }

    sr = _do_conn_connect(conn);
    if (!sql_succeeded(sr)) break;

    _conn_fill_out_connection_str(conn, OutConnectionString, BufferLength, StringLength2Ptr);

    conn_parser_param_release(&param);
    return SQL_SUCCESS;
  } while (0);

  conn_parser_param_release(&param);
  return SQL_ERROR;
}

void conn_disconnect(conn_t *conn)
{
  int stmts = atomic_load(&conn->stmts);
  OA_ILE(stmts == 0);

  if (conn->taos) {
    CALL_taos_close(conn->taos);
    conn->taos = NULL;
  }
  conn_cfg_release(&conn->cfg);
}

static SQLRETURN _conn_commit(conn_t *conn)
{
  int outstandings = atomic_load(&conn->outstandings);
  if (outstandings == 0) {
    conn_append_err_format(conn, "01000", 0, "General warning:no outstanding connection");
    return SQL_SUCCESS_WITH_INFO;
  }

  conn_append_err_format(conn, "25S02", 0, "Transaction is still active");
  return SQL_ERROR;
}

static int _conn_rollback(conn_t *conn)
{
  int outstandings = atomic_load(&conn->outstandings);
  if (outstandings == 0) {
    conn_append_err_format(conn, "01000", 0, "General warning:no outstandings");
    return SQL_SUCCESS_WITH_INFO;
  }

  conn_append_err_format(conn, "25S01", 0, "Transaction state unknown");
  return SQL_ERROR;
}

SQLRETURN conn_get_diag_rec(
    conn_t         *conn,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr)
{
  return errs_get_diag_rec(&conn->errs, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
}

SQLRETURN conn_alloc_stmt(conn_t *conn, SQLHANDLE *OutputHandle)
{
  *OutputHandle = SQL_NULL_HANDLE;

  stmt_t *stmt = stmt_create(conn);
  if (stmt == NULL) return SQL_ERROR;

  *OutputHandle = (SQLHANDLE)stmt;
  return SQL_SUCCESS;
}

SQLRETURN conn_alloc_desc(conn_t *conn, SQLHANDLE *OutputHandle)
{
  desc_t *desc = desc_create(conn);
  if (!desc) return SQL_ERROR;

  *OutputHandle = (SQLHANDLE)desc;
  return SQL_SUCCESS;
}

SQLRETURN conn_connect(
    conn_t        *conn,
    SQLCHAR       *ServerName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *UserName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *Authentication,
    SQLSMALLINT    NameLength3)
{
  int r = 0;

  if (NameLength1 == SQL_NTS) NameLength1 = ServerName ? (SQLSMALLINT)strlen((const char*)ServerName) : 0;
  if (NameLength2 == SQL_NTS) NameLength2 = UserName ? (SQLSMALLINT)strlen((const char*)UserName) : 0;
  if (NameLength3 == SQL_NTS) NameLength3 = Authentication ? (SQLSMALLINT)strlen((const char*)Authentication) : 0;

  conn_cfg_release(&conn->cfg);
  if (ServerName) {
    conn->cfg.dsn = strndup((const char*)ServerName, NameLength1);
    if (!conn->cfg.dsn) {
      conn_oom(conn);
      return SQL_ERROR;
    }
  }

  r = conn_cfg_init_other_fields(&conn->cfg);
  if (r) {
    conn_oom(conn);
    return SQL_ERROR;
  }

  if (UserName) {
    conn->cfg.uid = strndup((const char*)UserName, NameLength2);
    if (!conn->cfg.uid) {
      conn_oom(conn);
      return SQL_ERROR;
    }
  }
  if (Authentication) {
    conn->cfg.pwd = strndup((const char*)Authentication, NameLength3);
    if (!conn->cfg.pwd) {
      conn_oom(conn);
      return SQL_ERROR;
    }
  }

  return _do_conn_connect(conn);
}

static SQLRETURN _conn_set_string(
    conn_t         *conn,
    const char     *value,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  int n = snprintf((char*)InfoValuePtr, BufferLength, "%s", value);
  if (StringLengthPtr) *StringLengthPtr = n;

  if (n >= BufferLength) {
    conn_append_err_format(conn, "01004", 0, "String data, right truncated:`%s[%d/0x%x]`", sql_info_type(InfoType), InfoType, InfoType);
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _conn_get_info_dbms_name(
    conn_t         *conn,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  const char *server = CALL_taos_get_server_info(conn->taos);
  if (!server) {
    conn_append_err_format(conn, "HY000", 0, "General error:`%s[%d/0x%x]` internal logic error", sql_info_type(InfoType), InfoType, InfoType);
    return SQL_ERROR;
  }

  return _conn_set_string(conn, server, InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
}

static SQLRETURN _conn_get_info_driver_name(
    conn_t         *conn,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  (void)conn;

  const char *client = CALL_taos_get_client_info();
  if (!client) {
    conn_append_err_format(conn, "HY000", 0, "General error:`%s[%d/0x%x]` internal logic error", sql_info_type(InfoType), InfoType, InfoType);
    return SQL_ERROR;
  }
  int n = snprintf((char*)InfoValuePtr, BufferLength, "taos_odbc-0.1@taosc:%s", client);
  if (StringLengthPtr) *StringLengthPtr = n;

  if (n >= BufferLength) {
    conn_append_err_format(conn, "01004", 0, "String data, right truncated:`%s[%d/0x%x]`", sql_info_type(InfoType), InfoType, InfoType);
    return SQL_SUCCESS_WITH_INFO;
  }

  return SQL_SUCCESS;
}

static SQLRETURN _conn_get_catalog_name_separator(
    conn_t         *conn,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  (void)conn;
  const char *catalog_name_separator = ".";
  int n = snprintf((char*)InfoValuePtr, BufferLength, "%s", catalog_name_separator);
  if (StringLengthPtr) {
    *StringLengthPtr = n;
  }
  return SQL_SUCCESS;
}

SQLRETURN conn_get_info(
    conn_t         *conn,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver16
  switch (InfoType) {
    case SQL_DRIVER_ODBC_VER: {
      // https://learn.microsoft.com/en-us/sql/odbc/reference/install/driver-specification-subkeys?view=sql-server-ver16
      // `DriverODBCVer`: This must be the same as the value returned for the SQL_DRIVER_ODBC_VER option in SQLGetInfo.
#if (ODBCVER == 0x0351)              /* { */
      return _conn_set_string(conn, "03.51", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
#else                                /* }{ */
#error `ODBCVER` must be equal to 0x0351
#endif                               /* } */
    } break;
    case SQL_DRIVER_VER:
      return _conn_set_string(conn, "01.00.0000", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_DRIVER_NAME:
      return _conn_get_info_driver_name(conn, InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_DBMS_VER:
      return _conn_set_string(conn, "01.00.0000", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_DBMS_NAME:
      return _conn_get_info_dbms_name(conn, InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_SERVER_NAME:
      return _conn_set_string(conn, "", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_CURSOR_COMMIT_BEHAVIOR:
      *(SQLUSMALLINT*)InfoValuePtr = 0; // NOTE: refer to msdn listed above
      break;
    case SQL_CURSOR_ROLLBACK_BEHAVIOR: 
      *(SQLUSMALLINT*)InfoValuePtr = 0; // NOTE: refer to msdn listed above
      break;
    case SQL_TXN_ISOLATION_OPTION:
      *(SQLUINTEGER*)InfoValuePtr = 0; // TODO:
      break;
    case SQL_GETDATA_EXTENSIONS:
      *(SQLUINTEGER*)InfoValuePtr = SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER /* | SQL_GD_BLOCK */ | SQL_GD_BOUND /* | SQL_GD_OUTPUT_PARAMS */;
      break;
    case SQL_MAX_COLUMN_NAME_LEN:
      *(SQLUSMALLINT*)InfoValuePtr = MAX_COLUMN_NAME_LEN;
      break;
    case SQL_MAX_CATALOG_NAME_LEN:
      *(SQLUSMALLINT*)InfoValuePtr = MAX_CATALOG_NAME_LEN;
      break;
    case SQL_MAX_SCHEMA_NAME_LEN:
      *(SQLUSMALLINT*)InfoValuePtr = MAX_SCHEMA_NAME_LEN;
      break;
    case SQL_MAX_TABLE_NAME_LEN:
      *(SQLUSMALLINT*)InfoValuePtr = MAX_TABLE_NAME_LEN;
      break;
#if (ODBCVER >= 0x0380)      /* { */
    case SQL_ASYNC_DBC_FUNCTIONS:
      *(SQLUINTEGER*)InfoValuePtr = SQL_ASYNC_DBC_NOT_CAPABLE;
      break;
    case SQL_ASYNC_NOTIFICATION:
      *(SQLUINTEGER*)InfoValuePtr = SQL_ASYNC_NOTIFICATION_NOT_CAPABLE;
      break;
#endif                       /* } */
    case SQL_DTC_TRANSITION_COST:
      *(SQLUINTEGER*)InfoValuePtr = 0;
      break;
    case SQL_CATALOG_NAME_SEPARATOR:
      return _conn_get_catalog_name_separator(conn, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_CATALOG_NAME:
      return _conn_set_string(conn, "Y", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_CATALOG_USAGE:
      *(SQLUINTEGER*)InfoValuePtr = SQL_CU_DML_STATEMENTS | SQL_CU_PROCEDURE_INVOCATION | SQL_CU_TABLE_DEFINITION | SQL_CU_INDEX_DEFINITION | SQL_CU_PRIVILEGE_DEFINITION;
      break;
    case SQL_OJ_CAPABILITIES:
      // *(SQLUINTEGER*)InfoValuePtr = SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL | SQL_OJ_NESTED | SQL_OJ_NOT_ORDERED | SQL_OJ_INNER | SQL_OJ_ALL_COMPARISON_OPS;
      *(SQLUINTEGER*)InfoValuePtr = 0;
      break;
    case SQL_GROUP_BY:
      *(SQLUSMALLINT*)InfoValuePtr = SQL_GB_GROUP_BY_EQUALS_SELECT;
      break;
    case SQL_IDENTIFIER_CASE:
      *(SQLUSMALLINT*)InfoValuePtr = SQL_IC_UPPER;
      break;
    case SQL_ORDER_BY_COLUMNS_IN_SELECT:
      return _conn_set_string(conn, "N", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_IDENTIFIER_QUOTE_CHAR:
      return _conn_set_string(conn, "`", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_QUOTED_IDENTIFIER_CASE:
      *(SQLUSMALLINT*)InfoValuePtr = SQL_IC_SENSITIVE;
      break;
    case SQL_MAX_CONCURRENT_ACTIVITIES:
      *(SQLUSMALLINT*)InfoValuePtr = 0;
      break;
    case SQL_BOOKMARK_PERSISTENCE:
      *(SQLUINTEGER*)InfoValuePtr = 0;
      break;
    case SQL_SEARCH_PATTERN_ESCAPE:
      return _conn_set_string(conn, "\\", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_OWNER_USAGE:
      *(SQLUINTEGER*)InfoValuePtr = 0;
      break;
    case SQL_QUALIFIER_LOCATION:
      *(SQLUSMALLINT*)InfoValuePtr = SQL_CL_START;
      break;
    case SQL_SQL_CONFORMANCE:
      *(SQLUINTEGER*)InfoValuePtr = SQL_SC_SQL92_ENTRY;
      break;
    case SQL_MAX_COLUMNS_IN_ORDER_BY:
      *(SQLUSMALLINT*)InfoValuePtr = 0;
      break;
    case SQL_MAX_IDENTIFIER_LEN:
      *(SQLUSMALLINT*)InfoValuePtr = 192;
      break;
    case SQL_MAX_COLUMNS_IN_GROUP_BY:
      *(SQLUSMALLINT*)InfoValuePtr = 0;
      break;
    case SQL_MAX_COLUMNS_IN_SELECT:
      *(SQLUSMALLINT*)InfoValuePtr = 4096;
      break;
    case SQL_COLUMN_ALIAS:
      return _conn_set_string(conn, "Y", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_CATALOG_TERM:
      return _conn_set_string(conn, "database", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    case SQL_OWNER_TERM:
      return _conn_set_string(conn, "schema", InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
    default:
      conn_append_err_format(conn, "HY000", 0, "General error:`%s[%d/0x%x]` not implemented yet", sql_info_type(InfoType), InfoType, InfoType);
      return SQL_ERROR;
  }
  return SQL_SUCCESS;
}

SQLRETURN conn_end_tran(
    conn_t       *conn,
    SQLSMALLINT   CompletionType)
{
  switch (CompletionType) {
    case SQL_COMMIT:
      return _conn_commit(conn);
    case SQL_ROLLBACK:
      return _conn_rollback(conn);
    default:
      conn_append_err_format(conn, "HY000", 0, "General error:[DM]logic error");
      return SQL_ERROR;
  }
}

SQLRETURN conn_set_attr(
    conn_t       *conn,
    SQLINTEGER    Attribute,
    SQLPOINTER    ValuePtr,
    SQLINTEGER    StringLength)
{
  (void)StringLength;

  switch (Attribute) {
    case SQL_ATTR_CONNECTION_TIMEOUT:
      if (0 == (SQLUINTEGER)(uintptr_t)ValuePtr) return SQL_SUCCESS;
      conn_append_err_format(conn, "01S02", 0,
          "Option value changed:`%u` for `SQL_ATTR_CONNECTION_TIMEOUT` is substituted by `0`",
          (SQLUINTEGER)(uintptr_t)ValuePtr);
      return SQL_SUCCESS_WITH_INFO;
    case SQL_ATTR_LOGIN_TIMEOUT:
      if (0 == (SQLUINTEGER)(uintptr_t)ValuePtr) return SQL_SUCCESS;
      conn_append_err_format(conn, "01S02", 0,
          "Option value changed:`%u` for `SQL_ATTR_LOGIN_TIMEOUT` is substituted by `0`",
          (SQLUINTEGER)(uintptr_t)ValuePtr);
      return SQL_SUCCESS_WITH_INFO;
    default:
      conn_append_err_format(conn, "HY000", 0, "General error:`%s[0x%x/%d]` not supported yet", sql_conn_attr(Attribute), Attribute, Attribute);
      return SQL_ERROR;
  }
}

#if (ODBCVER >= 0x0300)           /* { */
static SQLRETURN _conn_get_attr_current_qualifier(
    conn_t       *conn,
    SQLPOINTER    Value,
    SQLINTEGER    BufferLength,
    SQLINTEGER   *StringLengthPtr)
{
  if (0) {
    conn_append_err(conn, "HYC00", 0, "Optional feature not implemented:`SQL_CURRENT_QUALIFIER` not supported yet");
    return SQL_ERROR;
  }
  const char *current_qualifier = "information_schema";
  current_qualifier = "";
  int n = snprintf((char*)Value, BufferLength, "%s", current_qualifier);
  if (StringLengthPtr) *StringLengthPtr = n;
  return SQL_SUCCESS;
}

SQLRETURN conn_get_attr(
    conn_t       *conn,
    SQLINTEGER    Attribute,
    SQLPOINTER    Value,
    SQLINTEGER    BufferLength,
    SQLINTEGER   *StringLengthPtr)
{
  switch (Attribute) {
    case SQL_CURRENT_QUALIFIER:
      return _conn_get_attr_current_qualifier(conn, Value, BufferLength, StringLengthPtr);
    default:
      conn_append_err_format(conn, "HY000", 0, "General error:`%s[0x%x/%d]` not supported yet", sql_conn_attr(Attribute), Attribute, Attribute);
      return SQL_ERROR;
  }
}
#endif                            /* } */

void conn_clr_errs(conn_t *conn)
{
  errs_clr(&conn->errs);
}

SQLRETURN conn_get_diag_field(
    conn_t         *conn,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  switch (DiagIdentifier) {
    case SQL_DIAG_CLASS_ORIGIN:
      return errs_get_diag_field_class_origin(&conn->errs, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_DIAG_SUBCLASS_ORIGIN:
      return errs_get_diag_field_subclass_origin(&conn->errs, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_DIAG_SQLSTATE:
      return errs_get_diag_field_sqlstate(&conn->errs, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_DIAG_CONNECTION_NAME:
      // TODO:
      *(char*)DiagInfoPtr = '\0';
      if (StringLengthPtr) *StringLengthPtr = 0;
      return SQL_SUCCESS;
    case SQL_DIAG_SERVER_NAME:
      // TODO:
      *(char*)DiagInfoPtr = '\0';
      if (StringLengthPtr) *StringLengthPtr = 0;
      return SQL_SUCCESS;
    default:
      conn_append_err_format(conn, "HY000", 0, "General error:RecNumber:[%d]; DiagIdentifier:[%d]%s", RecNumber, DiagIdentifier, sql_diag_identifier(DiagIdentifier));
      return SQL_ERROR;
  }
}

SQLRETURN conn_native_sql(
    conn_t        *conn,
    SQLCHAR       *InStatementText,
    SQLINTEGER     TextLength1,
    SQLCHAR       *OutStatementText,
    SQLINTEGER     BufferLength,
    SQLINTEGER    *TextLength2Ptr)
{
  (void)InStatementText;
  (void)TextLength1;
  (void)OutStatementText;
  (void)BufferLength;
  (void)TextLength2Ptr;
  conn_append_err(conn, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN conn_browse_connect(
    conn_t        *conn,
    SQLCHAR       *InConnectionString,
    SQLSMALLINT    StringLength1,
    SQLCHAR       *OutConnectionString,
    SQLSMALLINT    BufferLength,
    SQLSMALLINT   *StringLength2Ptr)
{
  (void)InConnectionString;
  (void)StringLength1;
  (void)OutConnectionString;
  (void)BufferLength;
  (void)StringLength2Ptr;
  conn_append_err(conn, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN conn_complete_async(
    conn_t      *conn,
    RETCODE     *AsyncRetCodePtr)
{
  (void)AsyncRetCodePtr;
  conn_append_err(conn, "HY000", 0, "General error:not supported yet");
  return SQL_ERROR;
}

SQLRETURN conn_get_cnv_tsdb_varchar_to_sql_c_wchar(conn_t *conn, charset_conv_t **cnv)
{
  *cnv = &conn->_cnv_tsdb_varchar_to_sql_c_wchar;
  return SQL_SUCCESS;
}

SQLRETURN conn_get_cnv_tsdb_varchar_to_sql_c_char(conn_t *conn, charset_conv_t **cnv)
{
  *cnv = &conn->_cnv_tsdb_varchar_to_sql_c_char;
  return SQL_SUCCESS;
}

SQLRETURN conn_get_cnv_sql_c_char_to_tsdb_varchar(conn_t *conn, charset_conv_t **cnv)
{
  *cnv = &conn->_cnv_sql_c_char_to_tsdb_varchar;
  return SQL_SUCCESS;
}

SQLRETURN conn_get_cnv_sql_c_char_to_sql_c_wchar(conn_t *conn, charset_conv_t **cnv)
{
  *cnv = &conn->_cnv_sql_c_char_to_sql_c_wchar;
  return SQL_SUCCESS;
}
