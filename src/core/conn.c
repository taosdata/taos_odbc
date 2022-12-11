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

#include "conn.h"
#include "desc.h"
// make sure `log.h` is included ahead of `taos_helpers.h`, for the `LOG_IMPL` issue
#include "log.h"
#include "parser.h"
#include "taos_helpers.h"

#ifndef _WIN32
#include <locale.h>
#endif
#include <odbcinst.h>
#include <string.h>

void charset_conv_release(charset_conv_t *cnv)
{
  if (!cnv) return;
  if (cnv->cnv) {
    iconv_close(cnv->cnv);
    cnv->cnv = NULL;
  }
  cnv->from[0] = '\0';
  cnv->to[0] = '\0';
  cnv->nr_from_terminator = 0;
  cnv->nr_to_terminator = 0;
}

static int _calc_terminator(const char *tocode)
{
  int nr = -1;
  iconv_t cnv = iconv_open(tocode, "UTF-8");
  if (!cnv) return -1;
  do {
    char buf[64]; buf[0] = '\0';
    char          *inbuf          = "";
    size_t         inbytesleft    = 1;
    char          *outbuf         = buf;
    size_t         outbytesleft   = sizeof(buf);

    size_t n = iconv(cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
    if (n) break;
    if (inbytesleft) break;
    nr = (int)(sizeof(buf) - outbytesleft);
  } while (0);
  iconv_close(cnv);
  return nr;
}

int charset_conv_reset(charset_conv_t *cnv, const char *from, const char *to)
{
  charset_conv_release(cnv);

  cnv->nr_from_terminator = _calc_terminator(from);
  cnv->nr_to_terminator = _calc_terminator(to);

  do {
    if (cnv->nr_from_terminator <= 0) break;
    if (cnv->nr_to_terminator <= 0) break;

    if (tod_strcasecmp(from, to)) {
      cnv->cnv = iconv_open(to, from);
      if (!cnv->cnv) return -1;
    }
    snprintf(cnv->from, sizeof(cnv->from), "%s", from);
    snprintf(cnv->to, sizeof(cnv->to), "%s", to);
    return 0;
  } while (0);

  charset_conv_release(cnv);
  return -1;
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
}

static void _conn_release_iconvs(conn_t *conn)
{
  charset_conv_release(&conn->cnv_sql_c_char_to_tsdb_varchar);
  charset_conv_release(&conn->cnv_tsdb_varchar_to_sql_c_char);
  charset_conv_release(&conn->cnv_tsdb_varchar_to_utf8);
  charset_conv_release(&conn->cnv_utf8_to_tsdb_varchar);
  charset_conv_release(&conn->cnv_sql_c_char_to_utf8);
  charset_conv_release(&conn->cnv_utf8_to_sql_c_char);
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

  connection_cfg_release(&conn->cfg);
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
    OD("===%s===", conn->s_##x);                                               \
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

  int r = 0;
  nr_fields = CALL_taos_num_fields(res);
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
    TAOS_FIELD *field;
    int col;

    col = 0;
    field = fields + col;
    const char *name = NULL;
    int name_len = 0;
    r = helper_get_data_len(res, field, rows, i, col, &name, &name_len);
    if (r) {
      OE("General error:Row[%d] Column[%s] conversion from `%s[0x%x/%d]` not implemented yet",
          i + 1, field->name, taos_data_type(field->type), field->type, field->type);
      conn_append_err_format(conn, "HY000", 0,
          "General error:Row[%d] Column[%s] conversion from `%s[0x%x/%d]` not implemented yet",
          i + 1, field->name, taos_data_type(field->type), field->type, field->type);
      return SQL_ERROR;
    }

    col = 1;
    field = fields + col;
    const char *value = NULL;
    int value_len = 0;
    r = helper_get_data_len(res, field, rows, i, col, &value, &value_len);
    if (r) {
      OE("General error:Row[%d] Column[%s] conversion from `%s[0x%x/%d]` not implemented yet",
          i + 1, field->name, taos_data_type(field->type), field->type, field->type);
      conn_append_err_format(conn, "HY000", 0,
          "General error:Row[%d] Column[%s] conversion from `%s[0x%x/%d]` not implemented yet",
          i + 1, field->name, taos_data_type(field->type), field->type, field->type);
      return SQL_ERROR;
    }
    OD("%.*s: %.*s", name_len, name, value_len, value);
    r = _conn_save_from_information_schema_ins_configs(conn, name_len, name, value_len, value);
    if (r) {
      OE("General error: save `%.*s:%.*s` failed", name_len, name, value_len, value);
      conn_append_err_format(conn, "HY000", 0,
          "General error: save `%.*s:%.*s` failed", name_len, name, value_len, value);
      return SQL_ERROR;
    }
  }
  return SQL_SUCCESS;
}

static SQLRETURN _conn_get_configs_from_information_schema_ins_configs(conn_t *conn)
{
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
  const char *tsdb_charset = conn->s_charset;
  const char *sql_c_charset = "";

#ifndef _WIN32
  const char *locale = setlocale(LC_CTYPE, NULL);
  const char *p = locale ? strchr(locale, '.') : NULL;
  p = p ? p + 1 : NULL;
  if (!p) {
    conn_append_err_format(conn, "HY000", 0,
        "General error:current locale [%s] not implemented yet", locale);
    return -1;
  }
  sql_c_charset = p;
#endif
#ifdef _WIN32
  UINT acp = GetACP();
  switch (acp) {
    case 936:
      sql_c_charset = "GB18030";
      break;
    case 65001:
      sql_c_charset = "UTF-8";
      break;
    default:
      conn_append_err_format(conn, "HY000", 0,
          "General error:current code page [%d] not implemented yet", acp);
      return -1;
  }
#endif

  const char *utf8 = "UTF-8";
  const char *from, *to;
  charset_conv_t *cnv;
  do {
    cnv = &conn->cnv_sql_c_char_to_utf8;
    from = sql_c_charset; to = utf8;
    if (charset_conv_reset(cnv, from, to)) break;

    cnv = &conn->cnv_utf8_to_sql_c_char;
    from = utf8; to = sql_c_charset;
    if (charset_conv_reset(cnv, from, to)) break;

    cnv = &conn->cnv_tsdb_varchar_to_utf8;
    from = tsdb_charset; to = utf8;
    if (charset_conv_reset(cnv, from, to)) break;

    cnv = &conn->cnv_utf8_to_tsdb_varchar;
    from = utf8; to = tsdb_charset;
    if (charset_conv_reset(cnv, from, to)) break;

    do {
      cnv = &conn->cnv_sql_c_char_to_tsdb_varchar;
      from = sql_c_charset; to = tsdb_charset;
      if (charset_conv_reset(cnv, from, to)) break;

      cnv = &conn->cnv_tsdb_varchar_to_sql_c_char;
      from = tsdb_charset; to = sql_c_charset;
      if (charset_conv_reset(cnv, from, to)) break;

      return 0;
    } while (0);

    _conn_release_iconvs(conn);
#ifdef _WIN32
    conn_append_err_format(conn, "HY000", 0,
        "General error:conversion between current code page [%d]%s <=> charset [%s] not supported yet",
        acp, sql_c_charset, tsdb_charset);
#else
    conn_append_err_format(conn, "HY000", 0,
        "General error:conversion between current charset [%s] of locale [%s] <=> charset [%s] not supported yet",
        p, locale, tsdb_charset);
#endif
    return -1;
  } while (0);

  _conn_release_iconvs(conn);
  conn_append_err_format(conn, "HY000", 0,
      "General error:conversion between charsets [%s] <=> [%s] not supported yet",
      from, to);
  return -1;
}

static SQLRETURN _do_conn_connect(conn_t *conn)
{
  OA_ILE(conn->taos == NULL);
  const connection_cfg_t *cfg = &conn->cfg;

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

  if (conn->cfg.pwd) {
    fixed_buf_sprintf(n, &buffer, "PWD=*;");
  } else {
    fixed_buf_sprintf(n, &buffer, "PWD=;");
  }
  if (n>0) count += n;

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
    fixed_buf_sprintf(n, &buffer, "UNSIGNED_PROMOTION=;");
  }
  if (n>0) count += n;

  if (conn->cfg.cache_sql) {
    fixed_buf_sprintf(n, &buffer, "CACHE_SQL=1;");
  } else {
    fixed_buf_sprintf(n, &buffer, "CACHE_SQL=;");
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

  switch (DriverCompletion) {
    case SQL_DRIVER_NOPROMPT:
      break;
    default:
      conn_append_err_format(conn, "HY000", 0,
          "General error:`%s[%d/0x%x]` not supported yet",
          sql_driver_completion(DriverCompletion), DriverCompletion, DriverCompletion);
      return SQL_ERROR;
  }

  if (conn->taos) {
    conn_append_err(conn, "HY000", 0, "General error:Already connected");
    return SQL_ERROR;
  }

  parser_param_t param = {0};
  param.debug_flex  = env_get_debug_flex(conn->env);
  param.debug_bison = env_get_debug_bison(conn->env);

  if (StringLength1 == SQL_NTS) StringLength1 = (SQLSMALLINT)strlen((const char*)InConnectionString);
  int r = parser_parse((const char*)InConnectionString, StringLength1, &param);
  SQLRETURN sr = SQL_SUCCESS;

  do {
    if (r) {
      conn_append_err_format(
        conn, "HY000", 0,
        "General error:bad syntax for connection string:[%.*s][(%d,%d)->(%d,%d):%s]",
        StringLength1, InConnectionString,
        param.row0, param.col0, param.row1, param.col1,
        param.errmsg);

      break;
    }

    connection_cfg_transfer(&param.conn_str, &conn->cfg);

    sr = _do_conn_connect(conn);
    if (!sql_succeeded(sr)) break;

    _conn_fill_out_connection_str(conn, OutConnectionString, BufferLength, StringLength2Ptr);

    parser_param_release(&param);
    return SQL_SUCCESS;
  } while (0);

  parser_param_release(&param);
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
  connection_cfg_release(&conn->cfg);
}

static void _conn_get_dbms_name(conn_t *conn, const char **name)
{
  OA_ILE(conn->taos);
  const char *server = CALL_taos_get_server_info(conn->taos);
  if (name) *name = server;
}

static void _get_driver_name(const char **name)
{
  const char *client = CALL_taos_get_client_info();
  if (name) *name = client;
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

static SQLRETURN _conn_connect( conn_t *conn)
{
  char buf[1024];
  buf[0] = '\0';

  int r;
  r = SQLGetPrivateProfileString((LPCSTR)conn->cfg.dsn, "UNSIGNED_PROMOTION", (LPCSTR)"0", (LPSTR)buf, sizeof(buf), NULL);
  if (r == 1 && buf[0] == '1') conn->cfg.unsigned_promotion = 1;

  if (!conn->cfg.pwd) {
    buf[0] = '\0';
    r = SQLGetPrivateProfileString((LPCSTR)conn->cfg.dsn, "PWD", (LPCSTR)"", (LPSTR)buf, sizeof(buf), NULL);
    if (buf[0]) {
      conn->cfg.pwd = strdup(buf);
      if (!conn->cfg.pwd) {
        conn_oom(conn);
        return SQL_ERROR;
      }
    }
  }

  if (!conn->cfg.uid) {
    buf[0] = '\0';
    r = SQLGetPrivateProfileString((LPCSTR)conn->cfg.dsn, "UID", (LPCSTR)"", (LPSTR)buf, sizeof(buf), NULL);
    if (buf[0]) {
      conn->cfg.uid = strdup(buf);
      if (!conn->cfg.uid) {
        conn_oom(conn);
        return SQL_ERROR;
      }
    }
  }

  return _do_conn_connect(conn);
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
  if (conn->taos) {
    conn_append_err(conn, "HY000", 0, "General error:Already connected");
    return SQL_ERROR;
  }

  if (NameLength1 == SQL_NTS) NameLength1 = ServerName ? (SQLSMALLINT)strlen((const char*)ServerName) : 0;
  if (NameLength2 == SQL_NTS) NameLength2 = UserName ? (SQLSMALLINT)strlen((const char*)UserName) : 0;
  if (NameLength3 == SQL_NTS) NameLength3 = Authentication ? (SQLSMALLINT)strlen((const char*)Authentication) : 0;

  connection_cfg_release(&conn->cfg);
  if (ServerName) {
    conn->cfg.dsn = strndup((const char*)ServerName, NameLength1);
    if (!conn->cfg.dsn) {
      conn_oom(conn);
      return SQL_ERROR;
    }
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

  SQLRETURN sr = _conn_connect(conn);
  return sr;
}

static SQLRETURN _conn_get_info_driver_odbc_ver(
    conn_t         *conn,
    char           *buf,
    size_t          sz,
    SQLSMALLINT    *StringLengthPtr)
{
  (void)conn;

  // https://learn.microsoft.com/en-us/sql/odbc/reference/install/driver-specification-subkeys?view=sql-server-ver16
  // `DriverODBCVer`: This must be the same as the value returned for the SQL_DRIVER_ODBC_VER option in SQLGetInfo.
#if (ODBCVER == 0x0351)
  const char *ver = "03.51";
#endif

  int n = snprintf(buf, sz, "%s", ver);
  if (StringLengthPtr) *StringLengthPtr = n;

  return SQL_SUCCESS;
}

static SQLRETURN _conn_get_info_dbms_name(
    conn_t         *conn,
    char           *buf,
    size_t          sz,
    SQLSMALLINT    *StringLengthPtr)
{
  const char *name;
  _conn_get_dbms_name(conn, &name);

  int n = snprintf(buf, sz, "%s", name);
  if (StringLengthPtr) *StringLengthPtr = n;

  return SQL_SUCCESS;
}

static SQLRETURN _conn_get_info_driver_name(
    conn_t         *conn,
    char           *buf,
    size_t          sz,
    SQLSMALLINT    *StringLengthPtr)
{
  (void)conn;
  const char *name;
  _get_driver_name(&name);

  int n = snprintf(buf, sz, "%s", name);
  if (StringLengthPtr) *StringLengthPtr = n;

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
    case SQL_DRIVER_ODBC_VER:
      return _conn_get_info_driver_odbc_ver(conn, (char*)InfoValuePtr, (size_t)BufferLength, StringLengthPtr);
    case SQL_DBMS_NAME:
      return _conn_get_info_dbms_name(conn, (char*)InfoValuePtr, (size_t)BufferLength, StringLengthPtr);
    case SQL_DRIVER_NAME:
      return _conn_get_info_driver_name(conn, (char*)InfoValuePtr, (size_t)BufferLength, StringLengthPtr);
    case SQL_CURSOR_COMMIT_BEHAVIOR:
      *(SQLUSMALLINT*)InfoValuePtr = 0; // NOTE: refer to msdn listed above
      return SQL_SUCCESS;
    case SQL_CURSOR_ROLLBACK_BEHAVIOR:
      *(SQLUSMALLINT*)InfoValuePtr = 0; // NOTE: refer to msdn listed above
      return SQL_SUCCESS;
    case SQL_TXN_ISOLATION_OPTION:
      *(SQLUINTEGER*)InfoValuePtr = 0; // TODO:
      return SQL_SUCCESS;
    case SQL_GETDATA_EXTENSIONS:
      *(SQLUINTEGER*)InfoValuePtr = SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER /* | SQL_GD_BLOCK */ | SQL_GD_BOUND /* | SQL_GD_OUTPUT_PARAMS */;
      return SQL_SUCCESS;
    case SQL_MAX_COLUMN_NAME_LEN:
      *(SQLUSMALLINT*)InfoValuePtr = MAX_COLUMN_NAME_LEN;
      return SQL_SUCCESS;
    case SQL_MAX_CATALOG_NAME_LEN:
      *(SQLUSMALLINT*)InfoValuePtr = MAX_CATALOG_NAME_LEN;
      return SQL_SUCCESS;
    case SQL_MAX_SCHEMA_NAME_LEN:
      *(SQLUSMALLINT*)InfoValuePtr = MAX_SCHEMA_NAME_LEN;
      return SQL_SUCCESS;
    case SQL_MAX_TABLE_NAME_LEN:
      *(SQLUSMALLINT*)InfoValuePtr = MAX_TABLE_NAME_LEN;
      return SQL_SUCCESS;
#if (ODBCVER >= 0x0380)      /* { */
    case SQL_ASYNC_DBC_FUNCTIONS:
      *(SQLUINTEGER*)InfoValuePtr = SQL_ASYNC_DBC_NOT_CAPABLE;
      return SQL_SUCCESS;
    case SQL_ASYNC_NOTIFICATION:
      *(SQLUINTEGER*)InfoValuePtr = SQL_ASYNC_NOTIFICATION_NOT_CAPABLE;
      return SQL_SUCCESS;
#endif
    default:
      conn_append_err_format(conn, "HY000", 0, "General error:`%s[%d/0x%x]` not implemented yet", sql_info_type(InfoType), InfoType, InfoType);
      OA_NIY(0);
      return SQL_ERROR;
  }
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
      OA(0, "RecNumber:[%d]; DiagIdentifier:[%d]%s", RecNumber, DiagIdentifier, sql_diag_identifier(DiagIdentifier));
      return SQL_ERROR;
  }
}
