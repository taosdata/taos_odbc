#include "internal.h"

#include "conn.h"
#include "desc.h"
#include "parser.h"

#include <odbcinst.h>
#include <string.h>

#define _conn_malloc_fail(_conn)             \
  conn_append_err(_conn,                     \
    "HY001",                                 \
    0,                                       \
    "memory allocation failure");

static int conn_init(conn_t *conn, env_t *env)
{
  conn->env = env_ref(env);
  int prev = atomic_fetch_add(&env->conns, 1);
  OA_ILE(prev >= 0);

  errs_init(&conn->errs);

  conn->refc = 1;

  return 0;
}

static void conn_release(conn_t *conn)
{
  OA_ILE(conn->taos == NULL);

  int prev = atomic_fetch_sub(&conn->env->conns, 1);
  OA_ILE(prev >= 1);
  int stmts = atomic_load(&conn->stmts);
  OA_ILE(stmts == 0);
  env_unref(conn->env);
  conn->env = NULL;

  connection_cfg_release(&conn->cfg);
  errs_release(&conn->errs);

  return;
}

conn_t* conn_create(env_t *env)
{
  OA_ILE(env);

  conn_t *conn = (conn_t*)calloc(1, sizeof(*conn));
  if (!conn) {
    env_append_err(env, "HY000", 0, "Memory allocation failure");
    return NULL;
  }

  int r = conn_init(conn, env);
  if (r) {
    conn_release(conn);
    env_append_err(env, "HY000", 0, "Memory allocation failure");
    return NULL;
  }

  return conn;
}

conn_t* conn_ref(conn_t *conn)
{
  OA_ILE(conn);
  int prev = atomic_fetch_add(&conn->refc, 1);
  OA_ILE(prev>0);
  return conn;
}

conn_t* conn_unref(conn_t *conn)
{
  OA_ILE(conn);
  int prev = atomic_fetch_sub(&conn->refc, 1);
  if (prev>1) return conn;
  OA_ILE(prev==1);

  conn_release(conn);
  free(conn);

  return NULL;
}

SQLRETURN conn_free(conn_t *conn)
{
  int stmts = atomic_load(&conn->stmts);
  if (stmts) {
    conn_append_err_format(conn, "HY000", 0, "#%d statements are still allocated", stmts);
    return SQL_ERROR;
  }

  int descs = atomic_load(&conn->descs);
  if (descs) {
    conn_append_err_format(conn, "HY000", 0, "#%d descriptors are still allocated", descs);
    return SQL_ERROR;
  }

  conn_unref(conn);
  return SQL_SUCCESS;
}

static SQLRETURN _do_conn_connect(conn_t *conn)
{
  OA_ILE(conn);
  OA_ILE(conn->taos == NULL);
  const connection_cfg_t *cfg = &conn->cfg;

  conn->taos = CALL_taos_connect(cfg->ip, cfg->uid, cfg->pwd, cfg->db, cfg->port);
  if (!conn->taos) {
    char buf[1024];
    buffer_t buffer = {};
    buffer.buf = buf;
    buffer.cap = sizeof(buf);
    buffer.nr  = 0;
    buffer_sprintf(&buffer, "taos_odbc://");
    if (cfg->uid) buffer_sprintf(&buffer, "%s:*@", cfg->uid);
    if (cfg->ip) {
      if (cfg->port) buffer_sprintf(&buffer, "%s:%d", cfg->ip, cfg->port);
      else           buffer_sprintf(&buffer, "%s", cfg->ip);
    } else {
      buffer_sprintf(&buffer, "localhost");
    }
    if (cfg->db) buffer_sprintf(&buffer, "/%s", cfg->db);

    conn_append_err_format(conn, "HY000", CALL_taos_errno(NULL), "target:[%s][%s]", buffer.buf, CALL_taos_errstr(NULL));
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
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

  buffer_t buffer = {};
  buffer.buf = p;
  buffer.cap = BufferLength;
  buffer.nr = 0;
  if (conn->cfg.driver) {
    n = buffer_sprintf(&buffer, "Driver={%s};", conn->cfg.driver);
  } else {
    n = buffer_sprintf(&buffer, "DSN=%s;", conn->cfg.dsn);
  }
  if (n>0) count += n;

  if (conn->cfg.uid) {
    n = buffer_sprintf(&buffer, "UID=%s;", conn->cfg.uid);
  } else {
    n = buffer_sprintf(&buffer, "UID=;");
  }
  if (n>0) count += n;

  if (conn->cfg.pwd) {
    n = buffer_sprintf(&buffer, "PWD=*;");
  } else {
    n = buffer_sprintf(&buffer, "PWD=;");
  }
  if (n>0) count += n;

  if (conn->cfg.ip) {
    if (conn->cfg.port) {
      n = buffer_sprintf(&buffer, "Server=%s:%d;", conn->cfg.ip, conn->cfg.port);
    } else {
      n = buffer_sprintf(&buffer, "Server=%s;", conn->cfg.ip);
    }
  } else {
    n = buffer_sprintf(&buffer, "Server=;");
  }
  if (n>0) count += n;

  if (conn->cfg.db) {
    n = buffer_sprintf(&buffer, "DB=%s;", conn->cfg.db);
  } else {
    n = buffer_sprintf(&buffer, "DB=;");
  }
  if (n>0) count += n;

  if (conn->cfg.unsigned_promotion) {
    n = buffer_sprintf(&buffer, "UNSIGNED_PROMOTION=1;");
  } else {
    n = buffer_sprintf(&buffer, "UNSIGNED_PROMOTION=;");
  }
  if (n>0) count += n;

  if (conn->cfg.cache_sql) {
    n = buffer_sprintf(&buffer, "CACHE_SQL=1;");
  } else {
    n = buffer_sprintf(&buffer, "CACHE_SQL=;");
  }
  if (n>0) count += n;

  if (buffer.nr+1 == buffer.cap) {
    char *x = buffer.buf + buffer.nr;
    for (int i=0; i<3 && x>buffer.buf; ++i, --x) x[-1] = '.';
  }

  if (StringLength2Ptr) {
    *StringLength2Ptr = count;
  }
}

SQLRETURN conn_driver_connect(
    conn_t         *conn,
    SQLHWND         WindowHandle,
    SQLCHAR        *InConnectionString,
    SQLSMALLINT     StringLength1,
    SQLCHAR        *OutConnectionString,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLength2Ptr)
{
  if (WindowHandle) {
    conn_append_err(conn, "HY000", 0, "Interactive connect not supported yet");
    return SQL_ERROR;
  }

  if (conn->taos) {
    conn_append_err(conn, "HY000", 0, "Already connected");
    return SQL_ERROR;
  }

  parser_param_t param = {};
  param.debug_flex  = env_get_debug_flex(conn->env);
  param.debug_bison = env_get_debug_bison(conn->env);

  if (StringLength1 == SQL_NTS) StringLength1 = strlen((const char*)InConnectionString);
  int r = parser_parse((const char*)InConnectionString, StringLength1, &param);
  SQLRETURN sr = SQL_SUCCESS;

  do {
    if (r) {
      conn_append_err_format(
        conn, "HY000", 0,
        "bad syntax for connection string: [%.*s][(%d,%d)->(%d,%d): %s]",
        StringLength1, InConnectionString,
        param.row0, param.col0, param.row1, param.col1,
        param.errmsg);

      sr = SQL_ERROR;
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
  OA_ILE(conn);
  OA_ILE(conn->taos);

  int stmts = atomic_load(&conn->stmts);
  OA_ILE(stmts == 0);

  CALL_taos_close(conn->taos);
  conn->taos = NULL;
  connection_cfg_release(&conn->cfg);
}

int conn_get_dbms_name(conn_t *conn, const char **name)
{
  OA_ILE(conn);
  OA_ILE(conn->taos);
  const char *server = CALL_taos_get_server_info(conn->taos);
  *name = server;
  return 0;
}

int conn_get_driver_name(conn_t *conn, const char **name)
{
  OA_ILE(conn);
  const char *client = CALL_taos_get_client_info();
  *name = client;
  return 0;
}

int conn_rollback(conn_t *conn)
{
  int outstandings = atomic_load(&conn->outstandings);
  return outstandings == 0 ? 0 : -1;
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
  if (stmt == NULL) {
    _conn_malloc_fail(conn);
    return SQL_ERROR;
  }

  *OutputHandle = (SQLHANDLE)stmt;
  return SQL_SUCCESS;
}

SQLRETURN conn_alloc_desc(conn_t *conn, SQLHANDLE *OutputHandle)
{
  desc_t *desc = desc_create(conn);
  if (!desc) {
    _conn_malloc_fail(conn);
    return SQL_ERROR;
  }
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
        _conn_malloc_fail(conn);
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
        _conn_malloc_fail(conn);
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
    conn_append_err(conn, "HY000", 0, "Already connected");
    return SQL_ERROR;
  }

  connection_cfg_release(&conn->cfg);
  conn->cfg.dsn = strndup((const char*)ServerName, NameLength1);
  if (!conn->cfg.dsn) {
    _conn_malloc_fail(conn);
    return SQL_ERROR;
  }
  if (UserName) {
    conn->cfg.uid = strndup((const char*)UserName, NameLength2);
    if (!conn->cfg.uid) {
      _conn_malloc_fail(conn);
      return SQL_ERROR;
    }
  }
  if (Authentication) {
    conn->cfg.pwd = strndup((const char*)Authentication, NameLength3);
    if (!conn->cfg.pwd) {
      _conn_malloc_fail(conn);
      return SQL_ERROR;
    }
  }

  return _conn_connect(conn);
}

static SQLRETURN do_conn_get_info_dbms_name(
    conn_t         *conn,
    char           *buf,
    size_t          sz,
    SQLSMALLINT    *StringLengthPtr)
{
  const char *name;
  int r = conn_get_dbms_name(conn, &name);
  if (r) return SQL_ERROR;

  int n = snprintf(buf, sz, "%s", name);
  *StringLengthPtr = n;

  return SQL_SUCCESS;
}

static SQLRETURN do_conn_get_info_driver_name(
    conn_t         *conn,
    char           *buf,
    size_t          sz,
    SQLSMALLINT    *StringLengthPtr)
{
  const char *name;
  int r = conn_get_driver_name(conn, &name);
  if (r) return SQL_ERROR;

  int n = snprintf(buf, sz, "%s", name);
  *StringLengthPtr = n;

  return SQL_SUCCESS;
}

SQLRETURN conn_get_info(
    conn_t         *conn,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  switch (InfoType) {
    case SQL_DBMS_NAME:
      return do_conn_get_info_dbms_name(conn, (char*)InfoValuePtr, (size_t)BufferLength, StringLengthPtr);
    case SQL_DRIVER_NAME:
      return do_conn_get_info_driver_name(conn, (char*)InfoValuePtr, (size_t)BufferLength, StringLengthPtr);
    case SQL_CURSOR_COMMIT_BEHAVIOR:
      *(SQLUSMALLINT*)InfoValuePtr = 0; // TODO:
      return SQL_ERROR;
    case SQL_TXN_ISOLATION_OPTION:
      *(SQLUINTEGER*)InfoValuePtr = 0; // TODO:
      return SQL_SUCCESS;
    case SQL_GETDATA_EXTENSIONS:
      *(SQLUINTEGER*)InfoValuePtr = SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER /* | SQL_GD_BLOCK */ | SQL_GD_BOUND /* | SQL_GD_OUTPUT_PARAMS */;
      return SQL_SUCCESS;
    case SQL_MAX_COLUMN_NAME_LEN:
      *(SQLUSMALLINT*)InfoValuePtr = 64;
      return SQL_SUCCESS;
    default:
      OA(0, "`%s[%d]` not implemented yet", sql_info_type(InfoType), InfoType);
      conn_append_err_format(conn, "HY000", 0, "`%s[%d]` not implemented yet", sql_info_type(InfoType), InfoType);
      return SQL_ERROR;
  }
}

