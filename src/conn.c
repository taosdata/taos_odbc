#include "internal.h"

#include "conn.h"
#include "desc.h"
#include "parser.h"

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

static SQLRETURN conn_connect(conn_t *conn, const connection_cfg_t *cfg)
{
  OA_ILE(conn);
  OA_ILE(conn->taos == NULL);

  if (cfg->legacy) {
    conn_append_err_format(conn, "HY000", 0, "`LEGACY` specified in `connection string`, but not implemented yet");
    return SQL_ERROR;
  }

  conn->fmt_time = cfg->fmt_time;

  // conn->taos = TAOS_connect(cfg->ip, cfg->uid, cfg->pwd, cfg->db, cfg->port);
  conn->taos = TAOS_connect("localhost", "root", "taosdata", NULL, 6030);
  if (!conn->taos) {
    conn_append_err(conn, "HY000", TAOS_errno(NULL), TAOS_errstr(NULL));
    return SQL_ERROR;
  }

  return SQL_SUCCESS;
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

  parser_param_t param = {};
  // param.debug_flex = 1;
  // param.debug_bison = 1;

  if (StringLength1 == SQL_NTS) StringLength1 = strlen((const char*)InConnectionString);
  int r = parser_parse((const char*)InConnectionString, StringLength1, &param);
  SQLRETURN sr = SQL_SUCCESS;

  do {
    if (r) {
      conn_append_err_format(conn, "HY000", 0, "bad syntax for connection string: [%.*s]", StringLength1, InConnectionString);
      sr = SQL_ERROR;
      break;
    }

    sr = conn_connect(conn, &param.conn_str);
    if (!sql_succeeded(sr)) break;

    if (OutConnectionString) {
      int n;
      if (StringLength1 > BufferLength) {
        n = BufferLength;
      } else {
        n = StringLength1;
      }

      strncpy((char*)OutConnectionString, (const char*)InConnectionString, n);
      if (n<BufferLength) {
        OutConnectionString[n] = '\0';
      } else if (BufferLength>0) {
        OutConnectionString[BufferLength-1] = '\0';
      }
      if (StringLength2Ptr) {
        *StringLength2Ptr = strlen((const char*)OutConnectionString);
      }
    } else {
      if (StringLength2Ptr) {
        *StringLength2Ptr = 0;
      }
    }

    connection_cfg_transfer(&param.conn_str, &conn->cfg);
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
  TAOS_close(conn->taos);
  conn->taos = NULL;
  connection_cfg_release(&conn->cfg);
}

int conn_get_dbms_name(conn_t *conn, const char **name)
{
  OA_ILE(conn);
  OA_ILE(conn->taos);
  const char *server = TAOS_get_server_info(conn->taos);
  *name = server;
  return 0;
}

int conn_get_driver_name(conn_t *conn, const char **name)
{
  OA_ILE(conn);
  const char *client = TAOS_get_client_info();
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

