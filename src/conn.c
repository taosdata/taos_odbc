#include "internal.h"

#include <string.h>

static int conn_init(conn_t *conn, env_t *env)
{
  conn->env = env_ref(env);
  int prev = atomic_fetch_add(&env->conns, 1);
  OA_ILE(prev >= 0);

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

  return;
}

conn_t* conn_create(env_t *env)
{
  OA_ILE(env);

  conn_t *conn = (conn_t*)calloc(1, sizeof(*conn));
  if (!conn) return NULL;

  int r = conn_init(conn, env);
  if (r) {
    conn_release(conn);
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

int conn_connect(conn_t *conn, const connection_cfg_t *cfg)
{
  OA_ILE(conn);
  OA_ILE(conn->taos == NULL);

  if (cfg->legacy) {
    err_set(&conn->err,
        "HY000",
        0,
        "`LEGACY` specified in `connection string`, but not implemented yet");
    return -1;
  }

  conn->fmt_time = cfg->fmt_time;

  // conn->taos = TAOS_connect(cfg->ip, cfg->uid, cfg->pwd, cfg->db, cfg->port);
  conn->taos = TAOS_connect("localhost", "root", "taosdata", NULL, 6030);
  if (!conn->taos) {
    err_set(&conn->err,
        "HY000",
        TAOS_errno(NULL),
        TAOS_errstr(NULL));
  }

  return conn->taos ? 0 : -1;
}

void conn_disconnect(conn_t *conn)
{
  OA_ILE(conn);
  OA_ILE(conn->taos);
  TAOS_close(conn->taos);
  conn->taos = NULL;
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
  if (RecNumber > 1) return SQL_NO_DATA;
  if (conn->err.sql_state[0] == '\0') return SQL_NO_DATA;
  if (NativeErrorPtr) *NativeErrorPtr = conn->err.err;
  if (SQLState) strncpy((char*)SQLState, (const char*)conn->err.sql_state, 6);
  int n = snprintf((char*)MessageText, BufferLength, "%s", conn->err.estr);
  if (TextLengthPtr) *TextLengthPtr = n;

  return SQL_SUCCESS;
}

