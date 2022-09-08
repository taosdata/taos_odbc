#include "internal.h"

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

int conn_connect(conn_t *conn, const connection_str_t *conn_str)
{
  OA_ILE(conn);
  OA_ILE(conn->taos == NULL);

  conn->taos = taos_connect(conn_str->ip, conn_str->uid, conn_str->pwd, conn_str->db, conn_str->port);

  return conn->taos ? 0 : -1;
}

void conn_disconnect(conn_t *conn)
{
  OA_ILE(conn);
  OA_ILE(conn->taos);
  taos_close(conn->taos);
  conn->taos = NULL;
}

int conn_get_dbms_name(conn_t *conn, const char **name)
{
  OA_ILE(conn);
  OA_ILE(conn->taos);
  const char *server = taos_get_server_info(conn->taos);
  *name = server;
  return 0;
}

int conn_get_driver_name(conn_t *conn, const char **name)
{
  OA_ILE(conn);
  const char *client = taos_get_client_info();
  *name = client;
  return 0;
}

int conn_rollback(conn_t *conn)
{
  int outstandings = atomic_load(&conn->outstandings);
  return outstandings == 0 ? 0 : -1;
}

