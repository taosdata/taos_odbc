#ifndef _conn_h_
#define _conn_h_

#include "env.h"
#include "log.h"
#include "utils.h"

EXTERN_C_BEGIN

typedef struct connection_str_s    connection_str_t;

struct connection_str_s {
  char                  *driver;
  char                  *dsn;
  char                  *uid;
  char                  *pwd;
  char                  *ip;
  char                  *db;
  int                    port;
};

static inline void connection_str_release(connection_str_t *conn_str)
{
  if (!conn_str) return;

  TOD_SAFE_FREE(conn_str->driver);
  TOD_SAFE_FREE(conn_str->dsn);
  TOD_SAFE_FREE(conn_str->uid);
  TOD_SAFE_FREE(conn_str->pwd);
  TOD_SAFE_FREE(conn_str->ip);
  TOD_SAFE_FREE(conn_str->db);
}

typedef struct conn_s              conn_t;

conn_t* conn_create(env_t *env) FA_HIDDEN;
conn_t* conn_ref(conn_t *conn) FA_HIDDEN;
conn_t* conn_unref(conn_t *conn) FA_HIDDEN;

int conn_connect(conn_t *conn, const connection_str_t *conn_str) FA_HIDDEN;
void conn_disconnect(conn_t *conn) FA_HIDDEN;

int conn_get_dbms_name(conn_t *conn, const char **name) FA_HIDDEN;
int conn_get_driver_name(conn_t *conn, const char **name) FA_HIDDEN;

EXTERN_C_END

#endif //  _conn_h_

