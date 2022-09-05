#include "internal.h"

#include <string.h>

static int stmt_init(stmt_t *stmt, conn_t *conn)
{
  stmt->conn = conn_ref(conn);

  stmt->refc = 1;

  return 0;
}

static void stmt_release(stmt_t *stmt)
{
  conn_unref(stmt->conn);
  stmt->conn = NULL;

  return;
}

stmt_t* stmt_create(conn_t *conn)
{
  OA_ILE(conn);

  stmt_t *stmt = (stmt_t*)calloc(1, sizeof(*stmt));
  if (!stmt) return NULL;

  int r = stmt_init(stmt, conn);
  if (r) {
    stmt_release(stmt);
    return NULL;
  }

  return stmt;
}

stmt_t* stmt_ref(stmt_t *stmt)
{
  OA_ILE(stmt);
  int prev = atomic_fetch_add(&stmt->refc, 1);
  OA_ILE(prev>0);
  return stmt;
}


stmt_t* stmt_unref(stmt_t *stmt)
{
  OA_ILE(stmt);
  int prev = atomic_fetch_sub(&stmt->refc, 1);
  if (prev>1) return stmt;
  OA_ILE(prev==1);

  stmt_release(stmt);
  free(stmt);

  return NULL;
}

int stmt_exec_direct(stmt_t *stmt, const char *sql, int len)
{
  OA_ILE(stmt);
  OA_ILE(stmt->conn);
  OA_ILE(stmt->conn->taos);
  OA_ILE(sql);

  char buf[1024];
  char *p = (char*)sql;
  if (p[len]) {
    if ((size_t)len < sizeof(buf)) {
      strncpy(buf, p, len);
      p[len] = '\0';
    } else {
      p = strndup(p, len);
      if (!p) return -1;
    }
  }

  TAOS *taos = stmt->conn->taos;

  TAOS_RES *res;
  res = taos_query(taos, p);

  int e = taos_errno(res);
  const char *estr = taos_errstr(res);
  OD("[%s] failed: [%d/0x%x]%s", p, e, e, estr);

  if (p != sql && p!= buf) free(p);

  if (res) taos_free_result(res);

  return e ? -1 : 0;
}

