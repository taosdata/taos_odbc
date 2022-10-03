#ifndef _conn_h_
#define _conn_h_

#include "env.h"
#include "utils.h"

#include <string.h>

EXTERN_C_BEGIN

typedef struct connection_cfg_s    connection_cfg_t;

struct connection_cfg_s {
  char                  *driver;
  char                  *dsn;
  char                  *uid;
  char                  *pwd;
  char                  *ip;
  char                  *db;
  int                    port;

  unsigned int           legacy:1;
  unsigned int           fmt_time:1;
  // NOTE: 1.this is to hack node.odbc, which maps SQL_TINYINT to SQL_C_UTINYINT
  //       2.node.odbc does not call SQLGetInfo/SQLColAttribute to get signess of integers
  unsigned int           unsigned_promotion:1;
  unsigned int           cache_sql:1;
};

static inline void connection_cfg_release(connection_cfg_t *conn_str)
{
  if (!conn_str) return;

  TOD_SAFE_FREE(conn_str->driver);
  TOD_SAFE_FREE(conn_str->dsn);
  TOD_SAFE_FREE(conn_str->uid);
  TOD_SAFE_FREE(conn_str->pwd);
  TOD_SAFE_FREE(conn_str->ip);
  TOD_SAFE_FREE(conn_str->db);
}

static inline void connection_cfg_transfer(connection_cfg_t *from, connection_cfg_t *to)
{
  if (from == to) return;
  connection_cfg_release(to);
  *to = *from;
  memset(from, 0, sizeof(*from));
}

typedef struct conn_s              conn_t;
typedef struct desc_s              desc_t;

conn_t* conn_create(env_t *env) FA_HIDDEN;
conn_t* conn_ref(conn_t *conn) FA_HIDDEN;
conn_t* conn_unref(conn_t *conn) FA_HIDDEN;
SQLRETURN conn_free(conn_t *conn) FA_HIDDEN;

SQLRETURN conn_driver_connect(
    conn_t         *conn,
    SQLHWND         WindowHandle,
    SQLCHAR        *InConnectionString,
    SQLSMALLINT     StringLength1,
    SQLCHAR        *OutConnectionString,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLength2Ptr) FA_HIDDEN;

void conn_disconnect(conn_t *conn) FA_HIDDEN;

int conn_get_dbms_name(conn_t *conn, const char **name) FA_HIDDEN;
int conn_get_driver_name(conn_t *conn, const char **name) FA_HIDDEN;

int conn_rollback(conn_t *conn) FA_HIDDEN;

SQLRETURN conn_get_diag_rec(
    conn_t         *conn,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr) FA_HIDDEN;

SQLRETURN conn_alloc_stmt(conn_t *conn, SQLHANDLE *OutputHandle) FA_HIDDEN;

SQLRETURN conn_alloc_desc(conn_t *conn, SQLHANDLE *OutputHandle) FA_HIDDEN;

SQLRETURN conn_connect(
    conn_t        *conn,
    SQLCHAR       *ServerName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *UserName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *Authentication,
    SQLSMALLINT    NameLength3) FA_HIDDEN;

SQLRETURN conn_get_info(
    conn_t         *conn,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr) FA_HIDDEN;

EXTERN_C_END

#endif //  _conn_h_

