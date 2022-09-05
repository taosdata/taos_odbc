#ifndef _stmt_h_
#define _stmt_h_

#include "conn.h"

EXTERN_C_BEGIN

typedef struct stmt_s              stmt_t;

stmt_t* stmt_create(conn_t *conn) FA_HIDDEN;
stmt_t* stmt_ref(stmt_t *stmt) FA_HIDDEN;
stmt_t* stmt_unref(stmt_t *stmt) FA_HIDDEN;

int stmt_exec_direct(stmt_t *stmt, const char *sql, int len);

EXTERN_C_END

#endif //  _stmt_h_

