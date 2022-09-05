#ifndef _internal_h_
#define _internal_h_

#include "env.h"
#include "conn.h"
#include "stmt.h"

#include <stdatomic.h>

#include <taos.h>

EXTERN_C_BEGIN

struct env_s {
  atomic_int          refc;
};

struct conn_s {
  atomic_int          refc;

  env_t              *env;

  TAOS               *taos;
};

struct stmt_s {
  atomic_int          refc;

  conn_t             *conn;
};

EXTERN_C_END

#endif // _internal_h_

