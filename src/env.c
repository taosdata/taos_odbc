#include "internal.h"

#include <libgen.h>
#include <pthread.h>
#include <string.h>

#include <sqlext.h>

static void _exit_routine(void)
{
  TAOS_cleanup();
}

static void _init_once(void)
{
  OA(0==TAOS_init(), "taos_init failed");
  atexit(_exit_routine);
}

void err_set_x(err_t *err, const char *file, int line, const char *func, const char *sql_state, int e, const char *estr)
{
    err->err = e;
    snprintf(err->buf, sizeof(err->buf),
        "%s[%d]:%s(): %s",
        basename((char*)file), line, func,
        estr);
    err->estr = err->buf;
    strncpy((char*)err->sql_state, sql_state, sizeof(err->sql_state));
}

const char *sql_c_data_type_to_str(SQLSMALLINT sql_c_data_type)
{
#define CASE(_x) case _x: return #_x
  switch (sql_c_data_type) {
    CASE(SQL_C_CHAR);
    CASE(SQL_C_TINYINT);
    CASE(SQL_C_SLONG);
    CASE(SQL_C_SSHORT);
    CASE(SQL_C_STINYINT);
    CASE(SQL_C_ULONG);
    CASE(SQL_C_USHORT);
    CASE(SQL_C_UTINYINT);
    CASE(SQL_C_DATE);
    CASE(SQL_C_TIME);
    CASE(SQL_C_TIMESTAMP);
    CASE(SQL_C_TYPE_DATE);
    CASE(SQL_C_TYPE_TIME);
    CASE(SQL_C_TYPE_TIMESTAMP);
    CASE(SQL_C_INTERVAL_YEAR);
    CASE(SQL_C_INTERVAL_MONTH);
    CASE(SQL_C_INTERVAL_DAY);
    CASE(SQL_C_INTERVAL_HOUR);
    CASE(SQL_C_INTERVAL_MINUTE);
    CASE(SQL_C_INTERVAL_SECOND);
    CASE(SQL_C_INTERVAL_YEAR_TO_MONTH);
    CASE(SQL_C_INTERVAL_DAY_TO_HOUR);
    CASE(SQL_C_INTERVAL_DAY_TO_MINUTE);
    CASE(SQL_C_INTERVAL_DAY_TO_SECOND);
    CASE(SQL_C_INTERVAL_HOUR_TO_MINUTE);
    CASE(SQL_C_INTERVAL_HOUR_TO_SECOND);
    CASE(SQL_C_INTERVAL_MINUTE_TO_SECOND);
    CASE(SQL_C_BINARY);
    CASE(SQL_C_BIT);
    CASE(SQL_C_SBIGINT);
    CASE(SQL_C_UBIGINT);
    CASE(SQL_C_WCHAR);
    CASE(SQL_C_NUMERIC);
    CASE(SQL_C_GUID);
    CASE(SQL_C_DEFAULT);
    default:
    OA(0, "unknown sql_c_data_type[%d]", sql_c_data_type);
  }
#undef CASE
}

const char *sql_data_type_to_str(SQLSMALLINT sql_data_type)
{
#define CASE(_x) case _x: return #_x
  switch (sql_data_type) {
    CASE(SQL_CHAR);
    CASE(SQL_VARCHAR);
    CASE(SQL_LONGVARCHAR);
    CASE(SQL_WCHAR);
    CASE(SQL_WVARCHAR);
    CASE(SQL_WLONGVARCHAR);
    CASE(SQL_DECIMAL);
    CASE(SQL_NUMERIC);
    CASE(SQL_SMALLINT);
    CASE(SQL_INTEGER);
    CASE(SQL_REAL);
    CASE(SQL_FLOAT);
    CASE(SQL_DOUBLE);
    CASE(SQL_BIT);
    CASE(SQL_TINYINT);
    CASE(SQL_BIGINT);
    CASE(SQL_BINARY);
    CASE(SQL_VARBINARY);
    CASE(SQL_LONGVARBINARY);
    CASE(SQL_TYPE_DATE);
    CASE(SQL_TYPE_TIME);
    CASE(SQL_TYPE_TIMESTAMP);
    // CASE(SQL_TYPE_UTCDATETIME);
    // CASE(SQL_TYPE_UTCTIME);
    CASE(SQL_INTERVAL_MONTH);
    CASE(SQL_INTERVAL_YEAR);
    CASE(SQL_INTERVAL_YEAR_TO_MONTH);
    CASE(SQL_INTERVAL_DAY);
    CASE(SQL_INTERVAL_HOUR);
    CASE(SQL_INTERVAL_MINUTE);
    CASE(SQL_INTERVAL_SECOND);
    CASE(SQL_INTERVAL_DAY_TO_HOUR);
    CASE(SQL_INTERVAL_DAY_TO_MINUTE);
    CASE(SQL_INTERVAL_DAY_TO_SECOND);
    CASE(SQL_INTERVAL_HOUR_TO_MINUTE);
    CASE(SQL_INTERVAL_HOUR_TO_SECOND);
    CASE(SQL_INTERVAL_MINUTE_TO_SECOND);
    CASE(SQL_GUID);
    default:
    OA(0, "unknown sql_data_type[%d]", sql_data_type);
  }
#undef CASE
}

static int env_init(env_t *env)
{
  static pthread_once_t          once;
  pthread_once(&once, _init_once);

  // TODO:

  env->refc = 1;

  return 0;
}

static void env_release(env_t *env)
{
  int conns = atomic_load(&env->conns);
  OA_ILE(conns == 0);
  return;
}

env_t* env_create(void)
{
  env_t *env = (env_t*)calloc(1, sizeof(*env));
  if (!env) return NULL;

  int r = env_init(env);
  if (r) {
    env_release(env);
    return NULL;
  }

  return env;
}

env_t* env_ref(env_t *env)
{
  OA_ILE(env);
  int prev = atomic_fetch_add(&env->refc, 1);
  OA_ILE(prev>0);
  return env;
}

env_t* env_unref(env_t *env)
{
  OA_ILE(env);
  int prev = atomic_fetch_sub(&env->refc, 1);
  if (prev>1) return env;
  OA_ILE(prev==1);

  env_release(env);
  free(env);

  return NULL;
}

int env_rollback(env_t *env)
{
  int conns = atomic_load(&env->conns);
  return conns == 0 ? 0 : -1;
}

