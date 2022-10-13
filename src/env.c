#include "internal.h"

#include "env.h"

#include <pthread.h>

static unsigned int         _taos_odbc_debug       = 0;
static unsigned int         _taos_odbc_debug_flex  = 0;
static unsigned int         _taos_odbc_debug_bison = 0;

static void _exit_routine(void)
{
  CALL_taos_cleanup();
}

static void _init_once(void)
{
  if (getenv("TAOS_ODBC_DEBUG"))       _taos_odbc_debug       = 1;
  if (getenv("TAOS_ODBC_DEBUG_FLEX"))  _taos_odbc_debug_flex  = 1;
  if (getenv("TAOS_ODBC_DEBUG_BISON")) _taos_odbc_debug_bison = 1;
  OA(0==CALL_taos_init(), "taos_init failed");
  atexit(_exit_routine);
}

static int env_init(env_t *env)
{
  static pthread_once_t          once;
  pthread_once(&once, _init_once);

  // TODO:

  env->debug       = _taos_odbc_debug;
  env->debug_flex  = _taos_odbc_debug_flex;
  env->debug_bison = _taos_odbc_debug_bison;

  errs_init(&env->errs);

  env->refc = 1;

  return 0;
}

static void env_release(env_t *env)
{
  int conns = atomic_load(&env->conns);
  OA_ILE(conns == 0);
  errs_release(&env->errs);
  return;
}

int tod_get_debug(void)
{
  return !!_taos_odbc_debug;
}

int env_get_debug(env_t *env)
{
  return !!env->debug;
}

int env_get_debug_flex(env_t *env)
{
  return !!env->debug_flex;
}

int env_get_debug_bison(env_t *env)
{
  return !!env->debug_bison;
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

SQLRETURN env_free(env_t *env)
{
  int conns = atomic_load(&env->conns);
  if (conns) {
    env_append_err_format(env, "HY000", 0, "#%d connections are still connected or allocated", conns);
    return SQL_ERROR;
  }

  env_unref(env);
  return SQL_SUCCESS;
}

int env_rollback(env_t *env)
{
  int conns = atomic_load(&env->conns);
  return conns == 0 ? 0 : -1;
}

SQLRETURN env_get_diag_rec(
    env_t          *env,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr)
{
  return errs_get_diag_rec(&env->errs, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
}

