#include "internal.h"

#include "env.h"

#include <pthread.h>

static unsigned int         _taos_odbc_debug       = 0;
static unsigned int         _taos_odbc_debug_flex  = 0;
static unsigned int         _taos_odbc_debug_bison = 0;
static unsigned int         _taos_init_failed      = 0;

static void _exit_routine(void)
{
  CALL_taos_cleanup();
}

static void _init_once(void)
{
  if (getenv("TAOS_ODBC_DEBUG"))       _taos_odbc_debug       = 1;
  if (getenv("TAOS_ODBC_DEBUG_FLEX"))  _taos_odbc_debug_flex  = 1;
  if (getenv("TAOS_ODBC_DEBUG_BISON")) _taos_odbc_debug_bison = 1;
  if (CALL_taos_init()) {
    _taos_init_failed = 1;
    return;
  }
  atexit(_exit_routine);
}

static int _env_init(env_t *env)
{
  static pthread_once_t          once;
  pthread_once(&once, _init_once);

  // TODO:

  env->debug       = _taos_odbc_debug;
  env->debug_flex  = _taos_odbc_debug_flex;
  env->debug_bison = _taos_odbc_debug_bison;

  errs_init(&env->errs);

  if (_taos_init_failed) return -1;

  env->refc = 1;

  return 0;
}

static void _env_release(env_t *env)
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

  int r = _env_init(env);
  if (r) {
    _env_release(env);
    free(env);
    return NULL;
  }

  return env;
}

env_t* env_ref(env_t *env)
{
  int prev = atomic_fetch_add(&env->refc, 1);
  OA_ILE(prev>0);
  return env;
}

env_t* env_unref(env_t *env)
{
  int prev = atomic_fetch_sub(&env->refc, 1);
  if (prev>1) return env;
  OA_ILE(prev==1);

  _env_release(env);
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

static int _env_rollback(env_t *env)
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

static SQLRETURN _env_set_odbc_version(env_t *env, SQLINTEGER odbc_version)
{
  switch (odbc_version) {
    case SQL_OV_ODBC3:
      return SQL_SUCCESS;
    default:
      env_append_err_format(env, "HY000", 0, "`%s`[0x%x/%d] not supported yet", sql_odbc_version(odbc_version), odbc_version, odbc_version);
      return SQL_ERROR;
  }
}

SQLRETURN env_set_attr(
    env_t       *env,
    SQLINTEGER   Attribute,
    SQLPOINTER   ValuePtr,
    SQLINTEGER   StringLength)
{
  (void)StringLength;

  switch (Attribute) {
    case SQL_ATTR_ODBC_VERSION:
      return _env_set_odbc_version(env, (SQLINTEGER)(size_t)ValuePtr);

    default:
      env_append_err_format(env, "HY000", 0, "`%s`[0x%x/%d] not supported yet", sql_env_attr(Attribute), Attribute, Attribute);
      return SQL_ERROR;
  }
}

SQLRETURN env_end_tran(env_t *env, SQLSMALLINT CompletionType)
{
  switch (CompletionType) {
    case SQL_COMMIT:
      return SQL_SUCCESS;
    case SQL_ROLLBACK:
      if (_env_rollback(env)) return SQL_ERROR;
      return SQL_SUCCESS;
    default:
      env_append_err_format(env, "HY000", 0, "`%s`[0x%x/%d] not supported yet", sql_completion_type(CompletionType), CompletionType, CompletionType);
      return SQL_ERROR;
  }
}

SQLRETURN env_alloc_conn(env_t *env, SQLHANDLE *OutputHandle)
{
  *OutputHandle = SQL_NULL_HANDLE;

  conn_t *conn = conn_create(env);
  if (conn == NULL) return SQL_ERROR;

  *OutputHandle = (SQLHANDLE)conn;
  return SQL_SUCCESS;
}

void env_clr_errs(env_t *env)
{
  errs_clr(&env->errs);
}

