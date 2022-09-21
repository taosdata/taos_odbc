#include "internal.h"

#include "env.h"

#include <pthread.h>

static void _exit_routine(void)
{
  TAOS_cleanup();
}

static void _init_once(void)
{
  OA(0==TAOS_init(), "taos_init failed");
  atexit(_exit_routine);
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
  errs_release(&env->errs);
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

