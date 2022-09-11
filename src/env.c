#include "internal.h"

#include <libgen.h>
#include <pthread.h>
#include <string.h>


static void _exit_routine(void)
{
  taos_cleanup();
}

static void _init_once(void)
{
  OA(0==taos_init(), "taos_init failed");
  atexit(_exit_routine);
}

void err_set_x(err_t *err, const char *file, int line, const char *func, int e, const char *estr, const char *sql_state)
{
    err->err = e;
    snprintf(err->buf, sizeof(err->buf),
        "%s[%d]:%s(): %s",
        basename((char*)file), line, func,
        estr);
    err->estr = err->buf;
    strncpy((char*)err->sql_state, sql_state, sizeof(err->sql_state));
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

