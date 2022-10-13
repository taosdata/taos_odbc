#ifndef _env_h_
#define _env_h_

#include "log.h"

#include <sqlext.h>

EXTERN_C_BEGIN

typedef struct env_s              env_t;

env_t* env_create(void) FA_HIDDEN;
env_t* env_ref(env_t *env) FA_HIDDEN;
env_t* env_unref(env_t *env) FA_HIDDEN;
SQLRETURN env_free(env_t *env) FA_HIDDEN;

int env_get_debug(env_t *env) FA_HIDDEN;
int env_get_debug_flex(env_t *env) FA_HIDDEN;
int env_get_debug_bison(env_t *env) FA_HIDDEN;

int env_rollback(env_t *env) FA_HIDDEN;

SQLRETURN env_get_diag_rec(
    env_t          *env,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr) FA_HIDDEN;

EXTERN_C_END

#endif // _env_h_

