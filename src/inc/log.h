#ifndef _log_h_
#define _log_h_

#include "macros.h"

#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>

EXTERN_C_BEGIN

int tod_get_debug(void) FA_HIDDEN;

#define OD(_fmt, ...)                                                \
  do {                                                               \
    if (!tod_get_debug()) break;                                     \
    fprintf(stderr, "%s[%d]:%s(): " _fmt "\n",                       \
                    basename((char*)__FILE__), __LINE__, __func__,   \
                    ##__VA_ARGS__);                                  \
  } while (0)

#define OA(_statement, _fmt, ...)                                         \
  do {                                                                    \
    if (!(_statement)) {                                                  \
    fprintf(stderr, "%s[%d]:%s(): assertion failed: [%s]" _fmt "\n",      \
                    basename((char*)__FILE__), __LINE__, __func__,        \
                      #_statement,                                        \
                    ##__VA_ARGS__);                                       \
      abort();                                                            \
    }                                                                     \
  } while (0)

#define OA_ILE(_statement)        OA(_statement, "internal logic error")
#define OA_NIY(_statement)        OA(_statement, "not implemented yet")
#define OA_DM(_statement)         OA(_statement, "DM logic error")

EXTERN_C_END


#endif // _log_h_

