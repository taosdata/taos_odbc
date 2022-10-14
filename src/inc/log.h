#ifndef _log_h_
#define _log_h_

#include "macros.h"

#define LOG_IMPL(fmt, ...) do {                  \
  if (!tod_get_debug()) break;                   \
  fprintf(stderr, fmt, ##__VA_ARGS__);           \
} while (0)

#include "helpers.h"

#include <stdio.h>
#include <stdlib.h>

EXTERN_C_BEGIN

int tod_get_debug(void) FA_HIDDEN;

#define OD                        D

#define OA                        A

#define OA_ILE(_statement)        OA(_statement, "internal logic error")
#define OA_NIY(_statement)        OA(_statement, "not implemented yet")
#define OA_DM(_statement)         OA(_statement, "DM logic error")

EXTERN_C_END

#endif // _log_h_

