#ifndef _utils_h_
#define _utils_h_

#include "macros.h"

#include <stdlib.h>

EXTERN_C_BEGIN

#define TOD_SAFE_FREE(_p) if (_p) { free(_p); _p = NULL; }


EXTERN_C_END

#endif // _utils_h_

