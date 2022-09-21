#ifndef _enums_h_
#define _enums_h_

#include "macros.h"

#include <sqlext.h>

EXTERN_C_BEGIN

const char *sql_c_data_type_to_str(SQLSMALLINT sql_c_data_type) FA_HIDDEN;
const char *sql_data_type_to_str(SQLSMALLINT sql_data_type) FA_HIDDEN;

EXTERN_C_END

#endif // _enums_h_

