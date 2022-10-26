#ifndef _enums_h_
#define _enums_h_

#include "macros.h"

#include <sqlext.h>

EXTERN_C_BEGIN

const char *sql_c_data_type(SQLSMALLINT v) FA_HIDDEN;
const char *sql_data_type(SQLSMALLINT v) FA_HIDDEN;
const char *sql_handle_type(SQLSMALLINT v) FA_HIDDEN;
const char *sql_driver_completion(SQLUSMALLINT v) FA_HIDDEN;
const char *sql_odbc_version(SQLINTEGER v) FA_HIDDEN;
const char *sql_info_type(SQLUSMALLINT v) FA_HIDDEN;
const char *sql_completion_type(SQLSMALLINT v) FA_HIDDEN;
const char *sql_connection_attr(SQLINTEGER v) FA_HIDDEN;
const char *sql_cursor_type(SQLULEN v) FA_HIDDEN;
const char *sql_statement_attr(SQLINTEGER v) FA_HIDDEN;
const char *sql_free_statement_option(SQLUSMALLINT v) FA_HIDDEN;
const char *sql_return_type(SQLRETURN v) FA_HIDDEN;
const char *sql_env_attr(SQLINTEGER v) FA_HIDDEN;
const char *sql_input_output_type(SQLSMALLINT v) FA_HIDDEN;
const char *sql_stmt_attr(SQLINTEGER v) FA_HIDDEN;
const char *sql_nullable(SQLSMALLINT v) FA_HIDDEN;

EXTERN_C_END

#endif // _enums_h_

