#include "enums.h"

const char *sql_c_data_type(SQLSMALLINT v)
{
#define CASE(_x) case _x: return #_x
  switch (v) {
    CASE(SQL_C_CHAR);
    CASE(SQL_C_TINYINT);
    CASE(SQL_C_SLONG);
    CASE(SQL_C_SSHORT);
    CASE(SQL_C_SHORT);
    CASE(SQL_C_STINYINT);
    CASE(SQL_C_ULONG);
    CASE(SQL_C_USHORT);
    CASE(SQL_C_UTINYINT);
    CASE(SQL_C_DOUBLE);
    CASE(SQL_C_DATE);
    CASE(SQL_C_TIME);
    CASE(SQL_C_TIMESTAMP);
    CASE(SQL_C_TYPE_DATE);
    CASE(SQL_C_TYPE_TIME);
    CASE(SQL_C_TYPE_TIMESTAMP);
    CASE(SQL_C_INTERVAL_YEAR);
    CASE(SQL_C_INTERVAL_MONTH);
    CASE(SQL_C_INTERVAL_DAY);
    CASE(SQL_C_INTERVAL_HOUR);
    CASE(SQL_C_INTERVAL_MINUTE);
    CASE(SQL_C_INTERVAL_SECOND);
    CASE(SQL_C_INTERVAL_YEAR_TO_MONTH);
    CASE(SQL_C_INTERVAL_DAY_TO_HOUR);
    CASE(SQL_C_INTERVAL_DAY_TO_MINUTE);
    CASE(SQL_C_INTERVAL_DAY_TO_SECOND);
    CASE(SQL_C_INTERVAL_HOUR_TO_MINUTE);
    CASE(SQL_C_INTERVAL_HOUR_TO_SECOND);
    CASE(SQL_C_INTERVAL_MINUTE_TO_SECOND);
    CASE(SQL_C_BINARY);
    CASE(SQL_C_BIT);
    CASE(SQL_C_SBIGINT);
    CASE(SQL_C_UBIGINT);
    CASE(SQL_C_WCHAR);
    CASE(SQL_C_NUMERIC);
    CASE(SQL_C_GUID);
    CASE(SQL_C_DEFAULT);
    default:
    return "SQL_C_unknown";
  }
#undef CASE
}

const char *sql_data_type(SQLSMALLINT v)
{
#define CASE(_x) case _x: return #_x
  switch (v) {
    CASE(SQL_CHAR);
    CASE(SQL_VARCHAR);
    CASE(SQL_LONGVARCHAR);
    CASE(SQL_WCHAR);
    CASE(SQL_WVARCHAR);
    CASE(SQL_WLONGVARCHAR);
    CASE(SQL_DECIMAL);
    CASE(SQL_NUMERIC);
    CASE(SQL_SMALLINT);
    CASE(SQL_INTEGER);
    CASE(SQL_REAL);
    CASE(SQL_FLOAT);
    CASE(SQL_DOUBLE);
    CASE(SQL_BIT);
    CASE(SQL_TINYINT);
    CASE(SQL_BIGINT);
    CASE(SQL_BINARY);
    CASE(SQL_VARBINARY);
    CASE(SQL_LONGVARBINARY);
    CASE(SQL_DATETIME);
    CASE(SQL_TYPE_DATE);
    CASE(SQL_TYPE_TIME);
    CASE(SQL_TYPE_TIMESTAMP);
    // CASE(SQL_TYPE_UTCDATETIME);
    // CASE(SQL_TYPE_UTCTIME);
    CASE(SQL_INTERVAL_MONTH);
    CASE(SQL_INTERVAL_YEAR);
    CASE(SQL_INTERVAL_YEAR_TO_MONTH);
    CASE(SQL_INTERVAL_DAY);
    CASE(SQL_INTERVAL_HOUR);
    CASE(SQL_INTERVAL_MINUTE);
    CASE(SQL_INTERVAL_SECOND);
    CASE(SQL_INTERVAL_DAY_TO_HOUR);
    CASE(SQL_INTERVAL_DAY_TO_MINUTE);
    CASE(SQL_INTERVAL_DAY_TO_SECOND);
    CASE(SQL_INTERVAL_HOUR_TO_MINUTE);
    CASE(SQL_INTERVAL_HOUR_TO_SECOND);
    CASE(SQL_INTERVAL_MINUTE_TO_SECOND);
    CASE(SQL_GUID);
    default:
    return "SQL_TYPE_unknown";
  }
#undef CASE
}

const char *sql_handle_type(SQLSMALLINT v)
{
#define CASE(_x) case _x: return #_x
  switch (v) {
    CASE(SQL_HANDLE_ENV);
    CASE(SQL_HANDLE_DBC);
    CASE(SQL_HANDLE_STMT);
    default:
    return "SQL_HANDLE_unknown";
  }
#undef CASE
}

const char *sql_driver_completion(SQLUSMALLINT v)
{
#define CASE(_x) case _x: return #_x
  switch (v) {
    CASE(SQL_DRIVER_NOPROMPT);
    default:
    return "SQL_DRIVER_unknown";
  }
#undef CASE
}

const char *sql_odbc_version(SQLINTEGER v)
{
#define CASE(_x) case _x: return #_x
  switch (v) {
    CASE(SQL_OV_ODBC3);
    CASE(SQL_OV_ODBC3_80);
    CASE(SQL_OV_ODBC2);
    default:
    return "SQL_OV_unknown";
  }
#undef CASE
}

const char *sql_info_type(SQLUSMALLINT v)
{
#define CASE(_x) case _x: return #_x
  switch (v) {
    CASE(SQL_DBMS_NAME);
    CASE(SQL_DRIVER_NAME);
    CASE(SQL_CURSOR_COMMIT_BEHAVIOR);
    CASE(SQL_TXN_ISOLATION_OPTION);
    CASE(SQL_GETDATA_EXTENSIONS);
    CASE(SQL_MAX_COLUMN_NAME_LEN);
    default:
    return "SQL_unknown";
  }
#undef CASE
}

const char *sql_completion_type(SQLSMALLINT v)
{
#define CASE(_x) case _x: return #_x
  switch (v) {
    CASE(SQL_COMMIT);
    CASE(SQL_ROLLBACK);
    default:
    return "SQL_unknown";
  }
#undef CASE
}

const char *sql_connection_attr(SQLINTEGER v)
{
#define CASE(_x) case _x: return #_x
  switch (v) {
    CASE(SQL_ATTR_CONNECTION_TIMEOUT);
    CASE(SQL_ATTR_LOGIN_TIMEOUT);
    default:
    return "SQL_ATTR_unknown";
  }
#undef CASE
}

const char *sql_cursor_type(SQLULEN v)
{
#define CASE(_x) case _x: return #_x
  switch (v) {
    CASE(SQL_CURSOR_FORWARD_ONLY);
    CASE(SQL_CURSOR_STATIC);
    CASE(SQL_CURSOR_KEYSET_DRIVEN);
    CASE(SQL_CURSOR_DYNAMIC);
    default:
    return "SQL_CURSOR_unknown";
  }
#undef CASE
}

const char *sql_statement_attr(SQLINTEGER v)
{
#define CASE(_x) case _x: return #_x
  switch (v) {
    CASE(SQL_ATTR_CURSOR_TYPE);
    CASE(SQL_ATTR_ROW_ARRAY_SIZE);
    CASE(SQL_ATTR_ROW_STATUS_PTR);
    CASE(SQL_ATTR_ROW_BIND_TYPE);
    CASE(SQL_ATTR_ROWS_FETCHED_PTR);
    default:
    return "SQL_ATTR_unknown";
  }
#undef CASE
}

const char *sql_free_statement_option(SQLUSMALLINT v)
{
#define CASE(_x) case _x: return #_x
  switch (v) {
    CASE(SQL_CLOSE);
    CASE(SQL_DROP);
    CASE(SQL_UNBIND);
    CASE(SQL_RESET_PARAMS);
    default:
    return "SQL_unknown";
  }
#undef CASE
}

