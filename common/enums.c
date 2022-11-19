/*
 * MIT License
 *
 * Copyright (c) 2022 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "enums.h"

#define CASE(_x) case _x: return #_x

const char *sql_c_data_type(SQLSMALLINT v)
{
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
    CASE(SQL_C_FLOAT);
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
}

const char *sql_data_type(SQLSMALLINT v)
{
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
}

const char *sql_handle_type(SQLSMALLINT v)
{
  switch (v) {
    CASE(SQL_HANDLE_ENV);
    CASE(SQL_HANDLE_DBC);
    CASE(SQL_HANDLE_STMT);
    default:
    return "SQL_HANDLE_unknown";
  }
}

const char *sql_driver_completion(SQLUSMALLINT v)
{
  switch (v) {
    CASE(SQL_DRIVER_NOPROMPT);
    default:
    return "SQL_DRIVER_unknown";
  }
}

const char *sql_odbc_version(SQLINTEGER v)
{
  switch (v) {
    CASE(SQL_OV_ODBC3);
    CASE(SQL_OV_ODBC3_80);
    CASE(SQL_OV_ODBC2);
    default:
    return "SQL_OV_unknown";
  }
}

const char *sql_info_type(SQLUSMALLINT v)
{
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
}

const char *sql_completion_type(SQLSMALLINT v)
{
  switch (v) {
    CASE(SQL_COMMIT);
    CASE(SQL_ROLLBACK);
    default:
    return "SQL_unknown";
  }
}

const char *sql_conn_attr(SQLINTEGER v)
{
  switch (v) {
    CASE(SQL_ATTR_CONNECTION_TIMEOUT);
    CASE(SQL_ATTR_LOGIN_TIMEOUT);
    default:
    return "SQL_ATTR_unknown";
  }
}

const char *sql_cursor_type(SQLULEN v)
{
  switch (v) {
    CASE(SQL_CURSOR_FORWARD_ONLY);
    CASE(SQL_CURSOR_STATIC);
    CASE(SQL_CURSOR_KEYSET_DRIVEN);
    CASE(SQL_CURSOR_DYNAMIC);
    default:
    return "SQL_CURSOR_unknown";
  }
}

const char *sql_free_statement_option(SQLUSMALLINT v)
{
  switch (v) {
    CASE(SQL_CLOSE);
    CASE(SQL_DROP);
    CASE(SQL_UNBIND);
    CASE(SQL_RESET_PARAMS);
    default:
    return "SQL_unknown";
  }
}

const char *sql_return_type(SQLRETURN v)
{
  switch (v) {
    CASE(SQL_SUCCESS);
    CASE(SQL_SUCCESS_WITH_INFO);
    CASE(SQL_NEED_DATA);
    CASE(SQL_STILL_EXECUTING);
    CASE(SQL_ERROR);
    CASE(SQL_NO_DATA);
    CASE(SQL_INVALID_HANDLE);
    CASE(SQL_PARAM_DATA_AVAILABLE);
    default:
    return "SQL_unknown";
  }
}

const char *sql_env_attr(SQLINTEGER v)
{
  switch (v) {
    CASE(SQL_ATTR_CONNECTION_POOLING);
    CASE(SQL_ATTR_CP_MATCH);
    CASE(SQL_ATTR_ODBC_VERSION);
    CASE(SQL_ATTR_OUTPUT_NTS);
    default:
    return "SQL_ATTR_unknown";
  }
}

const char *sql_input_output_type(SQLSMALLINT v)
{
  switch (v) {
    CASE(SQL_PARAM_INPUT);
    CASE(SQL_PARAM_INPUT_OUTPUT);
    CASE(SQL_PARAM_OUTPUT);
    CASE(SQL_PARAM_INPUT_OUTPUT_STREAM);
    CASE(SQL_PARAM_OUTPUT_STREAM);
    default:
    return "SQL_PARAM_unknown";
  }
}

const char *sql_stmt_attr(SQLINTEGER v)
{
  switch (v) {
    CASE(SQL_ATTR_APP_PARAM_DESC);
    CASE(SQL_ATTR_APP_ROW_DESC);
    CASE(SQL_ATTR_ASYNC_ENABLE);
    CASE(SQL_ATTR_ASYNC_STMT_EVENT);
    // ODBC 3.8
    // CASE(SQL_ATTR_ASYNC_STMT_PCALLBACK);
    // CASE(SQL_ATTR_ASYNC_STMT_PCONTEXT);
    CASE(SQL_ATTR_CONCURRENCY);
    CASE(SQL_ATTR_CURSOR_SCROLLABLE);
    CASE(SQL_ATTR_CURSOR_SENSITIVITY);
    CASE(SQL_ATTR_CURSOR_TYPE);
    CASE(SQL_ATTR_ENABLE_AUTO_IPD);
    CASE(SQL_ATTR_FETCH_BOOKMARK_PTR);
    CASE(SQL_ATTR_IMP_PARAM_DESC);
    CASE(SQL_ATTR_IMP_ROW_DESC);
    CASE(SQL_ATTR_KEYSET_SIZE);
    CASE(SQL_ATTR_MAX_LENGTH);
    CASE(SQL_ATTR_MAX_ROWS);
    CASE(SQL_ATTR_METADATA_ID);
    CASE(SQL_ATTR_NOSCAN);
    CASE(SQL_ATTR_PARAM_BIND_OFFSET_PTR);
    CASE(SQL_ATTR_PARAM_BIND_TYPE);
    CASE(SQL_ATTR_PARAM_OPERATION_PTR);
    CASE(SQL_ATTR_PARAM_STATUS_PTR);
    CASE(SQL_ATTR_PARAMS_PROCESSED_PTR);
    CASE(SQL_ATTR_PARAMSET_SIZE);
    CASE(SQL_ATTR_QUERY_TIMEOUT);
    CASE(SQL_ATTR_RETRIEVE_DATA);
    CASE(SQL_ATTR_ROW_BIND_OFFSET_PTR);
    CASE(SQL_ATTR_ROW_BIND_TYPE);
    CASE(SQL_ATTR_ROW_NUMBER);
    CASE(SQL_ATTR_ROW_OPERATION_PTR);
    CASE(SQL_ATTR_ROW_STATUS_PTR);
    CASE(SQL_ATTR_ROWS_FETCHED_PTR);
    CASE(SQL_ATTR_SIMULATE_CURSOR);
    CASE(SQL_ATTR_USE_BOOKMARKS);
    default:
    return "SQL_ATTR_unknown";
  }
}

const char *sql_nullable(SQLSMALLINT v)
{
  switch (v) {
    CASE(SQL_NO_NULLS);
    CASE(SQL_NULLABLE);
    CASE(SQL_NULLABLE_UNKNOWN);
    default:
    return "SQL_NULLABLE_unknown";
  }
}

const char *sql_fetch_orientation(SQLSMALLINT v)
{
  switch (v) {
    CASE(SQL_FETCH_NEXT);
    CASE(SQL_FETCH_PRIOR);
    CASE(SQL_FETCH_FIRST);
    CASE(SQL_FETCH_LAST);
    CASE(SQL_FETCH_ABSOLUTE);
    CASE(SQL_FETCH_RELATIVE);
    CASE(SQL_FETCH_BOOKMARK);
    default:
    return "SQL_FETCH_unknown";
  }
}

