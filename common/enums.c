/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
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

const char *sqlc_data_type(SQLLEN v)
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

const char *sql_data_type(SQLLEN v)
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
    CASE(SQL_ALL_TYPES);
    default:
    return "SQL_TYPE_unknown";
  }
}

const char *sql_handle_type(SQLLEN v)
{
  switch (v) {
    CASE(SQL_HANDLE_ENV);
    CASE(SQL_HANDLE_DBC);
    CASE(SQL_HANDLE_STMT);
    default:
      return "SQL_HANDLE_unknown";
  }
}

const char *sql_driver_completion(SQLULEN v)
{
  switch (v) {
    CASE(SQL_DRIVER_NOPROMPT);
    CASE(SQL_DRIVER_COMPLETE);
    CASE(SQL_DRIVER_COMPLETE_REQUIRED);
    CASE(SQL_DRIVER_PROMPT);
    default:
    return "SQL_DRIVER_unknown";
  }
}

const char *sql_odbc_version(SQLLEN v)
{
  switch (v) {
    CASE(SQL_OV_ODBC3);
#if (ODBCVER >= 0x0380)      /* { */
    CASE(SQL_OV_ODBC3_80);
#endif                       /* } */
    CASE(SQL_OV_ODBC2);
    default:
    return "SQL_OV_unknown";
  }
}

const char *sql_info_type(SQLULEN v)
{
  // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver16
  switch (v) {
    // Driver Information
    CASE(SQL_ACTIVE_ENVIRONMENTS);
#if (ODBCVER >= 0x0380)      /* { */
    CASE(SQL_ASYNC_DBC_FUNCTIONS);
    CASE(SQL_ASYNC_NOTIFICATION);
#endif                       /* } */
    CASE(SQL_ASYNC_MODE);
    CASE(SQL_BATCH_ROW_COUNT);
    CASE(SQL_BATCH_SUPPORT);
    CASE(SQL_DATA_SOURCE_NAME);
    CASE(SQL_DRIVER_AWARE_POOLING_SUPPORTED);
    CASE(SQL_DRIVER_HDBC);
    CASE(SQL_DRIVER_HDESC);
    CASE(SQL_DRIVER_HENV);
    CASE(SQL_DRIVER_HLIB);
    CASE(SQL_DRIVER_HSTMT);
    CASE(SQL_DRIVER_NAME);
    CASE(SQL_DRIVER_ODBC_VER);
    CASE(SQL_DRIVER_VER);
    CASE(SQL_DYNAMIC_CURSOR_ATTRIBUTES1);
    CASE(SQL_DYNAMIC_CURSOR_ATTRIBUTES2);
    CASE(SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1);
    CASE(SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2);
    CASE(SQL_FILE_USAGE);
    CASE(SQL_GETDATA_EXTENSIONS);
    CASE(SQL_INFO_SCHEMA_VIEWS);
    CASE(SQL_KEYSET_CURSOR_ATTRIBUTES1);
    CASE(SQL_KEYSET_CURSOR_ATTRIBUTES2);
    CASE(SQL_MAX_ASYNC_CONCURRENT_STATEMENTS);
    CASE(SQL_MAX_CONCURRENT_ACTIVITIES);
    CASE(SQL_MAX_DRIVER_CONNECTIONS);
    CASE(SQL_ODBC_INTERFACE_CONFORMANCE);
    // CASE(SQL_ODBC_STANDARD_CLI_CONFORMANCE);
    CASE(SQL_ODBC_VER);
    CASE(SQL_PARAM_ARRAY_ROW_COUNTS);
    CASE(SQL_PARAM_ARRAY_SELECTS);
    CASE(SQL_ROW_UPDATES);
    CASE(SQL_SEARCH_PATTERN_ESCAPE);
    CASE(SQL_SERVER_NAME);
    CASE(SQL_STATIC_CURSOR_ATTRIBUTES1);
    CASE(SQL_STATIC_CURSOR_ATTRIBUTES2);

    // DBMS Product Information
    CASE(SQL_DATABASE_NAME);
    CASE(SQL_DBMS_NAME);
    CASE(SQL_DBMS_VER);

    // Data Source Information
    CASE(SQL_ACCESSIBLE_PROCEDURES);
    CASE(SQL_ACCESSIBLE_TABLES);
    CASE(SQL_BOOKMARK_PERSISTENCE);
    CASE(SQL_CATALOG_TERM);
    CASE(SQL_COLLATION_SEQ);
    CASE(SQL_CONCAT_NULL_BEHAVIOR);
    CASE(SQL_CURSOR_COMMIT_BEHAVIOR);
    CASE(SQL_CURSOR_ROLLBACK_BEHAVIOR);
    CASE(SQL_CURSOR_SENSITIVITY);
    CASE(SQL_DATA_SOURCE_READ_ONLY);
    CASE(SQL_DEFAULT_TXN_ISOLATION);
    CASE(SQL_DESCRIBE_PARAMETER);
    CASE(SQL_MULT_RESULT_SETS);
    CASE(SQL_MULTIPLE_ACTIVE_TXN);
    CASE(SQL_NEED_LONG_DATA_LEN);
    CASE(SQL_NULL_COLLATION);
    CASE(SQL_PROCEDURE_TERM);
    CASE(SQL_SCHEMA_TERM);
    CASE(SQL_SCROLL_OPTIONS);
    CASE(SQL_TABLE_TERM);
    CASE(SQL_TXN_CAPABLE);
    CASE(SQL_TXN_ISOLATION_OPTION);
    CASE(SQL_USER_NAME);

    // Supported SQL
    CASE(SQL_AGGREGATE_FUNCTIONS);
    CASE(SQL_ALTER_DOMAIN);
    // CASE(SQL_ALTER_SCHEMA);
    CASE(SQL_ALTER_TABLE);
    // CASE(SQL_ANSI_SQL_DATETIME_LITERALS);
    CASE(SQL_QUALIFIER_LOCATION); /* SQL_CATALOG_LOCATION */
    CASE(SQL_CATALOG_NAME);
    CASE(SQL_CATALOG_NAME_SEPARATOR);
    CASE(SQL_CATALOG_USAGE);
    CASE(SQL_COLUMN_ALIAS);
    CASE(SQL_CORRELATION_NAME);
    CASE(SQL_CREATE_ASSERTION);
    CASE(SQL_CREATE_CHARACTER_SET);
    CASE(SQL_CREATE_COLLATION);
    CASE(SQL_CREATE_DOMAIN);
    CASE(SQL_CREATE_SCHEMA);
    CASE(SQL_CREATE_TABLE);
    CASE(SQL_CREATE_TRANSLATION);
    CASE(SQL_DDL_INDEX);
    CASE(SQL_DROP_ASSERTION);
    CASE(SQL_DROP_CHARACTER_SET);
    CASE(SQL_DROP_COLLATION);
    CASE(SQL_DROP_DOMAIN);
    CASE(SQL_DROP_SCHEMA);
    CASE(SQL_DROP_TABLE);
    CASE(SQL_DROP_TRANSLATION);
    CASE(SQL_DROP_VIEW);
    CASE(SQL_EXPRESSIONS_IN_ORDERBY);
    CASE(SQL_GROUP_BY);
    CASE(SQL_IDENTIFIER_CASE);
    CASE(SQL_IDENTIFIER_QUOTE_CHAR);
    CASE(SQL_INDEX_KEYWORDS);
    CASE(SQL_INSERT_STATEMENT);
    CASE(SQL_INTEGRITY);
    CASE(SQL_KEYWORDS);
    CASE(SQL_LIKE_ESCAPE_CLAUSE);
    CASE(SQL_NON_NULLABLE_COLUMNS);
    CASE(SQL_OJ_CAPABILITIES);
    CASE(SQL_ORDER_BY_COLUMNS_IN_SELECT);
    CASE(SQL_OUTER_JOINS);
    CASE(SQL_PROCEDURES);
    CASE(SQL_QUOTED_IDENTIFIER_CASE);
    CASE(SQL_OWNER_USAGE); /* SQL_SCHEMA_USAGE */
    CASE(SQL_SPECIAL_CHARACTERS);
    CASE(SQL_SQL_CONFORMANCE);
    CASE(SQL_SUBQUERIES);
    CASE(SQL_UNION);

    // SQL Limits
    CASE(SQL_MAX_BINARY_LITERAL_LEN);
    CASE(SQL_MAX_CATALOG_NAME_LEN);
    CASE(SQL_MAX_CHAR_LITERAL_LEN);
    CASE(SQL_MAX_COLUMN_NAME_LEN);
    CASE(SQL_MAX_COLUMNS_IN_GROUP_BY);
    CASE(SQL_MAX_COLUMNS_IN_INDEX);
    CASE(SQL_MAX_COLUMNS_IN_ORDER_BY);
    CASE(SQL_MAX_COLUMNS_IN_SELECT);
    CASE(SQL_MAX_COLUMNS_IN_TABLE);
    CASE(SQL_MAX_CURSOR_NAME_LEN);
    CASE(SQL_MAX_IDENTIFIER_LEN);
    CASE(SQL_MAX_INDEX_SIZE);
    CASE(SQL_MAX_PROCEDURE_NAME_LEN);
    CASE(SQL_MAX_ROW_SIZE);
    CASE(SQL_MAX_ROW_SIZE_INCLUDES_LONG);
    CASE(SQL_MAX_SCHEMA_NAME_LEN);
    CASE(SQL_MAX_STATEMENT_LEN);
    CASE(SQL_MAX_TABLE_NAME_LEN);
    CASE(SQL_MAX_TABLES_IN_SELECT);
    CASE(SQL_MAX_USER_NAME_LEN);

    // Scalar Function Information
    CASE(SQL_CONVERT_FUNCTIONS);
    CASE(SQL_NUMERIC_FUNCTIONS);
    CASE(SQL_STRING_FUNCTIONS);
    CASE(SQL_SYSTEM_FUNCTIONS);
    CASE(SQL_TIMEDATE_ADD_INTERVALS);
    CASE(SQL_TIMEDATE_DIFF_INTERVALS);
    CASE(SQL_TIMEDATE_FUNCTIONS);

    // Conversion Information
    CASE(SQL_CONVERT_BIGINT);
    CASE(SQL_CONVERT_BINARY);
    CASE(SQL_CONVERT_BIT);
    CASE(SQL_CONVERT_CHAR);
    CASE(SQL_CONVERT_DATE);
    CASE(SQL_CONVERT_DECIMAL);
    CASE(SQL_CONVERT_DOUBLE);
    CASE(SQL_CONVERT_FLOAT);
    CASE(SQL_CONVERT_INTEGER);
    CASE(SQL_CONVERT_INTERVAL_DAY_TIME);
    CASE(SQL_CONVERT_INTERVAL_YEAR_MONTH);
    CASE(SQL_CONVERT_LONGVARBINARY);
    CASE(SQL_CONVERT_LONGVARCHAR);
    CASE(SQL_CONVERT_NUMERIC);
    CASE(SQL_CONVERT_REAL);
    CASE(SQL_CONVERT_SMALLINT);
    CASE(SQL_CONVERT_TIME);
    CASE(SQL_CONVERT_TIMESTAMP);
    CASE(SQL_CONVERT_TINYINT);
    CASE(SQL_CONVERT_VARBINARY);
    CASE(SQL_CONVERT_VARCHAR);

    CASE(SQL_DTC_TRANSITION_COST);

    // Information Types Added for ODBC 3.x
    // Note: check it later
    // Information Types Renamed for ODBC 3.x
    // Note: check it later
    // Information Types Deprecated in ODBC 3.x
    // Note: check it later

    default:
    return "SQL_unknown";
  }
}

const char *sql_completion_type(SQLLEN v)
{
  switch (v) {
    CASE(SQL_COMMIT);
    CASE(SQL_ROLLBACK);
    default:
    return "SQL_unknown";
  }
}

const char *sql_conn_attr(SQLLEN v)
{
  switch (v) {
    CASE(SQL_ATTR_CONNECTION_TIMEOUT);
    CASE(SQL_ATTR_LOGIN_TIMEOUT);
    CASE(SQL_CURRENT_QUALIFIER);
    CASE(SQL_ATTR_QUIET_MODE);
    CASE(SQL_ATTR_TXN_ISOLATION);
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

const char *sql_free_statement_option(SQLULEN v)
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
#if (ODBCVER >= 0x0380)      /* { */
    CASE(SQL_PARAM_DATA_AVAILABLE);
#endif                       /* } */
    default:
    return "SQL_unknown";
  }
}

const char *sql_env_attr(SQLLEN v)
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

const char *sql_input_output_type(SQLLEN v)
{
  switch (v) {
    CASE(SQL_PARAM_INPUT);
    CASE(SQL_PARAM_INPUT_OUTPUT);
    CASE(SQL_PARAM_OUTPUT);
#if (ODBCVER >= 0x0380)      /* { */
    CASE(SQL_PARAM_INPUT_OUTPUT_STREAM);
    CASE(SQL_PARAM_OUTPUT_STREAM);
#endif                       /* } */
    default:
    return "SQL_PARAM_unknown";
  }
}

const char *sql_stmt_attr(SQLLEN v)
{
  switch (v) {
    CASE(SQL_ATTR_APP_PARAM_DESC);
    CASE(SQL_ATTR_APP_ROW_DESC);
    CASE(SQL_ATTR_ASYNC_ENABLE);
#if (ODBCVER >= 0x0380)      /* { */
    CASE(SQL_ATTR_ASYNC_STMT_EVENT);
    CASE(SQL_ATTR_ASYNC_STMT_PCALLBACK);
    CASE(SQL_ATTR_ASYNC_STMT_PCONTEXT);
#endif                       /* } */
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
    CASE(SQL_ATTR_ROW_ARRAY_SIZE);
    default:
    return "SQL_ATTR_unknown";
  }
}

const char *sql_nullable(SQLLEN v)
{
  switch (v) {
    CASE(SQL_NO_NULLS);
    CASE(SQL_NULLABLE);
    CASE(SQL_NULLABLE_UNKNOWN);
    default:
    return "SQL_NULLABLE_unknown";
  }
}

const char *sql_fetch_orientation(SQLLEN v)
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

const char *sql_diag_identifier(SQLLEN v)
{
  switch (v) {
    CASE(SQL_DIAG_CURSOR_ROW_COUNT);
    CASE(SQL_DIAG_DYNAMIC_FUNCTION);
    CASE(SQL_DIAG_DYNAMIC_FUNCTION_CODE);
    CASE(SQL_DIAG_NUMBER);
    CASE(SQL_DIAG_RETURNCODE);
    CASE(SQL_DIAG_ROW_COUNT);
    CASE(SQL_DIAG_CLASS_ORIGIN);
    CASE(SQL_DIAG_COLUMN_NUMBER);
    CASE(SQL_DIAG_CONNECTION_NAME);
    CASE(SQL_DIAG_MESSAGE_TEXT);
    CASE(SQL_DIAG_NATIVE);
    CASE(SQL_DIAG_ROW_NUMBER);
    CASE(SQL_DIAG_SERVER_NAME);
    CASE(SQL_DIAG_SQLSTATE);
    CASE(SQL_DIAG_SUBCLASS_ORIGIN);
    // CASE(SQL_DIAG_ALTER_DOMAIN);
    // CASE(SQL_DIAG_ALTER_TABLE);
    // CASE(SQL_DIAG_CREATE_ASSERTION);
    // CASE(SQL_DIAG_CREATE_CHARACTER_SET);
    // CASE(SQL_DIAG_CREATE_COLLATION);
    // CASE(SQL_DIAG_CREATE_DOMAIN);
    // CASE(SQL_DIAG_CREATE_INDEX);
    // CASE(SQL_DIAG_CREATE_TABLE);
    // CASE(SQL_DIAG_CREATE_VIEW);
    // CASE(SQL_DIAG_SELECT_CURSOR);
    // CASE(SQL_DIAG_DYNAMIC_DELETE_CURSOR);
    // CASE(SQL_DIAG_DELETE_WHERE);
    // CASE(SQL_DIAG_DROP_ASSERTION);
    // CASE(SQL_DIAG_DROP_CHARACTER_SET);
    // CASE(SQL_DIAG_DROP_COLLATION);
    // CASE(SQL_DIAG_DROP_DOMAIN);
    // CASE(SQL_DIAG_DROP_INDEX);
    // CASE(SQL_DIAG_DROP_SCHEMA);
    // CASE(SQL_DIAG_DROP_TABLE);
    // CASE(SQL_DIAG_DROP_TRANSLATION);
    // CASE(SQL_DIAG_DROP_VIEW);
    // CASE(SQL_DIAG_GRANT);
    // CASE(SQL_DIAG_INSERT);
    // CASE(SQL_DIAG_CALL);
    // CASE(SQL_DIAG_REVOKE);
    // CASE(SQL_DIAG_CREATE_SCHEMA);
    // CASE(SQL_DIAG_CREATE_TRANSLATION);
    // CASE(SQL_DIAG_DYNAMIC_UPDATE_CURSOR);
    // CASE(SQL_DIAG_UPDATE_WHERE);
    // CASE(SQL_DIAG_UNKNOWN_STATEMENT);
    default:
      return "SQL_DIAG_IDENTIFIER_unknown";
  }
}

const char *sql_bulk_operation(SQLLEN v)
{
  switch (v) {
    CASE(SQL_ADD);
    CASE(SQL_UPDATE_BY_BOOKMARK);
    CASE(SQL_DELETE_BY_BOOKMARK);
    CASE(SQL_FETCH_BY_BOOKMARK);
    default:
      return "SQL_BULK_OPERATION_unknown";
  }
}

const char *sql_col_attribute(SQLULEN v)
{
  switch (v) {
    CASE(SQL_COLUMN_COUNT);
    CASE(SQL_COLUMN_NAME);
    // CASE(SQL_COLUMN_TYPE);                   /* SQL_DESC_CONCISE_TYPE */
    CASE(SQL_COLUMN_LENGTH);
    CASE(SQL_COLUMN_PRECISION);
    CASE(SQL_COLUMN_SCALE);
    // CASE(SQL_COLUMN_DISPLAY_SIZE);           /* SQL_DESC_DISPLAY_SIZE */
    CASE(SQL_COLUMN_NULLABLE);
    // CASE(SQL_COLUMN_UNSIGNED);               /* SQL_DESC_UNSIGNED */
    // CASE(SQL_COLUMN_MONEY);                  /* SQL_DESC_FIXED_PREC_SCALE */
    // CASE(SQL_COLUMN_UPDATABLE);              /* SQL_DESC_UPDATABLE */
    // CASE(SQL_COLUMN_AUTO_INCREMENT);         /* SQL_DESC_AUTO_UNIQUE_VALUE */
    // CASE(SQL_COLUMN_CASE_SENSITIVE);         /* SQL_DESC_CASE_SENSITIVE */
    // CASE(SQL_COLUMN_SEARCHABLE);             /* SQL_DESC_SEARCHABLE */
    // CASE(SQL_COLUMN_TYPE_NAME);              /* SQL_DESC_TYPE_NAME */
    // CASE(SQL_COLUMN_TABLE_NAME);             /* SQL_DESC_TABLE_NAME */
    // CASE(SQL_COLUMN_OWNER_NAME);             /* SQL_DESC_SCHEMA_NAME */
    // CASE(SQL_COLUMN_QUALIFIER_NAME);         /* SQL_DESC_CATALOG_NAME */
    // CASE(SQL_COLUMN_LABEL);                  /* SQL_DESC_LABEL */
    CASE(SQL_DESC_AUTO_UNIQUE_VALUE);           /* SQL_COLUMN_AUTO_INCREMENT */
    CASE(SQL_DESC_BASE_COLUMN_NAME);
    CASE(SQL_DESC_BASE_TABLE_NAME);
    CASE(SQL_DESC_CASE_SENSITIVE);              /* SQL_COLUMN_CASE_SENSITIVE */
    CASE(SQL_DESC_CATALOG_NAME);                /* SQL_COLUMN_QUALIFIER_NAME */
    CASE(SQL_DESC_CONCISE_TYPE);                /* SQL_COLUMN_TYPE */
    CASE(SQL_DESC_COUNT);
    CASE(SQL_DESC_DISPLAY_SIZE);                /* SQL_COLUMN_DISPLAY_SIZE */
    CASE(SQL_DESC_FIXED_PREC_SCALE);            /* SQL_COLUMN_MONEY */
    CASE(SQL_DESC_LABEL);                       /* SQL_COLUMN_LABEL */
    CASE(SQL_DESC_LENGTH);
    CASE(SQL_DESC_LITERAL_PREFIX);
    CASE(SQL_DESC_LITERAL_SUFFIX);
    CASE(SQL_DESC_LOCAL_TYPE_NAME);
    CASE(SQL_DESC_NAME);
    CASE(SQL_DESC_NULLABLE);
    CASE(SQL_DESC_NUM_PREC_RADIX);
    CASE(SQL_DESC_OCTET_LENGTH);
    CASE(SQL_DESC_PRECISION);
    CASE(SQL_DESC_SCALE);
    CASE(SQL_DESC_SCHEMA_NAME);                 /* SQL_COLUMN_OWNER_NAME */
    CASE(SQL_DESC_SEARCHABLE);                  /* SQL_COLUMN_SEARCHABLE */
    CASE(SQL_DESC_TABLE_NAME);                  /* SQL_COLUMN_TABLE_NAME */
    CASE(SQL_DESC_TYPE);
    CASE(SQL_DESC_TYPE_NAME);                   /* SQL_COLUMN_TYPE_NAME */
    CASE(SQL_DESC_UNNAMED);
    CASE(SQL_DESC_UNSIGNED);                    /* SQL_COLUMN_UNSIGNED */
    CASE(SQL_DESC_UPDATABLE);                   /* SQL_COLUMN_UPDATABLE */
    default:
      return "SQL_DESC_unknown";
  }
}

const char *sql_pos_operation(SQLULEN v)
{
  switch (v) {
    CASE(SQL_POSITION);
    CASE(SQL_REFRESH);
    CASE(SQL_UPDATE);
    CASE(SQL_DELETE);
    default:
      return "SQL_POSITION_unknown";
  }
}

const char *sql_pos_locktype(SQLULEN v)
{
  switch (v) {
    CASE(SQL_LOCK_NO_CHANGE);
    CASE(SQL_LOCK_EXCLUSIVE);
    CASE(SQL_LOCK_UNLOCK);
    default:
      return "SQL_LOCK_unknown";
  }
}

const char *sql_special_columns_identifier(SQLULEN v)
{
  switch (v) {
    CASE(SQL_BEST_ROWID);
    CASE(SQL_ROWVER);
    default:
      return "SQL_unknown";
  }
}

const char *sql_scope(SQLULEN v)
{
  switch (v) {
    CASE(SQL_SCOPE_CURROW);
    CASE(SQL_SCOPE_TRANSACTION);
    CASE(SQL_SCOPE_SESSION);
    default:
      return "SQL_SCOPE_unknown";
  }
}

const char *sql_index(SQLULEN v)
{
  switch (v) {
    CASE(SQL_INDEX_UNIQUE);
    CASE(SQL_INDEX_ALL);
    default:
      return "SQL_INDEX_unknown";
  }
}

const char *sql_statistics_reserved(SQLULEN v)
{
  switch (v) {
    CASE(SQL_ENSURE);
    CASE(SQL_QUICK);
    default:
      return "SQL_unknown";
  }
}

const char *sql_updatable(SQLLEN v)
{
  switch (v) {
    CASE(SQL_ATTR_READONLY);
    CASE(SQL_ATTR_WRITE);
    CASE(SQL_ATTR_READWRITE_UNKNOWN);
    default:
      return "SQL_ATTR_unknown";
  }
}

const char *sql_searchable(SQLLEN v)
{
  switch (v) {
    CASE(SQL_PRED_NONE);
    CASE(SQL_PRED_CHAR);
    CASE(SQL_PRED_BASIC);
    CASE(SQL_SEARCHABLE);
    default:
      return "SQL_PRED_unknown";
  }
}
