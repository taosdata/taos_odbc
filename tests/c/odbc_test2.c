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

#include "odbc_helpers.h"
#include "c_test_helper.h"

#include "enums.h"

#include <assert.h>
#ifndef _WIN32
#include <dlfcn.h>
#endif
#include <errno.h>
#include <sql.h>
#include <sqlext.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>

struct test_context {
  SQLHANDLE henv;
  SQLHANDLE hconn;
  SQLHANDLE hstmt;

  char buf[1024];
  simple_str_t sql_str;
};

typedef struct {
  struct test_context ctx;
  const char* test_name;
  const char* connstr;
  const char* dsn;
  const char* user;
  const char* pwd;
  bool  valid;
} data_source_case;

#define LINKMODENATIVE "native"
#define LINKMODEWS "ws"
#define LINKMODECLOUD "cloud"
#define LINKMODEMYSQL "mysql"
#define LINKMODESQLSERVER "ss"
data_source_case _native_link = {
    {
      .henv = SQL_NULL_HANDLE,
      .hconn = SQL_NULL_HANDLE,
      .hstmt = SQL_NULL_HANDLE,
    },
    LINKMODENATIVE,
    "DSN=TAOS_ODBC_DSN;Server=192.168.1.93",
    "TAOS_ODBC_DSN",
    NULL,
    NULL,
    false
};
data_source_case _websocket_link = {
    {
      .henv = SQL_NULL_HANDLE,
      .hconn = SQL_NULL_HANDLE,
      .hstmt = SQL_NULL_HANDLE,
    },
    LINKMODEWS,
    "DSN=TAOS_ODBC_WS_DSN;Server=192.168.1.93:6041",
    "TAOS_ODBC_WS_DSN",
    NULL,
    NULL,
    false
};
data_source_case _cloud_link = {
    {
      .henv = SQL_NULL_HANDLE,
      .hconn = SQL_NULL_HANDLE,
      .hstmt = SQL_NULL_HANDLE,
    },
    LINKMODECLOUD,
    "DSN=TAOS_CLOUD;",
    "TAOS_CLOUD",
    NULL,
    NULL,
    false
};
data_source_case _mysql_link = {
  {
    .henv = SQL_NULL_HANDLE,
    .hconn = SQL_NULL_HANDLE,
    .hstmt = SQL_NULL_HANDLE,
  },
  LINKMODEMYSQL,
  "DSN=MYSQL_ODBC",
  "MYSQL_ODBC",
  NULL,
  NULL,
  false
};
data_source_case _sqlserver_link = {
  {
    .henv = SQL_NULL_HANDLE,
    .hconn = SQL_NULL_HANDLE,
    .hstmt = SQL_NULL_HANDLE,
  },
    LINKMODESQLSERVER,
    "DSN=SQLSERVER_ODBC_DSN",
    "SQLSERVER_ODBC_DSN",
    "sa",
    "Aa123456",
    false
};
data_source_case* link_info;

#define MAX_TABLE_NUMBER 100
#define MAX_TABLE_NAME_LEN 128
#define test_db "meter"

#define CHKENVR(henv, r)  do {                                                                                                   \
   if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)                                                                           \
   {                                                                                                                             \
     SQLCHAR sqlState[6];                                                                                                        \
     SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];                                                                                    \
     SQLSMALLINT messageLength;                                                                                                  \
     SQLINTEGER nativeError;                                                                                                     \
     if(SQLGetDiagRec(SQL_HANDLE_ENV, henv, 1, sqlState, &nativeError, message, sizeof(message), &messageLength)  != 100)        \
     {                                                                                                                           \
       E("SQLGetEnvAttr failed with error: %s", message);                                                                        \
     }                                                                                                                           \
     return -1;                                                                                                                  \
   }                                                                                                                             \
} while (0)                                                                                                                      \

#define CHKDBR(hconn, r)  do {                                                                                                   \
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)                                                                            \
  {                                                                                                                              \
    SQLCHAR sqlState[6];                                                                                                         \
    SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];                                                                                     \
    SQLSMALLINT messageLength;                                                                                                   \
    SQLINTEGER nativeError;                                                                                                      \
    if(SQLGetDiagRec(SQL_HANDLE_DBC, hconn, 1, sqlState, &nativeError, message, sizeof(message), &messageLength)  != 100)        \
    {                                                                                                                            \
      E("error: %s", message);                                                                                                   \
    }                                                                                                                            \
    return -1;                                                                                                                   \
  }                                                                                                                              \
} while (0)

#define CHKSTMTR(hstmt, r)  do {                                                                                                 \
   if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)                                                                           \
   {                                                                                                                             \
     SQLCHAR sqlState[6];                                                                                                        \
     SQLCHAR message[SQL_MAX_MESSAGE_LENGTH]={0};                                                                                \
     SQLSMALLINT messageLength;                                                                                                  \
     SQLINTEGER nativeError;                                                                                                     \
     if(SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, sqlState, &nativeError, message, sizeof(message), &messageLength)  != 100)      \
     {                                                                                                                           \
       E("error: %s", message);                                                                                                  \
     }                                                                                                                           \
     return -1;                                                                                                                  \
   }                                                                                                                             \
} while (0)  


#define ASSERT_EQUAL(actual, expected)                                                               \
    do {                                                                                             \
        if ((expected) != (actual)) {                                                                \
            printf("Assertion failed: Expected %d, but got %d\n", (expected), (actual));             \
            return -1;                                                                               \
        }                                                                                            \
    } while(0)

#define ASSERT_EQUALS(actual, expected)                                                              \
    do {                                                                                             \
        if (strcmp(actual, expected) != 0) {                                                         \
            printf("Assertion failed: Expected %s, but got %s\n", (expected), (actual));             \
            return -1;                                                                               \
        }                                                                                            \
    } while(0)

static void get_diag_rec(SQLSMALLINT handleType, SQLHANDLE h) {

  SQLRETURN r;
  SQLCHAR sqlState[6];
  SQLINTEGER nativeError;
  SQLCHAR messageText[SQL_MAX_MESSAGE_LENGTH];
  SQLSMALLINT textLength;

  r = SQLGetDiagRec(handleType, h, 1, sqlState, &nativeError, messageText, sizeof(messageText), &textLength);
  X("SQLGetDiagRec result: %d", r);
  X("SQLState: %s", sqlState);
  X("Native Error: %d", nativeError);
  X("Message Text: %s", messageText);
}

static int init_henv(void) {
  SQLRETURN r;
  r = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &link_info->ctx.henv);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  r = SQLSetEnvAttr(link_info->ctx.henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  r = SQLSetEnvAttr(link_info->ctx.henv, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_DRIVER, 0);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  return 0;
}

static int create_sql_connect(void) {
  int r = 0;

  r = CALL_SQLAllocHandle(SQL_HANDLE_DBC, link_info->ctx.henv, &link_info->ctx.hconn);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  X("connect dsn:%s user:%s pwd:%s", link_info->dsn, link_info->user, link_info->pwd);
  r = CALL_SQLConnect(link_info->ctx.hconn, (SQLCHAR*)link_info->dsn, SQL_NTS, (SQLCHAR*)link_info->user, SQL_NTS, (SQLCHAR*)link_info->pwd, SQL_NTS);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  X("connect dsn:%s result:%d", link_info->dsn, r);

  return 0;
}

static int free_connect(void) {
  int r = 0;
  if (link_info->ctx.hstmt != SQL_NULL_HANDLE) {
    r = CALL_SQLFreeHandle(SQL_HANDLE_STMT, link_info->ctx.hstmt);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    link_info->ctx.hstmt = SQL_NULL_HANDLE;
  }

  if (link_info->ctx.hconn != SQL_NULL_HANDLE) {
    X("disconnect dsn:%s", link_info->dsn);
    r = CALL_SQLDisconnect(link_info->ctx.hconn);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
    X("disconnect dsn:%s result:%d", link_info->dsn, r);

    r = CALL_SQLFreeHandle(SQL_HANDLE_DBC, link_info->ctx.hconn);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    link_info->ctx.hconn = SQL_NULL_HANDLE;
  }
  return 0;
}

static int create_driver_conn(void)
{
  int r = 0;
  r = CALL_SQLAllocHandle(SQL_HANDLE_DBC, link_info->ctx.henv, &link_info->ctx.hconn);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  SQLHWND WindowHandle = NULL;
  SQLCHAR* InConnectionString = (SQLCHAR*)link_info->connstr;
  SQLSMALLINT StringLength1 = (SQLSMALLINT)strlen(link_info->connstr);
  SQLCHAR OutConnectionString[1024];
  SQLSMALLINT BufferLength = sizeof(OutConnectionString);
  SQLSMALLINT StringLength2 = 0;
  SQLUSMALLINT DriverCompletion = SQL_DRIVER_NOPROMPT;

  OutConnectionString[0] = '\0';

  r = CALL_SQLDriverConnect(link_info->ctx.hconn, WindowHandle, InConnectionString, StringLength1, OutConnectionString, BufferLength, &StringLength2, DriverCompletion);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  return 0;
}

static int reset_stmt(void) {
  int r = 0;
  if (link_info->ctx.hstmt != SQL_NULL_HANDLE) {
    r = CALL_SQLFreeHandle(SQL_HANDLE_STMT, link_info->ctx.hstmt);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  }
  link_info->ctx.hstmt = SQL_NULL_HANDLE;
  r = CALL_SQLAllocHandle(SQL_HANDLE_STMT, link_info->ctx.hconn, &link_info->ctx.hstmt);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  return 0;
}

static int exec_sql(char* sql) {
  int r = 0;
  CHK0(reset_stmt, 0);
  SQLHANDLE hstmt = link_info->ctx.hstmt;
  r = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  D("CALL_SQLExecDirect: %s result:%d", sql, r);
  if (r == SQL_SUCCESS_WITH_INFO) {
    get_diag_rec(SQL_HANDLE_STMT, hstmt);
  }
  CHKSTMTR(hstmt, r);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  return 0;
}

static int drop_database(char* db) {
  char sql[128];
  memset(sql, 0, sizeof(sql));
  strcat(sql, "drop database if exists ");
  strcat(sql, db);
  strcat(sql, ";");
  CHK1(exec_sql, sql, 0);
  X("drop database %s finished", db);
  return 0;
}

static int create_database(char* db) {
  char sql[128];
  memset(sql, 0, sizeof(sql));
  strcat(sql, "create database if not exists ");
  strcat(sql, db);
  CHK1(exec_sql, sql, 0);
  X("create database %s finished", db);
   return 0;
}

static int get_tables(char table_names[MAX_TABLE_NUMBER][MAX_TABLE_NAME_LEN]) {
  int r = 0;
  CHK0(reset_stmt, 0);

  int table_count = 0;

  SQLHANDLE hstmt = link_info->ctx.hstmt;
  SQLCHAR* tableType = (SQLCHAR*)"TABLE";
  r = SQLTables(hstmt, NULL, 0, NULL, 0, NULL, 0, tableType, SQL_NTS);
  X("SQLTables result:%d", r);
  while (CALL_SQLFetch(hstmt) == SQL_SUCCESS) {
    CALL_SQLGetData(hstmt, 3, SQL_C_CHAR, table_names[table_count], MAX_TABLE_NAME_LEN, NULL);
    table_count++;
  }
  X("table count:%d", table_count);
  return table_count;
}

static int show_tables(void) {
  int r = 0;
  CHK0(reset_stmt, 0);

  SQLCHAR table_names[MAX_TABLE_NUMBER][MAX_TABLE_NAME_LEN];
  int table_count = get_tables(table_names);

  for (int j = 0; j < table_count; j++) {
    X("table name: %s", table_names[j]);
  }
  return 0;
}

static int init(void) {
  CHK0(init_henv, 0);
  CHK0(create_sql_connect, 0);
  CHK1(create_database, test_db, 0);
  CHK0(free_connect, 0);

  X("init finished!");
  return 0;
}

static int db_test(void) {
  CHK0(create_driver_conn, 0);
  CHK0(show_tables, 0);

  CHK0(free_connect, 0);
  X("basic test finished!");
  return 0;
}

static int use_db(char* db) {
  char sql[1024];
  strcpy(sql, "use ");
  strcat(sql, db);
  CHK1(exec_sql, sql, 0);
  return 0;
}

static int show_columns1(char* table_name) {
  #define STR_LEN 128 + 1
  #define REM_LEN 254 + 1
  // Declare buffers for result set data
  SQLCHAR strSchema[STR_LEN];
  SQLCHAR strCatalog[STR_LEN];
  SQLCHAR strColumnName[STR_LEN];
  SQLCHAR strTableName[STR_LEN];
  SQLCHAR strTypeName[STR_LEN];
  SQLCHAR strRemarks[REM_LEN];
  SQLCHAR strColumnDefault[STR_LEN];
  SQLCHAR strIsNullable[STR_LEN];

  SQLINTEGER ColumnSize;
  SQLINTEGER BufferLength;
  SQLINTEGER CharOctetLength;
  SQLINTEGER OrdinalPosition;

  SQLSMALLINT DataType;
  SQLSMALLINT DecimalDigits;
  SQLSMALLINT NumPrecRadix;
  SQLSMALLINT Nullable;
  SQLSMALLINT SQLDataType;
  SQLSMALLINT DatetimeSubtypeCode;

  SQLLEN lenCatalog;
  SQLLEN lenSchema;
  SQLLEN lenTableName;
  SQLLEN lenColumnName;
  SQLLEN lenDataType;
  SQLLEN lenTypeName;
  SQLLEN lenColumnSize;
  SQLLEN lenBufferLength;
  SQLLEN lenDecimalDigits;
  SQLLEN lenNumPrecRadix;
  SQLLEN lenNullable;
  SQLLEN lenRemarks;
  SQLLEN lenColumnDefault;
  SQLLEN lenSQLDataType;
  SQLLEN lenDatetimeSubtypeCode;
  SQLLEN lenCharOctetLength;
  SQLLEN lenOrdinalPosition;
  SQLLEN lenIsNullable;

  SQLRETURN retcode;
  CHK0(reset_stmt, 0);
  SQLHANDLE hstmt = link_info->ctx.hstmt;

  retcode = CALL_SQLColumns(hstmt, NULL, 0, NULL, 0,
    (SQLCHAR*)table_name, SQL_NTS, NULL, 0);

  if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
    SQLBindCol(hstmt, 1, SQL_C_CHAR, strCatalog,
      STR_LEN, &lenCatalog);
    SQLBindCol(hstmt, 2, SQL_C_CHAR, strSchema,
      STR_LEN, &lenSchema);
    SQLBindCol(hstmt, 3, SQL_C_CHAR, strTableName,
      STR_LEN, &lenTableName);
    SQLBindCol(hstmt, 4, SQL_C_CHAR, strColumnName,
      STR_LEN, &lenColumnName);
    SQLBindCol(hstmt, 5, SQL_C_SSHORT, &DataType,
      0, &lenDataType);
    SQLBindCol(hstmt, 6, SQL_C_CHAR, strTypeName,
      STR_LEN, &lenTypeName);
    SQLBindCol(hstmt, 7, SQL_C_SLONG, &ColumnSize,
      0, &lenColumnSize);
    SQLBindCol(hstmt, 8, SQL_C_SLONG, &BufferLength,
      0, &lenBufferLength);
    SQLBindCol(hstmt, 9, SQL_C_SSHORT, &DecimalDigits,
      0, &lenDecimalDigits);
    SQLBindCol(hstmt, 10, SQL_C_SSHORT, &NumPrecRadix,
      0, &lenNumPrecRadix);
    SQLBindCol(hstmt, 11, SQL_C_SSHORT, &Nullable,
      0, &lenNullable);
    SQLBindCol(hstmt, 12, SQL_C_CHAR, strRemarks,
      REM_LEN, &lenRemarks);
    SQLBindCol(hstmt, 13, SQL_C_CHAR, strColumnDefault,
      STR_LEN, &lenColumnDefault);
    SQLBindCol(hstmt, 14, SQL_C_SSHORT, &SQLDataType,
      0, &lenSQLDataType);
    SQLBindCol(hstmt, 15, SQL_C_SSHORT, &DatetimeSubtypeCode,
      0, &lenDatetimeSubtypeCode);
    SQLBindCol(hstmt, 16, SQL_C_SLONG, &CharOctetLength,
      0, &lenCharOctetLength);
    SQLBindCol(hstmt, 17, SQL_C_SLONG, &OrdinalPosition,
      0, &lenOrdinalPosition);
    SQLBindCol(hstmt, 18, SQL_C_CHAR, strIsNullable,
      STR_LEN, &lenIsNullable);
    
    // retrieve column data
    while (SQL_SUCCESS == retcode) {
      retcode = CALL_SQLFetch(hstmt);
      CHKSTMTR(hstmt, retcode);
      X("strCatalog  : %s", strCatalog);
      X("strSchema   : %s", strSchema);
      X("strTableName: %s", strTableName);
      X("Column Name : %s", strColumnName);
      X("Column Size : %i", ColumnSize);
      X("Data Type   : %i", SQLDataType);
      X("strTypeName : %s", strTypeName);
      X("Nullable    : %d", Nullable);
    }
  }
  return 0;
}

static int show_table_data(char* table_name) {
  reset_stmt();
  SQLHANDLE hstmt = link_info->ctx.hstmt;
  char sql[1024];
  SQLSMALLINT numberOfColumns;
  sprintf(sql, "SELECT * FROM %s", table_name);
  CALL_SQLExecDirect(hstmt, sql, SQL_NTS);
  CALL_SQLNumResultCols(hstmt, &numberOfColumns);

  int row = 0;
  SQLCHAR columnData[256];
  SQLLEN indicator;
  while (CALL_SQLFetch(hstmt) == SQL_SUCCESS) {
    row++;
    for (int i = 1; i <= numberOfColumns; i++) {
      CALL_SQLGetData(hstmt, i, SQL_C_CHAR, columnData, sizeof(columnData), &indicator);
      if (indicator == SQL_NULL_DATA) {
        D("Row:%d Column %d: NULL", row, i);
      }
      else {
        D("Row:%d Column %d: %s", row, i, columnData);
      }
    }
  }
  return row;
}

static int get_records_counts(char* table_name) {
  reset_stmt();
  SQLHANDLE hstmt = link_info->ctx.hstmt;
  char sql[1024];
  SQLLEN numberOfrows;
  sprintf(sql, "SELECT * FROM %s", table_name);
  CALL_SQLExecDirect(hstmt, sql, SQL_NTS);
  CALL_SQLRowCount(hstmt, &numberOfrows);
  X("%s record count: %lld", table_name, numberOfrows);

  return (int)numberOfrows;
}

static int show_columns2(char* table_name) {
  CHK0(reset_stmt, 0);
  SQLHANDLE hstmt = link_info->ctx.hstmt;
  SQLCHAR* tableName = (SQLCHAR*)"metertemplate";
  CALL_SQLColumns(hstmt, NULL, 0, NULL, 0, tableName, SQL_NTS, NULL, 0);

  SQLCHAR columnName[256];
  SQLLEN columnNameLen;
  SQLCHAR dataType[256];
  SQLLEN dataTypeLen;
  SQLCHAR typeName[256];
  SQLLEN typeNameLen;
  SQLINTEGER columnSize;
  SQLSMALLINT decimalDigits;
  SQLSMALLINT nullable;
  while (CALL_SQLFetch(hstmt) == SQL_SUCCESS) {
    SQLGetData(hstmt, 4, SQL_C_CHAR, columnName, 256, &columnNameLen);
    SQLGetData(hstmt, 5, SQL_C_CHAR, dataType, 256, &dataTypeLen);
    SQLGetData(hstmt, 6, SQL_C_CHAR, typeName, 256, &typeNameLen);
    SQLGetData(hstmt, 7, SQL_C_LONG, &columnSize, 0, NULL);
    SQLGetData(hstmt, 9, SQL_C_SHORT, &decimalDigits, 0, NULL);
    SQLGetData(hstmt, 11, SQL_C_SHORT, &nullable, 0, NULL);

    X("Column Name: %s", columnName);
    X("Data Type: %s", dataType);
    X("Type Name: %s", typeName);
    X("Column Size: %d", columnSize);
    X("Decimal Digits: %d", decimalDigits);
    X("Nullable: %d", nullable);
  }
  return 0;
}

static int case_1(void) {
  char* tmp_db = "tmp_db";
  CHK0(create_sql_connect, 0);
  CHK1(drop_database, tmp_db, 0);
  CHK1(create_database, tmp_db, 0);
  CHK1(use_db, tmp_db, 0);

  char sql[1024];

  SQLCHAR table_names[MAX_TABLE_NUMBER][MAX_TABLE_NAME_LEN];
  sql[0] = '\0';
  strcpy(sql, "DROP STABLE IF EXISTS `metertemplate`");
  CHK1(exec_sql, sql, 0);

  sql[0] = '\0';
  strcpy(sql, "CREATE STABLE `metertemplate` (`ts` TIMESTAMP, `current_val` DOUBLE, `current_status` INT) \
    TAGS(`element_id` NCHAR(100), `location` NCHAR(100))");
  CHK1(exec_sql, sql, 0);
  int table_counts = get_tables(table_names);
  ASSERT_EQUAL(table_counts, 1);
  ASSERT_EQUALS(table_names[0], "metertemplate");

  sql[0] = '\0';
  strcpy(sql, "CREATE TABLE tb1 using metertemplate TAGS ('00001', 'location_001')");
  CHK1(exec_sql, sql, 0);
  sql[0] = '\0';
  strcpy(sql, "CREATE TABLE tb2 using metertemplate TAGS ('00002', 'location_002')");
  CHK1(exec_sql, sql, 0);
  sql[0] = '\0';
  strcpy(sql, "CREATE TABLE tb3 using metertemplate TAGS ('00003', 'location_003')");
  CHK1(exec_sql, sql, 0);
  table_counts = get_tables(table_names);
  ASSERT_EQUAL(table_counts, 4);

  show_columns1("metertemplate");
  show_columns2("tb1");

  const int insert_count = 10;
  for (int i = 0; i < insert_count; i++) {
    sql[0] = '\0';
    sprintf(sql, "insert into tb1 (ts, current_val, current_status) values (now(), %d, 0)", 100+i);
    CHK1(exec_sql, sql, 0);
  }
  for (int i = 0; i < insert_count; i++) {
    sql[0] = '\0';
    sprintf(sql, "insert into tb2 (ts, current_val, current_status) values (now(), %d, 0)", 100 + i);
    CHK1(exec_sql, sql, 0);
  }
  show_table_data("tb1");
  show_table_data("metertemplate");
  // int tab1_counts = get_records_counts("tb1");
  // ASSERT_EQUAL(tab1_counts, insert_count);
  // int st_counts = get_records_counts("metertemplate");
  // ASSERT_EQUAL(st_counts, 2* insert_count);

  CHK0(free_connect, 0);
  return 0;
}

static int case_2(void) {
  char* tmp_db = "tmp_db2";
  CHK0(create_sql_connect, 0);
  CHK1(drop_database, tmp_db, 0);
  CHK1(create_database, tmp_db, 0);
  CHK1(use_db, tmp_db, 0);

  char sql[1024];

  SQLCHAR table_names[MAX_TABLE_NUMBER][MAX_TABLE_NAME_LEN];
  sql[0] = '\0';
  strcpy(sql, "DROP TABLE IF EXISTS tbx");
  CHK1(exec_sql, sql, 0);

  sql[0] = '\0';
  strcpy(sql, "CREATE TABLE `tbx` (`ts` TIMESTAMP, `current_val` DOUBLE, `current_status` INT)");
  CHK1(exec_sql, sql, 0);
  int table_counts = get_tables(table_names);
  ASSERT_EQUAL(table_counts, 1);

  const int insert_count = 10;
  for (int i = 0; i < insert_count; i++) {
    sql[0] = '\0';
    sprintf(sql, "insert into tbx (ts, current_val, current_status) values (now(), %d, 0)", 100 + i);
    CHK1(exec_sql, sql, 0);
  }

  show_table_data("tbx");
  // int tab1_counts = get_records_counts("tbx");
  // ASSERT_EQUAL(tab1_counts, insert_count);

  return 0;
}

static int case_3(void) {
  CHK0(create_sql_connect, 0);
  CHK1(use_db, test_db, 0);

  #define insert_times 100
  #define insert_count 1000

  char sql[100 + insert_count * 64];
  sql[0] = '\0';

  sql[0] = '\0';
  strcpy(sql, "CREATE TABLE if not exists `t_table` (`ts` TIMESTAMP, `current_val` DOUBLE, `current_status` INT)");
  CHK1(exec_sql, sql, 0);

  clock_t t1 = clock();


  time_t current_time = time(NULL);

  for (int num = 0; num < insert_times; num++) {
    sql[0] = '\0';
    sprintf(sql, "insert into t_table (ts, current_val, current_status) values ");
    char val[64];

    for (int i = 0; i < insert_count; i++) {
      val[0] = '\0';
      sprintf(val, " (%lld, %d, 0)", current_time * 1000 + num * insert_count + i, 100 + i);
      strcat(sql, val);
    }
    CHK1(exec_sql, sql, 0);
  }


  clock_t t2 = clock();
  double elapsed_time = (double)(t2 - t1) / CLOCKS_PER_SEC;
  X("Write %d * %d rows data, cost time : % f seconds", insert_times, insert_count, elapsed_time);

  int counts = show_table_data("t_table");

  clock_t t3 = clock();
  elapsed_time = (double)(t3 - t2) / CLOCKS_PER_SEC;
  X("Read %d rows data, cost time: %f seconds", counts, elapsed_time);

  return 0;
}

static const int default_supported = 1;
static const int default_unsupported = 2;
static bool isTestCase(int argc, char* argv[], const char* test_case, const int support) {
  if (argc <= 1 && support == default_supported) return true;
  if (argc <= 1 && support == default_unsupported) return false;
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], test_case) == 0) return true;
  }
  return false;
}

static int run(int argc, char* argv[]) {
  init();

  if (isTestCase(argc, argv, "db_test", default_supported)) CHK0(db_test, 0);
  if (isTestCase(argc, argv, "basic", default_supported)) CHK0(case_1, 0);
  if (isTestCase(argc, argv, "case_2", default_supported)) CHK0(case_2, 0);
  if (isTestCase(argc, argv, "case_3", default_supported)) CHK0(case_3, 0);

  X("The test finished successfully.");
  return 0;
}

static int cloud_test(int argc, char* argv[]) {
  CHK0(init_henv, 0);
  CHK0(create_driver_conn, 0);
  CHK0(show_tables, 0);
  if (isTestCase(argc, argv, "case_3", default_supported)) CHK0(case_3, 0);

  X("Cloud_test finished successfully.");
  return 0;
}

static int mysql_help_test(int argc, char* argv[]) {
  CHK0(init, 0);
  CHK0(case_2, 0);

  X("The test finished successfully.");
  return 0;
}

static int server_help_test(int argc, char* argv[]) {

  X("SQLServer help test skip.");
  return 0;
}

int main(int argc, char* argv[])
{
  int r = 0;
  do {
    if (argc >= 2 && strcmp(argv[1], LINKMODENATIVE) == 0) {
      link_info = &_native_link;
      r = run(argc - 1, argv + 1);
    }
    else if (argc >= 2 && strcmp(argv[1], LINKMODEWS) == 0) {
      link_info = &_websocket_link;
      r = run(argc - 1, argv + 1);
    }
    else if (argc >= 2 && strcmp(argv[1], LINKMODECLOUD) == 0) {
      link_info = &_cloud_link;
      r = cloud_test(argc - 1, argv + 1);
    }
    else if (argc >= 2 && strcmp(argv[1], LINKMODEMYSQL) == 0) {
      link_info = &_mysql_link;
      r = mysql_help_test(argc - 1, argv + 1);
    }
    else if (argc >= 2 && strcmp(argv[1], LINKMODEMYSQL) == 0) {
      link_info = &_sqlserver_link;
      r = server_help_test(argc - 1, argv + 1);
    }
    else {
      link_info = &_native_link;
      r = run(argc, argv);
      if (r != 0) break;
      link_info = &_websocket_link;
      r = run(argc, argv);
    }
  } while (0);

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");
  return !!r;
}
