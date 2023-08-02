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
    "DSN=TAOS_ODBC_DSN;Server=192.168.1.93",
    "TAOS_ODBC_DSN",
    NULL,
    NULL,
    false
};
data_source_case* link_info;

#define MAX_TABLE_NAME_LEN 128
#define test_db "meter"
static const char* tb_test1 = "tx1";
static const char* tb_test2 = "tx2";

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

static int sql_connect(void) {
  int r = 0;

  r = CALL_SQLAllocHandle(SQL_HANDLE_DBC, link_info->ctx.henv, &link_info->ctx.hconn);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  X("connect dsn:%s user:%s pwd:%s", link_info->dsn, link_info->user, link_info->pwd);
  r = CALL_SQLConnect(link_info->ctx.hconn, (SQLCHAR*)link_info->dsn, SQL_NTS, (SQLCHAR*)link_info->user, SQL_NTS, (SQLCHAR*)link_info->pwd, SQL_NTS);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  X("connect dsn:%s result:%d", link_info->dsn, r);

  return 0;
}

static int test_sql_driver_conn(SQLHANDLE connh, const char* conn_str)
{
  SQLRETURN r;
  SQLHWND WindowHandle = NULL;
  SQLCHAR* InConnectionString = (SQLCHAR*)conn_str;
  SQLSMALLINT StringLength1 = (SQLSMALLINT)strlen(conn_str);
  SQLCHAR OutConnectionString[1024];
  SQLSMALLINT BufferLength = sizeof(OutConnectionString);
  SQLSMALLINT StringLength2 = 0;
  SQLUSMALLINT DriverCompletion = SQL_DRIVER_NOPROMPT;

  OutConnectionString[0] = '\0';

  r = CALL_SQLDriverConnect(connh, WindowHandle, InConnectionString, StringLength1, OutConnectionString, BufferLength, &StringLength2, DriverCompletion);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  return 0;
}

static int reset_stmt(void) {
  int r = 0;
  if (link_info->ctx.hstmt != SQL_NULL_HANDLE) {
    r = CALL_SQLFreeHandle(SQL_HANDLE_STMT, link_info->ctx.hstmt);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  }

  r = CALL_SQLAllocHandle(SQL_HANDLE_STMT, link_info->ctx.hconn, &link_info->ctx.hstmt);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  return 0;
}

static int exec_sql(char* sql) {
  int r = 0;
  CHK0(reset_stmt, 0);
  SQLHANDLE hstmt = link_info->ctx.hstmt;

  r = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  X(" %s CALL_SQLExecDirect result:%d", sql, r);
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

static int show_tables(void) {
  int r = 0;
  CHK0(reset_stmt, 0);

  SQLCHAR table_names[100][MAX_TABLE_NAME_LEN];
  int table_count = 0;

  SQLHANDLE hstmt = link_info->ctx.hstmt;
  SQLCHAR* tableType = (SQLCHAR*)"TABLE";
  r = SQLTables(hstmt, NULL, 0, NULL, 0, NULL, 0, tableType, SQL_NTS);
  X("SQLTables result:%d", r);
  while (CALL_SQLFetch(hstmt) == SQL_SUCCESS) {
    CALL_SQLGetData(hstmt, 3, SQL_C_CHAR, table_names[table_count], MAX_TABLE_NAME_LEN, NULL);
    table_count++;
  }

  for (int j = 0; j < table_count; j++) {
    X("table name: %s", table_names[j]);
  }
  return 0;
}

static int basic(void) {
  CHK0(init_henv, 0);
  CHK0(sql_connect, 0);
  CHK1(drop_database, test_db, 0);
  CHK1(create_database, test_db, 0);

  char sql[1024];
  strcpy(sql, "use ");
  strcat(sql, test_db);
  CHK1(exec_sql, sql, 0);

  sql[0] = '\0';
  strcpy(sql, "CREATE STABLE `metertemplate` (`ts` TIMESTAMP, `current_val` DOUBLE, `current_status` INT) \
    TAGS(`element_id` NCHAR(100), `location` NCHAR(100))");
  CHK1(exec_sql, sql, 0);
  CHK0(show_tables, 0);

  X("basic test finished!");
  return 0;
}

static int basic1(void) {
  X("basic1 test unsupported!");
  return -1;
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
  if (isTestCase(argc, argv, "basic", default_supported)) CHK0(basic, 0);


  {
    // unsupported cases;
    if (isTestCase(argc, argv, "basic1", default_unsupported)) CHK0(basic1, 0);
  }

  X("The test finished successfully.");
  return 0;
}

int main(int argc, char* argv[])
{
  int r = 0;
  do {
    if (argc >= 2 && strcmp(argv[1], LINKMODENATIVE) == 0) {
      link_info = &_native_link;
      r = run(argc, argv);
    }
    else if (argc >= 2 && strcmp(argv[1], LINKMODEWS) == 0) {
      link_info = &_websocket_link;
      // r = run(argc, argv);
    }
    else {
      link_info = &_native_link;
      r = run(argc, argv);
      if (r != 0) break;
      link_info = &_websocket_link;
      // r = run(argc, argv);
    }
  } while (0);

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");
  return !!r;
}
