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

#define DB_SOURCE_ALL_NUM  3
static int db_source_ready_num = 0;

typedef struct {
  struct test_context ctx;
  const char* test_name;
  const char* connstr;
  const char* dsn;
  const char* user;
  const char* pwd;
  bool  valid;
} data_source_case;

#define TN_SQLSERVER "sqlserver-odbc"
#define TN_MYSQL "mysql-odbc"
#define TN_TAOS "taos-odbc"
data_source_case _cases_all[] = {
  {
    {
      .henv = SQL_NULL_HANDLE,
      .hconn = SQL_NULL_HANDLE,
      .hstmt = SQL_NULL_HANDLE,
    },
    TN_SQLSERVER,
    "DSN=SQLSERVER_ODBC_DSN",
    "SQLSERVER_ODBC_DSN",
    "",
    "",
    false
  },
  {
    {
      .henv = SQL_NULL_HANDLE,
      .hconn = SQL_NULL_HANDLE,
      .hstmt = SQL_NULL_HANDLE,
    },
    TN_MYSQL,
    "DSN=MYSQL-ODBC",
    "MYSQL_ODBC",
    NULL,
    NULL,
    false
  },
  {
    {
      .henv = SQL_NULL_HANDLE,
      .hconn = SQL_NULL_HANDLE,
      .hstmt = SQL_NULL_HANDLE,
    },
    TN_TAOS,
    "DSN=TAOS_ODBC_DSN;SERVER=127.0.0.1:6030",
    "TAOS_ODBC_DSN",
    NULL,
    NULL,
    false
  },
};

data_source_case* _cases;

#define MAX_TABLE_NAME_LEN 128
#define test_db "power"
#define test_db2 "power2"
#define ARRAY_SIZE 10
#define ARRAY_BIGGER_SIZE 2500

static const char* tb_test1 = "tx1";
static const char* tb_test2 = "tx2";

field_t fields[] = {
  {"ts", "timestamp"},
  {"vname", "varchar(10)"},
  {"wname", "nchar(10)"},
  {"bi", "bigint"},
};

#define XX(_fmt, ...) do {                           \
  X("COMPARETEST %s " _fmt "",                       \
    _cases[i].test_name, ##__VA_ARGS__);             \
} while (0)

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

static void set_test_case_valid(SQLCHAR* test_name) {
  for (size_t i = 0; i < sizeof(_cases_all) / sizeof(_cases_all[0]); ++i) {
    if (strcmp(_cases_all[i].dsn, (const char*)test_name) == 0) {
      _cases_all[i].valid = true;
    }
  }
}

static int get_data_source_num() {
  int num = 0;
  for (size_t i = 0; i < sizeof(_cases_all) / sizeof(_cases_all[0]); ++i) {
    if (_cases_all[i].valid) {
      num++;
    }
  }
  return num;
}

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
  for (int i = 0; i < db_source_ready_num; ++i) {
    r = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &_cases[i].ctx.henv);

    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
    r = SQLSetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    SQLINTEGER odbcVersion;
    r = SQLGetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_ODBC_VERSION, &odbcVersion, 0, NULL);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
    XX("ODBC version: %d\n", (int)odbcVersion);
    if (odbcVersion != SQL_OV_ODBC3) return -1;
  }
  for (int i = 0; i < db_source_ready_num; ++i) {
    r = SQLSetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)5UL, 0);
    XX("SQLSetEnvAttr SQL_ATTR_CONNECTION_POOLING set and invalid value, result:%d", r);
    if (SUCCEEDED(r)) return -1;
    r = 0;
  }
  for (int i = 0; i < db_source_ready_num; ++i) {
    r = SQLSetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_HENV, 0);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    SQLINTEGER connpoll;
    r = SQLGetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_CONNECTION_POOLING, &connpoll, 0, NULL);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
    XX("connection polling: %d\n", (int)connpoll);
    if (connpoll != SQL_CP_ONE_PER_HENV) return -1;
  }
  for (int i = 0; i < db_source_ready_num; ++i) {
    r = SQLSetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_DRIVER, 0);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    SQLINTEGER connpoll;
    r = SQLGetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_CONNECTION_POOLING, &connpoll, 0, NULL);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
    XX("connection polling: %d\n", (int)connpoll);
    if (connpoll != SQL_CP_ONE_PER_DRIVER) return -1;
  }
  for (int i = 0; i < db_source_ready_num; ++i) {
    r = SQLSetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_OUTPUT_NTS, (SQLPOINTER)SQL_FALSE, 0);
    XX("SQLSetEnvAttr SQL_ATTR_OUTPUT_NTS set SQL_FALSE, result:%d", r);
    r = 0;
  }
  for (int i = 0; i < db_source_ready_num; ++i) {
    r = SQLSetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_OUTPUT_NTS, (SQLPOINTER)SQL_TRUE, 0);
    XX("SQLSetEnvAttr SQL_ATTR_OUTPUT_NTS set SQL_TRUE, result:%d", r);
  }

  return 0;
}

static int connect_test(void) {
  int r = 0;
  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHANDLE hconntmp;

    r = CALL_SQLAllocHandle(SQL_HANDLE_DBC, _cases[i].ctx.henv, &hconntmp);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    XX("create tmp connect dsn:%s user:%s pwd:%s", _cases[i].dsn, _cases[i].user, _cases[i].pwd);
    r = CALL_SQLConnect(hconntmp, (SQLCHAR*)_cases[i].dsn, SQL_NTS, (SQLCHAR*)_cases[i].user, SQL_NTS, (SQLCHAR*)_cases[i].pwd, SQL_NTS);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
    XX("create tmp connect dsn:%s result:%d", _cases[i].dsn, r);

    r = CALL_SQLDisconnect(hconntmp);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    r = CALL_SQLDisconnect(hconntmp);
    if (r != SQL_ERROR) return -1;

    r = CALL_SQLFreeHandle(SQL_HANDLE_DBC, hconntmp);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    r = CALL_SQLFreeHandle(SQL_HANDLE_DBC, hconntmp);
    if (r != SQL_INVALID_HANDLE) return -1;
  }
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

static int set_null_stmt(void) {
  for (int i = 0; i < db_source_ready_num; ++i) {
    _cases[i].ctx.hstmt = NULL;
  }
  return 0;
}

static int connect_select_db(char* db) {
  int r = 0;
  for (int i = 0; i < db_source_ready_num; ++i) {
    char dsn[100] = { 0 };
    strcat(dsn, "DSN=");
    strcat(dsn, _cases[i].dsn);
    if (strcmp(_cases[i].test_name, TN_SQLSERVER) == 0) {
      strcat(dsn, ";Uid=");
      strcat(dsn, _cases[i].user);
      strcat(dsn, ";Pwd=");
      strcat(dsn, _cases[i].pwd);
      strcat(dsn, ";DATABASE=");
    }
    else {
      // taos-odbc 支持两种写法
      if (0) {
        strcat(dsn, ";DB=");
      }
      else {
        strcat(dsn, ";DATABASE=");
      }
    }
    strcat(dsn, db);

    XX("connect string:%s", dsn);
    r = test_sql_driver_conn(_cases[i].ctx.hconn, dsn);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
    XX("dsn:%s result:%d", dsn, r);
  }
  return 0;
}

static int create_stmt(void) {
  int r = 0;
  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHANDLE hconn = _cases[i].ctx.hconn;

    r = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &_cases[i].ctx.hstmt);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  }
  return 0;
}

static int free_stmt(void) {
  int r = 0;
  for (int i = 0; i < db_source_ready_num; ++i) {
    if (_cases[i].ctx.hstmt != NULL) {
      XX("CALL_SQLFreeHandle stmt %p", _cases[i].ctx.hstmt);
      r = CALL_SQLFreeHandle(SQL_HANDLE_STMT, _cases[i].ctx.hstmt);
      XX("CALL_SQLFreeHandle stmt;%p result:%d", _cases[i].ctx.hstmt, r);
      _cases[i].ctx.hstmt = NULL;
    }
  }
  return 0;
}

static int reset_stmt(void) {
  CHK0(free_stmt, 0);
  CHK0(create_stmt, 0);
  return 0;
}

static int drop_database(char* db) {
  int r = 0;

  CHK0(reset_stmt, 0);
  char sql[128];
  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHANDLE hstmt = _cases[i].ctx.hstmt;
    if (strcmp(_cases[i].test_name, TN_SQLSERVER) == 0) {
      memset(sql, 0, sizeof(sql));
      strcat(sql, "use master; ");
      strcat(sql, "drop database if exists ");
      strcat(sql, db);
      strcat(sql, ";");
    }
    else {
      memset(sql, 0, sizeof(sql));
      strcat(sql, "drop database if exists ");
      strcat(sql, db);
    }
    r = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    XX(" %s CALL_SQLExecDirect result:%d", sql, r);
    if (r == SQL_SUCCESS_WITH_INFO) {
      get_diag_rec(SQL_HANDLE_STMT, hstmt);
    }
    CHKSTMTR(hstmt, r);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
    XX("drop database %s finished", db);
  }
  return 0;
}

static int free_stmt_test(void) {
  int r = 0;
  char sql[1024];
  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHANDLE hconntmp;

    r = CALL_SQLAllocHandle(SQL_HANDLE_DBC, _cases[i].ctx.henv, &hconntmp);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    r = CALL_SQLConnect(hconntmp, (SQLCHAR*)_cases[i].dsn, SQL_NTS, (SQLCHAR*)_cases[i].user, SQL_NTS, (SQLCHAR*)_cases[i].pwd, SQL_NTS);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    SQLHANDLE hstmttmp;
    r = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconntmp, &hstmttmp);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    memset(sql, 0, sizeof(sql));
    strcat(sql, "create database ");
    strcat(sql, test_db2);
    r = CALL_SQLExecDirect(hstmttmp, (SQLCHAR*)sql, SQL_NTS);
    XX("%s CALL_SQLExecDirect result:%d", sql, r);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    r = CALL_SQLDisconnect(hconntmp);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    r = CALL_SQLSetConnectAttr(hconntmp, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)30, SQL_IS_UINTEGER);
    XX("CALL_SQLSetConnectAttr SQL_ATTR_LOGIN_TIMEOUT set 30, result:%d", r);

    r = CALL_SQLSetConnectAttr(hconntmp, SQL_ATTR_PACKET_SIZE, (SQLPOINTER)1048576, SQL_IS_UINTEGER);
    XX("CALL_SQLSetConnectAttr SQL_ATTR_PACKET_SIZE set 1048576 byte, result:%d", r);

    char* db = "power2";
    char dsn[100] = { 0 };
    strcat(dsn, "DSN=");
    strcat(dsn, _cases[i].dsn);
    if (strcmp(_cases[i].test_name, TN_SQLSERVER) == 0) {
      strcat(dsn, ";Uid=");
      strcat(dsn, _cases[i].user);
      strcat(dsn, ";Pwd=");
      strcat(dsn, _cases[i].pwd);
      strcat(dsn, ";DATABASE=");
    }
    else {
      // taos-odbc 支持两种写法
      if (0) {
        strcat(dsn, ";DB=");
      }
      else {
        strcat(dsn, ";DATABASE=");
      }
    }
    strcat(dsn, db);

    XX("connect string:%s", dsn);
    r = test_sql_driver_conn(hconntmp, dsn);
    XX("dsn:%s result:%d", dsn, r);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    //r = CALL_SQLConnect(hconntmp, (SQLCHAR*)_cases[i].dsn, SQL_NTS, (SQLCHAR*)_cases[i].user, SQL_NTS, (SQLCHAR*)_cases[i].pwd, SQL_NTS);
    //if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    r = CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmttmp);
    XX("free invalid stmt, result:%d", r);
  }

  return 0;
}

static int create_hconn(void) {
  int r = 0;
  for (int i = 0; i < db_source_ready_num; ++i) {
    r = CALL_SQLAllocHandle(SQL_HANDLE_DBC, _cases[i].ctx.henv, &_cases[i].ctx.hconn);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  }
  return 0;
}

static int set_conn_attr_before(void) {
  int r = 0;
  for (int i = 0; i < db_source_ready_num; ++i) {
    r = CALL_SQLSetConnectAttr(_cases[i].ctx.hconn, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)30, SQL_IS_UINTEGER);
    XX("CALL_SQLSetConnectAttr SQL_ATTR_LOGIN_TIMEOUT set 30, result:%d", r);
  }
  for (int i = 0; i < db_source_ready_num; ++i) {
    r = CALL_SQLSetConnectAttr(_cases[i].ctx.hconn, SQL_ATTR_PACKET_SIZE, (SQLPOINTER)1048576, SQL_IS_UINTEGER);
    XX("CALL_SQLSetConnectAttr SQL_ATTR_PACKET_SIZE set 1048576 byte, result:%d", r);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
      E("CALL_SQLSetConnectAttr SQL_ATTR_PACKET_SIZE set 1048576 unsupported by taos-odbc");
    }
  }
  for (int i = 0; i < db_source_ready_num; ++i) {
    r = CALL_SQLSetConnectAttr(_cases[i].ctx.hconn, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_ONLY, SQL_IS_UINTEGER);
    XX("CALL_SQLSetConnectAttr SQL_ATTR_ACCESS_MODE set SQL_MODE_READ_ONLY, result:%d", r);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
      E("CALL_SQLSetConnectAttr SQL_ATTR_ACCESS_MODE set SQL_MODE_READ_ONLY unsupported by taos-odbc");
    }
  }
  return 0;
}

static int connect_default(void) {
  int r = 0;
  for (int i = 0; i < db_source_ready_num; ++i) {
    XX("dsn:%s user:%s pwd:%s", _cases[i].dsn, _cases[i].user, _cases[i].pwd);
    r = CALL_SQLConnect(_cases[i].ctx.hconn, (SQLCHAR*)_cases[i].dsn, SQL_NTS, (SQLCHAR*)_cases[i].user, SQL_NTS, (SQLCHAR*)_cases[i].pwd, SQL_NTS);
    XX("dsn:%s result:%d", _cases[i].dsn, r);
    CHKDBR(_cases[i].ctx.hconn, r);
  }
  return 0;
}

static int connect_repeat(void) {
  int r = 0;
  for (int i = 0; i < db_source_ready_num; ++i) {
    XX("connect second time dsn:%s user:%s pwd:%s", _cases[i].dsn, _cases[i].user, _cases[i].pwd);
    r = CALL_SQLConnect(_cases[i].ctx.hconn, (SQLCHAR*)_cases[i].dsn, SQL_NTS, (SQLCHAR*)_cases[i].user, SQL_NTS, (SQLCHAR*)_cases[i].pwd, SQL_NTS);
    XX("connect second time dsn:%s result:%d", _cases[i].dsn, r);
    if (r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO) return -1;
  }
  return 0;
}

static int dis_connect() {
  int r = 0;
  for (int i = 0; i < db_source_ready_num; ++i) {
    if (_cases[i].ctx.hconn != NULL) {
      r = CALL_SQLDisconnect(_cases[i].ctx.hconn);
      XX("CALL_SQLDisconnect result:%d", r);
    }
  }
  return r;
}

static int browse_connect(void) {
  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHDBC hdbc = _cases[i].ctx.hconn;
    SQLCHAR inConnectionString[256] = "";
    strcpy((char*)inConnectionString, _cases[i].connstr);
    SQLCHAR outConnectionString[256] = "";
    SQLSMALLINT outConnectionStringLength; //

    SQLBrowseConnect(hdbc, inConnectionString, sizeof(inConnectionString), outConnectionString, sizeof(outConnectionString), &outConnectionStringLength);

    XX("Connection String: %s", outConnectionString);
  }
  return 0;
}

static int get_driver_info(void) {
  int r = 0;
  CHK0(reset_stmt, 0);
  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHDBC henv = _cases[i].ctx.henv;
    SQLHDBC hdbc = _cases[i].ctx.hconn;
    SQLHDBC hstmt = _cases[i].ctx.hstmt;

    SQLCHAR driverInfo[256];
    r = CALL_SQLGetInfo(hdbc, SQL_DRIVER_NAME, driverInfo, sizeof(driverInfo), NULL);
    CHKENVR(henv, r);
    XX("Driver name: %s", driverInfo);

    driverInfo[0] = '\0';
    r = CALL_SQLGetInfo(hstmt, SQL_DRIVER_HSTMT, driverInfo, sizeof(driverInfo), NULL);
    XX("SQL_DRIVER_HSTMT result:%d info: %s", r, driverInfo);
    r = CALL_SQLGetInfo(hstmt, SQL_DRIVER_HDESC, driverInfo, sizeof(driverInfo), NULL);
    XX("SQL_DRIVER_HDESC result:%d info: %s", r, driverInfo);
  }

  int notSupported[200];
  int notSupportedCount = 0;
  SQLCHAR driverInfo[256];
  for (SQLUSMALLINT ty = SQL_INFO_FIRST; ty <= SQL_CONVERT_GUID; ty++) {
    if (ty == SQL_DRIVER_HSTMT || ty == SQL_DRIVER_HDESC) continue;

    int rs[DB_SOURCE_ALL_NUM];
    for (int i = 0; i < db_source_ready_num; ++i) {
      SQLHDBC hdbc = _cases[i].ctx.hconn;

      rs[i] = CALL_SQLGetInfo(hdbc, ty, driverInfo, sizeof(driverInfo), NULL);
      if (i == (db_source_ready_num-1) && (rs[0] >= 0 && rs[i] < 0)) {
        notSupported[notSupportedCount] = ty;
        notSupportedCount++;
        X("CALL_SQLGetInfo type:%d not supported by taos-odbc.", ty);
      }
    }
  }

  char str[1024] = { 0 };
  for (int i = 0; i < notSupportedCount; i++) {
    snprintf(str + strlen(str), 1024 - strlen(str), " %d", notSupported[i]);
  }

  X("CALL_SQLGetInfo type not supported by taos-odbc: %s", str);

  return 0;
}

static int set_conn_attr(void) {
  int r = 0;
  for (int i = 0; i < db_source_ready_num; ++i) {
    r = CALL_SQLSetConnectAttr(_cases[i].ctx.hconn, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)30, SQL_IS_UINTEGER);
    XX("CALL_SQLSetConnectAttr SQL_ATTR_CONNECTION_TIMEOUT set 30, result:%d", r);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return r;
  }

  return 0;
}

static int create_database(char* db) {
  int r = 0;
  CHK0(reset_stmt, 0);
  char sql[128];
  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHANDLE hstmt = _cases[i].ctx.hstmt;
    memset(sql, 0, sizeof(sql));
    strcat(sql, "create database ");
    strcat(sql, db);
    r = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    XX("%s CALL_SQLExecDirect result:%d", sql, r);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  }

  return 0;
}

static int show_database(void) {
  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHANDLE hconn = _cases[i].ctx.hconn;

    SQLCHAR dbName[256];
    CALL_SQLGetInfo(hconn, SQL_DATABASE_NAME, dbName, sizeof(dbName), NULL);

    // 打印当前数据库名
    XX("Current database name:%s", dbName);
  }
  return 0;
}

static int show_tables(void) {
  int r = 0;
  CHK0(reset_stmt, 0);

  SQLCHAR table_names[DB_SOURCE_ALL_NUM][100][MAX_TABLE_NAME_LEN];
  int table_count[DB_SOURCE_ALL_NUM] = { 0 };

  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHANDLE hstmt = _cases[i].ctx.hstmt;
    SQLCHAR* tableType = (SQLCHAR*)"TABLE";
    r = SQLTables(hstmt, NULL, 0, NULL, 0, NULL, 0, tableType, SQL_NTS);
    XX("SQLTables result:%d", r);
    // 遍历结果集并打印表名
    // SQLCHAR table_name[MAX_TABLE_NAME_LEN];
    while (SQLFetch(hstmt) == SQL_SUCCESS) {
      SQLGetData(hstmt, 3, SQL_C_CHAR, table_names[i][table_count[i]], MAX_TABLE_NAME_LEN, NULL);
      // XX("table name: %s", table_names[i][table_count[i]]);
      table_count[i]++;
    }
    r = CALL_SQLCloseCursor(_cases[i].ctx.hstmt);
    XX("CALL_SQLCloseCursor result:%d", r);
    r = CALL_SQLCloseCursor(_cases[i].ctx.hstmt);
    XX("CALL_SQLCloseCursor Second result:%d", r);

  }
  for (int i = 0; i < db_source_ready_num; ++i) {
    for (int j = 0; j < table_count[i]; j++) {
      XX("table name: %s", table_names[i][j]);
    }
  }

  return 0;
}

static int create_table(const char* tb) {
  int r = 0;
  CHK0(reset_stmt, 0);
  char buf[1024];
  simple_str_t str = {
    .base = buf,
    .cap = sizeof(buf),
    .nr = 0,
  };

  r = _gen_table_create_sql(&str, tb, fields, sizeof(fields) / sizeof(fields[0]));

  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHANDLE hstmt = _cases[i].ctx.hstmt;
    r = CALL_SQLExecDirect(hstmt, (SQLCHAR*)buf, (SQLINTEGER)strlen(buf));
    XX("%s | result:%d", buf, r);
    CHKSTMTR(hstmt, r);
  }

  return 0;
}

static int drop_table(const char* tb) {
  int r = 0;
  CHK0(reset_stmt, 0);
  char sql[1024];
  memset(sql, 0, sizeof(sql));
  strcat(sql, "drop table ");
  strcat(sql, tb);
  strcat(sql, ";");

  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHANDLE hstmt = _cases[i].ctx.hstmt;
    r = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, (SQLINTEGER)strlen(sql));
    XX("%s | result:%d", sql, r);
    CHKSTMTR(hstmt, r);
  }

  return 0;
}

static int clear_test(void) {
  int r = 0;
  free_stmt();
  for (int i = 0; i < db_source_ready_num; ++i) {
    // 当且仅当先释放 sql-odbc驱动资源，后释放taos-odbc资源
    // 且先CALL_SQLDisconnect断开连接，后释放hstmt时，taos-odbc 释放stmt会引起程序异常
    // 很奇怪，不确定原因，暂时记录
    // r = CALL_SQLFreeHandle(SQL_HANDLE_STMT, _cases[i].ctx.hstmt);
    // XX("CALL_SQLFreeHandle stmt;%p result:%d", _cases[i].ctx.hstmt, r);
    r = CALL_SQLDisconnect(_cases[i].ctx.hconn);
    XX("CALL_SQLDisconnect result:%d", r);
    r = CALL_SQLFreeHandle(SQL_HANDLE_DBC, _cases[i].ctx.hconn);
    XX("CALL_SQLFreeHandle hconn:%p result:%d", _cases[i].ctx.hconn, r);
    r = CALL_SQLFreeHandle(SQL_HANDLE_ENV, _cases[i].ctx.henv);
    XX("CALL_SQLDisconnect henv:%p result:%d", _cases[i].ctx.henv, r);
  }
  return 0;
}

static int get_records_count(SQLHANDLE hstmt, const char* table)
{
  if (table == NULL) return -1;
  int r = 0;
  char sql[1024] = "select * from ";
  strcat(sql, table);

  X("get record data, exec(%s)", sql);
  r = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
  X("get record data, exec(%s) direct result:%d", sql, r);
  CHKSTMTR(hstmt, r);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  int count = 0;
  while (1) {
    r = SQLFetch(hstmt);
    if (r == SQL_ERROR)
    {
      X("get record, error.");
      CHKSTMTR(hstmt, r);
      return -1;
    }
    if (r == SQL_NO_DATA) {
      X("get record, no more data.");
      break;
    }
    X("get record data.");
    count += 1;
  }

  return count;
}


static int no_param_sql_test(void) {
  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHANDLE hstmt = _cases[i].ctx.hstmt;

    // item 7: 继续使用之前的 stmt， taos-odbc 报错
    // r = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
    // if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
    XX("get_records_count start...%p", hstmt);
    int count = get_records_count(hstmt, tb_test1);
    XX("get_records_count %d.", count);
    if (count != ARRAY_SIZE) {
      XX("get_records_count error, expect %d got %d.", ARRAY_SIZE, count);
      return -1;
    }
  }

  return 0;
}

static int case1_repeat_insert(void) {
  int r = 0;

  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHANDLE hstmt = _cases[i].ctx.hstmt;
    if (1) {
      char sql2[1024] = "insert into ";
      strcat(sql2, tb_test2);
      if (strcmp(_cases[i].test_name, TN_SQLSERVER) == 0) {
        strcat(sql2, "(vname, wname, bi) values(? , ? , ? )");
      }
      else {
        strcat(sql2, "(ts, vname, wname, bi) values(?, ? , ? , ? )");
      }

      r = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql2, SQL_NTS);
      X("exec(%s) direct result:%d", sql2, r);
      CHKSTMTR(hstmt, r);
      if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
    }

    if (1) {
      char sql2[1024] = "insert into ";
      strcat(sql2, tb_test1);
      if (strcmp(_cases[i].test_name, TN_SQLSERVER) == 0) {
        strcat(sql2, "(vname, wname, bi) values(? , ? , ? )");
      }
      else {
        strcat(sql2, "(ts, vname, wname, bi) values(?, ? , ? , ? )");
      }

      r = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql2, SQL_NTS);
      X("exec(%s) direct result:%d", sql2, r);
      CHKSTMTR(hstmt, r);
      if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
    }

    if (1) {
      char sql2[1024] = "insert into ";
      strcat(sql2, tb_test2);
      if (strcmp(_cases[i].test_name, TN_SQLSERVER) == 0) {
        strcat(sql2, "(vname, wname) values(? , ? )");
      }
      else {
        strcat(sql2, "(ts, vname, wname) values(?, ? , ? )");
      }

      r = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql2, SQL_NTS);
      X("exec(%s) direct result:%d", sql2, r);
      CHKSTMTR(hstmt, r);
      if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
    }
  }
  return 0;
}

static int get_param_pos(const char* dsn) {
  if (strcmp(dsn, TN_SQLSERVER) == 0) {
    return 0;
  }
  else if (strcmp(dsn, TN_MYSQL) == 0) {
    return 1;
  }
  else if (strcmp(dsn, TN_TAOS) == 0) {
    return 2;
  }
  else {
    return -1;
  }
}

static int case_1(int action) {
  int r = 0;

  time_t currentTime = time(NULL);

  int64_t ts_arr[ARRAY_SIZE] = { 0 };
  char ts_str_arr[ARRAY_SIZE][100];
  SQLLEN  ts_ind[ARRAY_SIZE] = { 0 };
  char    varchar_arr[ARRAY_SIZE][100];
  SQLLEN  varchar_ind[ARRAY_SIZE] = { 0 };
  char    nchar_arr[ARRAY_SIZE][100];
  SQLLEN  nchar_ind[ARRAY_SIZE] = { 0 };
  int64_t i64_arr[ARRAY_SIZE] = { 0 };
  SQLLEN  i64_ind[ARRAY_SIZE] = { 0 };

  // hard code for position of each data source
  int param_len[] = { 3, 4, 4 };

  const param_t params[DB_SOURCE_ALL_NUM][4] = {
    {
      // sqlserver
      {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_VARCHAR,         99,       0,          varchar_arr,      100, varchar_ind},
      {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_WVARCHAR,        99,       0,          nchar_arr,        100, nchar_ind},
      {SQL_PARAM_INPUT,  SQL_C_SBIGINT,  SQL_BIGINT,          99,       0,          i64_arr,          100, i64_ind},
    },{
      // mysql
      {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_TYPE_TIMESTAMP,  99,       0,          ts_str_arr,       100, nchar_ind},
      {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_VARCHAR,         99,       0,          varchar_arr,      100, varchar_ind},
      {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_WVARCHAR,        99,       0,          nchar_arr,        100, nchar_ind},
      {SQL_PARAM_INPUT,  SQL_C_SBIGINT,  SQL_BIGINT,          99,       0,          i64_arr,          100, i64_ind},
    },
    // taos-odbc
    {
      {SQL_PARAM_INPUT,  SQL_C_SBIGINT,  SQL_TYPE_TIMESTAMP,  23,       3,          ts_arr,           0,   ts_ind},
      {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_VARCHAR,         99,       0,          varchar_arr,      100, varchar_ind},
      {SQL_PARAM_INPUT,  SQL_C_CHAR,     SQL_WVARCHAR,        99,       0,          nchar_arr,        100, nchar_ind},
      {SQL_PARAM_INPUT,  SQL_C_SBIGINT,  SQL_BIGINT,          99,       0,          i64_arr,          100, i64_ind},
    },
  };

  for (int i = 0; i < ARRAY_SIZE; ++i) {
    time_t tempt = currentTime + i;
    char buffer[100];
    struct tm* timeinfo = localtime(&tempt);
    strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", timeinfo);

    ts_arr[i] = tempt * 1000;
    snprintf(ts_str_arr[i], sizeof(ts_str_arr[i]), "%s", buffer);
    snprintf(varchar_arr[i], 100, "abcd%d", i + 9);
    varchar_ind[i] = SQL_NTS;
    snprintf(nchar_arr[i], 100, "bt%d", i);
    nchar_ind[i] = SQL_NTS;
    i64_arr[i] = 54321 + i;
  }

  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHANDLE hconn = _cases[i].ctx.hconn;
    SQLHANDLE hstmt = _cases[i].ctx.hstmt;

    SQLUINTEGER convert_bigint;
    r = CALL_SQLGetInfo(hconn, SQL_CONVERT_BIGINT, &convert_bigint,
      sizeof(convert_bigint), NULL);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
      XX("Failed to CALL_SQLGetInfo SQL_CONVERT_BIGINT information.");
    }

    SQLULEN nr_paramset_size = ARRAY_SIZE;
    SQLUSMALLINT param_status_arr[ARRAY_SIZE] = { 0 };
    SQLULEN nr_params_processed = 0;
    memset(param_status_arr, 0, ARRAY_SIZE * sizeof(SQLUSMALLINT));

    char* buf = _cases[i].ctx.buf;
    simple_str_t str = _cases[i].ctx.sql_str;
    if (strcmp(_cases[i].test_name, TN_SQLSERVER) == 0) {
      snprintf(buf, 1024, "insert into tx1 (vname,wname,bi) values (?,?,?)");
    }
    else {
      r = _gen_table_param_insert(&str, tb_test1, fields, sizeof(fields) / sizeof(fields[0]));
    }
    XX("SQL:%s", buf);

    r = SQLPrepare(hstmt, (SQLCHAR*)buf, SQL_NTS);
    XX("SQLPrepare result:%d", r);
    CHKSTMTR(hstmt, r);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    r = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);
    XX("CALL_SQLSetStmtAttr SQL_ATTR_PARAM_BIND_TYPE result:%d", r);

    r = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)(uintptr_t)nr_paramset_size, 0);
    XX("CALL_SQLSetStmtAttr SQL_ATTR_PARAMSET_SIZE result:%d", r);

    r = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_STATUS_PTR, param_status_arr, 0);
    XX("CALL_SQLSetStmtAttr SQL_ATTR_PARAM_STATUS_PTR result:%d", r);

    r = CALL_SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &nr_params_processed, 0);
    XX("CALL_SQLSetStmtAttr SQL_ATTR_PARAMS_PROCESSED_PTR result:%d", r);

    int pos = get_param_pos(_cases[i].test_name);
    for (int j = 0; j < param_len[pos]; ++j) {
      const param_t* param = params[pos] + j;
      r = SQLBindParameter(hstmt, (SQLUSMALLINT)j + 1,
        param->InputOutputType,
        param->ValueType,
        param->ParameterType,
        param->ColumnSize,
        param->DecimalDigits,
        param->ParameterValuePtr,
        param->BufferLength,
        param->StrLen_or_IndPtr);
      XX("SQLBindParameter %dth column result:%d", (int)j, r);
      CHKSTMTR(hstmt, r);
    }

    r = CALL_SQLExecute(hstmt);
    X("CALL_SQLExecute result:%d.", r);
    CHKSTMTR(hstmt, r);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  }
  
  if (action == 1) {
    CHK0(no_param_sql_test, 0);
  }
  else if (action == 2) {
    CHK0(case1_repeat_insert, 0);
  }
  else {
    X("action:%d", action);
  }

  return 0;
}

static int case1_select1_after_exec(void) {
  CHK1(create_table, tb_test1, 0);
  CHK1(create_table, tb_test2, 0);

  CHK0(reset_stmt, 0);
  CHK1(case_1, 1, 0);

  CHK1(drop_table, tb_test1, 0);
  CHK1(drop_table, tb_test2, 0);

  return 0;
}

static int case1_select3_after_exec(void) {
  CHK1(create_table, tb_test1, 0);
  CHK1(create_table, tb_test2, 0);

  CHK0(reset_stmt, 0);
  CHK1(case_1, 3, 0);

  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHANDLE hstmt = _cases[i].ctx.hstmt;

    CALL_SQLCloseCursor(hstmt);
    // item 7: 继续使用之前的 stmt， taos-odbc 报错
    // r = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
    // if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
    XX("get_records_count start...%p", hstmt);
    int count = get_records_count(hstmt, tb_test1);
    XX("get_records_count %d.", count);
    if (count != ARRAY_SIZE) {
      XX("get_records_count error, expect %d got %d.", ARRAY_SIZE, count);
      return -1;
    }
  }

  CHK1(drop_table, tb_test1, 0);
  CHK1(drop_table, tb_test2, 0);

  return 0;
}

static int case1_select2_after_exec(void) {
  CHK1(create_table, tb_test1, 0);
  CHK1(create_table, tb_test2, 0);

  CHK0(reset_stmt, 0);
  CHK1(case_1, 3, 0);
  CHK0(no_param_sql_test, 0);

  CHK1(drop_table, tb_test1, 0);
  CHK1(drop_table, tb_test2, 0);

  return 0;
}

static int case1_insert1_after_exec(void) {
  CHK1(create_table, tb_test1, 0);
  CHK1(create_table, tb_test2, 0);

  CHK1(case_1, 2, 0);
  CHK0(free_stmt, 0);

  CHK1(drop_table, tb_test1, 0);
  CHK1(drop_table, tb_test2, 0);
  return 0;
}

static int case1_insert2_after_exec(void) {
  CHK1(create_table, tb_test1, 0);
  CHK1(create_table, tb_test2, 0);

  CHK1(case_1, 3, 0);
  CHK0(case1_repeat_insert, 0);
  CHK0(free_stmt, 0);

  CHK1(drop_table, tb_test1, 0);
  CHK1(drop_table, tb_test2, 0);
  return 0;
}

static int case_multi_line_sql(void) {
  char* tmp_tb = "tmp_tb";
  CHK1(create_table, tmp_tb, 0);

  CHK0(reset_stmt, 0);
  int r = 0;

  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHANDLE hstmt = _cases[i].ctx.hstmt;

    char* buf = _cases[i].ctx.buf;
    if (strcmp(_cases[i].test_name, TN_SQLSERVER) == 0) {
      snprintf(buf, 1024, "insert into tmp_tb (vname, wname, bi) values  ('vname1', 'wname1', '10001');");
      //strcat(buf, "insert into tmp_tb (vname, wname, bi) values  ('vname2', 'wname2', '10002');");
    }
    else {
      snprintf(buf, 1024, "insert into tmp_tb (ts, vname, wname, bi) values  (now(), 'vname1', 'wname1', '10001');");
      //strcat(buf, "insert into tmp_tb (ts, vname, wname, bi) values  (now(), 'vname2', 'wname2', '10002');");
    }
    XX("SQL:%s", buf);

    r = SQLPrepare(hstmt, (SQLCHAR*)buf, SQL_NTS);
    XX("SQLPrepare result:%d", r);
    CHKSTMTR(hstmt, r);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    r = CALL_SQLExecute(hstmt);
    X("CALL_SQLExecute result:%d.", r);
    CHKSTMTR(hstmt, r);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  }

  CHK0(reset_stmt, 0);
  for (int i = 0; i < db_source_ready_num; ++i) {
    SQLHANDLE hconn = _cases[i].ctx.hconn;
    SQLHANDLE hstmt = _cases[i].ctx.hstmt;
    r = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    int count = get_records_count(hstmt, tmp_tb);
    XX("get_records_count %d.", count);
    if (count != 1) {
      XX("get_records_count error, expect %d got %d.", ARRAY_BIGGER_SIZE, count);
      return -1;
    }
  }

  //CHK1(drop_table, tmp_tb, 0);
  return 0;
}

static int sql_tables_test(void) {
  CHK0(show_tables, 0);
  CHK1(create_table, tb_test1, 0);
  CHK1(create_table, tb_test2, 0);
  CHK0(show_tables, 0);
  CHK1(drop_table, tb_test1, 0);
  CHK1(drop_table, tb_test2, 0);
  CHK0(show_tables, 0);
  return 0;
}

static void init_sql_str(void) {
  for (int i = 0; i < db_source_ready_num; ++i) {
    _cases[i].ctx.sql_str.base = _cases[i].ctx.buf;
    _cases[i].ctx.sql_str.cap = sizeof(_cases[i].ctx.buf);
    _cases[i].ctx.sql_str.nr = 0;
  }
}

static int basic(void) {
  CHK0(browse_connect, 0);
  CHK0(connect_repeat, 0);
  CHK0(connect_test, 0);
  CHK0(set_conn_attr, 0);
  CHK0(get_driver_info, 0);
  CHK0(show_database, 0);
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
  if (isTestCase(argc, argv, "basic", default_supported)) CHK0(basic, 0);
  if (isTestCase(argc, argv, "sql_tables_test", default_supported)) CHK0(sql_tables_test, 0);
  
  if (isTestCase(argc, argv, "free_stmt_test", default_supported)) CHK0(free_stmt_test, 0);
    
  if (isTestCase(argc, argv, "case1_insert1_after_exec", default_supported)) CHK0(case1_insert1_after_exec, 0);
  if (isTestCase(argc, argv, "case_multi_line_sql", default_supported)) CHK0(case_multi_line_sql, 0);

  {
    // unsupported cases;
    if (isTestCase(argc, argv, "case1_select1_after_exec", default_unsupported)) CHK0(case1_select1_after_exec, 0);
    if (isTestCase(argc, argv, "case1_select2_after_exec", default_unsupported)) CHK0(case1_select2_after_exec, 0);
    if (isTestCase(argc, argv, "case1_select3_after_exec", default_unsupported)) CHK0(case1_select3_after_exec, 0);
    if (isTestCase(argc, argv, "case1_insert2_after_exec", default_unsupported)) CHK0(case1_insert2_after_exec, 0);
  }

  X("The test finished successfully.");
  return 0;
}

static int list_driver(void) {
  SQLRETURN ret;
  SQLCHAR driverDesc[1024];
  SQLCHAR driverAttr[1024];
  SQLSMALLINT driverDescLen, driverAttrLen;
  SQLUSMALLINT direction = SQL_FETCH_FIRST;

  SQLHANDLE env = SQL_NULL_HANDLE;
  ret = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
  if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    X("Failed to allocate ODBC environment handle.");
    return -1;
  }

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_UINTEGER);
  if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    X("Failed to set ODBC environment attribute.");
    CALL_SQLFreeHandle(SQL_HANDLE_ENV, env);
    return -1;
  }

  do {
    ret = SQLDrivers(env, direction, driverDesc, sizeof(driverDesc), &driverDescLen, driverAttr, sizeof(driverAttr), &driverAttrLen);
    if (ret == SQL_SUCCESS) {
      X("Driver Description: %s", driverDesc);
      X("Driver Attributes: %s\n\n", driverAttr);
    }
    else if (ret == SQL_SUCCESS_WITH_INFO) {
      X("Driver Description: %s", driverDesc);
      X("Driver Attributes: %s", driverAttr);
      get_diag_rec(SQL_HANDLE_ENV, env);
    }
    else if (ret == SQL_NO_DATA) {
      X("SQLDrivers check finishd.");
    }
    else {
      X("SQLDrivers result:%d", ret);
      CHKENVR(env, ret);
    }

    direction = SQL_FETCH_NEXT;
  } while (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO);

  CALL_SQLFreeHandle(SQL_HANDLE_ENV, env);

  return 0;
}

static int list_datasource(void) {
  SQLHENV env;
  SQLRETURN ret;
  SQLCHAR dataSourceName[1024];
  SQLCHAR description[1024];
  SQLSMALLINT nameLength, descLength;
  int count = 0;

  // 初始化ODBC环境
  ret = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
  if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    X("Failed to allocate ODBC environment handle.");
    return -1;
  }

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_UINTEGER);
  if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    X("Failed to set ODBC environment attribute.");
    CALL_SQLFreeHandle(SQL_HANDLE_ENV, env);
    return -1;
  }

  ret = SQLDataSources(env, SQL_FETCH_FIRST, dataSourceName, sizeof(dataSourceName), &nameLength, description, sizeof(description), &descLength);
  while (ret == SQL_SUCCESS) {
    count++;
    X("Data Source Name: %s", dataSourceName);
    X("Description: %s\n", description);
    set_test_case_valid(dataSourceName);

    ret = SQLDataSources(env, SQL_FETCH_NEXT, dataSourceName, sizeof(dataSourceName), &nameLength, description, sizeof(description), &descLength);
  }

  // 检查获取数据源列表的结果
  if (ret != SQL_NO_DATA) {
    printf("Failed to retrieve ODBC data sources.\n");
  }
  else {
    printf("Total data sources: %d\n", count);
  }

  // 释放句柄
  CALL_SQLFreeHandle(SQL_HANDLE_ENV, env);

  return -1;
}

static void list_test_data_source(void) {
  db_source_ready_num = get_data_source_num();
  _cases = (data_source_case*)malloc(db_source_ready_num * sizeof(data_source_case));
  int j = 0;
  for (size_t i = 0; i < sizeof(_cases_all) / sizeof(_cases_all[0]); ++i) {
    if (_cases_all[i].valid) {
      memcpy(&_cases[j], &_cases_all[i], sizeof(data_source_case));
      j++;
    }
  }
  for (int i = 0; i < db_source_ready_num; ++i) {
    const char* invalid = (_cases[i]).valid ? " ready to testing" : "not ready to testing";
    XX(" %s", invalid);
    XX("test_name: %s", _cases[i].test_name);
    XX("connstr: %s", _cases[i].connstr);
    XX("dsn: %s", _cases[i].dsn);
    XX("user: %s", _cases[i].user);
    XX("pwd: %s", _cases[i].pwd);
  }
}

static int init_test(void) {
  list_driver();
  list_datasource();
  list_test_data_source();
  init_sql_str();
  CHK0(init_henv, 0);
  CHK0(create_hconn, 0);
  CHK0(connect_default, 0);
  CHK1(drop_database, test_db, 0);
  CHK1(drop_database, test_db2, 0);
  CHK1(create_database, test_db, 0);
  CHK0(dis_connect, 0);
  CHK0(set_conn_attr_before, 0);
  CHK1(connect_select_db, test_db, 0);
  set_null_stmt();
  X("init_test finished!");

  return 0;
}

int main(int argc, char* argv[])
{
  int r = 0;
  CHK0(init_test, 0);
#ifndef _WIN32
  if (1) return 0;
#endif 

  r = run(argc, argv);
  clear_test();
  fprintf(stderr, "==%s==\n", r ? "failure" : "success");

  return !!r;
}
