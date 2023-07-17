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
#include "../test_helper.h"

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
};

struct test_case {
	const char* test_name;
	const char* connstr;
	const char* dsn;
	const char* user;
	const char* pwd;
	struct test_context ctx;
} _cases[] = {
	{
	  "sqlserver-odbc",
	  "DSN=SQLSERVER_ODBC_DSN",
	  "SQLSERVER_ODBC_DSN",
	  NULL,
	  NULL,
	  {
		.henv = SQL_NULL_HANDLE,
		.hconn = SQL_NULL_HANDLE,
		.hstmt = SQL_NULL_HANDLE,
	  },
	},
	{
	  "taos-odbc",
	  "DSN=TAOS_ODBC_DSN",
	  "TAOS_ODBC_DSN",
	  NULL,
	  NULL,
	  {
		.henv = SQL_NULL_HANDLE,
		.hconn = SQL_NULL_HANDLE,
		.hstmt = SQL_NULL_HANDLE,
	  },
	},
};

#define MAX_TABLE_NAME_LEN 128
#define test_db "power"


field_t fields[] = {
  {"ts", "timestamp"},
  {"vname", "varchar(10)"},
  {"wname", "nchar(10)"},
  {"bi", "bigint"},
};

#define XX(_fmt, ...) do {                           \
  X("COMPARETEST %s " _fmt "",                       \
      _cases[i].test_name, ##__VA_ARGS__);           \
} while (0)

#define CHKENVR(henv, r)  do {																												\
 	if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)																						\
	{																																		\
		SQLCHAR sqlState[6];																												\
		SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];																							\
		SQLSMALLINT messageLength;																											\
		SQLINTEGER nativeError;																												\
	    if(SQLGetDiagRec(SQL_HANDLE_ENV, henv, 1, sqlState, &nativeError, message, sizeof(message), &messageLength)  != 100)			    \
		{																																	\
			E("SQLGetEnvAttr failed with error: %s", message);																				\
		}																																	\
		return -1;																															\
	}																																		\
} while (0)																																	\


#define CHKDBR(hconn, r)  do {																												\
 	if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)																						\
	{																																		\
		SQLCHAR sqlState[6];																												\
		SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];																							\
		SQLSMALLINT messageLength;																											\
		SQLINTEGER nativeError;																												\
	    if(SQLGetDiagRec(SQL_HANDLE_DBC, hconn, 1, sqlState, &nativeError, message, sizeof(message), &messageLength)  != 100)			    \
		{																																	\
			E("error: %s", message);																										\
		}																																	\
		return -1;																															\
	}																																		\
} while (0)	

#define CHKSTMTR(hstmt, r)  do {																											\
 	if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)																						\
	{																																		\
		SQLCHAR sqlState[6];																												\
		SQLCHAR message[SQL_MAX_MESSAGE_LENGTH]={0};																						\
		SQLSMALLINT messageLength;																											\
		SQLINTEGER nativeError;																												\
	    if(SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, sqlState, &nativeError, message, sizeof(message), &messageLength)  != 100)			    \
		{																																	\
			E("error: %s", message);																										\
		}																																	\
		return -1;																															\
	}																																		\
} while (0)	


static int init_henv() {
	SQLRETURN r;
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHANDLE henv = _cases[i].ctx.henv;
		r = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &_cases[i].ctx.henv);

		if (FAILED(r)) return -1;
		r = SQLSetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
		if (FAILED(r)) return -1;

		SQLINTEGER odbcVersion;
		r = SQLGetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_ODBC_VERSION, &odbcVersion, 0, NULL);
		if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
		XX("ODBC version: %d\n", (int)odbcVersion);
		if (odbcVersion != SQL_OV_ODBC3) return -1;
	}
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHANDLE henv = _cases[i].ctx.henv;
		r = SQLSetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)5UL, 0);
		XX("SQLSetEnvAttr SQL_ATTR_CONNECTION_POOLING set and invalid value, result:%d", r);
		if (SUCCEEDED(r)) return -1;
		r = 0;
	}
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHANDLE henv = _cases[i].ctx.henv;
		r = SQLSetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_HENV, 0);
		if (FAILED(r)) return -1;

		SQLINTEGER connpoll;
		r = SQLGetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_CONNECTION_POOLING, &connpoll, 0, NULL);
		if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
		XX("connection polling: %d\n", (int)connpoll);
		if (connpoll != SQL_CP_ONE_PER_HENV) return -1;
	}
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHANDLE henv = _cases[i].ctx.henv;
		r = SQLSetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_DRIVER, 0);
		if (FAILED(r)) return -1;

		SQLINTEGER connpoll;
		r = SQLGetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_CONNECTION_POOLING, &connpoll, 0, NULL);
		if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
		XX("connection polling: %d\n", (int)connpoll);
		if (connpoll != SQL_CP_ONE_PER_DRIVER) return -1;
	}
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHANDLE henv = _cases[i].ctx.henv;
		r = SQLSetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_OUTPUT_NTS, (SQLPOINTER)SQL_FALSE, 0);
		XX("SQLSetEnvAttr SQL_ATTR_OUTPUT_NTS set SQL_FALSE, result:%d", r);
		r = 0;
	}
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHANDLE henv = _cases[i].ctx.henv;
		r = SQLSetEnvAttr(_cases[i].ctx.henv, SQL_ATTR_OUTPUT_NTS, (SQLPOINTER)SQL_TRUE, 0);
		XX("SQLSetEnvAttr SQL_ATTR_OUTPUT_NTS set SQL_TRUE, result:%d", r);
	}

	return r;
}

static int connect_another() {
	int r = 0;
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHANDLE hconntmp;

		r = SQLAllocHandle(SQL_HANDLE_DBC, _cases[i].ctx.henv, &hconntmp);
		if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

		XX("create tmp connect dsn:%s user:%s pwd:%s", _cases[i].dsn, _cases[i].user, _cases[i].pwd);
		r = SQLConnect(hconntmp, (SQLCHAR*)_cases[i].dsn, SQL_NTS, (SQLCHAR*)_cases[i].user, SQL_NTS, (SQLCHAR*)_cases[i].pwd, SQL_NTS);
		if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
		XX("create tmp connect dsn:%s result:%d", _cases[i].dsn, r);

		r = SQLDisconnect(hconntmp);
		if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

		r = SQLDisconnect(hconntmp);
		if (r != SQL_ERROR) return -1;

		r = SQLFreeHandle(SQL_HANDLE_DBC, hconntmp);
		if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

		r = SQLFreeHandle(SQL_HANDLE_DBC, hconntmp);
		if (r != SQL_INVALID_HANDLE) return -1;
	}
	return 0;
}

static int create_hconn(){
	int r = 0;
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		r = SQLAllocHandle(SQL_HANDLE_DBC, _cases[i].ctx.henv, &_cases[i].ctx.hconn);
		if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
	}
	return 0;
}

static int set_conn_attr_before() {
	int r = 0;
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		r = SQLSetConnectAttr(_cases[i].ctx.hconn, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)30, SQL_IS_UINTEGER);
		XX("SQLSetConnectAttr SQL_ATTR_LOGIN_TIMEOUT set 30, result:%d", r);
	}
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		r = SQLSetConnectAttr(_cases[i].ctx.hconn, SQL_ATTR_PACKET_SIZE, (SQLPOINTER)1048576, SQL_IS_UINTEGER);
		XX("SQLSetConnectAttr SQL_ATTR_PACKET_SIZE set 1048576 byte, result:%d", r);
		if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
			E("SQLSetConnectAttr SQL_ATTR_PACKET_SIZE set 1048576 unsupported by taos-odbc");
		}
	}
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHANDLE henv = _cases[i].ctx.henv;
		r = SQLSetConnectAttr(_cases[i].ctx.hconn, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_ONLY, SQL_IS_UINTEGER);
		XX("SQLSetConnectAttr SQL_ATTR_ACCESS_MODE set SQL_MODE_READ_ONLY, result:%d", r);
		if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
			E("SQLSetConnectAttr SQL_ATTR_ACCESS_MODE set SQL_MODE_READ_ONLY unsupported by taos-odbc");
		}
	}
	return 0;
}

static int connect_all() {
	int r = 0;
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		XX("dsn:%s user:%s pwd:%s",  _cases[i].dsn, _cases[i].user, _cases[i].pwd);
		r = SQLConnect(_cases[i].ctx.hconn, (SQLCHAR*)_cases[i].dsn, SQL_NTS, (SQLCHAR*)_cases[i].user, SQL_NTS, (SQLCHAR*)_cases[i].pwd, SQL_NTS);
		if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
		XX("dsn:%s result:%d", _cases[i].dsn, r);

		XX("connect second time dsn:%s user:%s pwd:%s", _cases[i].dsn, _cases[i].user, _cases[i].pwd);
		r = SQLConnect(_cases[i].ctx.hconn, (SQLCHAR*)_cases[i].dsn, SQL_NTS, (SQLCHAR*)_cases[i].user, SQL_NTS, (SQLCHAR*)_cases[i].pwd, SQL_NTS);
		XX("connect second time dsn:%s result:%d", _cases[i].dsn, r);
		if (r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO) return -1;
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

static int re_connect_db(char* db) {
	int r = 0;
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		if (_cases[i].ctx.hconn != NULL) {
			r = SQLFreeHandle(SQL_HANDLE_STMT, _cases[i].ctx.hstmt);
			XX("SQLFreeHandle stmt;%p result:%d", _cases[i].ctx.hstmt, r);
			r = SQLDisconnect(_cases[i].ctx.hconn);
			XX("SQLDisconnect result:%d", r);
		}

		char dsn[100] = {0};
		strcat(dsn, "DSN=");
		strcat(dsn, _cases[i].dsn);
		if (strcmp(_cases[i].test_name, "sqlserver-odbc") == 0) {
			
				strcat(dsn, ";DATABASE=");
		}
		else {
			strcat(dsn, ";DB=");
		}
		strcat(dsn, db);

		XX("dsn:%s user:%s pwd:%s", dsn, _cases[i].user, _cases[i].pwd);
		r = test_sql_driver_conn(_cases[i].ctx.hconn, dsn);
		if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
		XX("dsn:%s result:%d", dsn, r);
	}
	return 0;
}

static int browseConnect() {
	int r = 0;
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHENV henv = _cases[i].ctx.henv;
		SQLHDBC hdbc = _cases[i].ctx.hconn;
		SQLCHAR inConnectionString[256] = "";
		strcpy(inConnectionString, _cases[i].connstr);
		SQLCHAR outConnectionString[256] = "";
		SQLSMALLINT outConnectionStringLength; //

		SQLBrowseConnect(hdbc, inConnectionString, sizeof(inConnectionString), outConnectionString, sizeof(outConnectionString), &outConnectionStringLength);

		XX("Connection String: %s", outConnectionString);
	}
	return r;
}

static int getDriverInfo() {
	int r = 0;
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHDBC henv = _cases[i].ctx.henv;
		SQLHDBC hdbc = _cases[i].ctx.hconn;
		SQLHDBC hstmt = _cases[i].ctx.hstmt;

		SQLCHAR driverInfo[256];
		r = SQLGetInfo(hdbc, SQL_DRIVER_NAME, driverInfo, sizeof(driverInfo), NULL);
		CHKENVR(henv, r);
		XX("Driver name: %s", driverInfo);

		driverInfo[0] = '\0';
		r = SQLGetInfo(hstmt, SQL_DRIVER_HSTMT, driverInfo, sizeof(driverInfo), NULL);
		XX("SQL_DRIVER_HSTMT result:%d info: %s", r, driverInfo);
		r = SQLGetInfo(hstmt, SQL_DRIVER_HDESC, driverInfo, sizeof(driverInfo), NULL);
		XX("SQL_DRIVER_HDESC result:%d info: %s", r, driverInfo);
		
	}

	int notSupported[200];
	int notSupportedCount = 0;
	SQLCHAR driverInfo[256];
	for (int ty = SQL_INFO_FIRST; ty <= SQL_CONVERT_GUID; ty++) {
		if (ty == SQL_DRIVER_HSTMT || ty == SQL_DRIVER_HDESC) continue;

		int r[2];
		for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
			SQLHDBC henv = _cases[i].ctx.henv;
			SQLHDBC hdbc = _cases[i].ctx.hconn;

			r[i] = SQLGetInfo(hdbc, ty, driverInfo, sizeof(driverInfo), NULL);
			if (i == 1 && (r[0] >= 0 && r[1] < 0)) {
				notSupported[notSupportedCount] = ty;
				notSupportedCount++;
				X("SQLGetInfo type:%d not supported by taos-odbc.", ty);
			}
		}
	}

	char str[1024] = {0};
	for (int i = 0; i < notSupportedCount; i++) {
		snprintf(str + strlen(str), 1024 - strlen(str), " %d", notSupported[i]);
	}

	X("SQLGetInfo type not supported by taos-odbc: %s", str);

	return 0;
}

static int set_conn_attr() {
	int r = 0;
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHANDLE henv = _cases[i].ctx.henv;
		r = SQLSetConnectAttr(_cases[i].ctx.hconn, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)30, SQL_IS_UINTEGER);
		XX("SQLSetConnectAttr SQL_ATTR_CONNECTION_TIMEOUT set 30, result:%d", r);
		if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return r;
	}

	return 0;
}

static int creater_stmt() {
	int r = 0;
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHANDLE henv = _cases[i].ctx.henv;
		SQLHANDLE hconn = _cases[i].ctx.hconn;

		r = SQLAllocHandle(SQL_HANDLE_STMT, hconn, &_cases[i].ctx.hstmt);
		if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
	}
	return 0;
}

static int init_database(char* db) {
	int r = 0;

	char sql[128];
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHANDLE hstmt = _cases[i].ctx.hstmt;
		memset(sql, 0, sizeof(sql));
		strcat(sql, "drop database if exists ");
		strcat(sql, db);
		r = SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
		XX(" %s SQLExecDirect result:%d", sql, r);
		if (FAILED(r)) return -1;

		memset(sql, 0, sizeof(sql));
		strcat(sql, "create database ");
		strcat(sql, db);
		r = SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
		XX("%s SQLExecDirect result:%d", sql, r);
		if (FAILED(r)) return -1;
	}

	return 0;
}

static int show_database() {
	int r = 0;
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHANDLE hconn = _cases[i].ctx.hconn;
		
		SQLCHAR dbName[256];
		SQLGetInfo(hconn, SQL_DATABASE_NAME, dbName, sizeof(dbName), NULL);

		// 打印当前数据库名
		XX("Current database name:%s", dbName);
	}
	return 0;
}

static int show_tables() {
	int r = 0;

	SQLCHAR table_names[2][100][MAX_TABLE_NAME_LEN];
	int table_count[2] = {0, 0};

	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHANDLE hstmt = _cases[i].ctx.hstmt;
		r = SQLTables(hstmt, NULL, 0, NULL, 0, NULL, 0, "TABLE", SQL_NTS);
		XX("SQLTables result:%d", r);
		// 遍历结果集并打印表名
		// SQLCHAR table_name[MAX_TABLE_NAME_LEN];
		while (SQLFetch(hstmt) == SQL_SUCCESS) {
			SQLGetData(hstmt, 3, SQL_C_CHAR, table_names[i][table_count[i]], MAX_TABLE_NAME_LEN, NULL);
			// XX("table name: %s", table_names[i][table_count[i]]);
			table_count[i]++;
		}
		r = SQLCloseCursor(_cases[i].ctx.hstmt);
		XX("SQLCloseCursor result:%d", r);
		r = SQLCloseCursor(_cases[i].ctx.hstmt);
		XX("SQLCloseCursor Second result:%d", r);

	}
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		for (int j = 0; j < table_count[i]; j++) {
			XX("table name: %s", table_names[i][j]);
		}
	}

	return 0;
}

static int create_table(const char* tb){
	int r = 0;
	char buf[1024];
	simple_str_t str = {
	  .base = buf,
	  .cap = sizeof(buf),
	  .nr = 0,
	};

	r = _gen_table_create_sql(&str, tb, fields, sizeof(fields) / sizeof(fields[0]));

	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHANDLE hstmt = _cases[i].ctx.hstmt;
		r = SQLExecDirect(hstmt, (SQLCHAR*)buf, (SQLINTEGER)strlen(buf));
		XX("%s | result:%d", buf, r);
		CHKSTMTR(hstmt, r);
	}

	return 0;
}

static int clear_test() {
	int r = 0;
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		// 当且仅当先释放 sql-odbc驱动资源，后释放taos-odbc资源
		// 且先SQLDisconnect断开连接，后释放hstmt时，taos-odbc 释放stmt会引起程序异常
		// 很奇怪，不确定原因，暂时记录
		r = SQLFreeHandle(SQL_HANDLE_STMT, _cases[i].ctx.hstmt);
		XX("SQLFreeHandle stmt;%p result:%d", _cases[i].ctx.hstmt, r);
		r = SQLDisconnect(_cases[i].ctx.hconn);
		XX("SQLDisconnect result:%d", r);
		r = SQLFreeHandle(SQL_HANDLE_DBC, _cases[i].ctx.hconn);
		XX("SQLFreeHandle hconn:%p result:%d", _cases[i].ctx.hconn, r);
		r = SQLFreeHandle(SQL_HANDLE_ENV, _cases[i].ctx.henv);
		XX("SQLDisconnect henv:%p result:%d", _cases[i].ctx.henv, r);
	}
	return 0;
}

static int convert_test() {
	int r = 0;

	CHK1(init_database, test_db, 0);
	CHK1(re_connect_db, test_db, 0);
	CHK0(show_database, 0);
	CHK0(creater_stmt, 0);
	CHK0(show_tables, 0);
	CHK1(create_table, "tx1", 0);
	CHK0(show_tables, 0);
	return -1;
	for (size_t i = 0; i < sizeof(_cases) / sizeof(_cases[0]); ++i) {
		SQLHANDLE henv = _cases[i].ctx.henv;
		SQLHANDLE hconn = _cases[i].ctx.hconn;
		SQLHANDLE hstmt = _cases[i].ctx.hstmt;

		SQLUINTEGER convert_bigint;
		r = SQLGetInfo(hconn, SQL_CONVERT_BIGINT, &convert_bigint,
			sizeof(convert_bigint), NULL);
		if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
			XX("Failed to retrieve SQL_CONVERT_BIGINT information.");
			return -1;
		}

		SQLLEN param_value;
		SQLINTEGER int_value = 12345;
		XX("Config SQL_CONVERT_BIGINT:%d.", convert_bigint);
		if (convert_bigint != 0) {
			r = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_BIGINT,
				0, 0, &int_value, 0, &param_value);
			if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
				XX("Failed to bind parameter.");
				return -1;
			}
		}
		XX("SQL_CONVERT_BIGINT test finish.");
	}

	return -1;
}

static int config_test() {
	int r = 0;
	CHK0(convert_test, 0);
	return 0;
}


static int run() {
	int r = 0;
	CHK0(init_henv, 0);
	CHK0(browseConnect, 0);
	CHK0(create_hconn, 0);
	CHK0(set_conn_attr_before, 0);
	CHK0(connect_all, 0);
	CHK0(connect_another, 0);
	CHK0(set_conn_attr, 0);
	CHK0(creater_stmt, 0);
	CHK0(getDriverInfo, 0);
	CHK0(config_test, 0);

	return r;
}

int main(void)
{
	int r = 0;

#ifndef _WIN32
	if (1) return 0;
#endif 

	r = run();
	clear_test();
	fprintf(stderr, "==%s==\n", r ? "failure" : "success");

	return !!r;
}

