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

#define DUMP(fmt, ...) fprintf(stderr, fmt "\n", ##__VA_ARGS__)

static int run_with_stmt(odbc_case_t *odbc_case, odbc_handles_t *handles)
{
  int r = 0;

  r = odbc_case->run(odbc_case, ODBC_STMT, handles);

  return r ? -1 : 0;
}

static int run_with_connected_conn(odbc_case_t *odbc_case, odbc_handles_t *handles)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  char buf[1024]; buf[0] = '\0';

  sr = CALL_SQLGetInfo(handles->hconn, SQL_DRIVER_NAME, buf, sizeof(buf), NULL);
  if (SUCCEEDED(sr)) {
    DUMP("DRIVER_NAME:%s", buf);
  }

  sr = CALL_SQLGetInfo(handles->hconn, SQL_DRIVER_VER, buf, sizeof(buf), NULL);
  if (SUCCEEDED(sr)) {
    DUMP("DRIVER_VER:%s", buf);
  }

  sr = CALL_SQLGetInfo(handles->hconn, SQL_DRIVER_ODBC_VER, buf, sizeof(buf), NULL);
  if (SUCCEEDED(sr)) {
    DUMP("DRIVER_ODBC_VER:%s", buf);
  }

  sr = CALL_SQLGetInfo(handles->hconn, SQL_DBMS_NAME, buf, sizeof(buf), NULL);
  if (SUCCEEDED(sr)) {
    DUMP("DBMS_NAME:%s", buf);
  }

  sr = CALL_SQLGetInfo(handles->hconn, SQL_DBMS_VER, buf, sizeof(buf), NULL);
  if (SUCCEEDED(sr)) {
    DUMP("DBMS_VER:%s", buf);
  }

  r = odbc_case->run(odbc_case, ODBC_CONNECTED, handles);
  if (r) return -1;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, handles->hconn, &handles->hstmt);
  if (FAILED(sr)) return -1;

  r = run_with_stmt(odbc_case, handles);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, handles->hstmt);
  handles->hstmt = SQL_NULL_HANDLE;

  return r ? -1 : 0;
}

static int run_with_conn(odbc_case_t *odbc_case, odbc_handles_t *handles)
{
  int r = 0;

  r = odbc_case->run(odbc_case, ODBC_DBC, handles);
  if (r) return -1;

  r = run_with_connected_conn(odbc_case, handles);

  CALL_SQLDisconnect(handles->hconn);

  return r ? -1 : 0;
}

static int run_with_env(odbc_case_t *odbc_case, odbc_handles_t *handles)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, handles->henv, &handles->hconn);
  if (FAILED(sr)) return -1;

  do {
    r = odbc_case->run(odbc_case, ODBC_ENV, handles);
    if (r) break;

    r = run_with_conn(odbc_case, handles);
    if (r) break;
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_DBC, handles->hconn);
  handles->hconn = SQL_NULL_HANDLE;

  return r ? -1 : 0;
}

static int run_odbc_case(odbc_case_t *odbc_case, odbc_handles_t *handles)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  r = odbc_case->run(odbc_case, ODBC_INITED, handles);
  if (r) return -1;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &handles->henv);
  if (FAILED(sr)) return -1;

  do {
    sr = CALL_SQLSetEnvAttr(handles->henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (FAILED(sr)) { r = -1; break; }

    r = run_with_env(odbc_case, handles);
    if (r) break;
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_ENV, handles->henv);
  handles->henv = SQL_NULL_HANDLE;

  return r ? -1 : 0;
}

int run_odbc_cases(odbc_case_t *cases, size_t cases_nr)
{
  int r = 0;

  for (size_t i=0; i<cases_nr; ++i) {
    odbc_case_t *odbc_case = cases + i;
    odbc_handles_t handles = {
      .henv          = SQL_NULL_HANDLE,
      .hconn         = SQL_NULL_HANDLE,
      .hstmt         = SQL_NULL_HANDLE,
    };
    r = run_odbc_case(odbc_case, &handles);
    if (r) return -1;
  }

  return 0;
}

