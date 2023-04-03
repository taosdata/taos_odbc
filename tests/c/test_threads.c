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

#include <stdarg.h>
#include <stdint.h>
#ifdef _WIN32          /* { */
#include <process.h>
#endif                 /* }{ */

#define DUMP(fmt, ...)          printf(fmt "\n", ##__VA_ARGS__)

typedef int (*test_f)(SQLSMALLINT HandleType, SQLHANDLE Handle);

typedef struct sql_s                 sql_t;
struct sql_s {
  const char                *sql;
  uint8_t                    forcibly:1;
};

static int execute_sqls_with_stmt(SQLHANDLE hstmt, const char **sqls, size_t nr)
{
  SQLRETURN sr = SQL_SUCCESS;

  for (size_t i=0; i<nr; ++i) {
    const char *sql = sqls[i];
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;
    CALL_SQLCloseCursor(hstmt);
  }

  return 0;
}

static int _test_threads(SQLSMALLINT HandleType, SQLHANDLE Handle)
{
  // SQLRETURN sr = SQL_SUCCESS;
  int r = 0;
  if (HandleType != SQL_HANDLE_STMT) return 0;

  // DUMP("");
  // DUMP("%s:", __func__);

  SQLHANDLE hstmt = Handle;

  if (1) {
    const char *sqls[] = {
      "select * from information_schema.ins_configs",
    };

    r = execute_sqls_with_stmt(hstmt, sqls, sizeof(sqls)/sizeof(sqls[0]));
    if (r) return -1;
  }

  return 0;
}

#define RECORD(x) {x, #x}

static struct {
  test_f                func;
  const char           *name;
} _tests[] = {
  RECORD(_test_threads),
};

typedef struct arg_s             arg_t;
struct arg_s {
  const char      *dsn;
  const char      *uid;
  const char      *pwd;
  const char      *connstr;
  int              threads;
  int              limit;

  const char      *name;
};

static int _connect(SQLHANDLE hconn, const char *dsn, const char *uid, const char *pwd)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLConnect(hconn, (SQLCHAR*)dsn, SQL_NTS, (SQLCHAR*)uid, SQL_NTS, (SQLCHAR*)pwd, SQL_NTS);
  if (FAILED(sr)) {
    E("connect [dsn:%s,uid:%s,pwd:%s] failed", dsn, uid, pwd);
    return -1;
  }

  return 0;
}

static int _driver_connect(SQLHANDLE hconn, const char *connstr)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLDriverConnect(hconn, NULL, (SQLCHAR*)connstr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
  if (FAILED(sr)) {
    E("driver_connect [connstr:%s] failed", connstr);
    return -1;
  }

  return 0;
}

static int test_with_stmt(const arg_t *arg, SQLHANDLE hstmt)
{
  int r = 0;

  for (size_t i=0; i<sizeof(_tests)/sizeof(_tests[0]); ++i) {
    if (arg->name == NULL || strcmp(arg->name, _tests[i].name)==0) {
      r = _tests[i].func(SQL_HANDLE_STMT, hstmt);
      if (r) return -1;
    }
  }

  return 0;
}

static int execute_sqls(SQLHANDLE hstmt, sql_t *sqls, size_t nr)
{
  SQLRETURN sr = SQL_SUCCESS;

  for (size_t i=0; i<nr; ++i) {
    sql_t *sql = sqls + i;
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql->sql, SQL_NTS);
    CALL_SQLCloseCursor(hstmt);
    if (sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO) continue;
    if (sql->forcibly) return -1;
  }

  return 0;
}

static int test_with_connected_conn(const arg_t *arg, SQLHANDLE hconn, sql_t *sqls, size_t nr)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  for (size_t i=0; i<sizeof(_tests)/sizeof(_tests[0]); ++i) {
    if (arg->name == NULL || strcmp(arg->name, _tests[i].name)==0) {
      r = _tests[i].func(SQL_HANDLE_DBC, hconn);
      if (r) return -1;
    }
  }

  SQLHANDLE hstmt;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  r = execute_sqls(hstmt, sqls, nr);
  if (r == 0) r = test_with_stmt(arg, hstmt);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return r ? -1 : 0;
}

static int test_with_conn(const arg_t *arg, SQLHANDLE hconn, sql_t *sqls, size_t nr)
{
  int r = 0;

  if (arg->connstr) {
    r = _driver_connect(hconn, arg->connstr);
    if (r) return -1;
  } else {
    r = _connect(hconn, arg->dsn, arg->uid, arg->pwd);
    if (r) return -1;
  }

  r = test_with_connected_conn(arg, hconn, sqls, nr);

  CALL_SQLDisconnect(hconn);

  return r ? -1 : 0;
}

static int test_with_env(const arg_t *arg, SQLHANDLE henv, sql_t *sqls, size_t nr)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hconn = SQL_NULL_HANDLE;
  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, henv, &hconn);
  if (FAILED(sr)) return -1;

  r = test_with_conn(arg, hconn, sqls, nr);

  CALL_SQLFreeHandle(SQL_HANDLE_DBC, hconn);

  return r;
}

static int _run_with_arg_in_thread(const arg_t *arg, sql_t *sqls, size_t nr)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE henv  = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  if (FAILED(sr)) return 1;

  do {
    sr = CALL_SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (FAILED(sr)) { r = -1; break; }

    r = test_with_env(arg, henv, sqls, nr);
    if (r) break;
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_ENV, henv);

  return r;
}

static int _tick = 0;
static void _init_tick(void)
{
  A(_tick == 0, "");
  ++_tick;
}

static void _init_pthread_once(void)
{
  static pthread_once_t _once = PTHREAD_ONCE_INIT;
  pthread_once(&_once, _init_tick);
}

static int (*get_nr_load)(void);
static void *taos_odbc_so;

static int _thread_routine(const arg_t *arg)
{
  int r = 0;
  _init_pthread_once();
  _init_pthread_once();
  W("%d:%x:thread:running", GetProcessId(GetCurrentProcess()), GetCurrentThreadId());
  if (0) get_nr_load();
  else   r = _run_with_arg_in_thread((const arg_t*)arg, NULL, 0);
  W("%d:%x:thread:return:%d", GetProcessId(GetCurrentProcess()), GetCurrentThreadId(), r);
  if (r) return -1;
  return 0;
}

#ifdef _WIN32                /* { */
#if 1            /* { */
static DWORD _routine_win(LPVOID arg)
{
  int r = 0;
  r = _thread_routine((const arg_t*)arg);
  if (r) return -1;
  return 0;
}

static int _run_with_arg_once(const arg_t *arg)
{
  HANDLE threads[16]   = {0};
  DWORD dwThreadIds[16] = {0};
  DWORD exitCodes[16]   = {0};

  int r = 0;
  size_t i = 0;
  size_t nr = 0;

  r = 0;
  i = 0;
  nr = 0;

  size_t count = arg->threads;
  for (i=0; i<count && i < sizeof(threads)/sizeof(threads[0]); ++i) {
    threads[i] = CreateThread(NULL, 0, _routine_win, (LPVOID)arg, CREATE_SUSPENDED, dwThreadIds + i);
    W("%d:%x:thread:create", GetProcessId(GetCurrentProcess()), GetThreadId(threads[i]));
    if (!threads[i]) {
      E("CreateThread failed");
      r = -1;
      break;
    }
    ResumeThread(threads[i]);
    ++nr;
  }

  // WaitForMultipleObjects((DWORD)nr, threads, TRUE, INFINITE);
  for (size_t i=0; i<nr; ++i) {
    DWORD dw = WaitForSingleObject(threads[i], INFINITE);
    W("%d:%x:thread:join:%d", GetProcessId(GetCurrentProcess()), GetThreadId(threads[i]), dw);
    GetExitCodeThread(threads[i], exitCodes + i);
    if (exitCodes[i]) r = -1;
    CloseHandle(threads[i]);
  }

  return r;
}
#else            /* }{ */
static unsigned __stdcall _start_address_for_beginthreadex(void *arg)
{
  int r = 0;
  r = _thread_routine((const arg_t*)arg);
  if (r) return -1;
  return 0;
}

static int _run_with_arg_once(const arg_t *arg)
{
  while (0) {
    int r = _run_with_arg_in_thread(arg);
    if (r) return -1;
  }
  HANDLE threads[16]   = {0};
  unsigned threadIds[16] = {0};
  DWORD exitCodes[16]   = {0};

  int r = 0;
  size_t nr = 0;

  r = 0;
  nr = 0;

  for (size_t i=0; i<16; ++i) {
    threads[i] = (HANDLE)_beginthreadex(NULL, 0, _start_address_for_beginthreadex, (void*)arg, CREATE_SUSPENDED, threadIds + i);
    E("%d:%x:thread:create", GetProcessId(GetCurrentProcess()), GetThreadId(threads[i]));
    if (!threads[i]) {
      E("_beginthreadex failed");
      r = -1;
      break;
    }
    ResumeThread(threads[i]);
    ++nr;
  }

  for (size_t i=0; i<nr; ++i) {
    DWORD dw = WaitForSingleObject(threads[i], INFINITE);
    E("%d:%x:thread:join:%d", GetProcessId(GetCurrentProcess()), GetThreadId(threads[i]), dw);
    GetExitCodeThread(threads[i], exitCodes + i);
    if (exitCodes[i]) r = -1;
    CloseHandle(threads[i]);
  }

  return r;
}
#endif           /* } */
#else                        /* }{ */
#include <pthread.h>
static void* _start_routine(void *arg)
{
  int r = 0;
  r = _thread_routine((const arg_t*)arg);
  if (r) return -1;
  return 0;
}

static int _run_with_arg_once(const arg_t *arg)
{
  pthread_t threads[16]     = {0};

  int r = 0;
  size_t i = 0;
  size_t nr = 0;

  r = 0;
  i = 0;
  nr = 0;

  for (i=0; i<16; ++i) {
    r = pthread_create(threads + i, NULL, _start_routine, (void*)arg);
    if (r) {
      E("pthread_create failed");
      r = -1;
      break;
    }
    ++nr;
  }

  for (size_t i=0; i<nr; ++i) {
    void *p = NULL;
    pthread_join(threads[i], &p);
    if (p) r = -1;
  }

  return r;
}
#endif                       /* } */

static void usage(const char *arg0)
{
  DUMP("usage:");
  DUMP("  %s -h", arg0);
  DUMP("  %s --dsn <DSN> [--uid <UID>] [--pwd <PWD> [name]", arg0);
  DUMP("  %s --connstr <connstr> [name]", arg0);
  DUMP("");
  DUMP("supported test names:");
  for (size_t i=0; i<sizeof(_tests)/sizeof(_tests[0]); ++i) {
    DUMP("  %s", _tests[i].name);
  }
}

static int _run_with_arg(const arg_t *arg, sql_t *sqls, size_t nr)
{
  int r = 0;
  r = _run_with_arg_in_thread(arg, sqls, nr);
  if (r) return -1;

  int tick = 0;

again:

  if (tick++ < arg->limit) {
    r = _run_with_arg_once(arg);
    if (r) return -1;
    goto again;
  }

  return 0;
}

static int _run(int argc, char *argv[])
{
  int r = 0;
  arg_t arg = {0};
  arg.dsn = "TAOS_ODBC_DSN";
  arg.threads = 8;
  arg.limit = 2;

  sql_t sqls[] = {
    { "drop topic if exists demo", 0},
    { "drop topic if exists good", 0},
    { "drop database if exists foobar", 1},
    { "create database if not exists foobar", 1},
    { "create table foobar.demo (ts timestamp, name varchar(20))", 1},
    { "create topic demo as select * from foobar.demo", 1},
    { "create table foobar.good (ts timestamp, i32 int, name varchar(20))", 1},
    { "create topic good as select * from foobar.good", 1},
    { "insert into foobar.demo (ts, name) values (now(), 'hello')", 1},
  };

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-h") == 0) {
      usage(argv[0]);
      return 0;
    }
    if (strcmp(argv[i], "--dsn") == 0) {
      ++i;
      if (i>=argc) break;
      arg.dsn = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--uid") == 0) {
      ++i;
      if (i>=argc) break;
      arg.uid = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--pwd") == 0) {
      ++i;
      if (i>=argc) break;
      arg.pwd = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--connstr") == 0) {
      ++i;
      if (i>=argc) break;
      arg.connstr = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--threads") == 0) {
      ++i;
      if (i>=argc) break;
      arg.threads = atoi(argv[i]);
      continue;
    }
    if (strcmp(argv[i], "--limit") == 0) {
      ++i;
      if (i>=argc) break;
      arg.limit = atoi(argv[i]);
      continue;
    }

    arg.name = argv[i];
    r = _run_with_arg(&arg, sqls, sizeof(sqls)/sizeof(sqls[0]));
    if (r) return -1;
  }

  if (arg.name) return 0;

  return _run_with_arg(&arg, sqls, sizeof(sqls)/sizeof(sqls[0]));
}

int main(int argc, char *argv[])
{
  int r = 0;

  r = _run(argc, argv);

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");

  return !!r;
}
