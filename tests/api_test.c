// #include "taos.h"

#include <assert.h>
#include <dlfcn.h>
#include <errno.h>
#include <libgen.h>
#include <sql.h>
#include <sqlext.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


#define LOG(_file, _line, _func, _fmt, ...)                   \
  fprintf(stderr, "%s[%d]:%s: " _fmt "\n",                    \
      basename((char*)_file), _line, _func,                   \
      ##__VA_ARGS__)

#define D(_fmt, ...) LOG(__FILE__, __LINE__, __func__, _fmt, ##__VA_ARGS__)

#define A(_statement, _fmt, ...)                 \
  do {                                           \
    if (!(_statement)) {                         \
      LOG(__FILE__, __LINE__, __func__,          \
          "assertion failed: [%s] " _fmt "\n",   \
                      #_statement,               \
                      ##__VA_ARGS__);            \
      abort();                                   \
    }                                            \
  } while (0)

#define TEST_CASE_BEG(_func)                                \
  D("test case: %s...", _func)

#define TEST_CASE_END(_func, _exp)                          \
  if (MATCH(r, _exp)) {                                     \
    D("test case: %s --> %s%s%s",                           \
                    _func,                                  \
                    color_green(),                          \
                    "succeeded",                            \
                    color_reset());                         \
  } else if (r) {                                           \
    D("test case: %s --> %s%s%s",                           \
                    _func,                                  \
                    color_red(),                            \
                    "failed",                               \
                    color_reset());                         \
    return -1;                                              \
  } else {                                                  \
    D("test case: %s --> %s%s%s",                           \
                    _func,                                  \
                    color_red(),                            \
                    "failure expected but succeeded",       \
                    color_reset());                         \
    return -1;                                              \
  }

#define CHK0(_func, _exp)                                                                          \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_exp ")");                                                           \
    r = _func();                                                                                   \
    TEST_CASE_END(#_func "(" #_exp ")", _exp);                                                     \
  }

#define CHK1(_func, _arg1, _exp)                                                                   \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_arg1 "," #_exp ")");                                                \
    r = _func(_arg1);                                                                              \
    TEST_CASE_END(#_func "(" #_arg1 "," #_exp ")", _exp);                                          \
  }

#define CHK2(_func, _v1, _v2, _exp)                                                                \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_v1 "," #_v2 "," #_exp ")");                                         \
    r = _func(_v1, _v2);                                                                           \
    TEST_CASE_END(#_func "(" #_v1 "," #_v2 "," #_exp ")", _exp);                                   \
  }

#define CHK3(_func, _v1, _v2, _v3, _exp)                                                           \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_v1 "," #_v2 "," #_v3 "," #_exp ")");                                \
    r = _func(_v1, _v2, _v3);                                                                      \
    TEST_CASE_END(#_func "(" #_v1 "," #_v2 "," #_v3 "," #_exp ")", _exp);                          \
  }

#define CHK4(_func, _v1, _v2, _v3, _v4, _exp)                                                      \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_v1 "," #_v2 "," #_v3 "," #_v4 "," #_exp ")");                       \
    r = _func(_v1, _v2, _v3, _v4);                                                                 \
    TEST_CASE_END(#_func "(" #_v1 "," #_v2 "," #_v3 "," #_v4 "," #_exp ")", _exp);                 \
  }

static inline const char* color_red(void)
{
  return "\033[1;31m";
}

static inline const char* color_green(void)
{
  return "\033[1;32m";
}

static inline const char* color_reset(void)
{
  return "\033[0m";
}

static void _diagnostic(
  SQLSMALLINT handleType, SQLHANDLE handle,
  const char *file, int line, const char *func)
{
  SQLRETURN _r;
  SQLSMALLINT _RecNumber = 0;
  SQLCHAR _SQLState[10];
  SQLINTEGER _NativeError;
  SQLCHAR _MessageText[1024];
  SQLSMALLINT _BufferLength = sizeof(_MessageText);
  SQLSMALLINT _TextLength;

  while (1) {
    _NativeError = 0;
    _SQLState[0] = '\0';
    _MessageText[0] = '\0';
    _TextLength = 0;
    ++_RecNumber;
    _r = SQLGetDiagRec(handleType, handle, _RecNumber, _SQLState, &_NativeError, _MessageText, _BufferLength, &_TextLength);
    if (_r == SQL_NO_DATA) break;
    if (_r == SQL_SUCCESS || _r == SQL_SUCCESS_WITH_INFO) {
      LOG(file, line, func, "RecNumber: %d", _RecNumber);
      LOG(file, line, func, "SQLState: %s", _SQLState);
      LOG(file, line, func, "NativeError: %d", _NativeError);
      LOG(file, line, func, "MessageText: %s", _MessageText);
      LOG(file, line, func, "TextLength: %d", _TextLength);
      continue;
    }

    assert(_r != SQL_INVALID_HANDLE);
    assert(_r != SQL_ERROR);
    assert(0);
    break;
  }
}

#define CALL_ODBC(_r, _handleType, _handle, _fc)                                      \
  do {                                                                                \
    const char *_s;                                                                   \
    _r = _fc;                                                                         \
    if (_r == SQL_SUCCESS) break;                                                     \
    if (_r == SQL_ERROR) {                                                            \
      _s = "failed";                                                                  \
    } else if (_r == SQL_SUCCESS_WITH_INFO) {                                         \
      _s = "succeeded with info";                                                     \
    } else if (_r == SQL_NO_DATA) {                                                   \
      _s = "no data returned";                                                        \
    } else {                                                                          \
      D("%s: unknown result", #_fc);                                                  \
    }                                                                                 \
    D("%s: %s", #_fc, _s);                                                            \
    _diagnostic(_handleType, _handle, __FILE__, __LINE__, __func__);                  \
  } while (0)

#define MATCH(a, b)  (!!(a) == !!(b))

__attribute__((unused))
static int test_ok(void)
{
  return 0;
}

__attribute__((unused))
static int test_failure(void)
{
  return -1;
}

__attribute__((unused))
static int test_so(const char *so)
{
  void *h1 = dlopen(so, RTLD_NOW);
  if (h1 == NULL) {
    D("%s", dlerror());
    return -1;
  }

  int (*get_nr_load)(void);

  int r = -1;

  get_nr_load = dlsym(h1, "get_nr_load");
  if (!get_nr_load) {
    D("`get_nr_load` not exported in [%s]", so);
    goto fail_dlsym;
  }

  int nr_load = get_nr_load();
  if (nr_load != 1) {
    D("internal logic error in [%s]", so);
    goto fail_get_nr_load;
  }

  r = 0;

fail_get_nr_load:
fail_dlsym:
  dlclose(h1);

  return r ? -1 : 0;
}

__attribute__((unused))
static int test_sql_stmt_execute_direct(SQLHANDLE stmth, const char *statement)
{
    SQLRETURN r;
    CALL_ODBC(r, SQL_HANDLE_STMT, stmth,  SQLExecDirect(stmth, (SQLCHAR*)statement, strlen(statement)));
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

    return 0;
}

__attribute__((unused))
static int do_sql_stmt_execute_direct(SQLHANDLE stmth)
{
  CHK2(test_sql_stmt_execute_direct, stmth, "show databases", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "drop database if exists bar", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create database bar", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "use bar", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "show tables", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "drop table if exists t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create table t (ts timestamp, v int)", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select * from t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select count(*) from t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "insert into t (ts, v) values (now, 123)", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select * from t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select count(*) from t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "drop stable if exists s", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create stable s (ts timestamp, v int) tags (id int, name nchar(10))", 0);

  return 0;
}

__attribute__((unused))
static int do_sql_stmt_execute_direct_prepare(SQLHANDLE stmth)
{
  CHK2(test_sql_stmt_execute_direct, stmth, "create database if not exists bar", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "use bar", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create table if not exists t (ts timestamp, v int)", 0);

  SQLRETURN r;
  SQL_TIMESTAMP_STRUCT ts = {};
  SQLLEN cbOrderDate;

  CALL_ODBC(r, SQL_HANDLE_STMT, stmth, SQLBindParameter(stmth, 1, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP, sizeof(ts), 0, &ts, 0, &cbOrderDate));
  D("input: ipar: %d", 1);
  D("input: fParamType: %d", SQL_PARAM_INPUT);
  D("input: fCType: %d", SQL_C_TYPE_TIMESTAMP);
  D("input: fSqlType: %d", SQL_TYPE_TIMESTAMP);
  D("input: cbColDef: %ld", sizeof(ts));
  D("input: ibScale: %d", 0);
  D("input: rgbValue: %p", &ts);
  D("input: cbValueMax: %d", 0);
  D("input: pcbValue: %p", &cbOrderDate);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  if (r != SQL_SUCCESS) return -1;

  return 0;
}

__attribute__((unused))
static int do_sql_stmt(SQLHANDLE connh)
{
  SQLRETURN r;
  SQLHANDLE stmth;

  CALL_ODBC(r, SQL_HANDLE_DBC, connh, SQLAllocHandle(SQL_HANDLE_STMT, connh, &stmth));
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  do {
    r = do_sql_stmt_execute_direct(stmth);
    if (r) break;

    r = do_sql_stmt_execute_direct_prepare(stmth);
  } while (0);

  SQLFreeHandle(SQL_HANDLE_STMT, stmth);

  return r ? -1 : 0;
}

__attribute__((unused))
static int do_conn_get_info(SQLHANDLE connh)
{
  SQLRETURN  r;
  SQLSMALLINT     StringLength;

  char buf[1024];

  CALL_ODBC(r, SQL_HANDLE_DBC, connh, SQLGetInfo(connh, SQL_DRIVER_NAME, buf, sizeof(buf), &StringLength));
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  D("SQL_DRIVER_NAME: %s", buf);

  CALL_ODBC(r, SQL_HANDLE_DBC, connh, SQLGetInfo(connh, SQL_DBMS_NAME, buf, sizeof(buf), &StringLength));
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  D("SQL_DBMS_NAME: %s", buf);

  CALL_ODBC(r, SQL_HANDLE_DBC, connh, SQLGetInfo(connh, SQL_DM_VER, buf, sizeof(buf), &StringLength));
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  D("SQL_DM_VER: %s", buf);

  return 0;
}

__attribute__((unused))
static int test_sql_driver_conn(SQLHANDLE connh, const char *conn_str)
{
  SQLRETURN r;
  SQLHWND WindowHandle = NULL;
  SQLCHAR *InConnectionString = (SQLCHAR*)conn_str;
  SQLSMALLINT StringLength1 = strlen(conn_str);
  SQLCHAR OutConnectionString[1024];
  SQLSMALLINT BufferLength = sizeof(OutConnectionString);
  SQLSMALLINT StringLength2 = 0;
  SQLUSMALLINT DriverCompletion = SQL_DRIVER_NOPROMPT;

  OutConnectionString[0] = '\0';

  CALL_ODBC(r, SQL_HANDLE_DBC, connh, SQLDriverConnect(connh, WindowHandle, InConnectionString, StringLength1, OutConnectionString, BufferLength, &StringLength2, DriverCompletion));
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  do {
// #define LOOPING
#ifdef LOOPING        /* { */
again:
#endif                /* } */
    r = do_conn_get_info(connh);
#ifdef LOOPING        /* { */
    if (r == 0) goto again;
#endif                /* } */

    if (r) break;

// #define LOOPING
#ifdef LOOPING        /* { */
again:
#endif                /* } */
    r = do_sql_stmt(connh);
#ifdef LOOPING        /* { */
    if (r == 0) goto again;
#endif                /* } */
  } while (0);

  SQLDisconnect(connh);

  return r ? -1 : 0;
}

__attribute__((unused))
static int do_sql_driver_conns(SQLHANDLE connh)
{
  CHK2(test_sql_driver_conn, connh, "bad", -1);
  CHK2(test_sql_driver_conn, connh, "DSN=NOT_EXIST", -1);
  CHK2(test_sql_driver_conn, connh, "Driver={TAOS_ODBC_DRIVER};Server=127.0.0.1:6030", 0);
  CHK2(test_sql_driver_conn, connh, "DSN=TAOS_ODBC_DSN", 0);

  return 0;
}

__attribute__((unused))
static int do_sql_alloc_conn(SQLHANDLE envh)
{
  SQLRETURN r;
  SQLHANDLE connh;

  CALL_ODBC(r, SQL_HANDLE_ENV, envh, SQLAllocHandle(SQL_HANDLE_DBC, envh, &connh));
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

// #define LOOPING
#ifdef LOOPING        /* { */
again:
#endif                /* } */
  r = do_sql_driver_conns(connh);
#ifdef LOOPING        /* { */
  if (r == 0) goto again;
#endif                /* } */

  SQLFreeHandle(SQL_HANDLE_DBC, connh);
  return r ? -1 : 0;
}

__attribute__((unused))
static int test_sql_alloc_env(void)
{
  SQLRETURN r;
  SQLHANDLE envh;

  r = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &envh);
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
    D("failure in SQLAllocHandle(SQL_HANDLE_ENV)");
    return -1;
  }

  CALL_ODBC(r, SQL_HANDLE_ENV, envh, SQLSetEnvAttr(envh, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0));
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

// #define LOOPING
#ifdef LOOPING        /* { */
again:
#endif                /* } */
  r = do_sql_alloc_conn(envh);
#ifdef LOOPING        /* { */
  if (r==0) goto again;
#endif                /* } */

  SQLFreeHandle(SQL_HANDLE_ENV, envh);

  return r ? -1 : 0;
}

static int do_cases(void)
{
  CHK0(test_ok, 0);
  CHK0(test_failure, -1);
  CHK1(test_so, "/tmp/not_exists.so", -1);
  CHK1(test_so, "libtaos_odbc.so", 0);
  CHK0(test_sql_alloc_env, 0);

  return 0;
}

int main(void)
{
  int r = 0;

// #define CHK_LEAK
#ifdef CHK_LEAK                /* { */
again:
#endif                          /* } */
  r = do_cases();
#ifdef CHK_LEAK                /* { */
  if (r == 0) goto again;
#endif                          /* } */

  return r ? 1 : 0;
}

