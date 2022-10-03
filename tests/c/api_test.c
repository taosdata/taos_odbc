#include "../helpers.h"

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
#include <taos.h>


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
static int _sql_stmt_get_long_data_by_col(SQLHANDLE stmth, SQLSMALLINT ColumnNumber)
{
  char buf[1024];
  buf[0] = '\0';

  char *p = buf;

  SQLHANDLE hstmt = stmth;

  SQLUSMALLINT   Col_or_Param_Num    = ColumnNumber;
  SQLSMALLINT    TargetType          = SQL_C_CHAR;
  SQLPOINTER     TargetValuePtr      = (SQLPOINTER)p;
  SQLLEN         BufferLength        = 2;
  SQLLEN         StrLen_or_Ind;

  while (1) {
    SQLRETURN r = CALL_STMT(SQLGetData(stmth, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, &StrLen_or_Ind));
    if (r == SQL_NO_DATA) break;
    if (SUCCEEDED(r)) {
      if (StrLen_or_Ind == SQL_NULL_DATA) {
        D("Column[#%d]: [[null]]", ColumnNumber);
        return 0;
      }
      p += strlen(p);
      TargetValuePtr = p;
      continue;
    }
    A(0, "");
    return -1;
  }

  D("Column[#%d]: [%s]", ColumnNumber, buf);

  return 0;
}

__attribute__((unused))
static int _sql_stmt_get_long_data(SQLHANDLE stmth, SQLSMALLINT ColumnCount)
{
  int i = 1;
  for (i=1; i<=ColumnCount; ++i) {
    if (_sql_stmt_get_long_data_by_col(stmth, i)) break;
  }

  if (i <= ColumnCount) return -1;

  return 0;
}

__attribute__((unused))
static int _sql_stmt_get_data(SQLHANDLE stmth, SQLSMALLINT ColumnCount)
{
  SQLHANDLE hstmt = stmth;

  char buf[1024];
  int i = 1;
  for (i=1; i<=ColumnCount; ++i) {
    SQLUSMALLINT   Col_or_Param_Num    = i;
    SQLSMALLINT    TargetType          = SQL_C_CHAR;
    SQLPOINTER     TargetValuePtr      = (SQLPOINTER)buf;
    SQLLEN         BufferLength        = sizeof(buf);
    SQLLEN         StrLen_or_Ind;

    buf[0] = '\0';
    SQLRETURN r = CALL_STMT(SQLGetData(stmth, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, &StrLen_or_Ind));
    if (r != SQL_SUCCESS) break;
    if (StrLen_or_Ind == SQL_NULL_DATA) {
      D("Column[#%d]: [[null]]", i);
      continue;
    }

    D("Column[#%d]: [%s]", i, buf);

    r = CALL_STMT(SQLGetData(stmth, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, &StrLen_or_Ind));
    if (r != SQL_NO_DATA) break;

    r = CALL_STMT(SQLGetData(stmth, Col_or_Param_Num, TargetType, TargetValuePtr, BufferLength, &StrLen_or_Ind));
    if (r != SQL_NO_DATA) break;
  }

  if (i <= ColumnCount) return -1;

  return 0;
}

__attribute__((unused))
static int test_sql_stmt_execute_direct(SQLHANDLE stmth, const char *statement)
{
  SQLHANDLE hstmt = stmth;

  SQLRETURN r;
  SQLSMALLINT ColumnCount;
  int rr;

  r = CALL_STMT(SQLExecDirect(stmth, (SQLCHAR*)statement, strlen(statement)));
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  r = CALL_STMT(SQLNumResultCols(stmth, &ColumnCount));
  if (FAILED(r)) return -1;

  r = CALL_STMT(SQLFetch(stmth));
  if (r == SQL_NO_DATA) return 0;

  rr = _sql_stmt_get_data(stmth, ColumnCount);
  if (rr == 0) {
    if (ColumnCount > 1) {
      rr = _sql_stmt_get_data(stmth, ColumnCount);
    } else {
      rr = _sql_stmt_get_data(stmth, ColumnCount);
      rr = !rr;
    }
  }

  CALL_STMT(SQLCloseCursor(stmth));

  if (rr) return -1;

  r = CALL_STMT(SQLExecDirect(stmth, (SQLCHAR*)statement, strlen(statement)));
  if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;

  r = CALL_STMT(SQLNumResultCols(stmth, &ColumnCount));
  if (FAILED(r)) return -1;

  r = CALL_STMT(SQLFetch(stmth));
  if (r == SQL_NO_DATA) return 0;

  rr = _sql_stmt_get_long_data(stmth, ColumnCount);
  if (rr == 0) {
    if (ColumnCount > 1) {
      rr = _sql_stmt_get_long_data(stmth, ColumnCount);
    } else {
      rr = _sql_stmt_get_long_data(stmth, ColumnCount);
    }
  }

  CALL_STMT(SQLCloseCursor(stmth));

  return rr ? -1 : 0;
}

__attribute__((unused))
static int do_sql_stmt_execute_direct(SQLHANDLE stmth)
{
  CHK2(test_sql_stmt_execute_direct, stmth, "xshow databases", -1);
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

  CHK2(test_sql_stmt_execute_direct, stmth, "drop table if exists t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "insert into t (ts, name, age, sex, text) values (1662861448752, 'name1', 20, 'male', '中国人')", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select * from t", 0);

  CHK2(test_sql_stmt_execute_direct, stmth, "drop table if exists t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "insert into t (ts, name, age, sex, text) values (1662861449753, 'name2', 30, 'female', '苏州人')", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select * from t", 0);

  CHK2(test_sql_stmt_execute_direct, stmth, "drop table if exists t", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "insert into t (ts, name, age, sex, text) values (1662861450754, 'name3', null, null, null)", 0);
  CHK2(test_sql_stmt_execute_direct, stmth, "select * from t", 0);

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

  // SQLRETURN r;
  // SQL_TIMESTAMP_STRUCT ts = {};
  // SQLLEN cbOrderDate;

  // CALL_ODBC(r, SQL_HANDLE_STMT, stmth, SQLBindParameter(stmth, 1, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP, sizeof(ts), 0, &ts, 0, &cbOrderDate));
  // D("input: ipar: %d", 1);
  // D("input: fParamType: %d", SQL_PARAM_INPUT);
  // D("input: fCType: %d", SQL_C_TYPE_TIMESTAMP);
  // D("input: fSqlType: %d", SQL_TYPE_TIMESTAMP);
  // D("input: cbColDef: %ld", sizeof(ts));
  // D("input: ibScale: %d", 0);
  // D("input: rgbValue: %p", &ts);
  // D("input: cbValueMax: %d", 0);
  // D("input: pcbValue: %p", &cbOrderDate);
  // if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) return -1;
  // if (r != SQL_SUCCESS) return -1;

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
static int test_sql_conn(SQLHANDLE connh, const char *dsn, const char *uid, const char *pwd)
{
  SQLRETURN r;

  CALL_ODBC(r, SQL_HANDLE_DBC, connh, SQLConnect(connh, (SQLCHAR*)dsn, SQL_NTS, (SQLCHAR*)uid, SQL_NTS, (SQLCHAR*)pwd, SQL_NTS));
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
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", NULL, NULL, 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "root", "taosdata", 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "root", NULL, 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", NULL, "taosdata", 0);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "root", "", -1);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "", "taosdata", -1);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "", "", -1);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", "", NULL, -1);
  CHK4(test_sql_conn, connh, "TAOS_ODBC_DSN", NULL, "", -1);
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

