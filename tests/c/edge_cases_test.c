#include "../helpers.h"

#include <stdlib.h>
#include <time.h>

#define CHECK(_statement) do {           \
  if (_statement) {                      \
    D("failed: [%s]", #_statement);      \
    return 1;                            \
  }                                      \
} while (0)

static int create_connection(SQLHANDLE *penv, SQLHANDLE *pdbc, const char *conn_str, const char *dsn, const char *uid, const char *pwd)
{
  SQLRETURN sr;
  SQLHANDLE henv, hdbc;

  sr = CALL(SQL_HANDLE_ENV, henv, SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv));
  if (henv == SQL_NULL_HANDLE) goto fail_henv;

  sr = CALL_ENV(SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0));
  if (FAILED(sr)) goto fail_odbc_version;

  sr = CALL_ENV(SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc));
  if (hdbc == SQL_NULL_HANDLE) goto fail_hdbc;

  if (!conn_str) {
    sr = CALL_DBC(SQLConnect(hdbc, (SQLCHAR*)dsn, SQL_NTS, (SQLCHAR*)uid, SQL_NTS, (SQLCHAR*)pwd, SQL_NTS));
    if (FAILED(sr)) goto fail_connect;
  } else {
     SQLSMALLINT StringLength2 = 0;
     char buf[1024];
     buf[0] = '\0';
     sr = CALL_DBC(SQLDriverConnect(hdbc, 0, (SQLCHAR*)conn_str, SQL_NTS, (SQLCHAR*)buf, sizeof(buf), &StringLength2, SQL_DRIVER_NOPROMPT));
     D("driver completed connection string: [%s]", buf);
     if (FAILED(sr)) goto fail_connect;
  }

  *penv = henv;
  *pdbc = hdbc;

  return 0;

fail_connect:
  SQLFreeHandle(SQL_HANDLE_DBC, hdbc);

fail_hdbc:
fail_odbc_version:
  SQLFreeHandle(SQL_HANDLE_ENV, henv);

fail_henv:
  return -1;
}

static int create_statement(SQLHANDLE *pstmt, SQLHANDLE hdbc)
{
  SQLRETURN sr;
  SQLHANDLE hstmt;

  sr = CALL_DBC(SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt));
  if (FAILED(sr)) goto fail_hstmt;

  *pstmt = hstmt;
  return 0;

fail_hstmt:
  return -1;
}

static int test_connect(const char *conn_str, const char *dsn, const char *uid, const char *pwd)
{
  SQLRETURN sr;
  SQLHANDLE henv, hdbc;

  int r = create_connection(&henv, &hdbc, conn_str, dsn, uid, pwd);
  if (r) return -1;

  sr = CALL_DBC(SQLDisconnect(hdbc));
  if (FAILED(sr)) r = -1;

  SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
  SQLFreeHandle(SQL_HANDLE_ENV, henv);

  return r;
}

static int test_direct_exec(SQLHANDLE hstmt, const char *sql)
{
  SQLRETURN sr;

  sr = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS));
  if (FAILED(sr)) return -1;

  if (rand() % 2) {
    sr = CALL_STMT(SQLFreeStmt(hstmt, SQL_CLOSE));
    if (FAILED(sr)) return -1;
  }

  return 0;
}

static int test_prepare_test_data(SQLHANDLE hstmt)
{
  CHECK(test_direct_exec(hstmt, "drop database if exists foo"));
  CHECK(test_direct_exec(hstmt, "create database if not exists foo"));
  CHECK(test_direct_exec(hstmt, "use foo"));
  CHECK(test_direct_exec(hstmt, "drop table if exists t"));
  CHECK(test_direct_exec(hstmt, "create table if not exists t (ts timestamp, name varchar(10))"));
  CHECK(test_direct_exec(hstmt, "drop table if exists t"));
  CHECK(test_direct_exec(hstmt, "create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))"));

  return 0;
}

static int test_direct_executes(SQLHANDLE hstmt)
{
  CHECK(test_direct_exec(hstmt, "show databases"));
  CHECK(test_direct_exec(hstmt, "use foo"));
  CHECK(test_direct_exec(hstmt, "insert into t (ts, name, age, sex, text) values (1662861448752, 'name1', 20, 'male', '中国人')"));
  CHECK(test_direct_exec(hstmt, "insert into t (ts, name, age, sex, text) values (1662861449753, 'name2', 30, 'female', '苏州人')"));
  CHECK(test_direct_exec(hstmt, "insert into t (ts, name, age, sex, text) values (1662861450754, 'name3', null, null, null)"));
  CHECK(test_direct_exec(hstmt, "select * from t"));

  return 0;
}

static int test_execute(SQLHANDLE hstmt, const char *sql)
{
  SQLRETURN sr;

  sr = CALL_STMT(SQLPrepare(hstmt, (SQLCHAR*)sql, SQL_NTS));
  if (FAILED(sr)) return -1;

  sr = CALL_STMT(SQLExecute(hstmt));
  if (FAILED(sr)) return -1;

  if (rand() % 2) {
    sr = CALL_STMT(SQLFreeStmt(hstmt, SQL_CLOSE));
    if (FAILED(sr)) return -1;
  }

  return 0;
}

static int test_executes(SQLHANDLE hstmt)
{
  CHECK(test_execute(hstmt, "insert into t (ts, name, age, sex, text) values (1662861448752, 'name1', 20, 'male', '中国人')"));
  // CHECK(test_execute(hstmt, "insert into t (ts, name, age, sex, text) values (1662861449753, 'name2', 30, 'female', '苏州人')"));
  // CHECK(test_execute(hstmt, "insert into t (ts, name, age, sex, text) values (1662861450754, 'name3', null, null, null)"));

  return 0;
}

static int test_queries(SQLHANDLE hdbc)
{
  SQLHANDLE hstmt;

  int r = create_statement(&hstmt, hdbc);
  if (r) return -1;

  r = test_prepare_test_data(hstmt);
  if (r) goto end;

  do {
    r = test_direct_executes(hstmt);
    if (r) break;

    if (1) break;

    r = test_executes(hstmt);
    if (r) break;
  } while (0);

end:
  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return r;
}

int main(int argc, char *argv[])
{
  (void)argc;
  (void)argv;
  srand(time(0));

  CHECK(!test_connect(NULL, "xTAOS_ODBC_DSN", NULL, NULL));
  CHECK(test_connect(NULL, "TAOS_ODBC_DSN", NULL, NULL));
  CHECK(test_connect(NULL, "TAOS_ODBC_DSN", "root", "taosdata"));

  CHECK(!test_connect(NULL, "TAOS_ODBC_DSN", "root", ""));
  CHECK(test_connect(NULL, "TAOS_ODBC_DSN", "root", NULL));

  CHECK(!test_connect(NULL, "TAOS_ODBC_DSN", "", "taosdata"));
  CHECK(test_connect(NULL, "TAOS_ODBC_DSN", NULL, "taosdata"));

  CHECK(!test_connect("hello", NULL, NULL, NULL));

  CHECK(test_connect("DSN=TAOS_ODBC_DSN", NULL, NULL, NULL));
  CHECK(test_connect("DSN=TAOS_ODBC_DSN;UID=;PWD=;", NULL, NULL, NULL));
  CHECK(test_connect("Driver={TAOS_ODBC_DRIVER};UID=root;PWD=taosdata;Server=localhost:6030;DB=;UNSIGNED_PROMOTION=1;CACHE_SQL=1;", NULL, NULL, NULL));

  CHECK(test_connect("Driver={TAOS_ODBC_DRIVER}", NULL, NULL, NULL));

  CHECK(test_connect("Driver={TAOS_ODBC_DRIVER};UID=;", NULL, NULL, NULL));
  CHECK(test_connect("Driver={TAOS_ODBC_DRIVER};UID=root;", NULL, NULL, NULL));
  CHECK(!test_connect("Driver={TAOS_ODBC_DRIVER};UID=xroot;", NULL, NULL, NULL));

  CHECK(test_connect("Driver={TAOS_ODBC_DRIVER};PWD=taosdata;", NULL, NULL, NULL));
  CHECK(test_connect("Driver={TAOS_ODBC_DRIVER};PWD=;", NULL, NULL, NULL));
  CHECK(!test_connect("Driver={TAOS_ODBC_DRIVER};PWD=xtaosdata;", NULL, NULL, NULL));

  CHECK(test_connect("Driver={TAOS_ODBC_DRIVER};Server=;", NULL, NULL, NULL));
  CHECK(test_connect("Driver={TAOS_ODBC_DRIVER};Server=localhost;", NULL, NULL, NULL));
  CHECK(test_connect("Driver={TAOS_ODBC_DRIVER};Server=127.0.0.1;", NULL, NULL, NULL));
  CHECK(test_connect("Driver={TAOS_ODBC_DRIVER};Server=localhost:6030;", NULL, NULL, NULL));
  CHECK(test_connect("Driver={TAOS_ODBC_DRIVER};Server=127.0.0.1:6030;", NULL, NULL, NULL));
  CHECK(!test_connect("Driver={TAOS_ODBC_DRIVER};Server=localhost:5030;", NULL, NULL, NULL));
  CHECK(!test_connect("Driver={TAOS_ODBC_DRIVER};Server=127.0.0.1:5030;", NULL, NULL, NULL));

  CHECK(test_connect("Driver={TAOS_ODBC_DRIVER};UNSIGNED_PROMOTION=;", NULL, NULL, NULL));
  CHECK(test_connect("Driver={TAOS_ODBC_DRIVER};UNSIGNED_PROMOTION=1;", NULL, NULL, NULL));
  CHECK(!test_connect("Driver={TAOS_ODBC_DRIVER};UNSIGNED_PROMOTION=x;", NULL, NULL, NULL));
  CHECK(!test_connect("Driver={TAOS_ODBC_DRIVER};UNSIGNED_PROMOTION=1x;", NULL, NULL, NULL));

  CHECK(test_connect("Driver={TAOS_ODBC_DRIVER};CACHE_SQL=;", NULL, NULL, NULL));
  CHECK(test_connect("Driver={TAOS_ODBC_DRIVER};CACHE_SQL=1;", NULL, NULL, NULL));
  CHECK(!test_connect("Driver={TAOS_ODBC_DRIVER};CACHE_SQL=x;", NULL, NULL, NULL));
  CHECK(!test_connect("Driver={TAOS_ODBC_DRIVER};CACHE_SQL=1x;", NULL, NULL, NULL));

  // FIXME: why these two internal-databases can not be opened during taos_connect
  //        taosc reports failure cause as: Invalid database name
  CHECK(!test_connect("Driver={TAOS_ODBC_DRIVER};UID=root;PWD=taosdata;Server=localhost:6030;DB=performance_schema;UNSIGNED_PROMOTION=1;CACHE_SQL=1;", NULL, NULL, NULL));
  CHECK(!test_connect("Driver={TAOS_ODBC_DRIVER};UID=root;PWD=taosdata;Server=localhost:6030;DB=information_schema;UNSIGNED_PROMOTION=1;CACHE_SQL=1;", NULL, NULL, NULL));

  SQLRETURN sr;
  SQLHANDLE henv, hdbc;
  int r = 0;

  const char *conn_str = "DSN=TAOS_ODBC_DSN";
  const char *dsn = NULL;
  const char *uid = NULL;
  const char *pwd = NULL;
  r = create_connection(&henv, &hdbc, conn_str, dsn, uid, pwd);
  if (r) return 1;

  r = test_queries(hdbc);

  sr = CALL_DBC(SQLDisconnect(hdbc));
  if (FAILED(sr)) r = 1;

  SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
  SQLFreeHandle(SQL_HANDLE_ENV, henv);

  return r;
}

