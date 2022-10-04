#include "../helpers.h"

static int test_connect(const char *conn_str, const char *dsn, const char *uid, const char *pwd)
{
  int r = -1;

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

  sr = CALL_DBC(SQLDisconnect(hdbc));
  if (SUCCEEDED(sr)) r = 0;

fail_connect:
  SQLFreeHandle(SQL_HANDLE_DBC, hdbc);

fail_hdbc:
fail_odbc_version:
  SQLFreeHandle(SQL_HANDLE_ENV, henv);

fail_henv:
  return r;
}

#define CHECK(_statement) do {           \
  if (_statement) {                      \
    D("failed: [%s]", #_statement);      \
    return 1;                            \
  }                                      \
} while (0)

int main(int argc, char *argv[])
{
  (void)argc;
  (void)argv;

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

  return 0;
}

