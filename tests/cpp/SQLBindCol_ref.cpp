// SQLBindCol_ref.cpp

#include "../helpers.h"

#include <errno.h>
#include <iconv.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>


static int _encode(
    const char *fromcode, char **inbuf, size_t *inbytesleft,
    const char *tocode, char **outbuf, size_t *outbytesleft)
{
  iconv_t cd = iconv_open(tocode, fromcode);
  if ((size_t)cd == (size_t)-1) {
    D("[iconv] No character set conversion found for `%s` to `%s`: [%d] %s",
        fromcode, tocode, errno, strerror(errno));
    return -1;
  }

  size_t inbytes = *inbytesleft;
  size_t outbytes = *outbytesleft;
  size_t sz = iconv(cd, inbuf, inbytesleft, outbuf, outbytesleft);
  int e = errno;
  iconv_close(cd);
  if (*inbytesleft > 0) {
    D("[iconv] Character set conversion for `%s` to `%s` results in string truncation, #%ld out of #%ld bytes consumed, #%ld out of #%ld bytes converted: [%d] %s",
        fromcode, tocode, inbytes - *inbytesleft, inbytes, outbytes - * outbytesleft, outbytes, e, strerror(e));
    return -1;
  }
  if (sz == (size_t)-1) {
    D("[iconv] Character set conversion for `%s` to `%s` failed: [%d] %s",
        fromcode, tocode, e, strerror(e));
    return -1;
  }
  if (sz > 0) {
    // FIXME: what actually means when sz > 0???
    D("[iconv] Character set conversion for `%s` to `%s` succeeded with #[%ld] of nonreversible characters converted",
        fromcode, tocode, sz);
  }

  return 0;
}

static int test_case1(void)
{
  SQLHENV henv = 0;
  SQLHDBC hdbc = 0;
  SQLHSTMT hstmt = 0;
  SQLRETURN rc;

  int r = 0;

  rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  A(SUCCEEDED(rc), "");

  try {
    rc = CALL_ENV(SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER*)SQL_OV_ODBC3, 0));
    A(SUCCEEDED(rc), "");

    rc = CALL_ENV(SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc));
    A(SUCCEEDED(rc), "");

    try {
      rc = CALL_DBC(SQLSetConnectAttr(hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)5, 0));
      A(SUCCEEDED(rc), "");

      // Connect to data source
      rc = CALL_DBC(SQLDriverConnect(hdbc, NULL,
            (SQLCHAR*) "DSN=TAOS_ODBC_DSN", SQL_NTS,
            NULL, 0, NULL,
            SQL_DRIVER_NOPROMPT));
      A(SUCCEEDED(rc), "");

      // Allocate statement handle
      try {
        rc = CALL_DBC(SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt));
        A(SUCCEEDED(rc), "");

        try {
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"show databases", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"xshow databases", SQL_NTS));
          A(FAILED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"show databases", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"drop database if exists foo", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"create database if not exists foo", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"use foo", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"drop table if exists t", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"select * from t", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"insert into t (ts, name, age, sex, text) values (1662861448752, 'name1', 20, 'male', '中国人')", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"insert into t (ts, name, age, sex, text) values (1662861449753, 'name2', 30, 'female', '苏州人')", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"insert into t (ts, name, age, sex, text) values (1662861450754, 'name3', null, null, null)", SQL_NTS));
          A(SUCCEEDED(rc), "");

          for (size_t pos = 1; pos <= 5; ++pos ) {
            rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"SELECT * FROM t", SQL_NTS));
            A(SUCCEEDED(rc), "");

            const char *exp[][5] = {
              {"[2022-09-11 09:57:28.752]", "[name1]", "[20]", "[male]", "[中国人]"},
              {"[2022-09-11 09:57:29.753]", "[name2]", "[30]", "[female]", "[苏州人]"},
              {"[2022-09-11 09:57:30.754]", "[name3]", "null", "null", "null"},
            };

            SQLWCHAR Name[100];
            SQLLEN cbName = 0;
            const char * const p = (const char*)&Name[0];
            const size_t bytes = 46;
            rc = CALL_STMT(SQLFreeStmt(hstmt, SQL_UNBIND));
            A(SUCCEEDED(rc), "");
            rc = CALL_STMT(SQLBindCol(hstmt, pos, SQL_C_WCHAR, Name, bytes/*sizeof(Name)*/, &cbName));
            A(SUCCEEDED(rc), "");

            for (int i=0 ; ; i++) {
              memset(Name, '\x01', sizeof(Name));
              rc = CALL_STMT(SQLFetch(hstmt));
              if (rc == SQL_NO_DATA) break;
              A(SUCCEEDED(rc), "");
              char buf[1024];
              char cmp[sizeof(buf)+2];
              if (cbName != SQL_NULL_DATA) {
                A(p[bytes] == '\x01', "");
                A(p[cbName] == '\x01', "");
                char *inbuf = (char*)Name;
                size_t inbytes = cbName;
                char *outbuf = buf;
                size_t outbytes = sizeof(buf);

                int x = _encode("ucs2", &inbuf, &inbytes,
                    "utf8", &outbuf, &outbytes);
                A(x == 0, "");
                *outbuf = '\0';
                int n = snprintf(cmp, sizeof(cmp), "[%s]", buf);
                A(n >= 0 && (size_t)n < sizeof(cmp), "");
              } else {
                strcpy(cmp, "null");
              }
              A(strcmp(cmp, exp[i][pos-1]) == 0, "\nrow#%d, col#%ld\nactual  :%s\nexpected:%s", i+1, pos, cmp, exp[i][pos-1]);
            }
          }
        } catch (int e) {
          r = e;
        }

        SQLCancel(hstmt);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      } catch (int e) {
        r = e;
      }
      SQLDisconnect(hdbc);
    } catch (int e) {
      r = e;
    }
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
  } catch (int e) {
    r = e;
  }

  SQLFreeHandle(SQL_HANDLE_ENV, henv);

  return r ? 1 : 0;
}

static int test_case2(void)
{
  SQLHENV henv = 0;
  SQLHDBC hdbc = 0;
  SQLHSTMT hstmt = 0;
  SQLRETURN rc;

  int r = 0;

  rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  A(SUCCEEDED(rc), "");

  try {
    rc = CALL_ENV(SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER*)SQL_OV_ODBC3, 0));
    A(SUCCEEDED(rc), "");

    rc = CALL_ENV(SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc));
    A(SUCCEEDED(rc), "");

    try {
      rc = CALL_DBC(SQLSetConnectAttr(hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)5, 0));
      A(SUCCEEDED(rc), "");

      // Connect to data source
      rc = CALL_DBC(SQLDriverConnect(hdbc, NULL,
            (SQLCHAR*) "DSN=TAOS_ODBC_DSN", SQL_NTS,
            NULL, 0, NULL,
            SQL_DRIVER_NOPROMPT));
      A(SUCCEEDED(rc), "");

      // Allocate statement handle
      try {
        rc = CALL_DBC(SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt));
        A(SUCCEEDED(rc), "");

        try {
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"show databases", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"xshow databases", SQL_NTS));
          A(FAILED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"show databases", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"drop database if exists foo", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"create database if not exists foo", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"use foo", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"drop table if exists t", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"select * from t", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"insert into t (ts, name, age, sex, text) values (1662861448752, 'name1', 20, 'male', '中国人')", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"insert into t (ts, name, age, sex, text) values (1662861449753, 'name2', 30, 'female', '苏州人')", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"insert into t (ts, name, age, sex, text) values (1662861450754, 'name3', null, null, null)", SQL_NTS));
          A(SUCCEEDED(rc), "");

          for (size_t pos = 1; pos <= 5; ++pos ) {
            rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"SELECT * FROM t", SQL_NTS));
            A(SUCCEEDED(rc), "");

            const char *exp[][5] = {
              {"[2022-09-11 09:57:28.752]", "[name1]", "[20]", "[male]", "[中国人]"},
              {"[2022-09-11 09:57:29.753]", "[name2]", "[30]", "[female]", "[苏州人]"},
              {"[2022-09-11 09:57:30.754]", "[name3]", "null", "null", "null"},
            };

            SQLCHAR Name[100];
            SQLLEN cbName = 0;
            const char * const p = (const char*)&Name[0];
            const size_t bytes = 24;
            rc = CALL_STMT(SQLFreeStmt(hstmt, SQL_UNBIND));
            A(SUCCEEDED(rc), "");
            rc = CALL_STMT(SQLBindCol(hstmt, pos, SQL_C_CHAR, Name, bytes/*sizeof(Name)*/, &cbName));
            A(SUCCEEDED(rc), "");

            for (int i=0 ; ; i++) {
              memset(Name, 'x', sizeof(Name));
              rc = CALL_STMT(SQLFetch(hstmt));
              if (rc == SQL_NO_DATA) break;
              A(SUCCEEDED(rc), "");
              char buf[1024];
              if (cbName != SQL_NULL_DATA) {
                A(p[bytes] == 'x', "");
                A(p[cbName+1] == 'x', "i:%d; pos:%zd; p[%zd]:[0x%02x]", i, pos, cbName, p[cbName+1]);

                int n = snprintf(buf, sizeof(buf), "[%s]", (const char*)Name);
                A(n >= 0 && (size_t)n < sizeof(buf), "");
              } else {
                strcpy(buf, "null");
              }
              A(strcmp(buf, exp[i][pos-1]) == 0, "\nrow#%d, col#%ld\nactual  :%s\nexpected:%s", i+1, pos, buf, exp[i][pos-1]);
            }
          }
        } catch (int e) {
          r = e;
        }

        SQLCancel(hstmt);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      } catch (int e) {
        r = e;
      }
      SQLDisconnect(hdbc);
    } catch (int e) {
      r = e;
    }
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
  } catch (int e) {
    r = e;
  }

  SQLFreeHandle(SQL_HANDLE_ENV, henv);

  return r ? 1 : 0;
}

static int test_case3(void)
{
  SQLHENV henv = 0;
  SQLHDBC hdbc = 0;
  SQLHSTMT hstmt = 0;
  SQLRETURN rc;

  int r = 0;

  rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  A(SUCCEEDED(rc), "");

  try {
    rc = CALL_ENV(SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER*)SQL_OV_ODBC3, 0));
    A(SUCCEEDED(rc), "");

    rc = CALL_ENV(SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc));
    A(SUCCEEDED(rc), "");

    try {
      rc = CALL_DBC(SQLSetConnectAttr(hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)5, 0));
      A(SUCCEEDED(rc), "");

      // Connect to data source
      rc = CALL_DBC(SQLDriverConnect(hdbc, NULL,
            (SQLCHAR*) "DSN=TAOS_ODBC_DSN", SQL_NTS,
            NULL, 0, NULL,
            SQL_DRIVER_NOPROMPT));
      A(SUCCEEDED(rc), "");

      // Allocate statement handle
      try {
        rc = CALL_DBC(SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt));
        A(SUCCEEDED(rc), "");

        try {
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"show databases", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"xshow databases", SQL_NTS));
          A(FAILED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"show databases", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"drop database if exists foo", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"create database if not exists foo", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"use foo", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"drop table if exists t", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"select * from t", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"insert into t (ts, name, age, sex, text) values (1662861448752, 'name1', 20, 'male', '中国人')", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"insert into t (ts, name, age, sex, text) values (1662861449753, 'name2', 30, 'female', '苏州人')", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"insert into t (ts, name, age, sex, text) values (1662861450754, 'name3', null, null, null)", SQL_NTS));
          A(SUCCEEDED(rc), "");

          for (size_t pos = 1; pos <= 5; ++pos ) {
            rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"SELECT * FROM t", SQL_NTS));
            A(SUCCEEDED(rc), "");

            const char *exp[][5] = {
              {"[2022-09-11 09:57:28.752]", "[name1]", "[20]", "[male]", "[中国人]"},
              {"[2022-09-11 09:57:29.753]", "[name2]", "[30]", "[female]", "[苏州人]"},
              {"[2022-09-11 09:57:30.754]", "[name3]", "null", "null", "null"},
            };

            SQLCHAR Name[100];
            SQLLEN cbName = 0;
            const char * const p = (const char*)&Name[0];
            const size_t bytes = 24;
            rc = CALL_STMT(SQLFreeStmt(hstmt, SQL_UNBIND));
            A(SUCCEEDED(rc), "");
            rc = CALL_STMT(SQLBindCol(hstmt, pos, SQL_C_CHAR, Name, bytes/*sizeof(Name)*/, &cbName));
            A(SUCCEEDED(rc), "");

            for (int i=0 ; ; i++) {
              memset(Name, 'x', sizeof(Name));
              rc = CALL_STMT(SQLFetch(hstmt));
              if (rc == SQL_NO_DATA) break;
              A(SUCCEEDED(rc), "");
              char buf[1024];
              if (cbName != SQL_NULL_DATA) {
                A(p[bytes] == 'x', "");
                A(p[cbName+1] == 'x', "i:%d; pos:%zd; p[%zd]:[0x%02x]", i, pos, cbName, p[cbName+1]);

                D("cbName: %zd", cbName);
                Name[bytes-1] = '\0';
                int n = snprintf(buf, sizeof(buf), "[%s]", (const char*)Name);
                A(n >= 0 && (size_t)n < sizeof(buf), "");
              } else {
                strcpy(buf, "null");
              }
              A(strcmp(buf, exp[i][pos-1]) == 0, "\nrow#%d, col#%ld\nactual  :%s\nexpected:%s", i+1, pos, buf, exp[i][pos-1]);
            }
          }
        } catch (int e) {
          r = e;
        }

        SQLCancel(hstmt);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      } catch (int e) {
        r = e;
      }
      SQLDisconnect(hdbc);
    } catch (int e) {
      r = e;
    }
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
  } catch (int e) {
    r = e;
  }

  SQLFreeHandle(SQL_HANDLE_ENV, henv);

  return r ? 1 : 0;
}

int main()
{
  if (test_case1()) return 1;
  if (test_case2()) return 1;
  if (test_case3()) return 1;
  return 0;
}

