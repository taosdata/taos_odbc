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

static int test_mysql_case1(void)
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
            (SQLCHAR*) "Driver={MySQL ODBC 8.0}; UID=root; PWD=taosdata;", SQL_NTS,
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
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"create table if not exists t (name varchar(20), age int, sex varchar(8), text varchar(30))", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"select * from t", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"insert into t (name, age, sex, text) values ('name1', 20, 'male', '中国人')", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"insert into t (name, age, sex, text) values ('name2', 30, 'female', '苏州人')", SQL_NTS));
          A(SUCCEEDED(rc), "");
          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"insert into t (name, age, sex, text) values ('name3', null, null, null)", SQL_NTS));
          A(SUCCEEDED(rc), "");

          SQLSMALLINT ColumnCount;

          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"SELECT * FROM t", SQL_NTS));
          A(SUCCEEDED(rc), "");

          rc = CALL_STMT(SQLNumResultCols(hstmt, &ColumnCount));
          A(SUCCEEDED(rc), "");

          for (int i = -1; i <= 16; ++i ) {
            const char fill = 'X';
            char buf[1024]; memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';

            SQLUSMALLINT   ColumnNumber         = 1;
            SQLCHAR *      ColumnName           = (SQLCHAR*)buf;
            SQLSMALLINT    BufferLength         = i;
            SQLSMALLINT    NameLength;
            SQLSMALLINT    DataType;
            SQLULEN        ColumnSize;
            SQLSMALLINT    DecimalDigits;
            SQLSMALLINT    Nullable;

            rc = CALL_STMT(SQLDescribeCol(hstmt, ColumnNumber, ColumnName, BufferLength, &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable));
            if (i<0) {
              A(FAILED(rc), "");
            } else {
              A(SUCCEEDED(rc), "rc[%d]; BufferLength[%d]", rc, BufferLength);
              A(NameLength == 4, "BufferLength[%d]; NameLength[%d]", BufferLength, NameLength);
              if (BufferLength > NameLength) A(buf[NameLength] == '\0', "");
              else if (BufferLength == 1) A(buf[BufferLength-1] == fill, "buf[%d]: [%c]", BufferLength-1, buf[BufferLength-1]);
              else if (BufferLength > 0) A(buf[BufferLength-1] == '\0', "buf[%d]: [%c]", BufferLength-1, buf[BufferLength-1]);
              else if (BufferLength == 0) A(buf[0] == fill, "buf[%d]: [%c]", 0, buf[0]);
            }
          }

          for (int i = -1; i <= 16; ++i ) {
            const char fill = 'X';
            char buf[1024]; memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';

            SQLUSMALLINT   ColumnNumber         = 1;
            SQLWCHAR *     ColumnName           = (SQLWCHAR*)buf;
            SQLSMALLINT    BufferLength         = i;
            SQLSMALLINT    NameLength;
            SQLSMALLINT    DataType;
            SQLULEN        ColumnSize;
            SQLSMALLINT    DecimalDigits;
            SQLSMALLINT    Nullable;

            if (BufferLength==0) continue;
            if (BufferLength==1) continue;
            rc = CALL_STMT(SQLDescribeColW(hstmt, ColumnNumber, ColumnName, BufferLength, &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable));
            if (i<0) {
              A(FAILED(rc), "");
            } else {
              A(SUCCEEDED(rc), "rc[%d]; BufferLength[%d]", rc, BufferLength);
              A(NameLength == 4, "BufferLength[%d]; NameLength[%d]", BufferLength, NameLength);
              if (BufferLength > NameLength) A(buf[NameLength*2-1] == '\0', "");
              if (BufferLength > NameLength) A(buf[NameLength*2] == '\0', "");
              else if (BufferLength > 1) {
                for (int j=0; j<BufferLength; ++j) {
                  D("BufferLength[%d]; buf[%d]: 0x%02x, 0x%02x", BufferLength, j*2, buf[j*2], buf[j*2+1]);
                }
                A(buf[BufferLength*2-1] == '\0', "buf[%d]: [%c]", BufferLength*2-1, buf[BufferLength*2-1]);
                A(buf[BufferLength*2] == fill, "buf[%d]: [%c]", BufferLength*2, buf[BufferLength*2]);
              }
            }
          }

          if (1) {
            rc = CALL_STMT(SQLFetch(hstmt));
            A(SUCCEEDED(rc), "");

            const char fill = 'X';
            char buf[1024];

            SQLUSMALLINT   Col_or_Param_Num = 4;
            SQLSMALLINT    TargetType       = SQL_C_CHAR;
            SQLPOINTER     TargetValue      = (SQLPOINTER)buf;
            SQLLEN         BufferLength;
            SQLLEN         StrLen_or_Ind;

            memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';
            BufferLength = -1;
            rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
            A(FAILED(rc), "");

            memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';
            BufferLength = -1;
            rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
            A(FAILED(rc), "");

            memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';
            BufferLength = 0;
            rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
            A(SUCCEEDED(rc), "");
            for (int ch=0; ch<6; ++ch) {
              D("BufferLength[%ld]; ch[%d]; [0x%02x],[0x%02x]; StrLen_or_Ind[%ld]", BufferLength, ch, (unsigned char)buf[ch*2], (unsigned char)buf[ch*2+1], StrLen_or_Ind);
            }
            A(StrLen_or_Ind == 9, "");

            memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';
            BufferLength = 0;
            rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
            A(SUCCEEDED(rc), "");
            for (int ch=0; ch<6; ++ch) {
              D("BufferLength[%ld]; ch[%d]; [0x%02x],[0x%02x]; StrLen_or_Ind[%ld]", BufferLength, ch, (unsigned char)buf[ch*2], (unsigned char)buf[ch*2+1], StrLen_or_Ind);
            }
            A(StrLen_or_Ind == 9, "");

            memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';
            BufferLength = 1;
            rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
            A(SUCCEEDED(rc), "");
            for (int ch=0; ch<6; ++ch) {
              D("BufferLength[%ld]; ch[%d]; [0x%02x],[0x%02x]; StrLen_or_Ind[%ld]", BufferLength, ch, (unsigned char)buf[ch*2], (unsigned char)buf[ch*2+1], StrLen_or_Ind);
            }
            A(StrLen_or_Ind == 9, "");

            memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';
            BufferLength = 1;
            rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
            A(SUCCEEDED(rc), "");
            for (int ch=0; ch<6; ++ch) {
              D("BufferLength[%ld]; ch[%d]; [0x%02x],[0x%02x]; StrLen_or_Ind[%ld]", BufferLength, ch, (unsigned char)buf[ch*2], (unsigned char)buf[ch*2+1], StrLen_or_Ind);
            }
            A(StrLen_or_Ind == 9, "");

            memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';
            BufferLength = 2;
            rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
            A(SUCCEEDED(rc), "");
            for (int ch=0; ch<6; ++ch) {
              D("BufferLength[%ld]; ch[%d]; [0x%02x],[0x%02x]; StrLen_or_Ind[%ld]", BufferLength, ch, (unsigned char)buf[ch*2], (unsigned char)buf[ch*2+1], StrLen_or_Ind);
            }
            A(StrLen_or_Ind == 9, "");

            memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';
            BufferLength = 2;
            rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
            A(SUCCEEDED(rc), "");
            for (int ch=0; ch<6; ++ch) {
              D("BufferLength[%ld]; ch[%d]; [0x%02x],[0x%02x]; StrLen_or_Ind[%ld]", BufferLength, ch, (unsigned char)buf[ch*2], (unsigned char)buf[ch*2+1], StrLen_or_Ind);
            }
            A(StrLen_or_Ind == 8, "");

            memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';
            BufferLength = 2;
            rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
            A(SUCCEEDED(rc), "");
            for (int ch=0; ch<6; ++ch) {
              D("BufferLength[%ld]; ch[%d]; [0x%02x],[0x%02x]; StrLen_or_Ind[%ld]", BufferLength, ch, (unsigned char)buf[ch*2], (unsigned char)buf[ch*2+1], StrLen_or_Ind);
            }
            A(StrLen_or_Ind == 7, "");

            memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';
            BufferLength = 2;
            rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
            A(SUCCEEDED(rc), "");
            for (int ch=0; ch<6; ++ch) {
              D("BufferLength[%ld]; ch[%d]; [0x%02x],[0x%02x]; StrLen_or_Ind[%ld]", BufferLength, ch, (unsigned char)buf[ch*2], (unsigned char)buf[ch*2+1], StrLen_or_Ind);
            }
            A(StrLen_or_Ind == 6, "");

            memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';
            BufferLength = 2;
            rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
            A(SUCCEEDED(rc), "");
            for (int ch=0; ch<6; ++ch) {
              D("BufferLength[%ld]; ch[%d]; [0x%02x],[0x%02x]; StrLen_or_Ind[%ld]", BufferLength, ch, (unsigned char)buf[ch*2], (unsigned char)buf[ch*2+1], StrLen_or_Ind);
            }
            A(StrLen_or_Ind == 5, "");

            memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';
            BufferLength = 2;
            rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
            A(SUCCEEDED(rc), "");
            for (int ch=0; ch<6; ++ch) {
              D("BufferLength[%ld]; ch[%d]; [0x%02x],[0x%02x]; StrLen_or_Ind[%ld]", BufferLength, ch, (unsigned char)buf[ch*2], (unsigned char)buf[ch*2+1], StrLen_or_Ind);
            }
            A(StrLen_or_Ind == 4, "");

            memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';
            BufferLength = 2;
            rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
            A(SUCCEEDED(rc), "");
            for (int ch=0; ch<6; ++ch) {
              D("BufferLength[%ld]; ch[%d]; [0x%02x],[0x%02x]; StrLen_or_Ind[%ld]", BufferLength, ch, (unsigned char)buf[ch*2], (unsigned char)buf[ch*2+1], StrLen_or_Ind);
            }
            A(StrLen_or_Ind == 3, "");

            memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';
            BufferLength = 2;
            rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
            A(SUCCEEDED(rc), "");
            for (int ch=0; ch<6; ++ch) {
              D("BufferLength[%ld]; ch[%d]; [0x%02x],[0x%02x]; StrLen_or_Ind[%ld]", BufferLength, ch, (unsigned char)buf[ch*2], (unsigned char)buf[ch*2+1], StrLen_or_Ind);
            }
            A(StrLen_or_Ind == 2, "");

            memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';
            BufferLength = 2;
            rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
            A(SUCCEEDED(rc), "");
            for (int ch=0; ch<6; ++ch) {
              D("BufferLength[%ld]; ch[%d]; [0x%02x],[0x%02x]; StrLen_or_Ind[%ld]", BufferLength, ch, (unsigned char)buf[ch*2], (unsigned char)buf[ch*2+1], StrLen_or_Ind);
            }
            A(StrLen_or_Ind == 1, "");

            memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';
            BufferLength = 2;
            rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
            A(rc == SQL_NO_DATA, "");
          }

          rc = CALL_STMT(SQLCloseCursor(hstmt));
          A(SUCCEEDED(rc), "");

          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"SELECT * FROM t", SQL_NTS));
          A(SUCCEEDED(rc), "");

          rc = CALL_STMT(SQLNumResultCols(hstmt, &ColumnCount));
          A(SUCCEEDED(rc), "");

          for (int i=0; i<3; ++i) {
            rc = CALL_STMT(SQLFetch(hstmt));
            A(SUCCEEDED(rc), "");

            for (int j=1; j<ColumnCount; ++j) {

              for (int k=-1; k<=16; ++k) {
                const char fill = 'X';
                char buf[1024]; memset(buf, fill, sizeof(buf)); buf[sizeof(buf)-1] = '\0';

                SQLUSMALLINT   Col_or_Param_Num = j;
                SQLSMALLINT    TargetType       = SQL_C_CHAR;
                SQLPOINTER     TargetValue      = (SQLPOINTER)buf;
                SQLLEN         BufferLength     = k;
                SQLLEN         StrLen_or_Ind;

                rc = CALL_STMT(SQLGetData(hstmt, Col_or_Param_Num, TargetType, TargetValue, BufferLength, &StrLen_or_Ind));
                if (rc == SQL_NO_DATA) continue;
                if (BufferLength < 0) { A(FAILED(rc), ""); continue; }
                A(SUCCEEDED(rc), "i/j/k:[%d/%d/%d]", i, j, k);

                if (k<0) {
                  A(FAILED(rc), "");
                } else {
                  if (k==0) {
                    A(SUCCEEDED(rc), "rc[%d]; BufferLength[%ld]", rc, BufferLength);
                    if (StrLen_or_Ind == SQL_NULL_DATA) continue;
                    A(StrLen_or_Ind!=SQL_NO_TOTAL, "StrLen_or_Ind[0x%lx]", StrLen_or_Ind);
                    A(buf[0] == 'X', "");
                  } else {
                    A(SUCCEEDED(rc), "rc[%d]; BufferLength[%ld]", rc, BufferLength);
                    if (StrLen_or_Ind == SQL_NULL_DATA) continue;
                    A(StrLen_or_Ind!=SQL_NO_TOTAL, "StrLen_or_Ind[0x%lx]", StrLen_or_Ind);
                    A(strlen(buf) <= (size_t)StrLen_or_Ind, "i/j/k[%d/%d/%d]", i, j, k);
                    A(buf[strlen(buf)+2]=='X', "i/j/k[%d/%d/%d]", i, j, k);
                  }
                }
              }
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
  if (0) {
    if (test_case1()) return 1;
    if (test_case2()) return 1;
    if (test_case3()) return 1;
  } else {
    if (test_mysql_case1()) return 1;
  }
  return 0;
}
