// SQLBindCol_ref.cpp
#include <errno.h>
#include <iconv.h>
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>

#include <sqlext.h>

#define D(_fmt, ...)                                                 \
  do {                                                               \
    fprintf(stderr, "%s[%d]:%s(): " _fmt "\n",                       \
                    basename((char*)__FILE__), __LINE__, __func__,   \
                    ##__VA_ARGS__);                                  \
  } while (0)

#define A(_statement, _fmt, ...)                                          \
  do {                                                                    \
    if (!(_statement)) {                                                  \
      fprintf(stderr, "%s[%d]:%s(): assertion failed: [%s]" _fmt "\n",    \
          basename((char*)__FILE__), __LINE__, __func__,                  \
          #_statement,                                                    \
          ##__VA_ARGS__);                                                 \
      throw int(1);                                                       \
    }                                                                     \
  } while (0)

#define SUCCEEDED(_sr) ({ SQLRETURN __sr = _sr; (__sr == SQL_SUCCESS) || (__sr == SQL_SUCCESS_WITH_INFO); })
#define FAILED(_sr) !SUCCEEDED(_sr)

#define diagnostic(_HandleType, _Handle)                                             \
  do {                                                                               \
    SQLCHAR _sqlState[6];                                                            \
    SQLINTEGER _nativeErrno;                                                         \
    SQLCHAR _messageText[1024];                                                      \
    SQLSMALLINT _textLength;                                                         \
    SQLRETURN _sr;                                                                   \
    for (SQLSMALLINT _i=1; _i>=1; ++_i) {                                            \
      _sr = SQLGetDiagRec(_HandleType, _Handle, _i,                                  \
          _sqlState, &_nativeErrno,                                                  \
          _messageText, sizeof(_messageText), &_textLength);                         \
      if (_sr == SQL_NO_DATA) break;                                                 \
      if (_sr == SQL_ERROR) break;                                                   \
      if (_textLength == SQL_NTS)                                                    \
        D("[%s][%d]: %s", _sqlState, _nativeErrno, _messageText);                    \
      else                                                                           \
        D("[%s][%d]: %.*s", _sqlState, _nativeErrno, _textLength, _messageText);     \
    }                                                                                \
  } while (0)

#define CALL(_handletype, _handle, _statement)            \
  ({                                                      \
    D("%s...", #_statement);                              \
    SQLRETURN _sr = _statement;                           \
    diagnostic(_handletype, _handle);                     \
    D("%s => %d", #_statement, _sr);                      \
    _sr;                                                  \
  })

#define CALL_ENV(_statement) CALL(SQL_HANDLE_ENV, henv, _statement)
#define CALL_DBC(_statement) CALL(SQL_HANDLE_DBC, hdbc, _statement)
#define CALL_STMT(_statement) CALL(SQL_HANDLE_STMT, hstmt, _statement)

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

int main() {
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

          rc = CALL_STMT(SQLExecDirect(hstmt, (SQLCHAR*)"SELECT * FROM t", SQL_NTS));
          A(SUCCEEDED(rc), "");

          SQLWCHAR Name[100];
          SQLLEN cbName;
          const char *p = (const char*)&Name[0];
          const size_t bytes = 26;
          int pos = 1;
          rc = CALL_STMT(SQLBindCol(hstmt, pos, SQL_C_WCHAR, Name, bytes/*sizeof(Name)*/, &cbName));
          A(SUCCEEDED(rc), "");

          for (int i=0 ; ; i++) {
            memset(Name, '\x01', sizeof(Name));
            rc = CALL_STMT(SQLFetch(hstmt));
            if (rc == SQL_NO_DATA) break;
            if (cbName != SQL_NULL_DATA) {
              A(p[bytes] == '\x01', "");
              A(p[cbName] == '\x01', "");
              A(SUCCEEDED(rc), "");
              char *inbuf = (char*)Name;
              size_t inbytes = cbName;
              char buf[1024];
              char *outbuf = buf;
              size_t outbytes = sizeof(buf);

              int x = _encode("ucs2", &inbuf, &inbytes,
                  "utf8", &outbuf, &outbytes);
              A(x == 0, "");
              D("[%.*s]", (int)(sizeof(buf) - outbytes), buf);
            } else {
              D("null");
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

