#ifndef _helpers_h_
#define _helpers_h_

#include <libgen.h>
#include <stdio.h>

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

#endif // _helpers_h_

