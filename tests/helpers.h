#ifndef _helpers_h_
#define _helpers_h_

#include <libgen.h>
#include <stdio.h>

#include <sqlext.h>

#ifdef __cplusplus         /* { */
#define ABORT_OR_THROW throw int(1)
#else                      /* }{ */
#include <stdlib.h>
#define ABORT_OR_THROW abort()
#endif                     /* } */

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
      ABORT_OR_THROW;                            \
    }                                            \
  } while (0)

#define SUCCEEDED(_sr) ({ SQLRETURN __sr = _sr; (__sr == SQL_SUCCESS) || (__sr == SQL_SUCCESS_WITH_INFO); })
#define FAILED(_sr) !SUCCEEDED(_sr)

#define diagnostic(_HandleType, _Handle)                                             \
  do {                                                                               \
    SQLCHAR _sqlState[6];                                                            \
    SQLINTEGER _nativeErrno = 0;                                                     \
    SQLCHAR _messageText[1024];                                                      \
    _messageText[0] = '\0';                                                          \
    SQLSMALLINT _textLength = 0;                                                     \
    SQLRETURN _sr = SQL_SUCCESS;                                                     \
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

