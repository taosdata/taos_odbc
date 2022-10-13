#ifndef _odbc_helper_h_
#define _odbc_helper_h_

#include "helpers.h"

#include <sqlext.h>

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

#endif // _odbc_helper_h_


