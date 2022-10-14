#ifndef _odbc_helper_h_
#define _odbc_helper_h_

#include "helpers.h"
#include "enums.h"

#include <sqlext.h>
#include <string.h>

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

static inline void diag(SQLRETURN sr, SQLSMALLINT HandleType, SQLHANDLE Handle)
{
  do {
    if (sr == SQL_SUCCESS) break;
    SQLCHAR sqlState[6];
    sqlState[0] = '\0';
    SQLINTEGER nativeErrno = 0;
    SQLCHAR messageText[1024];
    messageText[0] = '\0';
    SQLSMALLINT textLength = 0;
    SQLRETURN _sr = SQL_SUCCESS;
    for (SQLSMALLINT i=1; i>=1; ++i) {
      _sr = SQLGetDiagRec(HandleType, Handle, i,
          sqlState, &nativeErrno,
          messageText, sizeof(messageText), &textLength);
      if (_sr == SQL_NO_DATA) break;
      if (_sr == SQL_ERROR) break;
      if (textLength == SQL_NTS)
        D("[%s][%d]: %s", sqlState, nativeErrno, messageText);
      else
        D("[%s][%d]: %.*s", sqlState, nativeErrno, textLength, messageText);
    }
  } while (0);
}

static inline SQLRETURN call_SQLAllocHandle(const char *file, int line, const char *func,
    SQLSMALLINT HandleType, SQLHANDLE InputHandle, SQLHANDLE *OutputHandle)
{
  LOGD(file, line, func, "SQLAllocHandle(HandleType:%s,InputHandle:%p,OutputHandle:%p) ...",
      sql_handle_type(HandleType), InputHandle, OutputHandle);
  SQLRETURN sr = SQLAllocHandle(HandleType, InputHandle, OutputHandle);
  if (sr != SQL_INVALID_HANDLE) {
    switch (HandleType) {
      case SQL_HANDLE_DBC:
        diag(sr, SQL_HANDLE_ENV, InputHandle);
        break;
      case SQL_HANDLE_DESC:
        diag(sr, SQL_HANDLE_DBC, InputHandle);
        break;
      case SQL_HANDLE_STMT:
        diag(sr, SQL_HANDLE_DBC, InputHandle);
        break;
      default:
        break;
    }
  }
  SQLHANDLE p = OutputHandle ? *OutputHandle : NULL;
  LOGD(file, line, func, "SQLAllocHandle(HandleType:%s,InputHandle:%p,OutputHandle:%p(%p)) => %s",
      sql_handle_type(HandleType), InputHandle, OutputHandle, p, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLFreeHandle(const char *file, int line, const char *func,
    SQLSMALLINT HandleType, SQLHANDLE Handle)
{
  LOGD(file, line, func, "SQLFreeHandle(HandleType:%s,Handle:%p) ...",
      sql_handle_type(HandleType), Handle);
  SQLRETURN sr = SQLFreeHandle(HandleType, Handle);
  LOGD(file, line, func, "SQLFreeHandle(HandleType:%s,Handle:%p) => %s",
      sql_handle_type(HandleType), Handle, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLSetEnvAttr(const char *file, int line, const char *func,
    SQLHENV EnvironmentHandle, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength)
{
  LOGD(file, line, func, "SQLSetEnvAttr(EnvironmentHandle:%p,Attribute:%s,ValuePtr:%p,StringLength:%d) ...",
      EnvironmentHandle, sql_env_attr(Attribute), ValuePtr, StringLength);
  SQLRETURN sr = SQLSetEnvAttr(EnvironmentHandle, Attribute, ValuePtr, StringLength);
  diag(sr, SQL_HANDLE_ENV, EnvironmentHandle);
  LOGD(file, line, func, "SQLSetEnvAttr(EnvironmentHandle:%p,Attribute:%s,ValuePtr:%p,StringLength:%d) => %s",
      EnvironmentHandle, sql_env_attr(Attribute), ValuePtr, StringLength, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLConnect(const char *file, int line, const char *func,
  SQLHDBC ConnectionHandle, SQLCHAR *ServerName, SQLSMALLINT NameLength1, SQLCHAR *UserName, SQLSMALLINT NameLength2, SQLCHAR *Authentication, SQLSMALLINT NameLength3)
{
  int n1 = (NameLength1 == SQL_NTS) ? (ServerName ? (int)strlen((const char*)ServerName) : 0) : (int)NameLength1;
  int n2 = (NameLength2 == SQL_NTS) ? (UserName ? (int)strlen((const char*)UserName) : 0) : (int)NameLength2;
  int n3 = (NameLength3 == SQL_NTS) ? (Authentication ? (int)strlen((const char*)Authentication) : 0) : (int)NameLength3;

  LOGD(file, line, func, "SQLConnect(ConnectionHandle:%p,ServerName:%.*s,NameLength1:%d,UserName:%.*s,NameLength2:%d,Authentication:%.*s,NameLength3:%d) ...",
      ConnectionHandle, n1, ServerName, NameLength1, n2, UserName, NameLength2, n3, Authentication, NameLength3);
  SQLRETURN sr = SQLConnect(ConnectionHandle, ServerName, NameLength1, UserName, NameLength2, Authentication, NameLength3);
  diag(sr, SQL_HANDLE_DBC, ConnectionHandle);
  LOGD(file, line, func, "SQLConnect(ConnectionHandle:%p,ServerName:%.*s,NameLength1:%d,UserName:%.*s,NameLength2:%d,Authentication:%.*s,NameLength3:%d) => %s",
      ConnectionHandle, n1, ServerName, NameLength1, n2, UserName, NameLength2, n3, Authentication, NameLength3, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLDriverConnect(const char *file, int line, const char *func,
    SQLHDBC ConnectionHandle, SQLHWND WindowHandle, SQLCHAR *InConnectionString, SQLSMALLINT StringLength1,
    SQLCHAR *OutConnectionString, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength2Ptr, SQLUSMALLINT DriverCompletion)
{
  int n1 = (StringLength1 == SQL_NTS) ? (InConnectionString ? (int)strlen((const char*)InConnectionString) : 0) : (int)StringLength1;

  LOGD(file, line, func,
      "SQLDriverConnect(ConnectionHandle:%p,WindowHandle:%p,InConnectionString:%.*s,StringLength1:%d,"
      "OutConnectionString:%p,BufferLength:%d,StringLength2Ptr:%p,DriverCompletion:%s) ...",
      ConnectionHandle, WindowHandle, n1, InConnectionString, StringLength1,
      OutConnectionString, BufferLength, StringLength2Ptr, sql_driver_completion(DriverCompletion));
  SQLRETURN sr = SQLDriverConnect(ConnectionHandle, WindowHandle, InConnectionString, StringLength1,
      OutConnectionString, BufferLength, StringLength2Ptr, DriverCompletion);
  diag(sr, SQL_HANDLE_DBC, ConnectionHandle);
  LOGD(file, line, func,
      "SQLDriverConnect(ConnectionHandle:%p,WindowHandle:%p,InConnectionString:%.*s,StringLength1:%d,"
      "OutConnectionString:%p,BufferLength:%d,StringLength2Ptr:%p,DriverCompletion:%s) => %s",
      ConnectionHandle, WindowHandle, n1, InConnectionString, StringLength1,
      OutConnectionString, BufferLength, StringLength2Ptr, sql_driver_completion(DriverCompletion),
      sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLDisconnect(const char *file, int line, const char *func,
    SQLHDBC ConnectionHandle)
{
  LOGD(file, line, func, "SQLDisconnect(ConnectionHandle:%p) ...", ConnectionHandle);
  SQLRETURN sr = SQLDisconnect(ConnectionHandle);
  LOGD(file, line, func, "SQLDisconnect(ConnectionHandle:%p) => %s", ConnectionHandle, sql_return_type(sr));
  return sr;
}


#define CALL_SQLAllocHandle(...)                   call_SQLAllocHandle(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLFreeHandle(...)                    call_SQLFreeHandle(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLSetEnvAttr(...)                    call_SQLSetEnvAttr(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLConnect(...)                       call_SQLConnect(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLDriverConnect(...)                 call_SQLDriverConnect(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLDisconnect(...)                    call_SQLDisconnect(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

#endif // _odbc_helper_h_


