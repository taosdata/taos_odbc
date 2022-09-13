// SQLBindCol_ref.cpp
#include <stdio.h>
#include <stdlib.h>
#include <wchar.h>

#include <sqlext.h>

#define NAME_LEN 50
#define PHONE_LEN 60

void show_error() {
  printf("error\n");
  abort();
}

int main() {
  SQLHENV henv;
  SQLHDBC hdbc;
  SQLHSTMT hstmt = 0;
  SQLRETURN rc;

  int r = -1;

  // Allocate environment handle
  rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);

  // Set the ODBC version environment attribute
  if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
    rc = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER*)SQL_OV_ODBC3, 0);

    // Allocate connection handle
    if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
      rc = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

      // Set login timeout to 5 seconds
      if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
        SQLSetConnectAttr(hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);

        // Connect to data source
        rc = SQLDriverConnect(hdbc, NULL,
            (SQLCHAR*) "DSN=TAOS_ODBC_DSN", SQL_NTS,
            NULL, 0, NULL,
            SQL_DRIVER_NOPROMPT);

        // Allocate statement handle
        if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
          rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);

          if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
            rc = SQLExecDirect(hstmt, (SQLCHAR*)"show databases", SQL_NTS);
            rc = SQLExecDirect(hstmt, (SQLCHAR*)"drop database if exists foo", SQL_NTS);
            rc = SQLExecDirect(hstmt, (SQLCHAR*)"create database if not exists foo", SQL_NTS);
            rc = SQLExecDirect(hstmt, (SQLCHAR*)"use foo", SQL_NTS);
            rc = SQLExecDirect(hstmt, (SQLCHAR*)"drop table if exists t", SQL_NTS);
            rc = SQLExecDirect(hstmt, (SQLCHAR*)"create table if not exists t (ts timestamp, name varchar(20), age int, sex varchar(8), text nchar(3))", SQL_NTS);
            rc = SQLExecDirect(hstmt, (SQLCHAR*)"select * from t", SQL_NTS);
            rc = SQLExecDirect(hstmt, (SQLCHAR*)"insert into t (ts, name, age, sex, text) values (1662861448752, 'name1', 20, 'male', '中国人')", SQL_NTS);
            // rc = SQLExecDirect(hstmt, (SQLCHAR*)"insert into t (ts, name, age, sex, text) values (1662861449753, 'name2', 30, 'female', '苏州人')", SQL_NTS);
            // rc = SQLExecDirect(hstmt, (SQLCHAR*)"insert into t (ts, name, age, sex, text) values (1662861450754, 'name3', null, null, null)", SQL_NTS);

            rc = SQLExecDirect(hstmt, (SQLCHAR*)"SELECT name FROM t", SQL_NTS);
            if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {

              SQLWCHAR Name[80];
              SQLLEN cbName;
              // SQLINTEGER Age;
              // SQLLEN cbAge;
              // SQLCHAR Sex[9];
              // SQLLEN cbSex;
              // SQLWCHAR Text[4];
              // SQLLEN cbText;
              rc = SQLBindCol(hstmt, 1, SQL_C_WCHAR,   Name, sizeof(Name)/sizeof(Name[0]), &cbName);
              // rc = SQLBindCol(hstmt, 2, SQL_C_SLONG, &Age,  0,                            &cbAge);
              // rc = SQLBindCol(hstmt, 3, SQL_C_CHAR,   Sex,  sizeof(Sex)/sizeof(Sex[0]),   &cbSex);
              // rc = SQLBindCol(hstmt, 4, SQL_C_WCHAR,  Text, sizeof(Text)/sizeof(Text[0]), &cbText);

              if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
                // Fetch and print each row of data. On an error, display a message and exit.
                for (int i=0 ; ; i++) {
                  rc = SQLFetch(hstmt);
                  if (rc == SQL_NO_DATA) {
                    rc = SQL_SUCCESS;
                    break;
                  }
                  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                    show_error();
                  if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
                  {
                    fwprintf(stderr, L"%d: %ld\n",
                        i+1,
                        cbName);
                    fwprintf(stderr, L"%d: %ld[%.*ls]\n",
                        i+1,
                        cbName,
                        cbName==SQL_NULL_DATA ? 4 : (int)cbName,
                        cbName==SQL_NULL_DATA ? L"null" : (const wchar_t*)Name);
                  }
                  else
                    break;
                }
              }
            }

            // Process data
            if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
              r = 0;
            }
            // SQLCancel(hstmt);
            SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
          }

          SQLDisconnect(hdbc);
        }

        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
      }
    }
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
  }

  return r ? 1 : 0;
}

