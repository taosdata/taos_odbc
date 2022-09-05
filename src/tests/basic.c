#include "conn.h"
#include "env.h"
#include "parser.h"

#include <string.h>

int main(void)
{
  env_t *env = env_create();

  conn_t *conn = conn_create(env);
  conn_unref(conn);

  env_unref(env);

  parser_param_t param = {};
  // param.debug_flex = 1;
  // param.debug_bison = 1;

  const char *connection_strs[] = {
    " driver = {    ax bcd ; ;   }; dsn=server; uid=xxx; pwd=yyy",
    // https://www.connectionstrings.com/dsn/
    "DSN=myDsn;Uid=myUsername;Pwd=;",
    "FILEDSN=c:\\myDsnFile.dsn;Uid=myUsername;Pwd=;",
    "Driver={any odbc driver's name};OdbcKey1=someValue;OdbcKey2=someValue;",
    // http://lunar.lyris.com/help/Content/sample_odbc_connection_str.html
    "Driver={SQL Server};Server=lmtest;Database=lmdb;Uid=sa;Pwd=pass",
    "DSN=DSN_Name;Server=lmtest;Uid=lmuser;Pwd=pass",
    "Driver={MySQL ODBC 3.51 driver};server=lmtest;database=lmdb;uid=mysqluser;pwd=pass;",
    "Driver={MySQL ODBC 3.51 driver};server=lmtest;database=lmdb;uid=mysqluser;pwd=pass;",
    "Driver={MySQL ODBC 3.51 driver};server=lm.test;database=lmdb;uid=mysqluser;pwd=pass;",
    "Driver={MySQL ODBC 3.51 driver};server=lmtest:378;database=lmdb;uid=mysqluser;pwd=pass;",
    "Driver={MySQL ODBC 3.51 driver};server=lmtest:378378378378;database=lmdb;uid=mysqluser;pwd=pass;",
  };
  for (size_t i=0; i<sizeof(connection_strs)/sizeof(connection_strs[0]); ++i) {
    const char *s = connection_strs[i];
    int r = parser_parse(s, strlen(s), &param);
    parser_param_release(&param);
    if (r) return 1;
  }

  return 0;
}

