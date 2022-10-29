/*
 * MIT License
 *
 * Copyright (c) 2022 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

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

