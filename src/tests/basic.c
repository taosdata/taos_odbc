/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
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

#include "../core/internal.h" // FIXME:

#include "conn.h"
#include "env.h"
#include "errs.h"
#include "helpers.h"
#include "logger.h"
#include "conn_parser.h"
#include "tls.h"
#include "utils.h"

#include <errno.h>
#include <string.h>

#define DUMP(fmt, ...)          printf(fmt "\n", ##__VA_ARGS__)

static int test_parser(void)
{
  env_t *env = env_create();

  conn_t *conn = conn_create(env);
  conn_unref(conn);

  env_unref(env);

  parser_param_t param = {0};
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
    // !topic
    "!topic demo",
    "!topic demo {}",
    "!topic demo {helloworld}",
    "!topic demo {helloworld=good}",
    "!topic demo {helloworld=good helloworld=good}",
    "!topic demo foo {helloworld=good helloworld=good}",
    "!topic demo {"
    " enable.auto.commit=true"
    " auto.commit.interval.ms=100"
    " group.id=cgrpName"
    " client.id=user_defined_name"
    " td.connect.user=root"
    " td.connect.pass=taosdata"
    " auto.offset.reset=earliest"
    " experimental.snapshot.enable=false"
    "}",
  };
  for (size_t i=0; i<sizeof(connection_strs)/sizeof(connection_strs[0]); ++i) {
    const char *s = connection_strs[i];
    int r = parser_parse(s, strlen(s), &param);
    if (r) {
      E("parsing:%s", s);
      E("location:(%d,%d)->(%d,%d)", param.row0, param.col0, param.row1, param.col1-1);
      E("failed:%s", param.err_msg);
    }

    parser_param_release(&param);
    if (r) return -1;
  }

  return 0;
}

static int _wildcard_match(const char *charset, const char *ex, const char *str, const int match)
{
  int r;
  wildex_t *wild = NULL;

  r = wildcomp_ex(&wild, charset, ex);
  if (r) {
    E("failed to compile wildcard `%s`", ex);
    return -1;
  }

  r = wildexec_ex(wild, charset, str);
  if ((!!r) != (!match)) {
    if (r) E("`%s` does not match by `%s`", str, ex);
    else   E("`%s` unexpectedly match by `%s`", str, ex);
    r = -1;
  } else {
    r = 0;
  }

  wildfree(wild);

  return r ? -1 : 0;
}

static int test_wildmatch(void)
{
  int r = 0;

#ifdef _WIN32            /* { */
  const char charset[] = "GB18030";
#else                    /* }{ */
  const char charset[] = "UTF-8";
#endif                   /* } */

  struct {
    const char *pattern;
    const char *content;
    int         match;
  } cases[] = {
    {"hello",            "hello",              1},
    {"%",                "hello",              1},
    {"%%%%%%%%%%%%%%%%", "hello",              1},
    {"_",                "h",                  1},
    {"_____",            "hello",              1},
    {"_%lo",             "hello",              1},
    {"_%el%o",           "hello",              1},
    {"_%ll%o",           "hello",              1},
    {"_a",               "人a",                1},
    {"a_",               "ah",                 1},
    {"a_",               "a人",                1},
    {"_a_",              "hah",                1},
    {"_%好啊",           "你好啊",             1},
    {"_%%好%%啊",        "你好啊",             1},
    {"_%%x好%%啊",       "你好啊",             0},
    {"你__",             "你好啊",             1},
    {"你%_%_%",          "你好啊",             1},
    {"你%%_%%_%%",       "你好啊",             1},
    {"你___",            "你好啊",             0},
    {"你\\_",            "你h",               0},
    {"你\\_",            "你_",               1},
    {"你\\_",            "你.",               0},
    {"",                 "",                   1},
  };

  for (size_t i=0; i<sizeof(cases)/sizeof(cases[0]); ++i) {
    r = _wildcard_match(charset, cases[i].pattern, cases[i].content, cases[i].match);
    if (r) return -1;
  }

  return 0;
}

static int test_basename_dirname(void)
{
  const char *path = "/Users/foo/bar.txt";
  char buf[PATH_MAX + 1];
  char *p;
  p = tod_basename(path, buf, sizeof(buf));
  if (strcmp(p, "bar.txt")) {
    E("`bar.txt` expected, but got ==%s==", p);
    return -1;
  }
  p = tod_dirname(path, buf, sizeof(buf));
#ifdef _WIN32
  errno_t err;
  char drive[16];
  char dir[PATH_MAX+1];
  char fname[PATH_MAX+1];
  char ext[PATH_MAX+1];
  err = _splitpath_s(p, drive, sizeof(drive), dir, sizeof(dir), fname, sizeof(fname), ext, sizeof(ext));
  char fullpath[PATH_MAX+1];
  snprintf(fullpath, sizeof(fullpath), "%s%s%s", dir, fname, ext);
  if (strcmp(fullpath, "\\Users\\foo")) {
    E("`\\Users\\foo` expected, but got ==%s==", fullpath);
    return -1;
  }
#else
  if (strcmp(p, "/Users/foo")) {
    E("`/Users/foo` expected, but got ==%s==", p);
    return -1;
  }
#endif
  return 0;
}

static int test_pthread_once_tick = 0;
static void test_pthread_once_init(void)
{
  ++test_pthread_once_tick;
}

static int test_pthread_once_step(void)
{
  static pthread_once_t once = PTHREAD_ONCE_INIT;
  int r = 0;
  r = pthread_once(&once, test_pthread_once_init);
  if (r) return -1;
  if (test_pthread_once_tick != 1) return -1;

  return 0;
}

static int test_pthread_once(void)
{
  int r = 0;
  r = test_pthread_once_step();
  if (r) return -1;

  r = test_pthread_once_step();
  if (r) return -1;

  return 0;
}

static int test_iconv(void)
{
  int r = 0;

  const char *tocode = "GB18030";
  const char *fromcode = "UTF-8";

  iconv_t cnv = iconv_open(tocode, fromcode);
  if (!cnv) {
    int e = errno;
    E("iconv_open(tocode:%s, fromcode:%s) failed:[%d]%s", tocode, fromcode, e, strerror(e));
    return -1;
  }

  do {
    char buf[1024];
    char utf8[] = "\xe4\xb8\xad\xe6\x96\x87"; //"中文";
    char          *inbuf                = utf8;
    size_t         inbytes              = sizeof(utf8);
    size_t         inbytesleft          = inbytes;
    char          *outbuf               = buf;
    size_t         outbytes;
    size_t         outbytesleft;
    size_t n;

    outbytes = 0;
    outbytesleft = outbytes;
    n = iconv(cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
    A(n == (size_t)-1, "-1 expected, but got ==%zd==", n);
    A(errno == E2BIG, "[%d]%s", errno, strerror(errno));
    A(inbytes - inbytesleft == 0, "0 expected, but got ==%zd==", inbytes - inbytesleft);
    A(inbuf - utf8 == 0, "");
    A(outbytes - outbytesleft == 0, "");
    A(outbuf - buf == 0, "");

    outbytes = 3;
    outbytesleft = outbytes;
    n = iconv(cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
    A(n == (size_t)-1, "-1 expected, but got ==%zd==", n);
    A(errno == E2BIG, "");
    A(inbytes - inbytesleft == 3, "3 expected, but got ==%zd==", inbytes - inbytesleft);
    A(inbuf - utf8 == 3, "");
    A(outbytes - outbytesleft == 2, "");
    A(outbuf - buf == 2, "");
    A(strncmp(buf, "\xd6\xd0", 2)==0, "");

    outbytes = 2;
    outbytesleft = outbytes;
    n = iconv(cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
    A(n == (size_t)-1, "-1 expected, but got ==%zd==", n);
    A(errno == E2BIG, "");
    A(inbytes - inbytesleft == 6, "6 expected, but got ==%zd==", inbytes - inbytesleft);
    A(outbytes - outbytesleft == 2, "");
    A(strncmp(buf, "\xd6\xd0\xce\xc4", 4)==0, "");

    outbytes = 1;
    outbytesleft = outbytes;
    n = iconv(cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
    A(n == 0, "0 expected, but got ==%zd==", n);
    A(inbytes - inbytesleft == 7, "7 expected, but got ==%zd==", inbytes - inbytesleft);
    A(outbytes - outbytesleft == 1, "");
    A(outbuf[-1] == '\0', "");
  } while (0);

  iconv_close(cnv);

  return r ? -1 : 0;
}

static int get_int(void)
{
  static int tick = 0;
  return tick++;
}

static int test_buffer(void)
{
  buffer_t str = {0};
  buffer_concat_fmt(&str, "%d", get_int());
  if (strcmp("0", str.base)) {
    E("0 expected, but got ==%s==", str.base);
    return -1;
  }
  int r = 0;
  do {
    int tick = get_int();
    if (tick != 1) {
      E("1 expected, but got ==%d==", tick);
      r = -1;
      break;
    }
  } while (0);
  buffer_release(&str);
  return r ? -1 : 0;
}

typedef int (*test_case_f)(void);

#define RECORD(x) {x, #x}

static struct {
  test_case_f           func;
  const char           *name;
} _cases[] = {
  RECORD(test_parser),
  RECORD(test_wildmatch),
  RECORD(test_basename_dirname),
  RECORD(test_pthread_once),
  RECORD(test_iconv),
  RECORD(test_buffer),
};

static void usage(const char *arg0)
{
  DUMP("usage:");
  DUMP("  %s -h", arg0);
  DUMP("");
  DUMP("supported test cases:");
  for (size_t i=0; i<sizeof(_cases)/sizeof(_cases[0]); ++i) {
    DUMP("  %s", _cases[i].name);
  }
}

static int run(const char *test_case)
{
  int r = 0;

  int tested = 0;

  for (size_t i=0; i<sizeof(_cases)/sizeof(_cases[0]); ++i) {
    const char *name = _cases[i].name;
    if (!test_case || strcmp(name, test_case) == 0) {
      tested = 1;
      r = _cases[i].func();
      if (r) return -1;
    }
  }

  if (!tested) {
    fprintf(stderr, "test case [%s] not exists\n", test_case);
    return -1;
  }

  return 0;
}

int main(int argc, char *argv[])
{
  int r = 0;

  int tested = 0;

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-h") == 0) {
      usage(argv[0]);
      return 0;
    }
    tested = 1;
    r = run(argv[i]);
    if (r) return -1;
  }

  if (!tested) r = run(NULL);

  fprintf(stderr,"==%s==\n", r ? "failure" : "success");

  return !!r;
}

