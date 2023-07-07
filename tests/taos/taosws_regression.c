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

#include <taosws.h>

#include "helpers.h"

#include <stdio.h>

#define DUMP(fmt, ...)          printf(fmt "\n", ##__VA_ARGS__)

typedef enum stage_s          stage_t;
enum stage_s {
  STAGE_INITED,
  STAGE_CONNECTED,
  STAGE_STATEMENT,
};

typedef struct arg_s                arg_t;

typedef int (*regress_f)(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt);

struct arg_s {
  const char           *url;

  const char           *name;

  const char           *regname;
  regress_f             regress;
};

typedef struct sql_s                     sql_t;
struct sql_s {
  const char            *sql;
  int                    __line__;
};

static int _dummy(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt)
{
  (void)arg;
  (void)stage;
  (void)taos;
  (void)stmt;
  return 0;
}

static int _query(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt)
{

  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage != STAGE_CONNECTED) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"insert into foo.t (ts, name) values (now(), 'a')", __LINE__, 0},
    {"create table foo.t (ts timestamp, name varchar(20))", __LINE__, 1},
    {"insert into foo.t (ts, name) values (now(), 'a')", __LINE__, 1},
    {"insert into foo.t (ts, name) values (now(), ?)", __LINE__, 0},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    WS_RES *res = ws_query(taos, sql);
    int e = ws_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, ws_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) ws_free_result(res);
    if (r) return -1;
  }

  return 0;
}

typedef struct prepare_case_s                  prepare_case_t;
struct prepare_case_s {
    int                        __line__;
    const char                *sql;

    int                        errCode;

    int                        nr_fields;
    int                        nr_tags;

    uint8_t                    req_tbname:1;
    uint8_t                    is_insert:1;
};

static int _prepare(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage == STAGE_CONNECTED) {
    r = _query(arg, stage, taos, stmt);
  } else if (stage == STAGE_STATEMENT) {
    const char *sql = "show databases";
    size_t      nr  = strlen(sql);
    r = ws_stmt_prepare(stmt, sql, (unsigned long)nr);
  }

  return r;
}

static int _fetch(const arg_t *arg, const stage_t stage, WS_TAOS *taos, WS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  int r = 0;

  if (stage != STAGE_CONNECTED) return 0;

  const struct {
    const char          *sql;
    int                  __line__;
    uint8_t              ok:1;
  } _cases[] = {
    {"drop database if exists foo", __LINE__, 1},
    {"create database if not exists foo", __LINE__, 1},
    {"insert into foo.t (ts, name) values (now(), 'a')", __LINE__, 0},
    {"create table foo.t (ts timestamp, name varchar(20))", __LINE__, 1},
    {"insert into foo.t (ts, name) values (now(), null)", __LINE__, 1},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i<nr; ++i) {
    const char *sql         = _cases[i].sql;
    uint8_t     expected_ok = _cases[i].ok;
    int         __line__    = _cases[i].__line__;

    WS_RES *res = ws_query(taos, sql);
    int e = ws_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, ws_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) ws_free_result(res);
    if (r) return -1;
  }

  // insert into foo.t (ts, name) values (now(), null)
  WS_RES *res = ws_query(taos, "select name from foo.t");
  int e = ws_errno(res);
  if (!!e) {
    E("failed:[%d]%s", e, ws_errstr(res));
    r = -1;
  }
  if (res) {
    const void *ptr = NULL;
    int32_t rows = 0;
    r = ws_fetch_block(res, &ptr, &rows);
    E("r:%d;ptr:%p;rows:%d", r, ptr, rows);
  }
  if (res) {
    uint8_t ty = 0;
    uint32_t len = 0;
    const void *p = NULL;
    int row = 0, col = 0;

    ty = 0; len = 0; p = NULL;
    p = ws_get_value_in_block(res, row, col, &ty, &len);
    if (p) {
      E("expected null, but got ==(%d,%d):p:%p;ty:%d;len:%d==", row, col, p, ty, len);
      r = -1;
    }
  }
  if (res) ws_free_result(res);
  if (r) return -1;

  return 0;
}

#define RECORD(x) {x, #x}

static struct {
  regress_f             func;
  const char           *name;
} _regressions[] = {
  RECORD(_dummy),
  RECORD(_query),
  RECORD(_prepare),
  RECORD(_fetch),
};

static int on_statement(const arg_t *arg, WS_TAOS *taos, WS_STMT *stmt)
{
  int r = 0;

  r = arg->regress(arg, STAGE_STATEMENT, taos, stmt);
  if (r) return -1;

  return 0;
}

static int on_connected(const arg_t *arg, WS_TAOS *taos)
{
  int r = 0;

  r = arg->regress(arg, STAGE_CONNECTED, taos, NULL);
  if (r) return -1;

  WS_STMT *stmt = ws_stmt_init(taos);
  if (!stmt) E("%d:%s", ws_errno(NULL), ws_errstr(NULL));
  if (!stmt) return -1;

  r = on_statement(arg, taos, stmt);

  ws_stmt_close(stmt);

  return r ? -1 : 0;
}

static int on_init(const arg_t *arg)
{
  int r = 0;

  r = arg->regress(arg, STAGE_INITED, NULL, NULL);
  if (r) return -1;

  WS_TAOS *taos = ws_connect_with_dsn(arg->url);
  if (!taos) E("%d:%s", ws_errno(NULL), ws_errstr(NULL));
  if (!taos) return -1;

  r = on_connected(arg, taos);

  ws_close(taos);

  return r ? -1 : 0;
}

static int run_regress(const arg_t *arg)
{
  int r = 0;

  r = on_init(arg);

  return r ? -1 : 0;
}

static int start_by_arg(arg_t *arg)
{
  int r = 0;

  const size_t nr = sizeof(_regressions) / sizeof(_regressions[0]);
  for (size_t i=0; i<nr; ++i) {
    arg->regname = _regressions[i].name;
    arg->regress = _regressions[i].func;
    if ((!arg->name) || tod_strcasecmp(arg->name, arg->regname) == 0) {
      r = run_regress(arg);
      if (r) return -1;
    }
  }

  return 0;
}

static void usage(const char *arg0)
{
  DUMP("usage:");
  DUMP("  %s -h", arg0);
  DUMP("  %s [--url <URL>] [name]", arg0);
  DUMP("");
  DUMP("supported regression names:");
  for (size_t i=0; i<sizeof(_regressions)/sizeof(_regressions[0]); ++i) {
    DUMP("  %s", _regressions[i].name);
  }
}

static int start(int argc, char *argv[])
{
  int r = 0;

  arg_t arg = {0};
  arg.url = "taos://localhost:6041";

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-h") == 0) {
      usage(argv[0]);
      return 0;
    }
    if (strcmp(argv[i], "--url") == 0) {
      ++i;
      if (i>=argc) break;
      arg.url = argv[i];
      continue;
    }

    arg.name = argv[i];
    r = start_by_arg(&arg);
    if (r) return -1;
  }

  if (arg.name) return 0;

  return start_by_arg(&arg);
}

int main(int argc, char *argv[])
{
  int r;

  r = start(argc, argv);

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");

  return !!r;
}

