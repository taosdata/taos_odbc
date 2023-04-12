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

#include "taos_helpers.h"

#include "../test_helper.h"

#define DUMP(fmt, ...)          printf(fmt "\n", ##__VA_ARGS__)

typedef struct arg_s                arg_t;
struct arg_s {
  const char           *ip;
  const char           *uid;
  const char           *pwd;
  const char           *db;
  uint16_t              port;

  const char      *name;
};

typedef enum stage_s          stage_t;
enum stage_s {
  STAGE_BEFORE_INIT,
  STAGE_INITED,
  STAGE_CONNECTED,
  STAGE_STATEMENT,
};

typedef int (*regress_f)(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt);

static int _dummy(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt)
{
  (void)arg;
  (void)stage;
  (void)taos;
  (void)stmt;
  return 0;
}

#define RECORD(x) {x, #x}

static struct {
  regress_f             func;
  const char           *name;
} _regressions[] = {
  RECORD(_dummy),
};

static int on_statement(const arg_t *arg, TAOS *taos, TAOS_STMT *stmt)
{
  int r = 0;

  const size_t nr = sizeof(_regressions) / sizeof(_regressions[0]);

  for (size_t i=0; i<nr; ++i) {
    regress_f   regress = _regressions[i].func;
    const char *name    = _regressions[i].name;
    if ((!arg->name) || tod_strcasecmp(arg->name, name) == 0) {
      r = regress(arg, STAGE_STATEMENT, taos, stmt);
      if (r) return -1;
    }
  }

  return 0;
}

static int on_connected(const arg_t *arg, TAOS *taos)
{
  int r = 0;

  const size_t nr = sizeof(_regressions) / sizeof(_regressions[0]);

  for (size_t i=0; i<nr; ++i) {
    regress_f   regress = _regressions[i].func;
    const char *name    = _regressions[i].name;
    if ((!arg->name) || tod_strcasecmp(arg->name, name) == 0) {
      r = regress(arg, STAGE_CONNECTED, taos, NULL);
      if (r) return -1;
    }
  }

  TAOS_STMT  *stmt = CALL_taos_stmt_init(taos);
  if (!stmt) return -1;

  r = on_statement(arg, taos, stmt);

  CALL_taos_stmt_close(stmt);

  return r ? -1 : 0;
}

static int on_init(const arg_t *arg)
{
  int r = 0;

  const size_t nr = sizeof(_regressions) / sizeof(_regressions[0]);

  for (size_t i=0; i<nr; ++i) {
    regress_f   regress = _regressions[i].func;
    const char *name    = _regressions[i].name;
    if ((!arg->name) || tod_strcasecmp(arg->name, name) == 0) {
      r = regress(arg, STAGE_INITED, NULL, NULL);
      if (r) return -1;
    }
  }

  TAOS *taos = CALL_taos_connect(arg->ip, arg->uid, arg->pwd, arg->db, arg->port);
  if (!taos) return -1;

  r = on_connected(arg, taos);

  CALL_taos_close(taos);

  return r ? -1 : 0;
}

static int start_by_arg(const arg_t *arg)
{
  int r = 0;

  const size_t nr = sizeof(_regressions) / sizeof(_regressions[0]);
  for (size_t i=0; i<nr; ++i) {
    regress_f   regress = _regressions[i].func;
    const char *name    = _regressions[i].name;
    if ((!arg->name) || tod_strcasecmp(arg->name, name) == 0) {
      r = regress(arg, STAGE_BEFORE_INIT, NULL, NULL);
      if (r) return -1;
    }
  }

  r = CALL_taos_init();
  if (r) return -1;

  r = on_init(arg);

  CALL_taos_cleanup();

  return r ? -1 : 0;
}

static void usage(const char *arg0)
{
  DUMP("usage:");
  DUMP("  %s -h", arg0);
  DUMP("  %s [--ip <IP>] [--uid <UID>] [--pwd <PWD>] [--db <DB>] [--port <PORT>] [name]", arg0);
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

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-h") == 0) {
      usage(argv[0]);
      return 0;
    }
    if (strcmp(argv[i], "--ip") == 0) {
      ++i;
      if (i>=argc) break;
      arg.ip= argv[i];
      continue;
    }
    if (strcmp(argv[i], "--uid") == 0) {
      ++i;
      if (i>=argc) break;
      arg.uid = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--pwd") == 0) {
      ++i;
      if (i>=argc) break;
      arg.pwd = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--db") == 0) {
      ++i;
      if (i>=argc) break;
      arg.db = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--port") == 0) {
      ++i;
      if (i>=argc) break;
      arg.port = atoi(argv[i]);
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

