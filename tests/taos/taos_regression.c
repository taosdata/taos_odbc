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

typedef enum stage_s          stage_t;
enum stage_s {
  STAGE_INITED,
  STAGE_CONNECTED,
  STAGE_STATEMENT,
};

typedef struct arg_s                arg_t;

typedef int (*regress_f)(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt);

struct arg_s {
  const char           *ip;
  const char           *uid;
  const char           *pwd;
  const char           *db;
  uint16_t              port;

  const char           *name;

  const char           *regname;
  regress_f             regress;
};

typedef struct sql_s                     sql_t;
struct sql_s {
  const char            *sql;
  int                    __line__;
};

static int _executes(TAOS *taos, const sql_t *sqls, size_t nr)
{
  int r = 0;
  for (size_t i=0; i<nr; ++i) {
    const sql_t *sql = sqls + i;

    TAOS_RES *res = CALL_taos_query(taos, sql->sql);
    int e = taos_errno(res);
    if (e) {
      E("executing sql @[%dL]:%s\n"
        "failed:[%d]%s",
        sql->__line__, sql->sql,
        e, taos_errstr(res));
      r = -1;
    }
    if (res) CALL_taos_free_result(res);
    if (r) return -1;
  }

  return 0;
}

static int _dummy(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt)
{
  (void)arg;
  (void)stage;
  (void)taos;
  (void)stmt;
  return 0;
}

static int _query(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt)
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

    TAOS_RES *res = CALL_taos_query(taos, sql);
    int e = taos_errno(res);
    if ((!!e) ^ (!expected_ok)) {
      E("executing sql @[%dL]:%s", __line__, sql);
      if (expected_ok) {
        E("failed:[%d]%s", e, taos_errstr(res));
      } else {
        E("succeed unexpectedly");
      }
      r = -1;
    }
    if (res) CALL_taos_free_result(res);
    if (r) return -1;
  }

  return 0;
}

static int _prepare_on_connected(TAOS *taos)
{
  const sql_t sqls[] = {
    {"drop database if exists foo", __LINE__},
    {"create database if not exists foo", __LINE__},
    {"use foo", __LINE__},
    {"create table t (ts timestamp, name varchar(20), age int, sex int)", __LINE__},
    {"create table st1 (ts timestamp, age int) tags (name varchar(20))", __LINE__},
    {"insert into suzhou using st1 tags ('suzhou') values (now(), 23)", __LINE__},
    {"insert into suzhou (ts, age) values (now(), 24)", __LINE__},
    {"insert into suzhou using st1 tags ('suzhou') (ts) values (now())", __LINE__},
  };

  const size_t nr = sizeof(sqls) / sizeof(sqls[0]);
  return _executes(taos, sqls, nr);
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

static int _prepare_describe_insert(TAOS_STMT *stmt, const prepare_case_t *prepare_case)
{
  int r = 0;

  const char     *_sql               = prepare_case->sql;
  int             __line__           = prepare_case->__line__;
  int             _errCode           = prepare_case->errCode;
  uint8_t         _req_tbname        = prepare_case->req_tbname;
  int             _nr_tags           = prepare_case->nr_tags;
  int             _nr_fields         = prepare_case->nr_fields;

  int fieldNum = 0;
  TAOS_FIELD_E *fields = NULL;

  int colNum = 0;
  TAOS_FIELD_E *cols = NULL;

  r = CALL_taos_stmt_get_tag_fields(stmt, &fieldNum, &fields);
  if (r) {
    if (taos_errno(NULL) == TSDB_CODE_TSC_STMT_TBNAME_ERROR) {
      if (_errCode == taos_errno(NULL)) return 0;
      E("sql @[%dL]::%s", __line__, _sql);
      E("failed:[0x%x/%d]%s", taos_errno(NULL), taos_errno(NULL), taos_stmt_errstr(stmt));
      return -1;
    }
    if (taos_errno(NULL) == TSDB_CODE_TSC_STMT_API_ERROR) {
      r = CALL_taos_stmt_get_col_fields(stmt, &colNum, &cols);
      if (r) {
        if (_errCode == taos_errno(NULL)) return 0;
        E("sql @[%dL]::%s", __line__, _sql);
        E("failed:[0x%x/%d]%s", taos_errno(NULL), taos_errno(NULL), taos_stmt_errstr(stmt));
        return -1;
      }
      CALL_taos_stmt_reclaim_fields(stmt, cols); cols = NULL;

      if (_req_tbname) {
        E("sql @[%dL]::%s", __line__, _sql);
        E("expected tblname-parameter, but got ==[none]==");
        return -1;
      }

      if (_nr_tags != fieldNum) {
        E("sql @[%dL]::%s", __line__, _sql);
        E("expected %d tags, but got ==%d==", _nr_tags, fieldNum);
        return -1;
      }

      if (_nr_fields != colNum) {
        E("sql @[%dL]::%s", __line__, _sql);
        E("expected %d cols, but got ==%d==", _nr_fields, colNum);
        return -1;
      }

      if (_errCode) {
        E("sql @[%dL]::%s", __line__, _sql);
        E("errCode expected to be [0x%x/%d], but got ==TSDB_CODE_SUCCESS==", _errCode, _errCode);
        return -1;
      }
      return 0;
    }
    if (_errCode == taos_errno(NULL)) return 0;
    E("sql @[%dL]::%s", __line__, _sql);
    E("failed:[0x%x/%d]%s", taos_errno(NULL), taos_errno(NULL), taos_stmt_errstr(stmt));
    return -1;
  }
  CALL_taos_stmt_reclaim_fields(stmt, fields); fields = NULL;

  r = CALL_taos_stmt_get_col_fields(stmt, &colNum, &cols);
  if (r) {
    if (_errCode == taos_errno(NULL)) return 0;
    E("sql @[%dL]::%s", __line__, _sql);
    E("failed:[0x%x/%d]%s", taos_errno(NULL), taos_errno(NULL), taos_stmt_errstr(stmt));
    return -1;
  }
  CALL_taos_stmt_reclaim_fields(stmt, cols); cols = NULL;

  if (_req_tbname) {
    E("sql @[%dL]::%s", __line__, _sql);
    E("expected tblname-parameter, but got ==[none]==");
    return -1;
  }

  if (_nr_tags != fieldNum) {
    E("sql @[%dL]::%s", __line__, _sql);
    E("expected %d tags, but got ==%d==", _nr_tags, fieldNum);
    return -1;
  }

  if (_nr_fields != colNum) {
    E("sql @[%dL]::%s", __line__, _sql);
    E("expected %d cols, but got ==%d==", _nr_fields, colNum);
    return -1;
  }

  if (_errCode) {
    E("sql @[%dL]::%s", __line__, _sql);
    E("errCode expected to be [0x%x/%d], but got ==TSDB_CODE_SUCCESS==", _errCode, _errCode);
    return -1;
  }
  return 0;
}

static int _prepare_describe_non_insert(TAOS_STMT *stmt, const prepare_case_t *prepare_case)
{
  int r = 0;

  const char     *_sql               = prepare_case->sql;
  int             __line__           = prepare_case->__line__;
  int             _errCode           = prepare_case->errCode;
  int             _nr_fields         = prepare_case->nr_fields;

  int num_params = 0;

  r = CALL_taos_stmt_num_params(stmt, &num_params);
  if (r) {
    if (_errCode == taos_errno(NULL)) return 0;
    E("sql @[%dL]::%s", __line__, _sql);
    E("failed:[0x%x/%d]%s", taos_errno(NULL), taos_errno(NULL), taos_stmt_errstr(stmt));
    return -1;
  }

  if (num_params != _nr_fields) {
    E("sql @[%dL]::%s", __line__, _sql);
    E("expecting %d params, but got ==%d==", _nr_fields, num_params);
    return -1;
  }

  if (_errCode) {
    E("sql @[%dL]::%s", __line__, _sql);
    E("errCode expected to be [0x%x/%d], but got ==TSDB_CODE_SUCCESS==", _errCode, _errCode);
    return -1;
  }

  return 0;
}

static int _prepare_describe_sql(TAOS_STMT *stmt, const prepare_case_t *prepare_case)
{
  int r = 0;

  const char        *_sql               = prepare_case->sql;
  int                __line__           = prepare_case->__line__;
  int                _errCode           = prepare_case->errCode;
  uint8_t            _is_insert         = prepare_case->is_insert;

  r = CALL_taos_stmt_prepare(stmt, _sql, strlen(_sql));
  if (r) {
    if (_errCode == taos_errno(NULL)) return 0;
    E("sql @[%dL]::%s", __line__, _sql);
    E("failed:[0x%x/%d]%s", taos_errno(NULL), taos_errno(NULL), taos_stmt_errstr(stmt));
    return -1;
  }

  int is_insert = 0;
  r = CALL_taos_stmt_is_insert(stmt, &is_insert);
  if (r) {
    if (_errCode == taos_errno(NULL)) return 0;
    E("sql @[%dL]::%s", __line__, _sql);
    E("failed:[0x%x/%d]%s", taos_errno(NULL), taos_errno(NULL), taos_stmt_errstr(stmt));
    return -1;
  }

  if ((!!is_insert) ^ (!!_is_insert)) {
    E("sql @[%dL]::%s", __line__, _sql);
    if (is_insert) {
      E("expected insert statement, but failed");
    } else {
      E("expected non-insert statement, but failed");
    }
    return -1;
  }

  if (is_insert) {
    return _prepare_describe_insert(stmt, prepare_case);
  }

  return _prepare_describe_non_insert(stmt, prepare_case);
}

static int _prepare_on_statement(TAOS *taos, TAOS_STMT *stmt)
{
  (void)taos;

  int r = 0;

  const prepare_case_t _cases[] = {
    {__LINE__, "insert into ? (ts, age) values (?, ?)", TSDB_CODE_TSC_STMT_TBNAME_ERROR, 0, 0, 0, 1},
    {__LINE__, "insert into ? (ts, sex) values (?, ?)", TSDB_CODE_TSC_STMT_TBNAME_ERROR, 0, 0, 0, 1},
    {__LINE__, "insert into suzhou using ? tags (?) values (?, ?)", TSDB_CODE_PAR_TABLE_NOT_EXIST, 0, 0, 0, 1}, // NOTE: this is totally not recoverable
    {__LINE__, "insert into ? using st1 tags (?) values (?, ?)", TSDB_CODE_TSC_STMT_TBNAME_ERROR, 0, 0, 0, 1}, // NOTE: lazy-meta-loading

    {__LINE__, "select * from t where ts > ? and name = ?", TSDB_CODE_SUCCESS, 2, 0, 0, 0},
    {__LINE__, "insert into ? values (?, ?)", TSDB_CODE_TSC_STMT_TBNAME_ERROR, 2, 0, 1, 1},
    {__LINE__, "insert into suzhou using st1 tags (?) values (?, ?)", TSDB_CODE_SUCCESS, 2, 1, 0, 1},
    {__LINE__, "insert into suzhou using st1 tags ('suzhou') values (now(), 3)", TSDB_CODE_SUCCESS, 2, 1, 0, 1}, // FLAW: no parameter-place-marker, but succeed
    {__LINE__, "insert into ? using st1 tags (?) values (?, ?)", TSDB_CODE_TSC_STMT_TBNAME_ERROR, 0, 0, 0, 1},

    {__LINE__, "insert into ? using st1 tags (?) values (?, ?)", TSDB_CODE_TSC_STMT_TBNAME_ERROR, 0, 0, 0, 1},
    {__LINE__, "insert into t (ts, name) values (?, ?)", TSDB_CODE_SUCCESS, 2, 0, 0, 1},
    {__LINE__, "insert into t (ts, name) values (?, 'a')", TSDB_CODE_TSC_INVALID_OPERATION, 2, 0, 0, 1},

    {__LINE__, "select * from t where ts > ? and name = ? foo = ?", TSDB_CODE_PAR_SYNTAX_ERROR, 0, 0, 0, 0},
    {__LINE__, "insert into ? (ts, name) values (?, ?)", TSDB_CODE_TSC_STMT_TBNAME_ERROR, 0, 0, 0, 1},
    {__LINE__, "insert into t (ts, name) values (now(), 'a')", TSDB_CODE_SUCCESS, 2, 0, 0, 1},   // FLAW: no parameter-place-marker, but succeed

    {__LINE__, "insert into suzhou values (?, ?)", TSDB_CODE_SUCCESS, 2, 0, 0, 1},
  };

  const size_t nr = sizeof(_cases) / sizeof(_cases[0]);
  for (size_t i=0; i < nr; ++i) {
    const prepare_case_t *prepare_case = _cases + i;

    r = _prepare_describe_sql(stmt, prepare_case);
    if (r) return -1;
  }

  return 0;
}

static int _prepare(const arg_t *arg, const stage_t stage, TAOS *taos, TAOS_STMT *stmt)
{
  (void)arg;
  (void)stmt;

  switch (stage) {
    case STAGE_INITED:    return 0;
    case STAGE_CONNECTED: return _prepare_on_connected(taos);
    case STAGE_STATEMENT: return _prepare_on_statement(taos, stmt);
    default: return 0;
  }

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
};

static int on_statement(const arg_t *arg, TAOS *taos, TAOS_STMT *stmt)
{
  int r = 0;

  r = arg->regress(arg, STAGE_STATEMENT, taos, stmt);
  if (r) return -1;

  return 0;
}

static int on_connected(const arg_t *arg, TAOS *taos)
{
  int r = 0;

  r = arg->regress(arg, STAGE_CONNECTED, taos, NULL);
  if (r) return -1;

  TAOS_STMT  *stmt = CALL_taos_stmt_init(taos);
  if (!stmt) return -1;

  r = on_statement(arg, taos, stmt);

  CALL_taos_stmt_close(stmt);

  return r ? -1 : 0;
}

static int on_init(const arg_t *arg)
{
  int r = 0;

  r = arg->regress(arg, STAGE_INITED, NULL, NULL);
  if (r) return -1;

  TAOS *taos = CALL_taos_connect(arg->ip, arg->uid, arg->pwd, arg->db, arg->port);
  if (!taos) return -1;

  r = on_connected(arg, taos);

  CALL_taos_close(taos);

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

  r = CALL_taos_init();
  if (r) goto end;

  r = start(argc, argv);

  CALL_taos_cleanup();

end:

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");

  return !!r;
}

