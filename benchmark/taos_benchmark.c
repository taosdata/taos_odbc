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

#ifdef _WIN32           /* { */
#include <windows.h>
#endif                  /* } */

#include <taos.h>

#include <errno.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32           /* { */
static int gettimeofday(struct timeval *tp, void *tzp);
#else                   /* }{ */
#include <sys/time.h>
#endif                  /* } */

#define E(fmt, ...) fprintf(stderr, "@%d:%s():" fmt "\n", __LINE__, __func__, ##__VA_ARGS__)
#define SFREE(x) if (x) { free(x); x = NULL; }

#ifdef _WIN32           /* { */
static int gettimeofday(struct timeval *tp, void *tzp)
{
  static const uint64_t EPOCH = ((uint64_t) 116444736000000000ULL);
  LARGE_INTEGER li;
  FILETIME ft;
  GetSystemTimeAsFileTime(&ft);

  li.QuadPart   = ft.dwHighDateTime;
  li.QuadPart <<= 32;
  li.QuadPart  += ft.dwLowDateTime;
  li.QuadPart  -= EPOCH;

  tp->tv_sec    = (long)(li.QuadPart / 10000000);
  tp->tv_usec   = (li.QuadPart % 10000000) / 10;

  return 0;
}
#endif                  /* } */

static void _binds_release(TAOS_MULTI_BIND *binds, size_t nr)
{
  if (!binds) return;
  for (size_t i=0; i<nr; ++i) {
    TAOS_MULTI_BIND *p = binds + i;
    SFREE(p->buffer);
    SFREE(p->length);
    SFREE(p->is_null);
  }
}

static void _binds_free(TAOS_MULTI_BIND *binds, size_t nr)
{
  if (!binds) return;
  _binds_release(binds, nr);
  free(binds);
}

static int _bind_timestamp_prepare(TAOS_MULTI_BIND *bind, size_t rows)
{
  int       buffer_type          = TSDB_DATA_TYPE_TIMESTAMP;
  uintptr_t buffer_length        = sizeof(int64_t);
  void     *buffer               = calloc(rows, buffer_length);
  int32_t  *length               = NULL;
  char     *is_null              = NULL;
  int       num                  = (int)rows;

  
  bind->buffer_type          = buffer_type;
  bind->buffer_length        = buffer_length;
  bind->buffer               = buffer;
  bind->length               = length;
  bind->is_null              = is_null;
  bind->num                  = num;

  if (!bind->buffer) {
    E("oom");
    return -1;
  }

  time_t t0; time(&t0);
  int64_t v = t0 * 1000;
  int64_t *base = (int64_t*)bind->buffer;

  for (size_t i=0; i<rows; ++i) {
    base[i] = v + i;
    // E("timestamp:[%zd]", base[i]);
  }

  return 0;
}

static int _bind_varchar_prepare(TAOS_MULTI_BIND *bind, size_t rows, size_t len)
{
  int       buffer_type          = TSDB_DATA_TYPE_VARCHAR;
  uintptr_t buffer_length        = len + 1;
  void     *buffer               = calloc(rows, buffer_length);
  int32_t  *length               = calloc(rows, sizeof(*length));
  char     *is_null              = NULL;
  int       num                  = (int)rows;
  
  bind->buffer_type          = buffer_type;
  bind->buffer_length        = buffer_length;
  bind->buffer               = buffer;
  bind->length               = length;
  bind->is_null              = is_null;
  bind->num                  = num;

  if (!bind->buffer || !bind->length) {
    E("oom");
    return -1;
  }

  char *base = (char*)bind->buffer;

  for (size_t i=0; i<rows; ++i) {
    int v = rand();
    int n = snprintf(base + i * bind->buffer_length, bind->buffer_length, "%0*d", (int)(bind->buffer_length - 1), v);
    // E("varchar:[%s]", base + i * bind->buffer_length);
    bind->length[i] = n;
  }

  return 0;
}

static int _binds_prepare_v(TAOS_MULTI_BIND *binds, size_t rows, size_t cols, va_list ap)
{
  int r = 0;
  for (size_t i=0; i<cols; ++i) {
    int tsdb_type = va_arg(ap, int);
    switch (tsdb_type) {
      case TSDB_DATA_TYPE_TIMESTAMP:
        r = _bind_timestamp_prepare(binds + i, rows);
        if (r) return -1;
        break;
      case TSDB_DATA_TYPE_VARCHAR:
        r = _bind_varchar_prepare(binds + i, rows, va_arg(ap, int));
        if (r) return -1;
        break;
      default:
        E("TSDB_DATA_TYPE_xxx[%d/0x%x] not implemented yet", tsdb_type, tsdb_type);
        return -1;
    }
  }

  return 0;
}

static int _prepare_and_run(TAOS_STMT *stmt, const char *sql, TAOS_MULTI_BIND *binds)
{
  int r = 0;
  r = taos_stmt_prepare(stmt, sql, (unsigned long)strlen(sql));
  if (r) {
    E("taos_stmt_prepare failed for `%s`:[%d/0x%x]%s", sql, taos_errno(NULL), taos_errno(NULL), taos_errstr(NULL));
    return -1;
  }

  r = taos_stmt_bind_param_batch(stmt, binds);
  if (r) {
    E("taos_stmt_bind_param_batch failed for `%s`:[%d/0x%x]%s", sql, taos_errno(NULL), taos_errno(NULL), taos_errstr(NULL));
    return -1;
  }

  r = taos_stmt_add_batch(stmt);
  if (r) {
    E("taos_stmt_add_batch failed for `%s`:[%d/0x%x]%s", sql, taos_errno(NULL), taos_errno(NULL), taos_errstr(NULL));
    return -1;
  }

  r = taos_stmt_execute(stmt);
  if (r) {
    E("taos_stmt_execute failed for `%s`:[%d/0x%x]%s", sql, taos_errno(NULL), taos_errno(NULL), taos_errstr(NULL));
    return -1;
  }

  return 0;
}

static int _run_prepare(TAOS_STMT *stmt, const char *sql, size_t rows, size_t cols, TAOS_MULTI_BIND **binds, ...)
{
  int r = 0;

  TAOS_MULTI_BIND *p = (TAOS_MULTI_BIND*)calloc(cols, sizeof(*p));
  if (!p) return -1;
  *binds = p;

  va_list ap;
  va_start(ap, binds);
  r = _binds_prepare_v(*binds, rows, cols, ap);
  va_end(ap);

  if (r) return -1;

  struct timeval tv0 = {0};
  struct timeval tv1 = {0};
  gettimeofday(&tv0, NULL);
  r = _prepare_and_run(stmt, sql, *binds);
  gettimeofday(&tv1, NULL);

  double diff = difftime(tv1.tv_sec, tv0.tv_sec);
  diff += ((double)(tv1.tv_usec - tv0.tv_usec)) / 1000000;

  E("run_with_params(%s), with %zd rows / %zd cols:", sql, rows, cols);
  E("elapsed: %lfsecs", diff);
  E("throughput: %lf rows/secs", rows / diff);

  return r ? -1 : 0;
}

static void usage(const char *arg0)
{
  fprintf(stderr, "%s -h\n"
                  "  show this help page\n"
                  "%s --ip <ip> --usr <usr> --pwd <pwd> --db <db> --port <port>\n"
                  "  running benchmark\n",
                  arg0, arg0);
}

static int benchmark_case0(TAOS *taos, TAOS_STMT *stmt)
{
  (void)stmt;

  const char *sqls[] = {
    "create database if not exists bar",
    "use bar",
    "drop table if exists benchmark_case0",
    "create table benchmark_case0 (ts timestamp, name varchar(20))",
  };
  size_t nr_sqls = sizeof(sqls) / sizeof(sqls[0]);
  for (size_t i=0; i<nr_sqls; ++i) {
    const char *sql = sqls[i];
    TAOS_RES *res = taos_query(taos, sql);
    int e = taos_errno(res);
    if (e) {
      E("taos_query failed for `%s`:[%d/0x%x]%s", sql, e, e, taos_errstr(res));
    }
    if (res) {
      taos_free_result(res);
      res = NULL;
    }
    if (e) return -1;
  }

  const char *sql = "insert into benchmark_case0 (ts, name) values (?, ?)";
  const size_t nr_cols = 2;
  const size_t nr_rows = INT16_MAX; // 32767

  TAOS_MULTI_BIND *binds = NULL;
  int r = _run_prepare(stmt, sql, nr_rows, nr_cols, &binds, TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_VARCHAR, 20);

  if (binds) {
    _binds_free(binds, nr_cols);
    binds = NULL;
  }

  return r ? -1 : 0;
}

typedef struct taos_conn_cfg_s               taos_conn_cfg_t;
struct taos_conn_cfg_s {
  const char                    *ip;
  const char                    *usr;
  const char                    *pwd;
  const char                    *db;
  uint16_t                       port;
};

int main(int argc, char *argv[])
{
  taos_conn_cfg_t cfg = {0};
  for (int i=1; i<argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "-h") == 0) {
      usage(argv[0]);
      return 0;
    }
    if (strcmp(arg, "--ip") == 0) {
      ++i;
      if (i>=argc) {
        E("<ip> is expected after `--ip`, but got ==null==");
        return -1;
      }
      cfg.ip = argv[i];
      continue;
    }
    if (strcmp(arg, "--usr") == 0) {
      ++i;
      if (i>=argc) {
        E("<usr> is expected after `--usr`, but got ==null==");
        return -1;
      }
      cfg.usr = argv[i];
      continue;
    }
    if (strcmp(arg, "--pwd") == 0) {
      ++i;
      if (i>=argc) {
        E("<pwd> is expected after `--pwd`, but got ==null==");
        return -1;
      }
      cfg.pwd = argv[i];
      continue;
    }
    if (strcmp(arg, "--db") == 0) {
      ++i;
      if (i>=argc) {
        E("<db> is expected after `--db`, but got ==null==");
        return -1;
      }
      cfg.db = argv[i];
      continue;
    }
    if (strcmp(arg, "--port") == 0) {
      ++i;
      if (i>=argc) {
        E("<port> is expected after `--port`, but got ==null==");
        return -1;
      }
      char *end = NULL;
      errno = 0;
      long long port = strtoll(argv[i], &end, 0);
      if (end && *end) {
        E("<port:uint16_t> is expected after `--port`, but got ==%s==", argv[i]);
        return -1;
      }
      if (port == LLONG_MIN && errno == ERANGE) {
        E("<port:uint16_t> is expected after `--port`, but got ==%s==", argv[i]);
        return -1;
      }
      if (port == LLONG_MAX && errno == ERANGE) {
        E("<port:uint16_t> is expected after `--port`, but got ==%s==", argv[i]);
        return -1;
      }
      if (port == 0 && errno == EINVAL) {
        E("<port:uint16_t> is expected after `--port`, but got ==%s==", argv[i]);
        return -1;
      }
      if (port < 0 || port > UINT16_MAX) {
        E("<port:uint16_t> is expected after `--port`, but got ==%s==", argv[i]);
        return -1;
      }
      cfg.port = (uint16_t)port;
      continue;
    }
  }

  int r = 0;

#ifdef _WIN32                    /* { */
  srand((unsigned int)time(NULL));
#else                            /* }{ */
  srand(time(NULL));
#endif                           /* } */

  TAOS *taos = taos_connect(cfg.ip, cfg.usr, cfg.pwd, cfg.db, cfg.port);
  if (!taos) {
    E("taos_connect failed:[%d/0x%x]%s", taos_errno(NULL), taos_errno(NULL), taos_errstr(NULL));
    return -1;
  }

  TAOS_STMT *stmt = taos_stmt_init(taos);
  do {
    if (!stmt) {
      E("taos_stmt_init failed:[%d/0x%x]%s", taos_errno(NULL), taos_errno(NULL), taos_errstr(NULL));
      r = -1;
      break;
    }
    r = benchmark_case0(taos, stmt);
  } while (0);

  if (stmt) {
    taos_stmt_close(stmt);
    stmt = NULL;
  }

  taos_close(taos);
  taos = NULL;

  return r ? 1 : 0;
}

