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

#include "logger.h"

#include "helpers.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#ifndef _WIN32           /* { */
#include <syslog.h>
#include <unistd.h>
#include <sys/syscall.h>
#endif                   /* } */


uintptr_t tod_get_current_thread_id(void)
{
  pid_t id = syscall(__NR_gettid);
  return id;
}

static char logger_level_char(logger_level_t level)
{
  switch (level) {
    case LOGGER_VERBOSE:       return 'V';
    case LOGGER_DEBUG:         return 'D';
    case LOGGER_INFO:          return 'I';
    case LOGGER_WARN:          return 'W';
    case LOGGER_ERROR:         return 'E';
    case LOGGER_FATAL:         return 'F';
    default:                   return 'V';
  }
}

static logger_level_t _system_logger_level = LOGGER_FATAL;

static void _init_system_logger_level(void)
{
  const char *env = getenv("TAOS_ODBC_LOG_LEVEL");
  if (!env) {
    fprintf(stderr,
        "environment variable `TAOS_ODBC_LOG_LEVEL` could be set as `VERBOSE/DEBUG/INFO/WARN/ERROR/FATAL`, but not set. system logger level fall back to `FATAL`\n");
    return;
  }
  if (0 == tod_strcasecmp(env, "VERBOSE")) {
    _system_logger_level = LOGGER_VERBOSE;
    return;
  }
  if (0 == tod_strcasecmp(env, "DEBUG")) {
    _system_logger_level = LOGGER_DEBUG;
    return;
  }
  if (0 == tod_strcasecmp(env, "INFO")) {
    _system_logger_level = LOGGER_INFO;
    return;
  }
  if (0 == tod_strcasecmp(env, "WARN")) {
    _system_logger_level = LOGGER_WARN;
    return;
  }
  if (0 == tod_strcasecmp(env, "ERROR")) {
    _system_logger_level = LOGGER_ERROR;
    return;
  }
  if (0 == tod_strcasecmp(env, "FATAL")) {
    _system_logger_level = LOGGER_FATAL;
    return;
  }
  fprintf(stderr,
      "environment variable `TAOS_ODBC_LOG_LEVEL` shall be set as `VERBOSE/DEBUG/INFO/WARN/ERROR/FATAL`, but got `%s`. system logger level fall back to `FATAL`\n",
      env);
}

logger_level_t tod_get_system_logger_level(void)
{
  static int init = 0;
  if (!init) {
    static pthread_once_t once;
    pthread_once(&once, _init_system_logger_level);
    init = 1;
  }
  return _system_logger_level;
}

#ifdef _WIN32                /* { */
static char           _temp_file[MAX_PATH+1];
#else                        /* }{ */
static char           _temp_file[PATH_MAX+1];
#endif                       /* } */

static logger_t         _system_logger = {
  .logger             = NULL,
  .ctx                = NULL,
};

static void logger_to_temp(const char *log, void *ctx)
{
  (void)ctx;
  FILE *f = fopen(_temp_file, "a");
  if (!f) return;
  fprintf(f, "%s\n", log);
  fclose(f);
}

#ifdef _WIN32           /* { */
static void logger_to_event(const char *log, void *ctx)
{
  (void)ctx;
  (void)log;
  // TODO:
}
#else                   /* }{ */
static void logger_to_syslog(const char *log, void *ctx)
{
  (void)ctx;
  int priority = LOG_SYSLOG | LOG_USER | LOG_INFO;
  syslog(priority, "%s", log);
}
#endif                  /* } */

static void _init_system_logger(void)
{
  const char *env = getenv("TAOS_ODBC_LOGGER");
  if (!env) {
#ifdef _WIN32           /* { */
    fprintf(stderr,
        "environment variable `TAOS_ODBC_LOGGER` could be set as `stderr/temp/event`, but not set. system logger level fall back to `stderr`\n");
    return;
#else                   /* }{ */
    fprintf(stderr,
        "environment variable `TAOS_ODBC_LOGGER` could be set as `stderr/temp/syslog`, but not set. system logger level fall back to `stderr`\n");
    return;
#endif                  /* } */
  }

  if (0 == tod_strcasecmp(env, "stderr")) {
    return;
  }

  if (0 == tod_strcasecmp(env, "temp")) {
    const char *temp = getenv("TEMP");
    if (!temp) {
#ifdef _WIN32             /* { */
      temp = "C:\\Windows\\Temp";
#else                     /* }{ */
      temp = "/tmp";
#endif                    /* } */
    }
    snprintf(_temp_file, sizeof(_temp_file), "%s/taos_odbc.log", temp);
    _system_logger.logger   = logger_to_temp;
    _system_logger.ctx      = NULL;
    return;
  }

#ifndef _WIN32          /* { */
  if (0 == tod_strcasecmp(env, "syslog")) {
    _system_logger.logger   = logger_to_syslog;
    _system_logger.ctx      = NULL;
    return;
  }
#else                   /* }{ */
  if (0 == tod_strcasecmp(env, "event")) {
    _system_logger.logger   = logger_to_event;
    _system_logger.ctx      = NULL;
    return;
  }
#endif                  /* } */

#ifdef _WIN32           /* { */
    fprintf(stderr,
        "environment variable `TAOS_ODBC_LOGGER` could be set as `stderr/temp/event`, but got `%s`. system logger level fall back to `stderr`\n",
        env);
    return;
#else                   /* }{ */
    fprintf(stderr,
        "environment variable `TAOS_ODBC_LOGGER` could be set as `stderr/temp/syslog`, but got `%s`. system logger level fall back to `stderr`\n",
        env);
    return;
#endif                  /* } */
  return;
}

logger_t* tod_get_system_logger(void)
{
  static int init = 0;
  if (!init) {
    static pthread_once_t once;
    pthread_once(&once, _init_system_logger);
    init = 1;
  }
  return &_system_logger;
}

void tod_logger_write_impl(logger_t *logger, logger_level_t request, logger_level_t level,
    const char *file, int line, const char *func,
    const char *fmt, ...)
{
  if (request < level) return;

  char fn[1024]; fn[0] = '\0';
  tod_basename(file, fn, sizeof(fn));

  char buf[1024]; buf[0] = '\0';
  char *p = buf;
  size_t l = sizeof(buf);
  int n;

  n = snprintf(p, l, "%c:%zx:%s[%d]:%s():", logger_level_char(request), tod_get_current_thread_id(), fn, line, func);
  p += n;
  l -= n;

  va_list ap;
  va_start(ap, fmt);
  vsnprintf(p, l, fmt, ap);
  va_end(ap);

  fprintf(stderr, "%s\n", buf);
  if (logger && logger->logger) {
    logger->logger(buf, logger->ctx);
  }
}

