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

#define _XOPEN_SOURCE

#include "helpers.h"

#include <errno.h>
#ifndef _WIN32
#include <libgen.h>
#endif
#include <time.h>

#ifdef _WIN32
// NOTE: moved to win_port_cpp.cpp
#else
const char* tod_strptime(const char *s, const char *format, struct tm *tm)
{
  return strptime(s, format, tm);
}
#endif

static void _get_local_time(struct timeval *tv0, struct tm *tm0)
{
  gettimeofday(tv0, NULL);
  time_t t0 = (time_t)tv0->tv_sec;
  localtime_r(&t0, tm0);
}

const char* tod_get_format_current_local_timestamp_ms(char *s, size_t n)
{
  struct timeval tv0;
  struct tm tm0;
  _get_local_time(&tv0, &tm0);

  snprintf(s, n, "%04d-%02d-%02d %02d:%02d:%02d.%03zd",
    tm0.tm_year + 1900, tm0.tm_mon + 1, tm0.tm_mday,
    tm0.tm_hour, tm0.tm_min, tm0.tm_sec,
    (size_t)(tv0.tv_usec/1000));

  return s;
}

const char* tod_get_format_current_local_timestamp_us(char *s, size_t n)
{
  struct timeval tv0;
  struct tm tm0;
  _get_local_time(&tv0, &tm0);

  snprintf(s, n, "%04d-%02d-%02d %02d:%02d:%02d.%06zd",
    tm0.tm_year + 1900, tm0.tm_mon + 1, tm0.tm_mday,
    tm0.tm_hour, tm0.tm_min, tm0.tm_sec,
    (size_t)tv0.tv_usec);

  return s;
}

#ifdef _WIN32
char* tod_basename(const char *path, char *buf, size_t sz)
{
  char *file = NULL;
  DWORD dw;
  dw = GetFullPathName(path, (DWORD)sz, buf, &file);
  if (dw == 0) {
    errno = GetLastError();
    return NULL;
  }
  if (dw >= sz) {
    errno = E2BIG;
    return NULL;
  }

  if (!file) {
    size_t nr = strlen(buf);
    if (nr > 3) buf[nr-1] = '\0';
    char tmp[PATH_MAX+1];
    int n;
    n = snprintf(tmp, sizeof(tmp), "%s", buf);
    if (n < 0) {
      return NULL;
    }
    if ((size_t)n >= sizeof(tmp)) {
      errno = E2BIG;
      return NULL;
    }

    dw = GetFullPathName(tmp, (DWORD)sz, buf, &file);
    if (dw == 0) {
      errno = GetLastError();
      return NULL;
    }
    if (dw >= sz) {
      errno = E2BIG;
      return NULL;
    }

    if (!file) file = buf;
  }

  return file;
}
#elif defined(__APPLE__)
char* tod_basename(const char *path, char *buf, size_t sz)
{
  char tmp[PATH_MAX+1];
  int n;
  n = snprintf(tmp, sizeof(tmp), "%s", path);
  if (n < 0) {
    return NULL;
  }
  if ((size_t)n >= sizeof(tmp)) {
    errno = E2BIG;
    return NULL;
  }
  if ((size_t)n >= sz) {
    errno = E2BIG;
    return NULL;
  }
  return basename_r(tmp, buf);
}
#else
char* tod_basename(const char *path, char *buf, size_t sz)
{
  char tmp[PATH_MAX+1];
  int n;
  n = snprintf(tmp, sizeof(tmp), "%s", path);
  if (n < 0) {
    return NULL;
  }
  if ((size_t)n >= sizeof(tmp)) {
    errno = E2BIG;
    return NULL;
  }
  char *p = basename(tmp);
  if (!p) return NULL;

  n = snprintf(buf, sz, "%s", p);
  if (n < 0) {
    return NULL;
  }
  if ((size_t)n >= sz) {
    errno = E2BIG;
    return NULL;
  }
  return buf;
}
#endif

#ifdef _WIN32
char* tod_dirname(const char *path, char *buf, size_t sz)
{
  char *file = NULL;
  DWORD dw;
  dw = GetFullPathName(path, (DWORD)sz, buf, &file);
  if (dw == 0) {
    errno = GetLastError();
    return NULL;
  }
  if (dw >= sz) {
    errno = E2BIG;
    return NULL;
  }

  if (!file) {
    size_t nr = strlen(buf);
    if (nr > 3) buf[nr-1] = '\0';
    char tmp[PATH_MAX+1];
    int n;
    n = snprintf(tmp, sizeof(tmp), "%s", buf);
    if (n < 0) {
      return NULL;
    }
    if ((size_t)n >= sizeof(tmp)) {
      errno = E2BIG;
      return NULL;
    }

    dw = GetFullPathName(tmp, (DWORD)sz, buf, &file);
    if (dw == 0) {
      errno = GetLastError();
      return NULL;
    }
    if (dw >= sz) {
      errno = E2BIG;
      return NULL;
    }
  }

  if (file) {
    size_t nr = strlen(buf);
    if (nr > 3) file[-1] = '\0';
  }

  return buf;
}
#elif defined(__APPLE__)
char* tod_dirname(const char *path, char *buf, size_t sz)
{
  char tmp[PATH_MAX+1];
  int n = snprintf(tmp, sizeof(tmp), "%s", path);
  if (n < 0) return NULL;
  if ((size_t)n >= sizeof(tmp)) {
    errno = E2BIG;
    return NULL;
  }
  if ((size_t)n >= sz) {
    errno = E2BIG;
    return NULL;
  }

  return dirname_r(tmp, buf);
}
#else
char* tod_dirname(const char *path, char *buf, size_t sz)
{
  char tmp[PATH_MAX+1];
  int n;
  n = snprintf(tmp, sizeof(tmp), "%s", path);
  if (n < 0) {
    return NULL;
  }
  if ((size_t)n >= sizeof(tmp)) {
    errno = E2BIG;
    return NULL;
  }
  char *p = dirname(tmp);
  if (!p) return NULL;

  n = snprintf(buf, sz, "%s", p);
  if (n < 0) {
    return NULL;
  }
  if ((size_t)n >= sz) {
    errno = E2BIG;
    return NULL;
  }
  return buf;
}
#endif
