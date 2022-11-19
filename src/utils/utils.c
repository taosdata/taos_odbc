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

#include "utils.h"

#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

struct static_pool_s {
  size_t                        cap;
  size_t                        nr;
};

static_pool_t* static_pool_create(size_t cap)
{
  size_t sz = sizeof(static_pool_t) + cap;
  static_pool_t *pool = (static_pool_t*)malloc(sz);
  if (!pool) return NULL;
  pool->cap  = cap;
  pool->nr   = 0;
  return pool;
}

void static_pool_destroy(static_pool_t *pool)
{
  if (pool) free(pool);
}

unsigned char* static_pool_malloc(static_pool_t *pool, size_t sz)
{
  if (pool->nr + sz > pool->cap) return NULL;

  unsigned char *p = (unsigned char*)pool + sizeof(*pool) + pool->nr;
  pool->nr += sz;

  return p;
}

unsigned char* static_pool_calloc(static_pool_t *pool, size_t sz)
{
  unsigned char *p = static_pool_malloc(pool, sz);
  if (p) memset(p, 0, sz);

  return p;
}

unsigned char* static_pool_malloc_align(static_pool_t *pool, size_t sz, size_t align)
{
  if (pool->nr + sz > pool->cap) return NULL;

  size_t pos = pool->nr;
  if (align > 1) pos = (pos + align - 1) / align * align;

  if (pos + sz > pool->cap) return NULL;

  return static_pool_malloc(pool, pos + sz - pool->nr);
}

unsigned char* static_pool_calloc_align(static_pool_t *pool, size_t sz, size_t align)
{
  unsigned char *p = static_pool_malloc_align(pool, sz, align);
  if (p) memset(p, 0, sz);

  return p;
}

void buf_release(buf_t *buf)
{
  if (buf->base) {
    free(buf->base);
    buf->base = NULL;
  }
  buf->cap = 0;
}

void* buf_realloc(buf_t *buf, size_t sz)
{
  if (buf->cap >= sz) return buf->base;

  size_t cap = (sz + 1 + 15) / 16 * 16;

  char *p = (char*)realloc(buf->base, cap);
  if (!p) return NULL;

  buf->base = p;
  buf->cap  = cap;
  buf->base[sz] = '\0';

  return buf->base;
}

void buffers_release(buffers_t *buffers)
{
  for (size_t i=0; i<buffers->cap; ++i) {
    buf_t *buf = buffers->bufs[i];
    buf_release(buf);
    free(buf);
    buffers->bufs[i] = NULL;
  }
  free(buffers->bufs);
  buffers->bufs = NULL;
  buffers->cap = 0;
}

void* buffers_realloc(buffers_t *buffers, size_t idx, size_t sz)
{
  if (idx >= buffers->cap) {
    size_t cap = (idx + 1 + 15) / 16 * 16;
    buf_t **bufs = (buf_t**)realloc(buffers->bufs, sizeof(*bufs) * cap);
    if (!bufs) return NULL;
    for (size_t i = buffers->cap; i < cap; ++i) {
      bufs[i] = NULL;
    }
    buffers->bufs = bufs;
    buffers->cap = cap;
  }

  void *p = buf_realloc(buffers->bufs[idx], sz);
  if (!p) return NULL;

  return p;
}

typedef int (*wild_match_f)(wildex_t *wild, const char *s, size_t len, size_t inode);

typedef struct wildex_node_s             wildex_node_t;
struct wildex_node_s {
  wild_match_f          match;
  size_t                start, end;
};

static void _wildex_node_release(wildex_node_t *node)
{
  node->match = NULL;
  node->start = 0;
  node->end   = 0;
}

struct wildex_s {
  char                  *ex;
  wildex_node_t         *nodes;
  size_t                 cap;
  size_t                 nr;
};

static int _wild_exec(wildex_t *wild, const char *s, size_t len, size_t inode);
static int _wild_match_all(wildex_t *wild, const char *s, size_t len, size_t inode);
static int _wild_match_one(wildex_t *wild, const char *s, size_t len, size_t inode);
static int _wild_match_specific(wildex_t *wild, const char *s, size_t len, size_t inode);

static int _wild_match_all(wildex_t *wild, const char *s, size_t len, size_t inode)
{
  int r = 0;
  if (inode + 1 == wild->nr) return 0;
  const char *p = s;
  const char *end = s + len;
  while (p<end && *p) {
    r = _wild_exec(wild, p, end-p, inode + 1);
    if (r == 0) return 0;
    ++p;
  }
  return -1;
}

static int _wild_match_one(wildex_t *wild, const char *s, size_t len, size_t inode)
{
  if (!*s) return -1;
  wildex_node_t *node = wild->nodes + inode;
  const char *p = s;
  const char *end = s + len;
  for (size_t i=node->start; i<node->end; ++i) {
    if (p == end) return -1;
    if (!*p) return -1;
    ++p;
  }

  if (inode + 1 == wild->nr) {
    return (p<end) ? -1 : 0;
  }
  return _wild_exec(wild, p, end-p, inode + 1);
}

static int _wild_match_specific(wildex_t *wild, const char *s, size_t len, size_t inode)
{
  int r = 0;
  wildex_node_t *node = wild->nodes + inode;
  if (len < node->end - node->start) return -1;
  r = strncmp(s, wild->ex + node->start, node->end - node->start);
  if (r) return -1;

  if (inode + 1 == wild->nr) return 0;

  const char *end = s + len;
  const char *p = s + node->end - node->start;
  return _wild_exec(wild, p, end-p, inode + 1);
}

static void _wild_release(wildex_t *wild)
{
  for (size_t i=0; i<wild->nr; ++i) {
    wildex_node_t *node = wild->nodes + i;
    _wildex_node_release(node);
  }

  free(wild->nodes);
  wild->nodes = NULL;

  wild->cap = 0;
  wild->nr  = 0;

  if (wild->ex) {
    free(wild->ex);
    wild->ex = NULL;
  }
}

static int _wild_append(wildex_t *wild, wild_match_f match, size_t start, size_t end)
{
  if (wild->nr == wild->cap) {
    size_t cap = (wild->cap + 1 + 15) / 16 * 16;
    wildex_node_t *nodes = (wildex_node_t*)realloc(wild->nodes, cap * sizeof(*nodes));
    if (!nodes) return -1;
    wild->nodes = nodes;
    wild->cap   = cap;
  }

  wildex_node_t *node = &wild->nodes[wild->nr++];
  node->match = match;
  node->start = start;
  node->end   = end;

  return 0;
}

static int _wild_comp(wildex_t *wild)
{
  int r = 0;

  wild_match_f    prev = NULL;

  const char *s = wild->ex;
  const char *p = wild->ex;
  while (p) {
    if (*p == 0) {
      if (p > s) {
        r = _wild_append(wild, _wild_match_specific, s-wild->ex, p-wild->ex);
        if (r) return -1;
      }
      break;
    }
    if (*p == '%') {
      if (prev != _wild_match_all) {
        if (p > s) {
          r = _wild_append(wild, _wild_match_specific, s-wild->ex, p-wild->ex);
          if (r) return -1;
        }
        r = _wild_append(wild, _wild_match_all, 0, 0);
        if (r) return -1;
        prev = _wild_match_all;
      }
      s = ++p;
      continue;
    }
    if (*p == '_') {
      if (prev != _wild_match_one) {
        if (p > s) {
          r = _wild_append(wild, _wild_match_specific, s-wild->ex, p-wild->ex);
          if (r) return -1;
        }
        r = _wild_append(wild, _wild_match_one, p-wild->ex, p+1-wild->ex);
        if (r) return -1;
        prev = _wild_match_one;
      } else {
        wildex_node_t *node = wild->nodes + wild->nr - 1;
        ++node->end;
      }
      s = ++p;
      continue;
    }
    ++p;
  }

  return 0;
}

int wildcomp(wildex_t **pwild, const char *wildex)
{
  int r = 0;

  wildex_t *wild = (wildex_t*)calloc(1, sizeof(*wild));
  if (!wild) return -1;

  do {
    wild->ex = strdup(wildex);
    if (!wild->ex) break;
    r = _wild_comp(wild);
    if (r == 0) {
      *pwild = wild;
      return 0;
    }
  } while (0);

  _wild_release(wild);
  free(wild);

  return -1;
}

static int _wild_exec(wildex_t *wild, const char *s, size_t len, size_t inode)
{
  if (inode == wild->nr) return -1;

  wildex_node_t *node = wild->nodes + inode;
  wild_match_f match = node->match;
  return match(wild, s, len, inode);
}

int wildexec_n(wildex_t *wild, const char *str, size_t len)
{
  const char *s = str;

  size_t inode = 0;

  if (!s) return -1;

  return _wild_exec(wild, s, len, inode);
}

void wildfree(wildex_t *wild)
{
  if (!wild) return;
  _wild_release(wild);
  free(wild);
}

static void _check_table_or_stable(const char *s, size_t n, int *table, int *stable)
{
  if (n == 5 && strncmp(s, "TABLE", 5) == 0) {
    *table  = 1;
    return;
  }
  if (n == 6 && strncmp(s, "STABLE", 6) == 0) {
    *stable = 1;
    return;
  }
}

int table_type_parse(const char *table_type, int *table, int *stable)
{
  const char *s = table_type;
  const char *p = table_type;

  char c;

  *table  = 0;
  *stable = 0;

step1:
  if (isblank(*p)) {
    s = ++p;
    goto step1;
  }
  if (!*p) return 0;

  if (*s == '\'') {
    c = *s;
  } else {
    c = ',';
  }

step2:
  ++p;
  if (!*p) {
    if (c != ',') return -1;
    _check_table_or_stable(s, p-s, table, stable);
    return 0;
  }
  if (*p == c) {
    if (c == ',') {
      _check_table_or_stable(s, p-s, table, stable);
      s = ++p;
      goto step1;
    }
    _check_table_or_stable(s+1, p-s-1, table, stable);
    s = ++p;
    goto step3;
  }
  if (isblank(*p)) {
    if (c != ',') return -1;
    _check_table_or_stable(s, p-s, table, stable);
    s = ++p;
    goto step1;
  }
  if (*p == ',') return -1;
  if (*p == '\'') return -1;
  goto step2;

step3:
  if (*p == ',') {
    s = ++p;
    goto step1;
  }
  if (isblank(*p)) {
    s = ++p;
    goto step3;
  }
  if (*p) return -1;
  return 0;
}

void buffer_reset(buffer_t *str)
{
  str->nr = 0;
  if (str->base && str->sz > 0) str->base[0] = '\0';
}

void buffer_release(buffer_t *str)
{
  if (str->base) {
    free(str->base);
    str->base = NULL;
  }
  str->sz = 0;
  str->nr = 0;
}

int buffer_expand(buffer_t *str, size_t sz)
{
  if (str->nr + sz <= str->sz) return 0;
  sz = (str->nr + sz + 15) / 16 * 16;
  char *base = (char*)realloc(str->base, sz);
  if (!base) return -1;
  str->base          = base;
  str->sz            = sz;
  str->base[str->nr] = '\0';
  return 0;
}

int buffer_copy_n(buffer_t *str, const unsigned char *mem, size_t len)
{
  int r = 0;
  str->nr = 0;

  r = buffer_expand(str, len + 1);
  if (r) return -1;

  memcpy(str->base + str->nr, mem, len);
  str->nr            += len;
  str->base[str->nr]  = '\0';
  return 0;
}

int buffer_concat_n(buffer_t *str, const char *s, size_t len)
{
  int r = 0;
  r = buffer_expand(str, len+1);
  if (r) return -1;
  strncpy(str->base + str->nr, s, len);
  str->nr            += len;
  str->base[str->nr]  = '\0';
  return 0;
}

static int _buffer_vconcat(buffer_t *str, const char *fmt, va_list ap)
{
  va_list apx;
  va_copy(apx, ap);

  int r = 0;
  int n = vsnprintf(str->base + str->nr, str->sz - str->nr, fmt, apx);
  va_end(apx);

  if (n < 0) return -1;

  if ((size_t)n < str->sz - str->nr) {
    str->nr            += n;
    str->base[str->nr]  = '\0';
    return 0;
  }

  r = buffer_expand(str, n+1);
  if (r) return -1;

  if (n != vsnprintf(str->base + str->nr, str->sz - str->nr, fmt, ap)) return -1;

  str->nr            += n;
  str->base[str->nr]  = '\0';
  return 0;
}

int buffer_vconcat(buffer_t *str, const char *fmt, va_list ap)
{
  int r = 0;
  size_t old_nr = str->nr;

  r = _buffer_vconcat(str, fmt, ap);
  if (r) {
    str->nr = old_nr;
    if (str->base) str->base[str->nr] = '\0';
    return -1;
  }

  return 0;
}

int buffer_concat_fmt(buffer_t *str, const char *fmt, ...)
{
  int r;
  va_list ap;
  va_start(ap, fmt);
  r = buffer_vconcat(str, fmt, ap);
  va_end(ap);
  return r ? -1 : 0;
}

static int _buffer_concat_replacement_n(buffer_t *str, const char *s, size_t len)
{
  int r = 0;
  const char *end = s + len;
  const char *p = s;
  while (p < end && *p) {
    if (*p == '\'') {
      if (p>s) {
        r = buffer_concat_n(str, s, p-s);
        if (r) return -1;
      }
      r = buffer_concat_n(str, "''", 2);
      if (r) return -1;
      s = ++p;
      continue;
    }
    ++p;
  }
  if (p > s) return buffer_concat_n(str, s, p-s);
  return 0;
}

int buffer_concat_replacement_n(buffer_t *str, const char *s, size_t len)
{
  int r = 0;
  size_t old_nr = str->nr;
  r = _buffer_concat_replacement_n(str, s, len);
  if (r == 0) return 0;

  str->nr = old_nr;
  if (str->base) str->base[str->nr] = '\0';
  return -1;
}

