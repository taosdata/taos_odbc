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

#define _XOPEN_SOURCE

#include "utils.h"

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

char *tod_strptime(const char *s, const char *format, struct tm *tm)
{
  return strptime(s, format, tm);
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

