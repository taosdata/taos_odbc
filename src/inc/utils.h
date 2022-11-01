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

#ifndef _utils_h_
#define _utils_h_

#include "macros.h"

#include <stdlib.h>
#include <time.h>

EXTERN_C_BEGIN

#define TOD_SAFE_FREE(_p) if (_p) { free(_p); _p = NULL; }

typedef struct buffer_s           buffer_t;
struct buffer_s {
  char              *buf;
  size_t             cap;
  size_t             nr;
};

#define buffer_sprintf(_buf, _fmt, ...) ({                \
  buffer_t *_buffer = _buf;                               \
  char *__buf = _buffer->buf;                             \
  size_t _nn = 0;                                         \
  if (__buf) {                                            \
    __buf += _buffer->nr;                                 \
    _nn = _buffer->cap - _buffer->nr;                     \
  }                                                       \
  int _n = snprintf(__buf, _nn, _fmt, ##__VA_ARGS__);     \
  if (__buf) {                                            \
    if (_n>0 && (size_t)_n < _nn) {                       \
      _buffer->nr += _n;                                  \
    } else {                                              \
      _buffer->nr += _nn - 1;                             \
    }                                                     \
  }                                                       \
  _n;                                                     \
})

char *tod_strptime(const char *s, const char *format, struct tm *tm) FA_HIDDEN;

typedef struct static_pool_s                   static_pool_t;

static_pool_t* static_pool_create(size_t cap) FA_HIDDEN;
void static_pool_destroy(static_pool_t *pool) FA_HIDDEN;
void static_pool_reset(static_pool_t *pool) FA_HIDDEN;
unsigned char* static_pool_malloc(static_pool_t *pool, size_t sz) FA_HIDDEN;
unsigned char* static_pool_calloc(static_pool_t *pool, size_t sz) FA_HIDDEN;
unsigned char* static_pool_malloc_align(static_pool_t *pool, size_t sz, size_t align) FA_HIDDEN;
unsigned char* static_pool_calloc_align(static_pool_t *pool, size_t sz, size_t align) FA_HIDDEN;

typedef struct buf_s               buf_t;
struct buf_s {
  char               *base;
  size_t              cap;
};

void buf_release(buf_t *buf) FA_HIDDEN;
void* buf_realloc(buf_t *buf, size_t sz) FA_HIDDEN;

typedef struct buffers_s           buffers_t;
struct buffers_s {
  buf_t             **bufs;
  size_t              cap;
};

void buffers_release(buffers_t *buffers) FA_HIDDEN;
void* buffers_realloc(buffers_t *buffers, size_t idx, size_t sz) FA_HIDDEN;

EXTERN_C_END

#endif // _utils_h_

