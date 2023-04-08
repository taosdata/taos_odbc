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

#include "os_port.h"
#include "utils.h"

#include "charset.h"
#include "tls.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#define TERMINATOR_MAX 4

#define DW(fmt, ...) if (0) { fprintf(stderr, "%s[%d]:%s():" fmt "\n", __FILE__, __LINE__, __func__, ##__VA_ARGS__); }
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

void mem_release(mem_t *mem)
{
  TOD_SAFE_FREE(mem->base);
  mem->cap = 0;
  mem->nr  = 0;
}

void mem_reset(mem_t *mem)
{
  mem->nr = 0;
}

int mem_expand(mem_t *mem, size_t delta)
{
  size_t cap = mem->cap + delta;
  if (cap < mem->cap) {
    errno = E2BIG;
    return -1;
  }
  return mem_keep(mem, cap);
}

int mem_keep(mem_t *mem, size_t cap)
{
  if (cap <= mem->cap) return 0;
  unsigned char *p = realloc(mem->base, cap);
  if (!p) return -1;
  mem->base = p;
  mem->cap  = cap;
  return 0;
}

int mem_conv(mem_t *mem, iconv_t cnv, const char *src, size_t len)
{
  int r = 0;

  char           *inbuf;
  size_t          inbytesleft;
  char           *outbuf;
  size_t          outbytesleft;

  size_t n;
  int e;

again:
  if (mem->base == NULL) {
    int r = mem_keep(mem, len + TERMINATOR_MAX);
    if (r) return -1;
  }

  inbuf          = (char*)src;
  inbytesleft    = len;
  outbuf         = (char*)mem->base;
  outbytesleft   = mem->cap;

  n = iconv(cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
  e = errno;
  iconv(cnv, NULL, NULL, NULL, NULL);
  if (n == (size_t)-1) {
    if (e != E2BIG) return -1;
    size_t indelta = len - inbytesleft;
    double outdelta = (double)(mem->cap - outbytesleft);
    size_t delta = (size_t)(outdelta / indelta * inbytesleft);
    r = mem_expand(mem, delta + TERMINATOR_MAX);
    if (r) return -1;
    goto again;
  }

  if (inbytesleft) return -1;
  mem->nr = mem->cap - outbytesleft;
  if (outbytesleft < TERMINATOR_MAX) {
    r = mem_expand(mem, TERMINATOR_MAX);
    if (r) return -1;
  }

  outbuf = (char*)mem->base + mem->nr;
  memset(outbuf, 0, TERMINATOR_MAX);

  return 0;
}

static iconv_t _mem_acquire_conv(const char *fromcode, const char *tocode)
{
  charset_conv_t *cnv = tls_get_charset_conv(fromcode, tocode);
  if (!cnv) return (iconv_t)0;
  return charset_conv_get(cnv);
}

static void _mem_revoke_conv(iconv_t cnv)
{
  (void)cnv;
}

int mem_conv_ex(mem_t *mem, const char *src_charset, const char *src, size_t len, const char *dst_charset)
{
  int r = 0;

  iconv_t cnv = _mem_acquire_conv(src_charset, dst_charset);
  do {
    if (cnv == (iconv_t)0) return -1;

    mem_reset(mem);
    r = mem_conv(mem, cnv, src, len);
    // reset initial state of `iconv_t`
    iconv(cnv, NULL, NULL, NULL, NULL);
  } while (0);
  _mem_revoke_conv(cnv);

  return r;
}

int mem_copy(mem_t *mem, const char *src)
{
  int r = 0;
  size_t len = strlen(src);
  mem_reset(mem);
  r = mem_keep(mem, len + 1);
  if (r) return -1;
  memcpy(mem->base, src, len);
  mem->base[len] = '\0';
  mem->nr = len;
  return 0;
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

typedef int (*wild_match_f)(wildex_t *wild, const int32_t *s, size_t nr, size_t inode);

typedef struct wildex_node_s             wildex_node_t;
struct wildex_node_s {
  wild_match_f          match;
  const int32_t        *base;
  size_t                nr;
};

static void _wildex_node_release(wildex_node_t *node)
{
  node->match = NULL;
  node->base  = NULL;
  node->nr    = 0;
}

struct wildex_s {
  char                  *ex;
  mem_t                  ex_ucs4;
  wildex_node_t         *nodes;
  size_t                 cap;
  size_t                 nr;
};

static int _wild_exec(wildex_t *wild, const int32_t *s, size_t nr, size_t inode);
static int _wild_match_all(wildex_t *wild, const int32_t *s, size_t nr, size_t inode);
static int _wild_match_one(wildex_t *wild, const int32_t *s, size_t nr, size_t inode);
static int _wild_match_specific(wildex_t *wild, const int32_t *s, size_t nr, size_t inode);

static int _wild_match_all(wildex_t *wild, const int32_t *s, size_t nr, size_t inode)
{
  int r = 0;
  if (inode + 1 == wild->nr) DW("nr:%zd;inode:%zd: matched", nr, inode);
  if (inode + 1 == wild->nr) return 0;
  const int32_t *p = s;
  const int32_t *end = s + nr;
  while (p<end && *p) {
    r = _wild_exec(wild, p, end-p, inode + 1);
    if (r == 0) DW("nr:%zd;inode:%zd: matched", nr, inode);
    if (r == 0) return 0;
    ++p;
  }
  DW("nr:%zd;inode:%zd: unmatched", nr, inode);
  return -1;
}

static int _wild_match_one(wildex_t *wild, const int32_t *s, size_t nr, size_t inode)
{
  if (!*s) DW("nr:%zd;inode:%zd: unmatched", nr, inode);
  if (!*s) return -1;
  wildex_node_t *node = wild->nodes + inode;
  const int32_t *p = s;
  const int32_t *end = s + nr;
  for (size_t i=0; i<node->nr; ++i) {
    if (p == end) DW("nr:%zd;inode:%zd: unmatched", nr, inode);
    if (p == end) return -1;
    if (!*p) DW("nr:%zd;inode:%zd: unmatched", nr, inode);
    if (!*p) return -1;
    ++p;
  }

  if (inode + 1 == wild->nr) {
    if (p<end) {
      DW("nr:%zd;inode:%zd: unmatched", nr, inode); 
    } else {
      DW("nr:%zd;inode:%zd: matched", nr, inode); 
    }
    return (p<end) ? -1 : 0;
  }
  return _wild_exec(wild, p, end-p, inode + 1);
}

static int _wild_match_specific(wildex_t *wild, const int32_t *s, size_t nr, size_t inode)
{
  int r = 0;
  wildex_node_t *node = wild->nodes + inode;
  if (nr < node->nr) DW("nr:%zd;inode:%zd;node->nr:%zd: unmatched", nr, inode, node->nr); 
  if (nr < node->nr) return -1; // FIXME:
  r = memcmp(s, node->base, node->nr * sizeof(*node->base));
  if (r) DW("nr:%zd;inode:%zd;node->nr:%zd: unmatched", nr, inode, node->nr); 
  if (r) return -1;

  if (inode + 1 == wild->nr) return 0;

  const int32_t *end = s + nr;
  const int32_t *p = s + node->nr;
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
  mem_release(&wild->ex_ucs4);
}

static int _wild_append(wildex_t *wild, wild_match_f match, const int32_t *p, size_t nr)
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
  node->base  = p;
  node->nr    = nr;

  return 0;
}

static int _wild_comp(wildex_t *wild)
{
  int r = 0;

  wild_match_f    prev = NULL;

  int escaping = 0;

  const int32_t *base = (const int32_t*)wild->ex_ucs4.base;
  const int32_t *s = base;
  const int32_t *p = base;
  const int32_t *end = (const int32_t*)(wild->ex_ucs4.base + wild->ex_ucs4.nr);
  while (p < end) {
    if (*p == '\\') {
      if (escaping) return -1;
      if (p > s) {
        r = _wild_append(wild, _wild_match_specific, s, p-s);
        if (r) return -1;
      }
      escaping = 1;
      s = ++p;
      continue;
    }
    if (*p == '%') {
      if (escaping) {
        r = _wild_append(wild, _wild_match_specific, p, 1);
        if (r) return -1;
        prev = NULL;
        escaping = 0;
        s = ++p;
        continue;
      }
      if (prev != _wild_match_all) {
        if (p > s) {
          r = _wild_append(wild, _wild_match_specific, s, p-s);
          if (r) return -1;
        }
        r = _wild_append(wild, _wild_match_all, NULL, 0);
        if (r) return -1;
        prev = _wild_match_all;
      }
      s = ++p;
      continue;
    }
    if (*p == '_') {
      if (escaping) {
        r = _wild_append(wild, _wild_match_specific, p, 1);
        if (r) return -1;
        prev = NULL;
        escaping = 0;
        s = ++p;
        continue;
      }
      if (prev != _wild_match_one) {
        if (p > s) {
          r = _wild_append(wild, _wild_match_specific, s, p-s);
          if (r) return -1;
        }
        r = _wild_append(wild, _wild_match_one, p, 1);
        if (r) return -1;
        prev = _wild_match_one;
      } else {
        wildex_node_t *node = wild->nodes + wild->nr - 1;
        ++node->nr;
      }
      s = ++p;
      continue;
    }
    if (escaping) return -1;
    ++p;
    prev = NULL;
  }

  if (p > s) {
    r = _wild_append(wild, _wild_match_specific, s, p-s);
    if (r) return -1;
  }

  return 0;
}

int wildcomp_n_ex(wildex_t **pwild, const char *charset, const char *wildex, size_t len)
{
  int r = 0;

  if (!charset) charset = "UTF-8";

  wildex_t *wild = (wildex_t*)calloc(1, sizeof(*wild));
  if (!wild) return -1;

  do {
    wild->ex = strndup(wildex, len);
    if (!wild->ex) break;

    iconv_t ucs4 = iconv_open("UCS-4LE", charset);
    if (ucs4 == (iconv_t)-1) break;
    r = mem_conv(&wild->ex_ucs4, ucs4, wild->ex, len);
    iconv_close(ucs4);
    if (r) break;

    r = _wild_comp(wild);
    if (r == 0) {
      DW("%.*s => %zd", (int)len, wildex, wild->nr);
      *pwild = wild;
      return 0;
    }
  } while (0);

  _wild_release(wild);
  free(wild);

  return -1;
}

static int _wild_exec(wildex_t *wild, const int32_t *s, size_t nr, size_t inode)
{
  if (inode == wild->nr) {
    if (s && nr == 0) DW("nr:%zd;inode:%zd: matched", nr, inode);
    if (s && nr == 0) return 0;
    DW("nr:%zd;inode:%zd: unmatched", nr, inode);
    return -1;
  }

  wildex_node_t *node = wild->nodes + inode;
  wild_match_f match = node->match;
  return match(wild, s, nr, inode);
}

int wildexec_n_ex(wildex_t *wild, const char *charset, const char *str, size_t len)
{
  int r = 0;

  if (!str) return -1;

  if (!charset) charset = "UTF-8";

  mem_t mem = {0};

  do {
    iconv_t ucs4 = iconv_open("UCS-4LE", charset);
    if (ucs4 == (iconv_t)-1) break;
    r = mem_conv(&mem, ucs4, str, len);
    iconv_close(ucs4);
    if (r) break;

    const int32_t *base = (const int32_t*)mem.base;
    r = _wild_exec(wild, base, mem.nr/sizeof(*base), 0);
  } while (0);

  mem_release(&mem);

  return r;
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

int buffer_concat_fmt_x(buffer_t *str, const char *fmt, ...)
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

static void _trim_left(const char *src, size_t nr, const char **first)
{
  *first = src + nr;
  if (nr == 0) return;

  const char *end = src + nr;
  const char *p = src;
  while (isspace(*p)) ++p;
  *first = p;
  if (p == end) *first = src + nr;
}

static void _trim_right(const char *src, size_t nr, const char **last)
{
  *last = src + nr;
  if (nr == 0) return;

  const char *p = src + nr;
  while (p > src) {
    if (isspace(*--p)) continue;
    *last = p + 1;
    return;
  }

  if (isspace(*src)) return;
  *last = src + 1;
}

void trim_left(const char *src, size_t nr, const char **first)
{
  if (nr == (size_t)-1) nr = strlen(src);
  else                  nr = strnlen(src, nr);

  _trim_left(src, nr, first);
}

void trim_right(const char *src, size_t nr, const char **last)
{
  if (nr == (size_t)-1) nr = strlen(src);
  else                  nr = strnlen(src, nr);

  _trim_right(src, nr, last);
}

void trim_string(const char *src, size_t nr, const char **start, const char **end)
{
  if (nr == (size_t)-1) nr = strlen(src);
  else                  nr = strnlen(src, nr);

  *start = src + nr;
  *end   = src + nr;

  _trim_left(src, nr, start);
  if (!*start) return;
  _trim_right(*start, src + nr - *start, end);
}

void trim_spaces(const char *src, size_t len, char *dst, size_t n)
{
  const char *start, *end;
  trim_string(src, len, &start, &end);
  snprintf(dst, n, "%.*s", (int)(end-start), start);
}

void get_kv(const char *kv, char *k, size_t kn, char *v, size_t vn)
{
  if (kn > 0) k[0] = '\0';
  if (vn > 0) v[0] = '\0';

  const char *p = strchr(kv, '=');
  if (p) {
    trim_spaces(kv, p-kv, k, kn);
    trim_spaces(p+1, strlen(p+1), v, vn);
  } else {
    trim_spaces(kv, strlen(kv), k, kn);
  }
}

void kv_release(kv_t *kv)
{
  if (!kv) return;
  TOD_SAFE_FREE(kv->key);
  TOD_SAFE_FREE(kv->val);
}

int kv_set(kv_t *kv, const char *k, size_t kn, const char *v, size_t vn)
{
  kv_release(kv);

  do {
    kv->key = strndup(k, kn);
    if (!kv->key) break;

    if (!v) return 0;

    kv->val = strndup(v, vn);
    if (!kv->val) break;

    return 0;
  } while (0);

  kv_release(kv);

  return -1;
}

void kvs_reset(kvs_t *kvs)
{
  if (!kvs) return;

  for (size_t i=0; i<kvs->nr; ++i) {
    kv_t *kv = kvs->kvs + i;
    kv_release(kv);
  }
  kvs->nr = 0;
}

void kvs_release(kvs_t *kvs)
{
  if (!kvs) return;
  kvs_reset(kvs);
  TOD_SAFE_FREE(kvs->kvs);
  kvs->cap = 0;
}

void kvs_transfer(kvs_t *from, kvs_t *to)
{
  if (from == to) return;

  kvs_release(to);

  memcpy(to, from, sizeof(*from));
  memset(from, 0, sizeof(*from));
}

int kvs_append(kvs_t *kvs, const char *k, size_t kn, const char *v, size_t vn)
{
  int r = 0;

  if (kvs->nr >= kvs->cap) {
    size_t cap = kvs->cap + 16;
    kv_t *kv = (kv_t*)realloc(kvs->kvs, sizeof(*kv) * cap);
    if (!kv) return -1;
    kvs->kvs = kv;
    kvs->cap = cap;
  }

  kv_t *kv = kvs->kvs + kvs->nr;
  kv->key = NULL;
  kv->val = NULL;

  r = kv_set(kv, k, kn, v, vn);
  if (r) return -1;

  kvs->nr += 1;

  return 0;
}

void locate_str(const char *src, int row0, int col0, int row1, int col1, const char **start, const char **end)
{
  const char *s = src;
  for (int i=1; i<row0; ++i) {
    while (*s) {
      if (*s == '\r') {
        ++s;
        if (*s == '\n') ++s;
        break;
      }
      if (*s == '\n') {
        ++s;
        if (*s == '\r') ++s;
        break;
      }
      if (*s == '\f') {
        ++s;
        break;
      }
      ++s;
    }
    if (!*s) return;
  }
  *start = s + col0 - 1;
  for (int i=row0; i<row1; ++i) {
    while (*s) {
      if (*s == '\r') {
        ++s;
        if (*s == '\n') ++s;
        break;
      }
      if (*s == '\n') {
        ++s;
        if (*s == '\r') ++s;
        break;
      }
      if (*s == '\f') {
        ++s;
        break;
      }
      ++s;
    }
    if (!*s) return;
  }
  *end = s + col1 - 1;
}

