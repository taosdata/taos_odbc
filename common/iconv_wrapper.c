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

#include "iconv_wrapper.h"

#include "helpers.h"
#include "logger.h"

#ifdef _WIN32                       /* { */
#ifndef USE_WIN_ICONV        /* { */

#ifdef LOG_ICONV          /* { */
#define LOGE_ICONV(file, line, func, fmt, ...) do {                                               \
  tod_logger_write(tod_get_system_logger(), LOGGER_ERROR, tod_get_system_logger_level(),          \
    file, line, func,                                                                             \
    fmt, ##__VA_ARGS__);                                                                          \
} while (0)
#else                     /* }{ */
#define LOGE_ICONV(...)
#endif                    /* } */

static int call_MultiByteToWideChar(const char *file, int line, const char *func,
  UINT CodePage, DWORD dwFlags, LPCCH lpMultiByteStr, int cbMultiByte, LPWSTR lpWideCharStr, int cchWideChar)
{
  LOGE_ICONV(file, line, func, "MultiByteToWideChar(codepage:%d,dwFlags:0x%x,lpMultiByteStr:%p,chMultiByte:%d,lpWideCharStr:%p,cchWideChar:%d) ...",
    CodePage, dwFlags, lpMultiByteStr, cbMultiByte, lpWideCharStr, cchWideChar);
  int n = MultiByteToWideChar(CodePage, dwFlags, lpMultiByteStr, cbMultiByte, lpWideCharStr, cchWideChar);
  LOGE_ICONV(file, line, func, "MultiByteToWideChar(codepage:%d,dwFlags:0x%x,lpMultiByteStr:%p,chMultiByte:%d,lpWideCharStr:%p,cchWideChar:%d) -> %d",
    CodePage, dwFlags, lpMultiByteStr, cbMultiByte, lpWideCharStr, cchWideChar, n);
  return n;
}

static int call_WideCharToMultiByte(const char *file, int line, const char *func,
  UINT CodePage, DWORD dwFlags, LPCWCH lpWideCharStr, int cchWideChar, LPSTR lpMultiByteStr, int cbMultiByte, LPCCH lpDefaultChar, LPBOOL lpUsedDefaultChar)
{
  LOGE_ICONV(file, line, func, "WideCharToMultiByte(CodePage:%d,dwFlags:0x%x,lpWideCharStr:%p,cchWideChar:%d,lpMultiByteStr:%p,cbMultiByte:%d,lpDefaultChar:%p,lpUsedDefaultChar:%p) ...",
    CodePage, dwFlags, lpWideCharStr, cchWideChar, lpMultiByteStr, cbMultiByte, lpDefaultChar, lpUsedDefaultChar);
  int n = WideCharToMultiByte(CodePage, dwFlags, lpWideCharStr, cchWideChar, lpMultiByteStr, cbMultiByte, lpDefaultChar, lpUsedDefaultChar);
  LOGE_ICONV(file, line, func, "WideCharToMultiByte(CodePage:%d,dwFlags:0x%x,lpWideCharStr:%p,cchWideChar:%d,lpMultiByteStr:%p,cbMultiByte:%d,lpDefaultChar:%p,lpUsedDefaultChar:%p) -> %d",
    CodePage, dwFlags, lpWideCharStr, cchWideChar, lpMultiByteStr, cbMultiByte, lpDefaultChar, lpUsedDefaultChar, n);
  return n;
}

#define CALL_MultiByteToWideChar(...) call_MultiByteToWideChar(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_WideCharToMultiByte(...) call_WideCharToMultiByte(__FILE__, __LINE__, __func__, ##__VA_ARGS__)

typedef struct cache_s           cache_t;
struct cache_s {
  char                 *buf;
  size_t                sz;
  size_t                nr;
};

static void cache_reset(cache_t *cache)
{
  cache->nr = 0;
}

static void cache_release(cache_t *cache)
{
  cache_reset(cache);
  if (cache->buf) {
    free(cache->buf);
    cache->buf = NULL;
  }
  cache->sz = 0;
}

static int cache_keep(cache_t *cache, size_t sz)
{
  if (sz <= cache->sz) return 0;
  sz = (sz + 255) / 256 * 256;
  char *p = (char*)realloc(cache->buf, sz);
  if (!p) return -1;
  cache->buf = p;
  cache->sz  = sz;
  // TOD_LOGE("cached expanded:%zd", cache->sz);
  return 0;
}

typedef struct cpinfo_s               cpinfo_t;
struct cpinfo_s {
  UINT           cp;
  CPINFO         info;
  char           name[64];
  char          *buf;
};

static void cpinfo_release(cpinfo_t *info)
{
  if (info->buf) {
    free(info->buf);
    info->buf = NULL;
  }
}

typedef struct buffer_s                buffer_t;
struct buffer_s {
  char                  *buf;
  size_t                 len;
};

struct iconv_s {
  cpinfo_t        from;
  cpinfo_t        to;

  cache_t         wchar;
  cache_t         mbcs;

  size_t (*conv)(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft);
};

static void hexify(const char *data, size_t sz)
{
  fprintf(stderr, "0x");
  for (size_t i=0; i<sz; ++i) {
    fprintf(stderr, "%02x", (unsigned char)(data[i]));
  }
}

/*
 * http://www.faqs.org/rfcs/rfc2781.html
 */
static uint32_t utf16_to_ucs4(const uint16_t *wbuf, int32_t *nbuf)
{
    uint32_t wc = wbuf[0];
    *nbuf = 1;
    if (0xD800 <= wbuf[0] && wbuf[0] <= 0xDBFF) {
        wc = ((wbuf[0] & 0x3FF) << 10) + (wbuf[1] & 0x3FF) + 0x10000;
        *nbuf = 2;
    }
    return wc;
}

static void ucs4_to_utf16(uint32_t wc, uint16_t *wbuf, int32_t *wbufsize)
{
    if (wc < 0x10000)
    {
        wbuf[0] = wc;
        *wbufsize = 1;
    }
    else
    {
        wc -= 0x10000;
        wbuf[0] = 0xD800 | ((wc >> 10) & 0x3FF);
        wbuf[1] = 0xDC00 | (wc & 0x3FF);
        *wbufsize = 2;
    }
}

static int iconv_get_acp(const char *code, cpinfo_t *info)
{
#define RECORD(x, y) { x, y}
  struct {
    const char          *name;
    UINT                 cp;
  } names[] = {
    RECORD("GB18030",        54936),
    RECORD("UTF8",           CP_UTF8),
    RECORD("UTF-8",          CP_UTF8),
    RECORD("UCS-2LE",        1200),
    RECORD("UCS-4LE",        12000),
    RECORD("CP437",          437),
    RECORD("CP850",          850),
    RECORD("CP858",          858),
  };
  const size_t nr_names = sizeof(names) / sizeof(names[0]);
#undef RECORD

  for (size_t i=0; i<nr_names; ++i) {
    if (tod_strcasecmp(code, names[i].name)) continue;
    info->cp = names[i].cp;
    goto cpinfo;
  }

  // TOD_LOGE("not implemented yet: encoder %s", code);
  goto err;

cpinfo:
  snprintf(info->name, sizeof(info->name), "%s", code);
  if (info->cp == 1200) return 0;
  if (info->cp == 12000) return 0;
  if (GetCPInfo(info->cp, &info->info)) {
    info->buf = calloc(info->info.MaxCharSize, sizeof(*info->buf));
    if (!info->buf) goto err;
    return 0;
  } else {
    // TOD_LOGE("GetCPInfo(%d) failed:[0x%x]", info->cp, GetLastError());
  }

err:
  errno = EINVAL;
  return (size_t)-1;
}

static int keep_wcs(iconv_t cd, size_t wnr)
{
  return cache_keep(&cd->wchar, wnr * 2);
}

static int add_wcs(iconv_t cd, WCHAR wc)
{
  int r = keep_wcs(cd, cd->wchar.sz + 2);
  if (r) return -1;
  memcpy(cd->wchar.buf + cd->wchar.nr, &wc, 2);
  cd->wchar.nr += 2;
  return 0;
}

static int keep_cs(iconv_t cd, size_t sz)
{
  return cache_keep(&cd->mbcs, sz);
}

static size_t do_finalize(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft, buffer_t *input, buffer_t *output)
{
  int n = 0;

  size_t input_delta  = *inbytesleft - input->len;
  size_t output_delta = *outbytesleft - output->len;
  // TOD_LOGE("delta:#%zd -> %zd", input_delta, output_delta);
  // TOD_LOGE("len:#%zd;#%zd", input->len, output->len);

  *inbuf               = input->buf;
  *inbytesleft         = input->len;

  *outbuf              = output->buf;
  *outbytesleft        = output->len;

  if (input->len == 0) {
    errno = 0;
    return 0;
  }

  DWORD dwFlags = MB_ERR_INVALID_CHARS;
  n = CALL_MultiByteToWideChar(cd->from.cp, dwFlags, input->buf, (int)input->len, NULL, 0);
  if (n > 0) {
    errno = E2BIG;
    return (size_t)-1;
  }

  char buf[4096];
  DWORD e = GetLastError();
  n = FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM|FORMAT_MESSAGE_IGNORE_INSERTS, NULL, e, MAKELANGID(LANG_ENGLISH, SUBLANG_DEFAULT), buf, sizeof(buf), NULL);
  buf[n] = '\0';
  switch (e) {
    case ERROR_INSUFFICIENT_BUFFER:
      // TOD_LOGE("[0x%x]%s", e, buf);
      errno = E2BIG;
      return (size_t)-1;
    case ERROR_INVALID_FLAGS:
      // TOD_LOGE("[0x%x]%s", e, buf);
      errno = EINVAL;
      return (size_t)-1;
    case ERROR_INVALID_PARAMETER:
      // TOD_LOGE("[0x%x]%s", e, buf);
      errno = EINVAL;
      return (size_t)-1;
    case ERROR_NO_UNICODE_TRANSLATION:
      // TOD_LOGE("[0x%x]%s", e, buf);
      errno = EINVAL;
      return (size_t)-1;
    default:
      // TOD_LOGE("[0x%x]%s", e, buf);
      errno = EINVAL;
      return (size_t)-1;
  }
}

static int to_wchar(iconv_t cd, buffer_t *mbcs)
{
  int r = 0;
  int n = 0;
  DWORD e = 0;
  char buf[4096];

  const char * const    src         = mbcs->buf;
  size_t       const    src_len     = mbcs->len;

  DWORD dwFlags = MB_ERR_INVALID_CHARS;

again:
  n = CALL_MultiByteToWideChar(cd->from.cp, dwFlags, src, (int)src_len, (WCHAR*)cd->wchar.buf, (int)(cd->wchar.sz / 2));
  if (n > 0) {
    if (cd->wchar.buf) {
      cd->wchar.nr   = n * 2;
      mbcs->buf     += src_len;
      mbcs->len     -= src_len;
      // TOD_LOGE("src_len:%zd;wchar.nr:%zd;n:%d", src_len, cd->wchar.nr, n);
      return 0;
    }
    r = keep_wcs(cd, n * 2);
    if (r) {
      // TOD_LOGE("");
      return (size_t)-1;
    }
    goto again;
  }

  e = GetLastError();
  n = FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM|FORMAT_MESSAGE_IGNORE_INSERTS, NULL, e, MAKELANGID(LANG_ENGLISH, SUBLANG_DEFAULT), buf, sizeof(buf), NULL);
  buf[n] = '\0';
  switch (e) {
    case ERROR_INSUFFICIENT_BUFFER:
      r = keep_wcs(cd, cd->wchar.sz + src_len);
      if (r) {
        // TOD_LOGE("");
        return (size_t)-1;
      }
      goto again;
    case ERROR_INVALID_FLAGS:
      // TOD_LOGE("[0x%x]%s", e, buf);
      errno = EINVAL;
      // TOD_LOGE("");
      return -1;
    case ERROR_INVALID_PARAMETER:
      // TOD_LOGE("[0x%x]%s", e, buf);
      errno = EINVAL;
      // TOD_LOGE("");
      return -1;
    case ERROR_NO_UNICODE_TRANSLATION:
      break;
    default:
      break;
  }

  // NOTE: really bothering
  cd->wchar.nr = 0;
  size_t nr = 0;
  size_t i = 0;
  while (i < src_len) {
    size_t j = 0;
    while (j < cd->from.info.MaxCharSize) {
      if (i+j >= src_len) break;
      cd->from.buf[j] = src[i + j];
      WCHAR wc;
      n = CALL_MultiByteToWideChar(cd->from.cp, dwFlags, cd->from.buf, (int)(j+1), &wc, 1);
      if (n > 0) {
        add_wcs(cd, wc);
        nr += j+1;
        i = nr;
        break;
      }
      ++j;
    }
    if (j == cd->from.info.MaxCharSize) break;
    if (i+j >= src_len) break;
  }

  mbcs->buf     += nr;
  mbcs->len     -= nr;

  // TOD_LOGE("");
  return 0;
}

static int to_mbcs(iconv_t cd, buffer_t *input, buffer_t *output)
{
  int r = 0;
  int n = 0;

  char               *dst               = output->buf;
  size_t              dst_len           = output->len;

  DWORD dwFlags = 0;
  n = CALL_WideCharToMultiByte(cd->to.cp, dwFlags, (const WCHAR*)cd->wchar.buf, (int)(cd->wchar.nr / 2), dst, (int)dst_len, NULL, NULL);
  if (n > 0) {
    output->buf         += n;
    output->len         -= n;
    errno = 0;
    return 0;
  }

  // NOTE: really bothering
  size_t wnr = 0;
  size_t i = 0;
  size_t delta = 0;
  int too_big = 0;
  int incomplete = 0;
  // TOD_LOGE("dst_len:#%zd;wchar.nr:#%zd", dst_len, cd->wchar.nr);
  for (;; i += 2) {
    // TOD_LOGE("i:%zd;nr:%zd", i, cd->wchar.nr);
    if (i + 2 > cd->wchar.nr) break;
    n = CALL_WideCharToMultiByte(cd->to.cp, dwFlags, (const WCHAR*)(cd->wchar.buf + i), 1, dst, (int)dst_len, NULL, NULL);
    if (n > 0) {
      dst          += n;
      dst_len      -= n;
      ++wnr;
      if (dst_len == 0) {
        TOD_LOGE("dst_len:#%zd;wchar.nr:#%zd;wnr:#%zd", dst_len, cd->wchar.nr, wnr);
        break;
      }
      continue;
    }

    TOD_LOGE("");
    goto err;
  }

  TOD_LOGE("");
  goto adjust;

err:
  int bad = 0;
  DWORD e = GetLastError();
  char buf[4096];
  e = GetLastError();
  n = FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM|FORMAT_MESSAGE_IGNORE_INSERTS, NULL, e, MAKELANGID(LANG_ENGLISH, SUBLANG_DEFAULT), buf, sizeof(buf), NULL);
  buf[n] = '\0';
  switch (e) {
    case ERROR_INSUFFICIENT_BUFFER:
      TOD_LOGE("[0x%x]%s", e, buf);
      bad = 1;
      errno = E2BIG;
      break;
    case ERROR_INVALID_FLAGS:
      TOD_LOGE("[0x%x]%s", e, buf);
      errno = EINVAL;
      return -1;
    case ERROR_INVALID_PARAMETER:
      TOD_LOGE("[0x%x]%s", e, buf);
      errno = EINVAL;
      return -1;
    case ERROR_NO_UNICODE_TRANSLATION:
      TOD_LOGE("[0x%x]%s", e, buf);
      bad = 1;
      errno = EINVAL;
      break;
    default:
      TOD_LOGE("[0x%x]%s", e, buf);
      bad = 1;
      errno = EINVAL;
      break;
  }

adjust:
  output->buf           = dst;
  output->len           = dst_len;
  delta = cd->wchar.nr - wnr * 2;
  cd->wchar.nr          = wnr * 2;
  TOD_LOGE("delta:#%zd;wnr:#%zd;i:#%zd", delta, wnr, i);

  n = CALL_WideCharToMultiByte(cd->from.cp, 0, (const WCHAR*)(cd->wchar.buf + cd->wchar.nr), (int)(delta / 2), NULL, 0, NULL, NULL);
  input->buf    -= n;
  input->len    += n;

  // if (bad) return (size_t)-1;
  // if (output->len) {
  //   errno = E2BIG;
  //   return (size_t)-1;
  // }

  errno = 0;
  return 0;
}

static size_t mbcs_to_mbcs(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  // TOD_LOGE("%p/%zd;%p/%zd", *inbuf, *inbytesleft, *outbuf, *outbytesleft);

  int r = 0;
  int n = 0;

  buffer_t         input = {
    .buf       = *inbuf,
    .len       = *inbytesleft,
  };
  buffer_t         output = {
    .buf       = *outbuf,
    .len       = *outbytesleft,
  };

  r = to_wchar(cd, &input);
  if (r) return (size_t)-1;
  // TOD_LOGE("input_len:#%zd;output_len:#%zd", input.len, output.len);
  r = to_mbcs(cd, &input, &output);
  if (r) return (size_t)-1;
  // TOD_LOGE("input_len:#%zd;output_len:#%zd", input.len, output.len);

  return do_finalize(cd, inbuf, inbytesleft, outbuf, outbytesleft, &input, &output);
}

static size_t mbcs_to_ucs2le(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  int r = 0;
  int n = 0;
  const char            *src      = *inbuf;
  size_t                 src_len  = *inbytesleft;
  char                  *dst      = *outbuf;
  size_t                 dst_len  = *outbytesleft;

  if (dst_len < 2) {
    // TOD_LOGE("no sufficient room for outbuf");
    errno = E2BIG;
    return (size_t)-1;
  }

  DWORD dwFlags0 = MB_ERR_INVALID_CHARS;
  n = CALL_MultiByteToWideChar(cd->from.cp, dwFlags0, src, (int)src_len, (WCHAR*)dst, (int)(dst_len / 2));
  if (n > 0) {
    *inbuf             += src_len;
    *inbytesleft       -= src_len;
    *outbuf            += n * 2;
    *outbytesleft      -= n * 2;
    errno = 0;
    return 0;
  }

  n = CALL_MultiByteToWideChar(cd->from.cp, dwFlags0, src, (int)src_len, NULL, 0);
  if (n == 0) {
    // TOD_LOGE("not supported yet");
    errno = ENOTSUP;
    return (size_t)-1;
  }
  r = keep_wcs(cd, n);
  if (r) return (size_t)-1;
  CALL_MultiByteToWideChar(cd->from.cp, dwFlags0, src, (int)src_len, (WCHAR*)cd->wchar.buf, (int)(cd->wchar.sz / 2));
  n = CALL_WideCharToMultiByte(cd->from.cp, 0, (const WCHAR*)cd->wchar.buf, (int)(dst_len/2), NULL, 0, NULL, NULL);
  *inbuf              += n;
  *inbytesleft        -= n;

  memcpy(dst, cd->wchar.buf, dst_len);
  *outbuf             += dst_len / 2;
  *outbytesleft       -= dst_len / 2;

  if (*inbytesleft) {
    errno = E2BIG;
    return (size_t)-1;
  }

  errno = 0;
  return 0;
}

static size_t ucs2le_to_mbcs(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  int r = 0;
  int n = 0;
  const char            *src      = *inbuf;
  size_t                 src_len  = *inbytesleft;
  char                  *dst      = *outbuf;
  size_t                 dst_len  = *outbytesleft;

  if (src_len == 1) {
    // TOD_LOGE("incomplete sequence");
    errno = EINVAL;
    return (size_t)-1;
  }

  if (dst_len < 1) {
    // TOD_LOGE("no sufficient room for outbuf");
    errno = E2BIG;
    return (size_t)-1;
  }

  DWORD dwFlags0 = 0;
  n = CALL_WideCharToMultiByte(cd->to.cp, dwFlags0, (const WCHAR*)src, (int)(src_len/2), dst, (int)dst_len, NULL, NULL);
  if (n == 0) {
    // TOD_LOGE("not supported yet:%p;%zd;%p;%zd", src, src_len, dst, dst_len);
    errno = ENOTSUP;
    return (size_t)-1;
  }
  *outbuf            += n;
  *outbytesleft      -= n;

  DWORD dwFlags1 = MB_ERR_INVALID_CHARS;
  n = CALL_MultiByteToWideChar(cd->to.cp, dwFlags1, dst, n, NULL, 0);
  *inbuf            += n * 2;
  *inbytesleft      -= n * 2;

  return 0;
}

static size_t mbcs_to_ucs4le(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  int r = 0;
  int n = 0;
  const char            *src      = *inbuf;
  size_t                 src_len  = *inbytesleft;
  char                  *dst      = *outbuf;
  size_t                 dst_len  = *outbytesleft;

  DWORD dwFlags0 = MB_ERR_INVALID_CHARS;
  n = CALL_MultiByteToWideChar(cd->from.cp, dwFlags0, src, (int)src_len, (WCHAR*)cd->wchar.buf, (int)(cd->wchar.sz / 2));
  if (n == 0 || cd->wchar.sz == 0) {
    n = CALL_MultiByteToWideChar(cd->from.cp, dwFlags0, src, (int)src_len, NULL, 0);
    if (n == 0) {
      // TOD_LOGE("not supported yet");
      errno = ENOTSUP;
      return (size_t)-1;
    }

    r = keep_wcs(cd, n);
    if (r) return (size_t)-1;
    n = CALL_MultiByteToWideChar(cd->from.cp, dwFlags0, src, (int)src_len, (WCHAR*)cd->wchar.buf, (int)(cd->wchar.sz / 2));
  }

  cd->wchar.nr = n * 2;

  int incomplete = 0;
  int too_big = 0;

  size_t i = 0;
  while (i<cd->wchar.nr) {
    size_t n = cd->wchar.nr - i;
    int16_t v[2] = {0};
    if (n >= 4) {
      memcpy(v, cd->wchar.buf + i, 4);
    } else if (n == 2 || n == 3) {
      memcpy(v, cd->wchar.buf + i, 2);
    } else {
      incomplete = 1;
      break;
    }
    int32_t nbuf = 0;
    uint32_t wc = utf16_to_ucs4(v, &nbuf);
    if (n<4 && nbuf == 2) {
      incomplete = 1;
      break;
    }
    if (dst_len < 4) {
      too_big = 1;
      break;
    }
    memcpy(dst, &wc, 4);
    i += nbuf * 2;
    dst += 4;
    dst_len -= 4;
  }

  DWORD dwFlags1 = 0;
  n = CALL_WideCharToMultiByte(cd->from.cp, dwFlags1, (const WCHAR*)cd->wchar.buf, (int)(i/2), NULL, 0, NULL, NULL);
  *inbuf         += n;
  *inbytesleft   -= n;
  *outbuf         = dst;
  *outbytesleft   = dst_len;

  if (*inbytesleft == 0) {
  }

  if (incomplete) {
    errno = EINVAL;
    return (size_t)-1;
  }
  if (too_big) {
    errno = E2BIG;
    return (size_t)-1;
  }

  errno = 0;
  return 0;
}

static size_t ucs4le_to_mbcs(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  int r = 0;
  int n = 0;
  const char            *src      = *inbuf;
  size_t                 src_len  = *inbytesleft;
  char                  *dst      = *outbuf;
  size_t                 dst_len  = *outbytesleft;

  if (src_len < 4) {
    errno = EINVAL;
    return (size_t)-1;
  }

  r = keep_wcs(cd, src_len);

  int too_big = 0;
  int incomplete = 1;

  char    *s_cache = cd->wchar.buf;
  size_t   n_cache = cd->wchar.sz;

  while (src_len >= 4) {
    uint32_t wc;
    memcpy(&wc, src, 4);
    uint16_t vbuf[2];
    int32_t nbuf;
    ucs4_to_utf16(wc, vbuf, &nbuf);
    if (nbuf == 1 && n_cache < 2) {
      too_big = 1;
      break;
    }
    if (nbuf == 2 && n_cache < 4) {
      too_big = 1;
      break;
    }
    if (n_cache < nbuf * 2) {
      // TODO: test case
      size_t old = cd->wchar.sz - n_cache;
      r = keep_wcs(cd, cd->wchar.sz + src_len);
      if (r) return (size_t)-1;
      s_cache  = cd->wchar.buf + old;
      n_cache  = cd->wchar.sz - old;
    }
    memcpy(s_cache, vbuf, nbuf * 2);
    src         += 4;
    src_len     -= 4;
    s_cache     += nbuf * 2;
    n_cache     -= nbuf * 2;
  }

  if (src_len >0 && src_len < 4) {
    too_big = 0;
    incomplete = 1;
  }

  DWORD dwFlags0 = 0;
  n = CALL_WideCharToMultiByte(cd->to.cp, dwFlags0, (const WCHAR*)cd->wchar.buf, (int)((s_cache - cd->wchar.buf)/2), dst, (int)dst_len, NULL, NULL);
  if (n == 0) {
    // TOD_LOGE("not supported yet");
    errno = ENOTSUP;
    return (size_t)-1;
  }

  *inbuf               = (char*)src;
  *inbytesleft         = src_len;
  *outbuf             += n;
  *outbytesleft       -= n;
  if (*inbytesleft == 0) return 0;
  if (incomplete) {
    errno = EINVAL;
    return (size_t)-1;
  }
  if (too_big) {
    errno = E2BIG;
    return (size_t)-1;
  }

  errno = 0;
  return 0;
}

static int iconv_init(iconv_t cd)
{
  if (cd->from.cp == 1200) {
    if (cd->to.cp == 12000) goto err;
    cd->conv = ucs2le_to_mbcs;
    return 0;
  }
  if (cd->from.cp == 12000) {
    if (cd->to.cp == 1200) goto err;
    cd->conv = ucs4le_to_mbcs;
    return 0;
  }

  if (cd->to.cp   == 1200) {
    cd->conv = mbcs_to_ucs2le;
    return 0;
  }

  if (cd->to.cp   == 12000) {
    cd->conv = mbcs_to_ucs4le;
    return 0;
  }

  cd->conv = mbcs_to_mbcs;
  return 0;

err:
  // TOD_LOGE("not supported yet:convert from %s[%d] -> %s[%d]", cd->from.name, cd->from.cp, cd->to.name, cd->to.cp);
  errno = EINVAL;
  return -1;
}

iconv_t iconv_open(const char *tocode, const char *fromcode)
{
  int r = 0;

  iconv_t cnv = (iconv_t)calloc(1, sizeof(*cnv));
  if (!cnv) {
    // TOD_LOGE("out of memory");
    errno = ENOMEM;
    return (iconv_t)-1;
  }

  BOOL ok = TRUE;

  do {
    r = iconv_get_acp(fromcode, &cnv->from);
    if (r) break;
    r = iconv_get_acp(tocode, &cnv->to);
    if (r) break;
    r = iconv_init(cnv);
    if (r) break;

    // TOD_LOGE("from %s -> %s", fromcode, tocode);
    // TOD_LOGE("from %d -> %d", cnv->from.cp, cnv->to.cp);
    // TOD_LOGE("ACP:%d", GetACP());
    return cnv;
  } while (0);

  iconv_close(cnv);
  return (iconv_t)-1;
}

int iconv_close(iconv_t cd)
{
  if (!cd) return 0;
  cache_release(&cd->wchar);
  cache_release(&cd->mbcs);
  cpinfo_release(&cd->from);
  cpinfo_release(&cd->to);
  free(cd);
  return 0;
}

size_t iconv(iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  if (inbuf == NULL || *inbuf == NULL) {
    if (outbuf == NULL || *outbuf == NULL) {
      return 0;
    }
    // TOD_LOGE("not supported yet");
    errno = ENOTSUP;
    return (size_t)-1;
  }

  if (outbuf == NULL || *outbuf == NULL) {
    // TOD_LOGE("invalid argument");
    errno = EINVAL;
    return (size_t)-1;
  }

  if (inbytesleft == NULL || outbytesleft == NULL) {
    // TOD_LOGE("invalid argument");
    errno = EINVAL;
    return (size_t)-1;
  }

  char   *s_inbuf        = *inbuf;
  size_t  n_inbytes      = *inbytesleft;
  char   *s_outbuf       = *outbuf;
  size_t  n_outbytes     = *outbytesleft;
  const size_t szWchar   = sizeof(WCHAR);

  if (n_inbytes == 0) {
    errno = 0;
    return 0;
  }

  if (n_outbytes == 0) {
    // TOD_LOGE("no sufficient room for outbuf");
    errno = E2BIG;
    return (size_t)-1;
  }

  // TOD_LOGE("inbytes:%zd;outbytes:%zd", n_inbytes, n_outbytes);
  // TOD_LOGE("enter: from %s[%zd] -> %s[%zd]", cd->from.name, *inbytesleft, cd->to.name, *outbytesleft);
  size_t r = cd->conv(cd, inbuf, inbytesleft, outbuf, outbytesleft);
  // TOD_LOGE("leave: from %s[%zd] -> %s[%zd]", cd->from.name, *inbytesleft, cd->to.name, *outbytesleft);
  return r;
}

#endif                       /* } */
#endif                              /* } */
