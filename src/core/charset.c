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

#include "internal.h"

#include "charset.h"
// make sure `log.h` is included ahead of `taos_helpers.h`, for the `LOG_IMPL` issue
#include "log.h"

size_t iconv_x(const char *file, int line, const char *func,
    iconv_t cd, char **inbuf, size_t *inbytesleft, char **outbuf, size_t *outbytesleft)
{
  LOGD(file, line, func, "inbuf:%p(%p);inbytesleft:%p(%zd);outbuf:%p(%p);outbytesleft:%p(%zd) ...",
      inbuf, inbuf ? *inbuf : NULL,
      inbytesleft, inbytesleft ? *inbytesleft : 0,
      outbuf, outbuf ? *outbuf : NULL,
      outbytesleft, outbytesleft ? *outbytesleft : 0);
  size_t n = iconv(cd, inbuf, inbytesleft, outbuf, outbytesleft);
  LOGD(file, line, func, "inbuf:%p(%p);inbytesleft:%p(%zd);outbuf:%p(%p);outbytesleft:%p(%zd) => %zd",
      inbuf, inbuf ? *inbuf : NULL,
      inbytesleft, inbytesleft ? *inbytesleft : 0,
      outbuf, outbuf ? *outbuf : NULL,
      outbytesleft, outbytesleft ? *outbytesleft : 0,
      n);
  return n;
}

void charset_conv_release(charset_conv_t *cnv)
{
  if (!cnv) return;
  if (cnv->cnv) {
    iconv_close(cnv->cnv);
    cnv->cnv = NULL;
  }
  cnv->from[0] = '\0';
  cnv->to[0] = '\0';
  cnv->nr_from_terminator = 0;
  cnv->nr_to_terminator = 0;
}

static int _calc_terminator(const char *tocode)
{
  int nr = -1;
  iconv_t cnv = iconv_open(tocode, "UTF-8");
  if (!cnv) return -1;
  do {
    char buf[64]; buf[0] = '\0';
    char          *inbuf          = "";
    size_t         inbytesleft    = 1;
    char          *outbuf         = buf;
    size_t         outbytesleft   = sizeof(buf);

    size_t n = iconv(cnv, &inbuf, &inbytesleft, &outbuf, &outbytesleft);
    if (n) break;
    if (inbytesleft) break;
    nr = (int)(sizeof(buf) - outbytesleft);
  } while (0);
  iconv_close(cnv);
  return nr;
}

int charset_conv_reset(charset_conv_t *cnv, const char *from, const char *to)
{
  charset_conv_release(cnv);

  cnv->nr_from_terminator = _calc_terminator(from);
  cnv->nr_to_terminator = _calc_terminator(to);

  do {
    if (cnv->nr_from_terminator <= 0) break;
    if (cnv->nr_to_terminator <= 0) break;

    if (1 || tod_strcasecmp(from, to)) {
      cnv->cnv = iconv_open(to, from);
      if (!cnv->cnv) return -1;
    }
    snprintf(cnv->from, sizeof(cnv->from), "%s", from);
    snprintf(cnv->to, sizeof(cnv->to), "%s", to);
    return 0;
  } while (0);

  charset_conv_release(cnv);
  return -1;
}

void charset_conv_mgr_release(charset_conv_mgr_t *mgr)
{
  charset_conv_t *p, *n;
  tod_list_for_each_entry_safe(p, n, &mgr->convs, charset_conv_t, node) {
    tod_list_del(&p->node);
    charset_conv_release(p);
    free(p);
  }
}

charset_conv_t* charset_conv_mgr_get_charset_conv(charset_conv_mgr_t *mgr, const char *fromcode, const char *tocode)
{
  charset_conv_t *p;
  tod_list_for_each_entry(p, &mgr->convs, charset_conv_t, node) {
    if (tod_strcasecmp(p->from, fromcode)) continue;
    if (tod_strcasecmp(p->to, tocode)) continue;
    return p;
  }

  charset_conv_t *cnv = (charset_conv_t*)calloc(1, sizeof(*cnv));
  if (!cnv) return NULL;
  do {
    int r = charset_conv_reset(cnv, fromcode, tocode);
    if (r) break;

    tod_list_add_tail(&cnv->node, &mgr->convs);

    return cnv;
  } while (0);

  charset_conv_release(cnv);
  free(cnv);
  return NULL;
}
