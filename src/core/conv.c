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

#include "conv.h"
#include "log.h"

#include <errno.h>
#include <math.h>

static int _conv_str(const char *src, size_t src_len, const char *src_charset, data_t *dst)
{
  (void)src;
  (void)src_len;
  (void)src_charset;
  // char        *inbuf             = (char*)src;
  // size_t       inbytes           = (size_t)src_len;
  // int64_t      i64;
  // uint64_t     u64;
  // float        flt;
  // double       dbl;

  switch (dst->dt) {
    case DATA_TYPE_INT8:
    case DATA_TYPE_UINT8:
    case DATA_TYPE_INT16:
    case DATA_TYPE_UINT16:
    case DATA_TYPE_INT32:
    case DATA_TYPE_UINT32:
    case DATA_TYPE_INT64:
    case DATA_TYPE_UINT64:
    case DATA_TYPE_FLOAT:
    case DATA_TYPE_DOUBLE:
    case DATA_TYPE_STR:
    default:
      return EINVAL;
  }

  dst->is_null = 0;
  return 0;
}

static int _conv_double(double dbl, data_t *dst)
{
  float flt;
  double v;
  int n;
  char buf[256];

  // NOTE: what about fractional truncation?
  switch (dst->dt) {
    case DATA_TYPE_INT8:
      if (dbl > INT8_MAX || dbl < INT8_MIN) return ERANGE;
      dst->i8 = (int8_t)dbl;
      break;
    case DATA_TYPE_UINT8:
      if (dbl > UINT8_MAX || dbl < 0) return ERANGE;
      dst->u8 = (uint8_t)dbl;
      break;
    case DATA_TYPE_INT16:
      if (dbl > INT16_MAX || dbl < INT16_MIN) return ERANGE;
      dst->i16 = (int16_t)dbl;
      break;
    case DATA_TYPE_UINT16:
      if (dbl > UINT16_MAX || dbl < 0) return ERANGE;
      dst->u16 = (uint16_t)dbl;
      break;
    case DATA_TYPE_INT32:
      if (dbl > INT32_MAX || dbl < INT32_MIN) return ERANGE;
      dst->i32 = (int32_t)dbl;
      break;
    case DATA_TYPE_UINT32:
      if (dbl > UINT32_MAX || dbl < 0) return ERANGE;
      dst->u32 = (uint32_t)dbl;
      break;
    case DATA_TYPE_INT64:
      dst->dbl = dbl;
      break;
    case DATA_TYPE_UINT64:
      if (dbl < 0) return ERANGE;
      dst->u64 = (uint64_t)dbl;
      break;
    case DATA_TYPE_FLOAT:
      flt = (float)dbl;
      v = flt;
      // NOTE: how to check fractional truncation
      if (fabs(dbl - v) > 1) return ERANGE;
      dst->flt = flt;
      break;
    case DATA_TYPE_DOUBLE:
      dst->dbl = dbl;
      break;
    case DATA_TYPE_STR:
      // NOTE: different way to stringify?
      n = snprintf(buf, sizeof(buf), "%g", dbl);
      if (n<0 || (size_t)n>sizeof(buf)) return ERANGE;
      return _conv_str(buf, (size_t)n, "UTF-8", dst);
    default:
      return EINVAL;
  }

  dst->is_null = 0;
  return 0;
}

static int _conv_int64(int64_t i64, data_t *dst)
{
  float flt;
  double dbl;
  int n;
  char buf[64];

  switch (dst->dt) {
    case DATA_TYPE_INT8:
      if (i64 > INT8_MAX || i64 < INT8_MIN) return ERANGE;
      dst->i8 = (int8_t)i64;
      break;
    case DATA_TYPE_UINT8:
      if (i64 > UINT8_MAX || i64 < 0) return ERANGE;
      dst->u8 = (uint8_t)i64;
      break;
    case DATA_TYPE_INT16:
      if (i64 > INT16_MAX || i64 < INT16_MIN) return ERANGE;
      dst->i16 = (int16_t)i64;
      break;
    case DATA_TYPE_UINT16:
      if (i64 > UINT16_MAX || i64 < 0) return ERANGE;
      dst->u16 = (uint16_t)i64;
      break;
    case DATA_TYPE_INT32:
      if (i64 > INT32_MAX || i64 < INT32_MIN) return ERANGE;
      dst->i32 = (int32_t)i64;
      break;
    case DATA_TYPE_UINT32:
      if (i64 > UINT32_MAX || i64 < 0) return ERANGE;
      dst->u32 = (uint32_t)i64;
      break;
    case DATA_TYPE_INT64:
      dst->i64 = i64;
      break;
    case DATA_TYPE_UINT64:
      if (i64 < 0) return ERANGE;
      dst->u64 = (uint64_t)i64;
      break;
    case DATA_TYPE_FLOAT:
      flt = (float)i64;
      if ((int64_t)flt != i64) return ERANGE;
      dst->flt = flt;
      break;
    case DATA_TYPE_DOUBLE:
      dbl = (double)i64;
      if ((int64_t)dbl != i64) return ERANGE;
      dst->dbl = dbl;
      break;
    case DATA_TYPE_STR:
      n = snprintf(buf, sizeof(buf), "%" PRId64 "", i64);
      if (n<0 || (size_t)n>sizeof(buf)) return ERANGE;
      return _conv_str(buf, (size_t)n, "UTF-8", dst);
    default:
      return EINVAL;
  }

  dst->is_null = 0;
  return 0;
}

static int _conv_uint64(uint64_t u64, data_t *dst)
{
  float flt;
  double dbl;
  int n;
  char buf[64];

  switch (dst->dt) {
    case DATA_TYPE_INT8:
      if (u64 > INT8_MAX) return ERANGE;
      dst->i8 = (int8_t)u64;
      break;
    case DATA_TYPE_UINT8:
      if (u64 > UINT8_MAX) return ERANGE;
      dst->u8 = (uint8_t)u64;
      break;
    case DATA_TYPE_INT16:
      if (u64 > INT16_MAX) return ERANGE;
      dst->i16 = (int16_t)u64;
      break;
    case DATA_TYPE_UINT16:
      if (u64 > UINT16_MAX) return ERANGE;
      dst->u16 = (uint16_t)u64;
      break;
    case DATA_TYPE_INT32:
      if (u64 > INT32_MAX) return ERANGE;
      dst->i32 = (int32_t)u64;
      break;
    case DATA_TYPE_UINT32:
      if (u64 > UINT32_MAX) return ERANGE;
      dst->u32 = (uint32_t)u64;
      break;
    case DATA_TYPE_INT64:
      if (u64 > INT64_MAX) return ERANGE;
      dst->i64 = (int64_t)u64;
      break;
    case DATA_TYPE_UINT64:
      dst->u64 = u64;
      break;
    case DATA_TYPE_FLOAT:
      flt = (float)u64;
      if (flt < 0) return ERANGE;
      if ((uint64_t)flt != u64) return ERANGE;
      dst->flt = flt;
      break;
    case DATA_TYPE_DOUBLE:
      dbl = (double)u64;
      if (dbl < 0) return ERANGE;
      if ((uint64_t)dbl != u64) return ERANGE;
      dst->dbl = dbl;
      break;
    case DATA_TYPE_STR:
      n = snprintf(buf, sizeof(buf), "%" PRIu64 "", u64);
      if (n<0 || (size_t)n>sizeof(buf)) return ERANGE;
      return _conv_str(buf, (size_t)n, "UTF-8", dst);
    default:
      return EINVAL;
  }

  dst->is_null = 0;
  return 0;
}

int conv_data(data_t *src, data_t *dst)
{
  if (!src) return EINVAL;
  if (!dst) return EINVAL;
  if (dst->dt <= 0 || dst->dt >= DATA_TYPE_MAX) return EINVAL;

  if (src->is_null) {
    dst->is_null = 1;
    return 0;
  }

  switch (src->dt) {
    case DATA_TYPE_INT8:
      return _conv_int64(src->i8, dst);
    case DATA_TYPE_UINT8:
      return _conv_uint64(src->u8, dst);
    case DATA_TYPE_INT16:
      return _conv_int64(src->i16, dst);
    case DATA_TYPE_UINT16:
      return _conv_uint64(src->u16, dst);
    case DATA_TYPE_INT32:
      return _conv_int64(src->i32, dst);
    case DATA_TYPE_UINT32:
      return _conv_uint64(src->u32, dst);
    case DATA_TYPE_INT64:
      return _conv_int64(src->i64, dst);
    case DATA_TYPE_UINT64:
      return _conv_uint64(src->u64, dst);
    case DATA_TYPE_FLOAT:
      return _conv_double(src->flt, dst);
    case DATA_TYPE_DOUBLE:
      return _conv_double(src->dbl, dst);
    case DATA_TYPE_STR:
      return _conv_str((const char*)src->str[0], src->str[1], (const char*)src->str[2], dst);
    default:
      return EINVAL;
  }
}

