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

#include "internal.h"

#include "variant.h"

#include "log.h"


static variant_t* _variant__add(variant_t *args)
{
  (void)args;
  return NULL;
}

variant_eval_f variant_get_eval(const char *name, size_t nr)
{
#define RECORD(x) {#x, _variant__##x}
  static const struct {
    const char        *name;
    variant_eval_f     eval;
  } _evals [] = {
    RECORD(add),
  };
  size_t _nr_evals = sizeof(_evals) / sizeof(_evals[0]);
#undef RECORD

  for (size_t i=0; i<_nr_evals; ++i) {
    const char       *_name   = _evals[i].name;
    variant_eval_f    _eval   = _evals[i].eval;
    if (nr == strlen(_name) && 0 == tod_strncasecmp(_name, name, nr)) {
      return _eval;
    }
  }

  return NULL;
}

static void _variant_release_arr(variant_t *v)
{
  for (size_t i=0; i<v->arr.nr; ++i) {
    variant_t *val = v->arr.vals[i];
    variant_release(val);
    TOD_SAFE_FREE(v->arr.vals[i]);
  }
  TOD_SAFE_FREE(v->arr.vals);
  v->arr.cap = 0;
  v->arr.nr  = 0;
}

static void _variant_release_eval(variant_t *v)
{
  variant_release(v->exp.args);
  TOD_SAFE_FREE(v->exp.args);
  v->exp.eval = NULL;
}

void variant_release(variant_t *v)
{
  if (!v) return;
  switch (v->type) {
    case VARIANT_NULL:
    case VARIANT_BOOL:
    case VARIANT_INT8:
    case VARIANT_INT16:
    case VARIANT_INT32:
    case VARIANT_INT64:
    case VARIANT_UINT8:
    case VARIANT_UINT16:
    case VARIANT_UINT32:
    case VARIANT_UINT64:
      break;
    case VARIANT_FLOAT:
      str_release(&v->flt.s);
      break;
    case VARIANT_DOUBLE:
      str_release(&v->dbl.s);
      break;
    case VARIANT_ID:
    case VARIANT_STRING:
      str_release(&v->str);
      break;
    case VARIANT_PARAM:
    case VARIANT_ARR:
      _variant_release_arr(v);
      break;
    case VARIANT_EVAL:
      _variant_release_eval(v);
      break;
    default:
      OA_NIY(0);
      break;
  }
  v->type = VARIANT_NULL;
}

variant_t* variant_add(variant_t *args)
{
  (void)args;
  return NULL;
}

variant_t* variant_sub(variant_t *args)
{
  (void)args;
  return NULL;
}

variant_t* variant_mul(variant_t *args)
{
  (void)args;
  return NULL;
}

variant_t* variant_div(variant_t *args)
{
  (void)args;
  return NULL;
}

variant_t* variant_neg(variant_t *args)
{
  (void)args;
  return NULL;
}

