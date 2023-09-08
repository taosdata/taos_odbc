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

#include "ejson_parser.h"

#include "logger.h"

#include <math.h>

static ejson_t* _ejson_new_num(const char *s, size_t n);
static ejson_t* _ejson_new_str(_ejson_str_t *str);
static ejson_t* _ejson_new_obj(_ejson_kv_t *kv);
static int _ejson_obj_set(ejson_t *ejson, _ejson_kv_t *kv);

static ejson_t* ejson_new_str(const char *v, size_t n);
// static int ejson_str_append(ejson_t *ejson, const char *v, size_t n);
// static int ejson_obj_set(ejson_t *ejson, const char *k, size_t n, ejson_t *v);

typedef struct ejson_num_s              ejson_num_t;
typedef struct ejson_obj_s              ejson_obj_t;
typedef struct ejson_arr_s              ejson_arr_t;


struct ejson_num_s {
  double                     dbl;
};

struct ejson_obj_s {
  _ejson_kv_t               *kvs;
  size_t                     cap;
  size_t                     nr;
};

struct ejson_arr_s {
  ejson_t                 **vals;
  size_t                    cap;
  size_t                    nr;
};

struct ejson_s {
  ejson_type_t               type;
  union {
    ejson_num_t              _num;
    _ejson_str_t             _str;
    ejson_obj_t              _obj;
    ejson_arr_t              _arr;
  };

  parser_loc_t               loc;
  int                        refc;
};

#include "ejson_parser.tab.h"
#include "ejson_parser.lex.c"

#include "ejson_parser.lex.h"
#undef yylloc
#undef yylval
#include "ejson_parser.tab.c"


ejson_t* ejson_new_null(void)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  ejson->type = EJSON_NULL;
  ejson->refc = 1;
  return ejson;
}

ejson_t* ejson_new_true(void)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  ejson->type = EJSON_TRUE;
  ejson->refc = 1;
  return ejson;
}

ejson_t* ejson_new_false(void)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  ejson->type = EJSON_FALSE;
  ejson->refc = 1;
  return ejson;
}

static void _ejson_num_release(ejson_num_t *num)
{
  if (!num) return;
}

void _ejson_str_reset(_ejson_str_t *str)
{
  if (!str) return;
  if (str->str) str->str[0] = '\0';
  str->nr = 0;
}

void _ejson_str_release(_ejson_str_t *str)
{
  if (!str) return;
  _ejson_str_reset(str);
  if (str->str) {
    free(str->str);
    str->str = NULL;
  }
  str->cap = 0;
}

int _ejson_str_append(_ejson_str_t *str, const char *v, size_t n)
{
  size_t len = strnlen(v, n);
  if (len == 0) return 0;
  size_t cap = str->nr + len;
  if (cap > str->cap) {
    cap = (cap + 15) / 16 * 16;
    char *p = (char*)realloc(str->str, cap + 1);
    if (!p) return -1;
    str->str = p;
    str->cap = cap;
  }
  strncpy(str->str + str->nr, v, len);
  str->nr += len;
  str->str[str->nr] = '\0';
  return 0;
}

int _ejson_str_init(_ejson_str_t *str, const char *v, size_t n)
{
  memset(str, 0, sizeof(*str));
  int r = _ejson_str_append(str, v, n);
  if (r) {
    _ejson_str_release(str);
    return -1;
  }
  return 0;
}

void _ejson_kv_release(_ejson_kv_t *kv)
{
  if (!kv) return;
  _ejson_str_release(&kv->key);
  if (kv->val) {
    ejson_dec_ref(kv->val);
    kv->val = NULL;
  }
}

static void _ejson_obj_reset(ejson_obj_t *obj)
{
  if (!obj) return;
  for (size_t i=0; i<obj->nr; ++i) {
    _ejson_kv_t *kv = obj->kvs + i;
    _ejson_kv_release(kv);
  }
  obj->nr = 0;
}

static void _ejson_obj_release(ejson_obj_t *obj)
{
  if (!obj) return;
  _ejson_obj_reset(obj);
  if (obj->kvs) {
    free(obj->kvs);
    obj->kvs = NULL;
  }
  obj->cap = 0;
}

static void _ejson_inc_ref(ejson_t *ejson)
{
  if (!ejson) return;

  ejson->refc += 1;
}

static int _ejson_obj_keep(ejson_obj_t *obj, size_t cap)
{
  if (cap >= obj->cap) {
    cap = (cap + 15) / 16 * 16;
    _ejson_kv_t *kvs = (_ejson_kv_t*)realloc(obj->kvs, cap * sizeof(*kvs));
    if (!kvs) return -1;
    obj->kvs = kvs;
    obj->cap = cap;
  }
  return 0;
}

// static int _ejson_kv_set(_ejson_kv_t *kv, const char *k, size_t n, ejson_t *v)
// {
//   size_t len = strnlen(k, n);
//   int r = _ejson_str_init(&kv->key, k, len);
//   if (r) return -1;
// 
//   kv->val = v;
//   _ejson_inc_ref(v);
// 
//   return 0;
// }

// static int _ejson_obj_append(ejson_obj_t *obj, const char *k, size_t n, ejson_t *v)
// {
//   int r = _ejson_obj_keep(obj, obj->nr + 1);
//   if (r) return -1;
// 
//   _ejson_kv_t *kv = obj->kvs + obj->nr;
//   memset(kv, 0, sizeof(*kv));
//   r = _ejson_kv_set(kv, k, n, v);
//   if (r) return -1;
// 
//   ++obj->nr;
// 
//   return 0;
// }

static void _ejson_arr_reset(ejson_arr_t *arr)
{
  if (!arr) return;
  for (size_t i=0; i<arr->nr; ++i) {
    ejson_t *v = arr->vals[i];
    ejson_dec_ref(v);
    arr->vals[i] = NULL;
  }
  arr->nr = 0;
}

static void _ejson_arr_release(ejson_arr_t *arr)
{
  if (!arr) return;
  _ejson_arr_reset(arr);
  if (arr->vals) {
    free(arr->vals);
    arr->vals = NULL;
  }
  arr->cap = 0;
}

static int _ejson_arr_append(ejson_arr_t *arr, ejson_t *v)
{
  if (arr->nr == arr->cap) {
    size_t cap = arr->cap + 16;
    ejson_t **vals = (ejson_t**)realloc(arr->vals, cap * sizeof(*vals));
    if (!vals) return -1;
    arr->vals = vals;
    arr->cap  = cap;
  }

  arr->vals[arr->nr++] = v;
  _ejson_inc_ref(v);

  return 0;
}

ejson_t* ejson_new_num(double v)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  ejson_num_t *num = &ejson->_num;
  num->dbl = v;
  ejson->type = EJSON_NUM;
  ejson->refc = 1;
  return ejson;
}

static ejson_t* ejson_new_str(const char *v, size_t n)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  int r = _ejson_str_init(&ejson->_str, v, n);
  if (r) {
    free(ejson);
    return NULL;
  }
  ejson->type = EJSON_STR;
  ejson->refc = 1;
  return ejson;
}

ejson_t* ejson_new_obj(void)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  ejson->type = EJSON_OBJ;
  ejson->refc = 1;
  return ejson;
}

ejson_t* ejson_new_arr(void)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  ejson->type = EJSON_ARR;
  ejson->refc = 1;
  return ejson;
}

void ejson_inc_ref(ejson_t *ejson)
{
  if (!ejson) return;
  ++ejson->refc;
}

static void _ejson_release(ejson_t *ejson)
{
  switch (ejson->type) {
    case EJSON_NULL:
    case EJSON_TRUE:
    case EJSON_FALSE:
      break;
    case EJSON_NUM:
      _ejson_num_release(&ejson->_num);
      break;
    case EJSON_STR:
      _ejson_str_release(&ejson->_str);
      break;
    case EJSON_OBJ:
      _ejson_obj_release(&ejson->_obj);
      break;
    case EJSON_ARR:
      _ejson_arr_release(&ejson->_arr);
      break;
    default: return;
  }
}

void ejson_dec_ref(ejson_t *ejson)
{
  if (!ejson) return;
  if (ejson->refc > 1) {
    --ejson->refc;
    return;
  }
  --ejson->refc;
  _ejson_release(ejson);
  free(ejson);
}

// static int ejson_str_append(ejson_t *ejson, const char *v, size_t n)
// {
//   if (ejson->type != EJSON_STR) return -1;
//   return _ejson_str_append(&ejson->_str, v, n);
// }

// static int ejson_obj_set(ejson_t *ejson, const char *k, size_t n, ejson_t *v)
// {
//   if (ejson->type != EJSON_OBJ) return -1;
//   return _ejson_obj_append(&ejson->_obj, k, n, v);
// }

int ejson_arr_append(ejson_t *ejson, ejson_t *v)
{
  if (ejson->type != EJSON_ARR) return -1;
  return _ejson_arr_append(&ejson->_arr, v);
}

static ejson_t* _ejson_new_num(const char *s, size_t n)
{
  char buf[128];
  snprintf(buf, sizeof(buf), "%.*s", (int)n, s);
  double dbl = 0;
  sscanf(buf, "%lg", &dbl);
  return ejson_new_num(dbl);
}

static ejson_t* _ejson_new_str(_ejson_str_t *str)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;
  memcpy(&ejson->_str, str, sizeof(*str));
  memset(str, 0, sizeof(*str));
  ejson->type = EJSON_STR;
  ejson->refc = 1;
  return ejson;
}

static int _ejson_obj_set(ejson_t *ejson, _ejson_kv_t *kv)
{
  ejson_obj_t *obj = &ejson->_obj;
  int r = _ejson_obj_keep(obj, obj->nr + 1);
  if (r) return -1;

  _ejson_kv_t *_kv = obj->kvs + obj->nr;
  if (_kv == kv) return 0;

  memcpy(_kv, kv, sizeof(*kv));
  memset(kv, 0, sizeof(*kv));

  ++obj->nr;

  return 0;
}

static ejson_t* _ejson_new_obj(_ejson_kv_t *kv)
{
  ejson_t *ejson = (ejson_t*)calloc(1, sizeof(*ejson));
  if (!ejson) return NULL;

  ejson->type = EJSON_OBJ;
  ejson->refc = 1;

  int r = _ejson_obj_set(ejson, kv);
  if (r) {
    ejson_dec_ref(ejson);
    return NULL;
  }

  return ejson;
}


int ejson_is_null(ejson_t *ejson)
{
  return (ejson && ejson->type == EJSON_NULL) ? 1 : 0;
}

int ejson_is_true(ejson_t *ejson)
{
  return (ejson && ejson->type == EJSON_TRUE) ? 1 : 0;
}

int ejson_is_false(ejson_t *ejson)
{
  return (ejson && ejson->type == EJSON_FALSE) ? 1 : 0;
}

int ejson_is_str(ejson_t *ejson)
{
  return (ejson && ejson->type == EJSON_STR) ? 1 : 0;
}

int ejson_is_num(ejson_t *ejson)
{
  return (ejson && ejson->type == EJSON_NUM) ? 1 : 0;
}

int ejson_is_obj(ejson_t *ejson)
{
  return (ejson && ejson->type == EJSON_OBJ) ? 1 : 0;
}

int ejson_is_arr(ejson_t *ejson)
{
  return (ejson && ejson->type == EJSON_ARR) ? 1 : 0;
}

const char* ejson_str_get(ejson_t *ejson)
{
  if (!ejson_is_str(ejson)) return NULL;
  return ejson->_str.str;
}

int ejson_num_get(ejson_t *ejson, double *v)
{
  if (!ejson_is_num(ejson)) return -1;
  *v = ejson->_num.dbl;
  return 0;
}

ejson_t* ejson_obj_get(ejson_t *ejson, const char *k)
{
  if (!ejson_is_obj(ejson)) return NULL;
  for (size_t i=0; i<ejson->_obj.nr; ++i) {
    _ejson_kv_t *kv = ejson->_obj.kvs + i;
    if (strcmp(k, kv->key.str)) continue;
    return kv->val;
  }
  return NULL;
}

size_t ejson_obj_count(ejson_t *ejson)
{
  if (!ejson_is_obj(ejson)) return 0;
  return ejson->_obj.nr;
}

ejson_t* ejson_obj_idx(ejson_t *ejson, size_t idx, const char **k)
{
  if (!ejson_is_obj(ejson)) return NULL;
  if (idx >= ejson->_obj.nr) return NULL;
  _ejson_kv_t *kv = ejson->_obj.kvs + idx;
  *k = kv->key.str;
  return kv->val;
}

size_t ejson_arr_count(ejson_t *ejson)
{
  if (!ejson_is_arr(ejson)) return 0;
  return ejson->_arr.nr;
}

ejson_t* ejson_arr_get(ejson_t *ejson, size_t idx)
{
  if (!ejson_is_arr(ejson)) return NULL;
  if (idx >= ejson->_arr.nr) return NULL;
  return ejson->_arr.vals[idx];
}

static int _ejson_kv_cmp(_ejson_kv_t *l, _ejson_kv_t *r)
{
  int rr = strcmp(l->key.str, r->key.str);
  if (rr) return rr;
  return ejson_cmp(l->val, r->val);
}

static int _ejson_obj_cmp(ejson_obj_t *l, ejson_obj_t *r)
{
  for (size_t i=0; i<l->nr && i<r->nr; ++i) {
    int rr = _ejson_kv_cmp(l->kvs + i, r->kvs + i);
    if (rr) return rr;
  }
  if (l->nr < r->nr) return -1;
  if (l->nr > r->nr) return 1;
  return 0;
}

static int _ejson_arr_cmp(ejson_arr_t *l, ejson_arr_t *r)
{
  for (size_t i=0; i<l->nr && i<r->nr; ++i) {
    int rr = ejson_cmp(l->vals[i], r->vals[i]);
    if (rr) return rr;
  }
  if (l->nr < r->nr) return -1;
  return 1;
}

static int _dbl_cmp(double l, double r)
{
  double epsilon = 1e-6;
  if(fabs(l-r) < epsilon) return 0;
  return (l<r) ? -1 : 1;
}

int ejson_cmp(ejson_t *l, ejson_t *r)
{
  if (l->type < r->type) return -1;
  if (l->type > r->type) return 1;
  switch (l->type) {
    case EJSON_NULL:
    case EJSON_FALSE:
    case EJSON_TRUE:
      return 0;
    case EJSON_NUM:
      return _dbl_cmp(l->_num.dbl, r->_num.dbl);
    case EJSON_STR:
      return strcmp(l->_str.str, r->_str.str);
    case EJSON_OBJ:
      return _ejson_obj_cmp(&l->_obj, &r->_obj);
    case EJSON_ARR:
      return _ejson_arr_cmp(&l->_arr, &r->_arr);
    default: return -1;
  }
}

#if 1        /* { */
#define _ADJUST() do {                               \
  if (n < 0) return -1;                              \
  count += n;                                        \
  p += n;                                            \
  len -= n;                                          \
  if ((size_t)n >= len) {                            \
    p = NULL;                                        \
    len = 0;                                         \
  }                                                  \
} while (0)

#define _SNPRINTF(fmt, ...) do {                     \
  n = snprintf(p, len, fmt, ##__VA_ARGS__);          \
  _ADJUST();                                         \
} while (0)

static int _str_serialize(const char *str, size_t nr, char *buf, size_t len)
{
  int count = 0;
  char *p = buf;
  int n = 0;

  // if (!strchr(str, '\\')) {
  //   if (!strchr(str, '"')) {
  //     return snprintf(buf, len, "\"%s\"", str);
  //   }
  //   if (!strchr(str, '\'')) {
  //     return snprintf(buf, len, "'%s'", str);
  //   }
  //   if (!strchr(str, '`')) {
  //     return snprintf(buf, len, "`%s`", str);
  //   }
  // }
  _SNPRINTF("%c", '"');

  const char *escape1 = "\\\"'`";
  const char *escape2         = "\b\f\r\n\t";
  const char *escape2_replace = "bfrnt";
  const char *prev = str;
  for (size_t i=0; i<nr; ++i) {
    const char *curr = str + i;
    const char c = *curr;
    if (strchr(escape1, c)) {
      if (curr > prev) {
        _SNPRINTF("%.*s", (int)(curr - prev), prev);
      }
      _SNPRINTF("\\%c", c);
      prev = ++curr;
      continue;
    }
    const char *pp = strchr(escape2, c);
    if (pp) {
      if (curr > prev) {
        _SNPRINTF("%.*s", (int)(curr - prev), prev);
      }
      _SNPRINTF("\\%c", escape2_replace[pp-escape2]);
      prev = ++curr;
      continue;
    }
  }
  if (*prev) {
    _SNPRINTF("%s", prev);
  }
  _SNPRINTF("%c", '"');

  return count;
}

static int _ejson_str_serialize(_ejson_str_t *str, char *buf, size_t len)
{
  return _str_serialize(str->str, str->nr, buf, len);
}

static int _ejson_kv_serialize(_ejson_kv_t *kv, char *buf, size_t len)
{
  int count = 0;
  char *p = buf;
  int n = 0;

  n = _str_serialize(kv->key.str, kv->key.nr, buf, len);
  _ADJUST();
  _SNPRINTF(":");

  n = ejson_serialize(kv->val, p, len);
  _ADJUST();

  return count;
}

static int _ejson_obj_serialize(ejson_obj_t *obj, char *buf, size_t len)
{
  int count = 0;
  char *p = buf;
  int n = 0;

  _SNPRINTF("{");
  for (size_t i=0; i<obj->nr; ++i) {
    _ejson_kv_t *kv = obj->kvs + i;
    if (i) _SNPRINTF(",");
    n = _ejson_kv_serialize(kv, p, len);
    _ADJUST();
  }
  _SNPRINTF("}");

  return count;
}

static int _ejson_arr_serialize(ejson_arr_t *arr, char *buf, size_t len)
{
  int count = 0;
  char *p = buf;
  int n = 0;

  _SNPRINTF("[");
  for (size_t i=0; i<arr->nr; ++i) {
    ejson_t *val = arr->vals[i];
    if (i) _SNPRINTF(",");
    n = ejson_serialize(val, p, len);
    _ADJUST();
  }
  _SNPRINTF("]");

  return count;
}

#undef _ADJUST
#undef _SNPRINTF
#endif       /* } */

int ejson_serialize(ejson_t *ejson, char *buf, size_t len)
{
  if (buf && len) *buf = '\0';
  if (!ejson) return 1;

  switch (ejson->type) {
    case EJSON_NULL:
      return snprintf(buf, len, "null");
    case EJSON_FALSE:
      return snprintf(buf, len, "false");
    case EJSON_TRUE:
      return snprintf(buf, len, "true");
    case EJSON_NUM:
      return snprintf(buf, len, "%lg", ejson->_num.dbl);
    case EJSON_STR:
      return _ejson_str_serialize(&ejson->_str, buf, len);
    case EJSON_OBJ:
      return _ejson_obj_serialize(&ejson->_obj, buf, len);
    case EJSON_ARR:
      return _ejson_arr_serialize(&ejson->_arr, buf, len);
    default: return -1;
  }
}

const parser_loc_t* ejson_get_loc(ejson_t *ejson)
{
  if (!ejson) return NULL;
  return &ejson->loc;
}

