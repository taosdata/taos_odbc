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

static ejson_t* _ejson_new_str(_ejson_str_t *str);
static ejson_t* _ejson_new_obj(_ejson_kv_t *kv);
static int _ejson_obj_set(ejson_t *ejson, _ejson_kv_t *kv);

#include "ejson_parser.tab.h"
#include "ejson_parser.lex.c"

#include "ejson_parser.lex.h"
#undef yylloc
#undef yylval
#include "ejson_parser.tab.c"


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
  int                        refc;
};

static const ejson_t        _ejson_null = {
  .type           = EJSON_NULL,
  .refc           = 1,
};

static const ejson_t        _ejson_true = {
  .type           = EJSON_TRUE,
  .refc           = 1,
};

static const ejson_t        _ejson_false = {
  .type           = EJSON_FALSE,
  .refc           = 1,
};

ejson_t* ejson_new_null(void)
{
  return (ejson_t*)&_ejson_null;
}

ejson_t* ejson_new_true(void)
{
  return (ejson_t*)&_ejson_true;
}

ejson_t* ejson_new_false(void)
{
  return (ejson_t*)&_ejson_false;
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
  if (ejson == (ejson_t*)&_ejson_null ||
      ejson == (ejson_t*)&_ejson_true ||
      ejson == (ejson_t*)&_ejson_false)
  {
    return;
  }

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

static int _ejson_kv_set(_ejson_kv_t *kv, const char *k, size_t n, ejson_t *v)
{
  size_t len = strnlen(k, n);
  int r = _ejson_str_init(&kv->key, k, len);
  if (r) return -1;

  kv->val = v;
  _ejson_inc_ref(v);

  return 0;
}

static int _ejson_obj_append(ejson_obj_t *obj, const char *k, size_t n, ejson_t *v)
{
  int r = _ejson_obj_keep(obj, obj->nr + 1);
  if (r) return -1;

  _ejson_kv_t *kv = obj->kvs + obj->nr;
  memset(kv, 0, sizeof(*kv));
  r = _ejson_kv_set(kv, k, n, v);
  if (r) return -1;

  ++obj->nr;

  return 0;
}

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

ejson_t* ejson_new_str(const char *v, size_t n)
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
  if (ejson == (ejson_t*)&_ejson_null ||
      ejson == (ejson_t*)&_ejson_true ||
      ejson == (ejson_t*)&_ejson_false)
  {
    return;
  }
  ++ejson->refc;
}

static void _ejson_release(ejson_t *ejson)
{
  switch (ejson->type) {
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
  if (ejson == (ejson_t*)&_ejson_null ||
      ejson == (ejson_t*)&_ejson_true ||
      ejson == (ejson_t*)&_ejson_false)
  {
    return;
  }
  if (ejson->refc > 1) {
    --ejson->refc;
    return;
  }
  --ejson->refc;
  _ejson_release(ejson);
  free(ejson);
}

int ejson_str_append(ejson_t *ejson, const char *v, size_t n)
{
  if (ejson->type != EJSON_STR) return -1;
  return _ejson_str_append(&ejson->_str, v, n);
}

int ejson_obj_set(ejson_t *ejson, const char *k, size_t n, ejson_t *v)
{
  if (ejson->type != EJSON_OBJ) return -1;
  return _ejson_obj_append(&ejson->_obj, k, n, v);
}

int ejson_arr_append(ejson_t *ejson, ejson_t *v)
{
  if (ejson->type != EJSON_ARR) return -1;
  return _ejson_arr_append(&ejson->_arr, v);
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


int ejson_is_null(ejson_t *ejson, int *is)
{
  if (!ejson) return -1;
  *is = (ejson->type == EJSON_NULL) ? 1 : 0;
  return 0;
}

int ejson_is_true(ejson_t *ejson, int *is)
{
  if (!ejson) return -1;
  *is = (ejson->type == EJSON_TRUE) ? 1 : 0;
  return 0;
}

int ejson_is_false(ejson_t *ejson, int *is)
{
  if (!ejson) return -1;
  *is = (ejson->type == EJSON_FALSE) ? 1 : 0;
  return 0;
}

int ejson_is_str(ejson_t *ejson, int *is)
{
  if (!ejson) return -1;
  *is = (ejson->type == EJSON_STR) ? 1 : 0;
  return 0;
}

int ejson_is_num(ejson_t *ejson, int *is)
{
  if (!ejson) return -1;
  *is = (ejson->type == EJSON_NUM) ? 1 : 0;
  return 0;
}

int ejson_is_obj(ejson_t *ejson, int *is)
{
  if (!ejson) return -1;
  *is = (ejson->type == EJSON_OBJ) ? 1 : 0;
  return 0;
}

int ejson_is_arr(ejson_t *ejson, int *is)
{
  if (!ejson) return -1;
  *is = (ejson->type == EJSON_ARR) ? 1 : 0;
  return 0;
}

int ejson_str_get(ejson_t *ejson, const char **v, size_t *n)
{
  int is = 0;
  int r = ejson_is_str(ejson, &is);
  if (r || !is) return -1;
  *v = ejson->_str.str;
  *n = ejson->_str.nr;
  return 0;
}

int ejson_num_get(ejson_t *ejson, double *v)
{
  int is = 0;
  int r = ejson_is_num(ejson, &is);
  if (r || !is) return -1;
  *v = ejson->_num.dbl;
  return 0;
}

int ejson_obj_get(ejson_t *ejson, const char *k, ejson_t **v)
{
  int is = 0;
  int r = ejson_is_obj(ejson, &is);
  if (r || !is) return -1;
  for (size_t i=0; i<ejson->_obj.nr; ++i) {
    _ejson_kv_t *kv = ejson->_obj.kvs + i;
    if (strcmp(k, kv->key.str)) continue;
    if (kv->val) {
      *v = kv->val;
      ejson_inc_ref(*v);
    } else {
      *v = ejson_new_null();
    }
    return 0;
  }
  *v = NULL;
  return 0;
}

int ejson_arr_get_count(ejson_t *ejson, size_t *count)
{
  int is = 0;
  int r = ejson_is_arr(ejson, &is);
  if (r || !is) return -1;
  *count = ejson->_arr.nr;
  return 0;
}

int ejson_arr_get(ejson_t *ejson, size_t idx, ejson_t **v)
{
  int is = 0;
  int r = ejson_is_arr(ejson, &is);
  if (r || !is) return -1;
  if (idx >= ejson->_arr.nr) {
    *v = NULL;
    return 0;
  }
  *v = ejson->_arr.vals[idx];
  ejson_inc_ref(*v);
  return 0;
}

