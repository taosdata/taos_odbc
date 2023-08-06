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

#ifndef _ejson_parser_h_
#define _ejson_parser_h_

#include "macros.h"

#include "parser.h"

#include <stddef.h>

EXTERN_C_BEGIN

typedef struct ejson_parser_param_s             ejson_parser_param_t;
typedef struct ejson_parser_ctx_s               ejson_parser_ctx_t;
typedef struct ejson_parser_token_s             ejson_parser_token_t;

typedef enum ejson_type_e               ejson_type_t;
typedef struct ejson_s                  ejson_t;
typedef struct _ejson_str_s             _ejson_str_t;
typedef struct _ejson_kv_s              _ejson_kv_t;

struct _ejson_str_s {
  char                      *str;
  size_t                     cap;
  size_t                     nr;

  parser_loc_t               loc;
};

struct _ejson_kv_s {
  _ejson_str_t               key;
  ejson_t                   *val;

  parser_loc_t               loc;
};

enum ejson_type_e {
  EJSON_NULL,
  EJSON_FALSE,
  EJSON_TRUE,
  EJSON_NUM,
  EJSON_STR,
  EJSON_OBJ,
  EJSON_ARR,
};

void ejson_inc_ref(ejson_t *ejson) FA_HIDDEN;
void ejson_dec_ref(ejson_t *ejson) FA_HIDDEN;
ejson_t* ejson_new_null(void) FA_HIDDEN;
ejson_t* ejson_new_true(void) FA_HIDDEN;
ejson_t* ejson_new_false(void) FA_HIDDEN;
ejson_t* ejson_new_num(double v) FA_HIDDEN;
ejson_t* ejson_new_obj(void) FA_HIDDEN;
ejson_t* ejson_new_arr(void) FA_HIDDEN;
int ejson_arr_append(ejson_t *ejson, ejson_t *v) FA_HIDDEN;


int ejson_is_null(ejson_t *ejson) FA_HIDDEN;
int ejson_is_true(ejson_t *ejson) FA_HIDDEN;
int ejson_is_false(ejson_t *ejson) FA_HIDDEN;
int ejson_is_str(ejson_t *ejson) FA_HIDDEN;
int ejson_is_num(ejson_t *ejson) FA_HIDDEN;
int ejson_is_obj(ejson_t *ejson) FA_HIDDEN;
int ejson_is_arr(ejson_t *ejson) FA_HIDDEN;

int ejson_cmp(ejson_t *l, ejson_t *r) FA_HIDDEN;
int ejson_serialize(ejson_t *ejson, char *buf, size_t len) FA_HIDDEN;

const parser_loc_t* ejson_get_loc(ejson_t *ejson) FA_HIDDEN;

const char* ejson_str_get(ejson_t *ejson) FA_HIDDEN;
int ejson_num_get(ejson_t *ejson, double *v) FA_HIDDEN;
size_t ejson_obj_count(ejson_t *ejson) FA_HIDDEN;
ejson_t* ejson_obj_idx(ejson_t *ejson, size_t idx, const char **k) FA_HIDDEN;
ejson_t* ejson_obj_get(ejson_t *ejson, const char *k) FA_HIDDEN;
size_t ejson_arr_count(ejson_t *ejson) FA_HIDDEN;
ejson_t* ejson_arr_get(ejson_t *ejson, size_t idx) FA_HIDDEN;


void _ejson_str_reset(_ejson_str_t *str) FA_HIDDEN;
void _ejson_str_release(_ejson_str_t *str) FA_HIDDEN;
int _ejson_str_init(_ejson_str_t *str, const char *v, size_t n) FA_HIDDEN;
int _ejson_str_append(_ejson_str_t *str, const char *v, size_t n) FA_HIDDEN;


void _ejson_kv_release(_ejson_kv_t *kv) FA_HIDDEN;
void _ejson_kv_init(_ejson_kv_t *kv) FA_HIDDEN;







struct ejson_parser_token_s {
  const char      *text;
  size_t           leng;
};

struct ejson_parser_ctx_s {
  parser_loc_t           loc;
  char                   err_msg[1024];

  unsigned int           debug_flex:1;
  unsigned int           debug_bison:1;
  unsigned int           oom:1;
};

struct ejson_parser_param_s {
  ejson_parser_ctx_t                 ctx;
  ejson_t                           *ejson;
};

void ejson_parser_param_release(ejson_parser_param_t *param) FA_HIDDEN;

int ejson_parser_parse(const char *input, size_t len,
    ejson_parser_param_t *param) FA_HIDDEN;

EXTERN_C_END

#endif // _ejson_parser_h_

