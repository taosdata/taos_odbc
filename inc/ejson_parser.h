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

#include <stddef.h>

EXTERN_C_BEGIN

typedef struct ejson_parser_param_s             ejson_parser_param_t;
typedef struct ejson_parser_ctx_s               ejson_parser_ctx_t;
typedef struct ejson_parser_token_s             ejson_parser_token_t;

struct ejson_parser_token_s {
  const char      *text;
  size_t           leng;
};

struct ejson_parser_ctx_s {
  int                    row0, col0;
  int                    row1, col1;
  char                   err_msg[1024];

  unsigned int           debug_flex:1;
  unsigned int           debug_bison:1;
  unsigned int           oom:1;
};

struct ejson_parser_param_s {
  ejson_parser_ctx_t                 ctx;
};

void ejson_parser_param_release(ejson_parser_param_t *param) FA_HIDDEN;

int ejson_parser_parse(const char *input, size_t len,
    ejson_parser_param_t *param) FA_HIDDEN;

EXTERN_C_END

#endif // _ejson_parser_h_

