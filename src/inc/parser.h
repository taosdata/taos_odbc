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

#ifndef _parser_h_
#define _parser_h_

// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqldriverconnect-function?view=sql-server-ver16

#include "macros.h"

#include "conn.h"

#include <stddef.h>

EXTERN_C_BEGIN

typedef struct parser_token_s           parser_token_t;

struct parser_token_s {
  const char      *text;
  size_t           leng;
};

typedef struct parser_param_s           parser_param_t;

struct parser_param_s {
  connection_cfg_t       conn_str;

  int                    row0, col0;
  int                    row1, col1;
  char                  *errmsg;

  unsigned int           debug_flex:1;
  unsigned int           debug_bison:1;
};

static inline void parser_param_release(parser_param_t *param)
{
  if (!param) return;
  connection_cfg_release(&param->conn_str);
  TOD_SAFE_FREE(param->errmsg);
  param->row0 = 0;
}

int parser_parse(const char *input, size_t len,
    parser_param_t *param) FA_HIDDEN;

EXTERN_C_END

#endif // _parser_h_

