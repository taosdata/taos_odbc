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

#ifndef _columns_h_
#define _columns_h_

#include "columns.h"

EXTERN_C_BEGIN

typedef struct columns_args_s              columns_args_t;
typedef struct columns_ctx_s               columns_ctx_t;
typedef struct columns_col_meta_s          columns_col_meta_t;

void columns_args_release(columns_args_t *args) FA_HIDDEN;
void columns_ctx_release(columns_ctx_t *ctx) FA_HIDDEN;

SQLSMALLINT columns_get_count_of_col_meta(void) FA_HIDDEN;
columns_col_meta_t* columns_get_col_meta(int i_col) FA_HIDDEN;

EXTERN_C_END

#endif //  _columns_h_
