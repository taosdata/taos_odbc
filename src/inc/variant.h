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

#ifndef _variant_h_
#define _variant_h_

#include "macros.h"
#include "typedefs.h"

EXTERN_C_BEGIN

void variant_release(variant_t *v) FA_HIDDEN;

variant_t* variant_add(variant_t *args) FA_HIDDEN;
variant_t* variant_sub(variant_t *args) FA_HIDDEN;
variant_t* variant_mul(variant_t *args) FA_HIDDEN;
variant_t* variant_div(variant_t *args) FA_HIDDEN;
variant_t* variant_neg(variant_t *args) FA_HIDDEN;

variant_eval_f variant_get_eval(const char *name, size_t nr) FA_HIDDEN;

EXTERN_C_END

#endif // _variant_h_

