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

#ifndef _charset_h_
#define _charset_h_

#include "env.h"

EXTERN_C_BEGIN

typedef struct charset_conv_s            charset_conv_t;
typedef struct charset_conv_mgr_s        charset_conv_mgr_t;

void charset_conv_release(charset_conv_t *cnv) FA_HIDDEN;
int charset_conv_reset(charset_conv_t *cnv, const char *from, const char *to) FA_HIDDEN;

void charset_conv_mgr_release(charset_conv_mgr_t *mgr) FA_HIDDEN;
charset_conv_t* charset_conv_mgr_get_charset_conv(charset_conv_mgr_t *mgr, const char *fromcode, const char *tocode) FA_HIDDEN;



EXTERN_C_END

#endif //  _charset_h_

