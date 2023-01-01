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

#ifndef _desc_h_
#define _desc_h_

#include "conn.h"

EXTERN_C_BEGIN

typedef struct descriptor_s        descriptor_t;
typedef struct desc_s              desc_t;
typedef struct desc_record_s       desc_record_t;

desc_t* desc_create(conn_t *conn) FA_HIDDEN;
desc_t* desc_ref(desc_t *desc) FA_HIDDEN;
desc_t* desc_unref(desc_t *desc) FA_HIDDEN;

SQLRETURN desc_free(desc_t *desc) FA_HIDDEN;

void descriptor_init(descriptor_t *descriptor) FA_HIDDEN;
void descriptor_release(descriptor_t *descriptor) FA_HIDDEN;

void descriptor_reclaim_buffers(descriptor_t *APD) FA_HIDDEN;

EXTERN_C_END

#endif //  _desc_h_

