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

#ifndef _ds_h_
#define _ds_h_

#include "macros.h"
#include "typedefs.h"

EXTERN_C_BEGIN

int ds_res_get_result_precision(ds_res_t *ds_res) FA_HIDDEN;
int ds_res_field_count(ds_res_t *ds_res) FA_HIDDEN;
int ds_res_errno(ds_res_t *ds_res) FA_HIDDEN;
const char* ds_res_errstr(ds_res_t *ds_res) FA_HIDDEN;
ds_fields_t* ds_res_fetch_fields(ds_res_t *ds_res, ds_fields_t *fields) FA_HIDDEN;
int8_t ds_fields_type(ds_fields_t *ds_fields, int i_col) FA_HIDDEN;
int ds_res_fetch_block(ds_res_t *ds_res, ds_block_t *ds_block) FA_HIDDEN;

EXTERN_C_END

#endif // _ds_h_

