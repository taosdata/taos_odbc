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

#include "odbc_helpers.h"
#include "c_test_helper.h"

#include <stdarg.h>

int _simple_str_fmt_impl(simple_str_t* str, const char* fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    char* base = str->base ? str->base + str->nr : NULL;
    size_t sz = str->cap - str->nr;
    int n = vsnprintf(base, sz, fmt, ap);
    va_end(ap);

    if (n < 0) {
        int e = errno;
        E("vsnprintf failed:[%d]%s", e, strerror(e));
        return -1;
    }

    if (str->base) {
        if ((size_t)n >= sz) {
            W("buffer too small");
            return -1;
        }
        str->nr += n;
    }

    return 0;
}

int _gen_table_create_sql(simple_str_t* str, const char* table, const field_t* fields, size_t nr_fields)
{
    int r = 0;
    r = _simple_str_fmt(str, "create table %s", table);
    if (r) return -1;

    for (size_t i = 0; i < nr_fields; ++i) {
        const field_t* field = fields + i;
        if (i == 0) {
            r = _simple_str_fmt(str, " (");
        }
        else {
            r = _simple_str_fmt(str, ",");
        }
        if (r) return -1;
        r = _simple_str_fmt(str, "%s %s", field->name, field->field);
    }
    if (nr_fields > 0) {
        r = _simple_str_fmt(str, ");");
        if (r) return -1;
    }

    return 0;
}

int _gen_table_param_insert(simple_str_t* str, const char* table, const field_t* fields, size_t nr_fields)
{
    int r = 0;
    r = _simple_str_fmt(str, "insert into %s", table);
    if (r) return -1;

    for (size_t i = 0; i < nr_fields; ++i) {
        const field_t* field = fields + i;
        if (i == 0) {
            r = _simple_str_fmt(str, " (");
        }
        else {
            r = _simple_str_fmt(str, ",");
        }
        if (r) return -1;
        r = _simple_str_fmt(str, "%s", field->name);

        if (i + 1 < nr_fields) continue;
        r = _simple_str_fmt(str, ")");
        if (r) return -1;
    }

    for (size_t i = 0; i < nr_fields; ++i) {
        if (i == 0) {
            r = _simple_str_fmt(str, " values (");
        }
        else {
            r = _simple_str_fmt(str, ",");
        }
        if (r) return -1;
        r = _simple_str_fmt(str, "?");

        if (i + 1 < nr_fields) continue;
        r = _simple_str_fmt(str, ")");
        if (r) return -1;
    }

    return 0;
}
