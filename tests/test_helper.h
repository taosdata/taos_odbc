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

#ifndef _test_helper_h_
#define _test_helper_h_

#include "os_port.h"
#include "cjson/cJSON.h"
#include "ejson_parser.h"
#include "odbc_helpers.h"

EXTERN_C_BEGIN

int json_get_by_path(cJSON *root, const char *path, cJSON **json) FA_HIDDEN;
const char* json_object_get_string(cJSON *json, const char *key) FA_HIDDEN;
int json_object_get_number(cJSON *json, const char *key, double *v) FA_HIDDEN;
int json_get_number(cJSON *json, double *v) FA_HIDDEN;
cJSON* json_object_get_array(cJSON *json, const char *key) FA_HIDDEN;
int json_get_by_item(cJSON *root, int item, cJSON **json) FA_HIDDEN;
cJSON* load_json_file(const char *json_file, char *buf, size_t bytes, const char *fromcode, const char *tocode) FA_HIDDEN;
const char* json_to_string(cJSON *json) FA_HIDDEN;



int ejson_get_by_path(ejson_t *ejson, const char *path, ejson_t **v) FA_HIDDEN;
const char* ejson_object_get_string(ejson_t *ejson, const char *key) FA_HIDDEN;
int ejson_object_get_number(ejson_t *ejson, const char *key, double *v) FA_HIDDEN;
int ejson_get_number(ejson_t *ejson, double *v) FA_HIDDEN;
ejson_t* ejson_object_get_array(ejson_t *ejson, const char *key) FA_HIDDEN;
int ejson_get_by_item(ejson_t *ejson, int item, ejson_t **v) FA_HIDDEN;
ejson_t* load_ejson_file(const char *ejson_file, char *buf, size_t bytes, const char *fromcode, const char *tocode) FA_HIDDEN;
const char* ejson_to_string(ejson_t *ejson) FA_HIDDEN;

/**************     from api_test.c  start    *************/
#define TEST_CASE_BEG(_func)                                \
  D("test case: %s...", _func)

#define TEST_CASE_END(_func, _exp)                          \
  if (MATCH(r, _exp)) {                                     \
    D("test case: %s --> %s%s%s",                           \
                    _func,                                  \
                    color_green(),                          \
                    "succeeded",                            \
                    color_reset());                         \
  } else if (r) {                                           \
    D("test case: %s --> %s%s%s",                           \
                    _func,                                  \
                    color_red(),                            \
                    "failed",                               \
                    color_reset());                         \
    return -1;                                              \
  } else {                                                  \
    D("test case: %s --> %s%s%s",                           \
                    _func,                                  \
                    color_red(),                            \
                    "failure expected but succeeded",       \
                    color_reset());                         \
    return -1;                                              \
  }

#define CHK0(_func, _exp)                                                                          \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_exp ")");                                                           \
    r = _func();                                                                                   \
    TEST_CASE_END(#_func "(" #_exp ")", _exp);                                                     \
  }

#define CHK1(_func, _arg1, _exp)                                                                   \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_arg1 "," #_exp ")");                                                \
    r = _func(_arg1);                                                                              \
    TEST_CASE_END(#_func "(" #_arg1 "," #_exp ")", _exp);                                          \
  }

#define CHK2(_func, _v1, _v2, _exp)                                                                \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_v1 "," #_v2 "," #_exp ")");                                         \
    r = _func(_v1, _v2);                                                                           \
    TEST_CASE_END(#_func "(" #_v1 "," #_v2 "," #_exp ")", _exp);                                   \
  }

#define CHK3(_func, _v1, _v2, _v3, _exp)                                                           \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_v1 "," #_v2 "," #_v3 "," #_exp ")");                                \
    r = _func(_v1, _v2, _v3);                                                                      \
    TEST_CASE_END(#_func "(" #_v1 "," #_v2 "," #_v3 "," #_exp ")", _exp);                          \
  }

#define CHK4(_func, _v1, _v2, _v3, _v4, _exp)                                                      \
  {                                                                                                \
    int r;                                                                                         \
    TEST_CASE_BEG(#_func "(" #_v1 "," #_v2 "," #_v3 "," #_v4 "," #_exp ")");                       \
    r = _func(_v1, _v2, _v3, _v4);                                                                 \
    TEST_CASE_END(#_func "(" #_v1 "," #_v2 "," #_v3 "," #_v4 "," #_exp ")", _exp);                 \
  }

#define MATCH(a, b)  (!!(a) == !!(b))

__attribute__((unused))
static int test_ok(void)
{
    return 0;
}

__attribute__((unused))
static int test_failure(void)
{
    return -1;
}
/**************     from api_test.c  end    *************/



/**************     from conformance_test.c  start    *************/
#define _exec_(hstmt, fmt, ...) (0 ? printf(fmt, ##__VA_ARGS__) : _exec_impl(hstmt, fmt, ##__VA_ARGS__))

typedef struct simple_str_s                 simple_str_t;
struct simple_str_s {
    size_t                       cap;
    size_t                       nr;
    char* base;
};

typedef struct param_s                   param_t;
struct param_s {
    SQLSMALLINT     InputOutputType;
    SQLSMALLINT     ValueType;
    SQLSMALLINT     ParameterType;
    SQLULEN         ColumnSize;
    SQLSMALLINT     DecimalDigits;
    SQLPOINTER      ParameterValuePtr;
    SQLLEN          BufferLength;
    SQLLEN* StrLen_or_IndPtr;
};

static int _simple_str_fmt_impl(simple_str_t* str, const char* fmt, ...)
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

#define _simple_str_fmt(str, fmt, ...) (0 ? printf(fmt, ##__VA_ARGS__) : _simple_str_fmt_impl(str, fmt, ##__VA_ARGS__))

typedef struct field_s            field_t;
struct field_s {
    const char* name;
    const char* field;
};

static int _gen_table_create_sql(simple_str_t* str, const char* table, const field_t* fields, size_t nr_fields)
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

static int _gen_table_param_insert(simple_str_t* str, const char* table, const field_t* fields, size_t nr_fields)
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
/**************     from conformance_test.c  end    *************/

EXTERN_C_END

#endif // _test_helper_h_

