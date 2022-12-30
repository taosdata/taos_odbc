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

#define _GNU_SOURCE

#include "taos_helpers.h"

typedef void (*taos_stmt_reclaim_fields_f)(TAOS_STMT *stmt, TAOS_FIELD_E *fields);

static taos_stmt_reclaim_fields_f loaded_taos_stmt_reclaim_fields = NULL;

static void init_taos_apis(void)
{
#ifdef _WIN32
#error not implemented yet
#else
  void *p = dlsym(RTLD_DEFAULT, "taos_stmt_reclaim_fields");
  loaded_taos_stmt_reclaim_fields = (taos_stmt_reclaim_fields_f)p;
#endif
}

void bridge_taos_stmt_reclaim_fields(TAOS_STMT *stmt, TAOS_FIELD_E *fields)
{
  static pthread_once_t once = PTHREAD_ONCE_INIT;
  pthread_once(&once, init_taos_apis);
  if (loaded_taos_stmt_reclaim_fields) {
    loaded_taos_stmt_reclaim_fields(stmt, fields);
    return;
  }
#ifdef _WIN32
#error not implemented yet
#else
  free(fields);
#endif
}

int helper_get_data_len(TAOS_RES *res, TAOS_FIELD *field, TAOS_ROW rows, int row, int col, const char **data, int *len)
{
  switch(field->type) {
    case TSDB_DATA_TYPE_BOOL:
      if (CALL_taos_is_null(res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        unsigned char *base = (unsigned char*)rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_TINYINT:
      if (CALL_taos_is_null(res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        unsigned char *base = (unsigned char*)rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_UTINYINT:
      if (CALL_taos_is_null(res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        unsigned char *base = (unsigned char*)rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_SMALLINT:
      if (CALL_taos_is_null(res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        int16_t *base = (int16_t*)rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_USMALLINT:
      if (CALL_taos_is_null(res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        uint16_t *base = (uint16_t*)rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_INT:
      if (CALL_taos_is_null(res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        int32_t *base = (int32_t*)rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_UINT:
      if (CALL_taos_is_null(res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        uint32_t *base = (uint32_t*)rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_BIGINT:
      if (CALL_taos_is_null(res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        int64_t *base = (int64_t*)rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_FLOAT:
      if (CALL_taos_is_null(res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        float *base = (float*)rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_DOUBLE:
      if (CALL_taos_is_null(res, row, col)) {
        *data = NULL;
        *len = 0;
      } else {
        double *base = (double*)rows[col];
        base += row;
        *data = (const char*)base;
        *len = sizeof(*base);
      } break;
    case TSDB_DATA_TYPE_VARCHAR: {
      int *offsets = CALL_taos_get_column_data_offset(res, col);
      // OA_ILE(offsets);
      if (offsets[row] == -1) {
        *data = NULL;
        *len = 0;
      } else {
        char *base = (char*)(rows[col]);
        base += offsets[row];
        int16_t length = *(int16_t*)base;
        base += sizeof(int16_t);
        *data = base;
        *len = length;
      }
    } break;
    case TSDB_DATA_TYPE_TIMESTAMP: {
      int64_t *base = (int64_t*)rows[col];
      base += row;
      *data = (const char*)base;
      *len = sizeof(*base);
    } break;
    case TSDB_DATA_TYPE_NCHAR: {
      int *offsets = CALL_taos_get_column_data_offset(res, col);
      // OA_ILE(offsets);
      if (offsets[row] == -1) {
        *data = NULL;
        *len = 0;
      } else {
        char *base = (char*)(rows[col]);
        base += offsets[row];
        int16_t length = *(int16_t*)base;
        base += sizeof(int16_t);
        *data = base;
        *len = length;
      }
    } break;
      return -1;
  }

  return 0;
}
