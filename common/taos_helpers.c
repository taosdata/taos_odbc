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

#define _GNU_SOURCE

#include "os_port.h"
#include "taos_helpers.h"

typedef void (*taos_stmt_reclaim_fields_f)(TAOS_STMT *stmt, TAOS_FIELD_E *fields);

static taos_stmt_reclaim_fields_f loaded_taos_stmt_reclaim_fields = NULL;

static void init_taos_apis(void)
{
  void *p = dlsym(RTLD_DEFAULT, "taos_stmt_reclaim_fields");
  loaded_taos_stmt_reclaim_fields = (taos_stmt_reclaim_fields_f)p;
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
  W("no `taos_stmt_reclaim_fields` exported in taos.dll, thus memory leakage is expected");
#else
  free(fields);
#endif
}

int helper_get_tsdb(TAOS_RES *res, TAOS_FIELD *fields, int time_precision, TAOS_ROW rows, int i_row, int i_col, tsdb_data_t *tsdb, char *buf, size_t len)
{
  TAOS_FIELD *field = fields + i_col;

  if (CALL_taos_is_null(res, i_row, i_col)) {
    tsdb->is_null = 1;
    return 0;
  }

  tsdb->is_null = 0;

  switch(field->type) {
    case TSDB_DATA_TYPE_BOOL:
      {
        uint8_t *col = (uint8_t*)rows[i_col];
        col += i_row;
        tsdb->b = !!*col;
      } break;
    case TSDB_DATA_TYPE_TINYINT:
      {
        int8_t *col = (int8_t*)rows[i_col];
        col += i_row;
        tsdb->i8 = *col;
      } break;
    case TSDB_DATA_TYPE_UTINYINT:
      {
        uint8_t *col= (uint8_t*)rows[i_col];
        col += i_row;
        tsdb->u8 = *col;
      } break;
    case TSDB_DATA_TYPE_SMALLINT:
      {
        int16_t *col = (int16_t*)rows[i_col];
        col += i_row;
        tsdb->i16 = *col;
      } break;
    case TSDB_DATA_TYPE_USMALLINT:
      {
        uint16_t *col = (uint16_t*)rows[i_col];
        col += i_row;
        tsdb->u16 = *col;
      } break;
    case TSDB_DATA_TYPE_INT:
      {
        int32_t *col = (int32_t*)rows[i_col];
        col += i_row;
        tsdb->i32 = *col;
      } break;
    case TSDB_DATA_TYPE_UINT:
      {
        uint32_t *col = (uint32_t*)rows[i_col];
        col += i_row;
        tsdb->u32 = *col;
      } break;
    case TSDB_DATA_TYPE_BIGINT:
      {
        int64_t *col = (int64_t*)rows[i_col];
        col += i_row;
        tsdb->i64 = *col;
      } break;
    case TSDB_DATA_TYPE_UBIGINT:
      {
        uint64_t *col = (uint64_t*)rows[i_col];
        col += i_row;
        tsdb->u64 = *col;
      } break;
    case TSDB_DATA_TYPE_FLOAT:
      {
        float *col = (float*)rows[i_col];
        col += i_row;
        tsdb->flt = *col;
      } break;
    case TSDB_DATA_TYPE_DOUBLE:
      {
        double *col = (double*)rows[i_col];
        col += i_row;
        tsdb->dbl = *col;
      } break;
    case TSDB_DATA_TYPE_VARCHAR:
      {
        int *offsets = CALL_taos_get_column_data_offset(res, i_col);
        char *col = (char*)(rows[i_col]);
        col += offsets[i_row];
        int16_t length = *(int16_t*)col;
        col += sizeof(int16_t);
        tsdb->str.str = col;
        tsdb->str.len = length;
      } break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      {
        int64_t *col = (int64_t*)rows[i_col];
        col += i_row;
        tsdb->ts.ts = *col;
        tsdb->ts.precision = time_precision;
      } break;
    case TSDB_DATA_TYPE_NCHAR:
      {
        int *offsets = CALL_taos_get_column_data_offset(res, i_col);
        char *col = (char*)(rows[i_col]);
        col += offsets[i_row];
        int16_t length = *(int16_t*)col;
        col += sizeof(int16_t);
        tsdb->str.str = col;
        tsdb->str.len = length;
      } break;
    default:
      snprintf(buf, len, "Column[%d/%s] conversion from `%s[0x%x/%d]` not implemented yet",
          i_col + 1, field->name, taos_data_type(field->type), field->type, field->type);
      return -1;
  }

  tsdb->type   = field->type;

  return 0;
}

