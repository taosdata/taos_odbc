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

#include <taos.h>
#include <taosudf.h>
#include <taoserror.h>

#include <libgen.h>
#include <math.h>
#include <stdio.h>

#include <unistd.h>

#define D(fmt, ...) do {                                   \
  FILE *f = fopen("/tmp/taos_udf_set.log", "a");           \
  if (!f) break;                                           \
  fprintf(f, "%s[%d]:%s():" fmt "\n",                      \
      basename((char*)__FILE__), __LINE__, __func__,       \
      ##__VA_ARGS__);                                      \
  fclose(f);                                               \
} while (0)

int32_t sd_init(void)
{
  D("");
  return TSDB_CODE_SUCCESS;
}

int32_t sd_destroy()
{
  D("");
  return TSDB_CODE_SUCCESS;
}

int32_t sd(SUdfDataBlock* block, SUdfColumn* resultCol)
{
  D("");
  if (resultCol->colMeta.type != TSDB_DATA_TYPE_DOUBLE) {
    D("sd(...):return type of [%d]:not implemented yet", resultCol->colMeta.type);
    return TSDB_CODE_UDF_INVALID_INPUT;
  }
  for (int32_t i_col = 0; i_col < block->numOfCols; ++i_col) {
    SUdfColumn* col = block->udfCols[i_col];
    if (col->colMeta.type != TSDB_DATA_TYPE_DOUBLE) {
      D("sd(...):@%d,type of [%d]:not implemented yet", i_col+1, col->colMeta.type);
      return TSDB_CODE_UDF_INVALID_INPUT;
    }
  }
  for (int32_t i_row = 0; i_row < block->numOfRows; ++i_row) {
    double sum = 0.;
    int is_null = 0;
    for (int32_t i_col = 0; i_col < block->numOfCols; ++i_col) {
      if (udfColDataIsNull(block->udfCols[i_col], i_row)) {
        udfColDataSetNull(resultCol, i_row);
        is_null = 1;
        break;
      }
      double dbl = *(double*)udfColDataGetData(block->udfCols[i_col], i_row);
      sum += dbl;
    }
    if (is_null) continue;
    double avg = sum / block->numOfCols;
    sum = 0.;
    for (int32_t i_col = 0; i_col < block->numOfCols; ++i_col) {
      double dbl = *(double*)udfColDataGetData(block->udfCols[i_col], i_row);
      sum += pow(dbl - avg, 2);
    }
    double sd = sqrt(sum / block->numOfCols);
    udfColDataSet(resultCol, i_row, (char*)&sd, false);
  }

  return TSDB_CODE_SUCCESS;
}

