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

#include "internal.h"

#include "topic.h"

#include "desc.h"
#include "errs.h"
#include "log.h"
#include "stmt.h"
#include "taos_helpers.h"

#include <errno.h>

void topic_reset(topic_t *topic)
{
  if (!topic) return;
  topic->row = NULL;
  if (topic->res) {
    taos_free_result(topic->res);
    topic->res = NULL;
  }
  if (topic->tmq) {
    tmq_unsubscribe(topic->tmq);
    tmq_consumer_close(topic->tmq);
    topic->tmq = NULL;
  }
}

void topic_release(topic_t *topic)
{
  if (!topic) return;
  topic_reset(topic);
}

static SQLRETURN _query(stmt_base_t *base, const char *sql)
{
  (void)sql;

  topic_t *topic = (topic_t*)base;
  (void)topic;
  stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _execute(stmt_base_t *base)
{
  topic_t *topic = (topic_t*)base;
  (void)topic;
  stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _get_fields(stmt_base_t *base, TAOS_FIELD **fields, size_t *nr)
{
  (void)fields;
  (void)nr;
  topic_t *topic = (topic_t*)base;
  (void)topic;
  stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _fetch_row(stmt_base_t *base)
{
  topic_t *topic = (topic_t*)base;
  (void)topic;

  TAOS_ROW row = NULL;

again:

  while (topic->res == NULL) {
    int32_t timeout = 100;
    topic->res = tmq_consumer_poll(topic->tmq, timeout);
  }

  row = taos_fetch_row(topic->res);
  if (row == NULL) {
    taos_free_result(topic->res);
    topic->res = NULL;
    goto again;
  }

  topic->row = row;
  return SQL_SUCCESS;
}

static SQLRETURN _describe_param(stmt_base_t *base,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT    *DataTypePtr,
    SQLULEN        *ParameterSizePtr,
    SQLSMALLINT    *DecimalDigitsPtr,
    SQLSMALLINT    *NullablePtr)
{
  (void)ParameterNumber;
  (void)DataTypePtr;
  (void)ParameterSizePtr;
  (void)DecimalDigitsPtr;
  (void)NullablePtr;

  topic_t *topic = (topic_t*)base;
  (void)topic;
  stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _get_num_params(stmt_base_t *base, SQLSMALLINT *ParameterCountPtr)
{
  (void)ParameterCountPtr;

  topic_t *topic = (topic_t*)base;
  (void)topic;
  stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _check_params(stmt_base_t *base)
{
  topic_t *topic = (topic_t*)base;
  (void)topic;
  stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _tsdb_field_by_param(stmt_base_t *base, int i_param, TAOS_FIELD_E **field)
{
  (void)i_param;
  (void)field;

  topic_t *topic = (topic_t*)base;
  (void)topic;
  stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _row_count(stmt_base_t *base, SQLLEN *row_count_ptr)
{
  (void)row_count_ptr;
  topic_t *topic = (topic_t*)base;
  (void)topic;
  stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

static SQLRETURN _get_num_cols(stmt_base_t *base, SQLSMALLINT *ColumnCountPtr)
{
  (void)ColumnCountPtr;
  topic_t *topic = (topic_t*)base;
  if (!topic->res) {
    stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
    return SQL_ERROR;
  }
  int32_t numOfFields = taos_field_count(topic->res);
  if (ColumnCountPtr) *ColumnCountPtr = numOfFields;
  return SQL_SUCCESS;
}

static SQLRETURN _get_data(stmt_base_t *base, SQLUSMALLINT Col_or_Param_Num, tsdb_data_t *tsdb)
{
  topic_t *topic = (topic_t*)base;
  TAOS_ROW row = topic->row;
  if (!row) {
    stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
    return SQL_ERROR;
  }
  TAOS_FIELD *fields = taos_fetch_fields(topic->res);
  int i = Col_or_Param_Num - 1;

  int *offsets = taos_get_column_data_offset(topic->res, i);

  char *col = row[i];
  if (col) col += offsets ? *offsets : 0;

  TAOS_FIELD *field = fields + i;

  tsdb->type = field->type;
  if (col == NULL) {
    tsdb->is_null = 1;
    return SQL_SUCCESS;
  }
  tsdb->is_null = 0;
  switch (tsdb->type) {
    case TSDB_DATA_TYPE_TINYINT:
      tsdb->i8 = *(int8_t*)col;
      break;

    case TSDB_DATA_TYPE_UTINYINT:
      tsdb->u8 = *(uint8_t*)col;
      break;

    case TSDB_DATA_TYPE_SMALLINT:
      tsdb->i16 = *(int16_t*)col;
      break;

    case TSDB_DATA_TYPE_USMALLINT:
      tsdb->u16 = *(uint16_t*)col;
      break;

    case TSDB_DATA_TYPE_INT:
      tsdb->i32 = *(int32_t*)col;
      break;

    case TSDB_DATA_TYPE_UINT:
      tsdb->u32 = *(uint32_t*)col;
      break;

    case TSDB_DATA_TYPE_BIGINT:
      tsdb->i64 = *(int64_t*)col;
      break;

    case TSDB_DATA_TYPE_UBIGINT:
      tsdb->u64 = *(uint64_t*)col;
      break;

    case TSDB_DATA_TYPE_FLOAT:
      tsdb->flt = *(float*)col;
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      tsdb->dbl = *(double*)col;
      break;

    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_NCHAR:
      tsdb->str.len = *(int16_t*)(col - sizeof(int16_t));
      tsdb->str.str = col;
      break;

    case TSDB_DATA_TYPE_TIMESTAMP:
      tsdb->ts.ts = *(int64_t*)col;
      tsdb->ts.precision = taos_result_precision(topic->res);
      break;

    case TSDB_DATA_TYPE_BOOL:
      tsdb->b = !!*(int8_t*)col;
      break;

    default:
      stmt_append_err_format(topic->owner, "HY000", 0, "General error:`%s` not supported yet", taos_data_type(tsdb->type));
      return SQL_ERROR;
  }

  return SQL_SUCCESS;
}

void topic_init(topic_t *topic, stmt_t *stmt)
{
  topic->owner = stmt;
  topic->base.query                        = _query;
  topic->base.execute                      = _execute;
  topic->base.get_fields                   = _get_fields;
  topic->base.fetch_row                    = _fetch_row;
  topic->base.describe_param               = _describe_param;
  topic->base.get_num_params               = _get_num_params;
  topic->base.check_params                 = _check_params;
  topic->base.tsdb_field_by_param          = _tsdb_field_by_param;
  topic->base.row_count                    = _row_count;
  topic->base.get_num_cols                 = _get_num_cols;
  topic->base.get_data                     = _get_data;
}

static void _tmq_commit_cb_print(tmq_t* tmq, int32_t code, void* param) {
  if (0) fprintf(stderr, "%s(): code: %d, tmq: %p, param: %p\n", __func__, code, tmq, param);
}

static tmq_t* _build_consumer() {
  tmq_conf_res_t code;
  tmq_conf_t*    conf = tmq_conf_new();

  code = tmq_conf_set(conf, "enable.auto.commit", "true");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "auto.commit.interval.ms", "100");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "group.id", "cgrpName");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "client.id", "user defined name");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  if (0) code = tmq_conf_set(conf, "td.connect.user", "root");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  if (0) code = tmq_conf_set(conf, "td.connect.pass", "taosdata");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "auto.offset.reset", "earliest");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  code = tmq_conf_set(conf, "experimental.snapshot.enable", "false");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  tmq_conf_set_auto_commit_cb(conf, _tmq_commit_cb_print, NULL);

  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  tmq_conf_destroy(conf);
  return tmq;
}

static tmq_list_t* _build_topic_list(const char *topic_name) {
  tmq_list_t* topicList = tmq_list_new();
  int32_t     code = tmq_list_append(topicList, topic_name);
  if (code) {
    return NULL;
  }
  return topicList;
}

SQLRETURN topic_open(
    topic_t       *topic,
    const char    *name,
    size_t         len)
{
  OA_ILE(topic->tmq == NULL);
  int r = 0;

  int n = snprintf(topic->name, sizeof(topic->name), "%.*s", (int)len, name);
  if (n<0 || (size_t)n >= sizeof(topic->name)) {
    stmt_append_err_format(topic->owner, "HY000", 0, "General error:buffer too small to hold topic `%.*s`", (int)len, name);
    return SQL_ERROR;
  }

  tmq_t* tmq = _build_consumer();
  if (NULL == tmq) {
    stmt_append_err(topic->owner, "HY000", 0, "General error:build_consumer() failed");
    return SQL_ERROR;
  }

  tmq_list_t* topic_list = _build_topic_list(topic->name);
  if (NULL == topic_list) {
    tmq_consumer_close(tmq);
    stmt_append_err(topic->owner, "HY000", 0, "General error:build_topic_list() failed");
    return SQL_ERROR;
  }

  r = tmq_subscribe(tmq, topic_list);
  tmq_list_destroy(topic_list);

  if (r) {
    stmt_append_err(topic->owner, "HY000", 0, "General error:tmp_subscribe() failed");
    return SQL_ERROR;
  }

  topic->tmq = tmq;
  return SQL_SUCCESS;
}

