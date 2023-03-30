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

static void _topic_reset_res(topic_t *topic)
{
  if (!topic) return;
  if (!topic->res) return;
  CALL_taos_free_result(topic->res);
  topic->res = NULL;
  topic->res_topic_name        = NULL;
  topic->res_db_name           = NULL;
  topic->res_vgroup_id         = 0;
}

static void _topic_reset_tmq(topic_t *topic)
{
  if (!topic) return;
  if (!topic->tmq) return;
  CALL_tmq_unsubscribe(topic->tmq);
  CALL_tmq_consumer_close(topic->tmq);
  topic->tmq = NULL;
}

void topic_reset(topic_t *topic)
{
  if (!topic) return;
  topic->row = NULL;
  _topic_reset_res(topic);
  _topic_reset_tmq(topic);
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
  if (!topic->res) {
    stmt_append_err(topic->owner, "HY000", 0, "General error:not implemented yet");
    return SQL_ERROR;
  }

  if (fields) *fields = topic->fields;
  if (nr) *nr = topic->fields_nr;
  return SQL_SUCCESS;
}

static SQLRETURN _topic_desc_tripple(topic_t *topic)
{
  topic->res_topic_name = CALL_tmq_get_topic_name(topic->res);
  if (!topic->res_topic_name) {
    stmt_append_err(topic->owner, "HY000", 0, "General error:tmq_get_topic_name failed");
    return SQL_ERROR;
  }
  topic->res_db_name    = CALL_tmq_get_db_name(topic->res);
  if (!topic->res_db_name) {
    stmt_append_err(topic->owner, "HY000", 0, "General error:tmq_get_db_name failed");
    return SQL_ERROR;
  }
  topic->res_vgroup_id  = CALL_tmq_get_vgroup_id(topic->res);

  TAOS_FIELD *fields = CALL_taos_fetch_fields(topic->res);
  if (!fields) {
    stmt_append_err_format(topic->owner, "HY000", 0,
        "General error:taos_fetch_fields failed:[%d]%s",
        taos_errno(topic->res), taos_errstr(topic->res));
    return SQL_ERROR;
  }
  size_t nr = CALL_taos_field_count(topic->res);

  const char *pseudos[] = {
    "_topic_name",
    "_db_name",
    "_vgroup_id",
  };
  for (size_t i=0; i<nr; ++i) {
    TAOS_FIELD *field = fields + i;
    for (size_t j=0; j<sizeof(pseudos)/sizeof(pseudos[0]); ++j) {
      if (tod_strncasecmp(field->name, pseudos[i], sizeof(field->name))) {
        stmt_append_err_format(topic->owner, "HY000", 0,
            "General error:column[%" PRId64 "]`%.*s` in select list conflicts with the pseudo-name:[%s]",
            i+1, (int)sizeof(field->name), field->name, pseudos[i]);
        return SQL_ERROR;
      }
    }
  }

  if (nr + 3 > topic->fields_cap) {
    size_t cap = (nr + 3 + 15) / 16 * 16;
    TAOS_FIELD *p = (TAOS_FIELD*)realloc(topic->fields, sizeof(*p) * cap);
    if (!p) {
      stmt_oom(topic->owner);
      return SQL_ERROR;
    }
    topic->fields        = p;
    topic->fields_cap    = cap;
  }
  topic->fields_nr = nr + 3;

  memcpy(topic->fields + 3, fields, nr);
  snprintf(topic->fields[0].name, sizeof(topic->fields[0].name), "_topic_name");
  snprintf(topic->fields[1].name, sizeof(topic->fields[1].name), "_db_name");
  snprintf(topic->fields[2].name, sizeof(topic->fields[2].name), "_vgroup_id");
  topic->fields[0].type = TSDB_DATA_TYPE_VARCHAR;
  topic->fields[0].bytes = strlen(topic->res_topic_name);
  topic->fields[1].type = TSDB_DATA_TYPE_VARCHAR;
  topic->fields[1].bytes = strlen(topic->res_db_name);
  topic->fields[2].type = TSDB_DATA_TYPE_INT;
  topic->fields[2].bytes = sizeof(int32_t);

  return SQL_SUCCESS;
}

static SQLRETURN _fetch_row(stmt_base_t *base)
{
  SQLRETURN sr = SQL_SUCCESS;
  topic_t *topic = (topic_t*)base;
  (void)topic;

  TAOS_ROW row = NULL;

again:

  while (topic->res == NULL) {
    int32_t timeout = 100;
    topic->res = CALL_tmq_consumer_poll(topic->tmq, timeout);
  }

  sr = _topic_desc_tripple(topic);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  row = CALL_taos_fetch_row(topic->res);
  if (row == NULL) {
    _topic_reset_res(topic);
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
  if (ColumnCountPtr) *ColumnCountPtr = topic->fields_nr;
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

  TAOS_FIELD *fields = topic->fields;
  int i = Col_or_Param_Num - 1;
  TAOS_FIELD *field = fields + i;

  if (i == 0) {
    tsdb->type                   = TSDB_DATA_TYPE_VARCHAR;
    tsdb->str.str                = topic->res_topic_name;
    tsdb->str.len                = strlen(topic->res_topic_name);
    return SQL_SUCCESS;
  }

  if (i == 1) {
    tsdb->type                   = TSDB_DATA_TYPE_VARCHAR;
    tsdb->str.str                = topic->res_db_name;
    tsdb->str.len                = strlen(topic->res_db_name);
    return SQL_SUCCESS;
  }

  if (i == 2) {
    tsdb->type                   = TSDB_DATA_TYPE_INT;
    tsdb->i32                    = topic->res_vgroup_id;
    return SQL_SUCCESS;
  }

  int *offsets = CALL_taos_get_column_data_offset(topic->res, i-3);

  char *col = row[i-3];
  if (col) col += offsets ? *offsets : 0;

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
      tsdb->ts.precision = CALL_taos_result_precision(topic->res);
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
  tmq_conf_t*    conf = CALL_tmq_conf_new();

  code = CALL_tmq_conf_set(conf, "enable.auto.commit", "true");
  if (TMQ_CONF_OK != code) {
    CALL_tmq_conf_destroy(conf);
    return NULL;
  }

  code = CALL_tmq_conf_set(conf, "auto.commit.interval.ms", "100");
  if (TMQ_CONF_OK != code) {
    CALL_tmq_conf_destroy(conf);
    return NULL;
  }

  code = CALL_tmq_conf_set(conf, "group.id", "cgrpName");
  if (TMQ_CONF_OK != code) {
    CALL_tmq_conf_destroy(conf);
    return NULL;
  }

  code = CALL_tmq_conf_set(conf, "client.id", "user defined name");
  if (TMQ_CONF_OK != code) {
    CALL_tmq_conf_destroy(conf);
    return NULL;
  }

  if (0) code = CALL_tmq_conf_set(conf, "td.connect.user", "root");
  if (TMQ_CONF_OK != code) {
    CALL_tmq_conf_destroy(conf);
    return NULL;
  }

  if (0) code = CALL_tmq_conf_set(conf, "td.connect.pass", "taosdata");
  if (TMQ_CONF_OK != code) {
    CALL_tmq_conf_destroy(conf);
    return NULL;
  }

  code = CALL_tmq_conf_set(conf, "auto.offset.reset", "earliest");
  if (TMQ_CONF_OK != code) {
    CALL_tmq_conf_destroy(conf);
    return NULL;
  }

  code = CALL_tmq_conf_set(conf, "experimental.snapshot.enable", "false");
  if (TMQ_CONF_OK != code) {
    CALL_tmq_conf_destroy(conf);
    return NULL;
  }

  CALL_tmq_conf_set_auto_commit_cb(conf, _tmq_commit_cb_print, NULL);

  tmq_t* tmq = CALL_tmq_consumer_new(conf, NULL, 0);
  CALL_tmq_conf_destroy(conf);
  return tmq;
}

static tmq_list_t* _build_topic_list(const char *topic_name) {
  tmq_list_t* topicList = CALL_tmq_list_new();
  int32_t     code = CALL_tmq_list_append(topicList, topic_name);
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
  SQLRETURN sr = SQL_SUCCESS;
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
    CALL_tmq_consumer_close(tmq);
    stmt_append_err(topic->owner, "HY000", 0, "General error:build_topic_list() failed");
    return SQL_ERROR;
  }

  r = CALL_tmq_subscribe(tmq, topic_list);
  CALL_tmq_list_destroy(topic_list);

  if (r) {
    stmt_append_err_format(topic->owner, "HY000", 0, "General error:tmp_subscribe() failed:[%d/0x%x]%s", r, r, tmq_err2str(r));
    return SQL_ERROR;
  }

  topic->tmq = tmq;

  while (topic->res == NULL) {
    int32_t timeout = 100;
    topic->res = CALL_tmq_consumer_poll(topic->tmq, timeout);
  }

  sr = _topic_desc_tripple(topic);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  return SQL_SUCCESS;
}

