#ifndef _taos_helpers_h_
#define _taos_helpers_h_

#include "helpers.h"

#include <taos.h>
#include <taoserror.h>

#define diag_res(res) do {                                           \
  TAOS_RES *__res = res;                                             \
  int _e         = taos_errno(__res);                                \
  if (!_e) break;                                                    \
  const char *_s = taos_errstr(__res);                               \
  D("taos_errno/str(res:%p) => [%d/0x%x]%s%s%s", __res, _e, _e, color_red(), _s, color_reset());     \
} while (0)

#define diag_stmt(stmt) do {                                                              \
  TAOS_STMT *__stmt = stmt;                                                               \
  int _e          = taos_errno(NULL);                                                     \
  if (!_e) break;                                                                         \
  const char *_s1 = taos_errstr(NULL);                                                    \
  const char *_s2 = taos_stmt_errstr(__stmt);                                             \
  D("taos_errno/str(stmt:%p) => [%d/0x%x]%s%s%s;stmt_errstr:%s%s%s", __stmt, _e, _e, color_red(), _s1, color_reset(), color_red(), _s2, color_reset());   \
} while (0)


#define CALL_taos_init() ({                  \
  D("taos_init() ...");                      \
  int _r = taos_init();                      \
  D("taos_init() => %d", _r);                \
  _r;                                        \
})

#define CALL_taos_cleanup() ({               \
  D("taos_cleanup() ...");                   \
  taos_cleanup();                            \
  D("taos_cleanup() => void");               \
})

#define CALL_taos_connect(ip, user, pass, db, port) ({                                                     \
  const char *_ip   = ip;                                                                                  \
  const char *_user = user;                                                                                \
  const char *_pass = pass;                                                                                \
  const char *_db   = db;                                                                                  \
  uint16_t _port    = port;                                                                                \
  D("taos_connect(ip:%s, usr:%s, pass:%s, db:%s, port:%d) ...", _ip, _user, _pass, _db, _port);            \
  TAOS *_taos = taos_connect(_ip, _user, _pass, _db, _port);                                               \
  if (!_taos) diag_res(NULL);                                                                              \
  D("taos_connect(ip:%s, usr:%s, pass:%s, db:%s, port:%d) => %p", _ip, _user, _pass, _db, _port, _taos);   \
  _taos;                                                                                                   \
})

#define CALL_taos_close(taos) ({           \
  TAOS *_taos = taos;                      \
  D("taos_close(taos:%p) ...", _taos);     \
  taos_close(_taos);                       \
  D("taos_close(taos:%p) => void", _taos); \
})

#define CALL_taos_get_server_info(taos) ({                \
  TAOS *_taos = taos;                                     \
  D("taos_get_server_info(taos:%p) ...", _taos);          \
  const char *_info = taos_get_server_info(_taos);        \
  diag_res(NULL);                                         \
  D("taos_get_server_info(taos:%p) => %s", _taos, _info); \
  _info;                                                  \
})

#define CALL_taos_get_client_info() ({                \
  D("taos_get_client_info() ...");                    \
  const char *_info = taos_get_client_info();         \
  diag_res(NULL);                                     \
  D("taos_get_client_info() => %s", _info);           \
  _info;                                              \
})

#define CALL_taos_query(taos, sql) ({                             \
  TAOS *_taos = taos;                                             \
  const char *_sql = sql;                                         \
  D("taos_query(taos:%p, sql:%s) ...", _taos, _sql);              \
  TAOS_RES *_res = taos_query(_taos, _sql);                       \
  diag_res(_res);                                                 \
  D("taos_query(taos:%p, sql:%s) => %p", _taos, _sql, _res);      \
  _res;                                                           \
})

#define CALL_taos_free_result(res) ({              \
  TAOS_RES *_res = res;                            \
  D("taos_free_result(res:%p) ...", _res);         \
  taos_free_result(_res);                          \
  D("taos_free_result(res:%p) => void", _res);     \
})

#define CALL_taos_stmt_init(taos) ({                      \
  TAOS *_taos = taos;                                     \
  D("taos_stmt_init(taos:%p) ...", _taos);                \
  TAOS_STMT *_stmt = taos_stmt_init(_taos);               \
  D("taos_stmt_init(taos:%p) => %p", _taos, _stmt);       \
  _stmt;                                                  \
})

#define CALL_taos_stmt_close(stmt) ({                     \
  TAOS_STMT *_stmt = stmt;                                \
  D("taos_stmt_close(stmt:%p) ...", _stmt);               \
  int _r = taos_stmt_close(_stmt);                        \
  D("taos_stmt_close(stmt:%p) => %d", _stmt, _r);         \
  _r;                                                     \
})

#define CALL_taos_stmt_prepare(stmt, sql, length) ({                                       \
  TAOS_STMT     *_stmt   = stmt;                                                           \
  const char    *_sql    = sql;                                                            \
  unsigned long  _length = length;                                                         \
  D("taos_stmt_prepare(stmt:%p, sql:%s, length:%ld) ...", _stmt, _sql, _length);           \
  int _r = taos_stmt_prepare(_stmt, _sql, _length);                                        \
  if (_r) diag_res(NULL);                                                                  \
  D("taos_stmt_prepare(stmt:%p, sql:%s, length:%ld) => %d", _stmt, _sql, _length, _r);     \
  _r;                                                                                      \
})

#define CALL_taos_stmt_execute(stmt) ({                   \
  TAOS_STMT     *_stmt   = stmt;                          \
  D("taos_stmt_execute(stmt:%p) ...", _stmt);             \
  int _r = taos_stmt_execute(_stmt);                      \
  if (_r) diag_stmt(_stmt);                               \
  D("taos_stmt_execute(stmt:%p) => %d", _stmt, _r);       \
  _r;                                                     \
})

#define CALL_taos_stmt_is_insert(stmt, insert) ({                                       \
  TAOS_STMT     *_stmt   = stmt;                                                        \
  int           *_insert = insert;                                                      \
  int            _n      = 0;                                                           \
  D("taos_stmt_is_insert(stmt:%p, insert:%p) ...", _stmt, _insert);                     \
  int _r = taos_stmt_is_insert(_stmt, &_n);                                             \
  if (_r) diag_stmt(_stmt);                                                             \
  if (_insert) *_insert = _n;                                                           \
  D("taos_stmt_is_insert(stmt:%p, insert:%p(%d)) => %d", _stmt, _insert, _n, _r);       \
  _r;                                                                                   \
})

#define CALL_taos_stmt_num_params(stmt, nums) ({                                      \
  TAOS_STMT     *_stmt = stmt;                                                        \
  int           *_nums = nums;                                                        \
  int            _n    = 0;                                                           \
  D("taos_stmt_num_params(stmt:%p, nums:%p) ...", _stmt, _nums);                      \
  int _r = taos_stmt_num_params(_stmt, &_n);                                          \
  if (_r) diag_stmt(_stmt);                                                           \
  if (_nums) *_nums = _n;                                                             \
  D("taos_stmt_num_params(stmt:%p, nums:%p(%d)) => %d", _stmt, _nums, _n, _r);        \
  _r;                                                                                 \
})

#define CALL_taos_stmt_get_param(stmt, idx, type, bytes) ({                                             \
  TAOS_STMT *_stmt  = stmt;                                                                             \
  int        _idx   = idx;                                                                              \
  int       *_type  = type;                                                                             \
  int       *_bytes = bytes;                                                                            \
  int        _a = 0;                                                                                    \
  int        _b = 0;                                                                                    \
  D("taos_stmt_get_param(stmt:%p, idx:%d, type:%p, bytes:%p) ...", _stmt, _idx, _type, _bytes);         \
  int _r = taos_stmt_get_param(_stmt, _idx, &_a, &_b);                                                  \
  diag_stmt(_stmt);                                                                                     \
  if (_type)  *_type  = _a;                                                                             \
  if (_bytes) *_bytes = _b;                                                                             \
  D("taos_stmt_get_param(stmt:%p, idx:%d, type:%p(%s[%d/0x%x]), bytes:%p(%d)) => %d",                   \
    _stmt, _idx, _type, taos_data_type(_a), _a, _a, _bytes, _b, _r);                                    \
  _r;                                                                                                   \
})

#define CALL_taos_stmt_use_result(stmt) ({                      \
  TAOS_STMT *_stmt = stmt;                                      \
  D("taos_stmt_use_result(stmt:%p) ...", _stmt);                \
  TAOS_RES *_res = taos_stmt_use_result(_stmt);                 \
  diag_res(_res);                                               \
  D("taos_stmt_use_result(stmt:%p) => %p", _stmt, _res);        \
  _res;                                                         \
})

#define CALL_taos_fetch_fields(res) ({                     \
  TAOS_RES *_res = res;                                    \
  D("taos_fetch_fields(res:%p) ...", _res);                \
  TAOS_FIELD *_record = taos_fetch_fields(_res);           \
  diag_res(_res);                                          \
  D("taos_fetch_fields(res:%p) => %p", _res, _record);     \
  _record;                                                 \
})

#define CALL_taos_is_null(res, row, col) ({                                                   \
  TAOS_RES *_res = res;                                                                       \
  int32_t   _row = row;                                                                       \
  int32_t   _col = col;                                                                       \
  D("taos_is_null(res:%p, row:%d, col:%d) ...", _res, _row, _col);                            \
  bool _b = taos_is_null(_res, _row, _col);                                                   \
  diag_res(_res);                                                                             \
  D("taos_is_null(res:%p, row:%d, col:%d) => %s", _res, _row, _col, _b ? "true" : "false");   \
  _b;                                                                                         \
})

#define CALL_taos_fetch_block(res, rows) ({                                    \
  TAOS_RES *_res  = res;                                                       \
  TAOS_ROW *_rows = rows;                                                      \
  D("taos_fetch_block(res:%p, rows:%p) ...", _res, _rows);                     \
  int _n = taos_fetch_block(_res, _rows);                                      \
  diag_res(_res);                                                              \
  D("taos_fetch_block(res:%p, rows:%p) => %d", _res, _rows, _n);               \
  _n;                                                                          \
})

#define CALL_taos_result_precision(res) ({                 \
  TAOS_RES *_res = res;                                    \
  D("taos_result_precision(res:%p) ...", _res);            \
  int _r = taos_result_precision(_res);                    \
  diag_res(_res);                                          \
  D("taos_result_precision(res:%p) => %d", _res, _r);      \
  _r;                                                      \
})

#define CALL_taos_field_count(res) ({                    \
  TAOS_RES *_res = res;                                  \
  D("taos_field_count(res:%p) ...", _res);               \
  int _n = taos_field_count(_res);                       \
  diag_res(_res);                                        \
  D("taos_field_count(res:%p) => %d", _res, _n);         \
  _n;                                                    \
})

#define CALL_taos_num_fields(res) ({                     \
  TAOS_RES *_res = res;                                  \
  D("taos_num_fields(res:%p) ...", _res);                \
  int _n = taos_num_fields(_res);                        \
  diag_res(_res);                                        \
  D("taos_num_fields(res:%p) => %d", _res, _n);          \
  _n;                                                    \
})

#define CALL_taos_affected_rows(res) ({                  \
  TAOS_RES *_res = res;                                  \
  D("taos_affected_rows(res:%p) ...", _res);             \
  int _n = taos_affected_rows(_res);                     \
  diag_res(_res);                                        \
  D("taos_affected_rows(res:%p) => %d", _res, _n);       \
  _n;                                                    \
})

#define CALL_taos_fetch_lengths(res) ({                   \
  TAOS_RES *_res = res;                                   \
  D("taos_fetch_lengths(res:%p) ...", _res);              \
  int *_lengths = taos_fetch_lengths(_res);               \
  diag_res(_res);                                         \
  D("taos_fetch_lengths(res:%p) => %p", _res, _lengths);  \
  _lengths;                                               \
})

#define CALL_taos_fetch_row(res) ({                     \
  TAOS_RES *_res = res;                                 \
  D("taos_fetch_row(res:%p) ...", _res);                \
  TAOS_ROW _row = taos_fetch_row(_res);                 \
  diag_res(_res);                                       \
  D("taos_fetch_row(res:%p) => %p", _res, _row);        \
  _row;                                                 \
})

#define CALL_taos_get_column_data_offset(res, col) ({                              \
  TAOS_RES *_res = res;                                                            \
  int       _col = col;                                                            \
  D("taos_get_column_data_offset(res:%p, col:%d) ...", _res, _col);                \
  int *_offsets = taos_get_column_data_offset(_res, _col);                         \
  diag_res(_res);                                                                  \
  D("taos_get_column_data_offset(res:%p, col:%d) => %p", _res, _col, _offsets);    \
  _offsets;                                                                        \
})

#define CALL_taos_stmt_add_batch(stmt) ({                      \
  TAOS_STMT *_stmt = stmt;                                     \
  D("taos_stmt_add_batch(stmt:%p) ...", _stmt);                \
  int _r = taos_stmt_add_batch(_stmt);                         \
  diag_stmt(_stmt);                                            \
  D("taos_stmt_add_batch(stmt:%p) => %d", _stmt, _r);          \
  _r;                                                          \
})

#define CALL_taos_stmt_bind_single_param_batch(stmt, mb, col) ({                                             \
  TAOS_STMT        *_stmt = stmt;                                                                            \
  TAOS_MULTI_BIND  *_mb   = mb;                                                                              \
  int               _col  = col;                                                                             \
  D("taos_stmt_bind_single_param_batch(stmt:%p, mb:%p, col:%d) ...", _stmt, _mb, _col);                      \
  int _r = taos_stmt_bind_single_param_batch(_stmt, _mb, _col);                                              \
  diag_stmt(_stmt);                                                                                          \
  D("taos_stmt_bind_single_param_batch(stmt:%p, mb:%p, col:%d) => %d", _stmt, _mb, _col, _r);                \
  _r;                                                                                                        \
})

#define CALL_taos_stmt_get_tag_fields(stmt, fieldNum, fields) ({                                                       \
  TAOS_STMT        *_stmt     = stmt;                                                                                  \
  int              *_fieldNum = fieldNum;                                                                              \
  TAOS_FIELD_E    **_fields   = fields;                                                                                \
  int a = 0;                                                                                                           \
  TAOS_FIELD_E *b = NULL;                                                                                              \
  D("taos_stmt_get_tag_fields(stmt:%p, fieldNum:%p, fields:%p) ...", _stmt, _fieldNum, _fields);                       \
  int _r = taos_stmt_get_tag_fields(_stmt, &a, &b);                                                                    \
  diag_stmt(_stmt);                                                                                                    \
  if (_fieldNum) *_fieldNum = a;                                                                                       \
  if (_fields) *_fields = b;                                                                                           \
  D("taos_stmt_get_tag_fields(stmt:%p, fieldNum:%p(%d), fields:%p(%p)) => %d", _stmt, _fieldNum, a, _fields, b, _r);   \
  _r;                                                                                                                  \
})

#define CALL_taos_stmt_get_col_fields(stmt, fieldNum, fields) ({                                                       \
  TAOS_STMT        *_stmt     = stmt;                                                                                  \
  int              *_fieldNum = fieldNum;                                                                              \
  TAOS_FIELD_E    **_fields   = fields;                                                                                \
  int a = 0;                                                                                                           \
  TAOS_FIELD_E *b = NULL;                                                                                              \
  D("taos_stmt_get_col_fields(stmt:%p, fieldNum:%p, fields:%p) ...", _stmt, _fieldNum, _fields);                       \
  int _r = taos_stmt_get_col_fields(_stmt, &a, &b);                                                                    \
  diag_stmt(_stmt);                                                                                                    \
  if (_fieldNum) *_fieldNum = a;                                                                                       \
  if (_fields) *_fields = b;                                                                                           \
  D("taos_stmt_get_col_fields(stmt:%p, fieldNum:%p(%d), fields:%p(%p)) => %d", _stmt, _fieldNum, a, _fields, b, _r);   \
  _r;                                                                                                                  \
})

#define CALL_taos_stmt_set_tags(stmt, tags) ({                             \
  TAOS_STMT        *_stmt = stmt;                                          \
  TAOS_MULTI_BIND  *_tags = tags;                                          \
  D("taos_stmt_set_tags(stmt:%p, tags:%p) ...", _stmt, _tags);             \
  int _r = taos_stmt_set_tags(_stmt, _tags);                               \
  diag_stmt(_stmt);                                                        \
  D("taos_stmt_set_tags(stmt:%p, tags:%p) => %d", _stmt, _tags, _r);       \
  _r;                                                                      \
})

#define CALL_taos_stmt_set_tbname(stmt, name) ({                                      \
  TAOS_STMT        *_stmt = stmt;                                                     \
  const char       *_name = name;                                                     \
  D("taos_stmt_set_tbname(stmt:%p, name:%p(%s)) ...", _stmt, _name, _name);           \
  int _r = taos_stmt_set_tbname(_stmt, _name);                                        \
  diag_stmt(_stmt);                                                                   \
  D("taos_stmt_set_tbname(stmt:%p, name:%p(%s)) => %d", _stmt, _name, _name, _r);     \
  _r;                                                                                 \
})

#define CALL_taos_stmt_set_sub_tbname(stmt, name) ({                                  \
  TAOS_STMT        *_stmt = stmt;                                                     \
  const char       *_name = name;                                                     \
  D("taos_stmt_set_sub_tbname(stmt:%p, name:%p(%s)) ...", _stmt, _name, _name);       \
  int _r = taos_stmt_set_sub_tbname(_stmt, _name);                                    \
  diag_stmt(_stmt);                                                                   \
  D("taos_stmt_set_sub_tbname(stmt:%p, name:%p(%s)) => %d", _stmt, _name, _name, _r); \
  _r;                                                                                 \
})

#define CALL_taos_stmt_affected_rows(stmt) ({                  \
  TAOS_STMT *_stmt = stmt;                                     \
  D("taos_stmt_affected_rows(stmt:%p) ...", _stmt);            \
  int _r = taos_stmt_affected_rows(_stmt);                     \
  diag_stmt(_stmt);                                            \
  D("taos_stmt_affected_rows(stmt:%p) => %d", _stmt, _r);      \
  _r;                                                          \
})

#define CALL_taos_stmt_affected_rows_once(stmt) ({             \
  TAOS_STMT *_stmt = stmt;                                     \
  D("taos_stmt_affected_rows_once(stmt:%p) ...", _stmt);       \
  int _r = taos_stmt_affected_rows_once(_stmt);                \
  diag_stmt(_stmt);                                            \
  D("taos_stmt_affected_rows_once(stmt:%p) => %d", _stmt, _r); \
  _r;                                                          \
})

#define CALL_taos_stmt_bind_param(stmt, mb) ({                                    \
  TAOS_STMT        *_stmt = stmt;                                                 \
  TAOS_MULTI_BIND  *_mb   = mb;                                                   \
  D("taos_stmt_bind_param(stmt:%p, mb:%p) ...", _stmt, _mb);                      \
  int _r = taos_stmt_bind_param(_stmt, _mb);                                      \
  diag_stmt(_stmt);                                                               \
  D("taos_stmt_bind_param(stmt:%p, mb:%p) => %d", _stmt, _mb, _r);                \
  _r;                                                                             \
})

#define CALL_taos_stmt_bind_param_batch(stmt, mb) ({                                    \
  TAOS_STMT        *_stmt = stmt;                                                       \
  TAOS_MULTI_BIND  *_mb   = mb;                                                         \
  D("taos_stmt_bind_param_batch(stmt:%p, mb:%p) ...", _stmt, _mb);                      \
  int _r = taos_stmt_bind_param_batch(_stmt, _mb);                                      \
  diag_stmt(_stmt);                                                                     \
  D("taos_stmt_bind_param_batch(stmt:%p, mb:%p) => %d", _stmt, _mb, _r);                \
  _r;                                                                                   \
})

#define CALL_taos_stmt_set_tbname_tags(stmt, name, tags) ({                                                \
  TAOS_STMT        *_stmt = stmt;                                                                          \
  const char       *_name = name;                                                                          \
  TAOS_MULTI_BIND  *_tags = tags;                                                                          \
  D("taos_stmt_set_tbname_tags(stmt:%p, name:%p(%s), tags:%p) ...", _stmt, _name, _name, _tags);           \
  int _r = taos_stmt_set_tbname_tags(_stmt, _name, _tags);                                                 \
  diag_stmt(_stmt);                                                                                        \
  D("taos_stmt_set_tbname_tags(stmt:%p, name:%p(%s), tags:%p) => %d", _stmt, _name, _name, _tags, _r);     \
  _r;                                                                                                      \
})

#define CALL_taos_print_row(str, row, fields, num_fields) ({                                                             \
  char             *_str        = str;                                                                                   \
  TAOS_ROW          _row        = row;                                                                                   \
  TAOS_FIELD       *_fields     = fields;                                                                                \
  int               _num_fields = num_fields;                                                                            \
  D("taos_print_row(str:%p(%s), row:%p, fields:%p, num_fields:%d) ...", _str, _str, _row, _fields, _num_fields);         \
  int _r = taos_print_row(_str, _row, _fields, _num_fields);                                                             \
  D("taos_print_row(str:%p(%s), row:%p, fields:%p, num_fields:%d) => %d", _str, _str, _row, _fields, _num_fields, _r);   \
  _r;                                                                                                                    \
})

#define CALL_taos_errno                taos_errno
#define CALL_taos_errstr               taos_errstr
#define CALL_taos_stmt_errstr          taos_stmt_errstr
#define CALL_taos_data_type            taos_data_type

#endif // _taos_helpers_h_

