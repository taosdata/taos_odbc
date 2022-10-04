#ifndef _utils_h_
#define _utils_h_

#include "macros.h"

#include <stdlib.h>

EXTERN_C_BEGIN

#define TOD_SAFE_FREE(_p) if (_p) { free(_p); _p = NULL; }

typedef struct buffer_s           buffer_t;
struct buffer_s {
  char              *buf;
  size_t             cap;
  size_t             nr;
};

#define buffer_sprintf(_buf, _fmt, ...) ({                \
  buffer_t *_buffer = _buf;                               \
  char *__buf = _buffer->buf;                             \
  size_t _nn = 0;                                         \
  if (__buf) {                                            \
    __buf += _buffer->nr;                                 \
    _nn = _buffer->cap - _buffer->nr;                     \
  }                                                       \
  int _n = snprintf(__buf, _nn, _fmt, ##__VA_ARGS__);     \
  if (__buf) {                                            \
    if (_n>0 && (size_t)_n < _nn) {                       \
      _buffer->nr += _n;                                  \
    } else {                                              \
      _buffer->nr += _nn - 1;                             \
    }                                                     \
  }                                                       \
  _n;                                                     \
})


EXTERN_C_END

#endif // _utils_h_

