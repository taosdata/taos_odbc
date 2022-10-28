#ifndef _errs_h_
#define _errs_h_

#include "macros.h"

#include <sqlext.h>

EXTERN_C_BEGIN

typedef struct errs_s            errs_t;

void errs_init(errs_t *errs) FA_HIDDEN;
void errs_append_x(errs_t *errs, const char *file, int line, const char *func, const char *data_source, const char *sql_state, int e, const char *estr) FA_HIDDEN;
void errs_clr_x(errs_t *errs) FA_HIDDEN;
void errs_release_x(errs_t *errs) FA_HIDDEN;
SQLRETURN errs_get_diag_rec_x(
    errs_t         *errs,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr) FA_HIDDEN;

#define errs_append(_errs, _data_source, _sql_state, _e, _estr)                              \
  ({                                                                                         \
    errs_append_x(_errs, __FILE__, __LINE__, __func__, _data_source, _sql_state, _e, _estr); \
  })

#define errs_append_format(_errs, _data_source, _sql_state, _e, _fmt, ...)    \
  ({                                                                          \
    char _buf[1024];                                                          \
    snprintf(_buf, sizeof(_buf), "" _fmt "", ##__VA_ARGS__);                  \
    errs_append(_errs, _data_source, _sql_state, _e, _buf);                   \
  })

#define errs_oom(_errs, _data_source) errs_append(_errs, _data_source, "HY001", 0, "Memory allocation error")

#define errs_clr(_errs) errs_clr_x(_errs)

#define errs_release(_errs) errs_release_x(_errs)

#define errs_get_diag_rec(_errs, _RecNumber, _SQLSTATE, _NativeErrorPtr, _MessageText, _BufferLength, _TextLengthPtr) \
  errs_get_diag_rec_x(_errs, _RecNumber, _SQLSTATE, _NativeErrorPtr, _MessageText, _BufferLength, _TextLengthPtr)

EXTERN_C_END

#endif // _errs_h_

