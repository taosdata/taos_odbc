#include "internal.h"

#include <string.h>

#include <sqlext.h>

static void err_set_x(err_t *err, const char *file, int line, const char *func, const char *data_source, const char *sql_state, int e, const char *estr)
{
  const char *vendor = "freemine@yeah.net";
  const char *odbc_component = "TAOS ODBC Driver";
  err->err = e;
  snprintf(err->buf, sizeof(err->buf),
      "[%s][%s][%s]%s[%d]:%s(): %s",
      vendor, odbc_component, data_source,
      basename((char*)file), line, func,
      estr);
  err->estr = err->buf;
  strncpy((char*)err->sql_state, sql_state, sizeof(err->sql_state));
}

void errs_append_x(errs_t *errs, const char *file, int line, const char *func, const char *data_source, const char *sql_state, int e, const char *estr)
{
  if (!errs->free) {
    err_t *err = (err_t*)calloc(1, sizeof(*err));
    if (!err) {
      OD("out of memory");
      return;
    }

    err->err           = 0;
    err->estr          = "";
    err->sql_state[0]  = '\0';
    err->buf[0]        = '\0';

    err->next = errs->free;
    errs->free = err;
  }

  err_t *err = errs->free;
  errs->free = err->next;
  err->next = NULL;

  err_set_x(err, file, line, func, data_source, sql_state, e, estr);

  err_t *tail = errs->head;
  if (tail) {
    while (tail->next) tail = tail->next;
    tail->next = err;
  } else {
    errs->head = err;
  }
}

void errs_clr_x(errs_t *errs)
{
  if (!errs->head) return;

  err_t *tail = errs->head;
  while (tail->next) tail = tail->next;
  tail->next = errs->free;
  errs->free = errs->head;
  errs->head = NULL;
}

void errs_release_x(errs_t *errs)
{
  err_t *tail;

  tail = errs->head;
  while (tail) {
    err_t *p = tail->next;
    free(tail);
    tail = p;
  }
  errs->head = NULL;

  tail = errs->free;
  while (tail) {
    err_t *p = tail->next;
    free(tail);
    tail = p;
  }
  errs->free = NULL;
}

SQLRETURN errs_get_diag_rec_x(
    errs_t         *errs,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr)
{
  if (RecNumber == 0) return SQL_NO_DATA;

  err_t *head = errs->head;

  int i = 1;
  while (i<RecNumber && head) {
    ++i;
    head = head->next;
  }

  if (head == NULL) return SQL_NO_DATA;

  if (NativeErrorPtr) *NativeErrorPtr = head->err;
  if (SQLState) strncpy((char*)SQLState, (const char*)head->sql_state, 6);
  int n = snprintf((char*)MessageText, BufferLength, "%s", head->estr);
  if (TextLengthPtr) *TextLengthPtr = n;

  return SQL_SUCCESS;
}


