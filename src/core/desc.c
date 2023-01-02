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

#include "internal.h"

#include "desc.h"
// make sure `log.h` is included ahead of `taos_helpers.h`, for the `LOG_IMPL` issue
#include "log.h"
// #include "taos_helpers.h"

static void _desc_init(desc_t *desc, conn_t *conn)
{
  desc->conn = conn_ref(conn);
  int prev = atomic_fetch_add(&conn->descs, 1);
  OA_ILE(prev >= 0);

  INIT_TOD_LIST_HEAD(&desc->associated_stmts_as_ARD);
  INIT_TOD_LIST_HEAD(&desc->associated_stmts_as_APD);

  desc->refc = 1;
}

static void _desc_release(desc_t *desc)
{
  int prev = atomic_fetch_sub(&desc->conn->descs, 1);
  OA_ILE(prev >= 1);

  descriptor_release(&desc->descriptor);

  conn_unref(desc->conn);
  desc->conn = NULL;

  errs_release(&desc->errs);

  return;
}

desc_t* desc_create(conn_t *conn)
{
  desc_t *desc = (desc_t*)calloc(1, sizeof(*desc));
  if (!desc) {
    conn_oom(conn);
    return NULL;
  }

  _desc_init(desc, conn);

  return desc;
}

desc_t* desc_ref(desc_t *desc)
{
  OA_ILE(desc);
  int prev = atomic_fetch_add(&desc->refc, 1);
  OA_ILE(prev>0);
  return desc;
}

desc_t* desc_unref(desc_t *desc)
{
  int prev = atomic_fetch_sub(&desc->refc, 1);
  if (prev>1) return desc;
  OA_ILE(prev==1);

  _desc_release(desc);
  free(desc);

  return NULL;
}

SQLRETURN desc_free(desc_t *desc)
{
  stmt_t *p, *n;
  tod_list_for_each_entry_safe(p, n, &desc->associated_stmts_as_ARD, stmt_t, associated_ARD_node) {
    stmt_dissociate_ARD(p);
  }

  tod_list_for_each_entry_safe(p, n, &desc->associated_stmts_as_APD, stmt_t, associated_APD_node) {
    stmt_dissociate_APD(p);
  }

  desc_unref(desc);

  return SQL_SUCCESS;
}

static void _desc_header_init(desc_header_t *header)
{
  header->DESC_ARRAY_SIZE                = 1;
  header->DESC_ARRAY_STATUS_PTR          = NULL;
  header->DESC_BIND_OFFSET_PTR           = NULL;
  header->DESC_BIND_TYPE                 = SQL_BIND_BY_COLUMN;
  header->DESC_ROWS_PROCESSED_PTR        = NULL;

  header->DESC_COUNT                     = 0;
}

void descriptor_release(descriptor_t *desc)
{
  TOD_SAFE_FREE(desc->records);
}

void descriptor_init(descriptor_t *desc)
{
  _desc_header_init(&desc->header);
}

void descriptor_reclaim_buffers(descriptor_t *APD)
{
  desc_header_t *APD_header = &APD->header;

  for (SQLUSMALLINT i=0; i<APD_header->DESC_COUNT; ++i) {
    desc_record_t *record = APD->records + i;
    record->bound = 0;
  }
}

void desc_clr_errs(desc_t *desc)
{
  errs_clr(&desc->errs);
}

SQLRETURN desc_get_diag_rec(
    desc_t         *desc,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr)
{
  return errs_get_diag_rec(&desc->errs, RecNumber, SQLState, NativeErrorPtr, MessageText, BufferLength, TextLengthPtr);
}

SQLRETURN desc_get_diag_field(
    desc_t         *desc,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  switch (DiagIdentifier) {
    case SQL_DIAG_CLASS_ORIGIN:
      return errs_get_diag_field_class_origin(&desc->errs, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_DIAG_SUBCLASS_ORIGIN:
      return errs_get_diag_field_subclass_origin(&desc->errs, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_DIAG_SQLSTATE:
      return errs_get_diag_field_sqlstate(&desc->errs, RecNumber, DiagIdentifier, DiagInfoPtr, BufferLength, StringLengthPtr);
    case SQL_DIAG_CONNECTION_NAME:
      // TODO:
      *(char*)DiagInfoPtr = '\0';
      if (StringLengthPtr) *StringLengthPtr = 0;
      return SQL_SUCCESS;
    case SQL_DIAG_SERVER_NAME:
      // TODO:
      *(char*)DiagInfoPtr = '\0';
      if (StringLengthPtr) *StringLengthPtr = 0;
      return SQL_SUCCESS;
    default:
      desc_append_err_format(desc, "HY000", 0, "General error:RecNumber:[%d]; DiagIdentifier:[%d]%s", RecNumber, DiagIdentifier, sql_diag_identifier(DiagIdentifier));
      return SQL_ERROR;
  }
}

#if (ODBCVER >= 0x0300)       /* { */
SQLRETURN desc_copy(
    desc_t *src,
    desc_t *tgt)
{
  (void)src;
  desc_append_err(tgt, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

SQLRETURN desc_get_field(
    desc_t         *desc,
    SQLSMALLINT RecNumber, SQLSMALLINT FieldIdentifier,
    SQLPOINTER Value, SQLINTEGER BufferLength,
    SQLINTEGER *StringLength)
{
  (void)RecNumber;
  (void)FieldIdentifier;
  (void)Value;
  (void)BufferLength;
  (void)StringLength;
  desc_append_err(desc, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

SQLRETURN desc_get_rec(
    desc_t         *desc,
    SQLSMALLINT RecNumber, SQLCHAR *Name,
    SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr,
    SQLSMALLINT *TypePtr, SQLSMALLINT *SubTypePtr,
    SQLLEN     *LengthPtr, SQLSMALLINT *PrecisionPtr,
    SQLSMALLINT *ScalePtr, SQLSMALLINT *NullablePtr)
{
  (void)RecNumber;
  (void)Name;
  (void)BufferLength;
  (void)StringLengthPtr;
  (void)TypePtr;
  (void)SubTypePtr;
  (void)LengthPtr;
  (void)PrecisionPtr;
  (void)ScalePtr;
  (void)NullablePtr;
  desc_append_err(desc, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

SQLRETURN desc_set_field(
    desc_t         *desc,
    SQLSMALLINT RecNumber, SQLSMALLINT FieldIdentifier,
    SQLPOINTER Value, SQLINTEGER BufferLength)
{
  (void)RecNumber;
  (void)FieldIdentifier;
  (void)Value;
  (void)BufferLength;
  desc_append_err(desc, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

SQLRETURN desc_set_rec(
    desc_t         *desc,
    SQLSMALLINT RecNumber, SQLSMALLINT Type,
    SQLSMALLINT SubType, SQLLEN Length,
    SQLSMALLINT Precision, SQLSMALLINT Scale,
    SQLPOINTER Data, SQLLEN *StringLength,
    SQLLEN *Indicator)
{
  (void)RecNumber;
  (void)Type;
  (void)SubType;
  (void)Length;
  (void)Precision;
  (void)Scale;
  (void)Data;
  (void)StringLength;
  (void)Indicator;
  desc_append_err(desc, "HY000", 0, "General error:not implemented yet");
  return SQL_ERROR;
}

#endif                        /* }*/

