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

#ifndef _conn_h_
#define _conn_h_

#include "env.h"
#include "utils.h"

#include <string.h>

EXTERN_C_BEGIN

typedef struct connection_cfg_s    connection_cfg_t;

struct connection_cfg_s {
  char                  *driver;
  char                  *dsn;
  char                  *uid;
  char                  *pwd;
  char                  *ip;
  char                  *db;
  int                    port;

  // NOTE: 1.this is to hack node.odbc, which maps SQL_TINYINT to SQL_C_UTINYINT
  //       2.node.odbc does not call SQLGetInfo/SQLColAttribute to get signess of integers
  unsigned int           unsigned_promotion:1;
  unsigned int           cache_sql:1;
};

static inline void connection_cfg_release(connection_cfg_t *conn_str)
{
  if (!conn_str) return;

  TOD_SAFE_FREE(conn_str->driver);
  TOD_SAFE_FREE(conn_str->dsn);
  TOD_SAFE_FREE(conn_str->uid);
  TOD_SAFE_FREE(conn_str->pwd);
  TOD_SAFE_FREE(conn_str->ip);
  TOD_SAFE_FREE(conn_str->db);
}

static inline void connection_cfg_transfer(connection_cfg_t *from, connection_cfg_t *to)
{
  if (from == to) return;
  connection_cfg_release(to);
  *to = *from;
  memset(from, 0, sizeof(*from));
}

typedef struct conn_s              conn_t;
typedef struct desc_s              desc_t;

conn_t* conn_create(env_t *env) FA_HIDDEN;
conn_t* conn_ref(conn_t *conn) FA_HIDDEN;
conn_t* conn_unref(conn_t *conn) FA_HIDDEN;
SQLRETURN conn_free(conn_t *conn) FA_HIDDEN;
logger_t* conn_get_logger(conn_t *conn) FA_HIDDEN;
void conn_clr_errs(conn_t *conn) FA_HIDDEN;

SQLRETURN conn_driver_connect(
    conn_t         *conn,
    SQLHWND         WindowHandle,
    SQLCHAR        *InConnectionString,
    SQLSMALLINT     StringLength1,
    SQLCHAR        *OutConnectionString,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLength2Ptr,
    SQLUSMALLINT    DriverCompletion) FA_HIDDEN;

void conn_disconnect(conn_t *conn) FA_HIDDEN;

SQLRETURN conn_get_diag_rec(
    conn_t         *conn,
    SQLSMALLINT     RecNumber,
    SQLCHAR        *SQLState,
    SQLINTEGER     *NativeErrorPtr,
    SQLCHAR        *MessageText,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *TextLengthPtr) FA_HIDDEN;

SQLRETURN conn_alloc_stmt(conn_t *conn, SQLHANDLE *OutputHandle) FA_HIDDEN;

SQLRETURN conn_alloc_desc(conn_t *conn, SQLHANDLE *OutputHandle) FA_HIDDEN;

SQLRETURN conn_connect(
    conn_t        *conn,
    SQLCHAR       *ServerName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *UserName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *Authentication,
    SQLSMALLINT    NameLength3) FA_HIDDEN;

SQLRETURN conn_get_info(
    conn_t         *conn,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr) FA_HIDDEN;

SQLRETURN conn_end_tran(
    conn_t       *conn,
    SQLSMALLINT   CompletionType) FA_HIDDEN;

SQLRETURN conn_set_attr(
    conn_t       *conn,
    SQLINTEGER    Attribute,
    SQLPOINTER    ValuePtr,
    SQLINTEGER    StringLength) FA_HIDDEN;

#if (ODBCVER >= 0x0300)           /* { */
SQLRETURN conn_get_attr(
    conn_t       *conn,
    SQLINTEGER    Attribute,
    SQLPOINTER    Value,
    SQLINTEGER    BufferLength,
    SQLINTEGER   *StringLengthPtr) FA_HIDDEN;
#endif                            /* } */

SQLRETURN conn_get_diag_field(
    conn_t         *conn,
    SQLSMALLINT     RecNumber,
    SQLSMALLINT     DiagIdentifier,
    SQLPOINTER      DiagInfoPtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr) FA_HIDDEN;

SQLRETURN conn_native_sql(
    conn_t        *conn,
    SQLCHAR       *InStatementText,
    SQLINTEGER     TextLength1,
    SQLCHAR       *OutStatementText,
    SQLINTEGER     BufferLength,
    SQLINTEGER    *TextLength2Ptr) FA_HIDDEN;

SQLRETURN conn_browse_connect(
    conn_t        *conn,
    SQLCHAR       *InConnectionString,
    SQLSMALLINT    StringLength1,
    SQLCHAR       *OutConnectionString,
    SQLSMALLINT    BufferLength,
    SQLSMALLINT   *StringLength2Ptr) FA_HIDDEN;

SQLRETURN conn_complete_async(
    conn_t      *conn,
    RETCODE     *AsyncRetCodePtr) FA_HIDDEN;

EXTERN_C_END

#endif //  _conn_h_
