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
  tod_list_for_each_entry_safe(p, n, &desc->associated_stmts_as_ARD, associated_ARD_node) {
    stmt_dissociate_ARD(p);
  }

  tod_list_for_each_entry_safe(p, n, &desc->associated_stmts_as_APD, associated_APD_node) {
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
    desc_record_reclaim_buffers(record);
    record->bound = 0;
  }
}

void desc_record_reclaim_buffers(desc_record_t *record)
{
  buf_release(&record->data_buffer);
  buf_release(&record->len_buffer);
  buf_release(&record->ind_buffer);
}

