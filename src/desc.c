#include "internal.h"

#include "desc.h"

static int desc_init(desc_t *desc, conn_t *conn)
{
  desc->conn = conn_ref(conn);
  int prev = atomic_fetch_add(&conn->descs, 1);
  OA_ILE(prev >= 0);

  INIT_TOD_LIST_HEAD(&desc->associated_stmts_as_ARD);
  INIT_TOD_LIST_HEAD(&desc->associated_stmts_as_APD);

  desc->refc = 1;

  return 0;
}

static void desc_release(desc_t *desc)
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
  OA_ILE(conn);

  desc_t *desc = (desc_t*)calloc(1, sizeof(*desc));
  if (!desc) return NULL;

  int r = desc_init(desc, conn);
  if (r) {
    desc_release(desc);
    return NULL;
  }

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
  OA_ILE(desc);
  int prev = atomic_fetch_sub(&desc->refc, 1);
  if (prev>1) return desc;
  OA_ILE(prev==1);

  desc_release(desc);
  free(desc);

  return NULL;
}

SQLRETURN desc_free(desc_t *desc)
{
  OA_ILE(desc);
  OA_ILE(desc->conn);

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

static void desc_header_init(desc_header_t *header)
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
  desc_header_init(&desc->header);
}

