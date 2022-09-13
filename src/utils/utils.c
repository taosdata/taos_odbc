#include "../internal.h"

#include <string.h>

static_pool_t* static_pool_create(size_t cap)
{
  size_t sz = sizeof(static_pool_t) + cap;
  static_pool_t *pool = (static_pool_t*)malloc(sz);
  if (!pool) return NULL;
  pool->cap  = cap;
  pool->nr   = 0;
  return pool;
}

void static_pool_destroy(static_pool_t *pool)
{
  if (pool) free(pool);
}

unsigned char* static_pool_malloc(static_pool_t *pool, size_t sz)
{
  if (pool->nr + sz > pool->cap) return NULL;

  unsigned char *p = (unsigned char*)pool + sizeof(*pool) + pool->nr;
  pool->nr += sz;

  return p;
}

unsigned char* static_pool_calloc(static_pool_t *pool, size_t sz)
{
  unsigned char *p = static_pool_malloc(pool, sz);
  if (p) memset(p, 0, sz);

  return p;
}

unsigned char* static_pool_malloc_align(static_pool_t *pool, size_t sz, size_t align)
{
  if (pool->nr + sz > pool->cap) return NULL;

  size_t pos = pool->nr;
  if (align > 1) pos = (pos + align - 1) / align * align;

  if (pos + sz > pool->cap) return NULL;

  return static_pool_malloc(pool, pos + sz - pool->nr);
}

unsigned char* static_pool_calloc_align(static_pool_t *pool, size_t sz, size_t align)
{
  unsigned char *p = static_pool_malloc_align(pool, sz, align);
  if (p) memset(p, 0, sz);

  return p;
}

