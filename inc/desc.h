#ifndef _desc_h_
#define _desc_h_

#include "conn.h"

EXTERN_C_BEGIN

typedef struct descriptor_s        descriptor_t;
typedef struct desc_s              desc_t;

desc_t* desc_create(conn_t *conn) FA_HIDDEN;
desc_t* desc_ref(desc_t *desc) FA_HIDDEN;
desc_t* desc_unref(desc_t *desc) FA_HIDDEN;

SQLRETURN desc_free(desc_t *desc) FA_HIDDEN;

void descriptor_init(descriptor_t *descriptor) FA_HIDDEN;
void descriptor_release(descriptor_t *descriptor) FA_HIDDEN;

EXTERN_C_END

#endif //  _desc_h_

