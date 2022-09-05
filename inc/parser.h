#ifndef _parser_h_
#define _parser_h_

// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqldriverconnect-function?view=sql-server-ver16

#include "macros.h"

#include "conn.h"
#include "utils.h"

#include <stddef.h>

EXTERN_C_BEGIN

typedef struct parser_token_s           parser_token_t;

struct parser_token_s {
  const char      *text;
  size_t           leng;
};

typedef struct parser_param_s           parser_param_t;

struct parser_param_s {
  connection_str_t       conn_str;

  int                    row0, col0;
  int                    row1, col1;
  char                  *errmsg;

  unsigned int           debug_flex:1;
  unsigned int           debug_bison:1;
};

static inline void parser_param_release(parser_param_t *param)
{
  if (!param) return;
  connection_str_release(&param->conn_str);
  TOD_SAFE_FREE(param->errmsg);
  param->row0 = 0;
}

int parser_parse(const char *input, size_t len,
    parser_param_t *param) FA_HIDDEN;

EXTERN_C_END

#endif // _parser_h_

