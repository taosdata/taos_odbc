/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
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

%code top {
}

%code top {
    // here to include header files required for generated code
}

%code requires {
    #define YYSTYPE       PARSER_YYSTYPE
    #define YYLTYPE       PARSER_YYLTYPE
    #ifndef YY_TYPEDEF_YY_SCANNER_T
    #define YY_TYPEDEF_YY_SCANNER_T
    typedef void* yyscan_t;
    #endif
}

%code provides {
}

%code {
    // generated header from flex
    // introduce yylex decl for later use
    static void _yyerror_impl(
        YYLTYPE *yylloc,                   // match %define locations
        yyscan_t arg,                      // match %param
        parser_param_t *param,             // match %parse-param
        const char *title,
        const char *errmsg
    );
    static void yyerror(
        YYLTYPE *yylloc,                   // match %define locations
        yyscan_t arg,                      // match %param
        parser_param_t *param,             // match %parse-param
        const char *errsg
    );

    #define SET_DSN(_v, _loc) do {                                                              \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_str.dsn);                                                       \
      param->conn_str.dsn = strndup(_v.text, _v.leng);                                          \
      if (!param->conn_str.dsn) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error", "out of memory");                     \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_UID(_v, _loc) do {                                                              \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_str.uid);                                                       \
      param->conn_str.uid = strndup(_v.text, _v.leng);                                          \
      if (!param->conn_str.uid) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error", "out of memory");                     \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_DB(_v, _loc) do {                                                               \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_str.db);                                                        \
      param->conn_str.db = strndup(_v.text, _v.leng);                                           \
      if (!param->conn_str.db) {                                                                \
        _yyerror_impl(&_loc, arg, param, "runtime error", "out of memory");                     \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_PWD(_v, _loc) do {                                                              \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_str.pwd);                                                       \
      param->conn_str.pwd = strndup(_v.text, _v.leng);                                          \
      if (!param->conn_str.pwd) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error", "out of memory");                     \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_DRIVER(_v, _loc) do {                                                           \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_str.driver);                                                    \
      param->conn_str.driver = strndup(_v.text, _v.leng);                                       \
      if (!param->conn_str.driver) {                                                            \
        _yyerror_impl(&_loc, arg, param, "runtime error", "out of memory");                     \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_FQDN(_v, _loc) do {                                                             \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_str.ip);                                                        \
      param->conn_str.ip = strndup(_v.text, _v.leng);                                           \
      if (!param->conn_str.ip) {                                                                \
        _yyerror_impl(&_loc, arg, param, "runtime error", "out of memory");                     \
        return -1;                                                                              \
      }                                                                                         \
      param->conn_str.port = 0;                                                                 \
      param->conn_str.port_set = 0;                                                             \
    } while (0)
    #define SET_FQDN_PORT(_v, _p, _loc) do {                                                    \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_str.ip);                                                        \
      param->conn_str.ip = strndup(_v.text, _v.leng);                                           \
      if (!param->conn_str.ip) {                                                                \
        _yyerror_impl(&_loc, arg, param, "runtime error", "out of memory");                     \
        return -1;                                                                              \
      }                                                                                         \
      param->conn_str.port = strtol(_p.text, NULL, 10);                                         \
      param->conn_str.port_set = 1;                                                             \
    } while (0)
    #define SET_UNSIGNED_PROMOTION(_s, _n, _loc) do {                                           \
      if (!param) break;                                                                        \
      OA_NIY(_s[_n] == '\0');                                                                   \
      param->conn_str.unsigned_promotion = !!(atoi(_s));                                        \
      param->conn_str.unsigned_promotion_set = 1;                                               \
    } while (0)
    #define SET_TIMESTAMP_AS_IS(_s, _n, _loc) do {                                              \
      if (!param) break;                                                                        \
      OA_NIY(_s[_n] == '\0');                                                                   \
      param->conn_str.timestamp_as_is = !!(atoi(_s));                                           \
      param->conn_str.timestamp_as_is_set = 1;                                                  \
    } while (0)
    #define SET_CACHE_SQL(_s, _n, _loc) do {                                                    \
      if (!param) break;                                                                        \
      OA_NIY(_s[_n] == '\0');                                                                   \
      param->conn_str.cache_sql = !!(atoi(_s));                                                 \
      param->conn_str.cache_sql_set = 1;                                                        \
    } while (0)

    void parser_param_release(parser_param_t *param)
    {
      if (!param) return;
      connection_cfg_release(&param->conn_str);
      param->err_msg[0] = '\0';
      param->row0 = 0;
    }
}

/* Bison declarations. */
%require "3.0.4"
%define api.pure full
%define api.token.prefix {TOK_}
%define locations
%define parse.error verbose
%define parse.lac full
%define parse.trace true
%defines
%verbose

%param { yyscan_t arg }
%parse-param { parser_param_t *param }

// union members
%union { parser_token_t token; }
%union { char c; }

%token DSN UID PWD DRIVER SERVER UNSIGNED_PROMOTION TIMESTAMP_AS_IS CACHE_SQL DB
%token <token> ID VALUE FQDN DIGITS

 /* %nterm <str>   args */ // non-terminal `input` use `str` to store
                           // token value as well
 /* %destructor { free($$); } <str> */

%% /* The grammar follows. */

input:
  %empty
| connect_str
;

connect_str:
  attribute
| connect_str ';'
| connect_str ';' attribute
;

attribute:
  DSN
| DSN '='
| DSN '=' VALUE                   { SET_DSN($3, @$); }
| UID
| UID '='
| UID '=' VALUE                   { SET_UID($3, @$); }
| DB
| DB '='
| DB '=' VALUE                    { SET_DB($3, @$); }
| PWD
| PWD '='
| PWD '=' VALUE                   { SET_PWD($3, @$); }
| DRIVER '=' VALUE                { SET_DRIVER($3, @$); }
| DRIVER '=' '{' VALUE '}'        { SET_DRIVER($4, @$); }
| ID
| ID '='
| ID '=' VALUE                    { ; }
| SERVER
| SERVER '='
| SERVER '=' FQDN                 { SET_FQDN($3, @$); }
| SERVER '=' FQDN ':'             { SET_FQDN($3, @$); }
| SERVER '=' FQDN ':' DIGITS      { SET_FQDN_PORT($3, $5, @$); }
| UNSIGNED_PROMOTION              { SET_UNSIGNED_PROMOTION("1", 1, @$); }
| UNSIGNED_PROMOTION '='          { SET_UNSIGNED_PROMOTION("0", 1, @$); }
| UNSIGNED_PROMOTION '=' DIGITS   { SET_UNSIGNED_PROMOTION($3.text, $3.leng, @$); }
| TIMESTAMP_AS_IS                 { SET_TIMESTAMP_AS_IS("1", 1, @$); }
| TIMESTAMP_AS_IS '='             { SET_TIMESTAMP_AS_IS("0", 1, @$); }
| TIMESTAMP_AS_IS '=' DIGITS      { SET_TIMESTAMP_AS_IS($3.text, $3.leng, @$); }
| CACHE_SQL                       { SET_CACHE_SQL("1", 1, @$); }
| CACHE_SQL '='                   { SET_CACHE_SQL("0", 1, @$); }
| CACHE_SQL '=' DIGITS            { SET_CACHE_SQL($3.text, $3.leng, @$); }
;

%%

static void _yyerror_impl(
    YYLTYPE *yylloc,                   // match %define locations
    yyscan_t arg,                      // match %param
    parser_param_t *param,             // match %parse-param
    const char *title,
    const char *errmsg
)
{
  // to implement it here
  (void)yylloc;
  (void)arg;
  (void)param;
  (void)errmsg;

  if (!param) {
    fprintf(stderr, "(%d,%d)->(%d,%d):%s:%s\n",
        yylloc->first_line, yylloc->first_column,
        yylloc->last_line, yylloc->last_column - 1,
        title,
        errmsg);

    return;
  }

  param->row0 = yylloc->first_line;
  param->col0 = yylloc->first_column;
  param->row1 = yylloc->last_line;
  param->col1 = yylloc->last_column;
  param->err_msg[0] = '\0';
  snprintf(param->err_msg, sizeof(param->err_msg), "%s:%s", title, errmsg);
}

/* Called by yyparse on error. */
static void yyerror(
    YYLTYPE *yylloc,                   // match %define locations
    yyscan_t arg,                      // match %param
    parser_param_t *param,             // match %parse-param
    const char *errmsg
)
{
  _yyerror_impl(yylloc, arg, param, "bad syntax for connection string", errmsg);
}

int parser_parse(const char *input, size_t len, parser_param_t *param)
{
  yyscan_t arg = {0};
  yylex_init(&arg);
  // yyset_in(in, arg);
  int debug_flex = param ? param->debug_flex : 0;
  int debug_bison = param ? param->debug_bison: 0;
  yyset_debug(debug_flex, arg);
  yydebug = debug_bison;
  // yyset_extra(param, arg);
  yy_scan_bytes(input ? input : "", input ? (int)len : 0, arg);
  int ret =yyparse(arg, param);
  yylex_destroy(arg);
  return ret ? -1 : 0;
}

