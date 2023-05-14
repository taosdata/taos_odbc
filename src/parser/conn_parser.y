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
    #define YYSTYPE       CONN_PARSER_YYSTYPE
    #define YYLTYPE       CONN_PARSER_YYLTYPE
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
        conn_parser_param_t *param,        // match %parse-param
        const char *errmsg
    );
    static void yyerror(
        YYLTYPE *yylloc,                   // match %define locations
        yyscan_t arg,                      // match %param
        conn_parser_param_t *param,        // match %parse-param
        const char *errsg
    );

    #define SET_DSN(_v, _loc) do {                                                              \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg->dsn);                                                      \
      param->conn_cfg->dsn = strndup(_v.text, _v.leng);                                         \
      if (!param->conn_cfg->dsn) {                                                              \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
      if (!param->init) break;                                                                  \
      char buf[1024]; buf[0] = '\0';                                                            \
      if (param->init(param->conn_cfg, buf, sizeof(buf))) {                                     \
        _yyerror_impl(&_loc, arg, param, buf);                                                  \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_UID(_v, _loc) do {                                                              \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg->uid);                                                      \
      param->conn_cfg->uid = strndup(_v.text, _v.leng);                                         \
      if (!param->conn_cfg->uid) {                                                              \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_DB(_v, _loc) do {                                                               \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg->db);                                                       \
      param->conn_cfg->db = strndup(_v.text, _v.leng);                                          \
      if (!param->conn_cfg->db) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_PWD(_v, _loc) do {                                                              \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg->pwd);                                                      \
      param->conn_cfg->pwd = strndup(_v.text, _v.leng);                                         \
      if (!param->conn_cfg->pwd) {                                                              \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_DRIVER(_v, _loc) do {                                                           \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg->driver);                                                   \
      param->conn_cfg->driver = strndup(_v.text, _v.leng);                                      \
      if (!param->conn_cfg->driver) {                                                           \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_FQDN(_v, _loc) do {                                                             \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg->ip);                                                       \
      param->conn_cfg->ip = strndup(_v.text, _v.leng);                                          \
      if (!param->conn_cfg->ip) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
      param->conn_cfg->port = 0;                                                                \
    } while (0)
    #define SET_FQDN_PORT(_v, _p, _loc) do {                                                    \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg->ip);                                                       \
      param->conn_cfg->ip = strndup(_v.text, _v.leng);                                          \
      if (!param->conn_cfg->ip) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
      param->conn_cfg->port = strtol(_p.text, NULL, 10);                                        \
    } while (0)
    #define SET_DATABASE(_v, _loc) do {                                                         \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg->db);                                                       \
      param->conn_cfg->db = strndup(_v.text, _v.leng);                                          \
      if (!param->conn_cfg->db) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_CHARSET_FOR_COL_BIND(_v, _loc) do {                                             \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg->charset_for_col_bind);                                     \
      param->conn_cfg->charset_for_col_bind = strndup(_v.text, _v.leng);                        \
      if (!param->conn_cfg->charset_for_col_bind) {                                             \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_CHARSET_FOR_PARAM_BIND(_v, _loc) do {                                           \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg->charset_for_param_bind);                                   \
      param->conn_cfg->charset_for_param_bind = strndup(_v.text, _v.leng);                      \
      if (!param->conn_cfg->charset_for_param_bind) {                                           \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_CHARSET(_v, _loc) do {                                                          \
      SET_CHARSET_FOR_COL_BIND(_v, _loc);                                                       \
      SET_CHARSET_FOR_PARAM_BIND(_v, _loc);                                                     \
    } while (0)
    #define SET_UNSIGNED_PROMOTION(_s, _n, _loc) do {                                           \
      if (!param) break;                                                                        \
      OA_NIY(_s[_n] == '\0');                                                                   \
      param->conn_cfg->unsigned_promotion = !!(atoi(_s));                                       \
    } while (0)
    #define SET_TIMESTAMP_AS_IS(_s, _n, _loc) do {                                              \
      if (!param) break;                                                                        \
      OA_NIY(_s[_n] == '\0');                                                                   \
      param->conn_cfg->timestamp_as_is = !!(atoi(_s));                                          \
    } while (0)

    void conn_parser_param_release(conn_parser_param_t *param)
    {
      if (!param) return;
      param->ctx.err_msg[0] = '\0';
      param->ctx.row0 = 0;
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
%parse-param { conn_parser_param_t *param }

// union members
%union { parser_token_t token; }
%union { char c; }

%token DSN UID PWD DRIVER SERVER DATABASE UNSIGNED_PROMOTION TIMESTAMP_AS_IS DB
%token CHARSET CHARSET_FOR_COL_BIND CHARSET_FOR_PARAM_BIND
%token TOPIC
%token <token> ID VALUE FQDN DIGITS
%token <token> TNAME TKEY TVAL

 /* %nterm <str>   args */ // non-terminal `input` use `str` to store
                           // token value as well
 /* %destructor { free($$); } <str> */

%% /* The grammar follows. */

input:
  %empty
| connect_str                     { (void)yynerrs; }
;

connect_str:
  dsnx
| dsn ';' attributes
| driverx
| driver ';' attributes
;

dsnx:
  dsn
| dsn ';'
;

dsn:
  DSN '=' VALUE                   { SET_DSN($3, @$); }
;

driverx:
  driver
| driver ';'
;


driver:
  DRIVER '=' VALUE                { SET_DRIVER($3, @$); }
| DRIVER '=' '{' VALUE '}'        { SET_DRIVER($4, @$); }
;

attributes:
  attribute
| attributes ';'
| attributes ';' attribute
;

attribute:
  UID '=' VALUE                   { SET_UID($3, @$); }
| DB '=' VALUE                    { SET_DB($3, @$); }
| PWD '=' VALUE                   { SET_PWD($3, @$); }
| ID '=' VALUE                    { ; }
| SERVER '=' FQDN                 { SET_FQDN($3, @$); }
| SERVER '=' FQDN ':'             { SET_FQDN($3, @$); }
| SERVER '=' FQDN ':' DIGITS      { SET_FQDN_PORT($3, $5, @$); }
| DATABASE '=' VALUE              { SET_DATABASE($3, @$); }
| UNSIGNED_PROMOTION '=' DIGITS   { SET_UNSIGNED_PROMOTION($3.text, $3.leng, @$); }
| TIMESTAMP_AS_IS '=' DIGITS      { SET_TIMESTAMP_AS_IS($3.text, $3.leng, @$); }
| CHARSET '=' VALUE               { SET_CHARSET($3, @$); }
| CHARSET_FOR_COL_BIND '=' VALUE               { SET_CHARSET_FOR_COL_BIND($3, @$); }
| CHARSET_FOR_PARAM_BIND '=' VALUE             { SET_CHARSET_FOR_PARAM_BIND($3, @$); }
;

%%

static void _yyerror_impl(
    YYLTYPE *yylloc,                   // match %define locations
    yyscan_t arg,                      // match %param
    conn_parser_param_t *param,             // match %parse-param
    const char *errmsg
)
{
  // to implement it here
  (void)yylloc;
  (void)arg;
  (void)param;
  (void)errmsg;

  if (!param) {
    fprintf(stderr, "(%d,%d)->(%d,%d):%s\n",
        yylloc->first_line, yylloc->first_column,
        yylloc->last_line, yylloc->last_column,
        errmsg);

    return;
  }

  param->ctx.row0 = yylloc->first_line;
  param->ctx.col0 = yylloc->first_column;
  param->ctx.row1 = yylloc->last_line;
  param->ctx.col1 = yylloc->last_column;
  param->ctx.err_msg[0] = '\0';
  snprintf(param->ctx.err_msg, sizeof(param->ctx.err_msg), "%s", errmsg);
}

/* Called by yyparse on error. */
static void yyerror(
    YYLTYPE *yylloc,                   // match %define locations
    yyscan_t arg,                      // match %param
    conn_parser_param_t *param,             // match %parse-param
    const char *errmsg
)
{
  _yyerror_impl(yylloc, arg, param, errmsg);
}

int conn_parser_parse(const char *input, size_t len, conn_parser_param_t *param)
{
  yyscan_t arg = {0};
  yylex_init(&arg);
  // yyset_in(in, arg);
  int debug_flex = param ? param->ctx.debug_flex : 0;
  int debug_bison = param ? param->ctx.debug_bison: 0;
  yyset_debug(debug_flex, arg);
  yydebug = debug_bison;
  // yyset_extra(param, arg);
  yy_scan_bytes(input ? input : "", input ? (int)len : 0, arg);
  int ret =yyparse(arg, param);
  yylex_destroy(arg);
  return ret ? -1 : 0;
}

