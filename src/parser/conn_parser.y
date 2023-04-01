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
        parser_param_t *param,             // match %parse-param
        const char *errmsg
    );
    static void yyerror(
        YYLTYPE *yylloc,                   // match %define locations
        yyscan_t arg,                      // match %param
        parser_param_t *param,             // match %parse-param
        const char *errsg
    );

    static int parser_param_append_topic_name(parser_param_t *param, const char *name, size_t len);
    static int parser_param_append_topic_conf(parser_param_t *param, const char *k, size_t kn, const char *v, size_t vn);

    #define SET_TOPIC(_v, _loc) do {                                                            \
      if (!param) break;                                                                        \
      if (parser_param_append_topic_name(param, _v.text, _v.leng)) {                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)

    #define SET_TOPIC_KEY(_k, _loc) do {                                                        \
      if (!param) break;                                                                        \
      if (parser_param_append_topic_conf(param, _k.text, _k.leng, NULL, 0)) {                   \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)

    #define SET_TOPIC_KEY_VAL(_k, _v, _loc) do {                                                \
      if (!param) break;                                                                        \
      if (parser_param_append_topic_conf(param, _k.text, _k.leng, _v.text, _v.leng)) {          \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)

    #define SET_DSN(_v, _loc) do {                                                              \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg.dsn);                                                       \
      param->conn_cfg.dsn = strndup(_v.text, _v.leng);                                          \
      if (!param->conn_cfg.dsn) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_UID(_v, _loc) do {                                                              \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg.uid);                                                       \
      param->conn_cfg.uid = strndup(_v.text, _v.leng);                                          \
      if (!param->conn_cfg.uid) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_DB(_v, _loc) do {                                                               \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg.db);                                                        \
      param->conn_cfg.db = strndup(_v.text, _v.leng);                                           \
      if (!param->conn_cfg.db) {                                                                \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_PWD(_v, _loc) do {                                                              \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg.pwd);                                                       \
      param->conn_cfg.pwd = strndup(_v.text, _v.leng);                                          \
      if (!param->conn_cfg.pwd) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_DRIVER(_v, _loc) do {                                                           \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg.driver);                                                    \
      param->conn_cfg.driver = strndup(_v.text, _v.leng);                                       \
      if (!param->conn_cfg.driver) {                                                            \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)
    #define SET_FQDN(_v, _loc) do {                                                             \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg.ip);                                                        \
      param->conn_cfg.ip = strndup(_v.text, _v.leng);                                           \
      if (!param->conn_cfg.ip) {                                                                \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
      param->conn_cfg.port = 0;                                                                 \
      param->conn_cfg.port_set = 0;                                                             \
    } while (0)
    #define SET_FQDN_PORT(_v, _p, _loc) do {                                                    \
      if (!param) break;                                                                        \
      TOD_SAFE_FREE(param->conn_cfg.ip);                                                        \
      param->conn_cfg.ip = strndup(_v.text, _v.leng);                                           \
      if (!param->conn_cfg.ip) {                                                                \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
      param->conn_cfg.port = strtol(_p.text, NULL, 10);                                         \
      param->conn_cfg.port_set = 1;                                                             \
    } while (0)
    #define SET_UNSIGNED_PROMOTION(_s, _n, _loc) do {                                           \
      if (!param) break;                                                                        \
      OA_NIY(_s[_n] == '\0');                                                                   \
      param->conn_cfg.unsigned_promotion = !!(atoi(_s));                                        \
      param->conn_cfg.unsigned_promotion_set = 1;                                               \
    } while (0)
    #define SET_TIMESTAMP_AS_IS(_s, _n, _loc) do {                                              \
      if (!param) break;                                                                        \
      OA_NIY(_s[_n] == '\0');                                                                   \
      param->conn_cfg.timestamp_as_is = !!(atoi(_s));                                           \
      param->conn_cfg.timestamp_as_is_set = 1;                                                  \
    } while (0)

    void parser_param_release(parser_param_t *param)
    {
      if (!param) return;
      conn_cfg_release(&param->conn_cfg);
      topic_cfg_release(&param->topic_cfg);
      param->err_msg[0] = '\0';
      param->row0 = 0;
      param->load_type = PARAM_LOAD_UNKNOWN;
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

%token DSN UID PWD DRIVER SERVER UNSIGNED_PROMOTION TIMESTAMP_AS_IS DB
%token TOPIC
%token <token> ID VALUE FQDN DIGITS
%token <token> TNAME TKEY TVAL

 /* %nterm <str>   args */ // non-terminal `input` use `str` to store
                           // token value as well
 /* %destructor { free($$); } <str> */

%% /* The grammar follows. */

input:
  %empty
| connect_str               { param->load_type = PARAM_LOAD_CONN_STR; }
| topic                     { param->load_type = PARAM_LOAD_TOPIC_CFG; }
;

topic:
  '!' TOPIC names
| '!' TOPIC names '{' '}'
| '!' TOPIC names '{' tconfs '}'
;

names:
  TNAME                     { SET_TOPIC($1, @$); }
| names TNAME               { SET_TOPIC($2, @$); }
;

tconfs:
  tconf
| tconfs tconf
;

tconf:
  TKEY                           { SET_TOPIC_KEY($1, @$); }
| TKEY '=' TVAL                  { SET_TOPIC_KEY_VAL($1, $3, @$); }
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
;

%%

static void _yyerror_impl(
    YYLTYPE *yylloc,                   // match %define locations
    yyscan_t arg,                      // match %param
    parser_param_t *param,             // match %parse-param
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
        yylloc->last_line, yylloc->last_column - 1,
        errmsg);

    return;
  }

  param->row0 = yylloc->first_line;
  param->col0 = yylloc->first_column;
  param->row1 = yylloc->last_line;
  param->col1 = yylloc->last_column;
  param->err_msg[0] = '\0';
  snprintf(param->err_msg, sizeof(param->err_msg), "%s", errmsg);
}

/* Called by yyparse on error. */
static void yyerror(
    YYLTYPE *yylloc,                   // match %define locations
    yyscan_t arg,                      // match %param
    parser_param_t *param,             // match %parse-param
    const char *errmsg
)
{
  _yyerror_impl(yylloc, arg, param, errmsg);
}

static int parser_param_append_topic_name(parser_param_t *param, const char *name, size_t len)
{
  topic_cfg_t *cfg = &param->topic_cfg;
  if (cfg->names_nr == cfg->names_cap) {
    size_t cap = cfg->names_cap + 16;
    char **names = (char**)realloc(cfg->names, sizeof(*names) * cap);
    if (!names) return -1;
    cfg->names = names;
    cfg->names_cap = cap;
  }
  cfg->names[cfg->names_nr] = strndup(name, len);
  if (!cfg->names[cfg->names_nr]) return -1;
  cfg->names_nr += 1;
  return 0;
}

static int parser_param_append_topic_conf(parser_param_t *param, const char *k, size_t kn, const char *v, size_t vn)
{
  topic_cfg_t *cfg = &param->topic_cfg;
  return topic_cfg_append_kv(cfg, k, kn, v, vn);
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

