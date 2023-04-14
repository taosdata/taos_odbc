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
    #define YYSTYPE       EJSON_PARSER_YYSTYPE
    #define YYLTYPE       EJSON_PARSER_YYLTYPE
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
        ejson_parser_param_t *param,       // match %parse-param
        const char *errmsg
    );
    static void yyerror(
        YYLTYPE *yylloc,                   // match %define locations
        yyscan_t arg,                      // match %param
        ejson_parser_param_t *param,       // match %parse-param
        const char *errsg
    );

    #define OOM(_loc) do {                                           \
        _yyerror_impl(_loc, arg, param, "out of memory");            \
        return -1;                                                   \
    } while (0)

    #define EJSON_NEW_WITH_ID(_ejson, _token, _loc) do {             \
      const char *s = (_token)->text;                                \
      size_t n = (_token)->leng;                                     \
      *_ejson = ejson_new_str(s, n);                                 \
      if (!*_ejson) OOM(_loc);                                       \
    } while (0)

    #define EJSON_NEW_WITH_NUM(_ejson, _token, _loc) do {            \
      const char *s = (_token)->text;                                \
      size_t n = (_token)->leng;                                     \
      char buf[128];                                                 \
      snprintf(buf, sizeof(buf), "%.*s", (int)n, s);                 \
      double dbl = 0;                                                \
      sscanf(buf, "%lg", &dbl);                                      \
      *_ejson = ejson_new_num(dbl);                                  \
      if (!*_ejson) OOM(_loc);                                       \
    } while (0)

    #define EJSON_NEW_WITH_STR(_ejson, _str, _loc) do {              \
      *_ejson = _ejson_new_str(_str);                                \
      if (!*_ejson) OOM(_loc);                                       \
    } while (0)

    #define EJSON_NEW_ARR(_ejson, _loc) do {                         \
      *_ejson = ejson_new_arr();                                     \
      if (!*_ejson) OOM(_loc);                                       \
    } while (0)

    #define EJSON_DEC_REF(_v) do {                                   \
      if (_v) { ejson_dec_ref(_v); _v = NULL; }                      \
    } while (0)

    #define EJSON_NEW_ARR_WITH_VAL(_ejson, _val, _loc) do {          \
      *_ejson = ejson_new_arr();                                     \
      if (*_ejson) {                                                 \
        int r = ejson_arr_append(*_ejson, _val);                     \
        EJSON_DEC_REF(_val);                                         \
        if (r == 0) break;                                           \
      }                                                              \
      EJSON_DEC_REF(_val);                                           \
      EJSON_DEC_REF(*_ejson);                                        \
      OOM(_loc);                                                     \
    } while (0)

    #define EJSON_ARR_APPEND(_ejson, _val, _loc) do {                \
      int r = ejson_arr_append(_ejson, _val);                        \
      EJSON_DEC_REF(_val);                                           \
      if (r == 0) break;                                             \
      EJSON_DEC_REF(_ejson);                                         \
      OOM(_loc);                                                     \
    } while (0)

    #define EJSON_NEW_OBJ(_ejson, _loc) do {                         \
      *_ejson = ejson_new_obj();                                     \
      if (*_ejson) break;                                            \
      OOM(_loc);                                                     \
    } while (0)

    #define EJSON_NEW_OBJ_WITH_KV(_ejson, _kv, _loc) do {            \
      *_ejson = _ejson_new_obj(_kv);                                 \
      if (*_ejson) break;                                            \
      _ejson_kv_release(_kv);                                        \
      OOM(_loc);                                                     \
    } while (0)

    #define EJSON_OBJ_SET_KV(_ejson, _kv, _loc) do {                 \
      int r = _ejson_obj_set(_ejson, _kv);                           \
      _ejson_kv_release(_kv);                                        \
      if (r == 0) break;                                             \
      OOM(_loc);                                                     \
    } while (0)

    #define STR_INIT_EMPTY(_str, _loc) do {                          \
      if (_ejson_str_init(_str, "", 0)) OOM(_loc);                   \
    } while (0)

    #define STR_INIT(_str, _token, _loc) do {                        \
      const char *s = (_token)->text;                                \
      size_t n = (_token)->leng;                                     \
      if (_ejson_str_init(_str, s, n)) OOM(_loc);                    \
    } while (0)

    #define STR_INIT_CHR(_str, _c, _loc) do {                        \
      if (_ejson_str_init(_str, &_c, 1)) OOM(_loc);                  \
    } while (0)

    #define STR_APPEND(_str, _token, _loc) do {                      \
      const char *s = (_token)->text;                                \
      size_t n = (_token)->leng;                                     \
      if (_ejson_str_append(_str, s, n)) OOM(_loc);                  \
    } while (0)

    #define STR_APPEND_CHR(_str, _c, _loc) do {                       \
      if (_ejson_str_append(_str, &_c, 1)) OOM(_loc);                 \
    } while (0)

    #define KV_INIT_WITH_ID(_kv, _token, _loc) do {                  \
      const char *s = (_token)->text;                                \
      size_t n = (_token)->leng;                                     \
      if (_ejson_str_init(&((_kv)->key), s, n) == 0) break;          \
      OOM(_loc);                                                     \
    } while (0)

    #define KV_INIT(_kv, _token, _val, _loc) do {                    \
      const char *s = (_token)->text;                                \
      size_t n = (_token)->leng;                                     \
      if (_ejson_str_init(&((_kv)->key), s, n) == 0) {               \
        (_kv)->val = _val;                                           \
        break;                                                       \
      }                                                              \
      EJSON_DEC_REF(_val);                                           \
      OOM(_loc);                                                     \
    } while (0)

    void ejson_parser_param_release(ejson_parser_param_t *param)
    {
      if (!param) return;
      param->ctx.err_msg[0] = '\0';
      param->ctx.row0 = 0;
      if (param->ejson) {
        ejson_dec_ref(param->ejson);
        param->ejson = NULL;
      }
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
%parse-param { ejson_parser_param_t *param }

// union members
%union { ejson_parser_token_t  token; }
%union { ejson_t              *ejson; }
%union { _ejson_str_t          str; }
%union { _ejson_kv_t           kv; }
%union { char                  c; }

%token V_TRUE V_FALSE V_NULL ESC
%token <token> ID NUMBER STR EUNI
%token <c> ECHR
%nterm <ejson> json obj arr kvs jsons
%nterm <str> str strings
%nterm <kv> kv

%destructor { ejson_dec_ref($$); $$ = NULL; }           <ejson>
%destructor { _ejson_str_release(&$$); }     <str>
%destructor { _ejson_kv_release(&$$);  }     <kv>

 /* %nterm <str>   args */ // non-terminal `input` use `str` to store
                           // token value as well
 /* %destructor { free($$); } <str> */

%% /* The grammar follows. */

input:
  %empty
| json              { param->ejson = $1; }
;

json:
  ID                { EJSON_NEW_WITH_ID(&$$, &$1, &@1); }
| NUMBER            { EJSON_NEW_WITH_NUM(&$$, &$1, &@1); }
| V_TRUE            { $$ = ejson_new_true(); }
| V_FALSE           { $$ = ejson_new_false(); }
| V_NULL            { $$ = ejson_new_null(); }
| str               { EJSON_NEW_WITH_STR(&$$, &$1, &@1); }
| obj
| arr
;

str:
  '"' strings '"'      { $$ = $2; }
| '\'' strings '\''    { $$ = $2; }
| '`' strings '`'      { $$ = $2; }
| '"' '"'              { STR_INIT_EMPTY(&$$, &@1); }
| '\'' '\''            { STR_INIT_EMPTY(&$$, &@1); }
| '`' '`'              { STR_INIT_EMPTY(&$$, &@1); }
;

strings:
  STR                  { STR_INIT(&$$, &$1, &@1); }
| ESC ECHR             { STR_INIT_CHR(&$$, $2, &@2); }
| strings STR          { STR_APPEND(&$1, &$2, &@2); $$ = $1; }
| strings ESC ECHR     { STR_APPEND_CHR(&$1, $3, &@3); $$ = $1; }
;

arr:
  '[' ']'              { EJSON_NEW_ARR(&$$, &@1); }
| '[' jsons ']'        { $$ = $2; }
;

jsons:
  json                 { EJSON_NEW_ARR_WITH_VAL(&$$, $1, &@1); }
| jsons ',' json       { EJSON_ARR_APPEND($1, $3, &@1); $$ = $1; }
;

obj:
  '{' '}'              { EJSON_NEW_OBJ(&$$, &@1); }
| '{' kvs '}'          { $$ = $2; }
;

kvs:
  kv                   { EJSON_NEW_OBJ_WITH_KV(&$$, &$1, &@1); }
| kvs ',' kv           { EJSON_OBJ_SET_KV($1, &$3, &@3); $$ = $1; }
;

kv:
  str                  { $$.key = $1; $$.val = NULL; }
| str ':' json         { $$.key = $1; $$.val = $3; }
| ID                   { KV_INIT_WITH_ID(&$$, &$1, &@1); }
| ID ':' json          { KV_INIT(&$$, &$1, $3, &@3); }
;


%%

static void _yyerror_impl(
    YYLTYPE *yylloc,                   // match %define locations
    yyscan_t arg,                      // match %param
    ejson_parser_param_t *param,       // match %parse-param
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
    ejson_parser_param_t *param,       // match %parse-param
    const char *errmsg
)
{
  _yyerror_impl(yylloc, arg, param, errmsg);
}

int ejson_parser_parse(const char *input, size_t len, ejson_parser_param_t *param)
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

