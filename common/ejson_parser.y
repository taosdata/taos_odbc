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
        _yyerror_impl(&_loc, arg, param, "out of memory");           \
        return -1;                                                   \
    } while (0)

    #define SET_LOC(_dst, _src) do {                                 \
      _dst = _src;                                                   \
    } while (0)

    #define EJSON_NEW_WITH_ID(_ejson, _token, _loc) do {             \
      const char *s = _token.text;                                   \
      size_t n = _token.leng;                                        \
      _ejson = ejson_new_str(s, n);                                  \
      if (!_ejson) OOM(_loc);                                        \
    } while (0)

    #define EJSON_NEW_WITH_NUM(_ejson, _token, _loc) do {            \
      const char *s = _token.text;                                   \
      size_t n = _token.leng;                                        \
      _ejson = _ejson_new_num(s, n);                                 \
      if (!_ejson) OOM(_loc);                                        \
    } while (0)

    #define EJSON_NEW_WITH_STR(_ejson, _str, _loc) do {              \
      _ejson = _ejson_new_str(&_str);                                \
      if (!_ejson) OOM(_loc);                                        \
    } while (0)

    #define EJSON_NEW_ARR(_ejson, _loc) do {                         \
      _ejson = ejson_new_arr();                                      \
      if (!_ejson) OOM(_loc);                                        \
    } while (0)

    #define EJSON_DEC_REF(_v) do {                                   \
      if (_v) { ejson_dec_ref(_v); _v = NULL; }                      \
    } while (0)

    #define EJSON_NEW_ARR_WITH_VAL(_ejson, _val, _loc) do {          \
      _ejson = ejson_new_arr();                                      \
      if (!_ejson) {                                                 \
        EJSON_DEC_REF(_val);                                         \
        OOM(_loc);                                                   \
      }                                                              \
      int r = ejson_arr_append(_ejson, _val);                        \
      EJSON_DEC_REF(_val);                                           \
      if (r) {                                                       \
        EJSON_DEC_REF(_ejson);                                       \
        OOM(_loc);                                                   \
      }                                                              \
    } while (0)

    #define EJSON_ARR_APPEND(_ejson, _vals, _val, _loc) do {         \
      int r = ejson_arr_append(_vals, _val);                         \
      EJSON_DEC_REF(_val);                                           \
      if (r) {                                                       \
        EJSON_DEC_REF(_vals);                                        \
        OOM(_loc);                                                   \
      }                                                              \
      _ejson = _vals;                                                \
    } while (0)

    #define EJSON_NEW_OBJ(_ejson, _loc) do {                         \
      _ejson = ejson_new_obj();                                      \
      if (!_ejson) OOM(_loc);                                        \
    } while (0)

    #define EJSON_NEW_OBJ_WITH_KV(_ejson, _kv, _loc) do {            \
      _ejson = _ejson_new_obj(&_kv);                                 \
      _ejson_kv_release(&_kv);                                       \
      if (!_ejson) OOM(_loc);                                        \
    } while (0)

    #define EJSON_OBJ_APPEND(_ejson, _kvs, _kv, _loc) do {           \
      int r = _ejson_obj_set(_kvs, &_kv);                            \
      _ejson_kv_release(&_kv);                                       \
      if (r) {                                                       \
        EJSON_DEC_REF(_kvs);                                         \
        OOM(_loc);                                                   \
      }                                                              \
      _ejson = _kvs;                                                 \
    } while (0)

    #define STR_INIT_EMPTY(_str, _loc) do {                          \
      if (_ejson_str_init(&_str, "", 0)) OOM(_loc);                  \
    } while (0)

    #define STR_INIT(_str, _token, _loc) do {                        \
      const char *s = _token.text;                                   \
      size_t n = _token.leng;                                        \
      if (_ejson_str_init(&_str, s, n)) OOM(_loc);                   \
    } while (0)

    #define STR_INIT_CHR(_str, _c, _loc) do {                        \
      if (_ejson_str_init(&_str, &_c, 1)) OOM(_loc);                 \
    } while (0)

    #define STR_APPEND(_str, _v, _token, _loc) do {                  \
      const char *s = _token.text;                                   \
      size_t n = _token.leng;                                        \
      if (_ejson_str_append(&_v, s, n)) {                            \
        _ejson_str_release(&_v);                                     \
        OOM(_loc);                                                   \
      }                                                              \
      _str = _v;                                                     \
    } while (0)

    #define STR_APPEND_CHR(_str, _v, _c, _loc) do {                  \
      if (_ejson_str_append(&_v, &_c, 1)) {                          \
        _ejson_str_release(&_v);                                     \
        OOM(_loc);                                                   \
      }                                                              \
      _str = _v;                                                     \
    } while (0)

    #define KV_INIT_WITH_STR(_kv, _str, _loc) do {                   \
      _kv.val = ejson_new_null();                                    \
      _kv.key = _str;                                                \
    } while (0)

    #define KV_INIT_WITH_ID(_kv, _token, _loc) do {                  \
      const char *s = _token.text;                                   \
      size_t n = _token.leng;                                        \
      memset(&_kv, 0, sizeof(_kv));                                  \
      if (_ejson_str_init(&_kv.key, s, n)) OOM(_loc);                \
      _kv.val = ejson_new_null();                                    \
    } while (0)

    #define KV_INIT(_kv, _token, _val, _loc) do {                    \
      const char *s = _token.text;                                   \
      size_t n = _token.leng;                                        \
      _kv.val = _val;                                                \
      if (_ejson_str_init(&_kv.key, s, n)) {                         \
        _ejson_kv_release(&_kv);                                     \
        OOM(_loc);                                                   \
      }                                                              \
    } while (0)

    void ejson_parser_param_release(ejson_parser_param_t *param)
    {
      if (!param) return;
      param->ctx.err_msg[0] = '\0';
      param->ctx.bad_token.first_line = 0;
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
%define api.location.type {parser_loc_t}
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
  %empty            { (void)yynerrs; }
| json              { param->ejson = $1; SET_LOC(param->ejson->loc, @$); }
;

json:
  ID                { EJSON_NEW_WITH_ID($$, $1, @$);  SET_LOC($$->loc, @$); }
| NUMBER            { EJSON_NEW_WITH_NUM($$, $1, @$); SET_LOC($$->loc, @$); }
| V_TRUE            { $$ = ejson_new_true();          SET_LOC($$->loc, @$); }
| V_FALSE           { $$ = ejson_new_false();         SET_LOC($$->loc, @$); }
| V_NULL            { $$ = ejson_new_null();          SET_LOC($$->loc, @$); }
| str               { EJSON_NEW_WITH_STR($$, $1, @$); SET_LOC($$->loc, @$); }
| obj               { $$ = $1; SET_LOC($$->loc, @$); }
| arr               { $$ = $1; SET_LOC($$->loc, @$); }
;

str:
  '"' strings '"'      { $$ = $2; SET_LOC($$.loc, @$); }
| '\'' strings '\''    { $$ = $2; SET_LOC($$.loc, @$); }
| '`' strings '`'      { $$ = $2; SET_LOC($$.loc, @$); }
| '"' '"'              { STR_INIT_EMPTY($$, @$); SET_LOC($$.loc, @$); }
| '\'' '\''            { STR_INIT_EMPTY($$, @$); SET_LOC($$.loc, @$); }
| '`' '`'              { STR_INIT_EMPTY($$, @$); SET_LOC($$.loc, @$); }
;

strings:
  STR                  { STR_INIT($$, $1, @$);           SET_LOC($$.loc, @$); }
| ESC ECHR             { STR_INIT_CHR($$, $2, @$);       SET_LOC($$.loc, @$); }
| strings STR          { STR_APPEND($$, $1, $2, @$);     SET_LOC($$.loc, @$); }
| strings ESC ECHR     { STR_APPEND_CHR($$, $1, $3, @$); SET_LOC($$.loc, @$); }
;

arr:
  '[' ']'              { EJSON_NEW_ARR($$, @$); SET_LOC($$->loc, @$); }
| '[' jsons ']'        { $$ = $2;               SET_LOC($$->loc, @$); }
;

jsons:
  json                 { EJSON_NEW_ARR_WITH_VAL($$, $1, @$); SET_LOC($$->loc, @$); }
| ','                  { EJSON_NEW_ARR($$, @$);              SET_LOC($$->loc, @$); }
| jsons ',' json       { EJSON_ARR_APPEND($$, $1, $3, @$);   SET_LOC($$->loc, @$); }
| jsons ','            { $$ = $1;                            SET_LOC($$->loc, @$); }
;

obj:
  '{' '}'              { EJSON_NEW_OBJ($$, @$); SET_LOC($$->loc, @$); }
| '{' kvs '}'          { $$ = $2;               SET_LOC($$->loc, @$); }
;

kvs:
  kv                   { EJSON_NEW_OBJ_WITH_KV($$, $1, @$); SET_LOC($$->loc, @$); }
| ','                  { EJSON_NEW_OBJ($$, @$);             SET_LOC($$->loc, @$); }
| kvs ',' kv           { EJSON_OBJ_APPEND($$, $1, $3, @$);  SET_LOC($$->loc, @$); }
| kvs ','              { $$ = $1;                           SET_LOC($$->loc, @$); }
;

kv:
  str                  { KV_INIT_WITH_STR($$, $1, @$); SET_LOC($$.loc, @$); }
| str ':'              { KV_INIT_WITH_STR($$, $1, @$); SET_LOC($$.loc, @$); }
| str ':' json         { $$.key = $1; $$.val = $3;     SET_LOC($$.loc, @$); }
| ID                   { KV_INIT_WITH_ID($$, $1, @$);  SET_LOC($$.loc, @$); }
| ID ':'               { KV_INIT_WITH_ID($$, $1, @$);  SET_LOC($$.loc, @$); }
| ID ':' json          { KV_INIT($$, $1, $3, @$);      SET_LOC($$.loc, @$); }
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

  param->ctx.bad_token.first_line   = yylloc->first_line;
  param->ctx.bad_token.first_column = yylloc->first_column;
  param->ctx.bad_token.last_line    = yylloc->last_line;
  param->ctx.bad_token.last_column  = yylloc->last_column;
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
  yyset_extra(param, arg);
  param->ctx.input = input;
  param->ctx.len   = len;
  param->ctx.prev  = 0;
  param->ctx.pres  = 0;
  yy_scan_bytes(input ? input : "", input ? (int)len : 0, arg);
  int ret =yyparse(arg, param);
  yylex_destroy(arg);
  return ret ? -1 : 0;
}

