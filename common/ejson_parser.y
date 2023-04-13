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

    #define SET_TOPIC(_v, _loc) do {                                                            \
      if (!param) break;                                                                        \
    } while (0)

    #define SET_TOPIC_KEY(_k, _loc) do {                                                        \
      if (!param) break;                                                                        \
    } while (0)

    #define SET_TOPIC_KEY_VAL(_k, _v, _loc) do {                                                \
      if (!param) break;                                                                        \
    } while (0)

    void ejson_parser_param_release(ejson_parser_param_t *param)
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
%parse-param { ejson_parser_param_t *param }

// union members
%union { ejson_parser_token_t token; }
%union { char c; }

%token V_TRUE V_FALSE V_NULL ESC
%token <token> ID NUMBER STR ECHR EUNI

 /* %nterm <str>   args */ // non-terminal `input` use `str` to store
                           // token value as well
 /* %destructor { free($$); } <str> */

%% /* The grammar follows. */

input:
  %empty
| json
;

json:
  ID
| NUMBER
| V_TRUE
| V_FALSE
| V_NULL
| string
| obj
| arr
;

string:
  '"' strs '"'
| '\'' strs '\''
| '`' strs '`'
| '"' '"'
| '\'' '\''
| '`' '`'
;

strs:
  str
| strs str
;

str:
  STR
| ESC ECHR
| ESC EUNI
;

arr:
  '[' ']'
| '[' jsons ']'
;

jsons:
  json
| jsons ',' json
;

obj:
  '{' '}'
| '{' kvs '}'
;

kvs:
  kv
| kvs ',' kv
;

kv:
  string
| string ':' json
| ID
| ID ':' json
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

