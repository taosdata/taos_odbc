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
    #define YYSTYPE       EXT_PARSER_YYSTYPE
    #define YYLTYPE       EXT_PARSER_YYLTYPE
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
        ext_parser_param_t *param,         // match %parse-param
        const char *errmsg
    );
    static void yyerror(
        YYLTYPE *yylloc,                   // match %define locations
        yyscan_t arg,                      // match %param
        ext_parser_param_t *param,         // match %parse-param
        const char *errsg
    );

    static int ext_parser_param_append_topic_name(ext_parser_param_t *param, const char *name, size_t len);
    static int ext_parser_param_append_topic_conf(ext_parser_param_t *param, const char *k, size_t kn, const char *v, size_t vn);

    #define SET_TOPIC(_v, _loc) do {                                                            \
      if (!param) break;                                                                        \
      if (ext_parser_param_append_topic_name(param, _v.text, _v.leng)) {                        \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
      param->is_topic = 1;                                                                      \
    } while (0)

    #define SET_TOPIC_KEY(_k, _loc) do {                                                        \
      if (!param) break;                                                                        \
      if (ext_parser_param_append_topic_conf(param, _k.text, _k.leng, NULL, 0)) {               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)

    #define SET_TOPIC_KEY_VAL(_k, _v, _loc) do {                                                \
      if (!param) break;                                                                        \
      if (ext_parser_param_append_topic_conf(param, _k.text, _k.leng, _v.text, _v.leng)) {      \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");                        \
        return -1;                                                                              \
      }                                                                                         \
    } while (0)

    void ext_parser_param_release(ext_parser_param_t *param)
    {
      if (!param) return;
      topic_cfg_release(&param->topic_cfg);
      param->ctx.err_msg[0] = '\0';
      param->ctx.row0 = 0;
      insert_eval_release(&param->insert_eval);
    }

    #define SET_INSERT_TBL_QM(_loc) do {                                       \
      int r = insert_eval_set_table_qm(&param->insert_eval);                   \
      if (!r) {                                                                \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");       \
        return -1;                                                             \
      }                                                                        \
    } while (0)

    #define SET_INSERT_TBL(_tbl, _loc) do {                                    \
      param->insert_eval.table_tbl = _tbl;                                     \
    } while (0)

    #define SET_INSERT_DB_TBL(_db, _tbl, _loc) do {                            \
      param->insert_eval.table_db  = _db;                                      \
      param->insert_eval.table_tbl = _tbl;                                     \
    } while (0)

    #define CREATE_ID(_r, _ID, _loc) do {                                      \
      _r = _create_id(_ID.text, _ID.leng);                                     \
      if (!_r) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");       \
        return -1;                                                             \
      }
    } while (0)

    #define SET_INSERT_SUPER_TBL(_tbl, _loc) do {                              \
      param->insert_eval.super_tbl = _tbl;                                     \
    } while (0)

    #define SET_INSERT_SUPER_DB_TBL(_db, _tbl, _loc) do {                      \
      param->insert_eval.super_db  = _db;                                      \
      param->insert_eval.super_tbl = _tbl;                                     \
    } while (0)

    #define SET_INSERT_TAGS(_names, _vals, _loc) do {                          \
      param->insert_eval.tag_names = _names;                                   \
      param->insert_eval.tag_vals  = _vals;                                    \
    } while (0)

    #define SET_INSERT_COLS(_names, _vals, _loc) do {                          \
      param->insert_eval.col_names = _names;                                   \
      param->insert_eval.col_vals  = _vals;                                    \
    } while (0)

    static inline variant_t* _create_ids(const char *s, size_t n, YYLTYPE *yylloc, yyscan_t arg, ext_parser_param_t *param)
    {
      variant_t *ids = (variant_t*)calloc(1, sizeof(*v));
      if (ids) {
        int r = strs_append(&v->ids, s, n);
        if (r == 0) return v;
      }

      _yyerror_impl(yylloc, arg, param, "runtime error:out of memory");
      variant_release(v);
      TOD_SAFE_FREE(v);
      return NULL;
    }

    #define CREATE_IDS(_r, _ID, _loc) do {                                     \
      _r = _create_ids(_ID.text, _ID.leng);                                    \
      if (!_r) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");       \
        return -1;                                                             \
      }
    } while (0)

    #define IDS_APPEND(_r, _ids, _ID, _loc) do {                               \
      int r = _create_ids(_ids, _ID.text, _ID.leng);                           \
      if (r) {                                                                 \
        variant_release(_ids);                                                 \
        TOD_SAFE_FREE(_ids);                                                   \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");       \
        return -1;                                                             \
      }
      _r = _ids;
    } while (0)

    #define CREATE_EXPS(_r, _exp, _loc) do {                                   \
      _r = _create_exps(_exp);                                                 \
      variant_release(_exp);                                                   \
      TOD_SAFE_FREE(_exp);                                                     \
      if (!_r) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");       \
        return -1;                                                             \
      }
    } while (0)

    #define EXPS_APPEND(_r, _ids, _exp, _loc) do {                             \
      int r = _create_ids(_ids, _exp);                                         \
      variant_release(_exp);                                                   \
      TOD_SAFE_FREE(_exp);                                                     \
      if (r) {                                                                 \
        variant_release(_ids);                                                 \
        TOD_SAFE_FREE(_ids);                                                   \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");       \
        return -1;                                                             \
      }
      _r = _ids;
    } while (0)

    #define CREATE_EXP(_r, _f, _e1, _e2, _loc) do {                            \
      _r = _create_exp(_f, _e1, _e2);                                          \
      if (!_r) {                                                               \
        variant_release(_e1);                                                  \
        TOD_SAFE_FREE(_e1);                                                    \
        variant_release(_e2);                                                  \
        TOD_SAFE_FREE(_e2);                                                    \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");       \
        return -1;                                                             \
      }
    } while (0)

    #define CREATE_EXP_BY_NAME(_r, _f, _e, _loc) do {
      _r = _create_exp_by_name(_f, _e);                                        \
      if (!_r) {                                                               \
        variant_release(_e);                                                   \
        TOD_SAFE_FREE(_e);                                                     \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");       \
        return -1;                                                             \
      }
    } while (0)

    #define CREATE_EXP_QM(_r, _loc) do {                                       \
      _r = _create_exp_qm();                                                   \
      if (!_r) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");       \
        return -1;                                                             \
      }
    } while (0)

    #define CREATE_EXP_ID(_r, _ID, _loc) do {                                  \
      _r = _create_exp_id(_ID.text, _ID.leng);                                 \
      if (!_r) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");       \
        return -1;                                                             \
      }
    } while (0)

    #define CREATE_EXP_INTEGRAL(_r, _I, _loc) do {                             \
      _r = _create_exp_integral(_I.text, _I.leng);                             \
      if (!_r) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");       \
        return -1;                                                             \
      }
    } while (0)

    #define CREATE_EXP_NUMBER(_r, _N, _loc) do {                               \
      _r = _create_exp_number(_N.text, _N.leng);                               \
      if (!_r) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");       \
        return -1;                                                             \
      }
    } while (0)

    #define STRS_FLAT(_r, _ss, _loc) do {                                      \
      _r = _strs_flat(_ss);                                                    \
      _variant_release(_ss);                                                   \
      TOD_SAFE_FREE(_ss);                                                      \
      if (!_r) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");       \
        return -1;                                                             \
      }                                                                        \
    } while (0)

    #define CREATE_STRS(_r, _s, _loc) do {                                     \
      _r = _create_strs(_s);                                                   \
      if (!_r) {                                                               \
        _variant_release(_s);                                                  \
        TOD_SAFE_FREE(_s);                                                     \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");       \
        return -1;                                                             \
      }                                                                        \
    } while (0)

    #define STRS_APPEND(_r, _ss, _s, _loc) do {                                \
      int r = _strs_append(_ss, _s);                                           \
      if (r) {                                                                 \
        _variant_release(_ss);                                                 \
        TOD_SAFE_FREE(_ss);                                                    \
        _variant_release(_s);                                                  \
        TOD_SAFE_FREE(_s);                                                     \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");       \
        return -1;                                                             \
      }                                                                        \
      _r = _ss;                                                                \
    } while (0)

    #define CREATE_STR(_r, _s, _n, _loc) do {                                  \
      _r = _create_str(_s, _n);                                                \
      if (!_r) {                                                               \
        _yyerror_impl(&_loc, arg, param, "runtime error:out of memory");       \
        return -1;                                                             \
      }                                                                        \
    } while (0)

    #define STR_APPEND(_r, _s, _a, _loc) do {
    } while (0)
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
%parse-param { ext_parser_param_t *param }

// union members
%union { parser_token_t token; }
%union { char c; }
%union { variant_t *v; }

%destructor { variant_release($$); free($$); } <v>

%token TOPIC UNEXP
%token <token> DIGITS
%token <token> TNAME TKEY TVAL

%token <v> id str ids exps exp term func str qstr sstr tstr

%token INSERT INTO USING WITH TAGS VALUES
%token ID INTEGRAL NUMBER QSTR SSTR TSTR
%left '+' '-'
%left '*' '/'
%left UMINUS

 /* %nterm <str>   args */ // non-terminal `input` use `str` to store
                           // token value as well
 /* %destructor { free($$); } <str> */

%% /* The grammar follows. */

input:
  %empty
| '!' topic                     { (void)yynerrs; }
| '!' insert                    { (void)yynerrs; }
;

insert:
  INSERT INTO table_name values_clause
| INSERT INTO table_name using_clause values_clause
;

table_name:
  '?'                           { SET_INSERT_TBL_QM(@$); }
| id                            { SET_INSERT_TBL($1, @$); }
| id '.' id                     { SET_INSERT_DB_TBL($1, $3, @$); }
;

id:
  ID                            { CREATE_ID($$, $1, @$); }
| str                           { $$ = $1; }
;

using_clause:
  USING super_table_name
| USING super_table_name WITH tags_clause
;

super_table_name:
  id                            { SET_INSERT_SUPER_TBL($1, @$); }
| id '.' id                     { SET_INSERT_SUPER_DB_TBL($1, $3, @$); }
;

tags_clause:
  '(' ids ')' TAGS '(' exps ')' { SET_INSERT_TAGS($2,   $6, @$); }
| TAGS '(' exps ')'             { SET_INSERT_TAGS(NULL, $3, @$); }
;

values_clause:
  '(' ids ')' COLUES '(' exps ')'  { SET_INSERT_COLS($2,   $6, @$); }
| COLUES '(' exps ')'              { SET_INSERT_COLS(NULL, $6, @$); }
;

ids:
  ID                               { CREATE_IDS($$, $1, @$); }
| ids ',' ID                       { IDS_APPEND($$, $1, $3, @$); }
;

exps:
  exp                              { CREATE_EXPS($$, $1, @$); }
| exps ',' exp                     { EXPS_APPEND($$, $1, $3, @$); }
;

exp:
  term                             { $$ = $1; }
| exp '+' exp                      { CREATE_EXP($$, _add, $1, $3, @$); }
| exp '-' exp                      { CREATE_EXP($$, _sub, $1, $3, @$); }
| exp '*' exp                      { CREATE_EXP($$, _mul, $1, $3, @$); }
| exp '/' exp                      { CREATE_EXP($$, _div, $1, $3, @$); }
| '-' exp           %prec UMINUS   { CREATE_EXP($$, _neg, $2, @$); }
| '(' exp ')'                      { $$ = $2; }
| func '(' exps ')'                { CREATE_EXP_BY_NAME($$, $1, $3, @$); }
;

term:
  '?'                              { CREATE_EXP_QM($$, @$); }
| ID                               { CREATE_EXP_ID($$, $1, @$); }
| INTEGRAL                         { CREATE_EXP_INTEGRAL($$, $1, @$); }
| NUMBER                           { CREATE_EXP_NUMBER($$, $1, @$); }
| str                              { $$ = $1; }
;

str:
  '"' qstrs '"'                    { STRS_FLAT($$, $2, @$); }
| '\'' sstrs '\''                  { STRS_FLAT($$, $2, @$); }
| '`' tstrs '`'                    { STRS_FLAT($$, $2, @$); }
;

qstrs:
  qstr                             { CREATE_STRS($$, $1, @$); }
| qstrs qstr                       { STRS_APPEND($$, $1, $2, @$); }
;

qstr:
  QSTR                             { CREATE_STR($$, $1.text, $1.leng, @$); }
| '\\' '"'                         { CREATE_STR($$, "\"", 1, @$); }
;

sstrs:
  sstr                             { CREATE_STRS($$, $1, @$); }
| sstrs sstr                       { STRS_APPEND($$, $1, $2, @$); }
;

sstr:
  SSTR                             { CREATE_STR($$, $1.text, $1.leng, @$); }
| '\\' '\''                        { CREATE_STR($$, "'", 1, @$); }
;

tstrs:
  tstr                             { CREATE_STRS($$, $1, @$); }
| tstrs tstr                       { STRS_APPEND($$, $1, $2, @$); }
;

tstr:
  TSTR                             { CREATE_STR($$, $1.text, $1.leng, @$); }
| '\\' '`'                         { CREATE_STR($$, "`", 1, @$); }
;

func:
  ID                               { CREATE_FUNC($$, $1, @$); }
;


topic:
  TOPIC names
| TOPIC names '{' tconfs '}'
;

names:
  TNAME                     { SET_TOPIC($1, @$); }
| names TNAME               { SET_TOPIC($2, @$); }
;

tconfs:
  %empty
| tconf
| tconfs delimits tconf
;

tconf:
  TKEY                           { SET_TOPIC_KEY($1, @$); }
| TKEY '=' TVAL                  { SET_TOPIC_KEY_VAL($1, $3, @$); }
;

delimits:
  ';'
| delimits ';'
;

%%

static void _yyerror_impl(
    YYLTYPE *yylloc,                   // match %define locations
    yyscan_t arg,                      // match %param
    ext_parser_param_t *param,         // match %parse-param
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
    ext_parser_param_t *param,         // match %parse-param
    const char *errmsg
)
{
  _yyerror_impl(yylloc, arg, param, errmsg);
}

static int ext_parser_param_append_topic_name(ext_parser_param_t *param, const char *name, size_t len)
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

static int ext_parser_param_append_topic_conf(ext_parser_param_t *param, const char *k, size_t kn, const char *v, size_t vn)
{
  topic_cfg_t *cfg = &param->topic_cfg;
  return topic_cfg_append_kv(cfg, k, kn, v, vn);
}

int ext_parser_parse(const char *input, size_t len, ext_parser_param_t *param)
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

