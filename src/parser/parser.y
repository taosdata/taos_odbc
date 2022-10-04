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
    static void yyerror(
        YYLTYPE *yylloc,                   // match %define locations
        yyscan_t arg,                      // match %param
        parser_param_t *param,             // match %parse-param
        const char *errsg
    );

    #define SET_DSN(_v) do {                                         \
      if (!param) break;                                             \
      TOD_SAFE_FREE(param->conn_str.dsn);                            \
      param->conn_str.dsn = strndup(_v.text, _v.leng);               \
    } while (0)
    #define SET_UID(_v) do {                                         \
      if (!param) break;                                             \
      TOD_SAFE_FREE(param->conn_str.uid);                            \
      param->conn_str.uid = strndup(_v.text, _v.leng);               \
    } while (0)
    #define SET_DB(_v) do {                                          \
      if (!param) break;                                             \
      TOD_SAFE_FREE(param->conn_str.db);                             \
      param->conn_str.db = strndup(_v.text, _v.leng);                \
    } while (0)
    #define SET_PWD(_v) do {                                         \
      if (!param) break;                                             \
      TOD_SAFE_FREE(param->conn_str.pwd);                            \
      param->conn_str.pwd = strndup(_v.text, _v.leng);               \
    } while (0)
    #define SET_DRIVER(_v) do {                                      \
      if (!param) break;                                             \
      TOD_SAFE_FREE(param->conn_str.driver);                         \
      param->conn_str.driver = strndup(_v.text, _v.leng);            \
    } while (0)
    #define SET_FQDN(_v) do {                                        \
      if (!param) break;                                             \
      TOD_SAFE_FREE(param->conn_str.ip);                             \
      param->conn_str.ip = strndup(_v.text, _v.leng);                \
      param->conn_str.port = 0;                                      \
    } while (0)
    #define SET_FQDN_PORT(_v, _p) do {                               \
      if (!param) break;                                             \
      TOD_SAFE_FREE(param->conn_str.ip);                             \
      param->conn_str.ip = strndup(_v.text, _v.leng);                \
      param->conn_str.port = strtol(_p.text, NULL, 10);              \
    } while (0)
    #define SET_UNSIGNED_PROMOTION(_s, _n) do {                      \
      if (!param) break;                                             \
      OA_NIY(_s[_n] == '\0');                                        \
      param->conn_str.unsigned_promotion = atoi(_s);                 \
    } while (0)
    #define SET_CACHE_SQL(_s, _n) do {                               \
      if (!param) break;                                             \
      OA_NIY(_s[_n] == '\0');                                        \
      param->conn_str.cache_sql = atoi(_s);                          \
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
%parse-param { parser_param_t *param }

// union members
%union { parser_token_t token; }
%union { char c; }

%token DSN UID PWD DRIVER SERVER UNSIGNED_PROMOTION CACHE_SQL DB
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
| DSN '=' VALUE                   { SET_DSN($3); }
| UID
| UID '='
| UID '=' VALUE                   { SET_UID($3); }
| DB
| DB '='
| DB '=' VALUE                    { SET_DB($3); }
| PWD
| PWD '='
| PWD '=' VALUE                   { SET_PWD($3); }
| DRIVER '=' VALUE                { SET_DRIVER($3); }
| DRIVER '=' '{' VALUE '}'        { SET_DRIVER($4); }
| ID
| ID '='
| ID '=' VALUE                    { ; }
| SERVER
| SERVER '='
| SERVER '=' FQDN                 { SET_FQDN($3); }
| SERVER '=' FQDN ':'             { SET_FQDN($3); }
| SERVER '=' FQDN ':' DIGITS      { SET_FQDN_PORT($3, $5); }
| UNSIGNED_PROMOTION              { SET_UNSIGNED_PROMOTION("1", 1); }
| UNSIGNED_PROMOTION '='          { SET_UNSIGNED_PROMOTION("1", 1); }
| UNSIGNED_PROMOTION '=' DIGITS   { SET_UNSIGNED_PROMOTION($3.text, $3.leng); }
| CACHE_SQL                       { SET_CACHE_SQL("1", 1); }
| CACHE_SQL '='                   { SET_CACHE_SQL("1", 1); }
| CACHE_SQL '=' DIGITS            { SET_CACHE_SQL($3.text, $3.leng); }
;

%%

/* Called by yyparse on error. */
static void
yyerror(
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

  fprintf(stderr, "(%d,%d)->(%d,%d): %s\n",
      yylloc->first_line, yylloc->first_column,
      yylloc->last_line, yylloc->last_column - 1,
      errmsg);

  if (!param) return;

  param->row0 = yylloc->first_line;
  param->col0 = yylloc->first_column;
  param->row1 = yylloc->last_line;
  param->col1 = yylloc->last_column - 1;
  TOD_SAFE_FREE(param->errmsg);
  param->errmsg = strdup(errmsg);
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
  yy_scan_bytes(input ? input : "", input ? len : 0, arg);
  int ret =yyparse(arg, param);
  yylex_destroy(arg);
  return ret ? -1 : 0;
}

