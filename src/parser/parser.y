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

%token DSN UID PWD DRIVER SERVER
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
| DSN '=' VALUE                   { fprintf(stderr, "DSN=%.*s\n", (int)$3.leng, $3.text); SET_DSN($3); }
| UID
| UID '='
| UID '=' VALUE                   { fprintf(stderr, "UID=%.*s\n", (int)$3.leng, $3.text); SET_UID($3); }
| PWD
| PWD '='
| PWD '=' VALUE                   { fprintf(stderr, "PWD=%.*s\n", (int)$3.leng, $3.text); SET_PWD($3); }
| DRIVER '=' VALUE                { fprintf(stderr, "DRIVER=%.*s\n", (int)$3.leng, $3.text); SET_DRIVER($3); }
| DRIVER '=' '{' VALUE '}'        { fprintf(stderr, "DRIVER={%.*s}\n", (int)$4.leng, $4.text); SET_DRIVER($4); }
| ID
| ID '='
| ID '=' VALUE                    { fprintf(stderr, "unknown: %.*s=%.*s\n", (int)$1.leng, $1.text, (int)$3.leng, $3.text); }
| SERVER
| SERVER '='
| SERVER '=' FQDN                 { fprintf(stderr, "SERVER=%.*s\n", (int)$3.leng, $3.text); SET_FQDN($3); }
| SERVER '=' FQDN ':'             { fprintf(stderr, "SERVER=%.*s\n", (int)$3.leng, $3.text); SET_FQDN($3); }
| SERVER '=' FQDN ':' DIGITS      { fprintf(stderr, "SERVER=%.*s:%.*s\n", (int)$3.leng, $3.text, (int)$5.leng, $5.text); SET_FQDN_PORT($3, $5); }
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

  fprintf(stderr, "(%d,%d)->(%d,%d): %s",
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

