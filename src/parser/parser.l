%{
%}


%{
#define MKT(x)    TOK_##x

#define PUSH(state)      yy_push_state(state, yyscanner)
#define POP()            yy_pop_state(yyscanner)

#define CHG(state) do {                           \
    yy_pop_state(yyscanner);                      \
    yy_push_state(state, yyscanner);              \
} while (0)

#define TOP_STATE()                               \
    ({  yy_push_state(INITIAL, yyscanner);        \
        int _top = yy_top_state(yyscanner);       \
        yy_pop_state(yyscanner);                  \
        _top; })

#define R() do {                                  \
    yylloc->first_column = yylloc->last_column ;  \
    yylloc->first_line   = yylloc->last_line;     \
} while (0)

#define L() do {                                  \
    yylloc->last_line   += 1;                     \
    yylloc->last_column  = 1;                     \
} while (0)

#define C()                                       \
do {                                              \
    yylloc->last_column += yyleng;                \
} while (0)

#define SET_STR() do {                            \
    yylval->token.text = yytext;                  \
    yylval->token.leng = yyleng;                  \
} while (0)

#define SET_CHR(chr) do {                         \
    yylval->c = chr;                              \
} while (0)

%}

%option bison-bridge bison-locations reentrant
%option noyywrap noinput nounput
%option verbose
%option debug
%option stack
%option nodefault
%option warn
%option perf-report
%option 8bit

%x EQ
%x CURLY
%x SERVER
%x SERVER_EQ
%x FQDN
%x COLON

SP       [ \t]
LN       "\r\n"|"\n\r"|[\f\r\n]
DSN      (?i:dsn)
UID      (?i:uid)
PWD      (?i:pwd)
DRIVER   (?i:driver)
SERVER   (?i:server)
LEGACY   (?i:legacy)
FMT_TIME (?i:fmt_time)
FQDN     [-[:alnum:]]+((\.[-[:alnum:]]+)+)*(\.)?
ID       [^\[\]{}(),;?*=!@[:space:]]+
VALUE    [^\[\]{}(),;?*=!@[:space:]]+
VALUEX   [^{}[:space:]][^{}]*[^{}[:space:]]

DIGITS   [[:digit:]]+
%%

<<EOF>> { int state = TOP_STATE();
          if (state != INITIAL) return -1;
          yyterminate(); }

{DSN}         { R(); C(); return MKT(DSN); }
{UID}         { R(); C(); return MKT(UID); }
{PWD}         { R(); C(); return MKT(PWD); }
{DRIVER}      { R(); C(); return MKT(DRIVER); }
{SERVER}      { R(); PUSH(SERVER); C(); return MKT(SERVER); }
{LEGACY}      { R(); C(); return MKT(LEGACY); }
{FMT_TIME}    { R(); C(); return MKT(FMT_TIME); }
{DIGITS}      { R(); SET_STR(); C(); return MKT(DIGITS); }
{ID}          { R(); SET_STR(); C(); return MKT(ID); }
"="           { R(); PUSH(EQ); C(); return *yytext; }
"{"           { R(); PUSH(CURLY); C(); return *yytext; }       /* "}" */
{SP}          { R(); C(); } /* eat */
{LN}          { R(); L(); } /* eat */
.             { R(); C(); return *yytext; } /* let bison to handle */

<SERVER>{
"="       { R(); CHG(SERVER_EQ); C(); return *yytext; }
{SP}      { R(); C(); } /* eat */
{LN}      { R(); L(); } /* eat */
.         { R(); C(); return *yytext; } /* let bison to handle */
}

<SERVER_EQ>{
{SP}      { R(); C(); } /* eat */
{LN}      { R(); L(); } /* eat */
{FQDN}    { R(); SET_STR(); CHG(FQDN); C(); return MKT(FQDN); }
";"       { R(); POP(); C(); return *yytext; }
.         { R(); C(); return *yytext; } /* let bison to handle */
}

<FQDN>{
":"       { R(); CHG(COLON); C(); return ':'; }
";"       { R(); POP(); C(); return ';'; }
{SP}      { R(); POP(); C(); }
{LN}      { R(); POP(); L(); }
.         { R(); C(); return *yytext; } /* let bison to handle */
}

<COLON>{
{DIGITS}  { R(); POP(); SET_STR(); C(); return MKT(DIGITS); }
";"       { R(); POP(); C(); return ';'; }
{SP}      { R(); POP(); C(); }
{LN}      { R(); POP(); L(); }
.         { R(); C(); return *yytext; } /* let bison to handle */
}

<EQ>{
{SP}      { R(); C(); } /* eat */
{LN}      { R(); L(); } /* eat */
"{"       { R(); CHG(CURLY); C(); return *yytext; }       /* "}" */
{VALUE}   { R(); POP(); SET_STR(); C(); return MKT(VALUE); }
.         { R(); C(); return *yytext; } /* let bison to handle */
}

<CURLY>{
{SP}      { R(); C(); } /* eat */
{LN}      { R(); L(); } /* eat */
  /* "{" */
"}"       { R(); POP(); C(); return *yytext; }
{VALUEX}  { R(); SET_STR(); C(); return MKT(VALUE); }
.         { R(); C(); return *yytext; } /* let bison to handle */
}

%%

