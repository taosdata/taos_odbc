%{
/*
 * MIT License
 *
 * Copyright (c) 2022 freemine <freemine@yeah.net>
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
%}


%{
#define MKT(x)    TOK_##x

#define PUSH(state)      yy_push_state(state, yyscanner)
#define POP()            yy_pop_state(yyscanner)

#define CHG(state) do {                           \
    yy_pop_state(yyscanner);                      \
    yy_push_state(state, yyscanner);              \
} while (0)

#define TOP_STATE(_top) do {                  \
    yy_push_state(INITIAL, yyscanner);        \
    _top = yy_top_state(yyscanner);           \
    yy_pop_state(yyscanner);                  \
} while (0)

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
%option nounistd
%option noyywrap noinput nounput
%option verbose
%option debug
%option stack
%option nodefault
%option warn
%option perf-report
%option 8bit
%option never-interactive
%option nounistd

%x EQ
%x CURLY
%x SERVER
%x SERVER_EQ
%x FQDN
%x COLON

SP            [ \t]
LN            "\r\n"|"\n\r"|[\f\r\n]
DSN           (?i:dsn)
UID           (?i:uid)
PWD           (?i:pwd)
DB            (?i:db)
DRIVER        (?i:driver)
SERVER        (?i:server)
UNSIGNED_PROMOTION          (?i:unsigned_promotion)
TIMESTAMP_AS_IS             (?i:timestamp_as_is)
CACHE_SQL     (?i:cache_sql)
FQDN          [-[:alnum:]]+((\.[-[:alnum:]]+)+)*(\.)?
ID            [^\[\]{}(),;?*=!@[:space:]]+
VALUE         [^\[\]{}(),;?*=!@[:space:]]+
VALUEX        [^{}[:space:]][^{}]*[^{}[:space:]]

DIGITS        [[:digit:]]+
%%

<<EOF>> { int state; TOP_STATE(state);
          if (state != INITIAL) return -1;
          yyterminate(); }

{DSN}         { R(); C(); return MKT(DSN); }
{UID}         { R(); C(); return MKT(UID); }
{PWD}         { R(); C(); return MKT(PWD); }
{DB}          { R(); C(); return MKT(DB); }
{DRIVER}      { R(); C(); return MKT(DRIVER); }
{SERVER}      { R(); PUSH(SERVER); C(); return MKT(SERVER); }
{UNSIGNED_PROMOTION}        { R(); C(); return MKT(UNSIGNED_PROMOTION); }
{TIMESTAMP_AS_IS}           { R(); C(); return MKT(TIMESTAMP_AS_IS); }
{CACHE_SQL}   { R(); C(); return MKT(CACHE_SQL); }
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
";"       { R(); POP(); C(); return *yytext; }
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
{DIGITS}  { R(); POP(); SET_STR(); C(); return MKT(DIGITS); }
{VALUE}   { R(); POP(); SET_STR(); C(); return MKT(VALUE); }
";"       { R(); POP(); C(); return *yytext; }
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

