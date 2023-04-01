%{
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


SP            [ \t]
LN            "\r\n"|"\n\r"|[\f\r\n]

%%

<<EOF>> { if (0) { int state; TOP_STATE(state); if (state != INITIAL) return -1; }
          yyterminate(); }
{SP}          { R(); C(); } /* eat */
{LN}          { R(); L(); } /* eat */
.             { R(); C(); return *yytext; } /* let bison to handle */

%%

