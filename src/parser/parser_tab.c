#include "parser.h"

#include "parser.tab.h"
#include "parser.lex.c"

#include "parser.lex.h"
#undef yylloc
#undef yylval
#include "parser.tab.c"

