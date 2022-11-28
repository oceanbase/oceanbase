/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "2.4.1"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 1

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1

/* Using locations.  */
#define YYLSP_NEEDED 1

/* Substitute the variable and function names.  */
#define yyparse         obpl_mysql_yyparse
#define yylex           obpl_mysql_yylex
#define yyerror         obpl_mysql_yyerror
#define yylval          obpl_mysql_yylval
#define yychar          obpl_mysql_yychar
#define yydebug         obpl_mysql_yydebug
#define yynerrs         obpl_mysql_yynerrs
#define yylloc          obpl_mysql_yylloc

/* Copy the first part of user declarations.  */

/* Line 189 of yacc.c  */
#line 26 "../../../src/pl/parser/pl_parser_mysql_mode.y"

#include <stdint.h>
#define YYDEBUG 1


/* Line 189 of yacc.c  */
#line 87 "../../../src/pl/parser/pl_parser_mysql_mode_tab.c"

/* Enabling traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 1
#endif

/* Enabling the token table.  */
#ifndef YYTOKEN_TABLE
# define YYTOKEN_TABLE 0
#endif

/* "%code requires" blocks.  */

/* Line 209 of yacc.c  */
#line 37 "../../../src/pl/parser/pl_parser_mysql_mode.y"

#ifndef YYLTYPE_IS_DECLARED
#define YYLTYPE_IS_DECLARED 1
typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
  int abs_first_column;
  int abs_last_column;
} YYLTYPE;
#endif /*YYLTYPE_IS_DECLARED*/

# define YYLLOC_DEFAULT(Current, Rhs, N)                        \
  do                                                            \
    if (YYID (N)) {                                             \
      (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;    \
      (Current).first_column = YYRHSLOC (Rhs, 1).first_column;  \
      (Current).last_line    = YYRHSLOC (Rhs, N).last_line;     \
      (Current).last_column  = YYRHSLOC (Rhs, N).last_column;   \
      (Current).abs_first_column = YYRHSLOC (Rhs, 1).abs_first_column;     \
      (Current).abs_last_column  = YYRHSLOC (Rhs, N).abs_last_column;      \
    } else {                                                    \
      (Current).first_line   = (Current).last_line   =          \
        YYRHSLOC (Rhs, 0).last_line;                            \
      (Current).first_column = (Current).last_column =          \
        YYRHSLOC (Rhs, 0).last_column;                          \
      (Current).abs_first_column = (Current).abs_last_column =          \
        YYRHSLOC (Rhs, 0).abs_last_column;                              \
    }                                                           \
  while (YYID (0))



/* Line 209 of yacc.c  */
#line 147 "../../../src/pl/parser/pl_parser_mysql_mode_tab.c"

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     END_P = 258,
     ALTER = 259,
     BEFORE = 260,
     BY = 261,
     CALL = 262,
     CASE = 263,
     CONDITION = 264,
     CONTINUE = 265,
     CREATE = 266,
     CURRENT_USER = 267,
     CURSOR = 268,
     DECLARE = 269,
     DEFAULT = 270,
     DELETE = 271,
     DETERMINISTIC = 272,
     DROP = 273,
     EACH = 274,
     ELSE = 275,
     ELSEIF = 276,
     EXISTS = 277,
     EXIT = 278,
     FETCH = 279,
     FOR = 280,
     FROM = 281,
     IF = 282,
     IN = 283,
     INDEX = 284,
     INOUT = 285,
     INSERT = 286,
     INTO = 287,
     IS = 288,
     ITERATE = 289,
     LEAVE = 290,
     LIMIT = 291,
     LONG = 292,
     LOOP = 293,
     MODIFIES = 294,
     NOT = 295,
     ON = 296,
     OR = 297,
     OUT = 298,
     PROCEDURE = 299,
     READS = 300,
     REPEAT = 301,
     REPLACE = 302,
     RESIGNAL = 303,
     RETURN = 304,
     SELECT = 305,
     SIGNAL = 306,
     SQL = 307,
     SQLEXCEPTION = 308,
     SQLSTATE = 309,
     SQLWARNING = 310,
     TABLE = 311,
     THEN = 312,
     TRIGGER = 313,
     UPDATE = 314,
     USING = 315,
     WHEN = 316,
     WHILE = 317,
     COMMIT = 318,
     ROLLBACK = 319,
     DO = 320,
     UNTIL = 321,
     SQL_KEYWORD = 322,
     SQL_TOKEN = 323,
     PARAM_ASSIGN_OPERATOR = 324,
     PARSER_SYNTAX_ERROR = 325,
     IDENT = 326,
     STRING = 327,
     INTNUM = 328,
     DECIMAL_VAL = 329,
     HEX_STRING_VALUE = 330,
     DATE_VALUE = 331,
     SYSTEM_VARIABLE = 332,
     USER_VARIABLE = 333,
     NULLX = 334,
     USER_NAME = 335,
     TINYINT = 336,
     SMALLINT = 337,
     MEDIUMINT = 338,
     INTEGER = 339,
     BIGINT = 340,
     FLOAT = 341,
     DOUBLE = 342,
     PRECISION = 343,
     NUMBER = 344,
     NUMERIC = 345,
     BIT = 346,
     DATETIME = 347,
     TIMESTAMP = 348,
     TIME = 349,
     DATE = 350,
     YEAR = 351,
     CHARACTER = 352,
     TEXT = 353,
     VARCHAR = 354,
     BINARY = 355,
     VARBINARY = 356,
     UNSIGNED = 357,
     SIGNED = 358,
     ZEROFILL = 359,
     COLLATE = 360,
     SET = 361,
     CHARSET = 362,
     BOOL = 363,
     BOOLEAN = 364,
     BLOB = 365,
     ENUM = 366,
     TINYTEXT = 367,
     MEDIUMTEXT = 368,
     LONGTEXT = 369,
     TINYBLOB = 370,
     MEDIUMBLOB = 371,
     LONGBLOB = 372,
     VARYING = 373,
     AFTER = 374,
     AUTHID = 375,
     BEGIN_KEY = 376,
     BINARY_INTEGER = 377,
     BODY = 378,
     C = 379,
     CATALOG_NAME = 380,
     CLASS_ORIGIN = 381,
     CLOSE = 382,
     COLUMN_NAME = 383,
     COMMENT = 384,
     CONSTRAINT_CATALOG = 385,
     CONSTRAINT_NAME = 386,
     CONSTRAINT_ORIGIN = 387,
     CONSTRAINT_SCHEMA = 388,
     CONTAINS = 389,
     COUNT = 390,
     CURSOR_NAME = 391,
     DATA = 392,
     DEFINER = 393,
     END_KEY = 394,
     EXTEND = 395,
     FOUND = 396,
     FUNCTION = 397,
     HANDLER = 398,
     INTERFACE = 399,
     INVOKER = 400,
     JSON = 401,
     LANGUAGE = 402,
     MESSAGE_TEXT = 403,
     MYSQL_ERRNO = 404,
     NEXT = 405,
     NO = 406,
     OF = 407,
     OPEN = 408,
     PACKAGE = 409,
     PRAGMA = 410,
     RECORD = 411,
     RETURNS = 412,
     ROW = 413,
     ROWTYPE = 414,
     SCHEMA_NAME = 415,
     SECURITY = 416,
     SUBCLASS_ORIGIN = 417,
     TABLE_NAME = 418,
     TYPE = 419,
     VALUE = 420,
     LOWER_PARENS = 421,
     PARENS = 422,
     DECLARATION = 423
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{

/* Line 214 of yacc.c  */
#line 31 "../../../src/pl/parser/pl_parser_mysql_mode.y"

  struct _ParseNode *node;
  const struct _NonReservedKeyword *non_reserved_keyword;
  int64_t ival;



/* Line 214 of yacc.c  */
#line 340 "../../../src/pl/parser/pl_parser_mysql_mode_tab.c"
} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif

#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} YYLTYPE;
# define yyltype YYLTYPE /* obsolescent; will be withdrawn */
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


/* Copy the second part of user declarations.  */

/* Line 264 of yacc.c  */
#line 71 "../../../src/pl/parser/pl_parser_mysql_mode.y"

#include "pl/parser/pl_parser_mysql_mode_lex.h"
#include "pl_parser_base.h"

typedef struct _YYLookaheadToken
{
  int *la_yychar;
  /* The semantic value of the lookahead symbol.  */
  YYSTYPE *la_yylval;
  /* Location data for the lookahead symbol.  */
  YYLTYPE *la_yylloc;
} YYLookaheadToken;

extern ParseNode *obpl_mysql_read_sql_construct(ObParseCtx *parse_ctx, const char *prefix, YYLookaheadToken *la_token, int end_token_cnt, ...);
extern void obpl_mysql_yyerror(YYLTYPE *yylloc, ObParseCtx *parse_ctx, char *s, ...);
extern void obpl_mysql_parse_fatal_error(int32_t errcode, yyscan_t yyscanner, yyconst char *msg, ...);

#define YY_FATAL_ERROR(msg, args...) (obpl_mysql_parse_fatal_error(OB_PARSER_ERR_NO_MEMORY, YYLEX_PARAM, msg, ##args))
#define YY_UNEXPECTED_ERROR(msg, args...) (obpl_mysql_parse_fatal_error(OB_PARSER_ERR_UNEXPECTED, YYLEX_PARAM, msg, ##args))

#define do_parse_sql_stmt(node, _parse_ctx, start_loc, end_loc, end_tok_num, ...)                          \
  do {                                                                                                     \
    YYLookaheadToken la_token;                                                                             \
    la_token.la_yychar = &yychar;                                                                          \
    la_token.la_yylval = &yylval;                                                                          \
    la_token.la_yylloc = &yylloc;                                                                          \
    _parse_ctx->scanner_ctx_.sql_start_loc = start_loc;                                                    \
    _parse_ctx->scanner_ctx_.sql_end_loc = end_loc;                                                        \
    node = obpl_mysql_read_sql_construct(_parse_ctx, "", &la_token, end_tok_num, ##__VA_ARGS__);           \
    if (NULL == node) {                                                                                    \
      YYERROR;                                                                                             \
    }                                                                                                      \
    reset_current_location(_parse_ctx->scanner_ctx_.sql_start_loc, _parse_ctx->scanner_ctx_.sql_end_loc);  \
  } while (0)

#define do_parse_sql_expr_rule(node, _parse_ctx, end_tok_num, ...)                                         \
  do {                                                                                                     \
    ParseNode *select_node = NULL;                                                                         \
    YYLookaheadToken la_token;                                                                             \
    la_token.la_yychar = &yychar;                                                                          \
    la_token.la_yylval = &yylval;                                                                          \
    la_token.la_yylloc = &yylloc;                                                                          \
    _parse_ctx->scanner_ctx_.sql_start_loc = -1;                                                           \
    _parse_ctx->scanner_ctx_.sql_end_loc = -1;                                                             \
    select_node = obpl_mysql_read_sql_construct(_parse_ctx, "SELECT ", &la_token, end_tok_num, ##__VA_ARGS__);        \
    reset_current_location(_parse_ctx->scanner_ctx_.sql_start_loc, _parse_ctx->scanner_ctx_.sql_end_loc);  \
    if (select_node != NULL) {                                                                             \
      if (NULL == select_node->children_[2] || NULL == select_node->children_[2]->children_[0]) {          \
        YYERROR;                                                                                           \
      } else {                                                                                             \
        node = select_node->children_[2]->children_[0]->children_[0];                                      \
      }                                                                                                    \
    } else {                                                                                               \
      node = NULL;                                                                                         \
    }                                                                                                      \
} while (0)


/* Line 264 of yacc.c  */
#line 424 "../../../src/pl/parser/pl_parser_mysql_mode_tab.c"

#ifdef short
# undef short
#endif

#ifdef YYTYPE_UINT8
typedef YYTYPE_UINT8 yytype_uint8;
#else
typedef unsigned char yytype_uint8;
#endif

#ifdef YYTYPE_INT8
typedef YYTYPE_INT8 yytype_int8;
#elif (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
typedef signed char yytype_int8;
#else
typedef short int yytype_int8;
#endif

#ifdef YYTYPE_UINT16
typedef YYTYPE_UINT16 yytype_uint16;
#else
typedef unsigned short int yytype_uint16;
#endif

#ifdef YYTYPE_INT16
typedef YYTYPE_INT16 yytype_int16;
#else
typedef short int yytype_int16;
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif ! defined YYSIZE_T && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned int
# endif
#endif

#define YYSIZE_MAXIMUM ((YYSIZE_T) -1)

#ifndef YY_
# if YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(e) ((void) (e))
#else
# define YYUSE(e) /* empty */
#endif

/* Identity function, used to suppress warnings about constant conditions.  */
#ifndef lint
# define YYID(n) (n)
#else
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static int
YYID (int yyi)
#else
static int
YYID (yyi)
    int yyi;
#endif
{
  return yyi;
}
#endif

#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#     ifndef _STDLIB_H
#      define _STDLIB_H 1
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's `empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (YYID (0))
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined _STDLIB_H \
       && ! ((defined YYMALLOC || defined malloc) \
	     && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef _STDLIB_H
#    define _STDLIB_H 1
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
	 || (defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL \
	     && defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss_alloc;
  YYSTYPE yyvs_alloc;
  YYLTYPE yyls_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE) + sizeof (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

/* Copy COUNT objects from FROM to TO.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(To, From, Count) \
      __builtin_memcpy (To, From, (Count) * sizeof (*(From)))
#  else
#   define YYCOPY(To, From, Count)		\
      do					\
	{					\
	  YYSIZE_T yyi;				\
	  for (yyi = 0; yyi < (Count); yyi++)	\
	    (To)[yyi] = (From)[yyi];		\
	}					\
      while (YYID (0))
#  endif
# endif

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)				\
    do									\
      {									\
	YYSIZE_T yynewbytes;						\
	YYCOPY (&yyptr->Stack_alloc, Stack, yysize);			\
	Stack = &yyptr->Stack_alloc;					\
	yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
	yyptr += yynewbytes / sizeof (*yyptr);				\
      }									\
    while (YYID (0))

#endif

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  159
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   4697

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  177
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  151
/* YYNRULES -- Number of rules.  */
#define YYNRULES  418
/* YYNRULES -- Number of states.  */
#define YYNSTATES  662

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   423

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,   176,     2,     2,
     168,   169,     2,     2,   172,     2,   173,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,   175,   171,
       2,   174,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    72,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   112,   113,   114,
     115,   116,   117,   118,   119,   120,   121,   122,   123,   124,
     125,   126,   127,   128,   129,   130,   131,   132,   133,   134,
     135,   136,   137,   138,   139,   140,   141,   142,   143,   144,
     145,   146,   147,   148,   149,   150,   151,   152,   153,   154,
     155,   156,   157,   158,   159,   160,   161,   162,   163,   164,
     165,   166,   167,   170
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint16 yyprhs[] =
{
       0,     0,     3,     6,    10,    12,    14,    15,    17,    19,
      21,    23,    25,    27,    29,    31,    33,    35,    37,    39,
      41,    43,    45,    47,    49,    51,    53,    55,    57,    59,
      61,    63,    65,    67,    70,    75,    78,    81,    83,    85,
      87,    89,    92,    95,    98,   101,   104,   108,   111,   112,
     116,   120,   122,   125,   126,   129,   130,   134,   136,   142,
     146,   148,   150,   152,   154,   156,   158,   160,   162,   164,
     166,   168,   170,   172,   174,   176,   178,   180,   182,   184,
     186,   188,   190,   192,   194,   196,   198,   200,   202,   204,
     206,   208,   210,   212,   214,   216,   218,   220,   222,   224,
     226,   228,   230,   232,   234,   236,   238,   240,   242,   244,
     246,   250,   257,   260,   263,   265,   268,   270,   271,   273,
     274,   277,   278,   280,   284,   287,   289,   291,   293,   301,
     307,   308,   310,   314,   322,   329,   336,   342,   346,   351,
     359,   362,   366,   368,   370,   372,   374,   377,   380,   384,
     388,   397,   399,   401,   403,   405,   407,   409,   414,   424,
     433,   445,   456,   457,   462,   467,   469,   471,   473,   474,
     475,   478,   479,   481,   485,   487,   492,   493,   495,   499,
     501,   504,   505,   507,   509,   511,   513,   515,   519,   524,
     530,   532,   535,   537,   539,   542,   545,   548,   551,   554,
     558,   562,   566,   570,   572,   574,   579,   584,   585,   588,
     590,   592,   594,   596,   598,   600,   602,   604,   606,   608,
     610,   612,   614,   616,   618,   620,   625,   630,   634,   641,
     647,   649,   652,   657,   660,   663,   665,   668,   672,   676,
     681,   686,   688,   689,   691,   694,   698,   701,   705,   706,
     710,   715,   721,   728,   734,   736,   738,   740,   744,   746,
     748,   750,   753,   755,   757,   759,   763,   766,   769,   773,
     778,   784,   787,   788,   790,   792,   796,   798,   799,   802,
     803,   806,   807,   809,   813,   814,   819,   826,   833,   838,
     841,   844,   847,   852,   857,   858,   861,   866,   871,   876,
     879,   881,   887,   894,   900,   903,   906,   908,   911,   913,
     915,   918,   924,   932,   940,   942,   946,   948,   950,   952,
     954,   956,   958,   960,   962,   965,   967,   969,   971,   973,
     976,   980,   981,   985,   986,   992,   996,  1000,  1001,  1007,
    1011,  1012,  1016,  1017,  1021,  1022,  1024,  1026,  1028,  1030,
    1032,  1034,  1036,  1038,  1040,  1042,  1045,  1047,  1049,  1051,
    1053,  1055,  1058,  1060,  1061,  1065,  1067,  1071,  1073,  1075,
    1077,  1078,  1080,  1082,  1083,  1085,  1086,  1088,  1089,  1091,
    1093,  1095,  1097,  1099,  1102,  1103,  1105,  1108,  1111,  1113,
    1114,  1118,  1122,  1123,  1125,  1127,  1129,  1130,  1133,  1135,
    1139,  1143,  1145,  1147,  1149,  1151,  1153,  1155,  1157,  1159,
    1161,  1163,  1165,  1167,  1169,  1171,  1173,  1175,  1177
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     178,     0,    -1,   179,     3,    -1,   179,   171,   180,    -1,
     180,    -1,   181,    -1,    -1,   221,    -1,   222,    -1,   285,
      -1,   286,    -1,   241,    -1,   242,    -1,   195,    -1,   208,
      -1,   210,    -1,   183,    -1,   185,    -1,   184,    -1,   318,
      -1,   319,    -1,   196,    -1,   209,    -1,   214,    -1,   220,
      -1,   215,    -1,    67,    -1,    56,    -1,    31,    -1,    16,
      -1,    59,    -1,   182,    -1,    47,    -1,    11,   182,    -1,
      11,    42,    47,   182,    -1,    18,   182,    -1,     4,   182,
      -1,   106,    -1,    63,    -1,    64,    -1,    50,    -1,   168,
      50,    -1,    65,   278,    -1,    65,   268,    -1,    65,   270,
      -1,    65,   269,    -1,     7,   192,   186,    -1,     7,   244,
      -1,    -1,   168,   187,   169,    -1,   187,   172,   188,    -1,
     188,    -1,   190,   189,    -1,    -1,    69,   190,    -1,    -1,
     193,   173,   193,    -1,   193,    -1,   193,   173,   193,   173,
     193,    -1,   193,   173,   193,    -1,   193,    -1,    71,    -1,
     194,    -1,   119,    -1,   120,    -1,   121,    -1,   122,    -1,
     123,    -1,   124,    -1,   125,    -1,   126,    -1,   127,    -1,
     128,    -1,   129,    -1,   130,    -1,   131,    -1,   132,    -1,
     133,    -1,   134,    -1,   135,    -1,   136,    -1,   137,    -1,
     138,    -1,   139,    -1,   140,    -1,   141,    -1,   142,    -1,
     143,    -1,   144,    -1,   145,    -1,   146,    -1,   147,    -1,
     148,    -1,   149,    -1,   150,    -1,   151,    -1,   152,    -1,
     153,    -1,   154,    -1,   155,    -1,   156,    -1,   157,    -1,
     158,    -1,   159,    -1,   160,    -1,   161,    -1,   162,    -1,
     163,    -1,   164,    -1,   165,    -1,    11,   201,   196,    -1,
     154,   191,   200,   202,   139,   207,    -1,   120,    12,    -1,
     120,   138,    -1,   197,    -1,   199,   198,    -1,   198,    -1,
      -1,   199,    -1,    -1,    42,    47,    -1,    -1,   203,    -1,
     203,   204,   171,    -1,   204,   171,    -1,   262,    -1,   205,
      -1,   206,    -1,   142,   193,   168,   227,   169,    49,   274,
      -1,    44,   193,   168,   227,   169,    -1,    -1,   193,    -1,
      11,   201,   209,    -1,   154,   123,   191,   212,   259,   139,
     207,    -1,   154,   123,   191,   259,   139,   207,    -1,   154,
     123,   191,   212,   139,   207,    -1,   154,   123,   191,   139,
     207,    -1,    18,   154,   191,    -1,    18,   154,   123,   191,
      -1,   155,   144,   168,   124,   172,   193,   169,    -1,   213,
     171,    -1,   212,   213,   171,    -1,   262,    -1,   205,    -1,
     206,    -1,   211,    -1,   205,   240,    -1,   206,   239,    -1,
      11,   223,   215,    -1,    58,   191,   216,    -1,   217,   218,
      41,   191,    25,    19,   158,   219,    -1,     5,    -1,   119,
      -1,    31,    -1,    16,    -1,    59,    -1,   244,    -1,    18,
      58,   287,   191,    -1,    11,   223,    44,   191,   168,   227,
     169,   236,   239,    -1,    11,   223,    44,   191,   168,   227,
     169,   239,    -1,    11,   223,   142,   191,   168,   230,   169,
     157,   274,   236,   240,    -1,    11,   223,   142,   191,   168,
     230,   169,   157,   274,   240,    -1,    -1,   138,   174,   224,
     225,    -1,   138,   174,    12,   226,    -1,    72,    -1,    71,
      -1,    78,    -1,    -1,    -1,   168,   169,    -1,    -1,   228,
      -1,   228,   172,   229,    -1,   229,    -1,   233,   193,   234,
     276,    -1,    -1,   231,    -1,   231,   172,   232,    -1,   232,
      -1,   193,   234,    -1,    -1,    28,    -1,    43,    -1,    30,
      -1,   274,    -1,   193,    -1,   193,   173,   193,    -1,   173,
     193,   173,   193,    -1,   193,   173,   193,   173,   193,    -1,
     237,    -1,   236,   237,    -1,   238,    -1,    17,    -1,    40,
      17,    -1,   129,    72,    -1,   147,    52,    -1,   151,    52,
      -1,   134,    52,    -1,    45,    52,   137,    -1,    39,    52,
     137,    -1,    52,   161,   138,    -1,    52,   161,   145,    -1,
     244,    -1,   247,    -1,     4,    44,   191,   243,    -1,     4,
     142,   191,   243,    -1,    -1,   243,   238,    -1,   245,    -1,
     246,    -1,   181,    -1,   247,    -1,   283,    -1,   284,    -1,
     268,    -1,   270,    -1,   269,    -1,   248,    -1,   250,    -1,
     254,    -1,   256,    -1,   280,    -1,   281,    -1,   282,    -1,
      27,   249,   139,    27,    -1,   279,    57,   259,   253,    -1,
     279,    57,   259,    -1,     8,   279,   251,   253,   139,     8,
      -1,     8,   279,   251,   139,     8,    -1,   252,    -1,   251,
     252,    -1,    61,   279,    57,   259,    -1,    21,   249,    -1,
      20,   259,    -1,   255,    -1,   121,   139,    -1,   121,   260,
     139,    -1,   121,   259,   139,    -1,   121,   260,   259,   139,
      -1,   257,   175,   255,   258,    -1,   193,    -1,    -1,   257,
      -1,   244,   171,    -1,   259,   244,   171,    -1,   260,   171,
      -1,   261,   262,   171,    -1,    -1,   261,   262,   171,    -1,
      14,   273,   274,   275,    -1,    14,   193,     9,    25,   266,
      -1,    14,   263,   143,    25,   264,   244,    -1,    14,   193,
      13,    25,   183,    -1,    23,    -1,    10,    -1,   265,    -1,
     264,   172,   265,    -1,   266,    -1,    71,    -1,    55,    -1,
      40,   141,    -1,    53,    -1,   300,    -1,   267,    -1,    54,
     272,    72,    -1,   153,   193,    -1,   127,   193,    -1,    24,
     193,   271,    -1,    24,    26,   193,   271,    -1,    24,   150,
      26,   193,   271,    -1,    32,   278,    -1,    -1,   165,    -1,
     193,    -1,   273,   172,   193,    -1,   288,    -1,    -1,    15,
     279,    -1,    -1,    15,   277,    -1,    -1,   279,    -1,   278,
     172,   279,    -1,    -1,    38,   259,   139,    38,    -1,    62,
     279,    65,   259,   139,    62,    -1,    46,   259,    66,   279,
     139,    46,    -1,   257,   175,   280,   258,    -1,    49,   279,
      -1,    34,   257,    -1,    35,   257,    -1,    18,    44,   287,
     191,    -1,    18,   142,   287,   191,    -1,    -1,    27,    22,
      -1,   290,   294,   309,   310,    -1,   291,   296,   309,   310,
      -1,    89,   297,   309,   310,    -1,   292,   299,    -1,   293,
      -1,    97,   308,   311,   314,   317,    -1,    97,   118,   305,
     311,   314,   317,    -1,    99,   305,   311,   314,   317,    -1,
     100,   308,    -1,   101,   305,    -1,    72,    -1,    91,   295,
      -1,   108,    -1,   109,    -1,   303,   304,    -1,   302,   304,
     311,   314,   317,    -1,   111,   168,   306,   169,   311,   314,
     317,    -1,   106,   168,   306,   169,   311,   314,   317,    -1,
     289,    -1,   289,   176,   159,    -1,   192,    -1,    81,    -1,
      82,    -1,    83,    -1,    84,    -1,    85,    -1,    86,    -1,
      87,    -1,    87,    88,    -1,    92,    -1,    93,    -1,    94,
      -1,    95,    -1,    96,   298,    -1,   168,    73,   169,    -1,
      -1,   168,    73,   169,    -1,    -1,   168,    73,   172,    73,
     169,    -1,   168,    73,   169,    -1,   168,    74,   169,    -1,
      -1,   168,    73,   172,    73,   169,    -1,   168,    73,   169,
      -1,    -1,   168,    73,   169,    -1,    -1,   168,    73,   169,
      -1,    -1,    73,    -1,    74,    -1,   300,    -1,    76,    -1,
      75,    -1,    79,    -1,   112,    -1,    98,    -1,   113,    -1,
     114,    -1,    37,    99,    -1,    37,    -1,   115,    -1,   110,
      -1,   116,    -1,   117,    -1,    37,   101,    -1,   305,    -1,
      -1,   168,   300,   169,    -1,   307,    -1,   306,   172,   307,
      -1,    72,    -1,    75,    -1,   305,    -1,    -1,   102,    -1,
     103,    -1,    -1,   104,    -1,    -1,   100,    -1,    -1,   193,
      -1,    72,    -1,   193,    -1,    72,    -1,   100,    -1,   315,
     313,    -1,    -1,   107,    -1,    97,   106,    -1,   105,   312,
      -1,   316,    -1,    -1,    51,   321,   322,    -1,    48,   320,
     322,    -1,    -1,   321,    -1,   193,    -1,   267,    -1,    -1,
     106,   323,    -1,   324,    -1,   323,   172,   324,    -1,   327,
     174,   325,    -1,   301,    -1,   326,    -1,   235,    -1,    72,
      -1,    77,    -1,    78,    -1,   126,    -1,   162,    -1,   130,
      -1,   133,    -1,   131,    -1,   125,    -1,   160,    -1,   163,
      -1,   128,    -1,   136,    -1,   148,    -1,   149,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   224,   224,   233,   237,   252,   266,   269,   270,   271,
     272,   273,   274,   275,   276,   277,   278,   279,   280,   281,
     282,   283,   284,   285,   286,   287,   296,   297,   298,   299,
     300,   304,   311,   318,   325,   332,   339,   346,   351,   359,
     367,   374,   384,   439,   440,   441,   445,   449,   460,   461,
     468,   475,   479,   496,   497,   506,   518,   522,   529,   533,
     537,   544,   545,   552,   553,   554,   555,   556,   557,   558,
     559,   560,   561,   562,   563,   564,   565,   566,   567,   568,
     569,   570,   571,   572,   573,   574,   575,   576,   577,   578,
     579,   580,   581,   582,   583,   584,   585,   586,   587,   588,
     589,   590,   591,   592,   593,   594,   595,   596,   597,   598,
     607,   622,   639,   644,   652,   656,   660,   664,   665,   672,
     673,   677,   678,   685,   689,   692,   693,   694,   698,   711,
     724,   725,   734,   751,   767,   782,   796,   816,   821,   829,
     836,   837,   844,   845,   846,   847,   848,   858,   876,   886,
     900,   907,   908,   912,   913,   914,   918,   943,   956,   968,
     981,   993,  1006,  1007,  1011,  1022,  1026,  1032,  1037,  1044,
    1045,  1049,  1050,  1057,  1061,  1065,  1074,  1075,  1082,  1086,
    1090,  1098,  1099,  1100,  1101,  1105,  1108,  1112,  1116,  1120,
    1127,  1128,  1135,  1136,  1140,  1144,  1145,  1146,  1147,  1148,
    1149,  1150,  1155,  1163,  1167,  1176,  1183,  1190,  1191,  1198,
    1203,  1211,  1223,  1228,  1233,  1238,  1243,  1248,  1256,  1260,
    1264,  1268,  1272,  1276,  1280,  1287,  1291,  1297,  1306,  1312,
    1321,  1322,  1329,  1344,  1350,  1359,  1363,  1367,  1373,  1379,
    1390,  1397,  1401,  1402,  1409,  1410,  1417,  1421,  1429,  1430,
    1437,  1444,  1449,  1459,  1476,  1477,  1481,  1482,  1489,  1490,
    1491,  1492,  1493,  1497,  1501,  1508,  1516,  1523,  1530,  1534,
    1538,  1545,  1554,  1555,  1558,  1559,  1566,  1570,  1571,  1578,
    1579,  1598,  1604,  1605,  1612,  1619,  1625,  1631,  1640,  1647,
    1655,  1662,  1669,  1677,  1685,  1686,  1690,  1696,  1710,  1720,
    1725,  1729,  1745,  1751,  1757,  1766,  1772,  1778,  1783,  1789,
    1795,  1805,  1815,  1823,  1831,  1848,  1855,  1861,  1862,  1863,
    1864,  1865,  1869,  1870,  1871,  1875,  1876,  1877,  1881,  1882,
    1886,  1887,  1891,  1892,  1896,  1902,  1908,  1916,  1920,  1935,
    1947,  1956,  1957,  1961,  1966,  1972,  1977,  1985,  1986,  1987,
    1988,  1992,  1993,  1994,  1995,  1996,  1997,  2001,  2002,  2003,
    2004,  2005,  2009,  2010,  2013,  2051,  2055,  2062,  2066,  2073,
    2078,  2084,  2085,  2086,  2090,  2091,  2095,  2100,  2104,  2110,
    2118,  2125,  2131,  2144,  2151,  2155,  2159,  2166,  2176,  2180,
    2184,  2191,  2198,  2199,  2203,  2204,  2208,  2209,  2216,  2217,
    2224,  2232,  2233,  2234,  2235,  2239,  2240,  2244,  2245,  2246,
    2247,  2248,  2249,  2250,  2251,  2252,  2253,  2254,  2255
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "END_P", "ALTER", "BEFORE", "BY", "CALL",
  "CASE", "CONDITION", "CONTINUE", "CREATE", "CURRENT_USER", "CURSOR",
  "DECLARE", "DEFAULT", "DELETE", "DETERMINISTIC", "DROP", "EACH", "ELSE",
  "ELSEIF", "EXISTS", "EXIT", "FETCH", "FOR", "FROM", "IF", "IN", "INDEX",
  "INOUT", "INSERT", "INTO", "IS", "ITERATE", "LEAVE", "LIMIT", "LONG",
  "LOOP", "MODIFIES", "NOT", "ON", "OR", "OUT", "PROCEDURE", "READS",
  "REPEAT", "REPLACE", "RESIGNAL", "RETURN", "SELECT", "SIGNAL", "SQL",
  "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "TABLE", "THEN", "TRIGGER",
  "UPDATE", "USING", "WHEN", "WHILE", "COMMIT", "ROLLBACK", "DO", "UNTIL",
  "SQL_KEYWORD", "SQL_TOKEN", "PARAM_ASSIGN_OPERATOR",
  "PARSER_SYNTAX_ERROR", "IDENT", "STRING", "INTNUM", "DECIMAL_VAL",
  "HEX_STRING_VALUE", "DATE_VALUE", "SYSTEM_VARIABLE", "USER_VARIABLE",
  "NULLX", "USER_NAME", "TINYINT", "SMALLINT", "MEDIUMINT", "INTEGER",
  "BIGINT", "FLOAT", "DOUBLE", "PRECISION", "NUMBER", "NUMERIC", "BIT",
  "DATETIME", "TIMESTAMP", "TIME", "DATE", "YEAR", "CHARACTER", "TEXT",
  "VARCHAR", "BINARY", "VARBINARY", "UNSIGNED", "SIGNED", "ZEROFILL",
  "COLLATE", "SET", "CHARSET", "BOOL", "BOOLEAN", "BLOB", "ENUM",
  "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "TINYBLOB", "MEDIUMBLOB",
  "LONGBLOB", "VARYING", "AFTER", "AUTHID", "BEGIN_KEY", "BINARY_INTEGER",
  "BODY", "C", "CATALOG_NAME", "CLASS_ORIGIN", "CLOSE", "COLUMN_NAME",
  "COMMENT", "CONSTRAINT_CATALOG", "CONSTRAINT_NAME", "CONSTRAINT_ORIGIN",
  "CONSTRAINT_SCHEMA", "CONTAINS", "COUNT", "CURSOR_NAME", "DATA",
  "DEFINER", "END_KEY", "EXTEND", "FOUND", "FUNCTION", "HANDLER",
  "INTERFACE", "INVOKER", "JSON", "LANGUAGE", "MESSAGE_TEXT",
  "MYSQL_ERRNO", "NEXT", "NO", "OF", "OPEN", "PACKAGE", "PRAGMA", "RECORD",
  "RETURNS", "ROW", "ROWTYPE", "SCHEMA_NAME", "SECURITY",
  "SUBCLASS_ORIGIN", "TABLE_NAME", "TYPE", "VALUE", "LOWER_PARENS",
  "PARENS", "'('", "')'", "DECLARATION", "';'", "','", "'.'", "'='", "':'",
  "'%'", "$accept", "stmt_block", "stmt_list", "stmt", "outer_stmt",
  "sql_keyword", "sql_stmt", "do_sp_stmt", "call_sp_stmt",
  "opt_sp_cparam_list", "opt_sp_cparams", "sp_cparam",
  "opt_sp_cparam_with_assign", "opt_cexpr", "sp_name", "sp_call_name",
  "ident", "unreserved_keyword", "create_package_stmt", "package_block",
  "invoke_right", "proc_clause", "proc_clause_list", "opt_proc_clause",
  "opt_replace", "opt_package_stmts", "package_stmts", "package_stmt",
  "func_decl", "proc_decl", "opt_tail_package_name",
  "create_package_body_stmt", "package_body_block", "drop_package_stmt",
  "interface_pragma", "decl_stmt_ext_list", "decl_stmt_ext",
  "create_trigger_stmt", "plsql_trigger_source", "trigger_definition",
  "trigger_time", "trigger_event", "trigger_body", "drop_trigger_stmt",
  "create_procedure_stmt", "create_function_stmt", "opt_sp_definer",
  "user", "opt_host_name", "opt_parentheses", "opt_sp_param_list",
  "sp_param_list", "sp_param", "opt_sp_fparam_list", "sp_fparam_list",
  "sp_fparam", "opt_sp_inout", "param_type", "simple_ident",
  "sp_create_chistics", "sp_create_chistic", "sp_chistic",
  "procedure_body", "function_body", "alter_procedure_stmt",
  "alter_function_stmt", "opt_sp_alter_chistics", "sp_proc_stmt",
  "sp_proc_outer_statement", "sp_proc_inner_statement",
  "sp_proc_independent_statement", "sp_proc_stmt_if", "sp_if",
  "sp_proc_stmt_case", "sp_when_list", "sp_when", "sp_elseifs",
  "sp_unlabeled_block", "sp_block_content", "sp_labeled_block",
  "label_ident", "opt_sp_label", "sp_proc_stmts", "sp_decls",
  "opt_sp_decls", "sp_decl", "sp_handler_type", "sp_hcond_list",
  "sp_hcond", "sp_cond", "sqlstate", "sp_proc_stmt_open",
  "sp_proc_stmt_close", "sp_proc_stmt_fetch", "into_clause", "opt_value",
  "sp_decl_idents", "sp_data_type", "opt_sp_decl_default",
  "opt_param_default", "default_expr", "expr_list", "expr",
  "sp_unlabeled_control", "sp_labeled_control", "sp_proc_stmt_return",
  "sp_proc_stmt_iterate", "sp_proc_stmt_leave", "drop_procedure_stmt",
  "drop_function_stmt", "opt_if_exists", "scalar_data_type",
  "pl_obj_access_ref", "int_type_i", "float_type_i", "datetime_type_i",
  "date_year_type_i", "opt_int_length_i", "opt_bit_length_i",
  "opt_float_precision", "opt_number_precision", "opt_year_i",
  "opt_datetime_fsp_i", "number_literal", "literal", "text_type_i",
  "blob_type_i", "opt_string_length_i_v2", "string_length_i",
  "string_list", "text_string", "opt_string_length_i", "opt_unsigned_i",
  "opt_zerofill_i", "opt_binary", "collation_name", "charset_name",
  "opt_charset", "charset_key", "collation", "opt_collation",
  "signal_stmt", "resignal_stmt", "opt_signal_value", "signal_value",
  "opt_set_signal_information", "signal_information_item_list",
  "signal_information_item", "signal_allowed_expr", "variable",
  "scond_info_item_name", 0
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[YYLEX-NUM] -- Internal token number corresponding to
   token YYLEX-NUM.  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     295,   296,   297,   298,   299,   300,   301,   302,   303,   304,
     305,   306,   307,   308,   309,   310,   311,   312,   313,   314,
     315,   316,   317,   318,   319,   320,   321,   322,   323,   324,
     325,   326,   327,   328,   329,   330,   331,   332,   333,   334,
     335,   336,   337,   338,   339,   340,   341,   342,   343,   344,
     345,   346,   347,   348,   349,   350,   351,   352,   353,   354,
     355,   356,   357,   358,   359,   360,   361,   362,   363,   364,
     365,   366,   367,   368,   369,   370,   371,   372,   373,   374,
     375,   376,   377,   378,   379,   380,   381,   382,   383,   384,
     385,   386,   387,   388,   389,   390,   391,   392,   393,   394,
     395,   396,   397,   398,   399,   400,   401,   402,   403,   404,
     405,   406,   407,   408,   409,   410,   411,   412,   413,   414,
     415,   416,   417,   418,   419,   420,   421,   422,    40,    41,
     423,    59,    44,    46,    61,    58,    37
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   177,   178,   179,   179,   180,   180,   181,   181,   181,
     181,   181,   181,   181,   181,   181,   181,   181,   181,   181,
     181,   181,   181,   181,   181,   181,   182,   182,   182,   182,
     182,   183,   183,   183,   183,   183,   183,   183,   183,   183,
     183,   183,   184,   184,   184,   184,   185,   185,   186,   186,
     187,   187,   188,   189,   189,   190,   191,   191,   192,   192,
     192,   193,   193,   194,   194,   194,   194,   194,   194,   194,
     194,   194,   194,   194,   194,   194,   194,   194,   194,   194,
     194,   194,   194,   194,   194,   194,   194,   194,   194,   194,
     194,   194,   194,   194,   194,   194,   194,   194,   194,   194,
     194,   194,   194,   194,   194,   194,   194,   194,   194,   194,
     195,   196,   197,   197,   198,   199,   199,   200,   200,   201,
     201,   202,   202,   203,   203,   204,   204,   204,   205,   206,
     207,   207,   208,   209,   209,   209,   209,   210,   210,   211,
     212,   212,   213,   213,   213,   213,   213,   213,   214,   215,
     216,   217,   217,   218,   218,   218,   219,   220,   221,   221,
     222,   222,   223,   223,   223,   224,   224,   225,   225,   226,
     226,   227,   227,   228,   228,   229,   230,   230,   231,   231,
     232,   233,   233,   233,   233,   234,   235,   235,   235,   235,
     236,   236,   237,   237,   237,   238,   238,   238,   238,   238,
     238,   238,   238,   239,   240,   241,   242,   243,   243,   244,
     244,   245,   246,   246,   246,   246,   246,   246,   247,   247,
     247,   247,   247,   247,   247,   248,   249,   249,   250,   250,
     251,   251,   252,   253,   253,   254,   255,   255,   255,   255,
     256,   257,   258,   258,   259,   259,   260,   260,   261,   261,
     262,   262,   262,   262,   263,   263,   264,   264,   265,   265,
     265,   265,   265,   266,   266,   267,   268,   269,   270,   270,
     270,   271,   272,   272,   273,   273,   274,   275,   275,   276,
     276,   277,   278,   278,   279,   280,   280,   280,   281,   282,
     283,   284,   285,   286,   287,   287,   288,   288,   288,   288,
     288,   288,   288,   288,   288,   288,   288,   288,   288,   288,
     288,   288,   288,   288,   288,   288,   289,   290,   290,   290,
     290,   290,   291,   291,   291,   292,   292,   292,   293,   293,
     294,   294,   295,   295,   296,   296,   296,   296,   297,   297,
     297,   298,   298,   299,   299,   300,   300,   301,   301,   301,
     301,   302,   302,   302,   302,   302,   302,   303,   303,   303,
     303,   303,   304,   304,   305,   306,   306,   307,   307,   308,
     308,   309,   309,   309,   310,   310,   311,   311,   312,   312,
     313,   313,   313,   314,   314,   315,   315,   316,   317,   317,
     318,   319,   320,   320,   321,   321,   322,   322,   323,   323,
     324,   325,   325,   325,   325,   326,   326,   327,   327,   327,
     327,   327,   327,   327,   327,   327,   327,   327,   327
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     2,     3,     1,     1,     0,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     2,     4,     2,     2,     1,     1,     1,
       1,     2,     2,     2,     2,     2,     3,     2,     0,     3,
       3,     1,     2,     0,     2,     0,     3,     1,     5,     3,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       3,     6,     2,     2,     1,     2,     1,     0,     1,     0,
       2,     0,     1,     3,     2,     1,     1,     1,     7,     5,
       0,     1,     3,     7,     6,     6,     5,     3,     4,     7,
       2,     3,     1,     1,     1,     1,     2,     2,     3,     3,
       8,     1,     1,     1,     1,     1,     1,     4,     9,     8,
      11,    10,     0,     4,     4,     1,     1,     1,     0,     0,
       2,     0,     1,     3,     1,     4,     0,     1,     3,     1,
       2,     0,     1,     1,     1,     1,     1,     3,     4,     5,
       1,     2,     1,     1,     2,     2,     2,     2,     2,     3,
       3,     3,     3,     1,     1,     4,     4,     0,     2,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     4,     4,     3,     6,     5,
       1,     2,     4,     2,     2,     1,     2,     3,     3,     4,
       4,     1,     0,     1,     2,     3,     2,     3,     0,     3,
       4,     5,     6,     5,     1,     1,     1,     3,     1,     1,
       1,     2,     1,     1,     1,     3,     2,     2,     3,     4,
       5,     2,     0,     1,     1,     3,     1,     0,     2,     0,
       2,     0,     1,     3,     0,     4,     6,     6,     4,     2,
       2,     2,     4,     4,     0,     2,     4,     4,     4,     2,
       1,     5,     6,     5,     2,     2,     1,     2,     1,     1,
       2,     5,     7,     7,     1,     3,     1,     1,     1,     1,
       1,     1,     1,     1,     2,     1,     1,     1,     1,     2,
       3,     0,     3,     0,     5,     3,     3,     0,     5,     3,
       0,     3,     0,     3,     0,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     2,     1,     1,     1,     1,
       1,     2,     1,     0,     3,     1,     3,     1,     1,     1,
       0,     1,     1,     0,     1,     0,     1,     0,     1,     1,
       1,     1,     1,     2,     0,     1,     2,     2,     1,     0,
       3,     3,     0,     1,     1,     1,     0,     2,     1,     3,
       3,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
       6,     0,     0,   162,    29,     0,    28,    32,   392,    40,
       0,    27,     0,    30,    38,    39,   284,    26,    37,     0,
       0,     0,     0,     4,     5,    31,    16,    18,    17,    13,
      21,    14,    22,    15,    23,    25,    24,     7,     8,    11,
      12,     9,    10,    19,    20,     0,     0,    36,   284,     0,
     284,     0,     0,     0,     0,   284,   284,    61,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    72,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   211,    48,    60,    62,    47,
     209,   210,   212,   218,   219,   220,   235,   221,     0,   215,
     217,   216,   222,   223,   224,   213,   214,     0,     0,    33,
       0,     0,   294,   294,   294,     0,    35,   272,    65,    71,
      97,    98,   394,   395,   396,   393,   396,     0,    57,     0,
       0,    43,    45,    44,    42,   282,    67,   117,    41,     1,
       2,     6,   207,   207,     0,     0,    94,     0,     0,     0,
     241,   290,   291,     0,     0,     0,   289,     0,   236,     0,
       0,     0,   267,   266,    55,    46,     0,     0,   120,     0,
     110,   132,     0,     0,   148,     0,     0,     0,     0,    67,
     137,   273,     0,     0,   391,   390,   151,   152,   149,     0,
       0,   284,     0,     0,   114,   116,   118,   121,     3,   205,
     206,   284,     0,   230,     0,     0,   284,   268,     0,     0,
     244,    83,     0,   284,     0,   238,   237,   246,     0,     0,
       0,     0,    51,    53,    59,   248,   242,   242,    34,   169,
     166,   165,   168,     0,     0,   295,   292,   157,   293,   138,
     265,   412,   407,   415,   409,   411,   410,   416,   417,   418,
     413,   408,   414,   397,   398,     0,   154,   153,   155,     0,
      56,   283,     0,   130,    86,    99,   143,   144,   145,     0,
       0,     0,   142,   112,   113,   115,     0,     0,   122,     0,
     126,   127,   125,     0,     0,     0,     0,     0,     0,     0,
     208,     0,     0,   284,     0,   231,     0,   269,     0,   271,
     225,   227,   285,   245,     0,     0,   239,   255,   254,   274,
       0,     0,   247,    49,    55,    55,    52,     0,   243,   240,
     288,     0,   164,   167,   163,   181,   176,     0,     0,     0,
       0,   131,   136,     0,     0,   146,   204,   147,   203,   130,
       0,     0,   140,   130,   130,     0,   124,     0,     0,     0,
     195,   198,   196,   197,     0,   234,   233,   229,     0,   270,
     226,     0,    83,     0,     0,     0,   356,   306,   317,   318,
     319,   320,   321,   322,   323,   340,   333,   325,   326,   327,
     328,   342,   370,   352,     0,   370,     0,     0,   308,   309,
     358,     0,   351,   353,   354,   357,   359,   360,     0,   316,
      60,   277,   276,   314,   331,   337,   344,   300,   363,   363,
      50,    54,    58,   170,   182,   184,   183,     0,   172,   174,
       0,     0,     0,   177,   179,   399,   404,   345,   346,   349,
     348,   405,   406,   350,     0,   186,   403,   347,   401,   400,
     402,     0,   181,   181,     0,   135,   141,   130,   134,   111,
     123,   200,   199,   201,   202,   232,   228,   287,   286,     0,
       0,     0,   355,   361,   324,     0,   373,     0,   307,     0,
     329,     0,     0,   369,   377,   377,   304,   305,     0,     0,
     275,   284,   250,     0,     0,   373,     0,   373,     0,   299,
     377,   362,   310,     0,   181,     0,   180,   185,     0,     0,
       0,     0,     0,     0,     0,     0,   133,   251,   264,   263,
       0,     0,     0,   253,     0,   262,   260,   259,     0,   256,
     258,     0,   371,   372,   375,     0,     0,   377,     0,   376,
     384,   384,   367,   368,     0,   365,     0,   278,   315,     0,
     375,     0,     0,   375,     0,   384,   193,     0,    73,    78,
      91,    95,     0,   190,   192,   159,   173,   279,     0,   178,
       0,   187,     0,   129,     0,     0,     0,   261,     0,   252,
     339,     0,   374,   298,   332,   341,   384,   364,     0,   385,
     389,     0,   389,   377,     0,   377,   330,   296,   335,     0,
     336,   297,   343,   389,   194,   191,   158,   281,   175,     0,
     188,     0,     0,     0,     0,     0,   257,     0,   389,   386,
       0,   388,   301,   381,   382,   380,   383,   303,   384,   366,
     384,     0,   311,   280,     0,   161,   189,   150,   156,   128,
     139,   338,   302,   379,   378,   387,   389,   389,   334,   160,
     313,   312
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    21,    22,    23,   105,    25,    26,    27,    28,   185,
     241,   242,   336,   243,   157,   419,   170,   108,    29,    30,
     214,   215,   216,   217,   130,   297,   298,   299,   286,   287,
     352,    31,    32,    33,   288,   289,   290,    34,    35,   208,
     209,   279,   647,    36,    37,    38,   131,   252,   344,   342,
     437,   438,   439,   442,   443,   444,   440,   516,   456,   572,
     573,   574,   357,   355,    39,    40,   219,   173,   110,   111,
     112,   113,   168,   114,   222,   223,   316,   115,   116,   117,
     118,   339,   179,   180,   181,   292,   330,   538,   539,   540,
     528,   119,   120,   121,   227,   202,   331,   517,   502,   618,
     643,   154,   155,   122,   123,   124,   125,   126,    41,    42,
     196,   422,   423,   424,   425,   426,   427,   505,   488,   507,
     486,   490,   509,   529,   458,   428,   429,   510,   493,   554,
     555,   494,   544,   593,   550,   655,   636,   600,   601,   631,
     632,    43,    44,   144,   145,   204,   273,   274,   459,   460,
     275
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -552
static const yytype_int16 yypact[] =
{
      19,   169,  1842,    29,  -552,    93,  -552,  -552,  3916,  -552,
    3916,  -552,  4295,  -552,  -552,  -552,    -8,  -552,  -552,  4390,
       7,    73,    18,  -552,  -552,  -552,  -552,  -552,  -552,  -552,
    -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,
    -552,  -552,  -552,  -552,  -552,  4295,  4295,  -552,  -552,  3546,
    -552,  4295,  4295,  1842,  1842,  -552,  -552,  -552,  -552,  -552,
    1356,  -552,  -552,  -552,  -552,  -552,  4295,  -552,  -552,  -552,
    -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,
    -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,
    -552,  -552,  4295,  4390,  -552,  -552,  -552,  -552,  -552,  -552,
    -552,  -552,  -552,  -552,  -552,  -552,   -73,    79,  -552,  -552,
    -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,   -72,  -552,
    -552,  -552,  -552,  -552,  -552,  -552,  -552,    65,   -45,  -552,
       3,   -12,   138,   138,   138,  4437,  -552,    21,  -552,  -552,
    -552,  -552,  -552,  -552,    66,  -552,    66,    17,    53,  4295,
    4295,  -552,  -552,  -552,    59,  -552,  4532,   123,  -552,  -552,
    -552,    19,  -552,  -552,   118,  4295,   222,   233,   135,   219,
    -552,  -552,  -552,   112,  2004,  1518,  -552,   227,   121,  2166,
     708,   287,  -552,  -552,  -552,  -552,  4295,    72,   203,    87,
    -552,  -552,  4295,  4295,  -552,   280,  4295,  4295,  4295,  4295,
    -552,  -552,   232,   188,  -552,  -552,  -552,  -552,  -552,    43,
    4295,  -552,  1032,    12,  -552,  -552,   123,    14,  -552,   116,
     116,  -552,    77,  -552,   233,  4295,  -552,  -552,   278,  1842,
    -552,   268,   137,  -552,  1842,   121,   121,  -552,  2328,  3499,
     139,  -107,  -552,   240,   142,  2490,  4295,  4295,  -552,   144,
    -552,  -552,   239,   152,   154,  -552,  -552,  -552,  -552,  -552,
    -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,
    -552,  -552,  -552,   153,  -552,   156,  -552,  -552,  -552,   285,
    -552,  -552,  4295,  3963,  4295,   184,  3343,  1842,  -552,  1194,
     160,  2652,  -552,  -552,  -552,  -552,  4295,   193,    14,   162,
    -552,  -552,  -552,   282,   286,   178,   269,   288,   290,   291,
    -552,   292,  1842,  -552,   344,  -552,   214,  -552,   233,    59,
    -552,  1680,  -552,  -552,   215,  2814,   121,  -552,  -552,   209,
     213,  3675,   345,  -552,  -552,  -552,  -552,  4295,  -552,  -552,
    -552,   189,  -552,  -552,  -552,    -3,  4295,   188,  4058,  4295,
     192,  -552,  -552,   196,   197,  -552,  -552,  -552,  -552,  3963,
     195,  2976,  -552,  3963,  4295,   200,  -552,   230,   231,   -34,
    -552,  -552,  -552,  -552,  1842,  3138,  -552,  -552,   364,  -552,
    -552,   327,   312,   350,   351,   353,   167,  -552,  -552,  -552,
    -552,  -552,  -552,  -552,   293,   211,   216,  -552,  -552,  -552,
    -552,   217,   -63,  -552,   218,   218,   218,   220,  -552,  -552,
    -552,   221,  -552,  -552,  -552,  -552,  -552,  -552,  4295,  -552,
     207,   367,  -552,   223,   225,   226,   228,  -552,   218,   218,
    -552,  -552,  -552,  -552,  -552,  -552,  -552,   229,   234,  -552,
    4295,  3804,   235,   236,  -552,  -552,  -552,  -552,  -552,  -552,
    -552,  -552,  -552,  -552,  4295,   210,  -552,  -552,  -552,  -552,
    -552,   362,    -3,    -3,   266,  -552,  -552,  3963,  -552,  -552,
    -552,  -552,  -552,  -552,  -552,  3138,  -552,  -552,  -552,    27,
      76,   224,  -552,  -552,  -552,   318,   108,   319,  -552,   322,
    -552,   218,   187,  -552,   297,   297,  -552,  -552,    45,    45,
    -552,  -552,  -552,   241,   328,   108,   212,   108,   329,  -552,
     297,  -552,  -552,   870,    78,  3804,  -552,  -552,   246,  4295,
     237,  4295,   386,   238,   242,   243,  -552,  -552,  -552,  -552,
     203,   190,   203,  -552,   271,  -552,  -552,  -552,   546,  -552,
    -552,    48,  -552,  -552,   305,   244,   245,   297,   248,  -552,
     -18,   -18,  -552,  -552,    61,  -552,    69,  -552,  -552,   249,
     305,    70,   250,   305,   251,   -18,  -552,   404,   269,   288,
     290,   291,   870,  -552,  -552,  -552,  -552,   407,  3804,  -552,
    4295,   252,   265,  -552,   375,  4295,   379,  -552,   224,  -552,
    -552,   354,  -552,  -552,  -552,  -552,   -18,  -552,   323,  -552,
     325,  4153,   325,   297,    45,   297,  -552,  -552,  -552,   355,
    -552,  -552,  -552,   325,  -552,  -552,  -552,  -552,  -552,  3296,
    -552,  4295,  1842,  3804,   262,   203,  -552,   263,   325,  -552,
    4248,  -552,  -552,  -552,  -552,  -552,  -552,  -552,   -18,  -552,
     -18,   264,  -552,  -552,  3296,  -552,  -552,  -552,  -552,  -552,
    -552,  -552,  -552,  -552,  -552,  -552,   325,   325,  -552,  -552,
    -552,  -552
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -552,  -552,  -552,   273,    13,     0,   -44,  -552,  -552,  -552,
    -552,   101,  -552,   102,     6,   438,    -2,  -552,  -552,   311,
    -552,   247,  -552,  -552,  -552,  -552,  -552,   145,  -176,  -154,
    -325,  -552,   314,  -552,  -552,  -552,   157,  -552,   316,  -552,
    -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,
    -175,  -552,   -69,  -552,  -552,   -77,  -552,   -67,  -552,  -170,
    -530,    71,  -457,  -551,  -552,  -552,   294,     2,  -552,  -552,
    -274,  -552,   140,  -552,  -552,   253,   129,  -552,   267,  -552,
     -32,   204,    -5,  -552,  -552,  -145,  -552,  -552,  -133,   -23,
     261,   442,   443,   445,  -191,  -552,  -552,  -320,  -552,  -552,
    -552,   254,   -41,   275,  -552,  -552,  -552,  -552,  -552,  -552,
     166,  -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,  -552,
    -552,  -552,  -552,  -346,  -552,  -552,  -552,    35,  -375,   -33,
    -137,    63,  -232,  -432,  -434,  -552,  -552,  -387,  -552,  -552,
    -401,  -552,  -552,  -552,   459,   324,  -552,   124,  -552,  -552,
    -552
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -250
static const yytype_int16 yytable[] =
{
     107,    47,   457,   129,   109,   136,   142,   164,   142,   169,
     148,   421,   356,    24,   176,   177,    49,   148,   147,   171,
     172,   160,   206,     1,   293,   434,     2,   435,   239,   495,
       3,   497,   192,   317,   465,     4,   240,     5,   468,   469,
     436,   300,   615,   148,   148,     4,    12,   167,   174,   175,
       6,   162,   163,   511,   511,   491,   575,   158,   282,   276,
       6,   551,   333,   301,   182,   334,     7,     8,   645,     9,
      10,   127,   302,   159,   277,    11,   565,    12,    13,   598,
     530,   137,    14,    15,    16,    11,    17,   531,    13,   599,
     183,   148,     4,   659,   532,   184,    17,   312,   313,   249,
     447,   448,   278,   187,   473,   492,   434,     6,   435,     4,
      53,   474,   188,   596,   615,   616,   547,   552,    54,   149,
     553,   436,   300,     7,     6,    18,     9,   379,   607,   189,
     193,   611,    11,   148,    56,    13,   207,   132,   221,    14,
      15,   200,   526,    17,   301,   150,   548,   182,   183,    11,
     294,   133,    13,   302,   148,   303,   296,    19,   250,   251,
      17,   304,   212,   224,   602,   195,  -171,   128,   305,   638,
     281,   640,   203,    19,    24,   238,   232,   232,   613,   221,
     311,   232,    18,  -119,   244,     4,   201,    20,   248,   161,
     148,   148,   324,   245,   148,   148,   148,   148,   253,   254,
       6,   637,   256,   257,   258,   259,     4,   291,   280,   628,
     542,   543,   642,    45,   338,   338,   314,   590,   383,     4,
     591,     6,   384,   318,   321,    11,   210,   652,    13,   325,
     603,   211,   586,   604,     6,   134,    17,   329,   605,   608,
     232,   604,   609,   213,    20,   306,    11,   135,   225,    13,
     307,   656,   186,   657,  -241,   660,   661,    17,   619,    11,
     447,   448,    13,   308,   534,   226,   482,   309,   483,   143,
      17,   143,   169,   560,   228,   563,   229,   535,   137,   536,
     350,   351,   353,   230,   361,   561,   562,   523,   524,   358,
     310,   310,   234,   232,   353,   537,   -83,   447,   448,   197,
     198,   239,   255,   649,   260,   320,   322,   375,   323,   335,
     332,    46,   341,   261,   262,   337,   263,   343,   264,   265,
     345,   266,   346,   232,   267,   347,   349,   232,   354,   420,
     348,   362,   364,   366,   367,   432,   268,   269,   368,   369,
     371,   370,   372,   373,   441,   356,   455,   148,   270,   374,
     271,   272,   377,   378,   381,   461,   385,   351,   433,  -249,
     462,   351,   351,   232,   463,   464,   466,   471,   472,   475,
     356,   470,   476,   477,   478,   479,   480,   232,   481,   485,
     186,   484,   501,   521,   487,   489,   492,   522,   498,   499,
     525,   541,   545,   504,   506,   546,   508,   549,   513,   503,
     558,   559,   564,   578,   518,   582,   514,   583,   519,   592,
     580,   584,   587,   594,   595,   585,   500,   597,   606,   610,
     612,   614,   617,   622,   623,   621,   625,   627,   641,   629,
     630,   650,   651,   658,   218,   430,   533,   431,   515,   420,
     106,   190,   579,   365,   191,   576,   360,   194,   577,   644,
     380,   340,   520,   376,   246,   626,   527,   220,   151,   152,
     557,   153,   247,   295,   512,   351,   556,   639,   496,   146,
     205,   445,     0,     0,     0,   315,     0,   232,     0,     0,
     319,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   420,     0,   358,     0,   441,     0,   581,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      47,   129,   136,     0,     0,     0,     0,     0,     0,     0,
     589,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       1,     0,     0,     2,    48,     0,     0,     3,     0,     0,
       0,     0,     4,     0,     5,     0,     0,     0,     0,     0,
      49,     0,     0,    50,   358,     0,   420,     6,   620,     0,
      51,    52,     0,   624,    53,     0,     0,     0,     0,     0,
       0,     0,    54,     7,     8,    55,     9,    10,     0,   635,
       0,     0,    11,     0,    12,    13,     0,     0,    56,    14,
      15,    16,     0,    17,     0,     0,     0,    57,     0,   646,
       0,   420,     0,     0,   648,   248,     0,     0,   654,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    18,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    58,    59,    60,    61,    62,
      63,    64,    65,    66,    67,    68,    69,    70,    71,    72,
      73,    74,    75,    76,    77,    78,    79,    80,    81,    82,
      83,    84,    85,    86,    87,    88,    89,    90,    91,    92,
      93,    94,    95,    96,    97,    98,    99,   100,   101,   102,
     103,   104,     1,     0,    20,     2,    48,     0,   588,     3,
       0,     0,     0,     0,     4,     0,     5,     0,     0,     0,
       0,     0,    49,     0,     0,    50,     0,     0,     0,     6,
       0,     0,    51,    52,     0,     0,    53,     0,     0,     0,
       0,     0,     0,     0,    54,     7,     8,    55,     9,    10,
       0,     0,     0,     0,    11,     0,    12,    13,     0,     0,
      56,    14,    15,    16,     0,    17,     0,     0,     0,    57,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    18,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    58,    59,    60,
      61,    62,    63,    64,    65,    66,    67,    68,    69,    70,
      71,    72,    73,    74,    75,    76,    77,   236,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,    94,    95,    96,    97,    98,    99,   100,
     101,   102,   103,   104,     1,     0,    20,     2,    48,   237,
       0,     3,     0,     0,     0,     0,     4,   566,     5,     0,
       0,     0,     0,     0,    49,     0,     0,    50,     0,     0,
       0,     6,     0,     0,    51,    52,     0,     0,    53,   303,
     567,     0,     0,     0,     0,   304,    54,     7,     8,    55,
       9,    10,   305,     0,     0,     0,    11,     0,    12,    13,
       0,     0,    56,    14,    15,    16,     0,    17,     0,     0,
       0,    57,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    18,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    58,
      59,    60,    61,    62,    63,    64,    65,    66,    67,   568,
      69,    70,    71,    72,   569,    74,    75,    76,    77,    78,
      79,    80,    81,    82,    83,    84,    85,   570,    87,    88,
      89,   571,    91,    92,    93,    94,    95,    96,    97,    98,
      99,   100,   101,   102,   103,   104,     1,     0,    20,     2,
      48,     0,     0,     3,     0,     0,   239,     0,     4,     0,
       5,     0,     0,     0,     0,     0,    49,     0,     0,    50,
       0,     0,     0,     6,     0,     0,    51,    52,     0,     0,
      53,     0,     0,     0,     0,     0,   282,     0,    54,     7,
       8,    55,     9,    10,     0,     0,     0,     0,    11,     0,
      12,    13,     0,     0,    56,    14,    15,    16,     0,    17,
       0,     0,     0,    57,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    18,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    58,    59,    60,    61,    62,    63,    64,    65,    66,
      67,    68,    69,    70,    71,    72,    73,    74,    75,    76,
      77,   283,    79,    80,   284,    82,    83,    84,    85,    86,
      87,    88,    89,    90,    91,    92,    93,   285,    95,    96,
      97,    98,    99,   100,   101,   102,   103,   104,     1,     0,
      20,     2,    48,     0,     0,     3,     0,     0,   239,     0,
       4,     0,     5,     0,     0,     0,     0,     0,    49,     0,
       0,    50,     0,     0,     0,     6,     0,     0,    51,    52,
       0,     0,    53,     0,     0,     0,     0,     0,   282,     0,
      54,     7,     8,    55,     9,    10,     0,     0,     0,     0,
      11,     0,    12,    13,     0,     0,    56,    14,    15,    16,
       0,    17,     0,     0,     0,    57,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      18,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    72,    73,    74,
      75,    76,    77,   359,    79,    80,   284,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,   285,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
       1,     0,    20,     2,    48,     0,     0,     3,     0,     0,
    -248,     0,     4,     0,     5,     0,     0,     0,     0,     0,
      49,     0,     0,    50,     0,     0,     0,     6,     0,     0,
      51,    52,     0,     0,    53,     0,     0,     0,     0,     0,
       0,     0,    54,     7,     8,    55,     9,    10,     0,     0,
       0,     0,    11,     0,    12,    13,     0,     0,    56,    14,
      15,    16,     0,    17,     0,     0,     0,    57,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    18,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    58,    59,    60,    61,    62,
      63,    64,    65,    66,    67,    68,    69,    70,    71,    72,
      73,    74,    75,    76,    77,   178,    79,    80,    81,    82,
      83,    84,    85,    86,    87,    88,    89,    90,    91,    92,
      93,    94,    95,    96,    97,    98,    99,   100,   101,   102,
     103,   104,     1,     0,    20,     2,    48,     0,     0,     3,
       0,     0,     0,     0,     4,     0,     5,     0,     0,     0,
       0,     0,    49,     0,     0,    50,     0,     0,     0,     6,
       0,     0,    51,    52,     0,     0,    53,     0,     0,     0,
       0,     0,     0,     0,    54,     7,     8,    55,     9,    10,
       0,     0,     0,     0,    11,     0,    12,    13,     0,     0,
      56,    14,    15,    16,   233,    17,     0,     0,     0,    57,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    18,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    58,    59,    60,
      61,    62,    63,    64,    65,    66,    67,    68,    69,    70,
      71,    72,    73,    74,    75,    76,    77,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,    94,    95,    96,    97,    98,    99,   100,
     101,   102,   103,   104,     1,     0,    20,     2,    48,     0,
       0,     3,     0,     0,     0,     0,     4,     0,     5,     0,
     312,   313,     0,     0,    49,     0,     0,    50,     0,     0,
       0,     6,     0,     0,    51,    52,     0,     0,    53,     0,
       0,     0,     0,     0,     0,     0,    54,     7,     8,    55,
       9,    10,     0,     0,     0,     0,    11,     0,    12,    13,
       0,     0,    56,    14,    15,    16,     0,    17,     0,     0,
       0,    57,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    18,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    58,
      59,    60,    61,    62,    63,    64,    65,    66,    67,    68,
      69,    70,    71,    72,    73,    74,    75,    76,    77,     0,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94,    95,    96,    97,    98,
      99,   100,   101,   102,   103,   104,     1,     0,    20,     2,
      48,     0,     0,     3,     0,     0,     0,     0,     4,     0,
       5,     0,     0,     0,     0,     0,    49,     0,     0,    50,
       0,     0,     0,     6,     0,     0,    51,    52,     0,     0,
      53,     0,     0,     0,     0,     0,     0,     0,    54,     7,
       8,    55,     9,    10,     0,     0,     0,     0,    11,     0,
      12,    13,     0,     0,    56,    14,    15,    16,     0,    17,
       0,     0,     0,    57,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    18,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    58,    59,    60,    61,    62,    63,    64,    65,    66,
      67,    68,    69,    70,    71,    72,    73,    74,    75,    76,
      77,    78,    79,    80,    81,    82,    83,    84,    85,    86,
      87,    88,    89,    90,    91,    92,    93,    94,    95,    96,
      97,    98,    99,   100,   101,   102,   103,   104,     1,     0,
      20,     2,    48,     0,     0,     3,     0,     0,     0,     0,
       4,     0,     5,     0,     0,     0,     0,     0,    49,     0,
       0,    50,     0,     0,     0,     6,     0,     0,    51,    52,
       0,     0,    53,     0,     0,     0,     0,     0,     0,     0,
      54,     7,     8,    55,     9,    10,     0,     0,     0,     0,
      11,     0,    12,    13,     0,     0,    56,    14,    15,    16,
       0,    17,     0,     0,     0,    57,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      18,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    72,    73,    74,
      75,    76,    77,   231,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
       1,     0,    20,     2,    48,     0,     0,     3,     0,     0,
       0,     0,     4,     0,     5,     0,     0,     0,     0,     0,
      49,     0,     0,    50,     0,     0,     0,     6,     0,     0,
      51,    52,     0,     0,    53,     0,     0,     0,     0,     0,
       0,     0,    54,     7,     8,    55,     9,    10,     0,     0,
       0,     0,    11,     0,    12,    13,     0,     0,    56,    14,
      15,    16,     0,    17,     0,     0,     0,    57,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    18,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    58,    59,    60,    61,    62,
      63,    64,    65,    66,    67,    68,    69,    70,    71,    72,
      73,    74,    75,    76,    77,   235,    79,    80,    81,    82,
      83,    84,    85,    86,    87,    88,    89,    90,    91,    92,
      93,    94,    95,    96,    97,    98,    99,   100,   101,   102,
     103,   104,     1,     0,    20,     2,    48,     0,     0,     3,
       0,     0,     0,     0,     4,     0,     5,     0,     0,     0,
       0,     0,    49,     0,     0,    50,     0,     0,     0,     6,
       0,     0,    51,    52,     0,     0,    53,     0,     0,     0,
       0,     0,     0,     0,    54,     7,     8,    55,     9,    10,
       0,     0,     0,     0,    11,     0,    12,    13,     0,     0,
      56,    14,    15,    16,     0,    17,     0,     0,     0,    57,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    18,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    58,    59,    60,
      61,    62,    63,    64,    65,    66,    67,    68,    69,    70,
      71,    72,    73,    74,    75,    76,    77,   326,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,    94,    95,    96,    97,    98,    99,   100,
     101,   102,   103,   104,     1,     0,    20,     2,    48,     0,
       0,     3,     0,     0,     0,     0,     4,     0,     5,     0,
       0,     0,     0,     0,    49,     0,     0,    50,     0,     0,
       0,     6,     0,     0,    51,    52,     0,     0,    53,     0,
       0,     0,     0,     0,     0,     0,    54,     7,     8,    55,
       9,    10,     0,     0,     0,     0,    11,     0,    12,    13,
       0,     0,    56,    14,    15,    16,     0,    17,     0,     0,
       0,    57,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    18,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    58,
      59,    60,    61,    62,    63,    64,    65,    66,    67,    68,
      69,    70,    71,    72,    73,    74,    75,    76,    77,   178,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94,    95,    96,    97,    98,
      99,   100,   101,   102,   103,   104,     1,     0,    20,     2,
      48,     0,     0,     3,     0,     0,     0,     0,     4,     0,
       5,     0,     0,     0,     0,     0,    49,     0,     0,    50,
       0,     0,     0,     6,     0,     0,    51,    52,     0,     0,
      53,     0,     0,     0,     0,     0,     0,     0,    54,     7,
       8,    55,     9,    10,     0,     0,     0,     0,    11,     0,
      12,    13,     0,     0,    56,    14,    15,    16,     0,    17,
       0,     0,     0,    57,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    18,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    58,    59,    60,    61,    62,    63,    64,    65,    66,
      67,    68,    69,    70,    71,    72,    73,    74,    75,    76,
      77,   363,    79,    80,    81,    82,    83,    84,    85,    86,
      87,    88,    89,    90,    91,    92,    93,    94,    95,    96,
      97,    98,    99,   100,   101,   102,   103,   104,     1,     0,
      20,     2,    48,     0,     0,     3,     0,     0,     0,     0,
       4,     0,     5,     0,     0,     0,     0,     0,    49,     0,
       0,    50,     0,     0,     0,     6,     0,     0,    51,    52,
       0,     0,    53,     0,     0,     0,     0,     0,     0,     0,
      54,     7,     8,    55,     9,    10,     0,     0,     0,     0,
      11,     0,    12,    13,     0,     0,    56,    14,    15,    16,
       0,    17,     0,     0,     0,    57,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      18,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    72,    73,    74,
      75,    76,    77,   382,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
       1,     0,    20,     2,    48,     0,     0,     3,     0,     0,
       0,     0,     4,     0,     5,     0,     0,     0,     0,     0,
      49,     0,     0,    50,     0,     0,     0,     6,     0,     0,
      51,    52,     0,     0,    53,     0,     0,     0,     0,     0,
       0,     0,    54,     7,     8,    55,     9,    10,     0,     0,
       0,     0,    11,     0,    12,    13,     0,     0,    56,    14,
      15,    16,     0,    17,     0,     0,     0,    57,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    18,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    58,    59,    60,    61,    62,
      63,    64,    65,    66,    67,    68,    69,    70,    71,    72,
      73,    74,    75,    76,    77,   467,    79,    80,    81,    82,
      83,    84,    85,    86,    87,    88,    89,    90,    91,    92,
      93,    94,    95,    96,    97,    98,    99,   100,   101,   102,
     103,   104,     1,     0,    20,     2,    48,     0,     0,     3,
       0,     0,     0,     0,     4,     0,     5,     0,     0,     0,
       0,     0,    49,     0,     0,    50,     0,     0,     0,     6,
       0,     0,    51,    52,     0,     0,    53,     0,     0,     0,
       0,     0,     0,     0,    54,     7,     8,    55,     9,    10,
       0,     0,     0,     0,    11,     0,    12,    13,     0,     0,
      56,    14,    15,    16,     0,    17,     0,     0,     0,    57,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    18,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    58,    59,    60,
      61,    62,    63,    64,    65,    66,    67,    68,    69,    70,
      71,    72,    73,    74,    75,    76,    77,     0,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,    94,    95,    96,    97,    98,    99,   100,
     101,   102,   103,   104,    48,     0,    20,     0,     0,     0,
       0,     0,     0,   566,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    50,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    53,   303,   567,     0,     0,     0,
       0,   304,    54,     0,     0,    55,     0,     0,   305,     0,
       0,    48,     0,     0,     0,     0,     0,     0,    56,     0,
       0,     0,     0,     0,     0,     0,     0,    57,     0,     0,
      50,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    53,     0,     0,     0,     0,     0,     0,     0,    54,
       0,     0,    55,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    56,     0,     0,     0,     0,
       0,     0,     0,     0,    57,    58,    59,    60,    61,    62,
      63,    64,    65,   139,    67,   568,    69,    70,    71,    72,
     569,    74,    75,    76,    77,    78,    79,    80,    81,    82,
      83,    84,    85,   570,    87,    88,    89,   571,    91,   140,
     141,    94,    95,    96,    97,    98,    99,   100,   101,   102,
     103,   104,    58,    59,    60,    61,    62,    63,    64,    65,
     139,    67,    68,    69,    70,    71,    72,    73,    74,    75,
      76,    77,    78,    79,    80,    81,    82,    83,    84,    85,
      86,    87,    88,    89,    90,    91,   140,   141,    94,    95,
      96,    97,    98,    99,   100,   101,   102,   103,   104,   327,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   328,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      57,     0,   165,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    57,    58,    59,
     138,    61,    62,    63,    64,    65,   139,    67,    68,    69,
      70,    71,    72,    73,    74,    75,    76,    77,    78,    79,
      80,    81,    82,    83,    84,    85,    86,    87,    88,    89,
      90,    91,   140,   141,    94,    95,    96,    97,    98,    99,
     100,   101,   102,   103,   104,    58,    59,   138,    61,    62,
      63,    64,    65,   139,    67,    68,    69,    70,    71,    72,
      73,    74,    75,    76,    77,    78,    79,    80,    81,    82,
      83,    84,    85,    86,    87,    88,   166,    90,    91,   140,
     141,    94,    95,    96,    97,    98,    99,   100,   101,   102,
     103,   104,   386,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    57,   387,     0,     0,
       0,     0,     0,     0,     0,     0,   388,   389,   390,   391,
     392,   393,   394,     0,   395,     0,   396,   397,   398,   399,
     400,   401,   402,   403,   404,   405,   406,     0,     0,     0,
       0,   407,     0,   408,   409,   410,   411,   412,   413,   414,
     415,   416,   417,     0,    58,    59,   138,    61,    62,    63,
      64,    65,   139,    67,    68,    69,    70,    71,    72,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,   140,   141,
      94,    95,    96,    97,    98,    99,   100,   101,   102,   103,
     104,   386,     0,     0,     0,     0,     0,   418,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    57,   387,     0,     0,     0,
       0,     0,     0,     0,     0,   388,   389,   390,   391,   392,
     393,   394,     0,   395,     0,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,     0,     0,     0,     0,
     407,     0,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,     0,    58,    59,   138,    61,    62,    63,    64,
      65,   139,    67,    68,    69,    70,    71,    72,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,   140,   141,    94,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
     137,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    57,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    57,    58,    59,   138,    61,    62,
      63,    64,    65,   139,    67,    68,    69,    70,    71,    72,
      73,    74,    75,    76,    77,    78,    79,    80,    81,    82,
      83,    84,    85,    86,    87,    88,    89,    90,    91,   140,
     141,    94,    95,    96,    97,    98,    99,   100,   101,   102,
     103,   104,    58,    59,   138,    61,    62,    63,    64,    65,
     139,    67,    68,    69,    70,    71,    72,    73,    74,    75,
      76,    77,    78,    79,    80,    81,    82,    83,    84,    85,
      86,    87,    88,    89,    90,    91,   140,   141,    94,    95,
      96,    97,    98,    99,   100,   101,   102,   103,   104,    57,
     446,   447,   448,   449,   450,   451,   452,   453,   -83,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    58,    59,   138,
      61,    62,    63,    64,    65,   139,    67,    68,    69,    70,
      71,    72,    73,    74,    75,    76,    77,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,   140,   141,    94,    95,    96,    97,    98,    99,   100,
     101,   102,   103,   104,    57,   633,     0,     0,     0,     0,
       0,   454,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   634,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    58,    59,   138,    61,    62,    63,    64,    65,
     139,    67,    68,    69,    70,    71,    72,    73,    74,    75,
      76,    77,    78,    79,    80,    81,    82,    83,    84,    85,
      86,    87,    88,    89,    90,    91,   140,   141,    94,    95,
      96,    97,    98,    99,   100,   101,   102,   103,   104,    57,
     653,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    57,    58,    59,   138,
      61,    62,    63,    64,    65,   139,    67,    68,    69,    70,
      71,    72,    73,    74,    75,    76,    77,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,   140,   141,    94,    95,    96,    97,    98,    99,   100,
     101,   102,   103,   104,    58,    59,   138,    61,    62,    63,
      64,    65,   139,    67,    68,    69,    70,    71,    72,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,   140,   141,
      94,    95,    96,    97,    98,    99,   100,   101,   102,   103,
     104,    57,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    57,    58,
      59,   138,    61,   156,    63,    64,    65,   139,    67,    68,
      69,    70,    71,    72,    73,    74,    75,    76,    77,    78,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,   140,   141,    94,    95,    96,    97,    98,
      99,   100,   101,   102,   103,   104,    58,    59,   138,    61,
     199,    63,    64,    65,   139,    67,    68,    69,    70,    71,
      72,    73,    74,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
     140,   141,    94,    95,    96,    97,    98,    99,   100,   101,
     102,   103,   104,    57,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    58,    59,   138,    61,    62,    63,    64,    65,   139,
      67,    68,    69,    70,    71,    72,    73,    74,    75,    76,
      77,     0,    79,    80,    81,    82,    83,    84,    85,    86,
      87,    88,    89,    90,    91,   140,   141,    94,    95,    96,
      97,    98,    99,   100,   101,   102,   103,   104
};

static const yytype_int16 yycheck[] =
{
       2,     1,   348,     3,     2,     5,     8,    48,    10,    50,
      12,   331,   286,     0,    55,    56,    24,    19,    12,    51,
      52,     3,     5,     4,    12,    28,     7,    30,    14,   404,
      11,   406,    44,   224,   359,    16,   181,    18,   363,   364,
      43,   217,   572,    45,    46,    16,    58,    49,    53,    54,
      31,    45,    46,   428,   429,   118,   513,    50,    44,    16,
      31,   495,   169,   217,    66,   172,    47,    48,   619,    50,
      51,    42,   217,     0,    31,    56,   510,    58,    59,    97,
       4,    54,    63,    64,    65,    56,    67,    11,    59,   107,
      92,    93,    16,   644,    18,   168,    67,    20,    21,    12,
      73,    74,    59,   175,   138,   168,    28,    31,    30,    16,
      38,   145,    47,   547,   644,   572,   491,    72,    46,   127,
      75,    43,   298,    47,    31,   106,    50,   318,   560,   174,
     142,   563,    56,   135,    62,    59,   119,    44,    61,    63,
      64,   135,   467,    67,   298,   153,   492,   149,   150,    56,
     138,    58,    59,   298,   156,    39,   142,   154,    71,    72,
      67,    45,   156,   165,   551,    27,   169,   138,    52,   603,
     211,   605,   106,   154,   161,   180,   174,   175,   565,    61,
     221,   179,   106,   154,   186,    16,   165,   168,   188,   171,
     192,   193,   233,   121,   196,   197,   198,   199,   192,   193,
      31,   602,   196,   197,   198,   199,    16,   212,   210,   596,
     102,   103,   613,    44,   246,   247,   139,   169,     9,    16,
     172,    31,    13,   225,   229,    56,   173,   628,    59,   234,
     169,   172,    42,   172,    31,   142,    67,   239,   169,   169,
     238,   172,   172,   120,   168,   129,    56,   154,    26,    59,
     134,   638,   173,   640,   175,   656,   657,    67,   578,    56,
      73,    74,    59,   147,    40,    32,    99,   151,   101,     8,
      67,    10,   313,   505,   139,   507,    57,    53,    54,    55,
     282,   283,   284,   171,   289,    73,    74,   462,   463,   287,
     219,   220,    65,   291,   296,    71,   175,    73,    74,   133,
     134,    14,    22,   623,    72,    27,    38,   312,   171,    69,
     171,   142,   168,   125,   126,   173,   128,    78,   130,   131,
     168,   133,   168,   321,   136,   172,    41,   325,   144,   331,
     174,   171,   139,   171,    52,   337,   148,   149,    52,   161,
      52,    72,    52,    52,   346,   619,   348,   349,   160,    57,
     162,   163,     8,   139,   139,   349,   143,   359,   169,    14,
     168,   363,   364,   361,   168,   168,   171,   137,   137,   374,
     644,   171,     8,    46,    62,    25,    25,   375,    25,   168,
     173,    88,    15,   173,   168,   168,   168,    25,   168,   168,
     124,    73,    73,   168,   168,    73,   168,   100,   169,   176,
     159,    73,    73,   157,   169,    19,   172,   169,   172,   104,
     173,   169,   141,   169,   169,   172,   418,   169,   169,   169,
     169,    17,    15,   158,    49,   173,    47,    73,    73,   106,
     105,   169,   169,   169,   161,   334,   480,   335,   440,   441,
       2,   130,   519,   298,   130,   514,   289,   131,   515,   619,
     321,   247,   454,   313,   187,   588,   479,   163,    16,    16,
     501,    16,   187,   216,   429,   467,   499,   604,   405,    10,
     146,   347,    -1,    -1,    -1,   222,    -1,   475,    -1,    -1,
     226,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   515,    -1,   513,    -1,   519,    -1,   521,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     530,   531,   532,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     538,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
       4,    -1,    -1,     7,     8,    -1,    -1,    11,    -1,    -1,
      -1,    -1,    16,    -1,    18,    -1,    -1,    -1,    -1,    -1,
      24,    -1,    -1,    27,   572,    -1,   578,    31,   580,    -1,
      34,    35,    -1,   585,    38,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    46,    47,    48,    49,    50,    51,    -1,   601,
      -1,    -1,    56,    -1,    58,    59,    -1,    -1,    62,    63,
      64,    65,    -1,    67,    -1,    -1,    -1,    71,    -1,   621,
      -1,   623,    -1,    -1,   622,   625,    -1,    -1,   630,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   119,   120,   121,   122,   123,
     124,   125,   126,   127,   128,   129,   130,   131,   132,   133,
     134,   135,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,   163,
     164,   165,     4,    -1,   168,     7,     8,    -1,   172,    11,
      -1,    -1,    -1,    -1,    16,    -1,    18,    -1,    -1,    -1,
      -1,    -1,    24,    -1,    -1,    27,    -1,    -1,    -1,    31,
      -1,    -1,    34,    35,    -1,    -1,    38,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    46,    47,    48,    49,    50,    51,
      -1,    -1,    -1,    -1,    56,    -1,    58,    59,    -1,    -1,
      62,    63,    64,    65,    -1,    67,    -1,    -1,    -1,    71,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   106,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,   131,
     132,   133,   134,   135,   136,   137,   138,   139,   140,   141,
     142,   143,   144,   145,   146,   147,   148,   149,   150,   151,
     152,   153,   154,   155,   156,   157,   158,   159,   160,   161,
     162,   163,   164,   165,     4,    -1,   168,     7,     8,   171,
      -1,    11,    -1,    -1,    -1,    -1,    16,    17,    18,    -1,
      -1,    -1,    -1,    -1,    24,    -1,    -1,    27,    -1,    -1,
      -1,    31,    -1,    -1,    34,    35,    -1,    -1,    38,    39,
      40,    -1,    -1,    -1,    -1,    45,    46,    47,    48,    49,
      50,    51,    52,    -1,    -1,    -1,    56,    -1,    58,    59,
      -1,    -1,    62,    63,    64,    65,    -1,    67,    -1,    -1,
      -1,    71,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   106,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   119,
     120,   121,   122,   123,   124,   125,   126,   127,   128,   129,
     130,   131,   132,   133,   134,   135,   136,   137,   138,   139,
     140,   141,   142,   143,   144,   145,   146,   147,   148,   149,
     150,   151,   152,   153,   154,   155,   156,   157,   158,   159,
     160,   161,   162,   163,   164,   165,     4,    -1,   168,     7,
       8,    -1,    -1,    11,    -1,    -1,    14,    -1,    16,    -1,
      18,    -1,    -1,    -1,    -1,    -1,    24,    -1,    -1,    27,
      -1,    -1,    -1,    31,    -1,    -1,    34,    35,    -1,    -1,
      38,    -1,    -1,    -1,    -1,    -1,    44,    -1,    46,    47,
      48,    49,    50,    51,    -1,    -1,    -1,    -1,    56,    -1,
      58,    59,    -1,    -1,    62,    63,    64,    65,    -1,    67,
      -1,    -1,    -1,    71,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   119,   120,   121,   122,   123,   124,   125,   126,   127,
     128,   129,   130,   131,   132,   133,   134,   135,   136,   137,
     138,   139,   140,   141,   142,   143,   144,   145,   146,   147,
     148,   149,   150,   151,   152,   153,   154,   155,   156,   157,
     158,   159,   160,   161,   162,   163,   164,   165,     4,    -1,
     168,     7,     8,    -1,    -1,    11,    -1,    -1,    14,    -1,
      16,    -1,    18,    -1,    -1,    -1,    -1,    -1,    24,    -1,
      -1,    27,    -1,    -1,    -1,    31,    -1,    -1,    34,    35,
      -1,    -1,    38,    -1,    -1,    -1,    -1,    -1,    44,    -1,
      46,    47,    48,    49,    50,    51,    -1,    -1,    -1,    -1,
      56,    -1,    58,    59,    -1,    -1,    62,    63,    64,    65,
      -1,    67,    -1,    -1,    -1,    71,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   119,   120,   121,   122,   123,   124,   125,
     126,   127,   128,   129,   130,   131,   132,   133,   134,   135,
     136,   137,   138,   139,   140,   141,   142,   143,   144,   145,
     146,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,   158,   159,   160,   161,   162,   163,   164,   165,
       4,    -1,   168,     7,     8,    -1,    -1,    11,    -1,    -1,
      14,    -1,    16,    -1,    18,    -1,    -1,    -1,    -1,    -1,
      24,    -1,    -1,    27,    -1,    -1,    -1,    31,    -1,    -1,
      34,    35,    -1,    -1,    38,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    46,    47,    48,    49,    50,    51,    -1,    -1,
      -1,    -1,    56,    -1,    58,    59,    -1,    -1,    62,    63,
      64,    65,    -1,    67,    -1,    -1,    -1,    71,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   119,   120,   121,   122,   123,
     124,   125,   126,   127,   128,   129,   130,   131,   132,   133,
     134,   135,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,   163,
     164,   165,     4,    -1,   168,     7,     8,    -1,    -1,    11,
      -1,    -1,    -1,    -1,    16,    -1,    18,    -1,    -1,    -1,
      -1,    -1,    24,    -1,    -1,    27,    -1,    -1,    -1,    31,
      -1,    -1,    34,    35,    -1,    -1,    38,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    46,    47,    48,    49,    50,    51,
      -1,    -1,    -1,    -1,    56,    -1,    58,    59,    -1,    -1,
      62,    63,    64,    65,    66,    67,    -1,    -1,    -1,    71,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   106,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,   131,
     132,   133,   134,   135,   136,   137,   138,   139,   140,   141,
     142,   143,   144,   145,   146,   147,   148,   149,   150,   151,
     152,   153,   154,   155,   156,   157,   158,   159,   160,   161,
     162,   163,   164,   165,     4,    -1,   168,     7,     8,    -1,
      -1,    11,    -1,    -1,    -1,    -1,    16,    -1,    18,    -1,
      20,    21,    -1,    -1,    24,    -1,    -1,    27,    -1,    -1,
      -1,    31,    -1,    -1,    34,    35,    -1,    -1,    38,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    46,    47,    48,    49,
      50,    51,    -1,    -1,    -1,    -1,    56,    -1,    58,    59,
      -1,    -1,    62,    63,    64,    65,    -1,    67,    -1,    -1,
      -1,    71,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   106,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   119,
     120,   121,   122,   123,   124,   125,   126,   127,   128,   129,
     130,   131,   132,   133,   134,   135,   136,   137,   138,    -1,
     140,   141,   142,   143,   144,   145,   146,   147,   148,   149,
     150,   151,   152,   153,   154,   155,   156,   157,   158,   159,
     160,   161,   162,   163,   164,   165,     4,    -1,   168,     7,
       8,    -1,    -1,    11,    -1,    -1,    -1,    -1,    16,    -1,
      18,    -1,    -1,    -1,    -1,    -1,    24,    -1,    -1,    27,
      -1,    -1,    -1,    31,    -1,    -1,    34,    35,    -1,    -1,
      38,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    46,    47,
      48,    49,    50,    51,    -1,    -1,    -1,    -1,    56,    -1,
      58,    59,    -1,    -1,    62,    63,    64,    65,    -1,    67,
      -1,    -1,    -1,    71,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   119,   120,   121,   122,   123,   124,   125,   126,   127,
     128,   129,   130,   131,   132,   133,   134,   135,   136,   137,
     138,   139,   140,   141,   142,   143,   144,   145,   146,   147,
     148,   149,   150,   151,   152,   153,   154,   155,   156,   157,
     158,   159,   160,   161,   162,   163,   164,   165,     4,    -1,
     168,     7,     8,    -1,    -1,    11,    -1,    -1,    -1,    -1,
      16,    -1,    18,    -1,    -1,    -1,    -1,    -1,    24,    -1,
      -1,    27,    -1,    -1,    -1,    31,    -1,    -1,    34,    35,
      -1,    -1,    38,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      46,    47,    48,    49,    50,    51,    -1,    -1,    -1,    -1,
      56,    -1,    58,    59,    -1,    -1,    62,    63,    64,    65,
      -1,    67,    -1,    -1,    -1,    71,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   119,   120,   121,   122,   123,   124,   125,
     126,   127,   128,   129,   130,   131,   132,   133,   134,   135,
     136,   137,   138,   139,   140,   141,   142,   143,   144,   145,
     146,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,   158,   159,   160,   161,   162,   163,   164,   165,
       4,    -1,   168,     7,     8,    -1,    -1,    11,    -1,    -1,
      -1,    -1,    16,    -1,    18,    -1,    -1,    -1,    -1,    -1,
      24,    -1,    -1,    27,    -1,    -1,    -1,    31,    -1,    -1,
      34,    35,    -1,    -1,    38,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    46,    47,    48,    49,    50,    51,    -1,    -1,
      -1,    -1,    56,    -1,    58,    59,    -1,    -1,    62,    63,
      64,    65,    -1,    67,    -1,    -1,    -1,    71,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   119,   120,   121,   122,   123,
     124,   125,   126,   127,   128,   129,   130,   131,   132,   133,
     134,   135,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,   163,
     164,   165,     4,    -1,   168,     7,     8,    -1,    -1,    11,
      -1,    -1,    -1,    -1,    16,    -1,    18,    -1,    -1,    -1,
      -1,    -1,    24,    -1,    -1,    27,    -1,    -1,    -1,    31,
      -1,    -1,    34,    35,    -1,    -1,    38,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    46,    47,    48,    49,    50,    51,
      -1,    -1,    -1,    -1,    56,    -1,    58,    59,    -1,    -1,
      62,    63,    64,    65,    -1,    67,    -1,    -1,    -1,    71,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   106,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,   131,
     132,   133,   134,   135,   136,   137,   138,   139,   140,   141,
     142,   143,   144,   145,   146,   147,   148,   149,   150,   151,
     152,   153,   154,   155,   156,   157,   158,   159,   160,   161,
     162,   163,   164,   165,     4,    -1,   168,     7,     8,    -1,
      -1,    11,    -1,    -1,    -1,    -1,    16,    -1,    18,    -1,
      -1,    -1,    -1,    -1,    24,    -1,    -1,    27,    -1,    -1,
      -1,    31,    -1,    -1,    34,    35,    -1,    -1,    38,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    46,    47,    48,    49,
      50,    51,    -1,    -1,    -1,    -1,    56,    -1,    58,    59,
      -1,    -1,    62,    63,    64,    65,    -1,    67,    -1,    -1,
      -1,    71,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   106,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   119,
     120,   121,   122,   123,   124,   125,   126,   127,   128,   129,
     130,   131,   132,   133,   134,   135,   136,   137,   138,   139,
     140,   141,   142,   143,   144,   145,   146,   147,   148,   149,
     150,   151,   152,   153,   154,   155,   156,   157,   158,   159,
     160,   161,   162,   163,   164,   165,     4,    -1,   168,     7,
       8,    -1,    -1,    11,    -1,    -1,    -1,    -1,    16,    -1,
      18,    -1,    -1,    -1,    -1,    -1,    24,    -1,    -1,    27,
      -1,    -1,    -1,    31,    -1,    -1,    34,    35,    -1,    -1,
      38,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    46,    47,
      48,    49,    50,    51,    -1,    -1,    -1,    -1,    56,    -1,
      58,    59,    -1,    -1,    62,    63,    64,    65,    -1,    67,
      -1,    -1,    -1,    71,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   119,   120,   121,   122,   123,   124,   125,   126,   127,
     128,   129,   130,   131,   132,   133,   134,   135,   136,   137,
     138,   139,   140,   141,   142,   143,   144,   145,   146,   147,
     148,   149,   150,   151,   152,   153,   154,   155,   156,   157,
     158,   159,   160,   161,   162,   163,   164,   165,     4,    -1,
     168,     7,     8,    -1,    -1,    11,    -1,    -1,    -1,    -1,
      16,    -1,    18,    -1,    -1,    -1,    -1,    -1,    24,    -1,
      -1,    27,    -1,    -1,    -1,    31,    -1,    -1,    34,    35,
      -1,    -1,    38,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      46,    47,    48,    49,    50,    51,    -1,    -1,    -1,    -1,
      56,    -1,    58,    59,    -1,    -1,    62,    63,    64,    65,
      -1,    67,    -1,    -1,    -1,    71,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   119,   120,   121,   122,   123,   124,   125,
     126,   127,   128,   129,   130,   131,   132,   133,   134,   135,
     136,   137,   138,   139,   140,   141,   142,   143,   144,   145,
     146,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,   158,   159,   160,   161,   162,   163,   164,   165,
       4,    -1,   168,     7,     8,    -1,    -1,    11,    -1,    -1,
      -1,    -1,    16,    -1,    18,    -1,    -1,    -1,    -1,    -1,
      24,    -1,    -1,    27,    -1,    -1,    -1,    31,    -1,    -1,
      34,    35,    -1,    -1,    38,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    46,    47,    48,    49,    50,    51,    -1,    -1,
      -1,    -1,    56,    -1,    58,    59,    -1,    -1,    62,    63,
      64,    65,    -1,    67,    -1,    -1,    -1,    71,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   106,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   119,   120,   121,   122,   123,
     124,   125,   126,   127,   128,   129,   130,   131,   132,   133,
     134,   135,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,   163,
     164,   165,     4,    -1,   168,     7,     8,    -1,    -1,    11,
      -1,    -1,    -1,    -1,    16,    -1,    18,    -1,    -1,    -1,
      -1,    -1,    24,    -1,    -1,    27,    -1,    -1,    -1,    31,
      -1,    -1,    34,    35,    -1,    -1,    38,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    46,    47,    48,    49,    50,    51,
      -1,    -1,    -1,    -1,    56,    -1,    58,    59,    -1,    -1,
      62,    63,    64,    65,    -1,    67,    -1,    -1,    -1,    71,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   106,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,   131,
     132,   133,   134,   135,   136,   137,   138,    -1,   140,   141,
     142,   143,   144,   145,   146,   147,   148,   149,   150,   151,
     152,   153,   154,   155,   156,   157,   158,   159,   160,   161,
     162,   163,   164,   165,     8,    -1,   168,    -1,    -1,    -1,
      -1,    -1,    -1,    17,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    27,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    38,    39,    40,    -1,    -1,    -1,
      -1,    45,    46,    -1,    -1,    49,    -1,    -1,    52,    -1,
      -1,     8,    -1,    -1,    -1,    -1,    -1,    -1,    62,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    71,    -1,    -1,
      27,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    38,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    46,
      -1,    -1,    49,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    62,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    71,   119,   120,   121,   122,   123,
     124,   125,   126,   127,   128,   129,   130,   131,   132,   133,
     134,   135,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,   163,
     164,   165,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,   131,   132,   133,   134,   135,   136,
     137,   138,   139,   140,   141,   142,   143,   144,   145,   146,
     147,   148,   149,   150,   151,   152,   153,   154,   155,   156,
     157,   158,   159,   160,   161,   162,   163,   164,   165,    10,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    23,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      71,    -1,    26,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    71,   119,   120,
     121,   122,   123,   124,   125,   126,   127,   128,   129,   130,
     131,   132,   133,   134,   135,   136,   137,   138,   139,   140,
     141,   142,   143,   144,   145,   146,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,   158,   159,   160,
     161,   162,   163,   164,   165,   119,   120,   121,   122,   123,
     124,   125,   126,   127,   128,   129,   130,   131,   132,   133,
     134,   135,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,   163,
     164,   165,    37,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    71,    72,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    81,    82,    83,    84,
      85,    86,    87,    -1,    89,    -1,    91,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   101,    -1,    -1,    -1,
      -1,   106,    -1,   108,   109,   110,   111,   112,   113,   114,
     115,   116,   117,    -1,   119,   120,   121,   122,   123,   124,
     125,   126,   127,   128,   129,   130,   131,   132,   133,   134,
     135,   136,   137,   138,   139,   140,   141,   142,   143,   144,
     145,   146,   147,   148,   149,   150,   151,   152,   153,   154,
     155,   156,   157,   158,   159,   160,   161,   162,   163,   164,
     165,    37,    -1,    -1,    -1,    -1,    -1,   172,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    71,    72,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    81,    82,    83,    84,    85,
      86,    87,    -1,    89,    -1,    91,    92,    93,    94,    95,
      96,    97,    98,    99,   100,   101,    -1,    -1,    -1,    -1,
     106,    -1,   108,   109,   110,   111,   112,   113,   114,   115,
     116,   117,    -1,   119,   120,   121,   122,   123,   124,   125,
     126,   127,   128,   129,   130,   131,   132,   133,   134,   135,
     136,   137,   138,   139,   140,   141,   142,   143,   144,   145,
     146,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,   158,   159,   160,   161,   162,   163,   164,   165,
      54,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    71,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    71,   119,   120,   121,   122,   123,
     124,   125,   126,   127,   128,   129,   130,   131,   132,   133,
     134,   135,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,   163,
     164,   165,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,   131,   132,   133,   134,   135,   136,
     137,   138,   139,   140,   141,   142,   143,   144,   145,   146,
     147,   148,   149,   150,   151,   152,   153,   154,   155,   156,
     157,   158,   159,   160,   161,   162,   163,   164,   165,    71,
      72,    73,    74,    75,    76,    77,    78,    79,   175,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,   131,
     132,   133,   134,   135,   136,   137,   138,   139,   140,   141,
     142,   143,   144,   145,   146,   147,   148,   149,   150,   151,
     152,   153,   154,   155,   156,   157,   158,   159,   160,   161,
     162,   163,   164,   165,    71,    72,    -1,    -1,    -1,    -1,
      -1,   173,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   100,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,   131,   132,   133,   134,   135,   136,
     137,   138,   139,   140,   141,   142,   143,   144,   145,   146,
     147,   148,   149,   150,   151,   152,   153,   154,   155,   156,
     157,   158,   159,   160,   161,   162,   163,   164,   165,    71,
      72,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    71,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,   131,
     132,   133,   134,   135,   136,   137,   138,   139,   140,   141,
     142,   143,   144,   145,   146,   147,   148,   149,   150,   151,
     152,   153,   154,   155,   156,   157,   158,   159,   160,   161,
     162,   163,   164,   165,   119,   120,   121,   122,   123,   124,
     125,   126,   127,   128,   129,   130,   131,   132,   133,   134,
     135,   136,   137,   138,   139,   140,   141,   142,   143,   144,
     145,   146,   147,   148,   149,   150,   151,   152,   153,   154,
     155,   156,   157,   158,   159,   160,   161,   162,   163,   164,
     165,    71,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    71,   119,
     120,   121,   122,   123,   124,   125,   126,   127,   128,   129,
     130,   131,   132,   133,   134,   135,   136,   137,   138,   139,
     140,   141,   142,   143,   144,   145,   146,   147,   148,   149,
     150,   151,   152,   153,   154,   155,   156,   157,   158,   159,
     160,   161,   162,   163,   164,   165,   119,   120,   121,   122,
     123,   124,   125,   126,   127,   128,   129,   130,   131,   132,
     133,   134,   135,   136,   137,   138,   139,   140,   141,   142,
     143,   144,   145,   146,   147,   148,   149,   150,   151,   152,
     153,   154,   155,   156,   157,   158,   159,   160,   161,   162,
     163,   164,   165,    71,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   119,   120,   121,   122,   123,   124,   125,   126,   127,
     128,   129,   130,   131,   132,   133,   134,   135,   136,   137,
     138,    -1,   140,   141,   142,   143,   144,   145,   146,   147,
     148,   149,   150,   151,   152,   153,   154,   155,   156,   157,
     158,   159,   160,   161,   162,   163,   164,   165
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,     4,     7,    11,    16,    18,    31,    47,    48,    50,
      51,    56,    58,    59,    63,    64,    65,    67,   106,   154,
     168,   178,   179,   180,   181,   182,   183,   184,   185,   195,
     196,   208,   209,   210,   214,   215,   220,   221,   222,   241,
     242,   285,   286,   318,   319,    44,   142,   182,     8,    24,
      27,    34,    35,    38,    46,    49,    62,    71,   119,   120,
     121,   122,   123,   124,   125,   126,   127,   128,   129,   130,
     131,   132,   133,   134,   135,   136,   137,   138,   139,   140,
     141,   142,   143,   144,   145,   146,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,   158,   159,   160,
     161,   162,   163,   164,   165,   181,   192,   193,   194,   244,
     245,   246,   247,   248,   250,   254,   255,   256,   257,   268,
     269,   270,   280,   281,   282,   283,   284,    42,   138,   182,
     201,   223,    44,    58,   142,   154,   182,    54,   121,   127,
     153,   154,   193,   267,   320,   321,   321,   191,   193,   127,
     153,   268,   269,   270,   278,   279,   123,   191,    50,     0,
       3,   171,   191,   191,   279,    26,   150,   193,   249,   279,
     193,   257,   257,   244,   259,   259,   279,   279,   139,   259,
     260,   261,   193,   193,   168,   186,   173,   175,    47,   174,
     196,   209,    44,   142,   215,    27,   287,   287,   287,   123,
     191,   165,   272,   106,   322,   322,     5,   119,   216,   217,
     173,   172,   191,   120,   197,   198,   199,   200,   180,   243,
     243,    61,   251,   252,   193,    26,    32,   271,   139,    57,
     171,   139,   244,    66,    65,   139,   139,   171,   259,    14,
     262,   187,   188,   190,   193,   121,   255,   280,   182,    12,
      71,    72,   224,   191,   191,    22,   191,   191,   191,   191,
      72,   125,   126,   128,   130,   131,   133,   136,   148,   149,
     160,   162,   163,   323,   324,   327,    16,    31,    59,   218,
     193,   279,    44,   139,   142,   155,   205,   206,   211,   212,
     213,   259,   262,    12,   138,   198,   142,   202,   203,   204,
     205,   206,   262,    39,    45,    52,   129,   134,   147,   151,
     238,   279,    20,    21,   139,   252,   253,   271,   193,   278,
      27,   259,    38,   171,   279,   259,   139,    10,    23,   193,
     263,   273,   171,   169,   172,    69,   189,   173,   257,   258,
     258,   168,   226,    78,   225,   168,   168,   172,   174,    41,
     193,   193,   207,   193,   144,   240,   247,   239,   244,   139,
     213,   259,   171,   139,   139,   204,   171,    52,    52,   161,
      72,    52,    52,    52,    57,   259,   249,     8,   139,   271,
     253,   139,   139,     9,    13,   143,    37,    72,    81,    82,
      83,    84,    85,    86,    87,    89,    91,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   101,   106,   108,   109,
     110,   111,   112,   113,   114,   115,   116,   117,   172,   192,
     193,   274,   288,   289,   290,   291,   292,   293,   302,   303,
     188,   190,   193,   169,    28,    30,    43,   227,   228,   229,
     233,   193,   230,   231,   232,   324,    72,    73,    74,    75,
      76,    77,    78,    79,   173,   193,   235,   300,   301,   325,
     326,   191,   168,   168,   168,   207,   171,   139,   207,   207,
     171,   137,   137,   138,   145,   259,     8,    46,    62,    25,
      25,    25,    99,   101,    88,   168,   297,   168,   295,   168,
     298,   118,   168,   305,   308,   305,   308,   305,   168,   168,
     193,    15,   275,   176,   168,   294,   168,   296,   168,   299,
     304,   305,   304,   169,   172,   193,   234,   274,   169,   172,
     193,   173,    25,   227,   227,   124,   207,   266,   267,   300,
       4,    11,    18,   183,    40,    53,    55,    71,   264,   265,
     266,    73,   102,   103,   309,    73,    73,   305,   300,   100,
     311,   311,    72,    75,   306,   307,   306,   279,   159,    73,
     309,    73,    74,   309,    73,   311,    17,    40,   129,   134,
     147,   151,   236,   237,   238,   239,   229,   234,   157,   232,
     173,   193,    19,   169,   169,   172,    42,   141,   172,   244,
     169,   172,   104,   310,   169,   169,   311,   169,    97,   107,
     314,   315,   314,   169,   172,   169,   169,   310,   169,   172,
     169,   310,   169,   314,    17,   237,   239,    15,   276,   274,
     193,   173,   158,    49,   193,    47,   265,    73,   314,   106,
     105,   316,   317,    72,   100,   193,   313,   317,   311,   307,
     311,    73,   317,   277,   236,   240,   193,   219,   244,   274,
     169,   169,   317,    72,   193,   312,   314,   314,   169,   240,
     317,   317
};

#define yyerrok		(yyerrstatus = 0)
#define yyclearin	(yychar = YYEMPTY)
#define YYEMPTY		(-2)
#define YYEOF		0

#define YYACCEPT	goto yyacceptlab
#define YYABORT		goto yyabortlab
#define YYERROR		goto yyerrorlab


/* Like YYERROR except do call yyerror.  This remains here temporarily
   to ease the transition to the new meaning of YYERROR, for GCC.
   Once GCC version 2 has supplanted version 1, this can go.  */

#define YYFAIL		goto yyerrlab

#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)					\
do								\
  if (yychar == YYEMPTY && yylen == 1)				\
    {								\
      yychar = (Token);						\
      yylval = (Value);						\
      yytoken = YYTRANSLATE (yychar);				\
      YYPOPSTACK (1);						\
      goto yybackup;						\
    }								\
  else								\
    {								\
      yyerror (&yylloc, parse_ctx, YY_("syntax error: cannot back up")); \
      YYERROR;							\
    }								\
while (YYID (0))


#define YYTERROR	1
#define YYERRCODE	256


/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#define YYRHSLOC(Rhs, K) ((Rhs)[K])
#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)				\
    do									\
      if (YYID (N))                                                    \
	{								\
	  (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;	\
	  (Current).first_column = YYRHSLOC (Rhs, 1).first_column;	\
	  (Current).last_line    = YYRHSLOC (Rhs, N).last_line;		\
	  (Current).last_column  = YYRHSLOC (Rhs, N).last_column;	\
	}								\
      else								\
	{								\
	  (Current).first_line   = (Current).last_line   =		\
	    YYRHSLOC (Rhs, 0).last_line;				\
	  (Current).first_column = (Current).last_column =		\
	    YYRHSLOC (Rhs, 0).last_column;				\
	}								\
    while (YYID (0))
#endif


/* YY_LOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

#ifndef YY_LOCATION_PRINT
# if YYLTYPE_IS_TRIVIAL
#  define YY_LOCATION_PRINT(File, Loc)			\
     fprintf (File, "%d.%d-%d.%d",			\
	      (Loc).first_line, (Loc).first_column,	\
	      (Loc).last_line,  (Loc).last_column)
# else
#  define YY_LOCATION_PRINT(File, Loc) ((void) 0)
# endif
#endif


/* YYLEX -- calling `yylex' with the right arguments.  */

#ifdef YYLEX_PARAM
# define YYLEX yylex (&yylval, &yylloc, YYLEX_PARAM)
#else
# define YYLEX yylex (&yylval, &yylloc)
#endif

/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)			\
do {						\
  if (yydebug)					\
    YYFPRINTF Args;				\
} while (YYID (0))

# define YY_SYMBOL_PRINT(Title, Type, Value, Location)			  \
do {									  \
  if (yydebug)								  \
    {									  \
      YYFPRINTF (stderr, "%s ", Title);					  \
      yy_symbol_print (stderr,						  \
		  Type, Value, Location, parse_ctx); \
      YYFPRINTF (stderr, "\n");						  \
    }									  \
} while (YYID (0))


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, ObParseCtx *parse_ctx)
#else
static void
yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, parse_ctx)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    ObParseCtx *parse_ctx;
#endif
{
  if (!yyvaluep)
    return;
  YYUSE (yylocationp);
  YYUSE (parse_ctx);
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyoutput, yytoknum[yytype], *yyvaluep);
# else
  YYUSE (yyoutput);
# endif
  switch (yytype)
    {
      default:
	break;
    }
}


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, ObParseCtx *parse_ctx)
#else
static void
yy_symbol_print (yyoutput, yytype, yyvaluep, yylocationp, parse_ctx)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    ObParseCtx *parse_ctx;
#endif
{
  if (yytype < YYNTOKENS)
    YYFPRINTF (yyoutput, "token %s (", yytname[yytype]);
  else
    YYFPRINTF (yyoutput, "nterm %s (", yytname[yytype]);

  YY_LOCATION_PRINT (yyoutput, *yylocationp);
  YYFPRINTF (yyoutput, ": ");
  yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, parse_ctx);
  YYFPRINTF (yyoutput, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_stack_print (yytype_int16 *yybottom, yytype_int16 *yytop)
#else
static void
yy_stack_print (yybottom, yytop)
    yytype_int16 *yybottom;
    yytype_int16 *yytop;
#endif
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)				\
do {								\
  if (yydebug)							\
    yy_stack_print ((Bottom), (Top));				\
} while (YYID (0))


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_reduce_print (YYSTYPE *yyvsp, YYLTYPE *yylsp, int yyrule, ObParseCtx *parse_ctx)
#else
static void
yy_reduce_print (yyvsp, yylsp, yyrule, parse_ctx)
    YYSTYPE *yyvsp;
    YYLTYPE *yylsp;
    int yyrule;
    ObParseCtx *parse_ctx;
#endif
{
  int yynrhs = yyr2[yyrule];
  int yyi;
  unsigned long int yylno = yyrline[yyrule];
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
	     yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr, yyrhs[yyprhs[yyrule] + yyi],
		       &(yyvsp[(yyi + 1) - (yynrhs)])
		       , &(yylsp[(yyi + 1) - (yynrhs)])		       , parse_ctx);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)		\
do {					\
  if (yydebug)				\
    yy_reduce_print (yyvsp, yylsp, Rule, parse_ctx); \
} while (YYID (0))

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef	YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif



#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static YYSIZE_T
yystrlen (const char *yystr)
#else
static YYSIZE_T
yystrlen (yystr)
    const char *yystr;
#endif
{
  YYSIZE_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static char *
yystpcpy (char *yydest, const char *yysrc)
#else
static char *
yystpcpy (yydest, yysrc)
    char *yydest;
    const char *yysrc;
#endif
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYSIZE_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYSIZE_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
	switch (*++yyp)
	  {
	  case '\'':
	  case ',':
	    goto do_not_strip_quotes;

	  case '\\':
	    if (*++yyp != '\\')
	      goto do_not_strip_quotes;
	    /* Fall through.  */
	  default:
	    if (yyres)
	      yyres[yyn] = *yyp;
	    yyn++;
	    break;

	  case '"':
	    if (yyres)
	      yyres[yyn] = '\0';
	    return yyn;
	  }
    do_not_strip_quotes: ;
    }

  if (! yyres)
    return yystrlen (yystr);

  return yystpcpy (yyres, yystr) - yyres;
}
# endif

/* Copy into YYRESULT an error message about the unexpected token
   YYCHAR while in state YYSTATE.  Return the number of bytes copied,
   including the terminating null byte.  If YYRESULT is null, do not
   copy anything; just return the number of bytes that would be
   copied.  As a special case, return 0 if an ordinary "syntax error"
   message will do.  Return YYSIZE_MAXIMUM if overflow occurs during
   size calculation.  */
static YYSIZE_T
yysyntax_error (char *yyresult, int yystate, int yychar)
{
  int yyn = yypact[yystate];

  if (! (YYPACT_NINF < yyn && yyn <= YYLAST))
    return 0;
  else
    {
      int yytype = YYTRANSLATE (yychar);
      YYSIZE_T yysize0 = yytnamerr (0, yytname[yytype]);
      YYSIZE_T yysize = yysize0;
      YYSIZE_T yysize1;
      int yysize_overflow = 0;
      enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
      char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
      int yyx;

# if 0
      /* This is so xgettext sees the translatable formats that are
	 constructed on the fly.  */
      YY_("syntax error, unexpected %s");
      YY_("syntax error, unexpected %s, expecting %s");
      YY_("syntax error, unexpected %s, expecting %s or %s");
      YY_("syntax error, unexpected %s, expecting %s or %s or %s");
      YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s");
# endif
      char *yyfmt;
      char const *yyf;
      static char const yyunexpected[] = "syntax error, unexpected %s";
      static char const yyexpecting[] = ", expecting %s";
      static char const yyor[] = " or %s";
      char yyformat[sizeof yyunexpected
		    + sizeof yyexpecting - 1
		    + ((YYERROR_VERBOSE_ARGS_MAXIMUM - 2)
		       * (sizeof yyor - 1))];
      char const *yyprefix = yyexpecting;

      /* Start YYX at -YYN if negative to avoid negative indexes in
	 YYCHECK.  */
      int yyxbegin = yyn < 0 ? -yyn : 0;

      /* Stay within bounds of both yycheck and yytname.  */
      int yychecklim = YYLAST - yyn + 1;
      int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
      int yycount = 1;

      yyarg[0] = yytname[yytype];
      yyfmt = yystpcpy (yyformat, yyunexpected);

      for (yyx = yyxbegin; yyx < yyxend; ++yyx)
	if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR)
	  {
	    if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
	      {
		yycount = 1;
		yysize = yysize0;
		yyformat[sizeof yyunexpected - 1] = '\0';
		break;
	      }
	    yyarg[yycount++] = yytname[yyx];
	    yysize1 = yysize + yytnamerr (0, yytname[yyx]);
	    yysize_overflow |= (yysize1 < yysize);
	    yysize = yysize1;
	    yyfmt = yystpcpy (yyfmt, yyprefix);
	    yyprefix = yyor;
	  }

      yyf = YY_(yyformat);
      yysize1 = yysize + yystrlen (yyf);
      yysize_overflow |= (yysize1 < yysize);
      yysize = yysize1;

      if (yysize_overflow)
	return YYSIZE_MAXIMUM;

      if (yyresult)
	{
	  /* Avoid sprintf, as that infringes on the user's name space.
	     Don't have undefined behavior even if the translation
	     produced a string with the wrong number of "%s"s.  */
	  char *yyp = yyresult;
	  int yyi = 0;
	  while ((*yyp = *yyf) != '\0')
	    {
	      if (*yyp == '%' && yyf[1] == 's' && yyi < yycount)
		{
		  yyp += yytnamerr (yyp, yyarg[yyi++]);
		  yyf += 2;
		}
	      else
		{
		  yyp++;
		  yyf++;
		}
	    }
	}
      return yysize;
    }
}
#endif /* YYERROR_VERBOSE */


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep, YYLTYPE *yylocationp, ObParseCtx *parse_ctx)
#else
static void
yydestruct (yymsg, yytype, yyvaluep, yylocationp, parse_ctx)
    const char *yymsg;
    int yytype;
    YYSTYPE *yyvaluep;
    YYLTYPE *yylocationp;
    ObParseCtx *parse_ctx;
#endif
{
  YYUSE (yyvaluep);
  YYUSE (yylocationp);
  YYUSE (parse_ctx);

  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  switch (yytype)
    {

      default:
	break;
    }
}

/* Prevent warnings from -Wmissing-prototypes.  */
#ifdef YYPARSE_PARAM
#if defined __STDC__ || defined __cplusplus
int yyparse (void *YYPARSE_PARAM);
#else
int yyparse ();
#endif
#else /* ! YYPARSE_PARAM */
#if defined __STDC__ || defined __cplusplus
int yyparse (ObParseCtx *parse_ctx);
#else
int yyparse ();
#endif
#endif /* ! YYPARSE_PARAM */





/*-------------------------.
| yyparse or yypush_parse.  |
`-------------------------*/

#ifdef YYPARSE_PARAM
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (void *YYPARSE_PARAM)
#else
int
yyparse (YYPARSE_PARAM)
    void *YYPARSE_PARAM;
#endif
#else /* ! YYPARSE_PARAM */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (ObParseCtx *parse_ctx)
#else
int
yyparse (parse_ctx)
    ObParseCtx *parse_ctx;
#endif
#endif
{
/* The lookahead symbol.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;

/* Location data for the lookahead symbol.  */
YYLTYPE yylloc;

    /* Number of syntax errors so far.  */
    int yynerrs;

    int yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       `yyss': related to states.
       `yyvs': related to semantic values.
       `yyls': related to locations.

       Refer to the stacks thru separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yytype_int16 yyssa[YYINITDEPTH];
    yytype_int16 *yyss;
    yytype_int16 *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    /* The location stack.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls;
    YYLTYPE *yylsp;

    /* The locations where the error started and ended.  */
    YYLTYPE yyerror_range[2];

    YYSIZE_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yytoken = 0;
  yyss = yyssa;
  yyvs = yyvsa;
  yyls = yylsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */

  /* Initialize stack pointers.
     Waste one element of value and location stack
     so that they stay on the same level as the state stack.
     The wasted elements are never initialized.  */
  yyssp = yyss;
  yyvsp = yyvs;
  yylsp = yyls;

#if YYLTYPE_IS_TRIVIAL
  /* Initialize the default location before parsing starts.  */
  yylloc.first_line   = yylloc.last_line   = 1;
  yylloc.first_column = yylloc.last_column = 1;
#endif

  goto yysetstate;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

 yysetstate:
  *yyssp = yystate;

  if (yyss + yystacksize - 1 <= yyssp)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = yyssp - yyss + 1;

#ifdef yyoverflow
      {
	/* Give user a chance to reallocate the stack.  Use copies of
	   these so that the &'s don't force the real ones into
	   memory.  */
	YYSTYPE *yyvs1 = yyvs;
	yytype_int16 *yyss1 = yyss;
	YYLTYPE *yyls1 = yyls;

	/* Each stack pointer address is followed by the size of the
	   data in use in that stack, in bytes.  This used to be a
	   conditional around just the two extra args, but that might
	   be undefined if yyoverflow is a macro.  */
	yyoverflow (YY_("memory exhausted"),
		    &yyss1, yysize * sizeof (*yyssp),
		    &yyvs1, yysize * sizeof (*yyvsp),
		    &yyls1, yysize * sizeof (*yylsp),
		    &yystacksize);

	yyls = yyls1;
	yyss = yyss1;
	yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyexhaustedlab;
# else
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
	goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
	yystacksize = YYMAXDEPTH;

      {
	yytype_int16 *yyss1 = yyss;
	union yyalloc *yyptr =
	  (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
	if (! yyptr)
	  goto yyexhaustedlab;
	YYSTACK_RELOCATE (yyss_alloc, yyss);
	YYSTACK_RELOCATE (yyvs_alloc, yyvs);
	YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
	if (yyss1 != yyssa)
	  YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
		  (unsigned long int) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
	YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yyn == YYPACT_NINF)
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = YYLEX;
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yyn == 0 || yyn == YYTABLE_NINF)
	goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token.  */
  yychar = YYEMPTY;

  yystate = yyn;
  *++yyvsp = yylval;
  *++yylsp = yylloc;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     `$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location.  */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 2:

/* Line 1455 of yacc.c  */
#line 225 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      merge_nodes((yyval.node), parse_ctx->mem_pool_, T_STMT_LIST, (yyvsp[(1) - (2)].node));
      parse_ctx->stmt_tree_ = (yyval.node);
      YYACCEPT;
    ;}
    break;

  case 3:

/* Line 1455 of yacc.c  */
#line 234 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 4:

/* Line 1455 of yacc.c  */
#line 238 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 5:

/* Line 1455 of yacc.c  */
#line 253 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    if(NULL != (yyvsp[(1) - (1)].node) && T_SP_DO == (yyvsp[(1) - (1)].node)->type_) {
          ParseNode *proc_stmts = NULL;
          merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, (yyvsp[(1) - (1)].node));
          ParseNode *block_content = NULL;
          merge_nodes(block_content, parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, proc_stmts);
          malloc_non_terminal_node((yyvsp[(1) - (1)].node), parse_ctx->mem_pool_, T_SP_ANONYMOUS_BLOCK, 1, block_content);
    }
    (yyval.node) = (yyvsp[(1) - (1)].node);
    int32_t str_len = (yylsp[(1) - (1)]).last_column - (yylsp[(1) - (1)]).first_column + 1;
    (yyval.node)->pos_ = (yylsp[(1) - (1)]).first_column;
    (yyval.node)->str_len_ = str_len;
  ;}
    break;

  case 6:

/* Line 1455 of yacc.c  */
#line 266 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 7:

/* Line 1455 of yacc.c  */
#line 269 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 8:

/* Line 1455 of yacc.c  */
#line 270 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 9:

/* Line 1455 of yacc.c  */
#line 271 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 10:

/* Line 1455 of yacc.c  */
#line 272 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 11:

/* Line 1455 of yacc.c  */
#line 273 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 12:

/* Line 1455 of yacc.c  */
#line 274 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 13:

/* Line 1455 of yacc.c  */
#line 275 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 14:

/* Line 1455 of yacc.c  */
#line 276 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 15:

/* Line 1455 of yacc.c  */
#line 277 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 16:

/* Line 1455 of yacc.c  */
#line 278 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 17:

/* Line 1455 of yacc.c  */
#line 279 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 18:

/* Line 1455 of yacc.c  */
#line 280 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 19:

/* Line 1455 of yacc.c  */
#line 281 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 20:

/* Line 1455 of yacc.c  */
#line 282 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 21:

/* Line 1455 of yacc.c  */
#line 283 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 22:

/* Line 1455 of yacc.c  */
#line 284 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 23:

/* Line 1455 of yacc.c  */
#line 285 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 24:

/* Line 1455 of yacc.c  */
#line 286 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 25:

/* Line 1455 of yacc.c  */
#line 287 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 26:

/* Line 1455 of yacc.c  */
#line 296 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 27:

/* Line 1455 of yacc.c  */
#line 297 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 28:

/* Line 1455 of yacc.c  */
#line 298 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 29:

/* Line 1455 of yacc.c  */
#line 299 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 30:

/* Line 1455 of yacc.c  */
#line 300 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 31:

/* Line 1455 of yacc.c  */
#line 305 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column, 2, ';', END_P);
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
    ;}
    break;

  case 32:

/* Line 1455 of yacc.c  */
#line 312 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column, 2, ';', END_P);
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
    ;}
    break;

  case 33:

/* Line 1455 of yacc.c  */
#line 319 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, (yylsp[(1) - (2)]).first_column, (yylsp[(1) - (2)]).last_column, 2, ';', END_P);
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
    ;}
    break;

  case 34:

/* Line 1455 of yacc.c  */
#line 326 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, (yylsp[(1) - (4)]).first_column, (yylsp[(1) - (4)]).last_column, 2, ';', END_P);
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
    ;}
    break;

  case 35:

/* Line 1455 of yacc.c  */
#line 333 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, (yylsp[(1) - (2)]).first_column, (yylsp[(1) - (2)]).last_column, 2, ';', END_P);
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
    ;}
    break;

  case 36:

/* Line 1455 of yacc.c  */
#line 340 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, (yylsp[(1) - (2)]).first_column, (yylsp[(1) - (2)]).last_column, 2, ';', END_P);
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
    ;}
    break;

  case 37:

/* Line 1455 of yacc.c  */
#line 347 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      //read sql query string直到读到token';'或者END_P
      do_parse_sql_stmt((yyval.node), parse_ctx, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column, 2, ';', END_P);
    ;}
    break;

  case 38:

/* Line 1455 of yacc.c  */
#line 352 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SQL_STMT);
      (yyval.node)->str_value_ = parse_strdup("COMMIT", parse_ctx->mem_pool_, &((yyval.node)->str_len_));
      if (OB_UNLIKELY(NULL == (yyval.node)->str_value_)) {
        YY_FATAL_ERROR("no more memory to malloc 'COMMIT; string\n");
      }
    ;}
    break;

  case 39:

/* Line 1455 of yacc.c  */
#line 360 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SQL_STMT);
      (yyval.node)->str_value_ = parse_strdup("ROLLBACK", parse_ctx->mem_pool_, &((yyval.node)->str_len_));
      if (OB_UNLIKELY(NULL == (yyval.node)->str_value_)) {
        YY_FATAL_ERROR("no more memory to malloc 'ROLLBACK; string\n");
      }
    ;}
    break;

  case 40:

/* Line 1455 of yacc.c  */
#line 368 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    //read sql query string直到读到token';'或者END_P
    ParseNode *sql_stmt = NULL;
    do_parse_sql_stmt(sql_stmt, parse_ctx, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column, 2, ';', END_P);
    malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
  ;}
    break;

  case 41:

/* Line 1455 of yacc.c  */
#line 375 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      //read sql query string直到读到token';'或者END_P
      ParseNode *sql_stmt = NULL;
      do_parse_sql_stmt(sql_stmt, parse_ctx, (yylsp[(1) - (2)]).first_column, (yylsp[(1) - (2)]).last_column, 2, ';', END_P);
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SQL_STMT, 1, sql_stmt);
  ;}
    break;

  case 42:

/* Line 1455 of yacc.c  */
#line 385 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      if (NULL == (yyvsp[(2) - (2)].node)) {
        obpl_mysql_yyerror(&(yylsp[(2) - (2)]), parse_ctx, "Syntax Error, Invalid expr to be evaluated\n");
        YYERROR;
      } else if (T_OBJ_ACCESS_REF == (yyvsp[(2) - (2)].node)->type_) {
        ParseNode *cur_node = (yyvsp[(2) - (2)].node);
        ParseNode *last_node = NULL;
        while (NULL != cur_node->children_[1] && T_OBJ_ACCESS_REF == cur_node->children_[1]->type_) {
          last_node = cur_node;
          cur_node = cur_node->children_[1];
        }
        if (OB_UNLIKELY(NULL == cur_node || NULL != cur_node->children_[1] || NULL == cur_node->children_[0])) {
          obpl_mysql_yyerror(&(yylsp[(2) - (2)]), parse_ctx, "Syntax Error, Invalid ObjAccess string\n");
          YYERROR;
        } else if (T_IDENT == cur_node->children_[0]->type_
            && 6 == cur_node->children_[0]->str_len_
            && 0 == strncasecmp(cur_node->children_[0]->str_value_, "EXTEND", 6)) {
          if (OB_UNLIKELY(NULL == last_node)) {
            obpl_mysql_yyerror(&(yylsp[(2) - (2)]), parse_ctx, "Syntax Error, Invalid ObjAccess string\n");
            YYERROR;
          } else {
            last_node->children_[1] = NULL;
            malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_EXTEND, 2, (yyvsp[(2) - (2)].node), NULL);
          }
        } else if (T_FUN_SYS == cur_node->children_[0]->type_
            && NULL != cur_node->children_[0]->children_[0]
            && T_IDENT == cur_node->children_[0]->children_[0]->type_
            && 6 == cur_node->children_[0]->children_[0]->str_len_
            && 0 == strncasecmp(cur_node->children_[0]->children_[0]->str_value_, "EXTEND", 6)) {
          if (OB_UNLIKELY(NULL == last_node)) {
            obpl_mysql_yyerror(&(yylsp[(2) - (2)]), parse_ctx, "Syntax Error, Invalid ObjAccess string\n");
            YYERROR;
          } else {
            last_node->children_[1] = NULL;
            malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_EXTEND, 2, (yyvsp[(2) - (2)].node), cur_node->children_[0]);
          }
        } else {
        ParseNode *do_expr_list = NULL;
        merge_nodes(do_expr_list, parse_ctx->mem_pool_, T_EXPR_LIST, (yyvsp[(2) - (2)].node));
        malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_DO, 1, do_expr_list);
        }
      } else {
        ParseNode *do_expr_list = NULL;
        merge_nodes(do_expr_list, parse_ctx->mem_pool_, T_EXPR_LIST, (yyvsp[(2) - (2)].node));
        for (int64_t i = 0; i < do_expr_list->num_child_; ++i) {
          if (T_COLUMN_REF == do_expr_list->children_[i]->type_) {
            obpl_mysql_yyerror(&(yylsp[(2) - (2)]), parse_ctx, "Syntax Error, DO statement cannot reference to a table\n");
            YYERROR;
            break;
          }
        }
        malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_DO, 1, do_expr_list);
      }
    ;}
    break;

  case 43:

/* Line 1455 of yacc.c  */
#line 439 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(2) - (2)].node); ;}
    break;

  case 44:

/* Line 1455 of yacc.c  */
#line 440 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(2) - (2)].node); ;}
    break;

  case 45:

/* Line 1455 of yacc.c  */
#line 441 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(2) - (2)].node); ;}
    break;

  case 46:

/* Line 1455 of yacc.c  */
#line 446 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_CALL_STMT, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 47:

/* Line 1455 of yacc.c  */
#line 450 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      if (!parse_ctx->is_inner_parse_) {
        obpl_mysql_yyerror(&(yylsp[(2) - (2)]), parse_ctx, "Syntax Error\n");
        YYERROR; //生成一个语法错误
      }
      (yyval.node) = (yyvsp[(2) - (2)].node);
    ;}
    break;

  case 48:

/* Line 1455 of yacc.c  */
#line 460 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 49:

/* Line 1455 of yacc.c  */
#line 462 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      merge_nodes((yyval.node), parse_ctx->mem_pool_, T_SP_CPARAM_LIST, (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 50:

/* Line 1455 of yacc.c  */
#line 469 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      if ((yyvsp[(1) - (3)].node) == NULL || (yyvsp[(3) - (3)].node) == NULL) {
        YYERROR;
      }
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 51:

/* Line 1455 of yacc.c  */
#line 475 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 52:

/* Line 1455 of yacc.c  */
#line 480 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    if (NULL == (yyvsp[(1) - (2)].node) && NULL != (yyvsp[(2) - (2)].node)) {
      YYERROR;
    }
    if (NULL == (yyvsp[(2) - (2)].node)) {
      (yyval.node) = (yyvsp[(1) - (2)].node);
    }
    if (NULL != (yyvsp[(2) - (2)].node)) {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_CPARAM, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    }
      if (NULL != (yyvsp[(1) - (2)].node))
    copy_node_abs_location((yyvsp[(1) - (2)].node)->stmt_loc_, (yylsp[(1) - (2)]));
  ;}
    break;

  case 53:

/* Line 1455 of yacc.c  */
#line 496 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 54:

/* Line 1455 of yacc.c  */
#line 498 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    if (NULL == (yyvsp[(2) - (2)].node)) YYERROR; (yyval.node) = (yyvsp[(2) - (2)].node);
      if (NULL != (yyvsp[(2) - (2)].node))
    copy_node_abs_location((yyvsp[(2) - (2)].node)->stmt_loc_, (yylsp[(2) - (2)]));
  ;}
    break;

  case 55:

/* Line 1455 of yacc.c  */
#line 506 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      //same as expr in sql rule, and terminate when read ';'
      do_parse_sql_expr_rule((yyval.node), parse_ctx, 3, ',', ')', PARAM_ASSIGN_OPERATOR);
    ;}
    break;

  case 56:

/* Line 1455 of yacc.c  */
#line 519 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_NAME, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 57:

/* Line 1455 of yacc.c  */
#line 523 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_NAME, 2, NULL, (yyvsp[(1) - (1)].node));
    ;}
    break;

  case 58:

/* Line 1455 of yacc.c  */
#line 530 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_ACCESS_NAME, 3, (yyvsp[(1) - (5)].node), (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    ;}
    break;

  case 59:

/* Line 1455 of yacc.c  */
#line 534 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_ACCESS_NAME, 3, NULL, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 60:

/* Line 1455 of yacc.c  */
#line 538 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_ACCESS_NAME, 3, NULL, NULL, (yyvsp[(1) - (1)].node));
    ;}
    break;

  case 61:

/* Line 1455 of yacc.c  */
#line 544 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 62:

/* Line 1455 of yacc.c  */
#line 546 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      get_non_reserved_node((yyval.node), parse_ctx->mem_pool_, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column);
    ;}
    break;

  case 110:

/* Line 1455 of yacc.c  */
#line 608 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      const char *stmt_str = parse_ctx->orig_stmt_str_ + (yylsp[(3) - (3)]).first_column;
      int32_t str_len = (yylsp[(3) - (3)]).last_column - (yylsp[(3) - (3)]).first_column + 1;
      (yyvsp[(3) - (3)].node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr((yyvsp[(3) - (3)].node)->str_value_);
      (yyvsp[(3) - (3)].node)->str_len_ = str_len;
      (yyvsp[(3) - (3)].node)->str_off_ = (yylsp[(3) - (3)]).first_column;
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_PACKAGE_CREATE, 1, (yyvsp[(3) - (3)].node));
      (yyval.node)->int32_values_[0] = (yyvsp[(2) - (3)].ival);
      (yyval.node)->int32_values_[1] = 0;
    ;}
    break;

  case 111:

/* Line 1455 of yacc.c  */
#line 623 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *pkg_decl_stmts = NULL;
      merge_nodes(pkg_decl_stmts, parse_ctx->mem_pool_, T_PACKAGE_STMTS, (yyvsp[(4) - (6)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_PACKAGE_BLOCK, 4, (yyvsp[(2) - (6)].node), (yyvsp[(3) - (6)].node), pkg_decl_stmts, (yyvsp[(6) - (6)].node));
      if (parse_ctx->is_inner_parse_) {
        const char *stmt_str = parse_ctx->orig_stmt_str_ + (yylsp[(4) - (6)]).first_column;
        int32_t str_len = (yylsp[(6) - (6)]).last_column - (yylsp[(4) - (6)]).first_column + 1;
        (yyval.node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
        check_ptr((yyval.node)->str_value_);
        (yyval.node)->str_len_ = str_len;
        (yyval.node)->str_off_ = (yylsp[(4) - (6)]).first_column;
      }
    ;}
    break;

  case 112:

/* Line 1455 of yacc.c  */
#line 640 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_INVOKE);
      (yyval.node)->value_ = SP_CURRENT_USER;
    ;}
    break;

  case 113:

/* Line 1455 of yacc.c  */
#line 645 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_INVOKE);
      (yyval.node)->value_ = SP_DEFINER;
    ;}
    break;

  case 114:

/* Line 1455 of yacc.c  */
#line 652 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 115:

/* Line 1455 of yacc.c  */
#line 657 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 116:

/* Line 1455 of yacc.c  */
#line 660 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 117:

/* Line 1455 of yacc.c  */
#line 664 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 118:

/* Line 1455 of yacc.c  */
#line 666 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      merge_nodes((yyval.node), parse_ctx->mem_pool_, T_SP_CLAUSE_LIST, (yyvsp[(1) - (1)].node));
    ;}
    break;

  case 119:

/* Line 1455 of yacc.c  */
#line 672 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = 0; ;}
    break;

  case 120:

/* Line 1455 of yacc.c  */
#line 673 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = 1; ;}
    break;

  case 121:

/* Line 1455 of yacc.c  */
#line 677 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 122:

/* Line 1455 of yacc.c  */
#line 679 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      merge_nodes((yyval.node), parse_ctx->mem_pool_, T_PACKAGE_STMTS, (yyvsp[(1) - (1)].node));
    ;}
    break;

  case 123:

/* Line 1455 of yacc.c  */
#line 686 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 124:

/* Line 1455 of yacc.c  */
#line 689 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 125:

/* Line 1455 of yacc.c  */
#line 692 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 126:

/* Line 1455 of yacc.c  */
#line 693 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 127:

/* Line 1455 of yacc.c  */
#line 694 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 128:

/* Line 1455 of yacc.c  */
#line 699 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      const char *stmt_str = parse_ctx->stmt_str_ + (yylsp[(1) - (7)]).first_column;
      int32_t str_len = (yylsp[(7) - (7)]).last_column - (yylsp[(1) - (7)]).first_column + 1;
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SUB_FUNC_DECL, 5, (yyvsp[(2) - (7)].node), (yyvsp[(4) - (7)].node), (yyvsp[(7) - (7)].node), NULL, NULL);
      check_ptr((yyval.node));
      (yyval.node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr((yyval.node)->str_value_);
      (yyval.node)->str_len_ = str_len;
    ;}
    break;

  case 129:

/* Line 1455 of yacc.c  */
#line 712 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      const char *stmt_str = parse_ctx->stmt_str_ + (yylsp[(1) - (5)]).first_column;
      int32_t str_len = (yylsp[(5) - (5)]).last_column - (yylsp[(1) - (5)]).first_column + 1;
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SUB_PROC_DECL, 3, (yyvsp[(2) - (5)].node), (yyvsp[(4) - (5)].node), NULL);
      check_ptr((yyval.node));
      (yyval.node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr((yyval.node)->str_value_);
      (yyval.node)->str_len_ = str_len;
    ;}
    break;

  case 130:

/* Line 1455 of yacc.c  */
#line 724 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 131:

/* Line 1455 of yacc.c  */
#line 725 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 132:

/* Line 1455 of yacc.c  */
#line 735 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      check_ptr((yyvsp[(3) - (3)].node));
      const char *stmt_str = parse_ctx->orig_stmt_str_ + (yylsp[(3) - (3)]).first_column;
      int32_t str_len = (yylsp[(3) - (3)]).last_column - (yylsp[(3) - (3)]).first_column + 1;
      (yyvsp[(3) - (3)].node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr((yyvsp[(3) - (3)].node)->str_value_);
      (yyvsp[(3) - (3)].node)->str_len_ = str_len;
      (yyvsp[(3) - (3)].node)->str_off_ = (yylsp[(3) - (3)]).first_column;
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_PACKAGE_CREATE_BODY, 1, (yyvsp[(3) - (3)].node));
      (yyval.node)->int32_values_[0] = (yyvsp[(2) - (3)].ival);
      (yyval.node)->int32_values_[1] = 0;
    ;}
    break;

  case 133:

/* Line 1455 of yacc.c  */
#line 752 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *proc_stmts = NULL;
      ParseNode *decl_ext_list = NULL;
      merge_nodes(decl_ext_list, parse_ctx->mem_pool_, T_PACKAGE_BODY_STMTS, (yyvsp[(4) - (7)].node));
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, (yyvsp[(5) - (7)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_PACKAGE_BODY_BLOCK, 4, (yyvsp[(3) - (7)].node), decl_ext_list, proc_stmts, (yyvsp[(7) - (7)].node));
      if (parse_ctx->is_inner_parse_) {
        const char *stmt_str = parse_ctx->orig_stmt_str_ + (yylsp[(4) - (7)]).first_column;
        int32_t str_len = (yylsp[(7) - (7)]).last_column - (yylsp[(4) - (7)]).first_column + 1;
        (yyval.node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
        check_ptr((yyval.node)->str_value_);
        (yyval.node)->str_len_ = str_len;
        (yyval.node)->str_off_ = (yylsp[(4) - (7)]).first_column;
      }
    ;}
    break;

  case 134:

/* Line 1455 of yacc.c  */
#line 768 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *proc_stmts = NULL;
      ParseNode *decl_ext_list = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, (yyvsp[(4) - (6)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_PACKAGE_BODY_BLOCK, 4, (yyvsp[(3) - (6)].node), decl_ext_list, proc_stmts, (yyvsp[(6) - (6)].node));
      if (parse_ctx->is_inner_parse_) {
        const char *stmt_str = parse_ctx->orig_stmt_str_ + (yylsp[(4) - (6)]).first_column;
        int32_t str_len = (yylsp[(6) - (6)]).last_column - (yylsp[(4) - (6)]).first_column + 1;
        (yyval.node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
        check_ptr((yyval.node)->str_value_);
        (yyval.node)->str_len_ = str_len;
        (yyval.node)->str_off_ = (yylsp[(4) - (6)]).first_column;
      }
    ;}
    break;

  case 135:

/* Line 1455 of yacc.c  */
#line 783 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *proc_stmts = NULL;
      ParseNode *decl_ext_list = NULL;
      merge_nodes(decl_ext_list, parse_ctx->mem_pool_, T_PACKAGE_BODY_STMTS, (yyvsp[(4) - (6)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_PACKAGE_BODY_BLOCK, 4, (yyvsp[(3) - (6)].node), decl_ext_list, proc_stmts, (yyvsp[(6) - (6)].node));
      if (parse_ctx->is_inner_parse_) {
        const char *stmt_str = parse_ctx->orig_stmt_str_ + (yylsp[(4) - (6)]).first_column;
        int32_t str_len = (yylsp[(6) - (6)]).last_column - (yylsp[(4) - (6)]).first_column + 1;
        (yyval.node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
        check_ptr((yyval.node)->str_value_);
        (yyval.node)->str_len_ = str_len;
      }
    ;}
    break;

  case 136:

/* Line 1455 of yacc.c  */
#line 797 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_PACKAGE_BODY_BLOCK, 4, (yyvsp[(3) - (5)].node), NULL, NULL, (yyvsp[(5) - (5)].node));
      if (parse_ctx->is_inner_parse_) {
        const char *stmt_str = parse_ctx->orig_stmt_str_ + (yylsp[(4) - (5)]).first_column;
        int32_t str_len = (yylsp[(5) - (5)]).last_column - (yylsp[(4) - (5)]).first_column + 1;
        (yyval.node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
        check_ptr((yyval.node)->str_value_);
        (yyval.node)->str_len_ = str_len;
        (yyval.node)->str_off_ = (yylsp[(4) - (5)]).first_column;
      }
    ;}
    break;

  case 137:

/* Line 1455 of yacc.c  */
#line 817 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_PACKAGE_DROP, 1, (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 0;
    ;}
    break;

  case 138:

/* Line 1455 of yacc.c  */
#line 822 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_PACKAGE_DROP, 1, (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 139:

/* Line 1455 of yacc.c  */
#line 830 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_PRAGMA_INTERFACE, 1, (yyvsp[(6) - (7)].node));
  ;}
    break;

  case 140:

/* Line 1455 of yacc.c  */
#line 836 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 141:

/* Line 1455 of yacc.c  */
#line 838 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 142:

/* Line 1455 of yacc.c  */
#line 844 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 143:

/* Line 1455 of yacc.c  */
#line 845 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 144:

/* Line 1455 of yacc.c  */
#line 846 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 145:

/* Line 1455 of yacc.c  */
#line 847 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 146:

/* Line 1455 of yacc.c  */
#line 849 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      check_ptr((yyvsp[(2) - (2)].node));
      const char *stmt_str = parse_ctx->stmt_str_ + (yylsp[(1) - (2)]).first_column;
      int32_t str_len = (yylsp[(2) - (2)]).last_column - (yylsp[(1) - (2)]).first_column + 1;
      (yyvsp[(2) - (2)].node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr((yyvsp[(2) - (2)].node)->str_value_);
      (yyvsp[(2) - (2)].node)->str_len_ = str_len;
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SUB_FUNC_DEF, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 147:

/* Line 1455 of yacc.c  */
#line 859 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      check_ptr((yyvsp[(2) - (2)].node));
      const char *stmt_str = parse_ctx->stmt_str_ + (yylsp[(1) - (2)]).first_column;
      int32_t str_len = (yylsp[(2) - (2)]).last_column - (yylsp[(1) - (2)]).first_column + 1;
      (yyvsp[(2) - (2)].node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr((yyvsp[(2) - (2)].node)->str_value_);
      (yyvsp[(2) - (2)].node)->str_len_ = str_len;
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SUB_PROC_DEF, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 148:

/* Line 1455 of yacc.c  */
#line 877 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      check_ptr((yyvsp[(3) - (3)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_TG_CREATE, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->int32_values_[0] = 0; // or_replace = false
      (yyval.node)->int32_values_[1] = 0; // editionable = false
    ;}
    break;

  case 149:

/* Line 1455 of yacc.c  */
#line 887 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      check_ptr((yyvsp[(3) - (3)].node));
      const char *stmt_str = parse_ctx->stmt_str_ + (yylsp[(1) - (3)]).first_column;
      int32_t str_len = (yylsp[(3) - (3)]).last_column - (yylsp[(1) - (3)]).first_column + 1;
      (yyvsp[(3) - (3)].node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr((yyvsp[(3) - (3)].node)->str_value_);
      (yyvsp[(3) - (3)].node)->str_len_ = str_len;
      (yyvsp[(3) - (3)].node)->str_off_ = (yylsp[(1) - (3)]).first_column;
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_TG_SOURCE, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 150:

/* Line 1455 of yacc.c  */
#line 901 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_TG_SIMPLE_DML, 3, (yyvsp[(2) - (8)].node), (yyvsp[(4) - (8)].node), (yyvsp[(8) - (8)].node));
      (yyval.node)->int16_values_[0] = (yyvsp[(1) - (8)].ival);
    ;}
    break;

  case 151:

/* Line 1455 of yacc.c  */
#line 907 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_BEFORE; ;}
    break;

  case 152:

/* Line 1455 of yacc.c  */
#line 908 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_AFTER; ;}
    break;

  case 153:

/* Line 1455 of yacc.c  */
#line 912 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_INSERT); ;}
    break;

  case 154:

/* Line 1455 of yacc.c  */
#line 913 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_DELETE); ;}
    break;

  case 155:

/* Line 1455 of yacc.c  */
#line 914 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_UPDATE); ;}
    break;

  case 156:

/* Line 1455 of yacc.c  */
#line 919 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    (yyval.node) = (yyvsp[(1) - (1)].node);
    const char *body_str = parse_ctx->stmt_str_ + (yylsp[(1) - (1)]).first_column;
    int32_t body_len = (yylsp[(1) - (1)]).last_column - (yylsp[(1) - (1)]).first_column + 1 + 2;
    char *dup_body = NULL;
    if (OB_LIKELY(NULL != (dup_body = (char *)parse_malloc(body_len + 1, parse_ctx->mem_pool_)))) {
      memmove(dup_body, body_str, body_len - 2);
      dup_body[body_len - 2] = ';';
      dup_body[body_len - 1] = '\n';
      dup_body[body_len] = '\0';
    }
    check_ptr(dup_body);
    (yyval.node)->str_value_ = dup_body;
    (yyval.node)->str_len_ = body_len;
    (yyval.node)->str_off_ = (yylsp[(1) - (1)]).first_column;
  ;}
    break;

  case 157:

/* Line 1455 of yacc.c  */
#line 944 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_TG_DROP, 1, (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = (yyvsp[(3) - (4)].ival);
    ;}
    break;

  case 158:

/* Line 1455 of yacc.c  */
#line 957 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      check_ptr((yyvsp[(9) - (9)].node));
      ParseNode *sp_clause_node = NULL;
      merge_nodes(sp_clause_node, parse_ctx->mem_pool_, T_SP_CLAUSE_LIST, (yyvsp[(8) - (9)].node));
      const char *stmt_str = parse_ctx->stmt_str_ + (yylsp[(9) - (9)]).first_column;
      int32_t str_len = (yylsp[(9) - (9)]).last_column - (yylsp[(9) - (9)]).first_column + 1;
      (yyvsp[(9) - (9)].node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr((yyvsp[(9) - (9)].node)->str_value_);
      (yyvsp[(9) - (9)].node)->str_len_ = str_len;
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_CREATE, 5, (yyvsp[(2) - (9)].node), (yyvsp[(4) - (9)].node), (yyvsp[(6) - (9)].node), sp_clause_node, (yyvsp[(9) - (9)].node));
    ;}
    break;

  case 159:

/* Line 1455 of yacc.c  */
#line 969 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      check_ptr((yyvsp[(8) - (8)].node));
      const char *stmt_str = parse_ctx->stmt_str_ + (yylsp[(8) - (8)]).first_column;
      int32_t str_len = (yylsp[(8) - (8)]).last_column - (yylsp[(8) - (8)]).first_column + 1;
      (yyvsp[(8) - (8)].node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr((yyvsp[(8) - (8)].node)->str_value_);
      (yyvsp[(8) - (8)].node)->str_len_ = str_len;
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_CREATE, 5, (yyvsp[(2) - (8)].node), (yyvsp[(4) - (8)].node), (yyvsp[(6) - (8)].node), NULL, (yyvsp[(8) - (8)].node));
    ;}
    break;

  case 160:

/* Line 1455 of yacc.c  */
#line 982 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      check_ptr((yyvsp[(11) - (11)].node));
      ParseNode *sp_clause_node = NULL;
      merge_nodes(sp_clause_node, parse_ctx->mem_pool_, T_SP_CLAUSE_LIST, (yyvsp[(10) - (11)].node));
      const char *stmt_str = parse_ctx->stmt_str_ + (yylsp[(11) - (11)]).first_column;
      int32_t str_len = (yylsp[(11) - (11)]).last_column - (yylsp[(11) - (11)]).first_column + 1;
      (yyvsp[(11) - (11)].node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr((yyvsp[(11) - (11)].node)->str_value_);
      (yyvsp[(11) - (11)].node)->str_len_ = str_len;
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SF_CREATE, 6, (yyvsp[(2) - (11)].node), (yyvsp[(4) - (11)].node), (yyvsp[(6) - (11)].node), (yyvsp[(9) - (11)].node), sp_clause_node, (yyvsp[(11) - (11)].node));
    ;}
    break;

  case 161:

/* Line 1455 of yacc.c  */
#line 994 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      check_ptr((yyvsp[(10) - (10)].node));
      const char *stmt_str = parse_ctx->stmt_str_ + (yylsp[(10) - (10)]).first_column;
      int32_t str_len = (yylsp[(10) - (10)]).last_column - (yylsp[(10) - (10)]).first_column + 1;
      (yyvsp[(10) - (10)].node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr((yyvsp[(10) - (10)].node)->str_value_);
      (yyvsp[(10) - (10)].node)->str_len_ = str_len;
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SF_CREATE, 6, (yyvsp[(2) - (10)].node), (yyvsp[(4) - (10)].node), (yyvsp[(6) - (10)].node), (yyvsp[(9) - (10)].node), NULL, (yyvsp[(10) - (10)].node));
    ;}
    break;

  case 162:

/* Line 1455 of yacc.c  */
#line 1006 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 163:

/* Line 1455 of yacc.c  */
#line 1008 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_USER_WITH_HOST_NAME, 2, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
  ;}
    break;

  case 164:

/* Line 1455 of yacc.c  */
#line 1012 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    ParseNode *user_node = NULL;
    malloc_terminal_node(user_node, parse_ctx->mem_pool_, T_IDENT);
    user_node->str_value_ = "CURRENT_USER";
    user_node->str_len_ = strlen("CURRENT_USER");
    malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_USER_WITH_HOST_NAME, 2, user_node, NULL);
  ;}
    break;

  case 165:

/* Line 1455 of yacc.c  */
#line 1023 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
  (yyval.node) = (yyvsp[(1) - (1)].node);
;}
    break;

  case 166:

/* Line 1455 of yacc.c  */
#line 1027 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
  (yyval.node) = (yyvsp[(1) - (1)].node);
;}
    break;

  case 167:

/* Line 1455 of yacc.c  */
#line 1033 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
  (yyval.node) = (yyvsp[(1) - (1)].node);
;}
    break;

  case 168:

/* Line 1455 of yacc.c  */
#line 1037 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
  (yyval.node) = NULL;
;}
    break;

  case 169:

/* Line 1455 of yacc.c  */
#line 1044 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 170:

/* Line 1455 of yacc.c  */
#line 1045 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 171:

/* Line 1455 of yacc.c  */
#line 1049 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 172:

/* Line 1455 of yacc.c  */
#line 1051 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      merge_nodes((yyval.node), parse_ctx->mem_pool_, T_SP_PARAM_LIST, (yyvsp[(1) - (1)].node));
    ;}
    break;

  case 173:

/* Line 1455 of yacc.c  */
#line 1058 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 174:

/* Line 1455 of yacc.c  */
#line 1061 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 175:

/* Line 1455 of yacc.c  */
#line 1066 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_PARAM, 3, (yyvsp[(2) - (4)].node), (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = (yyvsp[(1) - (4)].ival);
    ;}
    break;

  case 176:

/* Line 1455 of yacc.c  */
#line 1074 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 177:

/* Line 1455 of yacc.c  */
#line 1076 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      merge_nodes((yyval.node), parse_ctx->mem_pool_, T_SP_PARAM_LIST, (yyvsp[(1) - (1)].node));
    ;}
    break;

  case 178:

/* Line 1455 of yacc.c  */
#line 1083 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 179:

/* Line 1455 of yacc.c  */
#line 1086 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 180:

/* Line 1455 of yacc.c  */
#line 1091 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_PARAM, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
      (yyval.node)->value_ = MODE_IN;
    ;}
    break;

  case 181:

/* Line 1455 of yacc.c  */
#line 1098 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = MODE_IN; ;}
    break;

  case 182:

/* Line 1455 of yacc.c  */
#line 1099 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = MODE_IN; ;}
    break;

  case 183:

/* Line 1455 of yacc.c  */
#line 1100 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = MODE_OUT; ;}
    break;

  case 184:

/* Line 1455 of yacc.c  */
#line 1101 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = MODE_INOUT; ;}
    break;

  case 185:

/* Line 1455 of yacc.c  */
#line 1105 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 186:

/* Line 1455 of yacc.c  */
#line 1109 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_COLUMN_REF, 3, NULL, NULL, (yyvsp[(1) - (1)].node));
    ;}
    break;

  case 187:

/* Line 1455 of yacc.c  */
#line 1113 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_COLUMN_REF, 3, NULL, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 188:

/* Line 1455 of yacc.c  */
#line 1117 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_COLUMN_REF, 3, NULL, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 189:

/* Line 1455 of yacc.c  */
#line 1121 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_COLUMN_REF, 3, (yyvsp[(1) - (5)].node), (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    ;}
    break;

  case 190:

/* Line 1455 of yacc.c  */
#line 1127 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 191:

/* Line 1455 of yacc.c  */
#line 1129 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 192:

/* Line 1455 of yacc.c  */
#line 1135 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 193:

/* Line 1455 of yacc.c  */
#line 1137 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_DETERMINISTIC);
  ;}
    break;

  case 194:

/* Line 1455 of yacc.c  */
#line 1140 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { ;}
    break;

  case 195:

/* Line 1455 of yacc.c  */
#line 1144 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { ;}
    break;

  case 196:

/* Line 1455 of yacc.c  */
#line 1145 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { /* Just parse it, we only have one language for now. */ (yyval.node) = NULL; ;}
    break;

  case 197:

/* Line 1455 of yacc.c  */
#line 1146 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {;}
    break;

  case 198:

/* Line 1455 of yacc.c  */
#line 1147 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {;}
    break;

  case 199:

/* Line 1455 of yacc.c  */
#line 1148 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {;}
    break;

  case 200:

/* Line 1455 of yacc.c  */
#line 1149 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {;}
    break;

  case 201:

/* Line 1455 of yacc.c  */
#line 1151 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_INVOKE);
    (yyval.node)->value_ = SP_DEFINER;
  ;}
    break;

  case 202:

/* Line 1455 of yacc.c  */
#line 1156 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_INVOKE);
    (yyval.node)->value_ = SP_INVOKER;
  ;}
    break;

  case 203:

/* Line 1455 of yacc.c  */
#line 1163 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 204:

/* Line 1455 of yacc.c  */
#line 1167 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 205:

/* Line 1455 of yacc.c  */
#line 1177 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_ALTER, 2, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 206:

/* Line 1455 of yacc.c  */
#line 1184 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SF_ALTER, 2, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 207:

/* Line 1455 of yacc.c  */
#line 1190 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 208:

/* Line 1455 of yacc.c  */
#line 1192 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {

    ;}
    break;

  case 209:

/* Line 1455 of yacc.c  */
#line 1199 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      copy_node_abs_location((yyval.node)->stmt_loc_, (yylsp[(1) - (1)]));
    ;}
    break;

  case 210:

/* Line 1455 of yacc.c  */
#line 1204 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      copy_node_abs_location((yyval.node)->stmt_loc_, (yylsp[(1) - (1)]));
    ;}
    break;

  case 211:

/* Line 1455 of yacc.c  */
#line 1212 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      const char *stmt_str = parse_ctx->stmt_str_ + (yylsp[(1) - (1)]).first_column;
      int32_t str_len = (yylsp[(1) - (1)]).last_column - (yylsp[(1) - (1)]).first_column + 1;
      (yyvsp[(1) - (1)].node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr((yyvsp[(1) - (1)].node)->str_value_);
      (yyvsp[(1) - (1)].node)->str_len_ = str_len;
    ;}
    break;

  case 212:

/* Line 1455 of yacc.c  */
#line 1224 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      copy_node_abs_location((yyval.node)->stmt_loc_, (yylsp[(1) - (1)]));
    ;}
    break;

  case 213:

/* Line 1455 of yacc.c  */
#line 1229 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      copy_node_abs_location((yyval.node)->stmt_loc_, (yylsp[(1) - (1)]));
    ;}
    break;

  case 214:

/* Line 1455 of yacc.c  */
#line 1234 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      copy_node_abs_location((yyval.node)->stmt_loc_, (yylsp[(1) - (1)]));
    ;}
    break;

  case 215:

/* Line 1455 of yacc.c  */
#line 1239 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      copy_node_abs_location((yyval.node)->stmt_loc_, (yylsp[(1) - (1)]));
    ;}
    break;

  case 216:

/* Line 1455 of yacc.c  */
#line 1244 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      copy_node_abs_location((yyval.node)->stmt_loc_, (yylsp[(1) - (1)]));
    ;}
    break;

  case 217:

/* Line 1455 of yacc.c  */
#line 1249 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      copy_node_abs_location((yyval.node)->stmt_loc_, (yylsp[(1) - (1)]));
    ;}
    break;

  case 218:

/* Line 1455 of yacc.c  */
#line 1257 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 219:

/* Line 1455 of yacc.c  */
#line 1261 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 220:

/* Line 1455 of yacc.c  */
#line 1265 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 221:

/* Line 1455 of yacc.c  */
#line 1269 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 222:

/* Line 1455 of yacc.c  */
#line 1273 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 223:

/* Line 1455 of yacc.c  */
#line 1277 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 224:

/* Line 1455 of yacc.c  */
#line 1281 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 225:

/* Line 1455 of yacc.c  */
#line 1287 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(2) - (4)].node); ;}
    break;

  case 226:

/* Line 1455 of yacc.c  */
#line 1292 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, (yyvsp[(3) - (4)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_IF, 3, (yyvsp[(1) - (4)].node), proc_stmts, (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 227:

/* Line 1455 of yacc.c  */
#line 1298 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, (yyvsp[(3) - (3)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_IF, 3, (yyvsp[(1) - (3)].node), proc_stmts, NULL);
    ;}
    break;

  case 228:

/* Line 1455 of yacc.c  */
#line 1307 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *when_list = NULL;
      merge_nodes(when_list, parse_ctx->mem_pool_, T_WHEN_LIST, (yyvsp[(3) - (6)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_CASE, 3, (yyvsp[(2) - (6)].node), when_list, (yyvsp[(4) - (6)].node));
    ;}
    break;

  case 229:

/* Line 1455 of yacc.c  */
#line 1313 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *when_list = NULL;
      merge_nodes(when_list, parse_ctx->mem_pool_, T_WHEN_LIST, (yyvsp[(3) - (5)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_CASE, 3, (yyvsp[(2) - (5)].node), when_list, NULL);
    ;}
    break;

  case 230:

/* Line 1455 of yacc.c  */
#line 1321 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 231:

/* Line 1455 of yacc.c  */
#line 1323 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 232:

/* Line 1455 of yacc.c  */
#line 1330 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_WHEN, 2, (yyvsp[(2) - (4)].node), proc_stmts);
    ;}
    break;

  case 233:

/* Line 1455 of yacc.c  */
#line 1345 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, (yyvsp[(2) - (2)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_ELSE, 1, proc_stmts);
    ;}
    break;

  case 234:

/* Line 1455 of yacc.c  */
#line 1351 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, (yyvsp[(2) - (2)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_ELSE, 1, proc_stmts);
    ;}
    break;

  case 235:

/* Line 1455 of yacc.c  */
#line 1359 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 236:

/* Line 1455 of yacc.c  */
#line 1364 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 2, NULL, NULL);
    ;}
    break;

  case 237:

/* Line 1455 of yacc.c  */
#line 1368 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *decl_list = NULL;
      merge_nodes(decl_list, parse_ctx->mem_pool_, T_SP_DECL_LIST, (yyvsp[(2) - (3)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 2, decl_list, NULL);
    ;}
    break;

  case 238:

/* Line 1455 of yacc.c  */
#line 1374 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, (yyvsp[(2) - (3)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 2, NULL, proc_stmts);
    ;}
    break;

  case 239:

/* Line 1455 of yacc.c  */
#line 1380 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *decl_list = NULL;
      ParseNode *proc_stmts = NULL;
      merge_nodes(decl_list, parse_ctx->mem_pool_, T_SP_DECL_LIST, (yyvsp[(2) - (4)].node));
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, (yyvsp[(3) - (4)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_BLOCK_CONTENT, 2, decl_list, proc_stmts);
    ;}
    break;

  case 240:

/* Line 1455 of yacc.c  */
#line 1391 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_LABELED_BLOCK, 3, (yyvsp[(1) - (4)].node), (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 241:

/* Line 1455 of yacc.c  */
#line 1397 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 242:

/* Line 1455 of yacc.c  */
#line 1401 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL ;}
    break;

  case 243:

/* Line 1455 of yacc.c  */
#line 1403 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 244:

/* Line 1455 of yacc.c  */
#line 1409 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 245:

/* Line 1455 of yacc.c  */
#line 1411 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 246:

/* Line 1455 of yacc.c  */
#line 1418 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (2)].node);
    ;}
    break;

  case 247:

/* Line 1455 of yacc.c  */
#line 1422 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 248:

/* Line 1455 of yacc.c  */
#line 1429 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 249:

/* Line 1455 of yacc.c  */
#line 1431 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 250:

/* Line 1455 of yacc.c  */
#line 1438 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *decl_idents = NULL;
      merge_nodes(decl_idents, parse_ctx->mem_pool_, T_SP_DECL_IDENT_LIST, (yyvsp[(2) - (4)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_DECL, 3, decl_idents, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
      copy_node_abs_location((yyval.node)->stmt_loc_, (yylsp[(1) - (4)]));
    ;}
    break;

  case 251:

/* Line 1455 of yacc.c  */
#line 1445 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_DECL_COND, 2, (yyvsp[(2) - (5)].node), (yyvsp[(5) - (5)].node));
      copy_node_abs_location((yyval.node)->stmt_loc_, (yylsp[(1) - (5)]));
    ;}
    break;

  case 252:

/* Line 1455 of yacc.c  */
#line 1450 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *hcond_list = NULL;
      ParseNode *proc_stmts = NULL;
      malloc_non_terminal_node(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, 1, (yyvsp[(6) - (6)].node));
      merge_nodes(hcond_list, parse_ctx->mem_pool_, T_SP_HCOND_LIST, (yyvsp[(5) - (6)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_DECL_HANDLER, 2, hcond_list, proc_stmts);
      (yyval.node)->value_ = (yyvsp[(2) - (6)].ival);
      copy_node_abs_location((yyval.node)->stmt_loc_, (yylsp[(1) - (6)]));
    ;}
    break;

  case 253:

/* Line 1455 of yacc.c  */
#line 1460 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      if ((yyvsp[(5) - (5)].node)->children_[0]->type_ != T_SELECT) {
        obpl_mysql_yyerror(&(yylsp[(5) - (5)]), parse_ctx, "Syntax Error\n");
        YYERROR;
      }
      const char *stmt_str = parse_ctx->stmt_str_ + (yylsp[(5) - (5)]).first_column;
      int32_t str_len = (yylsp[(5) - (5)]).last_column - (yylsp[(5) - (5)]).first_column + 1;
      (yyvsp[(5) - (5)].node)->str_value_ = parse_strndup(stmt_str, str_len, parse_ctx->mem_pool_);
      check_ptr((yyvsp[(5) - (5)].node)->str_value_);
      (yyvsp[(5) - (5)].node)->str_len_ = str_len;
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_DECL_CURSOR, 4, (yyvsp[(2) - (5)].node), NULL, NULL, (yyvsp[(5) - (5)].node)); //4参数和Oracle模式保持一致
      copy_node_abs_location((yyval.node)->stmt_loc_, (yylsp[(1) - (5)]));
    ;}
    break;

  case 254:

/* Line 1455 of yacc.c  */
#line 1476 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = SP_HANDLER_TYPE_EXIT; ;}
    break;

  case 255:

/* Line 1455 of yacc.c  */
#line 1477 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = SP_HANDLER_TYPE_CONTINUE; ;}
    break;

  case 256:

/* Line 1455 of yacc.c  */
#line 1481 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 257:

/* Line 1455 of yacc.c  */
#line 1483 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 258:

/* Line 1455 of yacc.c  */
#line 1489 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 259:

/* Line 1455 of yacc.c  */
#line 1490 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 260:

/* Line 1455 of yacc.c  */
#line 1491 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SQL_WARNING); ;}
    break;

  case 261:

/* Line 1455 of yacc.c  */
#line 1492 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SQL_NOT_FOUND); ;}
    break;

  case 262:

/* Line 1455 of yacc.c  */
#line 1493 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SQL_EXCEPTION); ;}
    break;

  case 263:

/* Line 1455 of yacc.c  */
#line 1498 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_CONDITION, 1, (yyvsp[(1) - (1)].node));
    ;}
    break;

  case 264:

/* Line 1455 of yacc.c  */
#line 1502 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_CONDITION, 1, (yyvsp[(1) - (1)].node));
    ;}
    break;

  case 265:

/* Line 1455 of yacc.c  */
#line 1509 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (void)((yyvsp[(2) - (3)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SQL_STATE, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 266:

/* Line 1455 of yacc.c  */
#line 1517 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_PROC_OPEN, 3, (yyvsp[(2) - (2)].node), NULL, NULL, NULL); //4参数和Oracle模式保持一致
    ;}
    break;

  case 267:

/* Line 1455 of yacc.c  */
#line 1524 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_PROC_CLOSE, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 268:

/* Line 1455 of yacc.c  */
#line 1531 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_PROC_FETCH, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 269:

/* Line 1455 of yacc.c  */
#line 1535 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_PROC_FETCH, 2, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 270:

/* Line 1455 of yacc.c  */
#line 1539 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_PROC_FETCH, 2, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node));
    ;}
    break;

  case 271:

/* Line 1455 of yacc.c  */
#line 1546 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *vars_list = NULL;
      merge_nodes(vars_list, parse_ctx->mem_pool_, T_SP_INTO_LIST, (yyvsp[(2) - (2)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_INTO_VARIABLES, 1, vars_list);
    ;}
    break;

  case 272:

/* Line 1455 of yacc.c  */
#line 1554 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 273:

/* Line 1455 of yacc.c  */
#line 1555 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 274:

/* Line 1455 of yacc.c  */
#line 1558 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 275:

/* Line 1455 of yacc.c  */
#line 1560 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 276:

/* Line 1455 of yacc.c  */
#line 1566 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 277:

/* Line 1455 of yacc.c  */
#line 1570 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 278:

/* Line 1455 of yacc.c  */
#line 1572 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_DECL_DEFAULT, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 279:

/* Line 1455 of yacc.c  */
#line 1578 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 280:

/* Line 1455 of yacc.c  */
#line 1580 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      if (NULL == (yyvsp[(2) - (2)].node)) {
        YYERROR;
      }
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_DECL_DEFAULT, 1, (yyvsp[(2) - (2)].node));
      // 拷贝EXPR语句
      const char *expr_str = parse_ctx->stmt_str_ + (yylsp[(2) - (2)]).first_column;
      int32_t expr_str_len = (yylsp[(2) - (2)]).last_column - (yylsp[(2) - (2)]).first_column + 1;
      (yyval.node)->str_value_ = parse_strndup(expr_str, expr_str_len, parse_ctx->mem_pool_);
      check_ptr((yyval.node)->str_value_);
      (yyval.node)->str_len_ = expr_str_len;
      if (NULL != (yyvsp[(2) - (2)].node)) {
        copy_node_abs_location((yyvsp[(2) - (2)].node)->stmt_loc_, (yylsp[(2) - (2)]));
      }
    ;}
    break;

  case 281:

/* Line 1455 of yacc.c  */
#line 1598 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      do_parse_sql_expr_rule((yyval.node), parse_ctx, 3, ',', ')', ';');
    ;}
    break;

  case 282:

/* Line 1455 of yacc.c  */
#line 1604 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 283:

/* Line 1455 of yacc.c  */
#line 1606 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 284:

/* Line 1455 of yacc.c  */
#line 1612 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      //same as expr in sql rule, and terminate when read ';'
      do_parse_sql_expr_rule((yyval.node), parse_ctx, 9, INTO, USING, WHEN, THEN, ';', DO, LIMIT, ',', END_KEY);
    ;}
    break;

  case 285:

/* Line 1455 of yacc.c  */
#line 1620 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, (yyvsp[(2) - (4)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_LOOP, 1, proc_stmts);
    ;}
    break;

  case 286:

/* Line 1455 of yacc.c  */
#line 1626 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, (yyvsp[(4) - (6)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_WHILE, 2, (yyvsp[(2) - (6)].node), proc_stmts);
    ;}
    break;

  case 287:

/* Line 1455 of yacc.c  */
#line 1632 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      ParseNode *proc_stmts = NULL;
      merge_nodes(proc_stmts, parse_ctx->mem_pool_, T_SP_PROC_STMT_LIST, (yyvsp[(2) - (6)].node));
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_REPEAT, 2, proc_stmts, (yyvsp[(4) - (6)].node));
    ;}
    break;

  case 288:

/* Line 1455 of yacc.c  */
#line 1641 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_LABELED_CONTROL, 3, (yyvsp[(1) - (4)].node), (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 289:

/* Line 1455 of yacc.c  */
#line 1648 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      if (NULL == (yyvsp[(2) - (2)].node)) YYERROR;
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_RETURN, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 290:

/* Line 1455 of yacc.c  */
#line 1656 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_ITERATE, 2, (yyvsp[(2) - (2)].node), NULL);
    ;}
    break;

  case 291:

/* Line 1455 of yacc.c  */
#line 1663 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_LEAVE, 2, (yyvsp[(2) - (2)].node), NULL);
    ;}
    break;

  case 292:

/* Line 1455 of yacc.c  */
#line 1670 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_DROP, 1, (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = (yyvsp[(3) - (4)].ival);
    ;}
    break;

  case 293:

/* Line 1455 of yacc.c  */
#line 1678 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SF_DROP, 1, (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = (yyvsp[(3) - (4)].ival);
    ;}
    break;

  case 294:

/* Line 1455 of yacc.c  */
#line 1685 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = 0; ;}
    break;

  case 295:

/* Line 1455 of yacc.c  */
#line 1686 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = 1; ;}
    break;

  case 296:

/* Line 1455 of yacc.c  */
#line 1691 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, ((yyvsp[(3) - (4)].ival) || (yyvsp[(4) - (4)].ival)) ? (yyvsp[(1) - (4)].ival) + (T_UTINYINT - T_TINYINT) : (yyvsp[(1) - (4)].ival));
      (yyval.node)->int16_values_[0] = (yyvsp[(2) - (4)].ival);
      (yyval.node)->int16_values_[2] = (yyvsp[(4) - (4)].ival);   /* 2 is the same index as float or number. */
    ;}
    break;

  case 297:

/* Line 1455 of yacc.c  */
#line 1697 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      if (T_FLOAT != (yyvsp[(1) - (4)].ival) && NULL != (yyvsp[(2) - (4)].node) && -1 == (yyvsp[(2) - (4)].node)->int16_values_[1]) {
        obpl_mysql_yyerror(&(yylsp[(2) - (4)]), parse_ctx, "double type not support double(M) syntax\n");
        YYERROR;
      }
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, ((yyvsp[(3) - (4)].ival) || (yyvsp[(4) - (4)].ival)) ? (yyvsp[(1) - (4)].ival) + (T_UFLOAT - T_FLOAT) : (yyvsp[(1) - (4)].ival));
      if (NULL != (yyvsp[(2) - (4)].node)) {
        (yyval.node)->int16_values_[0] = (yyvsp[(2) - (4)].node)->int16_values_[0];
        (yyval.node)->int16_values_[1] = (yyvsp[(2) - (4)].node)->int16_values_[1];
      }
      /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
      (yyval.node)->int16_values_[2] = (yyvsp[(4) - (4)].ival);
    ;}
    break;

  case 298:

/* Line 1455 of yacc.c  */
#line 1711 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, ((yyvsp[(3) - (4)].ival) || (yyvsp[(4) - (4)].ival)) ? T_UNUMBER : T_NUMBER);
      if (NULL != (yyvsp[(2) - (4)].node)) {
        (yyval.node)->int16_values_[0] = (yyvsp[(2) - (4)].node)->int16_values_[0];
        (yyval.node)->int16_values_[1] = (yyvsp[(2) - (4)].node)->int16_values_[1];
      }
      /* malloc_terminal_node() has set memory to 0 filled, so there is no else. */
      (yyval.node)->int16_values_[2] = (yyvsp[(4) - (4)].ival);
    ;}
    break;

  case 299:

/* Line 1455 of yacc.c  */
#line 1721 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, (yyvsp[(1) - (2)].ival));
      (yyval.node)->int16_values_[1] = (yyvsp[(2) - (2)].ival);
    ;}
    break;

  case 300:

/* Line 1455 of yacc.c  */
#line 1726 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, (yyvsp[(1) - (1)].ival));
    ;}
    break;

  case 301:

/* Line 1455 of yacc.c  */
#line 1730 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_CHAR, 3, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node), (yyvsp[(3) - (5)].node));
      if ((yyvsp[(2) - (5)].ival) < 0) {
        (yyvsp[(2) - (5)].ival) = 1;
      }
      (yyval.node)->int32_values_[0] = (yyvsp[(2) - (5)].ival);
      (yyval.node)->int32_values_[1] = 0; /* is char */
    ;}
    break;

  case 302:

/* Line 1455 of yacc.c  */
#line 1746 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_VARCHAR, 3, (yyvsp[(5) - (6)].node), (yyvsp[(6) - (6)].node), (yyvsp[(4) - (6)].node));
      (yyval.node)->int32_values_[0] = (yyvsp[(3) - (6)].ival);
      (yyval.node)->int32_values_[1] = 0; /* is char */
    ;}
    break;

  case 303:

/* Line 1455 of yacc.c  */
#line 1752 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_VARCHAR, 3, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node), (yyvsp[(3) - (5)].node));
      (yyval.node)->int32_values_[0] = (yyvsp[(2) - (5)].ival);
      (yyval.node)->int32_values_[1] = 0; /* is char */
    ;}
    break;

  case 304:

/* Line 1455 of yacc.c  */
#line 1758 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_CHAR);
      if ((yyvsp[(2) - (2)].ival) < 0) {
        (yyvsp[(2) - (2)].ival) = 1;
      }
      (yyval.node)->int32_values_[0] = (yyvsp[(2) - (2)].ival);
      (yyval.node)->int32_values_[1] = 1; /* is binary */
    ;}
    break;

  case 305:

/* Line 1455 of yacc.c  */
#line 1767 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_VARCHAR);
      (yyval.node)->int32_values_[0] = (yyvsp[(2) - (2)].ival);
      (yyval.node)->int32_values_[1] = 1; /* is binary */
    ;}
    break;

  case 306:

/* Line 1455 of yacc.c  */
#line 1773 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_INVALID);
      (yyval.node)->str_value_ = (yyvsp[(1) - (1)].node)->str_value_;
      (yyval.node)->str_len_ = (yyvsp[(1) - (1)].node)->str_len_;
    ;}
    break;

  case 307:

/* Line 1455 of yacc.c  */
#line 1779 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_BIT);
      (yyval.node)->int16_values_[0] = (yyvsp[(2) - (2)].ival);
    ;}
    break;

  case 308:

/* Line 1455 of yacc.c  */
#line 1784 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_TINYINT);
	  (yyval.node)->int16_values_[0] = 1;
      (yyval.node)->int16_values_[2] = 0;  // zerofill always false
    ;}
    break;

  case 309:

/* Line 1455 of yacc.c  */
#line 1790 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_TINYINT);
      (yyval.node)->int16_values_[0] = 1;
      (yyval.node)->int16_values_[2] = 0; // zerofill always false
    ;}
    break;

  case 310:

/* Line 1455 of yacc.c  */
#line 1796 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, (yyvsp[(1) - (2)].ival));
      if ((yyvsp[(1) - (2)].ival) != T_TEXT && (yyvsp[(2) - (2)].ival) != 0) {
        obpl_mysql_yyerror(&(yylsp[(2) - (2)]), parse_ctx, "not support to specify the length in parentheses\n");
        YYERROR;
      }
      (yyval.node)->int32_values_[0] = (yyvsp[(2) - (2)].ival);
      (yyval.node)->int32_values_[1] = 1; /* is binary */
    ;}
    break;

  case 311:

/* Line 1455 of yacc.c  */
#line 1806 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, (yyvsp[(1) - (5)].ival), 3, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node), (yyvsp[(3) - (5)].node));
      if ((yyvsp[(1) - (5)].ival) != T_TEXT && (yyvsp[(2) - (5)].ival) != 0) {
        obpl_mysql_yyerror(&(yylsp[(2) - (5)]), parse_ctx, "not support to specify the length in parentheses\n");
        YYERROR;
      }
      (yyval.node)->int32_values_[0] = (yyvsp[(2) - (5)].ival);
      (yyval.node)->int32_values_[1] = 0; /* is text */
    ;}
    break;

  case 312:

/* Line 1455 of yacc.c  */
#line 1816 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    ParseNode *string_list_node = NULL;
    merge_nodes(string_list_node, parse_ctx->mem_pool_, T_STRING_LIST, (yyvsp[(3) - (7)].node));
    malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_ENUM, 4, (yyvsp[(6) - (7)].node), (yyvsp[(7) - (7)].node), (yyvsp[(5) - (7)].node), string_list_node);
    (yyval.node)->int32_values_[0] = 0;//not used so far
    (yyval.node)->int32_values_[1] = 0; /* is char */
  ;}
    break;

  case 313:

/* Line 1455 of yacc.c  */
#line 1824 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    ParseNode *string_list_node = NULL;
    merge_nodes(string_list_node, parse_ctx->mem_pool_, T_STRING_LIST, (yyvsp[(3) - (7)].node));
    malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SET, 4, (yyvsp[(6) - (7)].node), (yyvsp[(7) - (7)].node), (yyvsp[(5) - (7)].node), string_list_node);
    (yyval.node)->int32_values_[0] = 0;//not used so far
    (yyval.node)->int32_values_[1] = 0; /* is char */
  ;}
    break;

  case 314:

/* Line 1455 of yacc.c  */
#line 1832 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    if ((yyvsp[(1) - (1)].node) != NULL && (yyvsp[(1) - (1)].node)->type_ == T_SP_OBJ_ACCESS_REF &&
        (yyvsp[(1) - (1)].node)->num_child_ == 2 &&
        (yyvsp[(1) - (1)].node)->children_[0] != NULL &&
        (yyvsp[(1) - (1)].node)->children_[0]->type_ == T_SP_ACCESS_NAME &&
        (yyvsp[(1) - (1)].node)->children_[0]->num_child_ == 3 &&
        (yyvsp[(1) - (1)].node)->children_[0]->children_[0] == NULL &&
        (yyvsp[(1) - (1)].node)->children_[0]->children_[1] == NULL &&
        nodename_equal((yyvsp[(1) - (1)].node)->children_[0]->children_[2], "JSON", 4)) {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_JSON);
      (yyval.node)->int32_values_[0] = 0;
    } else {
      obpl_mysql_yyerror(&(yylsp[(1) - (1)]), parse_ctx, "Syntax Error\n");
      YYERROR;
    }
  ;}
    break;

  case 315:

/* Line 1455 of yacc.c  */
#line 1849 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_ROWTYPE, 1, (yyvsp[(1) - (3)].node));
  ;}
    break;

  case 316:

/* Line 1455 of yacc.c  */
#line 1856 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_OBJ_ACCESS_REF, 2, (yyvsp[(1) - (1)].node), NULL);
    ;}
    break;

  case 317:

/* Line 1455 of yacc.c  */
#line 1861 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_TINYINT; ;}
    break;

  case 318:

/* Line 1455 of yacc.c  */
#line 1862 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_SMALLINT; ;}
    break;

  case 319:

/* Line 1455 of yacc.c  */
#line 1863 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_MEDIUMINT; ;}
    break;

  case 320:

/* Line 1455 of yacc.c  */
#line 1864 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_INT32; ;}
    break;

  case 321:

/* Line 1455 of yacc.c  */
#line 1865 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_INT; ;}
    break;

  case 322:

/* Line 1455 of yacc.c  */
#line 1869 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_FLOAT; ;}
    break;

  case 323:

/* Line 1455 of yacc.c  */
#line 1870 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_DOUBLE; ;}
    break;

  case 324:

/* Line 1455 of yacc.c  */
#line 1871 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_DOUBLE; ;}
    break;

  case 325:

/* Line 1455 of yacc.c  */
#line 1875 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_DATETIME; ;}
    break;

  case 326:

/* Line 1455 of yacc.c  */
#line 1876 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_TIMESTAMP; ;}
    break;

  case 327:

/* Line 1455 of yacc.c  */
#line 1877 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_TIME; ;}
    break;

  case 328:

/* Line 1455 of yacc.c  */
#line 1881 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_DATE; ;}
    break;

  case 329:

/* Line 1455 of yacc.c  */
#line 1882 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_YEAR; ;}
    break;

  case 330:

/* Line 1455 of yacc.c  */
#line 1886 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = (yyvsp[(2) - (3)].node)->value_; ;}
    break;

  case 331:

/* Line 1455 of yacc.c  */
#line 1887 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = -1; ;}
    break;

  case 332:

/* Line 1455 of yacc.c  */
#line 1891 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = (yyvsp[(2) - (3)].node)->value_; ;}
    break;

  case 333:

/* Line 1455 of yacc.c  */
#line 1892 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = 1; ;}
    break;

  case 334:

/* Line 1455 of yacc.c  */
#line 1897 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE);
      (yyval.node)->int16_values_[0] = (yyvsp[(2) - (5)].node)->value_;
      (yyval.node)->int16_values_[1] = (yyvsp[(4) - (5)].node)->value_;
    ;}
    break;

  case 335:

/* Line 1455 of yacc.c  */
#line 1903 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE);
      (yyval.node)->int16_values_[0] = (yyvsp[(2) - (3)].node)->value_;
      (yyval.node)->int16_values_[1] = -1;
    ;}
    break;

  case 336:

/* Line 1455 of yacc.c  */
#line 1909 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE);
    int err_no = 0;
    (yyvsp[(2) - (3)].node)->value_ = ob_strntoll((yyvsp[(2) - (3)].node)->str_value_, (yyvsp[(2) - (3)].node)->str_len_, 10, NULL, &err_no);
    (yyval.node)->int16_values_[0] = (yyvsp[(2) - (3)].node)->value_;
    (yyval.node)->int16_values_[1] = -1;
  ;}
    break;

  case 337:

/* Line 1455 of yacc.c  */
#line 1916 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 338:

/* Line 1455 of yacc.c  */
#line 1921 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE);
      if((yyvsp[(2) - (5)].node)->value_ > OB_MAX_PARSER_INT16_VALUE) {
        (yyval.node)->int16_values_[0] = OB_MAX_PARSER_INT16_VALUE;
      } else {
        (yyval.node)->int16_values_[0] = (yyvsp[(2) - (5)].node)->value_;
      }
      if((yyvsp[(4) - (5)].node)->value_ > OB_MAX_PARSER_INT16_VALUE) {
        (yyval.node)->int16_values_[1] = OB_MAX_PARSER_INT16_VALUE;
      } else {
        (yyval.node)->int16_values_[1] = (yyvsp[(4) - (5)].node)->value_;
      }
      (yyval.node)->param_num_ = 2;
    ;}
    break;

  case 339:

/* Line 1455 of yacc.c  */
#line 1936 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE);
      if((yyvsp[(2) - (3)].node)->value_ > OB_MAX_PARSER_INT16_VALUE) {
        (yyval.node)->int16_values_[0] = OB_MAX_PARSER_INT16_VALUE;
      } else {
        (yyval.node)->int16_values_[0] = (yyvsp[(2) - (3)].node)->value_;
      }
      (yyval.node)->int16_values_[1] = 0;
      (yyval.node)->param_num_ = 1;
    ;}
    break;

  case 340:

/* Line 1455 of yacc.c  */
#line 1947 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE);
      (yyval.node)->int16_values_[0] = 10;
      (yyval.node)->int16_values_[1] = 0;
      (yyval.node)->param_num_ = 0;
    ;}
    break;

  case 341:

/* Line 1455 of yacc.c  */
#line 1956 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = (yyvsp[(2) - (3)].node)->value_; ;}
    break;

  case 342:

/* Line 1455 of yacc.c  */
#line 1957 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = 0; ;}
    break;

  case 343:

/* Line 1455 of yacc.c  */
#line 1962 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.ival) = (yyvsp[(2) - (3)].node)->value_;
    ;}
    break;

  case 344:

/* Line 1455 of yacc.c  */
#line 1966 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.ival) = 0;
    ;}
    break;

  case 345:

/* Line 1455 of yacc.c  */
#line 1973 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      (yyval.node)->param_num_ = 1;
    ;}
    break;

  case 346:

/* Line 1455 of yacc.c  */
#line 1978 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      (yyval.node)->param_num_ = 1;
    ;}
    break;

  case 347:

/* Line 1455 of yacc.c  */
#line 1985 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 348:

/* Line 1455 of yacc.c  */
#line 1986 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 349:

/* Line 1455 of yacc.c  */
#line 1987 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 350:

/* Line 1455 of yacc.c  */
#line 1988 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 351:

/* Line 1455 of yacc.c  */
#line 1992 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_TINYTEXT; ;}
    break;

  case 352:

/* Line 1455 of yacc.c  */
#line 1993 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_TEXT; ;}
    break;

  case 353:

/* Line 1455 of yacc.c  */
#line 1994 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_MEDIUMTEXT; ;}
    break;

  case 354:

/* Line 1455 of yacc.c  */
#line 1995 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_LONGTEXT;  ;}
    break;

  case 355:

/* Line 1455 of yacc.c  */
#line 1996 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_MEDIUMTEXT; ;}
    break;

  case 356:

/* Line 1455 of yacc.c  */
#line 1997 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_MEDIUMTEXT; ;}
    break;

  case 357:

/* Line 1455 of yacc.c  */
#line 2001 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_TINYTEXT; ;}
    break;

  case 358:

/* Line 1455 of yacc.c  */
#line 2002 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_TEXT; ;}
    break;

  case 359:

/* Line 1455 of yacc.c  */
#line 2003 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_MEDIUMTEXT; ;}
    break;

  case 360:

/* Line 1455 of yacc.c  */
#line 2004 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = T_LONGTEXT;  ;}
    break;

  case 361:

/* Line 1455 of yacc.c  */
#line 2005 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {(yyval.ival) = T_MEDIUMTEXT; ;}
    break;

  case 362:

/* Line 1455 of yacc.c  */
#line 2009 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = (yyvsp[(1) - (1)].ival); ;}
    break;

  case 363:

/* Line 1455 of yacc.c  */
#line 2010 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = 0; ;}
    break;

  case 364:

/* Line 1455 of yacc.c  */
#line 2014 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
  // 在 `*` 处报语法错误
  // select cast('' as BINARY(-1));
  //                          *
  // select cast('' as CHARACTER(-1));
  //                             *
  int64_t val = 0;
  if (T_NUMBER == (yyvsp[(2) - (3)].node)->type_) {
    errno = 0;
    val = strtoll((yyvsp[(2) - (3)].node)->str_value_, NULL, 10);
    if (ERANGE == errno) {
      (yyval.ival) = OUT_OF_STR_LEN;// out of str_max_len
    } else if (val < 0) {
      obpl_mysql_yyerror(&(yylsp[(2) - (3)]), parse_ctx, "length cannot < 0\n");
      YYERROR;
    } else if (val > UINT32_MAX) {
      (yyval.ival) = OUT_OF_STR_LEN;// out of str_max_len
    } else if (val > INT32_MAX) {
      (yyval.ival) = DEFAULT_STR_LENGTH;
    } else {
      (yyval.ival) = val;
    }
  } else if ((yyvsp[(2) - (3)].node)->value_ < 0) {
    obpl_mysql_yyerror(&(yylsp[(2) - (3)]), parse_ctx, "length cannot < 0\n");
    YYERROR;
  } else if ((yyvsp[(2) - (3)].node)->value_ > UINT32_MAX) {
    (yyval.ival) = OUT_OF_STR_LEN;;
  } else if ((yyvsp[(2) - (3)].node)->value_ > INT32_MAX) {
    (yyval.ival) = DEFAULT_STR_LENGTH;
  } else {
    (yyval.ival) = (yyvsp[(2) - (3)].node)->value_;
  }
  // $$ = $2->param_num_;
;}
    break;

  case 365:

/* Line 1455 of yacc.c  */
#line 2052 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
   (yyval.node) = (yyvsp[(1) - (1)].node);
  ;}
    break;

  case 366:

/* Line 1455 of yacc.c  */
#line 2056 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
  ;}
    break;

  case 367:

/* Line 1455 of yacc.c  */
#line 2063 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    (yyval.node) = (yyvsp[(1) - (1)].node);
  ;}
    break;

  case 368:

/* Line 1455 of yacc.c  */
#line 2067 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
    (yyval.node) = (yyvsp[(1) - (1)].node);
  ;}
    break;

  case 369:

/* Line 1455 of yacc.c  */
#line 2074 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.ival) = (yyvsp[(1) - (1)].ival);
    ;}
    break;

  case 370:

/* Line 1455 of yacc.c  */
#line 2078 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.ival) = 1;
    ;}
    break;

  case 371:

/* Line 1455 of yacc.c  */
#line 2084 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = 1; ;}
    break;

  case 372:

/* Line 1455 of yacc.c  */
#line 2085 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = 0; ;}
    break;

  case 373:

/* Line 1455 of yacc.c  */
#line 2086 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = 0; ;}
    break;

  case 374:

/* Line 1455 of yacc.c  */
#line 2090 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = 1; ;}
    break;

  case 375:

/* Line 1455 of yacc.c  */
#line 2091 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = 0; ;}
    break;

  case 376:

/* Line 1455 of yacc.c  */
#line 2096 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_BINARY);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 377:

/* Line 1455 of yacc.c  */
#line 2100 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {(yyval.node) = 0; ;}
    break;

  case 378:

/* Line 1455 of yacc.c  */
#line 2105 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      (yyval.node)->type_ = T_VARCHAR;
      (yyval.node)->param_num_ = 0;
    ;}
    break;

  case 379:

/* Line 1455 of yacc.c  */
#line 2111 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      (yyval.node)->param_num_ = 1;
    ;}
    break;

  case 380:

/* Line 1455 of yacc.c  */
#line 2119 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      (yyval.node)->type_ = T_VARCHAR;
      (yyval.node)->param_num_ = 0;
      (yyval.node)->is_hidden_const_ = 1;
    ;}
    break;

  case 381:

/* Line 1455 of yacc.c  */
#line 2126 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      (yyval.node)->param_num_ = 1;
      (yyval.node)->is_hidden_const_ = 0;
    ;}
    break;

  case 382:

/* Line 1455 of yacc.c  */
#line 2132 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_VARCHAR);
      (yyval.node)->str_value_ = parse_strdup("binary", parse_ctx->mem_pool_, &((yyval.node)->str_len_));
      if (OB_UNLIKELY(NULL == (yyval.node)->str_value_)) {
        YY_FATAL_ERROR("no more memory to malloc 'binary; string\n");
      }
      (yyval.node)->param_num_ = 0;
      (yyval.node)->is_hidden_const_ = 1;
    ;}
    break;

  case 383:

/* Line 1455 of yacc.c  */
#line 2145 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (void)((yyvsp[(1) - (2)].node));
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_CHARSET);
      (yyval.node)->str_value_ = (yyvsp[(2) - (2)].node)->str_value_;
      (yyval.node)->str_len_ = (yyvsp[(2) - (2)].node)->str_len_;
    ;}
    break;

  case 384:

/* Line 1455 of yacc.c  */
#line 2151 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 385:

/* Line 1455 of yacc.c  */
#line 2156 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = NULL;
    ;}
    break;

  case 386:

/* Line 1455 of yacc.c  */
#line 2160 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = NULL;
    ;}
    break;

  case 387:

/* Line 1455 of yacc.c  */
#line 2167 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_terminal_node((yyval.node), parse_ctx->mem_pool_, T_COLLATION);
      (yyval.node)->str_value_ = (yyvsp[(2) - (2)].node)->str_value_;
      (yyval.node)->str_len_ = (yyvsp[(2) - (2)].node)->str_len_;
      (yyval.node)->param_num_ = (yyvsp[(2) - (2)].node)->param_num_;
    ;}
    break;

  case 388:

/* Line 1455 of yacc.c  */
#line 2177 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 389:

/* Line 1455 of yacc.c  */
#line 2180 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 390:

/* Line 1455 of yacc.c  */
#line 2185 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_SIGNAL, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 391:

/* Line 1455 of yacc.c  */
#line 2192 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_RESIGNAL, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 392:

/* Line 1455 of yacc.c  */
#line 2198 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 393:

/* Line 1455 of yacc.c  */
#line 2199 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 394:

/* Line 1455 of yacc.c  */
#line 2203 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 395:

/* Line 1455 of yacc.c  */
#line 2204 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 396:

/* Line 1455 of yacc.c  */
#line 2208 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = NULL; ;}
    break;

  case 397:

/* Line 1455 of yacc.c  */
#line 2210 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      merge_nodes((yyval.node), parse_ctx->mem_pool_, T_SP_SIGNAL_INFO_LIST, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 398:

/* Line 1455 of yacc.c  */
#line 2216 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 399:

/* Line 1455 of yacc.c  */
#line 2218 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 400:

/* Line 1455 of yacc.c  */
#line 2225 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    {
      malloc_non_terminal_node((yyval.node), parse_ctx->mem_pool_, T_SP_SIGNAL_INFO_ITEM, 1, (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = (yyvsp[(1) - (3)].ival);
    ;}
    break;

  case 401:

/* Line 1455 of yacc.c  */
#line 2232 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 402:

/* Line 1455 of yacc.c  */
#line 2233 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 403:

/* Line 1455 of yacc.c  */
#line 2234 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 404:

/* Line 1455 of yacc.c  */
#line 2235 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 405:

/* Line 1455 of yacc.c  */
#line 2239 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 406:

/* Line 1455 of yacc.c  */
#line 2240 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 407:

/* Line 1455 of yacc.c  */
#line 2244 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = DIAG_CLASS_ORIGIN; ;}
    break;

  case 408:

/* Line 1455 of yacc.c  */
#line 2245 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = DIAG_SUBCLASS_ORIGIN; ;}
    break;

  case 409:

/* Line 1455 of yacc.c  */
#line 2246 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = DIAG_CONSTRAINT_CATALOG; ;}
    break;

  case 410:

/* Line 1455 of yacc.c  */
#line 2247 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = DIAG_CONSTRAINT_SCHEMA; ;}
    break;

  case 411:

/* Line 1455 of yacc.c  */
#line 2248 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = DIAG_CONSTRAINT_NAME; ;}
    break;

  case 412:

/* Line 1455 of yacc.c  */
#line 2249 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = DIAG_CATALOG_NAME; ;}
    break;

  case 413:

/* Line 1455 of yacc.c  */
#line 2250 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = DIAG_SCHEMA_NAME; ;}
    break;

  case 414:

/* Line 1455 of yacc.c  */
#line 2251 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = DIAG_TABLE_NAME; ;}
    break;

  case 415:

/* Line 1455 of yacc.c  */
#line 2252 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = DIAG_COLUMN_NAME; ;}
    break;

  case 416:

/* Line 1455 of yacc.c  */
#line 2253 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = DIAG_CURSOR_NAME; ;}
    break;

  case 417:

/* Line 1455 of yacc.c  */
#line 2254 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = DIAG_MESSAGE_TEXT; ;}
    break;

  case 418:

/* Line 1455 of yacc.c  */
#line 2255 "../../../src/pl/parser/pl_parser_mysql_mode.y"
    { (yyval.ival) = DIAG_MYSQL_ERRNO; ;}
    break;



/* Line 1455 of yacc.c  */
#line 6694 "../../../src/pl/parser/pl_parser_mysql_mode_tab.c"
      default: break;
    }
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;
  *++yylsp = yyloc;

  /* Now `shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */

  yyn = yyr1[yyn];

  yystate = yypgoto[yyn - YYNTOKENS] + *yyssp;
  if (0 <= yystate && yystate <= YYLAST && yycheck[yystate] == *yyssp)
    yystate = yytable[yystate];
  else
    yystate = yydefgoto[yyn - YYNTOKENS];

  goto yynewstate;


/*------------------------------------.
| yyerrlab -- here on detecting error |
`------------------------------------*/
yyerrlab:
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (&yylloc, parse_ctx, YY_("syntax error"));
#else
      {
	YYSIZE_T yysize = yysyntax_error (0, yystate, yychar);
	if (yymsg_alloc < yysize && yymsg_alloc < YYSTACK_ALLOC_MAXIMUM)
	  {
	    YYSIZE_T yyalloc = 2 * yysize;
	    if (! (yysize <= yyalloc && yyalloc <= YYSTACK_ALLOC_MAXIMUM))
	      yyalloc = YYSTACK_ALLOC_MAXIMUM;
	    if (yymsg != yymsgbuf)
	      YYSTACK_FREE (yymsg);
	    yymsg = (char *) YYSTACK_ALLOC (yyalloc);
	    if (yymsg)
	      yymsg_alloc = yyalloc;
	    else
	      {
		yymsg = yymsgbuf;
		yymsg_alloc = sizeof yymsgbuf;
	      }
	  }

	if (0 < yysize && yysize <= yymsg_alloc)
	  {
	    (void) yysyntax_error (yymsg, yystate, yychar);
	    yyerror (&yylloc, parse_ctx, yymsg);
	  }
	else
	  {
	    yyerror (&yylloc, parse_ctx, YY_("syntax error"));
	    if (yysize != 0)
	      goto yyexhaustedlab;
	  }
      }
#endif
    }

  yyerror_range[0] = yylloc;

  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
	 error, discard it.  */

      if (yychar <= YYEOF)
	{
	  /* Return failure if at end of input.  */
	  if (yychar == YYEOF)
	    YYABORT;
	}
      else
	{
	  yydestruct ("Error: discarding",
		      yytoken, &yylval, &yylloc, parse_ctx);
	  yychar = YYEMPTY;
	}
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:

  /* Pacify compilers like GCC when the user code never invokes
     YYERROR and the label yyerrorlab therefore never appears in user
     code.  */
  if (/*CONSTCOND*/ 0)
     goto yyerrorlab;

  yyerror_range[0] = yylsp[1-yylen];
  /* Do not reclaim the symbols of the rule which action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;	/* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (yyn != YYPACT_NINF)
	{
	  yyn += YYTERROR;
	  if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
	    {
	      yyn = yytable[yyn];
	      if (0 < yyn)
		break;
	    }
	}

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
	YYABORT;

      yyerror_range[0] = *yylsp;
      yydestruct ("Error: popping",
		  yystos[yystate], yyvsp, yylsp, parse_ctx);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  *++yyvsp = yylval;

  yyerror_range[1] = yylloc;
  /* Using YYLLOC is tempting, but would change the location of
     the lookahead.  YYLOC is available though.  */
  YYLLOC_DEFAULT (yyloc, (yyerror_range - 1), 2);
  *++yylsp = yyloc;

  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;

/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;

#if !defined(yyoverflow) || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (&yylloc, parse_ctx, YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif

yyreturn:
  if (yychar != YYEMPTY)
     yydestruct ("Cleanup: discarding lookahead",
		 yytoken, &yylval, &yylloc, parse_ctx);
  /* Do not reclaim the symbols of the rule which action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
		  yystos[*yyssp], yyvsp, yylsp, parse_ctx);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  /* Make sure YYID is used.  */
  return YYID (yyresult);
}



/* Line 1675 of yacc.c  */
#line 2258 "../../../src/pl/parser/pl_parser_mysql_mode.y"

/**
 * parser function
 * @param [out] pl_yychar, set the pl parser look-ahead token
 */
ParseNode *obpl_mysql_read_sql_construct(ObParseCtx *parse_ctx, const char *prefix, YYLookaheadToken *la_token, int end_token_cnt, ...)
{
  int errcode = OB_PARSER_SUCCESS;
  va_list va;
  bool is_break = false;
  int sql_str_len = -1;
  int parenlevel = 0;
  const char *sql_str = NULL;
  ParseResult parse_result;
  ParseNode *sql_node = NULL;
  if (*(la_token->la_yychar) != -2) { //#define YYEMPTY    (-2)
    parse_ctx->scanner_ctx_.sql_end_loc = la_token->la_yylloc->last_column;
    if (parse_ctx->scanner_ctx_.sql_start_loc < 0) {
      parse_ctx->scanner_ctx_.sql_start_loc = la_token->la_yylloc->first_column;
    }
  }
  while (!is_break) {
    *(la_token->la_yychar) = obpl_mysql_yylex(la_token->la_yylval, la_token->la_yylloc, parse_ctx->scanner_ctx_.yyscan_info_);
    if (parse_ctx->scanner_ctx_.sql_start_loc < 0) {
      //get sql rule start location
      parse_ctx->scanner_ctx_.sql_start_loc = la_token->la_yylloc->first_column;
    }
    va_start(va, end_token_cnt);
    int i = 0;
    for (; !is_break && i < end_token_cnt; ++i) {
      int end_token = va_arg(va, int);
      if (end_token == *(la_token->la_yychar) && parenlevel <= 0) { //正常应该用==0，但是可能外面已经读出了左括号，所以这里用<=
        is_break = true;
        if (END_P == end_token) {
          parse_ctx->scanner_ctx_.sql_end_loc = la_token->la_yylloc->last_column;
        } else {
          parse_ctx->scanner_ctx_.sql_end_loc = la_token->la_yylloc->first_column - 1;
        }
      }
    }
    va_end(va);
    if (*(la_token->la_yychar) == '(' || *(la_token->la_yychar) == '[') {
      ++parenlevel;
    } else if (*(la_token->la_yychar) == ')' || *(la_token->la_yychar) == ']') {
      --parenlevel;
    }
    if (END_P == *(la_token->la_yychar)) {
      is_break = true;
      parse_ctx->scanner_ctx_.sql_end_loc = la_token->la_yylloc->last_column;
    }
    if (!is_break) {
      //get sql rule end location
      parse_ctx->scanner_ctx_.sql_end_loc = la_token->la_yylloc->last_column;
    }
  }
  if (OB_PARSER_SUCCESS == errcode) {
    sql_str_len = parse_ctx->scanner_ctx_.sql_end_loc - parse_ctx->scanner_ctx_.sql_start_loc + 1;
  }
  if (OB_PARSER_SUCCESS == errcode && sql_str_len > 0) {
    sql_str = strndup_with_prefix(prefix, parse_ctx->stmt_str_ + parse_ctx->scanner_ctx_.sql_start_loc,
                                  sql_str_len, parse_ctx->mem_pool_);
    if (NULL == sql_str) {
      YY_FATAL_ERROR("no memory to strdup sql string\n");
    }
    //parse sql_str...
    memset(&parse_result, 0, sizeof(ParseResult));
    parse_result.input_sql_ = sql_str;
    parse_result.input_sql_len_ = sql_str_len + strlen(prefix);
    parse_result.malloc_pool_ = parse_ctx->mem_pool_;
    parse_result.pl_parse_info_.is_pl_parse_ = true;
    parse_result.pl_parse_info_.is_pl_parse_expr_ = false;
    parse_result.is_for_trigger_ = (1 == parse_ctx->is_for_trigger_);
    //将pl_parser的question_mark_size赋值给sql_parser，使得parser sql的question mark能够接着pl_parser的index
    parse_result.question_mark_ctx_ = parse_ctx->question_mark_ctx_;
    parse_result.charset_info_ = parse_ctx->charset_info_;
    parse_result.is_not_utf8_connection_ = parse_ctx->is_not_utf8_connection_;
    parse_result.connection_collation_ = parse_ctx->connection_collation_;
  }
  if (sql_str_len <= 0) {
    //do nothing
  } else if (0 != parse_sql_stmt(&parse_result)) {
    if (parse_result.extra_errno_ != OB_PARSER_SUCCESS) {
      obpl_mysql_parse_fatal_error(parse_result.extra_errno_, YYLEX_PARAM, parse_result.error_msg_);
    } else {
      YYLTYPE sql_yylloc;
      memset(&sql_yylloc, 0, sizeof(YYLTYPE));
      sql_yylloc.first_column = parse_ctx->scanner_ctx_.sql_start_loc;
      sql_yylloc.last_column = parse_ctx->scanner_ctx_.sql_end_loc;
      sql_yylloc.first_line = la_token->la_yylloc->first_line;
      sql_yylloc.last_line = la_token->la_yylloc->last_line;
      obpl_mysql_yyerror(&sql_yylloc, parse_ctx, "Syntax Error\n");
    }
  } else {
    sql_node = parse_result.result_tree_->children_[0];
    //通过sql_parser的question_mark_size来更新pl_parser
    parse_ctx->question_mark_ctx_ = parse_result.question_mark_ctx_;
  }
  return sql_node;
}

void obpl_mysql_yyerror(YYLTYPE *yylloc, ObParseCtx *parse_ctx, char *s, ...)
{
  if (OB_LIKELY(NULL != parse_ctx)) {
    va_list ap;
    va_start(ap, s);
    vsnprintf(parse_ctx->global_errmsg_, MAX_ERROR_MSG, s, ap);
    // vfprintf(stderr, s, ap);
    if (OB_LIKELY(NULL != yylloc)) {
      ObParseErrorInfo *error_info = (ObParseErrorInfo*)parse_malloc(sizeof(ObParseErrorInfo), parse_ctx->mem_pool_);
      if (NULL == error_info) {
        YY_FATAL_ERROR("No memory for malloc parse error info\n");
      } else {
        memset(error_info, 0, sizeof(ObParseErrorInfo));
        error_info->stmt_loc_.first_column_ = yylloc->first_column;
        error_info->stmt_loc_.last_column_ = yylloc->last_column;
        error_info->stmt_loc_.first_line_ = yylloc->first_line;
        error_info->stmt_loc_.last_line_ = yylloc->last_line;
        parse_ctx->cur_error_info_ = error_info;
      }
    }
    va_end(ap);
  }
}
