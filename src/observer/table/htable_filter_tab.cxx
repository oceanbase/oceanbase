
/* A Bison parser, made by GNU Bison 2.4.1.  */

/* Skeleton implementation for Bison's Yacc-like parsers in C

      Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output.  */
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

/* "%code top" blocks.  */

/* Line 171 of yacc.c  */
#line 14 "../../../src/observer/table/htable_filter_tab.yxx"

/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */



/* Line 171 of yacc.c  */
#line 87 "../../../src/observer/table/htable_filter_tab.cxx"
/* Substitute the variable and function names.  */
#define yyparse         ob_hfilter_parse
#define yylex           ob_hfilter_lex
#define yyerror         ob_hfilter_error
#define yylval          ob_hfilter_lval
#define yychar          ob_hfilter_char
#define yydebug         ob_hfilter_debug
#define yynerrs         ob_hfilter_nerrs
#define yylloc          ob_hfilter_lloc

/* Copy the first part of user declarations.  */

/* Line 189 of yacc.c  */
#line 36 "../../../src/observer/table/htable_filter_tab.yxx"

#define USING_LOG_PREFIX SERVER
#include "observer/table/ob_table_filter.h"
#include "observer/table/ob_htable_filter_parser.h"
#include "observer/table/htable_filter_lex.hxx"
#define YYDEBUG 1
#define YYLEX_PARAM (parse_ctx->scanner_)
using namespace oceanbase::table;
using namespace oceanbase::common;


/* Line 189 of yacc.c  */
#line 116 "../../../src/observer/table/htable_filter_tab.cxx"

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
#line 1 "../../../src/observer/table/htable_filter_tab.yxx"

/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */



/* Line 209 of yacc.c  */
#line 156 "../../../src/observer/table/htable_filter_tab.cxx"

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     END = 0,
     RowFilter = 258,
     ValueFilter = 259,
     QualifierFilter = 260,
     SingleColumnValueFilter = 261,
     PageFilter = 262,
     ColumnCountGetFilter = 263,
     ColumnPrefixFilter = 264,
     ColumnPaginationFilter = 265,
     FirstKeyValueMatchingQualifiersFilter = 266,
     CheckAndMutateFilter = 267,
     PrefixFilter = 268,
     FirstKeyOnlyFilter = 269,
     KeyOnlyFilter = 270,
     TimestampsFilter = 271,
     MultiRowRangeFilter = 272,
     InclusiveStopFilter = 273,
     FuzzyRowFilter = 274,
     ColumnValueFilter = 275,
     TableCompareFilter = 276,
     RandomRowFilter = 277,
     DependentColumnFilter = 278,
     SingleColumnValueExcludeFilter = 279,
     ColumnRangeFilter = 280,
     MultipleColumnPrefixFilter = 281,
     FamilyFilter = 282,
     LESS = 283,
     LESS_OR_EQUAL = 284,
     EQUAL = 285,
     NOT_EQUAL = 286,
     GREATER = 287,
     GREATER_OR_EQUAL = 288,
     NO_OP = 289,
     IS = 290,
     IS_NOT = 291,
     BOOL_VALUE = 292,
     STRING_VALUE = 293,
     INT_VALUE = 294,
     OR = 295,
     AND = 296,
     T_OR = 297,
     T_AND = 298,
     WHILE = 299,
     SKIP = 300,
     ERROR = 301
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{

/* Line 214 of yacc.c  */
#line 50 "../../../src/observer/table/htable_filter_tab.yxx"

    int32_t ival;
    int64_t lval;
    oceanbase::table::hfilter::CompareOperator cmp_op;
    oceanbase::table::ObHTableFilterParser::SimpleString sval;
    oceanbase::table::hfilter::Filter *fval;
    oceanbase::table::KeyRange *rval;
    oceanbase::common::ObSEArray<oceanbase::ObString, 8> *sarray;
    oceanbase::common::ObSEArray<int64_t, 8> *array;
    oceanbase::common::ObSEArray<oceanbase::table::KeyRange*, 8> *range_array;
    oceanbase::common::ObSEArray<oceanbase::table::hfilter::ObPair<oceanbase::ObString, oceanbase::ObString>*, 8> *fuzzy_array;



/* Line 214 of yacc.c  */
#line 235 "../../../src/observer/table/htable_filter_tab.cxx"
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
#line 260 "../../../src/observer/table/htable_filter_tab.cxx"

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
#define YYFINAL  60
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   220

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  50
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  14
/* YYNRULES -- Number of rules.  */
#define YYNRULES  62
/* YYNRULES -- Number of states.  */
#define YYNSTATES  217

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   301

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
      47,    48,     2,     2,    49,     2,     2,     2,     2,     2,
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
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint16 yyprhs[] =
{
       0,     0,     3,     6,    10,    14,    18,    22,    25,    28,
      30,    34,    41,    48,    55,    60,    75,    86,   101,   110,
     123,   128,   133,   138,   145,   152,   156,   161,   166,   173,
     184,   189,   194,   199,   204,   211,   216,   221,   234,   241,
     243,   245,   247,   249,   251,   253,   255,   257,   259,   261,
     263,   265,   267,   271,   275,   281,   283,   287,   295,   297,
     301,   303,   307
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int8 yyrhs[] =
{
      51,     0,    -1,    52,     0,    -1,    52,    41,    52,    -1,
      52,    40,    52,    -1,    52,    43,    52,    -1,    52,    42,
      52,    -1,    45,    52,    -1,    44,    52,    -1,    53,    -1,
      47,    52,    48,    -1,     3,    47,    56,    49,    57,    48,
      -1,     5,    47,    56,    49,    57,    48,    -1,     4,    47,
      56,    49,    57,    48,    -1,    13,    47,    38,    48,    -1,
       6,    47,    54,    49,    55,    49,    56,    49,    57,    49,
      37,    49,    37,    48,    -1,     6,    47,    54,    49,    55,
      49,    56,    49,    57,    48,    -1,    24,    47,    54,    49,
      55,    49,    56,    49,    57,    49,    37,    49,    37,    48,
      -1,    23,    47,    54,    49,    55,    49,    37,    48,    -1,
      23,    47,    54,    49,    55,    49,    37,    49,    56,    49,
      57,    48,    -1,     7,    47,    39,    48,    -1,    22,    47,
      39,    48,    -1,     9,    47,    38,    48,    -1,    10,    47,
      39,    49,    39,    48,    -1,    10,    47,    39,    49,    38,
      48,    -1,    14,    47,    48,    -1,    15,    47,    37,    48,
      -1,    19,    47,    59,    48,    -1,    16,    47,    60,    49,
      37,    48,    -1,    20,    47,    54,    49,    55,    49,    56,
      49,    57,    48,    -1,    17,    47,    62,    48,    -1,    18,
      47,    38,    48,    -1,    25,    47,    62,    48,    -1,    26,
      47,    63,    48,    -1,    27,    47,    56,    49,    57,    48,
      -1,     8,    47,    39,    48,    -1,    11,    47,    58,    48,
      -1,    12,    47,    56,    49,    57,    49,    54,    49,    55,
      49,    37,    48,    -1,    21,    47,    56,    49,    57,    48,
      -1,    38,    -1,    38,    -1,    28,    -1,    29,    -1,    30,
      -1,    31,    -1,    32,    -1,    33,    -1,    34,    -1,    35,
      -1,    36,    -1,    38,    -1,    38,    -1,    58,    49,    38,
      -1,    38,    49,    38,    -1,    59,    49,    38,    49,    38,
      -1,    39,    -1,    60,    49,    39,    -1,    38,    49,    37,
      49,    38,    49,    37,    -1,    61,    -1,    62,    49,    61,
      -1,    38,    -1,    63,    49,    38,    -1,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,    91,    91,    98,   119,   140,   161,   182,   197,   212,
     219,   221,   243,   265,   287,   309,   337,   365,   393,   411,
     434,   449,   465,   480,   498,   516,   531,   546,   565,   580,
     603,   618,   648,   663,   678,   700,   715,   730,   754,   776,
     779,   782,   783,   784,   785,   786,   787,   788,   789,   790,
     793,   797,   813,   827,   845,   864,   880,   894,   910,   926,
     940,   967,   991
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "$undefined", "RowFilter", "ValueFilter",
  "QualifierFilter", "SingleColumnValueFilter", "PageFilter",
  "ColumnCountGetFilter", "ColumnPrefixFilter", "ColumnPaginationFilter",
  "FirstKeyValueMatchingQualifiersFilter", "CheckAndMutateFilter",
  "PrefixFilter", "FirstKeyOnlyFilter", "KeyOnlyFilter",
  "TimestampsFilter", "MultiRowRangeFilter", "InclusiveStopFilter",
  "FuzzyRowFilter", "ColumnValueFilter", "TableCompareFilter",
  "RandomRowFilter", "DependentColumnFilter",
  "SingleColumnValueExcludeFilter", "ColumnRangeFilter",
  "MultipleColumnPrefixFilter", "FamilyFilter", "LESS", "LESS_OR_EQUAL",
  "EQUAL", "NOT_EQUAL", "GREATER", "GREATER_OR_EQUAL", "NO_OP", "IS",
  "IS_NOT", "BOOL_VALUE", "STRING_VALUE", "INT_VALUE", "OR", "AND", "T_OR",
  "T_AND", "WHILE", "SKIP", "ERROR", "'('", "')'", "','", "$accept",
  "result_filter", "filter", "simple_filter", "family", "qualifier",
  "compare_op", "comparator", "param_list", "fuzzy_list", "timestamp_list",
  "range", "range_list", "prefix_list", 0
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
     295,   296,   297,   298,   299,   300,   301,    40,    41,    44
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint8 yyr1[] =
{
       0,    50,    51,    52,    52,    52,    52,    52,    52,    52,
      53,    53,    53,    53,    53,    53,    53,    53,    53,    53,
      53,    53,    53,    53,    53,    53,    53,    53,    53,    53,
      53,    53,    53,    53,    53,    53,    53,    53,    53,    54,
      55,    56,    56,    56,    56,    56,    56,    56,    56,    56,
      57,    58,    58,    59,    59,    60,    60,    61,    62,    62,
      63,    63,    63
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     2,     3,     3,     3,     3,     2,     2,     1,
       3,     6,     6,     6,     4,    14,    10,    14,     8,    12,
       4,     4,     4,     6,     6,     3,     4,     4,     6,    10,
       4,     4,     4,     4,     6,     4,     4,    12,     6,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     3,     3,     5,     1,     3,     7,     1,     3,
       1,     3,     0
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     9,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    62,     0,     8,     7,     0,
       1,     2,     0,     0,     0,     0,    41,    42,    43,    44,
      45,    46,    47,    48,    49,     0,     0,     0,    39,     0,
       0,     0,     0,     0,    51,     0,     0,     0,    25,     0,
      55,     0,     0,    58,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    60,     0,     0,    10,     4,     3,
       6,     5,     0,     0,     0,     0,    20,    35,    22,     0,
      36,     0,     0,    14,    26,     0,     0,    30,     0,    31,
       0,    27,     0,     0,     0,    21,     0,     0,    32,    33,
       0,     0,    50,     0,     0,     0,    40,     0,     0,     0,
      52,     0,     0,    56,     0,    59,    53,     0,     0,     0,
       0,     0,    61,     0,    11,    13,    12,     0,    24,    23,
       0,    28,     0,     0,     0,    38,     0,     0,    34,     0,
       0,     0,    54,     0,     0,     0,     0,     0,     0,     0,
      18,     0,     0,     0,     0,    57,     0,     0,     0,    16,
       0,     0,    29,     0,     0,     0,     0,     0,     0,     0,
      37,    19,     0,     0,     0,    15,    17
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    29,    30,    31,    79,   147,    75,   143,    85,    97,
      91,    93,    94,   105
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -127
static const yytype_int16 yypact[] =
{
      30,   -38,   -21,   -17,   -15,    26,    31,    45,    59,    60,
      61,    63,    64,    65,    66,    67,    68,    69,    70,    71,
      72,    73,    74,    75,    77,    78,    30,    30,    30,   109,
      22,  -127,    55,    55,    55,    88,    89,    90,    92,    93,
      95,    55,    97,    79,    33,    98,   100,   101,   102,    88,
      55,   103,    88,    88,   100,   105,    55,  -127,  -127,   -27,
    -127,  -127,    30,    30,    30,    30,  -127,  -127,  -127,  -127,
    -127,  -127,  -127,  -127,  -127,    82,    87,    96,  -127,    99,
     104,   106,   107,   108,  -127,   -20,   110,   112,  -127,   113,
    -127,   114,   115,  -127,    18,   117,   118,    20,   119,   120,
     122,   123,   124,    23,  -127,    50,   125,  -127,   -23,    38,
     -23,    38,   111,   111,   111,   128,  -127,  -127,  -127,    62,
    -127,   133,   111,  -127,  -127,   -12,   116,  -127,   100,  -127,
     137,  -127,   138,   128,   111,  -127,   128,   128,  -127,  -127,
     139,   111,  -127,   130,   131,   132,  -127,   134,   136,   140,
    -127,   141,   143,  -127,   144,  -127,  -127,   145,   146,   148,
     149,   150,  -127,   152,  -127,  -127,  -127,    55,  -127,  -127,
      88,  -127,   147,   151,    55,  -127,   155,    55,  -127,   153,
     154,   156,  -127,   157,    54,   158,   111,   128,   160,   111,
    -127,    55,   111,    56,   159,  -127,   161,   162,   163,  -127,
     164,   167,  -127,   111,   173,   165,   168,   169,   166,   176,
    -127,  -127,   181,   171,   172,  -127,  -127
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
    -127,  -127,    32,  -127,   -47,  -126,   -33,  -110,  -127,  -127,
    -127,    19,   127,  -127
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -1
static const yytype_uint8 yytable[] =
{
      76,    77,    98,   144,   145,   101,   102,   158,    86,    32,
     160,   161,   151,    62,    63,    64,    65,    99,    63,    64,
      65,   107,    61,   106,   159,   152,    33,   153,   120,   121,
      34,   163,    35,     1,     2,     3,     4,     5,     6,     7,
       8,     9,    10,    11,    12,    13,    14,    15,    16,    17,
      18,    19,    20,    21,    22,    23,    24,    25,    57,    58,
      59,   194,    62,    63,    64,    65,   127,   128,   131,   132,
      89,   138,   128,    36,    26,    27,   193,    28,    37,   196,
      64,    65,   198,    66,    67,    68,    69,    70,    71,    72,
      73,    74,    38,   207,   108,   109,   110,   111,   139,   140,
     148,   149,   190,   191,   199,   200,    39,    40,    41,    60,
      42,    43,    44,    45,    46,    47,    48,    49,    50,    51,
      52,    53,    54,   180,    55,    56,    78,    88,    80,    81,
      82,   112,    83,    84,   179,    87,   113,    90,    92,    95,
      96,   183,   100,   104,   185,   114,     0,   155,   115,   142,
       0,     0,   116,   154,   117,   118,     0,   119,   197,   122,
     123,   124,     0,   125,   126,   129,   146,   130,   133,   134,
     135,   150,   136,   137,   141,   156,   157,   162,   164,   165,
     166,   103,     0,   167,   168,   181,     0,     0,   169,   182,
     170,   171,   184,   172,   173,   174,   175,   195,   176,   177,
     178,   205,   186,   187,   206,   188,   189,   192,   201,   202,
     208,   203,   204,   213,   209,   212,   210,   211,   214,   215,
     216
};

static const yytype_int16 yycheck[] =
{
      33,    34,    49,   113,   114,    52,    53,   133,    41,    47,
     136,   137,   122,    40,    41,    42,    43,    50,    41,    42,
      43,    48,     0,    56,   134,    37,    47,    39,    48,    49,
      47,   141,    47,     3,     4,     5,     6,     7,     8,     9,
      10,    11,    12,    13,    14,    15,    16,    17,    18,    19,
      20,    21,    22,    23,    24,    25,    26,    27,    26,    27,
      28,   187,    40,    41,    42,    43,    48,    49,    48,    49,
      37,    48,    49,    47,    44,    45,   186,    47,    47,   189,
      42,    43,   192,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    47,   203,    62,    63,    64,    65,    48,    49,
      38,    39,    48,    49,    48,    49,    47,    47,    47,     0,
      47,    47,    47,    47,    47,    47,    47,    47,    47,    47,
      47,    47,    47,   170,    47,    47,    38,    48,    39,    39,
      38,    49,    39,    38,   167,    38,    49,    39,    38,    38,
      38,   174,    39,    38,   177,    49,    -1,   128,    49,    38,
      -1,    -1,    48,    37,    48,    48,    -1,    49,   191,    49,
      48,    48,    -1,    49,    49,    48,    38,    49,    49,    49,
      48,    38,    49,    49,    49,    38,    38,    38,    48,    48,
      48,    54,    -1,    49,    48,    38,    -1,    -1,    48,    38,
      49,    48,    37,    49,    49,    49,    48,    37,    49,    49,
      48,    37,    49,    49,    37,    49,    49,    49,    49,    48,
      37,    49,    49,    37,    49,    49,    48,    48,    37,    48,
      48
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,     3,     4,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,    16,    17,    18,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    44,    45,    47,    51,
      52,    53,    47,    47,    47,    47,    47,    47,    47,    47,
      47,    47,    47,    47,    47,    47,    47,    47,    47,    47,
      47,    47,    47,    47,    47,    47,    47,    52,    52,    52,
       0,     0,    40,    41,    42,    43,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    56,    56,    56,    38,    54,
      39,    39,    38,    39,    38,    58,    56,    38,    48,    37,
      39,    60,    38,    61,    62,    38,    38,    59,    54,    56,
      39,    54,    54,    62,    38,    63,    56,    48,    52,    52,
      52,    52,    49,    49,    49,    49,    48,    48,    48,    49,
      48,    49,    49,    48,    48,    49,    49,    48,    49,    48,
      49,    48,    49,    49,    49,    48,    49,    49,    48,    48,
      49,    49,    38,    57,    57,    57,    38,    55,    38,    39,
      38,    57,    37,    39,    37,    61,    38,    38,    55,    57,
      55,    55,    38,    57,    48,    48,    48,    49,    48,    48,
      49,    48,    49,    49,    49,    48,    49,    49,    48,    56,
      54,    38,    38,    56,    37,    56,    49,    49,    49,    49,
      48,    49,    49,    57,    55,    37,    57,    56,    57,    48,
      49,    49,    48,    49,    49,    37,    37,    57,    37,    49,
      48,    48,    49,    37,    37,    48,    48
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
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, oceanbase::table::ObHTableFilterParser *parse_ctx)
#else
static void
yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, parse_ctx)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    oceanbase::table::ObHTableFilterParser *parse_ctx;
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
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, oceanbase::table::ObHTableFilterParser *parse_ctx)
#else
static void
yy_symbol_print (yyoutput, yytype, yyvaluep, yylocationp, parse_ctx)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    oceanbase::table::ObHTableFilterParser *parse_ctx;
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
yy_reduce_print (YYSTYPE *yyvsp, YYLTYPE *yylsp, int yyrule, oceanbase::table::ObHTableFilterParser *parse_ctx)
#else
static void
yy_reduce_print (yyvsp, yylsp, yyrule, parse_ctx)
    YYSTYPE *yyvsp;
    YYLTYPE *yylsp;
    int yyrule;
    oceanbase::table::ObHTableFilterParser *parse_ctx;
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
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep, YYLTYPE *yylocationp, oceanbase::table::ObHTableFilterParser *parse_ctx)
#else
static void
yydestruct (yymsg, yytype, yyvaluep, yylocationp, parse_ctx)
    const char *yymsg;
    int yytype;
    YYSTYPE *yyvaluep;
    YYLTYPE *yylocationp;
    oceanbase::table::ObHTableFilterParser *parse_ctx;
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
int yyparse (oceanbase::table::ObHTableFilterParser *parse_ctx);
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
yyparse (oceanbase::table::ObHTableFilterParser *parse_ctx)
#else
int
yyparse (parse_ctx)
    oceanbase::table::ObHTableFilterParser *parse_ctx;
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
#line 91 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                   parse_ctx->set_result_filter((yyvsp[(1) - (2)].fval));
                   YYACCEPT;
                ;}
    break;

  case 3:

/* Line 1455 of yacc.c  */
#line 99 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    hfilter::FilterListBase *filter_list = nullptr;
                    (yyval.fval) = filter_list = OB_NEWx(hfilter::FilterListAND, parse_ctx->allocator(),
                                               hfilter::FilterListBase::Operator::MUST_PASS_ALL);
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    } else if (OB_FAIL(filter_list->add_filter((yyvsp[(1) - (3)].fval)))) {
                        LOG_WARN("failed to add filter to list", K(ret));
                    } else if (OB_FAIL(filter_list->add_filter((yyvsp[(3) - (3)].fval)))) {
                        LOG_WARN("failed to add filter to list", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse FilterList with AND");
                        YYABORT;
                    }
                ;}
    break;

  case 4:

/* Line 1455 of yacc.c  */
#line 120 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    hfilter::FilterListBase *filter_list = nullptr;
                    (yyval.fval) = filter_list = OB_NEWx(hfilter::FilterListOR, parse_ctx->allocator(),
                                               hfilter::FilterListBase::Operator::MUST_PASS_ONE);
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    } else if (OB_FAIL(filter_list->add_filter((yyvsp[(1) - (3)].fval)))) {
                        LOG_WARN("failed to add filter to list", K(ret));
                    } else if (OB_FAIL(filter_list->add_filter((yyvsp[(3) - (3)].fval)))) {
                        LOG_WARN("failed to add filter to list", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse FilterList with AND");
                        YYABORT;
                    }
                ;}
    break;

  case 5:

/* Line 1455 of yacc.c  */
#line 141 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    hfilter::FilterListBase *filter_list = nullptr;
                    (yyval.fval) = filter_list = OB_NEWx(ObTableFilterListAnd, parse_ctx->allocator(),
                                               hfilter::FilterListBase::Operator::MUST_PASS_ALL);
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    } else if (OB_FAIL(filter_list->add_filter((yyvsp[(1) - (3)].fval)))) {
                        LOG_WARN("failed to add filter to list", K(ret));
                    } else if (OB_FAIL(filter_list->add_filter((yyvsp[(3) - (3)].fval)))) {
                        LOG_WARN("failed to add filter to list", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse FilterList with AND");
                        YYABORT;
                    }
                ;}
    break;

  case 6:

/* Line 1455 of yacc.c  */
#line 162 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    hfilter::FilterListBase *filter_list = nullptr;
                    (yyval.fval) = filter_list = OB_NEWx(ObTableFilterListOr, parse_ctx->allocator(),
                                               hfilter::FilterListBase::Operator::MUST_PASS_ONE);
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    } else if (OB_FAIL(filter_list->add_filter((yyvsp[(1) - (3)].fval)))) {
                        LOG_WARN("failed to add filter to list", K(ret));
                    } else if (OB_FAIL(filter_list->add_filter((yyvsp[(3) - (3)].fval)))) {
                        LOG_WARN("failed to add filter to list", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse FilterList with AND");
                        YYABORT;
                    }
                ;}
    break;

  case 7:

/* Line 1455 of yacc.c  */
#line 183 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.fval) = OB_NEWx(hfilter::SkipFilter, parse_ctx->allocator(), (yyvsp[(2) - (2)].fval));
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse SkipFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 8:

/* Line 1455 of yacc.c  */
#line 198 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.fval) = OB_NEWx(hfilter::WhileMatchFilter, parse_ctx->allocator(), (yyvsp[(2) - (2)].fval));
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse WhileFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 9:

/* Line 1455 of yacc.c  */
#line 213 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    (yyval.fval) = (yyvsp[(1) - (1)].fval);
                ;}
    break;

  case 10:

/* Line 1455 of yacc.c  */
#line 220 "../../../src/observer/table/htable_filter_tab.yxx"
    { (yyval.fval) = (yyvsp[(2) - (3)].fval); ;}
    break;

  case 11:

/* Line 1455 of yacc.c  */
#line 222 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    hfilter::Comparable *comparable = nullptr;
                    if (OB_FAIL(parse_ctx->create_comparator((yyvsp[(5) - (6)].sval), comparable))) {
                        LOG_WARN("failed to create comparator", K(ret));
                    } else {
                        (yyval.fval) = OB_NEWx(hfilter::RowFilter, parse_ctx->allocator(), (yyvsp[(3) - (6)].cmp_op), comparable);
                        if (nullptr == (yyval.fval)) {
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            LOG_WARN("no memory", K(ret));
                        } else if (OB_FAIL(static_cast<hfilter::RowFilter*>((yyval.fval))->check_arguments())) {
                            LOG_WARN("failed to check arguments", K(ret));
                        } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                            LOG_WARN("failed to store filter", K(ret));
                        }
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse RowFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 12:

/* Line 1455 of yacc.c  */
#line 244 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    hfilter::Comparable *comparable = nullptr;
                    if (OB_FAIL(parse_ctx->create_comparator((yyvsp[(5) - (6)].sval), comparable))) {
                        LOG_WARN("failed to create comparator", K(ret));
                    } else {
                        (yyval.fval) = OB_NEWx(hfilter::QualifierFilter, parse_ctx->allocator(), (yyvsp[(3) - (6)].cmp_op), comparable);
                        if (nullptr == (yyval.fval)) {
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            LOG_WARN("no memory", K(ret));
                        } else if (OB_FAIL(static_cast<hfilter::QualifierFilter*>((yyval.fval))->check_arguments())) {
                            LOG_WARN("failed to check arguments", K(ret));
                        } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                            LOG_WARN("failed to store filter", K(ret));
                        }
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse QualifierFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 13:

/* Line 1455 of yacc.c  */
#line 266 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    hfilter::Comparable *comparable = nullptr;
                    if (OB_FAIL(parse_ctx->create_comparator((yyvsp[(5) - (6)].sval), comparable))) {
                        LOG_WARN("failed to create comparator", K(ret));
                    } else {
                        (yyval.fval) = OB_NEWx(hfilter::ValueFilter, parse_ctx->allocator(), (yyvsp[(3) - (6)].cmp_op), comparable);
                        if (nullptr == (yyval.fval)) {
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            LOG_WARN("no memory", K(ret));
                        } else if (OB_FAIL(static_cast<hfilter::ValueFilter*>((yyval.fval))->check_arguments())) {
                            LOG_WARN("failed to check arguments", K(ret));
                        } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                            LOG_WARN("failed to store filter", K(ret));
                        }
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse ValueFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 14:

/* Line 1455 of yacc.c  */
#line 288 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    hfilter::Comparable *comparable = nullptr;
                    if (OB_FAIL(parse_ctx->create_prefix_comparator((yyvsp[(3) - (4)].sval), comparable))) {
                        LOG_WARN("failed to create comparator", K(ret));
                    } else {
                        (yyval.fval) = OB_NEWx(hfilter::RowFilter, parse_ctx->allocator(), hfilter::CompareOperator::EQUAL, comparable);
                        if (nullptr == (yyval.fval)) {
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            LOG_WARN("no memory", K(ret));
                        } else if (OB_FAIL(static_cast<hfilter::RowFilter*>((yyval.fval))->check_arguments())) {
                            LOG_WARN("failed to check arguments", K(ret));
                        } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                            LOG_WARN("failed to store filter", K(ret));
                        }
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse PrefixFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 15:

/* Line 1455 of yacc.c  */
#line 310 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    hfilter::Comparable *comparable = nullptr;
                    if (OB_FAIL(parse_ctx->create_comparator((yyvsp[(9) - (14)].sval), comparable))) {
                        LOG_WARN("failed to create comparator", K(ret));
                    } else {
                        ObString family((yyvsp[(3) - (14)].sval).len_, (yyvsp[(3) - (14)].sval).str_);
                        ObString qualifier((yyvsp[(5) - (14)].sval).len_, (yyvsp[(5) - (14)].sval).str_);
                        bool filter_if_missing = ((yyvsp[(11) - (14)].ival) == 1);
                        bool latest_version_only = ((yyvsp[(13) - (14)].ival) == 1);
                        hfilter::SingleColumnValueFilter *filter = NULL;
                        (yyval.fval) = filter = OB_NEWx(hfilter::SingleColumnValueFilter, parse_ctx->allocator(), family, qualifier, (yyvsp[(7) - (14)].cmp_op), comparable);
                        if (nullptr == (yyval.fval)) {
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            LOG_WARN("no memory", K(ret));
                        } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                            LOG_WARN("failed to store filter", K(ret));
                        } else {
                            filter->set_filter_if_missing(filter_if_missing);
                            filter->set_latest_version_only(latest_version_only);
                        }
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse SingleColumnValueFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 16:

/* Line 1455 of yacc.c  */
#line 338 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    hfilter::Comparable *comparable = nullptr;
                    if (OB_FAIL(parse_ctx->create_comparator((yyvsp[(9) - (10)].sval), comparable))) {
                        LOG_WARN("failed to create comparator", K(ret));
                    } else {
                        ObString family((yyvsp[(3) - (10)].sval).len_, (yyvsp[(3) - (10)].sval).str_);
                        ObString qualifier((yyvsp[(5) - (10)].sval).len_, (yyvsp[(5) - (10)].sval).str_);
                        const bool filter_if_missing = false;
                        const bool latest_version_only = true;
                        hfilter::SingleColumnValueFilter *filter = NULL;
                        (yyval.fval) = filter = OB_NEWx(hfilter::SingleColumnValueFilter, parse_ctx->allocator(), family, qualifier, (yyvsp[(7) - (10)].cmp_op), comparable);
                        if (nullptr == (yyval.fval)) {
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            LOG_WARN("no memory", K(ret));
                        } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                            LOG_WARN("failed to store filter", K(ret));
                        } else {
                            filter->set_filter_if_missing(filter_if_missing);
                            filter->set_latest_version_only(latest_version_only);
                        }
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse SingleColumnValueFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 17:

/* Line 1455 of yacc.c  */
#line 366 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    hfilter::Comparable *comparable = nullptr;
                    if (OB_FAIL(parse_ctx->create_comparator((yyvsp[(9) - (14)].sval), comparable))) {
                        LOG_WARN("failed to create comparator", K(ret));
                    } else {
                        ObString family((yyvsp[(3) - (14)].sval).len_, (yyvsp[(3) - (14)].sval).str_);
                        ObString qualifier((yyvsp[(5) - (14)].sval).len_, (yyvsp[(5) - (14)].sval).str_);
                        bool filter_if_missing = ((yyvsp[(11) - (14)].ival) == 1);
                        bool latest_version_only = ((yyvsp[(13) - (14)].ival) == 1);
                        hfilter::SingleColumnValueExcludeFilter *filter = NULL;
                        (yyval.fval) = filter = OB_NEWx(hfilter::SingleColumnValueExcludeFilter, parse_ctx->allocator(), family, qualifier, (yyvsp[(7) - (14)].cmp_op), comparable);
                        if (nullptr == (yyval.fval)) {
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            LOG_WARN("no memory", K(ret));
                        } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                            LOG_WARN("failed to store filter", K(ret));
                        } else {
                            filter->set_filter_if_missing(filter_if_missing);
                            filter->set_latest_version_only(latest_version_only);
                        }
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse SingleColumnValueExcludeFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 18:

/* Line 1455 of yacc.c  */
#line 394 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    ObString family((yyvsp[(3) - (8)].sval).len_, (yyvsp[(3) - (8)].sval).str_);
                    ObString qualifier((yyvsp[(5) - (8)].sval).len_, (yyvsp[(5) - (8)].sval).str_);
                    bool drop_dependent_column = ((yyvsp[(7) - (8)].ival) == 1);
                    (yyval.fval) = OB_NEWx(hfilter::DependentColumnFilter, parse_ctx->allocator(), family, qualifier, drop_dependent_column);
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse DependentColumnFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 19:

/* Line 1455 of yacc.c  */
#line 412 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    hfilter::Comparable *comparable = nullptr;
                    if (OB_FAIL(parse_ctx->create_comparator((yyvsp[(11) - (12)].sval), comparable))) {
                        LOG_WARN("failed to create comparator", K(ret));
                    } else {
                        ObString family((yyvsp[(3) - (12)].sval).len_, (yyvsp[(3) - (12)].sval).str_);
                        ObString qualifier((yyvsp[(5) - (12)].sval).len_, (yyvsp[(5) - (12)].sval).str_);
                        bool drop_dependent_column = ((yyvsp[(7) - (12)].ival) == 1);
                        (yyval.fval) = OB_NEWx(hfilter::DependentColumnFilter, parse_ctx->allocator(), family, qualifier, drop_dependent_column, (yyvsp[(9) - (12)].cmp_op), comparable);
                        if (nullptr == (yyval.fval)) {
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            LOG_WARN("no memory", K(ret));
                        } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                            LOG_WARN("failed to store filter", K(ret));
                        }
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse DependentColumnFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 20:

/* Line 1455 of yacc.c  */
#line 435 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.fval) = OB_NEWx(hfilter::PageFilter, parse_ctx->allocator(), (yyvsp[(3) - (4)].lval));
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse PageFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 21:

/* Line 1455 of yacc.c  */
#line 450 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    float_t chance = *(reinterpret_cast<float_t*>(&((yyvsp[(3) - (4)].lval))));
                    (yyval.fval) = OB_NEWx(hfilter::RandomRowFilter, parse_ctx->allocator(), chance);
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse RandomRowFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 22:

/* Line 1455 of yacc.c  */
#line 466 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.fval) = OB_NEWx(hfilter::ColumnPrefixFilter, parse_ctx->allocator(), parse_ctx->create_ob_string((yyvsp[(3) - (4)].sval)));
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse ColumnPrefixFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 23:

/* Line 1455 of yacc.c  */
#line 481 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.fval) = OB_NEWx(hfilter::ColumnPaginationFilter, parse_ctx->allocator(), (yyvsp[(3) - (6)].lval), (yyvsp[(5) - (6)].lval));
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    } else if ((yyvsp[(3) - (6)].lval) < 0 || (yyvsp[(5) - (6)].lval) < 0) {
                        ret = OB_INVALID_ARGUMENT;
                        LOG_WARN("arguments must be positive", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse ColumnPaginationFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 24:

/* Line 1455 of yacc.c  */
#line 499 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.fval) = OB_NEWx(hfilter::ColumnPaginationFilter, parse_ctx->allocator(), (yyvsp[(3) - (6)].lval), parse_ctx->create_ob_string((yyvsp[(5) - (6)].sval)));
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    } else if ((yyvsp[(3) - (6)].lval) < 0) {
                        ret = OB_INVALID_ARGUMENT;
                        LOG_WARN("arguments must be positive", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse ColumnPaginationFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 25:

/* Line 1455 of yacc.c  */
#line 517 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.fval) = OB_NEWx(hfilter::FirstKeyOnlyFilter, parse_ctx->allocator());
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse FirstKeyOnlyFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 26:

/* Line 1455 of yacc.c  */
#line 532 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.fval) = OB_NEWx(hfilter::KeyOnlyFilter, parse_ctx->allocator(), (yyvsp[(3) - (4)].ival));
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse KeyOnlyFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 27:

/* Line 1455 of yacc.c  */
#line 547 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    if (nullptr != parse_ctx->allocator() && nullptr != (yyvsp[(3) - (4)].fuzzy_array)) {
                        (yyval.fval) = OB_NEWx(hfilter::FuzzyRowFilter, parse_ctx->allocator(), (*parse_ctx->allocator()), *(yyvsp[(3) - (4)].fuzzy_array));
                    } else {
                        (yyval.fval) = nullptr;
                    }
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse FuzzyRowFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 28:

/* Line 1455 of yacc.c  */
#line 566 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.fval) = OB_NEWx(hfilter::TimestampsFilter, parse_ctx->allocator(), (yyvsp[(3) - (6)].array), (yyvsp[(5) - (6)].ival));
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse TimestampsFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 29:

/* Line 1455 of yacc.c  */
#line 581 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    hfilter::Comparable *comparable = nullptr;
                    if (OB_FAIL(parse_ctx->create_comparator((yyvsp[(9) - (10)].sval), comparable))) {
                        LOG_WARN("failed to create comparator", K(ret));
                    } else {
                        ObString family((yyvsp[(3) - (10)].sval).len_, (yyvsp[(3) - (10)].sval).str_);
                        ObString qualifier((yyvsp[(5) - (10)].sval).len_, (yyvsp[(5) - (10)].sval).str_);
                        hfilter::ColumnValueFilter *filter = NULL;
                        (yyval.fval) = filter = OB_NEWx(hfilter::ColumnValueFilter, parse_ctx->allocator(), family, qualifier, (yyvsp[(7) - (10)].cmp_op), comparable);
                        if (nullptr == (yyval.fval)) {
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            LOG_WARN("no memory", K(ret));
                        } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                            LOG_WARN("failed to store filter", K(ret));
                        }
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse ColumnValueFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 30:

/* Line 1455 of yacc.c  */
#line 604 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.fval) = OB_NEWx(hfilter::MultiRowRangeFilter, parse_ctx->allocator(), (yyvsp[(3) - (4)].range_array));
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse MultiRowRangeFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 31:

/* Line 1455 of yacc.c  */
#line 619 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    typedef ObSEArray<KeyRange*, 8> RangeArray;
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    RangeArray* arr = OB_NEWx(RangeArray, parse_ctx->allocator());
                    KeyRange* range = OB_NEWx(KeyRange, parse_ctx->allocator());
                    if (nullptr == arr || nullptr == range) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else {
                        range->set_max(parse_ctx->create_ob_string((yyvsp[(3) - (4)].sval)));
                        range->set_max_inclusive(true);
                        if (OB_FAIL(arr->push_back(range))) {
                            LOG_WARN("failed to push back", K(ret));
                        } else {
                            (yyval.fval) = OB_NEWx(hfilter::InclusiveStopFilter, parse_ctx->allocator(), arr, parse_ctx->create_ob_string((yyvsp[(3) - (4)].sval)));
                        }
                        if (nullptr == (yyval.fval)) {
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            LOG_WARN("no memory", K(ret));
                        } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                            LOG_WARN("failed to store filter", K(ret));
                        }
                    }

                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse InclusiveStopFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 32:

/* Line 1455 of yacc.c  */
#line 649 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.fval) = OB_NEWx(hfilter::ColumnRangeFilter, parse_ctx->allocator(), (yyvsp[(3) - (4)].range_array));
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse ColumnRangeFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 33:

/* Line 1455 of yacc.c  */
#line 664 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.fval) = OB_NEWx(hfilter::MultipleColumnPrefixFilter, parse_ctx->allocator(), (yyvsp[(3) - (4)].range_array));
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse MultipleColumnPrefixFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 34:

/* Line 1455 of yacc.c  */
#line 679 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    hfilter::Comparable *comparable = nullptr;
                    if (OB_FAIL(parse_ctx->create_comparator((yyvsp[(5) - (6)].sval), comparable))) {
                        LOG_WARN("failed to create comparator", K(ret));
                    } else {
                        (yyval.fval) = OB_NEWx(hfilter::FamilyFilter, parse_ctx->allocator(), (yyvsp[(3) - (6)].cmp_op), comparable);
                        if (nullptr == (yyval.fval)) {
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            LOG_WARN("no memory", K(ret));
                        } else if (OB_FAIL(static_cast<hfilter::FamilyFilter*>((yyval.fval))->check_arguments())) {
                            LOG_WARN("failed to check arguments", K(ret));
                        } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                            LOG_WARN("failed to store filter", K(ret));
                        }
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse FamilyFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 35:

/* Line 1455 of yacc.c  */
#line 701 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.fval) = OB_NEWx(hfilter::ColumnCountGetFilter, parse_ctx->allocator(), (yyvsp[(3) - (4)].lval));
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse ColumnCountGetFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 36:

/* Line 1455 of yacc.c  */
#line 716 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.fval) = OB_NEWx(hfilter::FirstKeyValueMatchingQualifiersFilter, parse_ctx->allocator(), (yyvsp[(3) - (4)].sarray));
                    if (nullptr == (yyval.fval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                        LOG_WARN("failed to store filter", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse FirstKeyValueMatchingQualifiersFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 37:

/* Line 1455 of yacc.c  */
#line 731 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    hfilter::Comparable *comparable = nullptr;
                    if (OB_FAIL(parse_ctx->create_comparator((yyvsp[(5) - (12)].sval), comparable))) {
                        LOG_WARN("failed to create comparator", K(ret));
                    } else {
                        const ObString family((yyvsp[(7) - (12)].sval).len_, (yyvsp[(7) - (12)].sval).str_);
                        const ObString qualifier((yyvsp[(9) - (12)].sval).len_, (yyvsp[(9) - (12)].sval).str_);
                        const bool value_is_null = ((yyvsp[(11) - (12)].ival) == 1);
                        hfilter::CheckAndMutateFilter *filter = NULL;
                        (yyval.fval) = filter = OB_NEWx(hfilter::CheckAndMutateFilter, parse_ctx->allocator(), family, qualifier, (yyvsp[(3) - (12)].cmp_op), comparable, value_is_null);
                        if (nullptr == (yyval.fval)) {
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            LOG_WARN("no memory", K(ret));
                        } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                            LOG_WARN("failed to store filter", K(ret));
                        }
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse CheckAndMutateFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 38:

/* Line 1455 of yacc.c  */
#line 755 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    hfilter::Comparable *comparable = nullptr;
                    if (OB_FAIL(parse_ctx->create_comparator((yyvsp[(5) - (6)].sval), comparable))) {
                        LOG_WARN("failed to create comparator", K(ret));
                    } else {
                        (yyval.fval) = OB_NEWx(ObTableCompareFilter, parse_ctx->allocator(), (yyvsp[(3) - (6)].cmp_op), comparable);
                        if (nullptr == (yyval.fval)) {
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            LOG_WARN("no memory", K(ret));
                        } else if (OB_FAIL(parse_ctx->store_filter((yyval.fval)))) {
                            LOG_WARN("failed to store filter", K(ret));
                        }
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse TableCompareFilter");
                        YYABORT;
                    }
                ;}
    break;

  case 39:

/* Line 1455 of yacc.c  */
#line 776 "../../../src/observer/table/htable_filter_tab.yxx"
    { (yyval.sval) = (yyvsp[(1) - (1)].sval); ;}
    break;

  case 40:

/* Line 1455 of yacc.c  */
#line 779 "../../../src/observer/table/htable_filter_tab.yxx"
    { (yyval.sval) = (yyvsp[(1) - (1)].sval); ;}
    break;

  case 41:

/* Line 1455 of yacc.c  */
#line 782 "../../../src/observer/table/htable_filter_tab.yxx"
    { (yyval.cmp_op) = hfilter::CompareOperator::LESS; ;}
    break;

  case 42:

/* Line 1455 of yacc.c  */
#line 783 "../../../src/observer/table/htable_filter_tab.yxx"
    { (yyval.cmp_op) = hfilter::CompareOperator::LESS_OR_EQUAL; ;}
    break;

  case 43:

/* Line 1455 of yacc.c  */
#line 784 "../../../src/observer/table/htable_filter_tab.yxx"
    { (yyval.cmp_op) = hfilter::CompareOperator::EQUAL; ;}
    break;

  case 44:

/* Line 1455 of yacc.c  */
#line 785 "../../../src/observer/table/htable_filter_tab.yxx"
    { (yyval.cmp_op) = hfilter::CompareOperator::NOT_EQUAL; ;}
    break;

  case 45:

/* Line 1455 of yacc.c  */
#line 786 "../../../src/observer/table/htable_filter_tab.yxx"
    { (yyval.cmp_op) = hfilter::CompareOperator::GREATER; ;}
    break;

  case 46:

/* Line 1455 of yacc.c  */
#line 787 "../../../src/observer/table/htable_filter_tab.yxx"
    { (yyval.cmp_op) = hfilter::CompareOperator::GREATER_OR_EQUAL; ;}
    break;

  case 47:

/* Line 1455 of yacc.c  */
#line 788 "../../../src/observer/table/htable_filter_tab.yxx"
    { (yyval.cmp_op) = hfilter::CompareOperator::NO_OP; ;}
    break;

  case 48:

/* Line 1455 of yacc.c  */
#line 789 "../../../src/observer/table/htable_filter_tab.yxx"
    { (yyval.cmp_op) = hfilter::CompareOperator::IS; ;}
    break;

  case 49:

/* Line 1455 of yacc.c  */
#line 790 "../../../src/observer/table/htable_filter_tab.yxx"
    { (yyval.cmp_op) = hfilter::CompareOperator::IS_NOT; ;}
    break;

  case 50:

/* Line 1455 of yacc.c  */
#line 794 "../../../src/observer/table/htable_filter_tab.yxx"
    { (yyval.sval) = (yyvsp[(1) - (1)].sval); ;}
    break;

  case 51:

/* Line 1455 of yacc.c  */
#line 798 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    typedef ObSEArray<ObString, 8> param_array;
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.sarray) = OB_NEWx(param_array, parse_ctx->allocator());
                    if (nullptr == (yyval.sarray)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL((yyval.sarray)->push_back(parse_ctx->create_ob_string((yyvsp[(1) - (1)].sval))))) {
                        LOG_WARN("failed to push back", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse param list");
                        YYABORT;
                    }
                ;}
    break;

  case 52:

/* Line 1455 of yacc.c  */
#line 814 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    if (OB_FAIL((yyvsp[(1) - (3)].sarray)->push_back(parse_ctx->create_ob_string((yyvsp[(3) - (3)].sval))))) {
                        LOG_WARN("failed to push back", K(ret));
                    }
                    (yyval.sarray) = (yyvsp[(1) - (3)].sarray);
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse param list");
                        YYABORT;
                    }
                ;}
    break;

  case 53:

/* Line 1455 of yacc.c  */
#line 828 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    typedef oceanbase::table::hfilter::ObPair<oceanbase::ObString, oceanbase::ObString> FuzzyKey;
                    typedef ObSEArray<FuzzyKey*, 8> ParamArray;
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.fuzzy_array) = OB_NEWx(ParamArray, parse_ctx->allocator());
                    FuzzyKey *pair = OB_NEWx(FuzzyKey, parse_ctx->allocator(), parse_ctx->create_ob_string((yyvsp[(1) - (3)].sval)), parse_ctx->create_ob_string((yyvsp[(3) - (3)].sval)));
                    if (nullptr == (yyval.fuzzy_array) || nullptr == pair) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL((yyval.fuzzy_array)->push_back(pair))) {
                        LOG_WARN("failed to push back", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse param list");
                        YYABORT;
                    }
                ;}
    break;

  case 54:

/* Line 1455 of yacc.c  */
#line 846 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    typedef oceanbase::table::hfilter::ObPair<oceanbase::ObString, oceanbase::ObString> FuzzyKey;
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    FuzzyKey *pair = OB_NEWx(FuzzyKey, parse_ctx->allocator(), parse_ctx->create_ob_string((yyvsp[(3) - (5)].sval)), parse_ctx->create_ob_string((yyvsp[(5) - (5)].sval)));
                    if (nullptr == pair) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL((yyvsp[(1) - (5)].fuzzy_array)->push_back(pair))) {
                        LOG_WARN("failed to push back", K(ret));
                    }
                    (yyval.fuzzy_array) = (yyvsp[(1) - (5)].fuzzy_array);
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse param list");
                        YYABORT;
                    }
                ;}
    break;

  case 55:

/* Line 1455 of yacc.c  */
#line 865 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    typedef ObSEArray<int64_t, 8> i64_array;
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.array) = OB_NEWx(i64_array, parse_ctx->allocator());
                    if (nullptr == (yyval.array)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL((yyval.array)->push_back((yyvsp[(1) - (1)].lval)))) {
                        LOG_WARN("failed to push back", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse timestamp list");
                        YYABORT;
                    }
                ;}
    break;

  case 56:

/* Line 1455 of yacc.c  */
#line 881 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    if (OB_FAIL((yyvsp[(1) - (3)].array)->push_back((yyvsp[(3) - (3)].lval)))) {
                        LOG_WARN("failed to push back", K(ret));
                    }
                    (yyval.array) = (yyvsp[(1) - (3)].array);
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse timestamp list");
                        YYABORT;
                    }
                ;}
    break;

  case 57:

/* Line 1455 of yacc.c  */
#line 895 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.rval) = OB_NEWx(KeyRange, parse_ctx->allocator(), parse_ctx->create_ob_string((yyvsp[(1) - (7)].sval)), (yyvsp[(3) - (7)].ival), parse_ctx->create_ob_string((yyvsp[(5) - (7)].sval)), (yyvsp[(7) - (7)].ival));
                    if (nullptr == (yyval.rval)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse range");
                        YYABORT;
                    }
                ;}
    break;

  case 58:

/* Line 1455 of yacc.c  */
#line 911 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    typedef ObSEArray<KeyRange*, 8> RangeArray;
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.range_array) = OB_NEWx(RangeArray, parse_ctx->allocator());
                    if (nullptr == (yyval.range_array)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else if (OB_FAIL((yyval.range_array)->push_back((yyvsp[(1) - (1)].rval)))) {
                        LOG_WARN("failed to push back", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse range list");
                        YYABORT;
                    }
                ;}
    break;

  case 59:

/* Line 1455 of yacc.c  */
#line 927 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    if (OB_FAIL((yyvsp[(1) - (3)].range_array)->push_back((yyvsp[(3) - (3)].rval)))) {
                        LOG_WARN("failed to push back", K(ret));
                    }
                    (yyval.range_array) = (yyvsp[(1) - (3)].range_array);
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse range list");
                        YYABORT;
                    }
                ;}
    break;

  case 60:

/* Line 1455 of yacc.c  */
#line 941 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    typedef ObSEArray<KeyRange*, 8> RangeArray;
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    ObString prefix = parse_ctx->create_ob_string((yyvsp[(1) - (1)].sval));
                    KeyRange *range = OB_NEWx(KeyRange, parse_ctx->allocator());
                    if (nullptr == (yyval.range_array)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else {
                        range->set_min(prefix);
                        range->set_min_inclusive(true);
                        (yyval.range_array) = OB_NEWx(RangeArray, parse_ctx->allocator());
                        if (nullptr == (yyval.range_array)) {
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            LOG_WARN("no memory", K(ret));
                        } else if (OB_FAIL(ObHTableUtils::get_prefix_key_range(*parse_ctx->allocator(), prefix, range))) {
                            LOG_WARN("failed to create key range", K(ret));
                        } else if (OB_FAIL((yyval.range_array)->push_back(range))) {
                            LOG_WARN("failed to push back", K(ret));
                        }
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse prefix list");
                        YYABORT;
                    }
                ;}
    break;

  case 61:

/* Line 1455 of yacc.c  */
#line 968 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    ObString prefix = parse_ctx->create_ob_string((yyvsp[(3) - (3)].sval));
                    KeyRange *range = OB_NEWx(KeyRange, parse_ctx->allocator());
                    if (nullptr == (yyval.range_array)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    } else {
                        range->set_min(prefix);
                        range->set_min_inclusive(true);
                        if (OB_FAIL(ObHTableUtils::get_prefix_key_range(*parse_ctx->allocator(), prefix, range))) {
                            LOG_WARN("failed to create key range", K(ret));
                        } else if (OB_FAIL((yyvsp[(1) - (3)].range_array)->push_back(range))) {
                            LOG_WARN("failed to push back", K(ret));
                        }
                        (yyval.range_array) = (yyvsp[(1) - (3)].range_array);
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse prefix list");
                        YYABORT;
                    }
                ;}
    break;

  case 62:

/* Line 1455 of yacc.c  */
#line 991 "../../../src/observer/table/htable_filter_tab.yxx"
    {
                    typedef ObSEArray<KeyRange*, 8> RangeArray;
                    int &ret = parse_ctx->error_code_ = OB_SUCCESS;
                    (yyval.range_array) = OB_NEWx(RangeArray, parse_ctx->allocator());
                    if (nullptr == (yyval.range_array)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("no memory", K(ret));
                    }
                    if (OB_SUCCESS != ret) {
                        ob_hfilter_error(&((yyloc)), parse_ctx, "failed to parse prefix list");
                        YYABORT;
                    }
                ;}
    break;



/* Line 1455 of yacc.c  */
#line 2868 "../../../src/observer/table/htable_filter_tab.cxx"
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
#line 1005 "../../../src/observer/table/htable_filter_tab.yxx"

