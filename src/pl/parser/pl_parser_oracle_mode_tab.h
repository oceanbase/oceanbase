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

#line 37 "../../../src/pl/parser/pl_parser_oracle_mode.y"

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



/* Line 1676 of yacc.c  */
#line 76 "../../../src/pl/parser/pl_parser_oracle_mode_tab.h"

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     END_P = 258,
     SQL_KEYWORD = 259,
     LABEL_LEFT = 260,
     LABEL_RIGHT = 261,
     ASSIGN_OPERATOR = 262,
     RANGE_OPERATOR = 263,
     PARAM_ASSIGN_OPERATOR = 264,
     IDENT = 265,
     STRING = 266,
     INTNUM = 267,
     DECIMAL_VAL = 268,
     DATE_VALUE = 269,
     QUESTIONMARK = 270,
     ALL = 271,
     ALTER = 272,
     AND = 273,
     ANY = 274,
     AS = 275,
     ASC = 276,
     AT = 277,
     BEGIN_KEY = 278,
     BETWEEN = 279,
     BY = 280,
     CASE = 281,
     CHECK = 282,
     CLUSTERS = 283,
     CLUSTER = 284,
     COLAUTH = 285,
     COLUMNS = 286,
     COMPRESS = 287,
     CONNECT = 288,
     CRASH = 289,
     CREATE = 290,
     CURSOR = 291,
     COMMIT = 292,
     DELETE = 293,
     DECLARE = 294,
     DEFAULT = 295,
     DESC = 296,
     DISTINCT = 297,
     DROP = 298,
     EXISTS = 299,
     ELSE = 300,
     ELSIF = 301,
     END_KEY = 302,
     EXCEPTION = 303,
     EXCLUSIVE = 304,
     FETCH = 305,
     FOR = 306,
     FROM = 307,
     FUNCTION = 308,
     GOTO = 309,
     GRANT = 310,
     GROUP = 311,
     HAVING = 312,
     IDENTIFIED = 313,
     IF = 314,
     IN = 315,
     INDEX = 316,
     INDEXES = 317,
     INSERT = 318,
     INTERSECT = 319,
     INTO = 320,
     IS = 321,
     LIKE = 322,
     LOCK = 323,
     MINUS = 324,
     MODE = 325,
     NOCOMPRESS = 326,
     NOT = 327,
     NOWAIT = 328,
     NULLX = 329,
     OF = 330,
     ON = 331,
     OPTION = 332,
     OR = 333,
     ORDER = 334,
     OVERLAPS = 335,
     PROCEDURE = 336,
     PUBLIC = 337,
     RESOURCE = 338,
     REVOKE = 339,
     RETURN = 340,
     ROLLBACK = 341,
     SELECT = 342,
     SHARE = 343,
     SIZE = 344,
     SQL = 345,
     START = 346,
     SUBTYPE = 347,
     SAVEPOINT = 348,
     SET = 349,
     TABAUTH = 350,
     TABLE = 351,
     THEN = 352,
     TO = 353,
     UNION = 354,
     UNIQUE = 355,
     UPDATE = 356,
     VALUES = 357,
     VIEW = 358,
     VIEWS = 359,
     WHEN = 360,
     WHERE = 361,
     WITH = 362,
     WHILE = 363,
     ACCESSIBLE = 364,
     AUTHID = 365,
     AFTER = 366,
     AGGREGATE = 367,
     AUTONOMOUS_TRANSACTION = 368,
     ARRAY = 369,
     BULK = 370,
     BYTE = 371,
     BINARY = 372,
     BOOL = 373,
     BLOB = 374,
     BINARY_DOUBLE = 375,
     BINARY_FLOAT = 376,
     BEFORE = 377,
     BODY = 378,
     C = 379,
     CALL = 380,
     CHARSET = 381,
     COLLATE = 382,
     COLLATION = 383,
     COLLECT = 384,
     COMPILE = 385,
     CURRENT_USER = 386,
     CUSTOMDATUM = 387,
     CHARACTER = 388,
     CLOB = 389,
     CONSTRUCTOR = 390,
     CONTINUE = 391,
     COMPOUND = 392,
     CLOSE = 393,
     CONSTANT = 394,
     DEFINER = 395,
     DETERMINISTIC = 396,
     DAY = 397,
     DEBUG = 398,
     DISABLE = 399,
     DATE = 400,
     EDITIONABLE = 401,
     EXECUTE = 402,
     EACH = 403,
     ENABLE = 404,
     ERROR = 405,
     EXCEPTIONS = 406,
     EXTERNAL = 407,
     EXCEPTION_INIT = 408,
     EXIT = 409,
     FINAL = 410,
     FORCE = 411,
     FORALL = 412,
     FLOAT = 413,
     HASH = 414,
     HOUR = 415,
     IMMEDIATE = 416,
     INDICES = 417,
     INSTEAD = 418,
     INTERFACE = 419,
     JAVA = 420,
     INTERVAL = 421,
     INSTANTIABLE = 422,
     LIMIT = 423,
     LOCAL = 424,
     LONG = 425,
     LANGUAGE = 426,
     LEVEL = 427,
     LOOP = 428,
     INLINE = 429,
     MONTH = 430,
     MINUTE = 431,
     MAP = 432,
     MEMBER = 433,
     MERGE = 434,
     NAME = 435,
     NESTED = 436,
     NO = 437,
     NONEDITIONABLE = 438,
     NATURALN = 439,
     NUMERIC = 440,
     NVARCHAR2 = 441,
     NCHAR = 442,
     NOCOPY = 443,
     NEW = 444,
     NATURAL = 445,
     NUMBER = 446,
     OBJECT = 447,
     OID = 448,
     OPAQUE = 449,
     ORADATA = 450,
     OVERRIDING = 451,
     OLD = 452,
     OPEN = 453,
     OTHERS = 454,
     OUT = 455,
     PARALLEL_ENABLE = 456,
     PIPE = 457,
     PIPELINED = 458,
     PLS_INTEGER = 459,
     BINARY_INTEGER = 460,
     POSITIVEN = 461,
     PARENT = 462,
     POSITIVE = 463,
     PRAGMA = 464,
     PACKAGE_P = 465,
     PARTITION = 466,
     REF = 467,
     RELIES_ON = 468,
     REPLACE = 469,
     RESTRICT_REFERENCES = 470,
     RESULT = 471,
     RESULT_CACHE = 472,
     RETURNING = 473,
     RNDS = 474,
     RNPS = 475,
     RAISE = 476,
     RECORD = 477,
     RAW = 478,
     REUSE = 479,
     REFERENCING = 480,
     ROW = 481,
     REVERSE = 482,
     ROWTYPE = 483,
     RANGE = 484,
     REAL = 485,
     ROWID = 486,
     SAVE = 487,
     SERIALLY_REUSABLE = 488,
     SETTINGS = 489,
     SPECIFICATION = 490,
     SQLDATA = 491,
     SECOND = 492,
     SELF = 493,
     SIGNTYPE = 494,
     SIMPLE_INTEGER = 495,
     STATIC = 496,
     SIMPLE_DOUBLE = 497,
     SIMPLE_FLOAT = 498,
     STATEMENT = 499,
     TIME = 500,
     TRIGGER = 501,
     TRUST = 502,
     TIMESTAMP = 503,
     TYPE = 504,
     UDF = 505,
     UNDER = 506,
     USING = 507,
     USING_NLS_COMP = 508,
     UROWID = 509,
     VALIDATE = 510,
     VALUE = 511,
     VARIABLE = 512,
     VARRAY = 513,
     VARYING = 514,
     VARCHAR = 515,
     VARCHAR2 = 516,
     WNDS = 517,
     WNPS = 518,
     YEAR = 519,
     YES = 520,
     ZONE = 521,
     LOWER_PARENS = 522,
     DECLARATION = 523
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{

/* Line 1676 of yacc.c  */
#line 31 "../../../src/pl/parser/pl_parser_oracle_mode.y"

  struct _ParseNode *node;
  const struct _NonReservedKeyword *non_reserved_keyword;
  int32_t ival[3]; //ival[0]表示value, ival[1]表示fast parse在对应的该node及其子node可识别的常量个数, ival[2] for length_semantics



/* Line 1676 of yacc.c  */
#line 369 "../../../src/pl/parser/pl_parser_oracle_mode_tab.h"
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
