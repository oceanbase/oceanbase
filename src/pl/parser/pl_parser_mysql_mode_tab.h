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



/* Line 1676 of yacc.c  */
#line 76 "../../../src/pl/parser/pl_parser_mysql_mode_tab.h"

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

/* Line 1676 of yacc.c  */
#line 31 "../../../src/pl/parser/pl_parser_mysql_mode.y"

  struct _ParseNode *node;
  const struct _NonReservedKeyword *non_reserved_keyword;
  int64_t ival;



/* Line 1676 of yacc.c  */
#line 269 "../../../src/pl/parser/pl_parser_mysql_mode_tab.h"
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
