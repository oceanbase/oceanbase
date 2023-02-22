
/* A Bison parser, made by GNU Bison 2.4.1.  */

/* Skeleton interface for Bison's Yacc-like parsers in C
   
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
     CheckAndMutateFilter = 264,
     PrefixFilter = 265,
     TableCompareFilter = 266,
     LESS = 267,
     LESS_OR_EQUAL = 268,
     EQUAL = 269,
     NOT_EQUAL = 270,
     GREATER = 271,
     GREATER_OR_EQUAL = 272,
     NO_OP = 273,
     IS = 274,
     IS_NOT = 275,
     BOOL_VALUE = 276,
     STRING_VALUE = 277,
     INT_VALUE = 278,
     OR = 279,
     AND = 280,
     T_OR = 281,
     T_AND = 282,
     WHILE = 283,
     SKIP = 284,
     ERROR = 285
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{

/* Line 1676 of yacc.c  */
#line 24 "../../../src/observer/table/htable_filter_tab.yxx"

    int32_t ival;
    int64_t lval;
    oceanbase::table::hfilter::CompareOperator cmp_op;
    oceanbase::table::ObHTableFilterParser::SimpleString sval;
    oceanbase::table::hfilter::Filter *fval;



/* Line 1676 of yacc.c  */
#line 93 "../../../src/observer/table/htable_filter_tab.hxx"
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



