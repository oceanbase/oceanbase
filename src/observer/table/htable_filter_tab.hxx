
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

/* "%code requires" blocks.  */

/* Line 1676 of yacc.c  */
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



/* Line 1676 of yacc.c  */
#line 56 "../../../src/observer/table/htable_filter_tab.hxx"

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

/* Line 1676 of yacc.c  */
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



/* Line 1676 of yacc.c  */
#line 135 "../../../src/observer/table/htable_filter_tab.hxx"
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



