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

#ifndef OCEANBASE_COMMON_OB_SQL_MODE_H
#define OCEANBASE_COMMON_OB_SQL_MODE_H
#include <stdint.h>

/// @see http://dev.mysql.com/doc/refman/5.6/en/sql-mode.html
// These types will also by used by parser, so define them as C symbols
#ifdef __cplusplus
extern "C" {
#endif

/* individual modes */

#define SMO_REAL_AS_FLOAT (1ULL)           /* support */
#define SMO_PIPES_AS_CONCAT (1ULL << 1)         /* support */
#define SMO_ANSI_QUOTES (1ULL << 2)    /* support */
#define SMO_IGNORE_SPACE (1ULL << 3) /* support but not used */
#define SMO_NOT_USED (1ULL << 4) /* not support now */
#define SMO_ONLY_FULL_GROUP_BY (1ULL << 5) /* support */
#define SMO_NO_UNSIGNED_SUBTRACTION (1ULL << 6) /* support */
#define SMO_NO_DIR_IN_CREATE (1ULL << 7)      /* support but not used */
#define SMO_POSTGRESQL (1ULL << 8) /*not support*/
#define SMO_ORACLE (1ULL << 9) /*not support*/
#define SMO_MSSQL (1ULL << 10) /*not support*/
#define SMO_DB2 (1ULL << 11) /*not support*/
#define MODE_MAXDB (1ULL << 12) /*not support*/
#define SMO_NO_KEY_OPTIONS (1ULL << 13)        /* not support */
#define SMO_NO_TABLE_OPTIONS (1ULL << 14)      /* not support */
#define SMO_NO_FIELD_OPTIONS (1ULL << 15)      /* not support */
#define SMO_MYSQL323 (1ULL << 16) /*not support*/
#define SMO_MYSQL40 (1ULL << 17) /*not support*/
#define SMO_ANSI (1ULL << 18)
#define SMO_NO_AUTO_VALUE_ON_ZERO (1ULL << 19) /* support */
#define SMO_NO_BACKSLASH_ESCAPES (1ULL << 20)  /* support */
#define SMO_STRICT_TRANS_TABLES (1ULL << 21)     /* support */
#define SMO_STRICT_ALL_TABLES (1ULL << 22)       /* support */
#define SMO_NO_ZERO_IN_DATE (1ULL << 23) /* deprecated as of MySQL 5.6.17 */
#define SMO_NO_ZERO_DATE (1ULL << 24) /* deprecated as of MySQL 5.6.17 */
#define SMO_ALLOW_INVALID_DATES (1ULL << 25) /* support */
#define SMO_ERROR_FOR_DIVISION_BY_ZERO (1ULL << 26) /* deprecated as of MySQL 5.6.17 */
#define SMO_TRADITIONAL (1ULL << 27)
#define SMO_NO_AUTO_CREATE_USER (1ULL << 28) /* support */
#define SMO_HIGH_NOT_PRECEDENCE (1ULL << 29)        /*support */
#define SMO_NO_ENGINE_SUBSTITUTION (1ULL << 30) /* support but not used */
#define SMO_PAD_CHAR_TO_FULL_LENGTH (1ULL << 31) /* support */
#define SMO_ERROR_ON_RESOLVE_CAST (1ULL << 32) /* ERROR_ON_RESOLVE_CAST */
#define SMO_TIME_TRUNCATE_FRACTIONAL (1ULL << 33) /* TIME_TRUNCATE_FRACTIONAL */

#define STR_ALLOW_INVALID_DATES "ALLOW_INVALID_DATES"
#define STR_ANSI_QUOTES "ANSI_QUOTES"
#define STR_ERROR_FOR_DIVISION_BY_ZERO "ERROR_FOR_DIVISION_BY_ZERO"
#define STR_HIGH_NOT_PRECEDENCE "HIGH_NOT_PRECEDENCE"
#define STR_IGNORE_SPACE "IGNORE_SPACE"
#define STR_NOT_USED ","
#define STR_NO_AUTO_CREATE_USER "NO_AUTO_CREATE_USER"
#define STR_NO_AUTO_VALUE_ON_ZERO "NO_AUTO_VALUE_ON_ZERO"
#define STR_NO_BACKSLASH_ESCAPES "NO_BACKSLASH_ESCAPES"
#define STR_NO_DIR_IN_CREATE "NO_DIR_IN_CREATE"
#define STR_NO_ENGINE_SUBSTITUTION "NO_ENGINE_SUBSTITUTION"
#define STR_NO_FIELD_OPTIONS "NO_FIELD_OPTIONS"
#define STR_NO_KEY_OPTIONS "NO_KEY_OPTIONS"
#define STR_NO_TABLE_OPTIONS "NO_TABLE_OPTIONS"
#define STR_NO_UNSIGNED_SUBTRACTION "NO_UNSIGNED_SUBTRACTION"
#define STR_NO_ZERO_DATE "NO_ZERO_DATE"
#define STR_NO_ZERO_IN_DATE "NO_ZERO_IN_DATE"
#define STR_ONLY_FULL_GROUP_BY "ONLY_FULL_GROUP_BY"
#define STR_PAD_CHAR_TO_FULL_LENGTH "PAD_CHAR_TO_FULL_LENGTH"
#define STR_PIPES_AS_CONCAT "PIPES_AS_CONCAT"
#define STR_REAL_AS_FLOAT "REAL_AS_FLOAT"
#define STR_STRICT_ALL_TABLES "STRICT_ALL_TABLES"
#define STR_STRICT_TRANS_TABLES "STRICT_TRANS_TABLES"
#define STR_ERROR_ON_RESOLVE_CAST "ERROR_ON_RESOLVE_CAST"
#define STR_STANDARD_ASSIGNMENT "STANDARD_ASSIGNMENT"
#define STR_TRADITIONAL  "TRADITIONAL"
#define STR_ANSI "ANSI"
#define STR_DB2 "DB2"
#define STR_MAXDB "MAXDB"
#define STR_MSSQL "MSSQL"
#define STR_ORACLE "ORACLE"
#define STR_POSTGRESQL "POSTGRESQL"
#define STR_MYSQL323 "MYSQL323"
#define STR_MYSQL40 "MYSQL40"
#define STR_TIME_TRUNCATE_FRACTIONAL "TIME_TRUNCATE_FRACTIONAL"

#define COMBINE_SMO_TRADITIONAL (SMO_STRICT_TRANS_TABLES | SMO_STRICT_ALL_TABLES | SMO_NO_ZERO_IN_DATE | \
                                 SMO_NO_ZERO_DATE | SMO_ERROR_FOR_DIVISION_BY_ZERO | SMO_NO_AUTO_CREATE_USER | \
                                 SMO_NO_ENGINE_SUBSTITUTION | SMO_TRADITIONAL)
#define COMBINE_SMO_ANSI (SMO_REAL_AS_FLOAT | SMO_PIPES_AS_CONCAT | SMO_ANSI_QUOTES | SMO_IGNORE_SPACE | \
                          SMO_ANSI)
#define COMBINE_SMO_DB2 (SMO_PIPES_AS_CONCAT | SMO_ANSI_QUOTES | SMO_IGNORE_SPACE | SMO_NO_KEY_OPTIONS | \
                         SMO_NO_TABLE_OPTIONS | SMO_NO_FIELD_OPTIONS | SMO_DB2)
#define COMBINE_SMO_MAXDB (SMO_PIPES_AS_CONCAT | SMO_ANSI_QUOTES | SMO_IGNORE_SPACE | SMO_NO_KEY_OPTIONS | \
                           SMO_NO_TABLE_OPTIONS | SMO_NO_FIELD_OPTIONS | SMO_NO_AUTO_CREATE_USER | \
                           MODE_MAXDB)
#define COMBINE_SMO_MSSQL (SMO_PIPES_AS_CONCAT | SMO_ANSI_QUOTES | SMO_IGNORE_SPACE | SMO_NO_KEY_OPTIONS | \
                           SMO_NO_TABLE_OPTIONS | SMO_NO_FIELD_OPTIONS | \
                           SMO_MSSQL)
#define COMBINE_SMO_ORACLE (SMO_PIPES_AS_CONCAT | SMO_ANSI_QUOTES | SMO_IGNORE_SPACE | SMO_NO_KEY_OPTIONS | \
                            SMO_NO_TABLE_OPTIONS | SMO_NO_FIELD_OPTIONS | SMO_NO_AUTO_CREATE_USER | \
                            SMO_ORACLE)
#define COMBINE_SMO_POSTGRESQL (SMO_PIPES_AS_CONCAT |  SMO_ANSI_QUOTES | SMO_IGNORE_SPACE | SMO_NO_KEY_OPTIONS | \
                                SMO_NO_TABLE_OPTIONS | SMO_NO_FIELD_OPTIONS | \
                                SMO_POSTGRESQL)
#define COMBINE_SMO_MYSQL323 (SMO_HIGH_NOT_PRECEDENCE | SMO_MYSQL323)
#define COMBINE_SMO_MYSQL40 (SMO_HIGH_NOT_PRECEDENCE | SMO_MYSQL40)

#define ALL_SMO_COMPACT_MODE (SMO_POSTGRESQL | SMO_ORACLE | SMO_MSSQL | SMO_DB2)

#define SMO_DEFAULT (SMO_TRADITIONAL|SMO_ONLY_FULL_GROUP_BY)
typedef uint64_t ObSQLMode;

#define STR_OCEANBASE_MODE  "OCEANBASE_MODE"
#define STR_MYSQL_MODE  "MYSQL_MODE"
#define STR_ORACLE_MODE  "ORACLE_MODE"
#define DEFAULT_OCEANBASE_MODE (SMO_STRICT_ALL_TABLES)
#define STR_DEFAULT_OCEANBASE_MODE "STRICT_ALL_TABLES"
#define STR_DEFAULT_MYSQL_MODE "STRICT_ALL_TABLES"
#define DEFAULT_MYSQL_MODE (SMO_STRICT_ALL_TABLES)
#define DEFAULT_ORACLE_MODE (SMO_STRICT_ALL_TABLES | SMO_PIPES_AS_CONCAT | SMO_PAD_CHAR_TO_FULL_LENGTH)
#define IS_PIPES_AS_CONCAT(mode, is_as)\
{\
  is_as = (SMO_PIPES_AS_CONCAT & mode);\
}\

#define SMO_IS_ORACLE_MODE(mode)   (SMO_ORACLE & (mode))

#define IS_HIGH_NOT_PRECEDENCE(mode, is_true)\
{\
  is_true = (SMO_HIGH_NOT_PRECEDENCE & mode);\
}\

#define IS_NO_BACKSLASH_ESCAPES(mode, is_true)\
{\
  is_true = (SMO_NO_BACKSLASH_ESCAPES & mode);\
}\

#define IS_ANSI_QUOTES(mode, is_true)\
{\
  is_true = (SMO_ANSI_QUOTES & mode);\
}\

#ifdef __cplusplus
}
#endif

#endif /*OCEANBASE_COMMON_OB_SQL_MODE_H */
