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

#ifndef OCEANBASE_RPC_OBMYSQL_OB_MYSQL_GLOBAL_
#define OCEANBASE_RPC_OBMYSQL_OB_MYSQL_GLOBAL_

#include <float.h>
#include "easy_define.h"  // for conflict of macro likely
#include "lib/charset/ob_mysql_global.h"

/* From limits.h instead */
/*
#ifndef DBL_MIN
#define DBL_MIN		4.94065645841246544e-324
#define FLT_MIN		((float)1.40129846432481707e-45)
#endif
#ifndef DBL_MAX
#define DBL_MAX		1.79769313486231470e+308
#define FLT_MAX		((float)3.40282346638528860e+38)
#endif
#ifndef SIZE_T_MAX
#define SIZE_T_MAX      (~((size_t) 0))
#endif
*/


/*

#define sint2korr(A)  (int16_t) (((int16_t) ((uint8_t) (A)[0])) +     \
                                 ((int16_t) ((int16_t) (A)[1]) << 8))
#define sint3korr(A)  ((int32_t) ((((uint8_t) (A)[2]) & 128) ?        \
                                  (((uint32_t) 255L << 24) |          \
                                   (((uint32_t) (uint8_t) (A)[2]) << 16) | \
                                   (((uint32_t) (uint8_t) (A)[1]) << 8) | \
                                   ((uint32_t) (uint8_t) (A)[0])) :   \
                                  (((uint32_t) (uint8_t) (A)[2]) << 16) | \
                                  (((uint32_t) (uint8_t) (A)[1]) << 8) | \
                                  ((uint32_t) (uint8_t) (A)[0])))
#define sint4korr(A)  (int32_t) (((int32_t) ((uint8_t) (A)[0])) +     \
                                 (((int32_t) ((uint8_t) (A)[1]) << 8)) + \
                                 (((int32_t) ((uint8_t) (A)[2]) << 16)) + \
                                 (((int32_t) ((int16_t) (A)[3]) << 24)))
#define sint8korr(A)  (int64_t) uint8korr(A)
#define uint2korr(A)  (uint16_t) (((uint16_t) ((uint8_t) (A)[0])) +   \
                                  ((uint16_t) ((uint8_t) (A)[1]) << 8))
#define uint3korr(A)  (uint32_t) (((uint32_t) ((uint8_t) (A)[0])) +   \
                                  (((uint32_t) ((uint8_t) (A)[1])) << 8) + \
                                  (((uint32_t) ((uint8_t) (A)[2])) << 16))
#define uint4korr(A)  (uint32_t) (((uint32_t) ((uint8_t) (A)[0])) +   \
                                  (((uint32_t) ((uint8_t) (A)[1])) << 8) + \
                                  (((uint32_t) ((uint8_t) (A)[2])) << 16) + \
                                  (((uint32_t) ((uint8_t) (A)[3])) << 24))
#define uint5korr(A)  ((uint64_t)(((uint32_t) ((uint8_t) (A)[0])) +   \
                                  (((uint32_t) ((uint8_t) (A)[1])) << 8) + \
                                  (((uint32_t) ((uint8_t) (A)[2])) << 16) + \
                                  (((uint32_t) ((uint8_t) (A)[3])) << 24)) + \
                       (((uint64_t) ((uint8_t) (A)[4])) << 32))
#define uint6korr(A)  ((uint64_t)(((uint32_t)    ((uint8_t) (A)[0]))          + \
                                  (((uint32_t)    ((uint8_t) (A)[1])) << 8)   + \
                                  (((uint32_t)    ((uint8_t) (A)[2])) << 16)  + \
                                  (((uint32_t)    ((uint8_t) (A)[3])) << 24)) + \
                       (((uint64_t) ((uint8_t) (A)[4])) << 32) +      \
                       (((uint64_t) ((uint8_t) (A)[5])) << 40))
#define uint8korr(A)  ((uint64_t)(((uint32_t) ((uint8_t) (A)[0])) +   \
                                  (((uint32_t) ((uint8_t) (A)[1])) << 8) + \
                                  (((uint32_t) ((uint8_t) (A)[2])) << 16) + \
                                  (((uint32_t) ((uint8_t) (A)[3])) << 24)) + \
                       (((uint64_t) (((uint32_t) ((uint8_t) (A)[4])) + \
                                     (((uint32_t) ((uint8_t) (A)[5])) << 8) + \
                                     (((uint32_t) ((uint8_t) (A)[6])) << 16) + \
                                     (((uint32_t) ((uint8_t) (A)[7])) << 24))) << \
                        32))
*/

#if 0
#define int2store(T,A)       do { uint def_temp= (uint) (A) ;   \
    *((uint8_t*) (T))=  (uint8_t)(def_temp);                    \
    *((uint8_t*) (T)+1)=(uint8_t)((def_temp >> 8));             \
  } while(0)
#define int3store(T,A)       do { /*lint -save -e734 */ \
    *((uint8_t*)(T))=(uint8_t) ((A));                   \
    *((uint8_t*) (T)+1)=(uint8_t) (((A) >> 8));         \
    *((uint8_t*)(T)+2)=(uint8_t) (((A) >> 16));         \
    /*lint -restore */} while(0)
#define int4store(T,A)       do { *((char *)(T))=(char) ((A));  \
    *(((char *)(T))+1)=(char) (((A) >> 8));                     \
    *(((char *)(T))+2)=(char) (((A) >> 16));                    \
    *(((char *)(T))+3)=(char) (((A) >> 24)); } while(0)
#define int5store(T,A)       do { *((char *)(T))=     (char)((A));      \
    *(((char *)(T))+1)= (char)(((A) >> 8));                             \
    *(((char *)(T))+2)= (char)(((A) >> 16));                            \
    *(((char *)(T))+3)= (char)(((A) >> 24));                            \
    *(((char *)(T))+4)= (char)(((A) >> 32));                            \
  } while(0)
#define int6store(T,A)       do { *((char *)(T))=     (char)((A));      \
    *(((char *)(T))+1)= (char)(((A) >> 8));                             \
    *(((char *)(T))+2)= (char)(((A) >> 16));                            \
    *(((char *)(T))+3)= (char)(((A) >> 24));                            \
    *(((char *)(T))+4)= (char)(((A) >> 32));                            \
    *(((char *)(T))+5)= (char)(((A) >> 40));                            \
  } while(0)
#define int8store(T,A)       do { uint def_temp= (uint) (A), def_temp2= (uint) ((A) >> 32); \
    int4store((T),def_temp);                                            \
    int4store((T+4),def_temp2); } while(0)
#endif

#define float4get(V,M)   MEMCPY(&V, (M), sizeof(float))
#define float4store(V,M) MEMCPY(V, (&M), sizeof(float))

#define float8get(V,M)   doubleget((V),(M))
#define float8store(V,M) doublestore((V),(M))
/*
#define int4net(A)        (int32_t) (((uint32_t) ((uint8_t) (A)[3]))        | \
                                     (((uint32_t) ((uint8_t) (A)[2])) << 8)  | \
                                     (((uint32_t) ((uint8_t) (A)[1])) << 16) | \
                                     (((uint32_t) ((uint8_t) (A)[0])) << 24))
*/
/*
  Define-funktions for reading and storing in machine format from/to
  short/long to/from some place in memory V should be a (not
  register) variable, M is a pointer to byte
*/
#if 0
#define ushortget(V,M)  do { V = uint2korr(M); } while(0)
#define shortget(V,M) do { V = sint2korr(M); } while(0)
#define longget(V,M)  do { V = sint4korr(M); } while(0)
#define ulongget(V,M)   do { V = uint4korr(M); } while(0)
#define shortstore(T,V) int2store(T,V)
#define longstore(T,V)  int4store(T,V)
#ifndef floatstore
#define floatstore(T,V)  MEMCPY((T), (void *) (&V), sizeof(float))
#define floatget(V,M)    MEMCPY(&V, (M), sizeof(float))
#endif
#ifndef doubleget
#define doubleget(V,M)   MEMCPY(&V, (M), sizeof(double))
#define doublestore(T,V) MEMCPY((T), (void *) &V, sizeof(double))
#endif /* doubleget */
#define longlongget(V,M) MEMCPY(&V, (M), sizeof(uint64_t))
#define longlongstore(T,V) MEMCPY((T), &V, sizeof(uint64_t))
#endif /*0*/

//#define MY_ALIGN(A,L) (((A) + (L) - 1) & ~((L) - 1))
//#define ALIGN_SIZE(A) MY_ALIGN((A),sizeof(double))

#define SIZEOF_CHARP 8
#define NOT_FIXED_DEC 31
#define ORACLE_NOT_FIXED_DEC -1

/*
  The longest string ob_fcvt can return is 311 + "precision" bytes.
  Here we assume that we never cal ob_fcvt() with precision >= NOT_FIXED_DEC
  (+ 1 byte for the terminating '\0').
*/
#define FLOATING_POINT_BUFFER (311 + NOT_FIXED_DEC)

#define DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE FLOATING_POINT_BUFFER
/* The fabs(float) < 10^39 */
#define FLOAT_TO_STRING_CONVERSION_BUFFER_SIZE (39 + NOT_FIXED_DEC)

#define MAX_TINYINT_WIDTH       3       /* Max width for a TINY w.o. sign */
#define MAX_SMALLINT_WIDTH      5       /* Max width for a SHORT w.o. sign */
#define MAX_MEDIUMINT_WIDTH     8       /* Max width for a INT24 w.o. sign */
#define MAX_INT_WIDTH           10      /* Max width for a LONG w.o. sign */
#define MAX_BIGINT_WIDTH        20      /* Max width for a LONGLONG */
#define MAX_CHAR_WIDTH    255 /* Max length for a CHAR colum */

#define OB_MAX_BLOB_WIDTH    16777216  /* Default width for ob blob */

#define MAX_DECPT_FOR_F_FORMAT DBL_DIG
#define MAX_DATETIME_WIDTH  19  /* YYYY-MM-DD HH:MM:SS */

/* -[digits].E+## */
#define MAX_FLOAT_STR_LENGTH (FLT_DIG + 6)
/* -[digits].E+### */
#define MAX_DOUBLE_STR_LENGTH (DBL_DIG + 7)

#define DEFAULT_FLOAT_PRECISION -1

#define DEFAULT_FLOAT_SCALE -1

#define DEFAULT_DOUBLE_PRECISION -1

#define DEFAULT_DOUBLE_SCALE -1

namespace oceanbase
{
namespace obmysql
{

// from mysql_com.h, for the flags in Field.
#define NOT_NULL_FLAG 1   /* Field can't be NULL */
#define PRI_KEY_FLAG  2   /* Field is part of a primary key */
#define UNIQUE_KEY_FLAG 4   /* Field is part of a unique key */
#define MULTIPLE_KEY_FLAG 8   /* Field is part of a key */
#define BLOB_FLAG 16    /* Field is a blob */
#define UNSIGNED_FLAG 32    /* Field is unsigned */
#define ZEROFILL_FLAG 64    /* Field is zerofill */
#define BINARY_FLAG 128   /* Field is binary   */

/* The following are only sent to new clients */
#define ENUM_FLAG 256   /* field is an enum */
#define AUTO_INCREMENT_FLAG 512   /* field is a autoincrement field */
#define TIMESTAMP_FLAG  1024    /* Field is a timestamp */
#define SET_FLAG  2048    /* field is a set */
#define NO_DEFAULT_VALUE_FLAG 4096  /* Field doesn't have default value */
#define ON_UPDATE_NOW_FLAG 8192         /* Field is set to NOW on UPDATE */
#define NUM_FLAG  32768   /* Field is num (for clients) */
#define PART_KEY_FLAG 16384   /* Intern; Part of some key */
#define GROUP_FLAG  32768   /* Intern: Group field */
#define UNIQUE_FLAG 65536   /* Intern: Used by sql_yacc */
#define BINCMP_FLAG 131072    /* Intern: Used by sql_yacc */
#define GET_FIXED_FIELDS_FLAG (1 << 18) /* Used to get fields in item tree */
#define FIELD_IN_PART_FUNC_FLAG (1 << 19)/* Field part of partition func */
#define NOT_NULL_WRITE_FLAG (1 << 20)
#define HAS_NOT_NULL_VALIDATE_CONSTRAINT_FLAG (1 << 21)
#define HAS_LOB_HEADER_FLAG (1 << 22)
#define DECIMAL_INT_ADJUST_FLAG (1 << 23)

enum EMySQLFieldType
{
  MYSQL_TYPE_DECIMAL,
  MYSQL_TYPE_TINY,
  MYSQL_TYPE_SHORT,
  MYSQL_TYPE_LONG,
  MYSQL_TYPE_FLOAT,
  MYSQL_TYPE_DOUBLE,
  MYSQL_TYPE_NULL,
  MYSQL_TYPE_TIMESTAMP,
  MYSQL_TYPE_LONGLONG,
  MYSQL_TYPE_INT24,
  MYSQL_TYPE_DATE,
  MYSQL_TYPE_TIME,
  MYSQL_TYPE_DATETIME,
  MYSQL_TYPE_YEAR,
  MYSQL_TYPE_NEWDATE,
  MYSQL_TYPE_VARCHAR,
  MYSQL_TYPE_BIT,
  MYSQL_TYPE_COMPLEX = 160, //0xa0
  MYSQL_TYPE_ARRAY = 161, //0xa1
  MYSQL_TYPE_STRUCT = 162, //0xa2
  MYSQL_TYPE_CURSOR = 163, //0xa3
  MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE = 200,
  MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE = 201,
  MYSQL_TYPE_OB_TIMESTAMP_NANO = 202,
  MYSQL_TYPE_OB_RAW = 203,
  MYSQL_TYPE_OB_INTERVAL_YM = 204,
  MYSQL_TYPE_OB_INTERVAL_DS = 205,
  MYSQL_TYPE_OB_NUMBER_FLOAT = 206,
  MYSQL_TYPE_OB_NVARCHAR2 = 207,
  MYSQL_TYPE_OB_NCHAR = 208,
  MYSQL_TYPE_OB_UROWID = 209,
  MYSQL_TYPE_ORA_BLOB = 210,
  MYSQL_TYPE_ORA_CLOB = 211,
  MYSQL_TYPE_ROARINGBITMAP = 215,
  MYSQL_TYPE_JSON = 245,
  MYSQL_TYPE_NEWDECIMAL = 246,
  MYSQL_TYPE_ENUM = 247,
  MYSQL_TYPE_SET = 248,
  MYSQL_TYPE_TINY_BLOB = 249,
  MYSQL_TYPE_MEDIUM_BLOB = 250,
  MYSQL_TYPE_LONG_BLOB = 251,
  MYSQL_TYPE_BLOB = 252,
  MYSQL_TYPE_VAR_STRING = 253,
  MYSQL_TYPE_STRING = 254,
  MYSQL_TYPE_GEOMETRY = 255,
  MYSQL_TYPE_ORA_BINARY_FLOAT = 256,
  MYSQL_TYPE_ORA_BINARY_DOUBLE = 257,
  MYSQL_TYPE_ORA_XML = 258,
  MYSQL_TYPE_NOT_DEFINED = 65535
};

} // end of namespace obmysql
} // end of namespace oceanbase

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
const char *get_emysql_field_type_str(const obmysql::EMySQLFieldType &type);

inline const char *get_emysql_field_type_str(const obmysql::EMySQLFieldType &type)
{
  const char *str = "UNKNOWN_TYPE";
  switch (type) {
    case obmysql::MYSQL_TYPE_DECIMAL:
      str = "MYSQL_TYPE_DECIMAL";
      break;
    case obmysql::MYSQL_TYPE_TINY:
      str = "MYSQL_TYPE_TINY";
      break;
    case obmysql::MYSQL_TYPE_SHORT:
      str = "MYSQL_TYPE_SHORT";
      break;
    case obmysql::MYSQL_TYPE_LONG:
      str = "MYSQL_TYPE_LONG";
      break;
    case obmysql::MYSQL_TYPE_FLOAT:
      str = "MYSQL_TYPE_FLOAT";
      break;
    case obmysql::MYSQL_TYPE_DOUBLE:
      str = "MYSQL_TYPE_DOUBLE";
      break;
    case obmysql::MYSQL_TYPE_NULL:
      str = "MYSQL_TYPE_NULL";
      break;
    case obmysql::MYSQL_TYPE_TIMESTAMP:
      str = "MYSQL_TYPE_TIMESTAMP";
      break;
    case obmysql::MYSQL_TYPE_LONGLONG:
      str = "MYSQL_TYPE_LONGLONG";
      break;
    case obmysql::MYSQL_TYPE_INT24:
      str = "MYSQL_TYPE_INT24";
      break;
    case obmysql::MYSQL_TYPE_DATE:
      str = "MYSQL_TYPE_DATE";
      break;
    case obmysql::MYSQL_TYPE_TIME:
      str = "MYSQL_TYPE_TIME";
      break;
    case obmysql::MYSQL_TYPE_DATETIME:
      str = "MYSQL_TYPE_DATETIME";
      break;
    case obmysql::MYSQL_TYPE_YEAR:
      str = "MYSQL_TYPE_YEAR";
      break;
    case obmysql::MYSQL_TYPE_NEWDATE:
      str = "MYSQL_TYPE_NEWDATE";
      break;
    case obmysql::MYSQL_TYPE_VARCHAR:
      str = "MYSQL_TYPE_VARCHAR";
      break;
    case obmysql::MYSQL_TYPE_BIT:
      str = "MYSQL_TYPE_BIT";
      break;
    case obmysql::MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:
      str = "MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE";
      break;
    case obmysql::MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      str = "MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE";
      break;
    case obmysql::MYSQL_TYPE_OB_TIMESTAMP_NANO:
      str = "MYSQL_TYPE_OB_TIMESTAMP_NANO";
      break;
    case obmysql::MYSQL_TYPE_OB_RAW:
      str = "MYSQL_TYPE_OB_RAW";
      break;
    case obmysql::MYSQL_TYPE_ROARINGBITMAP:
      str = "MYSQL_TYPE_ROARINGBITMAP";
      break;
    case obmysql::MYSQL_TYPE_NEWDECIMAL:
      str = "MYSQL_TYPE_NEWDECIMAL";
      break;
    case obmysql::MYSQL_TYPE_ENUM:
      str = "MYSQL_TYPE_ENUM";
      break;
    case obmysql::MYSQL_TYPE_SET:
      str = "MYSQL_TYPE_SET";
      break;
    case obmysql::MYSQL_TYPE_TINY_BLOB:
      str = "MYSQL_TYPE_TINYBLOB";
      break;
    case obmysql::MYSQL_TYPE_MEDIUM_BLOB:
      str = "MYSQL_TYPE_MEDIUMBLOB";
      break;
    case obmysql::MYSQL_TYPE_LONG_BLOB:
      str = "MYSQL_TYPE_LONGBLOB";
      break;
    case obmysql::MYSQL_TYPE_BLOB:
      str = "MYSQL_TYPE_BLOB";
      break;
    case obmysql::MYSQL_TYPE_VAR_STRING:
      str = "MYSQL_TYPE_VAR_STRING";
      break;
    case obmysql::MYSQL_TYPE_STRING:
      str = "MYSQL_TYPE_STRING";
      break;
    case obmysql::MYSQL_TYPE_GEOMETRY:
      str = "MYSQL_TYPE_GEOMETRY";
      break;
    case obmysql::MYSQL_TYPE_NOT_DEFINED:
      str = "MYSQL_TYPE_NOTDEFINED";
      break;
    case obmysql::MYSQL_TYPE_OB_INTERVAL_YM:
      str = "MYSQL_TYPE_OB_INTERVAL_YM";
      break;
    case obmysql::MYSQL_TYPE_OB_INTERVAL_DS:
      str = "MYSQL_TYPE_OB_INTERVAL_DS";
      break;
    case obmysql::MYSQL_TYPE_OB_NUMBER_FLOAT:
      str = "MYSQL_TYPE_OB_NUMBER_FLOAT";
      break;
    case obmysql::MYSQL_TYPE_OB_NVARCHAR2:
      str = "MYSQL_TYPE_OB_NVARCHAR2";
      break;
    case obmysql::MYSQL_TYPE_OB_NCHAR:
      str = "MYSQL_TYPE_OB_NCHAR";
      break;
    case obmysql::MYSQL_TYPE_OB_UROWID:
      str = "MYSQL_TYPE_OB_UROWID";
      break;
    case obmysql::MYSQL_TYPE_ORA_BLOB:
      str = "MYSQL_TYPE_ORA_BLOB";
      break;
    case obmysql::MYSQL_TYPE_ORA_CLOB:
      str = "MYSQL_TYPE_ORA_CLOB";
      break;
    case obmysql::MYSQL_TYPE_JSON:
      str = "MYSQL_TYPE_JSON";
      break;
    default:
      break;
  }
  return str;
}

inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const obmysql::EMySQLFieldType &type)
{
  return common::databuff_printf(buf, buf_len, pos, "%s", get_emysql_field_type_str(type));
}

inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                  const bool with_comma, const obmysql::EMySQLFieldType &type)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:\"%s\""), key, get_emysql_field_type_str(type));
}

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_RPC_OBMYSQL_OB_MYSQL_GLOBAL_
