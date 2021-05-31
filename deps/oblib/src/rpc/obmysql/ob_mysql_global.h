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

#define float4get(V, M) MEMCPY(&V, (M), sizeof(float))
#define float4store(V, M) MEMCPY(V, (&M), sizeof(float))

#define float8get(V, M) doubleget((V), (M))
#define float8store(V, M) doublestore((V), (M))

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

#define MAX_TINYINT_WIDTH 3   /* Max width for a TINY w.o. sign */
#define MAX_SMALLINT_WIDTH 5  /* Max width for a SHORT w.o. sign */
#define MAX_MEDIUMINT_WIDTH 8 /* Max width for a INT24 w.o. sign */
#define MAX_INT_WIDTH 10      /* Max width for a LONG w.o. sign */
#define MAX_BIGINT_WIDTH 20   /* Max width for a LONGLONG */
#define MAX_CHAR_WIDTH 255    /* Max length for a CHAR colum */

#define MAX_DECPT_FOR_F_FORMAT DBL_DIG
#define MAX_DATETIME_WIDTH 19 /* YYYY-MM-DD HH:MM:SS */

/* -[digits].E+## */
#define MAX_FLOAT_STR_LENGTH (FLT_DIG + 6)
/* -[digits].E+### */
#define MAX_DOUBLE_STR_LENGTH (DBL_DIG + 7)

#define DEFAULT_FLOAT_PRECISION -1

#define DEFAULT_FLOAT_SCALE -1

#define DEFAULT_DOUBLE_PRECISION -1

#define DEFAULT_DOUBLE_SCALE -1

namespace oceanbase {
namespace obmysql {

// compat with mariadb_com
#define OB_MYSQL_NOT_NULL_FLAG (1 << 0)
#define OB_MYSQL_PRI_KEY_FLAG (1 << 1)
#define OB_MYSQL_UNIQUE_KEY_FLAG (1 << 2)
#define OB_MYSQL_MULTIPLE_KEY_FLAG (1 << 3)
#define OB_MYSQL_BLOB_FLAG (1 << 4)
#define OB_MYSQL_UNSIGNED_FLAG (1 << 5)
#define OB_MYSQL_ZEROFILL_FLAG (1 << 6)
#define OB_MYSQL_BINARY_FLAG (1 << 7)

// following are only sent to new clients
#define OB_MYSQL_ENUM_FLAG (1 << 8)
#define OB_MYSQL_AUTO_INCREMENT_FLAG (1 << 9)
#define OB_MYSQL_TIMESTAMP_FLAG (1 << 10)
#define OB_MYSQL_SET_FLAG (1 << 11)
#define OB_MYSQL_NO_DEFAULT_VALUE_FLAG (1 << 12)
#define OB_MYSQL_ON_UPDATE_NOW_FLAG (1 << 13)
#define OB_MYSQL_PART_KEY_FLAG (1 << 14)
#define OB_MYSQL_NUM_FLAG (1 << 15)    // 32768
#define OB_MYSQL_GROUP_FLAG (1 << 15)  // 32768
#define OB_MYSQL_UNIQUE_FLAG (1 << 16)
#define OB_MYSQL_BINCMP_FLAG (1 << 17)
#define OB_MYSQL_GET_FIXED_FIELDS_FLAG (1 << 18)
#define OB_MYSQL_FIELD_IN_PART_FUNC_FLAG (1 << 19)

enum EMySQLFieldType {
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
  MYSQL_TYPE_COMPLEX = 160,  // 0xa0
  MYSQL_TYPE_ARRAY = 161,    // 0xa1
  MYSQL_TYPE_STRUCT = 162,   // 0xa2
  MYSQL_TYPE_CURSOR = 163,   // 0xa3
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
  MYSQL_TYPE_NOT_DEFINED = 65535
};

}  // end of namespace obmysql
}  // end of namespace oceanbase

#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
namespace common {
const char* get_emysql_field_type_str(const obmysql::EMySQLFieldType& type);

inline const char* get_emysql_field_type_str(const obmysql::EMySQLFieldType& type)
{
  const char* str = "UNKNOWN_TYPE";
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
    default:
      break;
  }
  return str;
}

inline int databuff_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const obmysql::EMySQLFieldType& type)
{
  return common::databuff_printf(buf, buf_len, pos, "%s", get_emysql_field_type_str(type));
}

inline int databuff_print_key_obj(char* buf, const int64_t buf_len, int64_t& pos, const char* key,
    const bool with_comma, const obmysql::EMySQLFieldType& type)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:\"%s\""), key, get_emysql_field_type_str(type));
}

}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_RPC_OBMYSQL_OB_MYSQL_GLOBAL_
