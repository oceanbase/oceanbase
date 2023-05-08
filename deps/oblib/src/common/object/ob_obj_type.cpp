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

#include <float.h>
#define USING_LOG_PREFIX COMMON
#include "lib/ob_define.h"
#include "common/object/ob_obj_type.h"
#include "common/object/ob_object.h"
#include "common/ob_accuracy.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_string.h"
namespace oceanbase
{
namespace common
{

const char *ob_obj_type_str(ObObjType type)
{
  return ob_sql_type_str(type);
}

const char *ob_obj_tc_str(ObObjTypeClass tc)
{
  return ob_sql_tc_str(tc);
}

const char *ob_sql_type_str(ObObjType type)
{
  static const char *sql_type_name[OB_MAX_MODE_CNT][ObMaxType+1] = {
    {
      "NULL",

      "TINYINT",
      "SMALLINT",
      "MEDIUMINT",
      "INT",
      "BIGINT",

      "TINYINT UNSIGNED",
      "SMALLINT UNSIGNED",
      "MEDIUMINT UNSIGNED",
      "INT UNSIGNED",
      "BIGINT UNSIGNED",

      "FLOAT",
      "DOUBLE",

      "FLOAT UNSIGNED",
      "DOUBLE UNSIGNED",

      "DECIMAL",
      "DECIMAL UNSIGNED",

      "DATETIME",
      "TIMESTAMP",
      "DATE",
      "TIME",
      "YEAR",

      "VARCHAR",
      "CHAR",
      "HEX_STRING",

      "EXT",
      "UNKNOWN",

      "TINYTEXT",
      "TEXT",
      "MEDIUMTEXT",
      "LONGTEXT",
      "BIT",
      "ENUM",
      "SET",
      "ENUM_INNER",
      "SET_INNER",
      "TIMESTAMP_WITH_TIME_ZONE",
      "TIMESTAMP_WITH_LOCAL_TIME_ZONE",
      "TIMESTAMP",
      "RAW",
      "INTERVAL_YEAR_TO_MONTH",
      "INTERVAL_DAY_TO_SECOND",
      "NUMBER_FLOAT",
      "NVARCHAR2",
      "NCHAR",
      "ROWID",
      "LOB",
      "JSON",
      "GEOMETRY",
      "UDT",
      ""
    },
    {
      "NULL",
      "TINYINT",
      "SMALLINT",
      "MEDIUMINT",
      "INT",
      "BIGINT",
      "TINYINT UNSIGNED",
      "SMALLINT UNSIGNED",
      "MEDIUMINT UNSIGNED",
      "INT UNSIGNED",
      "BIGINT UNSIGNED",

      "BINARY_FLOAT",
      "BINARY_DOUBLE",

      "FLOAT UNSIGNED",
      "DOUBLE UNSIGNED",
      "NUMBER",
      "DECIMAL UNSIGNED",
      "DATE",
      "TIMESTAMP",
      "DATE",
      "TIME",
      "YEAR",
      "VARCHAR2",
      "CHAR",
      "HEX_STRING",
      "EXT",
      "UNKNOWN",
      "TINYTEXT",
      "TEXT",
      "MEDIUMTEXT",
      "LONGTEXT",
      "BIT",
      "ENUM",
      "SET",
      "ENUM_INNER",
      "SET_INNER",
      "TIMESTAMP_WITH_TIME_ZONE",
      "TIMESTAMP_WITH_LOCAL_TIME_ZONE",
      "TIMESTAMP",
      "RAW",
      "INTERVAL_YEAR_TO_MONTH",
      "INTERVAL_DAY_TO_SECOND",
      "NUMBER_FLOAT",
      "NVARCHAR2",
      "NCHAR",
      "ROWID",
      "LOB",
      "JSON",
      "GEOMETRY",
      "UDT",
      ""
    }
  };
  return sql_type_name[lib::is_oracle_mode()][OB_LIKELY(type < ObMaxType) ? type : ObMaxType];
}

typedef int (*ObSqlTypeStrFunc)(char *buff, int64_t buff_length, int64_t &pos, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type);

typedef int (*obSqlTypeStrWithoutAccuracyFunc)(char *buff, int64_t buff_length, ObCollationType coll_type);


////////////////////////////////////print with accuracy//////////////////////////////////////////////
//For date, null/unkown/max/extend
#define DEF_TYPE_STR_FUNCS(TYPE, STYPE1, STYPE2)                                                        \
  int ob_##TYPE##_str(char *buff, int64_t buff_length, int64_t &pos, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type) \
  {                                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    UNUSED(length);                                                                                     \
    UNUSED(precision);                                                                                  \
    UNUSED(scale);                                                                                      \
    UNUSED(coll_type);                                                  \
    ret = databuff_printf(buff, buff_length, pos, STYPE1 STYPE2);                                       \
    return ret;                                                                                         \
  }

//For tinyint/smallint/mediumint/int/bigint (unsigned), year
#define DEF_TYPE_STR_FUNCS_PRECISION(TYPE, STYPE1, STYPE2)                                              \
  int ob_##TYPE##_str(char *buff, int64_t buff_length, int64_t &pos, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type)\
  {                                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    UNUSED(length);                                                                                     \
    UNUSED(scale);                                                                                      \
    UNUSED(coll_type);                                                  \
    if (precision < 0) {                                                                                \
      ret = databuff_printf(buff, buff_length, pos, STYPE1 STYPE2);                                     \
    } else {                                                                                            \
      ret = databuff_printf(buff, buff_length, pos, STYPE1 "(%ld)" STYPE2, precision);                  \
    }                                                                                                   \
    return ret;                                                                                         \
  }

//For datetime/timestamp/time
#define DEF_TYPE_STR_FUNCS_SCALE_DEFAULT_ZERO(TYPE, STYPE1, STYPE2, STYPE3)                             \
  int ob_##TYPE##_str(char *buff, int64_t buff_length, int64_t &pos, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type) \
  {                                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    UNUSED(length);                                                                                     \
    UNUSED(precision);                                                                                  \
    UNUSED(coll_type);                                                                                  \
    if (scale <= 0) {                                                                                   \
      if (lib::is_oracle_mode()) {                                                                      \
        ret = databuff_printf(buff, buff_length, pos, STYPE3 STYPE2);                                   \
      } else {                                                                                          \
        ret = databuff_printf(buff, buff_length, pos, STYPE1 STYPE2);                                   \
      }                                                                                                 \
    } else {                                                                                            \
      ret = databuff_printf(buff, buff_length, pos, STYPE1 "(%ld)" STYPE2, scale);                      \
    }                                                                                                   \
    return ret;                                                                                         \
  }

//For otimestamp, need print (0) if scale is zero
#define DEF_TYPE_STR_FUNCS_SCALE(TYPE, STYPE1, STYPE2)                                                  \
  int ob_##TYPE##_str(char *buff, int64_t buff_length, int64_t &pos, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type) \
  {                                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    UNUSED(length);                                                                                     \
    UNUSED(precision);                                                                                  \
    UNUSED(coll_type);                                                                                  \
    if (scale < 0) {                                                                                    \
      ret = databuff_printf(buff, buff_length, pos, STYPE1 STYPE2);                                     \
    } else {                                                                                            \
      ret = databuff_printf(buff, buff_length, pos, STYPE1 "(%ld)" STYPE2, scale);                      \
    }                                                                                                   \
    return ret;                                                                                         \
  }

//For number/float/double (unsigned)
#define DEF_TYPE_STR_FUNCS_PRECISION_SCALE(TYPE, STYPE1, STYPE2, STYPE3)                                 \
  int ob_##TYPE##_str(char *buff, int64_t buff_length, int64_t &pos, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type) \
  {                                                                                                       \
    int ret = OB_SUCCESS;                                                                                 \
    UNUSED(length);                                                                                       \
    UNUSED(coll_type);                                                                                    \
    if (lib::is_oracle_mode()) {                                                                          \
      if (precision < OB_MIN_NUMBER_PRECISION && scale < OB_MIN_NUMBER_SCALE) {                           \
        ret = databuff_printf(buff, buff_length, pos, "%s", STYPE3);                                      \
      } else if (precision < OB_MIN_NUMBER_PRECISION) {                                                   \
        ret = databuff_printf(buff, buff_length, pos, "%s(*,%ld)", STYPE3, scale);                        \
      } else if (scale == 0) {                                                                            \
        ret = databuff_printf(buff, buff_length, pos, "%s(%ld)", STYPE3, precision);                      \
      } else {                                                                                            \
        ret = databuff_printf(buff, buff_length, pos, "%s(%ld,%ld)", STYPE3, precision, scale);           \
      }                                                                                                   \
    } else {                                                                                              \
      if (precision < 0 || scale < 0) {                                                                 \
        ret = databuff_printf(buff, buff_length, pos, "%s%s", STYPE1, STYPE2);                            \
      } else {                                                                                            \
        ret = databuff_printf(buff, buff_length, pos, "%s(%ld,%ld)%s", STYPE1, precision, scale, STYPE2); \
      }                                                                                                   \
    }                                                                                                     \
    return ret;                                                                                           \
  }

//For char/varchar
//when null, dsp varchar2 without length 
#define DEF_TYPE_STR_FUNCS_LENGTH(TYPE, STYPE1, STYPE2, STYPE3)                 \
  int ob_##TYPE##_str(char *buff, int64_t buff_length, int64_t &pos, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type) \
  {                                                                     \
    int ret = OB_SUCCESS;                                               \
    UNUSED(precision);                                                  \
    UNUSED(scale) ;                                                     \
    if (CS_TYPE_BINARY == coll_type) {                                  \
      ret = databuff_printf(buff, buff_length, pos, STYPE2 "(%ld)", length); \
    } else {                                                            \
      int16_t length_semantics = static_cast<int16_t>(precision);\
      if (lib::is_oracle_mode()) { \
        if (length <= 0) { \
          ret = databuff_printf(buff, buff_length, pos, "%s", STYPE3); \
        } else { \
          if (LS_DEFAULT == length_semantics) { \
            ret = databuff_printf(buff, buff_length, pos, "%s(%ld)", STYPE3, length); \
          } else { \
            ret = databuff_printf(buff, buff_length, pos, "%s(%ld %s)", STYPE3, length, get_length_semantics_str(length_semantics)); \
          } \
        } \
      } else {\
        ret = databuff_printf(buff, buff_length, pos, "%s(%ld)", STYPE1, length); \
      }\
    }                                                                   \
    return ret;                                                         \
  }

//For text/blob
#define DEF_TYPE_TEXT_FUNCS_LENGTH(TYPE, STYPE1, STYPE2)                 \
  int ob_##TYPE##_str(char *buff, int64_t buff_length, int64_t &pos, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type) \
  {                                                                     \
    int ret = OB_SUCCESS;                                               \
    UNUSED(length);                                                  \
    UNUSED(precision);                                                  \
    UNUSED(scale) ;                                                     \
    if (CS_TYPE_BINARY == coll_type) {                                  \
      ret = databuff_printf(buff, buff_length, pos, STYPE2);            \
    } else {                                                            \
      ret = databuff_printf(buff, buff_length, pos, STYPE1);            \
    }                                                                   \
    return ret;                                                         \
  }

////////////////////////////////////print without accuracy//////////////////////////////////////////////

//For non-string
#define DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(TYPE, STYPE1, STYPE2)                                                        \
  int ob_##TYPE##_str_without_accuracy(char *buff, int64_t buff_length, ObCollationType coll_type)\
  {                                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    UNUSED(coll_type);                                                  \
    int64_t pos = 0;                                                                                    \
    ret = databuff_printf(buff, buff_length, pos, STYPE1);                                       \
    return ret;                                                                                         \
  }

//For string
#define DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_STRING(TYPE, STYPE1, STYPE2)                                                        \
  int ob_##TYPE##_str_without_accuracy(char *buff, int64_t buff_length, ObCollationType coll_type)\
  {                                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    int64_t pos = 0;                                                                                    \
    if (CS_TYPE_BINARY == coll_type) {                                  \
      ret = databuff_printf(buff, buff_length, pos, STYPE2); \
    } else {                                                            \
      ret = databuff_printf(buff, buff_length, pos, STYPE1); \
    }\
    return ret;                                                                                         \
  }

//For odate
#define DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_ODATE(TYPE, STYPE1, STYPE2)                                                        \
  int ob_##TYPE##_str_without_accuracy(char *buff, int64_t buff_length, ObCollationType coll_type)\
  {                                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    UNUSED(coll_type);                                                                                  \
    int64_t pos = 0;                                                                                    \
    if (lib::is_oracle_mode()) {                                                                        \
      ret = databuff_printf(buff, buff_length, pos, STYPE2);                                            \
    } else {                                                                                            \
      ret = databuff_printf(buff, buff_length, pos, STYPE1);                                            \
    }                                                                                                   \
    return ret;                                                                                         \
  }


DEF_TYPE_STR_FUNCS(null, "null", "");
DEF_TYPE_STR_FUNCS_PRECISION(tinyint, "tinyint", "");
DEF_TYPE_STR_FUNCS_PRECISION(smallint, "smallint", "");
DEF_TYPE_STR_FUNCS_PRECISION(mediumint, "mediumint", "");
DEF_TYPE_STR_FUNCS_PRECISION(int, "int", "");
DEF_TYPE_STR_FUNCS_PRECISION(bigint, "bigint", "");
DEF_TYPE_STR_FUNCS_PRECISION(utinyint, "tinyint", " unsigned");
DEF_TYPE_STR_FUNCS_PRECISION(usmallint, "smallint", " unsigned");
DEF_TYPE_STR_FUNCS_PRECISION(umediumint, "mediumint", " unsigned");
DEF_TYPE_STR_FUNCS_PRECISION(uint, "int", " unsigned");
DEF_TYPE_STR_FUNCS_PRECISION(ubigint, "bigint", " unsigned");
DEF_TYPE_STR_FUNCS_PRECISION_SCALE(float, "float", "", "binary_float");
DEF_TYPE_STR_FUNCS_PRECISION_SCALE(double, "double", "", "binary_double");
DEF_TYPE_STR_FUNCS_PRECISION_SCALE(ufloat, "float", " unsigned", "");
DEF_TYPE_STR_FUNCS_PRECISION_SCALE(udouble, "double", " unsigned", "");
DEF_TYPE_STR_FUNCS_PRECISION_SCALE(number, "decimal", "", "number");
DEF_TYPE_STR_FUNCS_PRECISION_SCALE(unumber, "decimal", " unsigned", "");
DEF_TYPE_STR_FUNCS_SCALE_DEFAULT_ZERO(datetime, "datetime", "", "date");
DEF_TYPE_STR_FUNCS_SCALE_DEFAULT_ZERO(timestamp, "timestamp", "", "timestamp");
DEF_TYPE_STR_FUNCS(date, "date", "");
DEF_TYPE_STR_FUNCS_SCALE_DEFAULT_ZERO(time, "time", "", "time");
DEF_TYPE_STR_FUNCS_PRECISION(year, "year", "");
DEF_TYPE_STR_FUNCS_SCALE(timestamp_tz, "timestamp", " with time zone");
DEF_TYPE_STR_FUNCS_SCALE(timestamp_ltz, "timestamp", " with local time zone");
DEF_TYPE_STR_FUNCS_SCALE(timestamp_nano, "timestamp", "");
DEF_TYPE_STR_FUNCS_LENGTH(varchar, "varchar", "varbinary", "varchar2");
DEF_TYPE_STR_FUNCS_LENGTH(char, "char", "binary", "char");
DEF_TYPE_STR_FUNCS_LENGTH(hex_string, "hex_string", "hex_string", "hex_string");
DEF_TYPE_STR_FUNCS_LENGTH(raw, "raw", "raw", "raw");
DEF_TYPE_STR_FUNCS(extend, "ext", "");
DEF_TYPE_STR_FUNCS(unknown, "unknown", "");
DEF_TYPE_TEXT_FUNCS_LENGTH(tinytext, "tinytext", "tinyblob");
DEF_TYPE_TEXT_FUNCS_LENGTH(text, "text", "blob");
DEF_TYPE_TEXT_FUNCS_LENGTH(mediumtext, "mediumtext", "mediumblob");
DEF_TYPE_TEXT_FUNCS_LENGTH(longtext, (lib::is_oracle_mode() ? "clob" : "longtext"), (lib::is_oracle_mode() ? "blob" : "longblob"));
DEF_TYPE_STR_FUNCS_PRECISION(bit, "bit", "");
DEF_TYPE_STR_FUNCS(enum, "enum", "");
DEF_TYPE_STR_FUNCS(set, "set", "");
DEF_TYPE_STR_FUNCS_PRECISION(number_float, "float", "");
DEF_TYPE_TEXT_FUNCS_LENGTH(lob, (lib::is_oracle_mode() ? "clob" : "longtext"), (lib::is_oracle_mode() ? "blob" : "longblob"));
DEF_TYPE_TEXT_FUNCS_LENGTH(json, "json", "json");
DEF_TYPE_TEXT_FUNCS_LENGTH(geometry, "geometry", "geometry");

///////////////////////////////////////////////////////////
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(null, "null", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(tinyint, "tinyint", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(smallint, "smallint", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(mediumint, "mediumint", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(int, "int", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(bigint, "bigint", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(utinyint, "tinyint", " unsigned");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(usmallint, "smallint", " unsigned");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(umediumint, "mediumint", " unsigned");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(uint, "int", " unsigned");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(ubigint, "bigint", " unsigned");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(float, (lib::is_oracle_mode() ? "binary_float" : "float"), "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(double, (lib::is_oracle_mode() ? "binary_double" : "double"), "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(ufloat, "float", " unsigned");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(udouble, "double", " unsigned");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(number, (lib::is_oracle_mode() ? "number" : "decimal"), "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(unumber, "decimal", " unsigned");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_ODATE(datetime, "datetime", "date");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(timestamp, "timestamp", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(timestamp_tz, "timestamp", " with time zone");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(timestamp_ltz, "timestamp", " with local time zone");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(timestamp_nano, "timestamp", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(date, "date", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(time, "time", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(year, "year", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_STRING(varchar, (lib::is_oracle_mode() ? "varchar2" : "varchar"), "varbinary");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_STRING(char, "char", "binary");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_STRING(hex_string, "hex_string", "hex_string");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_STRING(raw, "raw", "raw");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(extend, "ext", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(unknown, "unknown", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_STRING(tinytext, "tinytext", "tinyblob");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_STRING(text, "text", "blob");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_STRING(mediumtext, "mediumtext", "mediumblob");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_STRING(longtext, (lib::is_oracle_mode() ? "clob" : "longtext"), (lib::is_oracle_mode() ? "blob" : "longblob"));
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(bit, "bit", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(enum, "enum", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(set, "set", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(interval_ym, "interval year to month", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(interval_ds, "interval day to second", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(number_float, "float", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_STRING(nvarchar2, "nvarchar2", "nvarchar2");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_STRING(nchar, "nchar", "nchar");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_NON_STRING(urowid, "urowid", "");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_STRING(lob, (lib::is_oracle_mode() ? "clob" : "longtext"), (lib::is_oracle_mode() ? "blob" : "longblob"));
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_STRING(json, "json", "json");
DEF_TYPE_STR_FUNCS_WITHOUT_ACCURACY_FOR_STRING(geometry, "geometry", "geometry");


int ob_empty_str(char *buff, int64_t buff_length, ObCollationType coll_type)
{
  int ret = OB_SUCCESS;
  UNUSED(coll_type);
  if (OB_ISNULL(buff) || buff_length <= 0) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    buff[0] = '\0';
  }
  return ret;
}

int ob_empty_str(char *buff, int64_t buff_length, int64_t &pos, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type)
{
  UNUSED(length);
  UNUSED(scale) ;
  UNUSED(precision);
  UNUSED(pos);
  return ob_empty_str(buff, buff_length, coll_type);
}

int ob_geometry_sub_type_str(char *buff, int64_t buff_length, int64_t &pos, const common::ObGeoType geo_type)
{
  int ret = OB_SUCCESS;
  switch (geo_type) {
    case common::ObGeoType::POINT: {
      ret = databuff_printf(buff, buff_length, pos, "point");
      break;
    }

    case common::ObGeoType::LINESTRING: {
      ret = databuff_printf(buff, buff_length, pos, "linestring");
      break;
    }

    case common::ObGeoType::POLYGON: {
      ret = databuff_printf(buff, buff_length, pos, "polygon");
      break;
    }

    case common::ObGeoType::MULTIPOINT: {
      ret = databuff_printf(buff, buff_length, pos, "multipoint");
      break;
    }

    case common::ObGeoType::MULTILINESTRING: {
      ret = databuff_printf(buff, buff_length, pos, "multilinestring");
      break;
    }

    case common::ObGeoType::MULTIPOLYGON: {
      ret = databuff_printf(buff, buff_length, pos, "multipolygon");
      break;
    }

    case common::ObGeoType::GEOMETRYCOLLECTION: {
      ret = databuff_printf(buff, buff_length, pos, "geomcollection");
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("undefined geometry type", K(ret), K(geo_type));
      break;
    }
  }
  return ret;
}

int ob_udt_sub_type_str(char *buff, int64_t buff_length, int64_t &pos, const uint64_t sub_type, bool is_sql_type = false)
{
  int ret = OB_SUCCESS;
  if (is_sql_type && sub_type == ObXMLSqlType) {
    ret = databuff_printf(buff, buff_length, pos, "XMLTYPE");
  } else if (sub_type == T_OBJ_XML) {
    ret = databuff_printf(buff, buff_length, pos, "XMLTYPE");
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("undefined geometry type", K(ret), K(sub_type), K(is_sql_type));
  }
  return ret;
}

int ob_enum_or_set_str(const ObObjMeta &obj_meta, const common::ObIArray<ObString> &type_info, char *buff, int64_t buff_length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_alloc;
  if (OB_UNLIKELY(!obj_meta.is_enum_or_set())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column type", K(ret), K(obj_meta));
  } else if (ObEnumType == obj_meta.get_type()) {
    if (OB_FAIL(databuff_printf(buff, buff_length, pos, "enum("))) {
      LOG_WARN("fail to print buffer", K(ret), K(buff_length), K(pos));
    }
  } else if (OB_FAIL(databuff_printf(buff, buff_length, pos, "set("))) {
    LOG_WARN("fail to print buffer", K(ret), K(buff_length), K(pos));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < type_info.count(); ++i) {
    ObString cur_str = type_info.at(i);
    if (OB_FAIL(ObCharset::charset_convert(tmp_alloc,
                                           cur_str,
                                           obj_meta.get_collation_type(),
                                           ObCharset::get_system_collation(),
                                           cur_str))) {
      LOG_WARN("convert string to system collation failed",
               K(ret), K(obj_meta), K(cur_str));
    } else if (OB_FAIL(databuff_printf(buff, buff_length, pos, "'%.*s'", cur_str.length(), cur_str.ptr()))) {
      LOG_WARN("fail to print buffer", K(ret), K(buff_length), K(pos), K(i));
    } else if (i == type_info.count() - 1) {
      if (OB_FAIL(databuff_printf(buff, buff_length, pos, ")"))) {
        LOG_WARN("fail to print buffer", K(ret), K(buff_length), K(pos), K(i));
      }
    } else if (OB_FAIL(databuff_printf(buff, buff_length, pos, ","))) {
      LOG_WARN("fail to print buffer", K(ret), K(buff_length), K(pos), K(i));
    }
  }
  return ret;
}

//For interval year to month
int ob_interval_ym_str(char *buff, int64_t buff_length, int64_t &pos, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type)
{
  int ret = OB_SUCCESS;
  UNUSED(length);
  UNUSED(precision);
  UNUSED(coll_type);
  if (OB_UNLIKELY(scale > 9 || scale < 0)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t year_scale = ObIntervalScaleUtil::ob_scale_to_interval_ym_year_scale(static_cast<int8_t>(scale));
    ret = databuff_printf(buff, buff_length, pos, "interval year (%ld) to month", year_scale);
  }
  return ret;
}

//For interval day to second
int ob_interval_ds_str(char *buff, int64_t buff_length, int64_t &pos, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type)
{
  int ret = OB_SUCCESS;
  UNUSED(length);
  UNUSED(precision);
  UNUSED(coll_type);
  if (OB_UNLIKELY(scale > 99 || scale < 0)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t day_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_day_scale(static_cast<int8_t>(scale));
    int64_t fs_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_second_scale(static_cast<int8_t>(scale));
    ret = databuff_printf(buff, buff_length, pos, "interval day (%ld) to second (%ld)", day_scale, fs_scale);
  }
  return ret;
}

int ob_nvarchar2_str(char *buff, int64_t buff_length, int64_t &pos, int64_t length,
                     int64_t precision, int64_t scale, ObCollationType coll_type)
{
  UNUSED(precision);
  UNUSED(coll_type);
  UNUSED(scale);
  int ret = OB_SUCCESS;
  //when null, dsp nvarchar2 without length 
  if (length <= 0) {
    ret = databuff_printf(buff, buff_length, pos, "nvarchar2");
  } else {
    ret = databuff_printf(buff, buff_length, pos, "nvarchar2(%ld)", length);
  }
  return ret;
}

int ob_nchar_str(char *buff, int64_t buff_length, int64_t &pos, int64_t length,
                 int64_t precision, int64_t scale, ObCollationType coll_type)
{
  UNUSED(precision);
  UNUSED(coll_type);
  UNUSED(scale);
  int ret = OB_SUCCESS;
  //when null, dsp nchar without length 
  if (length <= 0) {
    ret = databuff_printf(buff, buff_length, pos, "nchar");
  } else {
    ret = databuff_printf(buff, buff_length, pos, "nchar(%ld)", length);
  }
  return ret;
}

int ob_urowid_str(char *buff, int64_t buff_length, int64_t &pos, int64_t length, int64_t precision,
                 int64_t scale, ObCollationType coll_type)
{
  UNUSED(precision);
  UNUSED(coll_type);
  UNUSED(scale);
  return databuff_printf(buff, buff_length, pos, "urowid(%ld)", length);
}

int ob_sql_type_str(char *buff,
    int64_t buff_length,
    int64_t &pos,
    ObObjType type,
    int64_t length,
    int64_t precision,
    int64_t scale,
    ObCollationType coll_type,
    const uint64_t sub_type/* common::ObGeoType::GEOTYPEMAX */)
{
  int ret = OB_SUCCESS;
  static ObSqlTypeStrFunc sql_type_name[ObMaxType+1] =
  {
    ob_null_str,  // NULL

    ob_tinyint_str,  // TINYINT
    ob_smallint_str, // SAMLLINT
    ob_mediumint_str,   // MEDIUM
    ob_int_str,      // INT
    ob_bigint_str,   // BIGINT

    ob_utinyint_str,  // TYIYINT UNSIGNED
    ob_usmallint_str, // SMALLINT UNSIGNED
    ob_umediumint_str,   // MEDIUM UNSIGNED
    ob_uint_str,      // INT UNSIGNED
    ob_ubigint_str,   // BIGINT UNSIGNED

    ob_float_str,  // FLOAT
    ob_double_str, // DOUBLE

    ob_ufloat_str,  // FLOAT UNSIGNED
    ob_udouble_str, // DOUBLE UNSIGNED

    ob_number_str,  // DECIMAL
    ob_unumber_str,  // DECIMAL UNSIGNED

    ob_datetime_str,  // DATETIME
    ob_timestamp_str, // TIMESTAMP
    ob_date_str,      // DATE
    ob_time_str,      // TIME
    ob_year_str,      // YEAR

    ob_varchar_str,  // VARCHAR
    ob_char_str,     // CHAR
    ob_hex_string_str,  // HEX_STRING

    ob_extend_str,  // EXT
    ob_unknown_str,  // UNKNOWN
    ob_tinytext_str, //TINYTEXT
    ob_text_str, //TEXT
    ob_mediumtext_str, //MEDIUMTEXT
    ob_longtext_str, //LONGTEXT
    ob_bit_str, //BIT
    ob_enum_str,//enum TODO(yts):wait for yts
    ob_set_str,//set
    ob_empty_str,//enum_inner
    ob_empty_str,//enum_inner
    ob_timestamp_tz_str,//timestamp with time zone
    ob_timestamp_ltz_str,//timestamp with local time zone
    ob_timestamp_nano_str,//timestamp nano
    ob_raw_str,// raw
    ob_interval_ym_str,//interval year to month
    ob_interval_ds_str,//interval day to second
    ob_number_float_str,//number float
    ob_nvarchar2_str,//NVARCHAR2
    ob_nchar_str,//NCHAR
    ob_urowid_str,//urowid
    ob_lob_str,//lob
    ob_json_str,//json
    ob_geometry_str,//geometry
    ob_empty_str             // MAX
  };
  static_assert(sizeof(sql_type_name) / sizeof(ObSqlTypeStrFunc) == ObMaxType + 1, "Not enough initializer");
  if (ob_is_geometry_tc(type) && static_cast<common::ObGeoType>(sub_type) != common::ObGeoType::GEOMETRY) {
    if (OB_FAIL(ob_geometry_sub_type_str(buff, buff_length, pos, static_cast<common::ObGeoType>(sub_type)))) {
      LOG_WARN("fail to get geometry sub type str", K(ret), K(sub_type), K(buff), K(buff_length), K(pos));
    }
  } else if (lib::is_oracle_mode() && (ob_is_user_defined_sql_type(type) || sub_type == T_OBJ_XML)) {
     if (OB_FAIL(ob_udt_sub_type_str(buff, buff_length, pos, sub_type, true))) {
       LOG_WARN("fail to get udt sub type str", K(ret), K(sub_type), K(buff), K(buff_length), K(pos));
     }
  } else {
    ret = sql_type_name[OB_LIKELY(type < ObMaxType) ? type : ObMaxType](buff, buff_length, pos, length, precision, scale, coll_type);
  }
  return ret;
}

int ob_sql_type_str(char *buff,
    int64_t buff_length,
    ObObjType type,
    ObCollationType coll_type,
    const common::ObGeoType geo_type/* common::ObGeoType::GEOTYPEMAX */)
{
  int ret = OB_SUCCESS;
  static obSqlTypeStrWithoutAccuracyFunc sql_type_name[ObMaxType+1] =
  {
    ob_null_str_without_accuracy,  // NULL

    ob_tinyint_str_without_accuracy,  // TINYINT
    ob_smallint_str_without_accuracy, // SAMLLINT
    ob_mediumint_str_without_accuracy,   // MEDIUM
    ob_int_str_without_accuracy,      // INT
    ob_bigint_str_without_accuracy,   // BIGINT

    ob_utinyint_str_without_accuracy,  // TYIYINT UNSIGNED
    ob_usmallint_str_without_accuracy, // SMALLINT UNSIGNED
    ob_umediumint_str_without_accuracy,   // MEDIUM UNSIGNED
    ob_uint_str_without_accuracy,      // INT UNSIGNED
    ob_ubigint_str_without_accuracy,   // BIGINT UNSIGNED

    ob_float_str_without_accuracy,  // FLOAT
    ob_double_str_without_accuracy, // DOUBLE

    ob_ufloat_str_without_accuracy,  // FLOAT UNSIGNED
    ob_udouble_str_without_accuracy, // DOUBLE UNSIGNED

    ob_number_str_without_accuracy,  // DECIMAL
    ob_unumber_str_without_accuracy,  // DECIMAL UNSIGNED

    ob_datetime_str_without_accuracy,  // DATETIME
    ob_timestamp_str_without_accuracy, // TIMESTAMP
    ob_date_str_without_accuracy,      // DATE
    ob_time_str_without_accuracy,      // TIME
    ob_year_str_without_accuracy,      // YEAR

    ob_varchar_str_without_accuracy,  // VARCHAR
    ob_char_str_without_accuracy,     // CHAR
    ob_hex_string_str_without_accuracy,  // HEX_STRING

    ob_extend_str_without_accuracy,  // EXT
    ob_unknown_str_without_accuracy,  // UNKNOWN
    ob_tinytext_str_without_accuracy, //TINYTEXT
    ob_text_str_without_accuracy, //TEXT
    ob_mediumtext_str_without_accuracy, //MEDIUMTEXT
    ob_longtext_str_without_accuracy, //LONGTEXT
    ob_bit_str_without_accuracy, //BIT
    ob_enum_str_without_accuracy,//enum
    ob_set_str_without_accuracy,//set
    ob_empty_str,//enum_inner
    ob_empty_str,//enum_inner
    ob_timestamp_tz_str_without_accuracy, // TIMESTAMP WITH TIME ZONE
    ob_timestamp_ltz_str_without_accuracy, // TIMESTAMP WITH LOCAL TIME ZONE
    ob_timestamp_nano_str_without_accuracy, // TIMESTAMP
    ob_raw_str_without_accuracy,  // RAW_STRING
    ob_interval_ym_str_without_accuracy, // INTERVAL YEAR TO MONTH
    ob_interval_ds_str_without_accuracy, // INTERVAL DAY TO SECOND
    ob_number_float_str_without_accuracy,//number float
    ob_nvarchar2_str_without_accuracy, // NVARCHAR2
    ob_nchar_str_without_accuracy, // NCHAR
    ob_urowid_str_without_accuracy,  // urowid
    ob_lob_str_without_accuracy,//lob
    ob_json_str_without_accuracy,//json
    ob_geometry_str_without_accuracy,//geometry
    ob_empty_str   // MAX
  };
  static_assert(sizeof(sql_type_name) / sizeof(obSqlTypeStrWithoutAccuracyFunc) == ObMaxType + 1, "Not enough initializer");
  if (OB_UNLIKELY(type > ObMaxType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(type), K(ObMaxType), K(ret));
  } else if (ob_is_geometry_tc(type) && geo_type != common::ObGeoType::GEOMETRY) {
    int64_t pos = 0;
    if (OB_FAIL(ob_geometry_sub_type_str(buff, buff_length, pos, geo_type))) {
      LOG_WARN("fail to get geometry sub type str", K(ret), K(geo_type), K(buff), K(buff_length), K(pos));
    }
  } else if (OB_ISNULL(sql_type_name[type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("function pointer is NULL", K(type), K(ret));
  } else if (OB_FAIL(sql_type_name[type](buff, buff_length, coll_type))) {
    LOG_WARN("fail to print type", K(type), K(buff_length), K(coll_type), K(ret));
  }

  return ret;
}

int ob_sql_type_str(const ObObjMeta &obj_meta,
    const ObAccuracy &accuracy,
    const common::ObIArray<ObString> &type_info,
    const int16_t default_length_semantics,
    char *buff,
    int64_t buff_length,
    int64_t &pos,
    const uint64_t sub_type/* common::ObGeoType::GEOTYPEMAX */)
{
  int ret = OB_SUCCESS;
  int16_t precision_or_length_semantics = accuracy.get_precision();
  LOG_DEBUG("ob_sql_type_str",K(ret), K(accuracy), K(precision_or_length_semantics), K(precision_or_length_semantics), KCSTRING(common::lbt()));
  if (lib::is_oracle_mode() && obj_meta.is_varchar_or_char() && precision_or_length_semantics == default_length_semantics) {
    precision_or_length_semantics = LS_DEFAULT;
  }
  if (obj_meta.is_enum_or_set()) {
    if (OB_FAIL(ob_enum_or_set_str(obj_meta, type_info, buff, buff_length, pos))) {
      LOG_WARN("fail to get enum_or_set str", K(ret), K(obj_meta), K(accuracy), K(buff_length), K(pos));
    }
  } else if (obj_meta.is_geometry() && static_cast<common::ObGeoType>(sub_type) != common::ObGeoType::GEOMETRY) {
    if (OB_FAIL(ob_geometry_sub_type_str(buff, buff_length, pos, static_cast<common::ObGeoType>(sub_type)))) {
      LOG_WARN("fail to get geometry sub type str", K(ret), K(sub_type), K(buff), K(buff_length), K(pos));
    }
  } else if (lib::is_oracle_mode() && obj_meta.is_ext()) {
     if (OB_FAIL(ob_udt_sub_type_str(buff, buff_length, pos, sub_type))) {
       LOG_WARN("fail to get udt sub type str", K(ret), K(accuracy.get_accuracy()), K(buff), K(buff_length), K(pos));
     }
  } else {
    ObObjType datatype = obj_meta.get_type();
    ObCollationType coll_type = obj_meta.get_collation_type();
    ObLength length = accuracy.get_length();
    /* oracle has no null datatype, map to varchar2 */
    if (lib::is_oracle_mode() && ObNullType == datatype) { 
      datatype = ObVarcharType; 
      coll_type = CS_TYPE_UTF8MB4_BIN;
      length = 0;
    }
    if (OB_FAIL(ob_sql_type_str(buff, buff_length, pos,
                                datatype, length,
                                precision_or_length_semantics,
                                accuracy.get_scale(), coll_type, sub_type))) {
      LOG_WARN("fail to print sql type", K(ret), K(obj_meta), K(accuracy));
    }
  }
  return ret;
}

//DEF_TYPE_STR_FUNCS(number, "decimal", "");
const char *ob_sql_tc_str(ObObjTypeClass tc)
{
  static const char *sql_tc_name[] =
  {
    "NULL",
    "INT",
    "UINT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL",
    "DATETIME",
    "DATE",
    "TIME",
    "YEAR",
    "STRING",
    "EXT",
    "UNKNOWN",
    "TEXT",
    "BIT",
    "ENUM_SET",
    "ENUM_SET_INNER",
    "OTIMESTAMP",
    "RAW",
    "INTERVAL",
    "ROWID",
    "LOB",
    "JSON",
    "GEOMETRY",
    "UDT",
    ""
  };
  static_assert(sizeof(sql_tc_name) / sizeof(const char *) == ObMaxTC + 1, "Not enough initializer");
  return sql_tc_name[OB_LIKELY(tc < ObMaxTC) ? tc : ObMaxTC];
}

int32_t ob_obj_type_size(ObObjType type)
{
  int32_t size = 0;
  UNUSED(type);
  // @todo
  return size;
}

#ifndef INT24_MIN
#define INT24_MIN     (-8388607 - 1)
#endif
#ifndef INT24_MAX
#define INT24_MAX     (8388607)
#endif
#ifndef UINT24_MAX
#define UINT24_MAX    (16777215U)
#endif

const int64_t INT_MIN_VAL[ObMaxType] =
{
  0,        // null.
  INT8_MIN,
  INT16_MIN,
  INT24_MIN,
  INT32_MIN,
  INT64_MIN
};

const int64_t INT_MAX_VAL[ObMaxType] =
{
  0,        // null.
  INT8_MAX,
  INT16_MAX,
  INT24_MAX,
  INT32_MAX,
  INT64_MAX
};

const uint64_t UINT_MAX_VAL[ObMaxType] =
{
  0,              // null.
  0, 0, 0, 0, 0,  // int8, int16, int24, int32, int64.
  UINT8_MAX,
  UINT16_MAX,
  UINT24_MAX,
  UINT32_MAX,
  UINT64_MAX
};

const double REAL_MIN_VAL[ObMaxType] =
{
  0.0,                      // null.
  0.0, 0.0, 0.0, 0.0, 0.0,  // int8, int16, int24, int32, int64.
  0.0, 0.0, 0.0, 0.0, 0.0,  // uint8, uint16, uint24, uint32, uint64.
  -FLT_MAX,
  -DBL_MAX,
  0.0,
  0.0
};

const double REAL_MAX_VAL[ObMaxType] =
{
  0.0,                      // null.
  0.0, 0.0, 0.0, 0.0, 0.0,  // int8, int16, int24, int32, int64.
  0.0, 0.0, 0.0, 0.0, 0.0,  // uint8, uint16, uint24, uint32, uint64.
  FLT_MAX,
  DBL_MAX,
  FLT_MAX,
  DBL_MAX
};

// [in] pos : 开始查找的位置
// [out] pos : 匹配的下标；-1表示没有找到
int find_type(const ObIArray<common::ObString> &type_infos,
              ObCollationType cs_type, const ObString &val, int32_t &pos)
{
  int ret = OB_SUCCESS;
  int32_t start_pos = pos;
  pos = OB_INVALID_INDEX;
  if (OB_UNLIKELY(start_pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_pos));
  } else {
    for(int32_t i = start_pos; i < type_infos.count() && OB_INVALID_INDEX == pos; ++i) {
      const ObString &cur_val = type_infos.at(i);
      if (0 == ObCharset::strcmp(cs_type, val.ptr(), val.length(), cur_val.ptr(), cur_val.length())) {
        pos = i;
      }
    }
  }
  return ret;
}


} // common
} // oceanbase
