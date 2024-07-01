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

#define USING_LOG_PREFIX LIB_MYSQLC
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include <mysql.h>
#include "lib/string/ob_string.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_prepared_param.h"
#include "lib/mysqlclient/ob_mysql_prepared_result.h"
#include "lib/mysqlclient/ob_mysql_prepared_statement.h"
#include "share/schema/ob_routine_info.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
static const obmysql::EMySQLFieldType ob_type_to_mysql_type[ObMaxType] =
{
  /* ObMinType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NULL,          /* ObNullType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TINY,          /* ObTinyIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_SHORT,         /* ObSmallIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_INT24,         /* ObMediumIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONG,          /* ObInt32Type */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONGLONG,      /* ObIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TINY,          /* ObUTinyIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_SHORT,         /* ObUSmallIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_INT24,         /* ObUMediumIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONG,          /* ObUInt32Type */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONGLONG,      /* ObUInt64Type */
  obmysql::EMySQLFieldType::MYSQL_TYPE_FLOAT,         /* ObFloatType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_DOUBLE,        /* ObDoubleType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_FLOAT,         /* ObUFloatType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_DOUBLE,        /* ObUDoubleType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL,    /* ObNumberType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL,    /* ObUNumberType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_DATETIME,      /* ObDateTimeType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TIMESTAMP,     /* ObTimestampType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_DATE,          /* ObDateType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TIME,          /* ObTimeType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_YEAR,          /* ObYearType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING,    /* ObVarcharType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_STRING,        /* ObCharType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_VARCHAR,       /* ObHexStringType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_COMPLEX,       /* ObExtendType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,   /* ObUnknownType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_TINY_BLOB,     /* ObTinyTextType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_BLOB,          /* ObTextType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_MEDIUM_BLOB,   /* ObMediumTextType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_LONG_BLOB,     /* ObLongTextType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_BIT,           /* ObBitType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_STRING,        /* ObEnumType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_STRING,        /* ObSetType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,   /* ObEnumInnerType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,   /* ObSetInnerType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE,       /* ObTimestampTZType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE, /* ObTimestampLTZType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_NANO,                 /* ObTimestampNanoType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_RAW,                            /* ObRawType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_YM,                    /* ObIntervalYMType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_DS,                    /* ObIntervalDSType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NUMBER_FLOAT,                   /* ObNumberFloatType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NVARCHAR2,                      /* ObNVarchar2Type */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NCHAR,                          /* ObNCharType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_OB_UROWID,
  obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_BLOB,                          /* ObLobType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_JSON,                              /* ObJsonType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY,                          /* ObGeometryType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_COMPLEX,                           /* ObUserDefinedSQLType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL,                        /* ObDecimalIntType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_COMPLEX,                           /* ObCollectionSQLType */
  obmysql::EMySQLFieldType::MYSQL_TYPE_BLOB,                              /* ObRoaringBitmapType */
  /* ObMaxType */
};

int ObBindParamEncode::encode_null(ENCODE_FUNC_ARG_DECL)
{
  UNUSEDx(is_output_param, tz_info, allocator);
  int ret = OB_SUCCESS;
  const ObObjType obj_type = param.get_type();
  enum_field_types buffer_type = static_cast<enum_field_types>(ob_type_to_mysql_type[obj_type]);
  bind_param.col_idx_ = col_idx;
  bind_param.buffer_type_ = buffer_type;
  bind_param.is_null_ = 1;
  return ret;
}

int ObBindParamEncode::encode_int(ENCODE_FUNC_ARG_DECL)
{
  UNUSEDx(is_output_param, tz_info, allocator);
  int ret = OB_SUCCESS;
  const ObObjType obj_type = param.get_type();
  enum_field_types buffer_type = static_cast<enum_field_types>(ob_type_to_mysql_type[obj_type]);
  bind_param.col_idx_ = col_idx;
  bind_param.buffer_type_ = buffer_type;
  bind_param.buffer_ = &(param.v_.int64_);
  bind_param.buffer_len_ = sizeof(param.v_.int64_);
  return ret;
}

int ObBindParamEncode::encode_uint(ENCODE_FUNC_ARG_DECL)
{
  UNUSEDx(is_output_param, tz_info, allocator);
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_int(col_idx, is_output_param, tz_info, param, bind_param, allocator))) {
    LOG_WARN("fail to encode", K(ret));
  } else {
    bind_param.is_unsigned_ = 1;
  }
  return ret;
}

int ObBindParamEncode::encode_float(ENCODE_FUNC_ARG_DECL)
{
  UNUSEDx(is_output_param, tz_info, allocator);
  int ret = OB_SUCCESS;
  const ObObjType obj_type = param.get_type();
  enum_field_types buffer_type = static_cast<enum_field_types>(ob_type_to_mysql_type[obj_type]);
  bind_param.col_idx_ = col_idx;
  bind_param.buffer_type_ = buffer_type;
  bind_param.buffer_ = &(param.v_.float_);
  bind_param.buffer_len_ = sizeof(param.v_.float_);
  return ret;
}

int ObBindParamEncode::encode_ufloat(ENCODE_FUNC_ARG_DECL)
{
  UNUSEDx(is_output_param, tz_info, allocator);
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_float(col_idx, is_output_param, tz_info, param, bind_param, allocator))) {
    LOG_WARN("fail to encode", K(ret));
  } else {
    bind_param.is_unsigned_ = 1;
  }
  return ret;
}

int ObBindParamEncode::encode_double(ENCODE_FUNC_ARG_DECL)
{
  UNUSEDx(is_output_param, tz_info, allocator);
  int ret = OB_SUCCESS;
  const ObObjType obj_type = param.get_type();
  enum_field_types buffer_type = static_cast<enum_field_types>(ob_type_to_mysql_type[obj_type]);
  bind_param.col_idx_ = col_idx;
  bind_param.buffer_type_ = buffer_type;
  bind_param.buffer_ = &(param.v_.double_);
  bind_param.buffer_len_ = sizeof(param.v_.double_);
  return ret;
}

int ObBindParamEncode::encode_udouble(ENCODE_FUNC_ARG_DECL)
{
  UNUSEDx(is_output_param, tz_info, allocator);
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_double(col_idx, is_output_param, tz_info, param, bind_param, allocator))) {
    LOG_WARN("fail to encode", K(ret));
  } else {
    bind_param.is_unsigned_ = 1;
  }
  return ret;
}

int ObBindParamEncode::encode_number(ENCODE_FUNC_ARG_DECL)
{
  UNUSEDx(is_output_param, tz_info);
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *buf = nullptr;
  number::ObNumber num;
  const int64_t buf_len = OB_CAST_TO_VARCHAR_MAX_LENGTH;
  const ObObjType obj_type = param.get_type();
  enum_field_types buffer_type = static_cast<enum_field_types>(ob_type_to_mysql_type[obj_type]);
  bind_param.col_idx_ = col_idx;
  bind_param.buffer_type_ = buffer_type;
  if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(buf_len));
  } else if (OB_FAIL(param.get_number(num))) {
    LOG_WARN("fail to get number", K(ret), K(param));
  } else if (OB_FAIL(num.format(buf, buf_len, pos, param.get_scale()))) {
    LOG_WARN("fail to convert number to string", K(ret));
  } else {
    bind_param.buffer_ = buf;
    bind_param.buffer_len_ = buf_len;
    bind_param.length_ = pos;
  }
  return ret;
}

int ObBindParamEncode::encode_unumber(ENCODE_FUNC_ARG_DECL)
{
  UNUSEDx(is_output_param, tz_info);
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_number(col_idx, is_output_param, tz_info, param, bind_param, allocator))) {
    LOG_WARN("fail to encode", K(ret));
  } else {
    bind_param.is_unsigned_ = 1;
  }
  return ret;
}

int ObBindParamEncode::encode_datetime(ENCODE_FUNC_ARG_DECL)
{
  UNUSEDx(is_output_param);
  int ret = OB_SUCCESS;
  ObTime ob_time;
  MYSQL_TIME *tm = nullptr;
  const ObObjType obj_type = param.get_type();
  enum_field_types buffer_type = static_cast<enum_field_types>(ob_type_to_mysql_type[obj_type]);
  bind_param.col_idx_ = col_idx;
  bind_param.buffer_type_ = buffer_type;
  if (OB_ISNULL(tm = reinterpret_cast<MYSQL_TIME *>(allocator.alloc(sizeof(MYSQL_TIME))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(param.get_datetime(), &tz_info, ob_time))) {
    LOG_WARN("convert usec ", K(ret));
  } else {
    MEMSET(tm, 0, sizeof(MYSQL_TIME));
    tm->year = ob_time.parts_[DT_YEAR];
    tm->month = ob_time.parts_[DT_MON];
    tm->day = ob_time.parts_[DT_MDAY];
    tm->hour= ob_time.parts_[DT_HOUR];
    tm->minute= ob_time.parts_[DT_MIN];
    tm->second= ob_time.parts_[DT_SEC];
    tm->second_part= ob_time.parts_[DT_USEC];
    tm->neg = DT_MODE_NEG & ob_time.mode_;
    bind_param.buffer_ = tm;
    bind_param.buffer_len_ = sizeof(MYSQL_TIME);
  }
  return ret;
}

int ObBindParamEncode::encode_date(ENCODE_FUNC_ARG_DECL)
{
  UNUSEDx(is_output_param);
  int ret = OB_SUCCESS;
  ObTime ob_time;
  MYSQL_TIME *tm = nullptr;
  const ObObjType obj_type = param.get_type();
  enum_field_types buffer_type = static_cast<enum_field_types>(ob_type_to_mysql_type[obj_type]);
  bind_param.col_idx_ = col_idx;
  bind_param.buffer_type_ = buffer_type;
  if (OB_ISNULL(tm = reinterpret_cast<MYSQL_TIME *>(allocator.alloc(sizeof(MYSQL_TIME))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(ObTimeConverter::date_to_ob_time(param.get_date(), ob_time))) {
    LOG_WARN("convert usec ", K(ret));
  } else {
    MEMSET(tm, 0, sizeof(MYSQL_TIME));
    tm->year = ob_time.parts_[DT_YEAR];
    tm->month = ob_time.parts_[DT_MON];
    tm->day = ob_time.parts_[DT_MDAY];
    tm->neg = DT_MODE_NEG & ob_time.mode_;
    bind_param.buffer_ = tm;
    bind_param.buffer_len_ = sizeof(MYSQL_TIME);
  }
  return ret;
}

int ObBindParamEncode::encode_time(ENCODE_FUNC_ARG_DECL)
{
  UNUSEDx(is_output_param);
  int ret = OB_SUCCESS;
  ObTime ob_time;
  MYSQL_TIME *tm = nullptr;
  const ObObjType obj_type = param.get_type();
  enum_field_types buffer_type = static_cast<enum_field_types>(ob_type_to_mysql_type[obj_type]);
  bind_param.col_idx_ = col_idx;
  bind_param.buffer_type_ = buffer_type;
  if (OB_ISNULL(tm = reinterpret_cast<MYSQL_TIME *>(allocator.alloc(sizeof(MYSQL_TIME))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(ObTimeConverter::time_to_ob_time(param.get_time(), ob_time))) {
    LOG_WARN("convert usec ", K(ret));
  } else {
    MEMSET(tm, 0, sizeof(MYSQL_TIME));
    tm->day = ob_time.parts_[DT_DATE];
    tm->hour= ob_time.parts_[DT_HOUR];
    tm->minute= ob_time.parts_[DT_MIN];
    tm->second= ob_time.parts_[DT_SEC];
    tm->second_part= ob_time.parts_[DT_USEC];
    tm->neg = DT_MODE_NEG & ob_time.mode_;
    bind_param.buffer_ = tm;
    bind_param.buffer_len_ = sizeof(MYSQL_TIME);
  }
  return ret;
}

int ObBindParamEncode::encode_year(ENCODE_FUNC_ARG_DECL)
{
  UNUSEDx(is_output_param);
  int ret = OB_SUCCESS;
  int64_t *year = nullptr;
  const ObObjType obj_type = param.get_type();
  enum_field_types buffer_type = static_cast<enum_field_types>(ob_type_to_mysql_type[obj_type]);
  bind_param.col_idx_ = col_idx;
  bind_param.buffer_type_ = buffer_type;
  if (OB_ISNULL(year = reinterpret_cast<int64_t *>(allocator.alloc(sizeof(int64_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(ObTimeConverter::year_to_int(param.get_year(), *year))) {
    LOG_WARN("convert usec ", K(ret));
  } else {
    bind_param.col_idx_ = col_idx;
    bind_param.buffer_type_ = buffer_type;
    bind_param.buffer_ = year;
    bind_param.buffer_len_ = sizeof(int64_t);
  }
  return ret;
}

int ObBindParamEncode::encode_string(ENCODE_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ObLength length = param.get_length();
  ObString val = param.get_string();
  const ObObjType obj_type = param.get_type();
  enum_field_types buffer_type = static_cast<enum_field_types>(ob_type_to_mysql_type[obj_type]);
  bind_param.col_idx_ = col_idx;
  bind_param.buffer_type_ = buffer_type;
  if (is_output_param) {
    char *buf = nullptr;
    const int64_t buf_len = length * 3;
    if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(buf_len));
    } else {
      MEMCPY(buf, val.ptr(), val.length());
      bind_param.buffer_ = buf;
      bind_param.buffer_len_ = buf_len;
      bind_param.length_ = val.length();
    }
  } else {
    bind_param.buffer_ = val.ptr();
    bind_param.buffer_len_ = val.length();
    bind_param.length_ = val.length();
  }
  return ret;
}

int ObBindParamEncode::encode_not_supported(ENCODE_FUNC_ARG_DECL)
{
  UNUSEDx(col_idx, is_output_param, tz_info, bind_param, allocator);
  const ObObjType obj_type = param.get_type();
  int ret = OB_SUCCESS;
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not supported type", K(ret), K(obj_type));
  return ret;
}

const ObBindParamEncode::EncodeFunc ObBindParamEncode::encode_map_[ObMaxType + 1] =
{
  ObBindParamEncode::encode_null,                    // ObNullType
  ObBindParamEncode::encode_int,                     // ObTinyIntType
  ObBindParamEncode::encode_int,                     // ObSmallIntType
  ObBindParamEncode::encode_int,                     // ObMediumIntType
  ObBindParamEncode::encode_int,                     // ObInt32Type
  ObBindParamEncode::encode_int,                     // ObIntType
  ObBindParamEncode::encode_uint,                    // ObUTinyIntType
  ObBindParamEncode::encode_uint,                    // ObUSmallIntType
  ObBindParamEncode::encode_uint,                    // ObUMediumIntType
  ObBindParamEncode::encode_uint,                    // ObUInt32Type
  ObBindParamEncode::encode_uint,                    // ObUInt64Type
  ObBindParamEncode::encode_float,                   // ObFloatType
  ObBindParamEncode::encode_double,                  // ObDoubleType
  ObBindParamEncode::encode_ufloat,                  // ObUFloatType
  ObBindParamEncode::encode_udouble,                 // ObUDoubleType
  ObBindParamEncode::encode_number,                  // ObNumberType
  ObBindParamEncode::encode_unumber,                 // ObUNumberType
  ObBindParamEncode::encode_datetime,                // ObDateTimeType
  ObBindParamEncode::encode_datetime,                // ObTimestampType
  ObBindParamEncode::encode_date,                    // ObDateType
  ObBindParamEncode::encode_time,                    // ObTimeType
  ObBindParamEncode::encode_year,                    // ObYearType
  ObBindParamEncode::encode_string,                  // ObVarcharType
  ObBindParamEncode::encode_string,                  // ObCharType
  ObBindParamEncode::encode_not_supported,           // ObHexStringType
  ObBindParamEncode::encode_not_supported,           // ObExtendType
  ObBindParamEncode::encode_not_supported,           // ObUnknownType
  ObBindParamEncode::encode_string,                  // ObTinyTextType
  ObBindParamEncode::encode_string,                  // ObTextType
  ObBindParamEncode::encode_string,                  // ObMediumTextType
  ObBindParamEncode::encode_string,                  // ObLongTextType
  ObBindParamEncode::encode_not_supported,           // ObBitType
  ObBindParamEncode::encode_not_supported,           // ObEnumType
  ObBindParamEncode::encode_not_supported,           // ObSetType
  ObBindParamEncode::encode_not_supported,           // ObEnumInnerType
  ObBindParamEncode::encode_not_supported,           // ObSetInnerType
  ObBindParamEncode::encode_not_supported,           // ObTimestampTZType
  ObBindParamEncode::encode_not_supported,           // ObTimestampLTZType
  ObBindParamEncode::encode_not_supported,           // ObTimestampNanoType
  ObBindParamEncode::encode_not_supported,           // ObRawType
  ObBindParamEncode::encode_not_supported,           // ObIntervalYMType
  ObBindParamEncode::encode_not_supported,           // ObIntervalDSType
  ObBindParamEncode::encode_not_supported,           // ObNumberFloatType
  ObBindParamEncode::encode_not_supported,           // ObNVarchar2Type
  ObBindParamEncode::encode_not_supported,           // ObNCharType
  ObBindParamEncode::encode_not_supported,           // ObURowIDType
  ObBindParamEncode::encode_not_supported,           // ObLobType
  ObBindParamEncode::encode_not_supported,           // ObJsonType
  ObBindParamEncode::encode_not_supported,           // ObGeometryType
  ObBindParamEncode::encode_not_supported            // ObMaxType
};

int ObBindParamDecode::decode_null(DECODE_FUNC_ARG_DECL)
{
  UNUSEDx(field_type, tz_info, bind_param, allocator);
  int ret = OB_SUCCESS;
  param.set_null();
  return ret;
}

int ObBindParamDecode::decode_int(DECODE_FUNC_ARG_DECL)
{
  UNUSEDx(tz_info, allocator);
  int ret = OB_SUCCESS;
  switch (field_type) {
    case MYSQL_TYPE_TINY:
      param.set_tinyint(*(reinterpret_cast<int8_t*>(bind_param.buffer_)));
      break;
    case MYSQL_TYPE_SHORT:
      param.set_smallint(*(reinterpret_cast<int16_t*>(bind_param.buffer_)));
      break;
    case MYSQL_TYPE_INT24:
      param.set_mediumint(*(reinterpret_cast<int32_t*>(bind_param.buffer_)));
      break;
    case MYSQL_TYPE_LONG:
      param.set_int32(*(reinterpret_cast<int32_t*>(bind_param.buffer_)));
      break;
    case MYSQL_TYPE_LONGLONG:
      param.set_int(*(reinterpret_cast<int64_t*>(bind_param.buffer_)));
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown type", K(field_type));
      break;
  };
  return ret;
}

int ObBindParamDecode::decode_uint(DECODE_FUNC_ARG_DECL)
{
  UNUSEDx(tz_info, allocator);
  int ret = OB_SUCCESS;
  switch (field_type) {
    case MYSQL_TYPE_TINY:
      param.set_utinyint(*(reinterpret_cast<uint8_t*>(bind_param.buffer_)));
      break;
    case MYSQL_TYPE_SHORT:
      param.set_usmallint(*(reinterpret_cast<uint16_t*>(bind_param.buffer_)));
      break;
    case MYSQL_TYPE_INT24:
      param.set_umediumint(*(reinterpret_cast<uint32_t*>(bind_param.buffer_)));
      break;
    case MYSQL_TYPE_LONG:
      param.set_uint32(*(reinterpret_cast<uint32_t*>(bind_param.buffer_)));
      break;
    case MYSQL_TYPE_LONGLONG:
      param.set_uint64(*(reinterpret_cast<uint64_t*>(bind_param.buffer_)));
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown type", K(field_type));
      break;
  };
  return ret;
}

int ObBindParamDecode::decode_float(DECODE_FUNC_ARG_DECL)
{
  UNUSEDx(field_type, tz_info, allocator);
  int ret = OB_SUCCESS;
  param.set_float(*(reinterpret_cast<float*>(bind_param.buffer_)));
  return ret;
}

int ObBindParamDecode::decode_ufloat(DECODE_FUNC_ARG_DECL)
{
  UNUSEDx(field_type, tz_info, allocator);
  int ret = OB_SUCCESS;
  param.set_ufloat(*(reinterpret_cast<float*>(bind_param.buffer_)));
  return ret;
}

int ObBindParamDecode::decode_double(DECODE_FUNC_ARG_DECL)
{
  UNUSEDx(field_type, tz_info, allocator);
  int ret = OB_SUCCESS;
  param.set_double(*(reinterpret_cast<double*>(bind_param.buffer_)));
  return ret;
}

int ObBindParamDecode::decode_udouble(DECODE_FUNC_ARG_DECL)
{
  UNUSEDx(field_type, tz_info, allocator);
  int ret = OB_SUCCESS;
  param.set_udouble(*(reinterpret_cast<double*>(bind_param.buffer_)));
  return ret;
}

int ObBindParamDecode::decode_number(DECODE_FUNC_ARG_DECL)
{
  UNUSEDx(field_type, tz_info);
  int ret = OB_SUCCESS;
  number::ObNumber nb;
  if (OB_FAIL(nb.from(reinterpret_cast<char *>(bind_param.buffer_), bind_param.length_, allocator))) {
    LOG_WARN("decode param to number failed", K(ret), K(bind_param));
  } else {
    param.set_number(nb);
  }
  return ret;
}

int ObBindParamDecode::decode_unumber(DECODE_FUNC_ARG_DECL)
{
  UNUSEDx(field_type, tz_info);
  int ret = OB_SUCCESS;
  number::ObNumber nb;
  if (OB_FAIL(nb.from(reinterpret_cast<char *>(bind_param.buffer_), bind_param.length_, allocator))) {
    LOG_WARN("decode param to number failed", K(ret), K(bind_param));
  } else {
    param.set_unumber(nb);
  }
  return ret;
}

int ObBindParamDecode::decode_datetime(DECODE_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  ObPreciseDateTime value;
  MYSQL_TIME *tm = reinterpret_cast<MYSQL_TIME *>(bind_param.buffer_);
  if (0 == bind_param.length_) {
    value = 0;
  } else {
    ob_time.parts_[DT_YEAR] = tm->year;
    ob_time.parts_[DT_MON] = tm->month;
    ob_time.parts_[DT_MDAY] = tm->day;
    ob_time.parts_[DT_HOUR] = tm->hour;
    ob_time.parts_[DT_MIN] = tm->minute;
    ob_time.parts_[DT_SEC] = tm->second;
    if (lib::is_oracle_mode()) {
      ob_time.parts_[DT_USEC] = 0;
    } else {
      ob_time.parts_[DT_USEC] = tm->second_part;
    }
    ObTimeConvertCtx cvrt_ctx(NULL, false);
    ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
    if (MYSQL_TYPE_DATE == field_type) {
      value = ob_time.parts_[DT_DATE];
    } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, value))){
      LOG_WARN("convert obtime to datetime failed", K(ret), K(value), K(tm->year), K(tm->month),
                K(tm->day), K(tm->hour), K(tm->minute), K(tm->second));
    }
  }
  if (OB_SUCC(ret)) {
    if (MYSQL_TYPE_TIMESTAMP == field_type) {
      int64_t ts_value = 0;
      if (OB_FAIL(ObTimeConverter::datetime_to_timestamp(value, &tz_info, ts_value))) {
        LOG_WARN("datetime to timestamp failed", K(ret));
      } else {
        param.set_timestamp(ts_value);
      }
    } else if (MYSQL_TYPE_DATETIME == field_type) {
      param.set_datetime(value);
    } else if (MYSQL_TYPE_DATE == field_type) {
      param.set_date(static_cast<int32_t>(value));
    }
  }
  LOG_TRACE("get datetime", K(tm->year), K(tm->month), K(tm->day), K(tm->hour), K(tm->minute),
  K(tm->second), K(tm->second_part), K(value));
  return ret;
}

int ObBindParamDecode::decode_time(DECODE_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  ObPreciseDateTime value;
  MYSQL_TIME *tm = reinterpret_cast<MYSQL_TIME *>(bind_param.buffer_);
  if (0 == bind_param.length_) {
    value = 0;
  } else {
    ob_time.parts_[DT_YEAR] = tm->year;
    ob_time.parts_[DT_MON] = tm->month;
    ob_time.parts_[DT_MDAY] = tm->day;
    ob_time.parts_[DT_HOUR] = tm->hour;
    ob_time.parts_[DT_MIN] = tm->minute;
    ob_time.parts_[DT_SEC] = tm->second;
    ob_time.parts_[DT_USEC] = tm->second_part;
    ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
    value = ObTimeConverter::ob_time_to_time(ob_time);
  }
  if (OB_SUCC(ret)) {
    param.set_time(value);
  }
  LOG_TRACE("get time", K(tm->year), K(tm->month), K(tm->day), K(tm->hour), K(tm->minute),
  K(tm->second), K(tm->second_part), K(value));
  return ret;
}

int ObBindParamDecode::decode_year(DECODE_FUNC_ARG_DECL)
{
  UNUSEDx(field_type, tz_info, allocator);
  int ret = OB_SUCCESS;
  param.set_year(*reinterpret_cast<uint8_t*>(bind_param.buffer_));
  return ret;
}

int ObBindParamDecode::decode_string(DECODE_FUNC_ARG_DECL)
{
  UNUSEDx(tz_info);
  int ret = OB_SUCCESS;
  ObString dst(bind_param.length_, static_cast<char *>(bind_param.buffer_));
  if (OB_FAIL(ob_write_string(allocator, dst, dst))) {
    LOG_WARN("failed to write str", K(ret));
  } else {
    switch (field_type) {
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_VAR_STRING:
        param.set_varchar(dst);
        break;
      case MYSQL_TYPE_STRING:
        param.set_char(dst);
      case MYSQL_TYPE_TINY_BLOB:
        param.set_lob_value(ObTinyTextType, dst.ptr(), dst.length());
      case MYSQL_TYPE_BLOB:
        param.set_lob_value(ObTextType, dst.ptr(), dst.length());
      case MYSQL_TYPE_MEDIUM_BLOB:
        param.set_lob_value(ObMediumTextType, dst.ptr(), dst.length());
      case MYSQL_TYPE_LONG_BLOB:
        param.set_lob_value(ObLongTextType, dst.ptr(), dst.length());
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown type", K(ret), K(field_type));
        break;
    }
  }
  return ret;
}

int ObBindParamDecode::decode_not_supported(DECODE_FUNC_ARG_DECL)
{
  UNUSEDx(tz_info, bind_param, param, allocator);
  int ret = OB_SUCCESS;
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("not supported type", K(ret), K(field_type));
  return ret;
}

const ObBindParamDecode::DecodeFunc ObBindParamDecode::decode_map_[ObMaxType + 1] =
{
  ObBindParamDecode::decode_null,                    // ObNullType
  ObBindParamDecode::decode_int,                     // ObTinyIntType
  ObBindParamDecode::decode_int,                     // ObSmallIntType
  ObBindParamDecode::decode_int,                     // ObMediumIntType
  ObBindParamDecode::decode_int,                     // ObInt32Type
  ObBindParamDecode::decode_int,                     // ObIntType
  ObBindParamDecode::decode_uint,                    // ObUTinyIntType
  ObBindParamDecode::decode_uint,                    // ObUSmallIntType
  ObBindParamDecode::decode_uint,                    // ObUMediumIntType
  ObBindParamDecode::decode_uint,                    // ObUInt32Type
  ObBindParamDecode::decode_uint,                    // ObUInt64Type
  ObBindParamDecode::decode_float,                   // ObFloatType
  ObBindParamDecode::decode_double,                  // ObDoubleType
  ObBindParamDecode::decode_ufloat,                  // ObUFloatType
  ObBindParamDecode::decode_udouble,                 // ObUDoubleType
  ObBindParamDecode::decode_number,                  // ObNumberType
  ObBindParamDecode::decode_unumber,                 // ObUNumberType
  ObBindParamDecode::decode_datetime,                // ObDateTimeType
  ObBindParamDecode::decode_datetime,                // ObTimestampType
  ObBindParamDecode::decode_datetime,                // ObDateType
  ObBindParamDecode::decode_time,                    // ObTimeType
  ObBindParamDecode::decode_year,                    // ObYearType
  ObBindParamDecode::decode_string,                  // ObVarcharType
  ObBindParamDecode::decode_string,                  // ObCharType
  ObBindParamDecode::decode_not_supported,           // ObHexStringType
  ObBindParamDecode::decode_not_supported,           // ObExtendType
  ObBindParamDecode::decode_not_supported,           // ObUnknownType
  ObBindParamDecode::decode_string,                  // ObTinyTextType
  ObBindParamDecode::decode_string,                  // ObTextType
  ObBindParamDecode::decode_string,                  // ObMediumTextType
  ObBindParamDecode::decode_string,                  // ObLongTextType
  ObBindParamDecode::decode_not_supported,           // ObBitType
  ObBindParamDecode::decode_not_supported,           // ObEnumType
  ObBindParamDecode::decode_not_supported,           // ObSetType
  ObBindParamDecode::decode_not_supported,           // ObEnumInnerType
  ObBindParamDecode::decode_not_supported,           // ObSetInnerType
  ObBindParamDecode::decode_not_supported,           // ObTimestampTZType
  ObBindParamDecode::decode_not_supported,           // ObTimestampLTZType
  ObBindParamDecode::decode_not_supported,           // ObTimestampNanoType
  ObBindParamDecode::decode_not_supported,           // ObRawType
  ObBindParamDecode::decode_not_supported,           // ObIntervalYMType
  ObBindParamDecode::decode_not_supported,           // ObIntervalDSType
  ObBindParamDecode::decode_not_supported,           // ObNumberFloatType
  ObBindParamDecode::decode_not_supported,           // ObNVarchar2Type
  ObBindParamDecode::decode_not_supported,           // ObNCharType
  ObBindParamDecode::decode_not_supported,           // ObURowIDType
  ObBindParamDecode::decode_not_supported,           // ObLobType
  ObBindParamDecode::decode_not_supported,           // ObJsonType
  ObBindParamDecode::decode_not_supported,           // ObGeometryType
  ObBindParamDecode::decode_not_supported            // ObMaxType
};

int ObBindParam::assign(const ObBindParam &other)
{
  int ret = OB_SUCCESS;
  col_idx_ = other.col_idx_;
  buffer_type_ = other.buffer_type_;
  buffer_ = other.buffer_;
  buffer_len_ = other.buffer_len_;
  length_ = other.length_;
  is_unsigned_ = other.is_unsigned_;
  is_null_ = other.is_null_;
  return ret;
}

ObMySQLPreparedStatement::ObMySQLPreparedStatement() :
    conn_(NULL),
    arena_allocator_(ObModIds::MYSQL_CLIENT_CACHE),
    alloc_(&arena_allocator_),
    param_(*this),
    result_(*this),
    stmt_param_count_(0),
    result_column_count_(0),
    stmt_(NULL),
    bind_params_(NULL),
    result_params_(NULL)
{
}

ObMySQLPreparedStatement::~ObMySQLPreparedStatement()
{
}

ObIAllocator &ObMySQLPreparedStatement::get_allocator()
{
  return *alloc_;
}

MYSQL_STMT *ObMySQLPreparedStatement::get_stmt_handler()
{
  return stmt_;
}

MYSQL *ObMySQLPreparedStatement::get_conn_handler()
{
  return conn_->get_handler();
}

ObMySQLConnection *ObMySQLPreparedStatement::get_connection()
{
  return conn_;
}

int ObMySQLPreparedStatement::alloc_bind_params(const int64_t size, ObBindParam *&bind_params)
{
  int ret = OB_SUCCESS;
  if (size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(size));
  } else if (OB_ISNULL(bind_params = reinterpret_cast<ObBindParam *>(alloc_->alloc(sizeof(ObBindParam) * size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("out of memory, alloc mem for mysql_bind error", K(ret));
  } else {
    MEMSET(bind_params, 0, sizeof(ObBindParam) * size);
  }
  return ret;
}

int ObMySQLPreparedStatement::get_bind_param_by_idx(const int64_t idx,
                                                    ObBindParam *&param)
{
  int ret = OB_SUCCESS;
  param = nullptr;
  if (idx >= stmt_param_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index", K(ret), K(idx), K(stmt_param_count_));
  } else {
    param = &(bind_params_[idx]);
  }
  return ret;
}

int ObMySQLPreparedStatement::get_bind_result_param_by_idx(const int64_t idx,
                                                           ObBindParam *&param)
{
  int ret = OB_SUCCESS;
  param = nullptr;
  if (idx >= result_column_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index", K(ret), K(idx), K(result_column_count_));
  } else {
    param = &(result_params_[idx]);
  }
  return ret;
}

int ObMySQLPreparedStatement::get_mysql_type(ObObjType ob_type, obmysql::EMySQLFieldType &mysql_type) const
{
  int ret = OB_SUCCESS;
  if (ob_type < ObNullType || ob_type >= ObMaxType) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(ret), K(ob_type));
  } else {
    mysql_type = static_cast<obmysql::EMySQLFieldType>(ob_type_to_mysql_type[ob_type]);
  }
  return ret;
}

int ObMySQLPreparedStatement::get_ob_type(ObObjType &ob_type, obmysql::EMySQLFieldType mysql_type) const
{
  int ret = OB_SUCCESS;
  switch (mysql_type) {
    case obmysql::EMySQLFieldType::MYSQL_TYPE_NULL:
      ob_type = ObNullType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_TINY:
      ob_type = ObTinyIntType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_SHORT:
      ob_type = ObSmallIntType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_LONG:
      ob_type = ObInt32Type;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_LONGLONG:
      ob_type = ObIntType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_FLOAT:
      ob_type = ObFloatType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_DOUBLE:
      ob_type = ObDoubleType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_TIMESTAMP:
      ob_type = ObTimestampType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_DATETIME:
      ob_type = ObDateTimeType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_TIME:
      ob_type = ObTimeType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_DATE:
      ob_type = ObDateType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_YEAR:
      ob_type = ObYearType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_VARCHAR:
    case obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING:
      ob_type = ObVarcharType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_STRING:
      ob_type = ObCharType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_TINY_BLOB:
      ob_type = ObTinyTextType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_BLOB:
      ob_type = ObTextType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_MEDIUM_BLOB:
      ob_type = ObMediumTextType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_LONG_BLOB:
      ob_type = ObLongTextType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:
      ob_type = ObTimestampTZType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      ob_type = ObTimestampLTZType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_NANO:
      ob_type = ObTimestampNanoType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_RAW:
      ob_type = ObRawType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL:
      ob_type = ObNumberType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NUMBER_FLOAT:
      ob_type = ObNumberFloatType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_JSON:
      ob_type = ObJsonType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY:
      ob_type = ObGeometryType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_BIT:
      ob_type = ObBitType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_ENUM:
      ob_type = ObEnumType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_SET:
      ob_type = ObSetType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_YM:
      ob_type = ObIntervalYMType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_DS:
      ob_type = ObIntervalDSType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NVARCHAR2:
      ob_type = ObNVarchar2Type;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_NCHAR:
      ob_type = ObNCharType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_COMPLEX:
      ob_type = ObExtendType;
      break;
    case obmysql::EMySQLFieldType::MYSQL_TYPE_OB_UROWID:
      ob_type = ObURowIDType;
      break;
    default:
      LOG_WARN("unsupport MySQL type", K(ret), K(mysql_type));
      ret = OB_OBJ_TYPE_ERROR;
  }
  return ret;
}

int ObMySQLPreparedStatement::init(ObMySQLConnection &conn, const char *sql)
{
  int ret = OB_SUCCESS;
  conn_ = &conn;
  if (OB_ISNULL(sql)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql", KP(sql), K(ret));
  } else if (OB_ISNULL(stmt_ = mysql_stmt_init(conn_->get_handler()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to init stmt", K(ret));
  } else if (0 != mysql_stmt_prepare(stmt_, sql, STRLEN(sql))) {
    ret = -mysql_errno(conn_->get_handler());
    LOG_WARN("fail to prepare stmt", "info", mysql_error(conn_->get_handler()), K(ret));
  } else if (OB_FAIL(param_.init())) {
    LOG_WARN("fail to init prepared result", K(ret));
  } else if (OB_FAIL(result_.init())) {
    LOG_WARN("fail to init prepared result", K(ret));
  } else if (FALSE_IT(stmt_param_count_ = param_.get_stmt_param_count())) {
  } else if (FALSE_IT(result_column_count_ = result_.get_result_column_count())) {
  } else if (stmt_param_count_ > 0 && OB_FAIL(alloc_bind_params(stmt_param_count_, bind_params_))) {
    LOG_WARN("fail to alloc stmt bind params", K(ret));
  } else if (result_column_count_ > 0 && OB_FAIL(alloc_bind_params(result_column_count_, result_params_))) {
    LOG_WARN("fail to alloc result bind params", K(ret));
  } else {
    LOG_INFO("conn_handler", "handler", conn_->get_handler(), K_(stmt), K_(stmt_param_count), K_(result_column_count));
  }
  return ret;
}

int ObMySQLPreparedStatement::close()
{
  int ret = OB_SUCCESS;
  if (nullptr != stmt_) {
    if (0 != mysql_stmt_close(stmt_)) {
      ret = -mysql_errno(conn_->get_handler());
      LOG_WARN("fail to close stmt", "info", mysql_error(conn_->get_handler()), K(ret));
    }
  }
  stmt_param_count_ = 0;
  result_column_count_ = 0;
  bind_params_ = NULL;
  result_params_ = NULL;
  return ret;
}

int ObMySQLPreparedStatement::bind_param(const ObBindParam &bind_param)
{
  int ret = OB_SUCCESS;
  if (bind_param.col_idx_ >= stmt_param_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index", K(ret), K(bind_param), K(stmt_param_count_));
  } else {
    ObBindParam &param = bind_params_[bind_param.col_idx_];
    if (OB_FAIL(param.assign(bind_param))) {
      LOG_WARN("fail to assing bind param", K(ret));
    } else if (OB_FAIL(param_.bind_param(param))) {
      LOG_WARN("fail to bind param", K(ret), K(bind_param));
    }
  }
  return ret;
}

int ObMySQLPreparedStatement::bind_result(const ObBindParam &bind_param)
{
  int ret = OB_SUCCESS;
  if (bind_param.col_idx_ >= result_column_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index", K(ret), K(bind_param), K(result_column_count_));
  } else {
    ObBindParam &param = result_params_[bind_param.col_idx_];
    if (OB_FAIL(param.assign(bind_param))) {
      LOG_WARN("fail to assing bind param", K(ret));
    } else if (OB_FAIL(result_.bind_result(param))) {
      LOG_WARN("fail to bind param", K(ret), K(bind_param));
    }
  }
  return ret;
}

int ObMySQLPreparedStatement::bind_param_int(const int64_t col_idx, int64_t *in_int_val)
{
  int ret = OB_SUCCESS;
  ObBindParam *param = nullptr;
  if (OB_FAIL(get_bind_param_by_idx(col_idx, param))) {
    LOG_WARN("fail to get bind param by idx", K(ret), K(col_idx), K(stmt_param_count_));
  } else if (OB_ISNULL(param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get bind param by idx", K(ret), K(col_idx), K(stmt_param_count_));
  } else {
    param->col_idx_ = col_idx;
    param->buffer_type_ = enum_field_types::MYSQL_TYPE_LONGLONG;
    param->buffer_ = reinterpret_cast<char *>(in_int_val);
    param->buffer_len_ = sizeof(int64_t);
    if (OB_FAIL(bind_param(*param))) {
      LOG_WARN("fail to bind int param", K(ret));
    }
  }
  return ret;
}

int ObMySQLPreparedStatement::bind_param_varchar(const int64_t col_idx, char *in_str_val,
                                                 unsigned long in_str_len)
{
  int ret = OB_SUCCESS;
  ObBindParam *param = nullptr;
  if (OB_FAIL(get_bind_param_by_idx(col_idx, param))) {
    LOG_WARN("fail to get bind param by idx", K(ret), K(col_idx), K(stmt_param_count_));
  } else if (OB_ISNULL(param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get bind param by idx", K(ret), K(col_idx), K(stmt_param_count_));
  } else {
    param->col_idx_ = col_idx;
    param->buffer_type_ = enum_field_types::MYSQL_TYPE_STRING;
    param->buffer_ = in_str_val;
    param->buffer_len_ = 0;
    param->length_ = in_str_len;
    if (OB_FAIL(bind_param(*param))) {
      LOG_WARN("fail to bind int param", K(ret));
    }
  }
  return ret;
}

int ObMySQLPreparedStatement::bind_result_int(const int64_t col_idx, int64_t *out_buf)
{
  int ret = OB_SUCCESS;
  ObBindParam *param = nullptr;
  if (OB_FAIL(get_bind_result_param_by_idx(col_idx, param))) {
    LOG_WARN("fail to get bind param by idx", K(ret), K(col_idx), K(result_column_count_));
  } else if (OB_ISNULL(param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get bind result param by idx", K(ret), K(col_idx), K(result_column_count_));
  } else {
    param->col_idx_ = col_idx;
    param->buffer_type_ = enum_field_types::MYSQL_TYPE_LONGLONG;
    param->buffer_ = reinterpret_cast<char *>(out_buf);
    param->buffer_len_ = sizeof(int64_t);
    if (OB_FAIL(bind_result(*param))) {
      LOG_WARN("fail to bind int result param", K(ret));
    }
  }
  return ret;
}

int ObMySQLPreparedStatement::bind_result_varchar(const int64_t col_idx,
                                                  char *out_buf,
                                                  const int buf_len,
                                                  unsigned long *&res_len)
{
  int ret = OB_SUCCESS;
  ObBindParam *param = nullptr;
  if (OB_FAIL(get_bind_result_param_by_idx(col_idx, param))) {
    LOG_WARN("fail to get bind param by idx", K(ret), K(col_idx), K(result_column_count_));
  } else if (OB_ISNULL(param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get bind param by idx", K(ret), K(col_idx), K(result_column_count_));
  } else {
    param->col_idx_ = col_idx;
    param->buffer_type_ = enum_field_types::MYSQL_TYPE_STRING;
    param->buffer_ = out_buf;
    param->buffer_len_ = buf_len;
    param->length_ = 0;
    res_len = &param->length_;
    if (OB_FAIL(bind_result(*param))) {
      LOG_WARN("fail to bind varchar result param", K(ret));
    }
  }
  return ret;
}

int ObMySQLPreparedStatement::execute_update()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stmt handler is null", K(ret));
  } else if (OB_FAIL(param_.bind_param())) {
    LOG_WARN("fail to bind prepared input param", K(ret));
  } else if (0 != mysql_stmt_execute(stmt_)) {
    ret = -mysql_stmt_errno(stmt_);
    LOG_WARN("fail to execute stmt", "info", mysql_stmt_error(stmt_), K(ret));
  }
  return ret;
}

ObMySQLPreparedResult *ObMySQLPreparedStatement::execute_query()
{
  ObMySQLPreparedResult *result = NULL;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stmt handler is null", K(ret));
  } else if (OB_FAIL(param_.bind_param())) {
    LOG_WARN("fail to bind prepared input param", K(ret));
  } else if (OB_FAIL(result_.bind_result_param())) {
    LOG_WARN("bind result param fail", K(ret));
  } else if (0 != mysql_stmt_execute(stmt_)) {
    ret = -mysql_stmt_errno(stmt_);
    LOG_WARN("fail to execute stmt", "info", mysql_stmt_error(stmt_), K(ret));
  } else if (0 != mysql_stmt_store_result(stmt_)) {
    ret = -mysql_stmt_errno(stmt_);
    LOG_WARN("fail to store prepared result", "info", mysql_stmt_error(stmt_), K(ret));
  } else {
    result = &result_;
  }
  conn_->set_last_error(ret);
  return result;
}

int ObMySQLProcStatement::bind_param(const int64_t col_idx,
                                     const bool is_output_param,
                                     const ObTimeZoneInfo *tz_info,
                                     ObObjParam &param,
                                     const share::schema::ObRoutineInfo &routine_info,
                                     ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObBindParam *bind_param = nullptr;
  const ObObjType obj_type = param.get_type();
  if (OB_FAIL(get_bind_param_by_idx(col_idx, bind_param))) {
    LOG_WARN("fail to get bind param by idx", K(ret), K(col_idx));
  } else if (OB_ISNULL(bind_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get bind param by idx", K(ret), K(col_idx), K(stmt_param_count_));
  } else if (OB_FAIL(ObBindParamEncode::encode_map_[obj_type](col_idx,
                                                              is_output_param,
                                                              *tz_info,
                                                              param,
                                                              *bind_param,
                                                              allocator))) {
    LOG_WARN("fail to encode param", K(ret));
  } else if (OB_FAIL(param_.bind_param(*bind_param))) {
    LOG_WARN("failed to bind param", K(ret), KPC(bind_param));
  }
  return ret;
}

int ObMySQLProcStatement::bind_proc_param(ObIAllocator &allocator,
                                          ParamStore &params,
                                          const share::schema::ObRoutineInfo &routine_info,
                                          common::ObIArray<int64_t> &basic_out_param,
                                          const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
    ObObjParam &param = params.at(i);
    const share::schema::ObRoutineParam *r_param = routine_info.get_routine_params().at(i);
    bool is_output = false;
    if (OB_ISNULL(r_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param is null", K(ret), K(i));
    } else {
      is_output = r_param->is_out_sp_param() || r_param->is_inout_sp_param();
      if (is_output && OB_FAIL(basic_out_param.push_back(i))) {
        LOG_WARN("push back failed", K(ret), K(i));
      } else if (OB_FAIL(bind_param(i, is_output, tz_info, param, routine_info, allocator))) {
        LOG_WARN("failed to bind param", K(ret));
      }
    }
  }
  return ret;
}

int ObMySQLProcStatement::convert_proc_output_param_result(const ObTimeZoneInfo &tz_info,
                                                           const ObBindParam &bind_param,
                                                           ObObjParam &param,
                                                           const share::schema::ObRoutineInfo &routine_info,
                                                           ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObObjType obj_type = ObNullType;
  if (OB_FAIL(get_ob_type(obj_type, static_cast<obmysql::EMySQLFieldType>(bind_param.buffer_type_)))) {
    LOG_WARN("fail to get ob type", K(ret), K(bind_param));
  } else if (OB_FAIL(ObBindParamDecode::decode_map_[obj_type](bind_param.buffer_type_,
                                                              tz_info,
                                                              bind_param,
                                                              param,
                                                              allocator))) {
    LOG_WARN("failed to decode param", K(ret));
  }
  return ret;
}

int ObMySQLProcStatement::process_proc_output_params(ObIAllocator &allocator,
                                                     ParamStore &params,
                                                     const share::schema::ObRoutineInfo &routine_info,
                                                     common::ObIArray<int64_t> &basic_out_param,
                                                     const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  const int64_t params_count = params.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < basic_out_param.count(); i++) {
    const int64_t col_idx = basic_out_param.at(i);
    ObBindParam *bind_param = nullptr;
    if (col_idx >= params_count || col_idx > stmt_param_count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("idx out of range", K(ret), K(col_idx), K(params_count));
    } else if (OB_FAIL(get_bind_param_by_idx(col_idx, bind_param))) {
      LOG_WARN("fail to get bind param by idx", K(ret), K(col_idx), K_(stmt_param_count));
    } else if (OB_FAIL(convert_proc_output_param_result(*tz_info, *bind_param, params.at(col_idx), routine_info, allocator))) {
      LOG_WARN("fail to convert proc output param result", K(ret));
    }
    LOG_INFO("end process param", K(i), KPC(bind_param), K(ret));
  }
  return ret;
}

int ObMySQLProcStatement::execute_proc(ObIAllocator &allocator,
                                       ParamStore &params,
                                       const share::schema::ObRoutineInfo &routine_info,
                                       const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<int64_t, 8> basic_out_param;
  if (OB_ISNULL(tz_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tz info is null", K(ret));
  } else if (OB_FAIL(bind_proc_param(allocator, params, routine_info, basic_out_param, tz_info))) {
    LOG_WARN("failed to bind proc param", K(ret));
  } else if (OB_FAIL(param_.bind_param())) {
    LOG_WARN("failed to bind prepared input param", "info", mysql_error(conn_->get_handler()), K(ret));
  } else if (0 != mysql_stmt_execute(stmt_)) {
    ret = -mysql_stmt_errno(stmt_);
    LOG_WARN("fail to execute stmt", "info", mysql_stmt_error(stmt_), "info", mysql_error(conn_->get_handler()), K(ret));
  } else if (OB_FAIL(process_proc_output_params(allocator, params, routine_info, basic_out_param, tz_info))) {
    LOG_WARN("fail to process proc output params", K(ret));
  }
  return ret;
};

} // end namespace sqlcient
} // end namespace common
} // end namespace oceanbase
