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
#include "lib/mysqlclient/ob_dblink_error_trans.h"

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
  if (OB_FAIL(encode_int(col_idx, is_output_param, tz_info, param, bind_param, allocator, buffer_type))) {
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
  if (OB_FAIL(encode_float(col_idx, is_output_param, tz_info, param, bind_param, allocator, buffer_type))) {
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
  if (OB_FAIL(encode_double(col_idx, is_output_param, tz_info, param, bind_param, allocator, buffer_type))) {
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
  if (OB_FAIL(encode_number(col_idx, is_output_param, tz_info, param, bind_param, allocator, buffer_type))) {
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
  bind_param.col_idx_ = col_idx;
  bind_param.buffer_type_ = buffer_type;
  const ObTimeZoneInfo *tmp_tz = &tz_info;
  if (obj_type == ObObjType::ObDateTimeType) {
    tmp_tz = NULL;
  }
  if (OB_ISNULL(tm = reinterpret_cast<MYSQL_TIME *>(allocator.alloc(sizeof(MYSQL_TIME))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(param.get_datetime(), tmp_tz, ob_time))) {
    LOG_WARN("convert usec ", K(ret));
  } else {
    MEMSET(tm, 0, sizeof(MYSQL_TIME));
    tm->year = ob_time.parts_[DT_YEAR];
    tm->month = ob_time.parts_[DT_MON];
    tm->day = ob_time.parts_[DT_MDAY];
    tm->hour = ob_time.parts_[DT_HOUR];
    tm->minute = ob_time.parts_[DT_MIN];
    tm->second = ob_time.parts_[DT_SEC];
    tm->second_part = ob_time.parts_[DT_USEC];
    tm->neg = DT_MODE_NEG & ob_time.mode_;
    bind_param.buffer_ = tm;
    bind_param.buffer_len_ = sizeof(MYSQL_TIME);
  }
  return ret;
}

int ObBindParamEncode::encode_timestamp(ENCODE_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
#else
  const ObObjType obj_type = param.get_type();
  const bool is_tz = (ObTimestampTZType == obj_type);
  ORACLE_TIME *ora_time = NULL;
  int64_t len = sizeof(ORACLE_TIME);
  bind_param.col_idx_ = col_idx;
  bind_param.buffer_type_ = buffer_type;
  const ObOTimestampData &ot_data = param.get_otimestamp_value();
  ObTime ob_time(DT_TYPE_ORACLE_TIMESTAMP);
  if (OB_FAIL(ObTimeConverter::otimestamp_to_ob_time(obj_type, ot_data, &tz_info,
                                                     ob_time, false))) {
    LOG_WARN("failed to convert timestamp to ob time", K(ret));
  } else if (!ObTimeConverter::valid_oracle_year(ob_time)) {
    ret = OB_ERR_DATETIME_INTERVAL_INTERNAL_ERROR;
    LOG_WARN("invalid oracle timestamp", K(ret), K(ob_time));
  } else if (OB_ISNULL(ora_time = reinterpret_cast<ORACLE_TIME *>(allocator.alloc(len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    MEMSET(ora_time, 0, sizeof(ORACLE_TIME));
    const int32_t unsigned_year = ob_time.parts_[DT_YEAR] >= 0 ? ob_time.parts_[DT_YEAR] : (0 - ob_time.parts_[DT_YEAR]);
    int32_t century = static_cast<int32_t>(unsigned_year / YEARS_PER_CENTURY * (ob_time.parts_[DT_YEAR] >= 0 ? 1 : -1));
    int32_t decade = static_cast<int32_t>(unsigned_year % YEARS_PER_CENTURY);
    if (0 == century) {
      decade *= static_cast<int32_t>(ob_time.parts_[DT_YEAR] >= 0 ? 1 : -1);
    }
    ora_time->century = century;
    ora_time->year = decade;
    ora_time->month = ob_time.parts_[DT_MON];
    ora_time->day = ob_time.parts_[DT_MDAY];
    ora_time->hour = ob_time.parts_[DT_HOUR];
    ora_time->minute = ob_time.parts_[DT_MIN];
    ora_time->second = ob_time.parts_[DT_SEC];
    ora_time->second_part = ob_time.parts_[DT_USEC];
    ora_time->scale = param.get_scale();
    if (is_tz) {
      if (!ob_time.get_tz_name_str().empty()) {
        ora_time->tz_name = ob_time.get_tz_name_str().ptr();
      }
      if (!ob_time.get_tzd_abbr_str().empty()) {
        ora_time->tz_abbr = ob_time.get_tzd_abbr_str().ptr();
      }
      const int32_t unsigned_offset = (ob_time.parts_[DT_OFFSET_MIN] >= 0 ? ob_time.parts_[DT_OFFSET_MIN] : (0 - ob_time.parts_[DT_OFFSET_MIN]));
      int32_t offset_hour = static_cast<int32_t>(unsigned_offset / MINS_PER_HOUR * (ob_time.parts_[DT_OFFSET_MIN] >= 0 ? 1 : -1));
      int32_t offset_minute = static_cast<int32_t>(unsigned_offset % MINS_PER_HOUR);
      if (0 == offset_hour) {
        offset_minute *= static_cast<int32_t>(ob_time.parts_[DT_OFFSET_MIN] >= 0 ? 1 : -1);
      }
      ora_time->offset_hour = offset_hour;
      ora_time->offset_minute = offset_minute;
    }

    bind_param.buffer_ = ora_time;
    bind_param.buffer_len_ = len;
    bind_param.length_ = len;
  }
#endif
  return ret;
}

int ObBindParamEncode::encode_date(ENCODE_FUNC_ARG_DECL)
{
  UNUSEDx(is_output_param);
  int ret = OB_SUCCESS;
  ObTime ob_time;
  MYSQL_TIME *tm = nullptr;
  const ObObjType obj_type = param.get_type();
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
  ObString val = param.get_string();
  const ObObjType obj_type = param.get_type();
  bind_param.col_idx_ = col_idx;
  bind_param.buffer_type_ = buffer_type;
  bind_param.buffer_ = val.ptr();
  bind_param.buffer_len_ = val.length();
  bind_param.length_ = val.length();
  return ret;
}

int ObBindParamEncode::encode_number_float(ENCODE_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  void *buf = NULL;
  int64_t buf_len = 41;
  if (OB_ISNULL(buf = allocator.alloc(buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", K(ret));
  } else if (OB_FAIL(param.get_number().format_v1((char *)buf, buf_len, pos, param.get_scale()))) {
    LOG_WARN("fail to format float", K(ret));
  } else {
    bind_param.col_idx_ = col_idx;
    bind_param.buffer_type_ = buffer_type;
    bind_param.buffer_ = buf;
    bind_param.buffer_len_ = buf_len;
    bind_param.length_ = pos;
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
  ObBindParamEncode::encode_timestamp,               // ObTimestampTZType
  ObBindParamEncode::encode_timestamp,               // ObTimestampLTZType
  ObBindParamEncode::encode_timestamp,               // ObTimestampNanoType
  ObBindParamEncode::encode_not_supported,           // ObRawType
  ObBindParamEncode::encode_not_supported,           // ObIntervalYMType
  ObBindParamEncode::encode_not_supported,           // ObIntervalDSType
  ObBindParamEncode::encode_number_float,            // ObNumberFloatType
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

int ObBindParamDecode::decode_timestamp(DECODE_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
#else
  const bool is_tz = (MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE == field_type);
  ObOTimestampData ot_data;
  ot_data.reset();
  ORACLE_TIME *ora_time = reinterpret_cast<ORACLE_TIME *>(bind_param.buffer_);
  ObTime ob_time(DT_TYPE_ORACLE_TIMESTAMP);
  if (OB_ISNULL(ora_time)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("oracle time is NULL", K(ret));
  } else {
    int32_t century = ora_time->century;
    int32_t year = ora_time->year;
    ob_time.parts_[DT_YEAR] = (int32_t)(century > 0 ? (century * YEARS_PER_CENTURY + year)
                                        : (0 - (0 - century * YEARS_PER_CENTURY + year)));
    ob_time.parts_[DT_MON] = ora_time->month;
    ob_time.parts_[DT_MDAY] = ora_time->day;
    ob_time.parts_[DT_HOUR] = ora_time->hour;
    ob_time.parts_[DT_MIN] = ora_time->minute;
    ob_time.parts_[DT_SEC] = ora_time->second;
    ob_time.parts_[DT_USEC] = ora_time->second_part;
    ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
    ObTimeConvertCtx ctx(&tz_info, ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT, true);
    if (is_tz) {
      ob_time.mode_ |= DT_TYPE_TIMEZONE;
      if (OB_NOT_NULL(ora_time->tz_name)) {
        if (OB_FAIL(ob_time.set_tz_name(ObString(ora_time->tz_name)))) {
          LOG_WARN("fail to set_tz_name", K(ret), K(ObString(ora_time->tz_name)));
        } else if (OB_NOT_NULL(ora_time->tz_abbr)
                   && OB_FAIL(ob_time.set_tzd_abbr(ObString(ora_time->tz_abbr)))) {
          LOG_WARN("fail to set_tzd_abbr", K(ret), K(ObString(ora_time->tz_abbr)));
        } else if (OB_FAIL(ObTimeConverter::str_to_tz_offset(ctx, ob_time))) {
          LOG_WARN("fail to convert string to tz_offset", K(ret));
        }
      } else {
        int32_t offset_hour = ora_time->offset_hour;
        int32_t offset_min = ora_time->offset_minute;
        ob_time.parts_[DT_OFFSET_MIN] = static_cast<int32_t>(offset_hour >= 0 ? (offset_hour * MINS_PER_HOUR + offset_min) : (0 - (0 - offset_hour * MINS_PER_HOUR + offset_min)));
        ob_time.is_tz_name_valid_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      ObObjType obj_type = ObTimestampNanoType;
      if (field_type == MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE) {
        obj_type = ObTimestampTZType;
      } else if (field_type == MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
        obj_type = ObTimestampLTZType;
      }
      if (OB_FAIL(ObTimeConverter::ob_time_to_utc(obj_type, ctx, ob_time))) {
        LOG_WARN("failed to convert ob_time to utc", K(ret));
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_otimestamp(ob_time, ot_data))) {
        LOG_WARN("failed to ob_time to otimestamp", K(ret));
      } else {
        param.set_otimestamp_value(param.get_type(), ot_data);
      }
    }
  }
#endif
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
        break;
      case MYSQL_TYPE_TINY_BLOB:
        param.set_lob_value(ObTinyTextType, dst.ptr(), dst.length());
        break;
      case MYSQL_TYPE_BLOB:
        param.set_lob_value(ObTextType, dst.ptr(), dst.length());
        break;
      case MYSQL_TYPE_MEDIUM_BLOB:
        param.set_lob_value(ObMediumTextType, dst.ptr(), dst.length());
        break;
      case MYSQL_TYPE_LONG_BLOB:
        param.set_lob_value(ObLongTextType, dst.ptr(), dst.length());
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown type", K(ret), K(field_type));
        break;
    }
  }
  return ret;
}

int ObBindParamDecode::decode_number_float(DECODE_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ObString value(bind_param.length_, (char *)bind_param.buffer_);
  number::ObNumber nb;
  if (OB_FAIL(nb.from(value.ptr(), value.length(), allocator))) {
    LOG_WARN("fail to decode number float", K(ret), K(value));
  } else {
    param.set_number_float(nb);
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
  ObBindParamDecode::decode_timestamp,               // ObTimestampTZType
  ObBindParamDecode::decode_timestamp,               // ObTimestampLTZType
  ObBindParamDecode::decode_timestamp,               // ObTimestampNanoType
  ObBindParamDecode::decode_not_supported,           // ObRawType
  ObBindParamDecode::decode_not_supported,           // ObIntervalYMType
  ObBindParamDecode::decode_not_supported,           // ObIntervalDSType
  ObBindParamDecode::decode_number_float,            // ObNumberFloatType
  ObBindParamDecode::decode_not_supported,           // ObNVarchar2Type
  ObBindParamDecode::decode_not_supported,           // ObNCharType
  ObBindParamDecode::decode_not_supported,           // ObURowIDType
  ObBindParamDecode::decode_not_supported,           // ObLobType
  ObBindParamDecode::decode_not_supported,           // ObJsonType
  ObBindParamDecode::decode_not_supported,           // ObGeometryType
  ObBindParamDecode::decode_not_supported            // ObMaxType
};

void ObBindParam::assign(const ObBindParam &other)
{
  col_idx_ = other.col_idx_;
  buffer_type_ = other.buffer_type_;
  buffer_ = other.buffer_;
  buffer_len_ = other.buffer_len_;
  length_ = other.length_;
  is_unsigned_ = other.is_unsigned_;
  is_null_ = other.is_null_;
  array_buffer_ = other.array_buffer_;
  ele_size_ = other.ele_size_;
  max_array_size_ = other.max_array_size_;
  out_valid_array_size_ = other.out_valid_array_size_;
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

ObIAllocator *ObMySQLPreparedStatement::get_allocator()
{
  return alloc_;
}

void ObMySQLPreparedStatement::set_allocator(ObIAllocator *alloc)
{
  alloc_ = alloc;
  result_.alloc_ = alloc;
  param_.alloc_ = alloc;
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

int ObMySQLPreparedStatement::init(ObMySQLConnection &conn, const char *sql, int64_t param_count)
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
  param_.close();
  result_.close();
  return ret;
}

int ObMySQLPreparedStatement::bind_param(ObBindParam &bind_param)
{
  int ret = OB_SUCCESS;
  if (bind_param.col_idx_ >= stmt_param_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index", K(ret), K(bind_param), K(stmt_param_count_));
  } else if (OB_FAIL(param_.bind_param(bind_param))) {
    LOG_WARN("fail to bind param", K(ret), K(bind_param));
  }
  return ret;
}

int ObMySQLPreparedStatement::bind_result(ObBindParam &bind_param)
{
  int ret = OB_SUCCESS;
  if (bind_param.col_idx_ >= result_column_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index", K(ret), K(bind_param), K(result_column_count_));
  } else if (OB_FAIL(result_.bind_result(bind_param))) {
    LOG_WARN("fail to bind param", K(ret), K(bind_param));
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
                                     const int64_t param_idx,
                                     const bool is_output_param,
                                     const ObTimeZoneInfo *tz_info,
                                     ObObj &param,
                                     const share::schema::ObRoutineInfo &routine_info,
                                     ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObBindParam *bind_param = nullptr;
  const ObObjType obj_type = param.get_type();
  const share::schema::ObRoutineParam *routine_param = NULL;
  if (param_idx >= routine_info.get_routine_params().count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("col_idx invalid", K(ret), K(param_idx));
  } else if (FALSE_IT(routine_param = routine_info.get_routine_params().at(param_idx))) {
  } else if (OB_ISNULL(routine_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("routine_param is NULL", K(ret), K(col_idx));
  } else {
    enum_field_types buffer_type = MAX_NO_FIELD_TYPES;
    if (param.is_null()) {
      buffer_type = static_cast<enum_field_types>(ob_type_to_mysql_type[routine_param->get_param_type().get_obj_type()]);
    } else {
      buffer_type = static_cast<enum_field_types>(ob_type_to_mysql_type[param.get_type()]);
    }
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
                                                                allocator,
                                                                buffer_type))) {
      LOG_WARN("fail to encode param", K(ret));
    } else if (OB_FAIL(param_.bind_param(*bind_param))) {
      LOG_WARN("failed to bind param", K(ret), KPC(bind_param));
    }
  }
  return ret;
}

int ObMySQLProcStatement::bind_basic_type_by_pos(uint64_t position,
                                                 void *param_buffer,
                                                 int64_t param_size,
                                                 int32_t datatype,
                                                 int32_t &indicator,
                                                 bool is_out_param)
{
  int ret = OB_SUCCESS;
  ObBindParam *bind_param = NULL;
  if (OB_FAIL(get_bind_param_by_idx(position, bind_param))) {
    LOG_WARN("fail to get bind param by idx", K(ret), K(position));
  } else if (OB_ISNULL(bind_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get bind param by idx", K(ret), K(position), K(stmt_param_count_));
  } else {
    enum_field_types buffer_type = static_cast<enum_field_types>(ob_type_to_mysql_type[(datatype)]);
    bind_param->col_idx_ = position;
    bind_param->buffer_type_ = buffer_type;
    bind_param->buffer_ = param_buffer;
    bind_param->buffer_len_ = param_size;
    bind_param->is_null_ = 0;
    bind_param->length_ = param_size;
    if (OB_FAIL(in_out_map_.push_back(is_out_param))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(param_.bind_param(*bind_param))) {
      LOG_WARN("failed tp bind param", K(ret), KPC(bind_param));
    } else if (stmt_param_count_ == position + 1) {
      if (OB_FAIL(param_.bind_param())) {
        LOG_WARN("failed to bind param", K(ret),
                 "info", mysql_stmt_error(stmt_), "info", mysql_error(conn_->get_handler()));
      }
    }
  }
  return ret;
}

int ObMySQLProcStatement::bind_array_type_by_pos(uint64_t position,
                                                 void *array,
                                                 int32_t *indicators,
                                                 int64_t ele_size,
                                                 int32_t ele_datatype,
                                                 uint64_t array_size,
                                                 uint32_t *out_valid_array_size)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
#else
  ObBindParam *bind_param = NULL;
  if (OB_FAIL(get_bind_param_by_idx(position, bind_param))) {
    LOG_WARN("fail to get bind param by idx", K(ret), K(position));
  } else if (OB_ISNULL(bind_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get bind param by idx", K(ret), K(position), K(stmt_param_count_));
  } else {
    enum_field_types ele_type = static_cast<enum_field_types>(ob_type_to_mysql_type[(ele_datatype)]);
    MYSQL_COMPLEX_BIND_PLARRAY *my_array = NULL;
    // input param out_valid_array_size is 0 by default
    int64_t array_elem_size = convert_type_to_complex(ele_type);
    // libobclient has a bug, even if the length of the input param array is 0,
    // still need to apply for a memory to prevent observer core.
    int64_t buf_len = sizeof(MYSQL_COMPLEX_BIND_PLARRAY) + array_elem_size;
    char *buf = NULL;
    if (OB_ISNULL(buf = (char *)alloc_->alloc(buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else {
      MEMSET(buf, 0, buf_len);
      bind_param->col_idx_ = position;
      bind_param->buffer_type_ = MYSQL_TYPE_OBJECT;
      bind_param->buffer_ = buf;
      bind_param->buffer_len_ = buf_len;
      bind_param->is_null_ = 0;
      bind_param->array_buffer_ = array;
      bind_param->ele_size_ = ele_size;
      bind_param->max_array_size_ = array_size;
      bind_param->out_valid_array_size_ = out_valid_array_size;

      my_array = (MYSQL_COMPLEX_BIND_PLARRAY *)buf;
      my_array->buffer_type = MYSQL_TYPE_PLARRAY;
      my_array->buffer = buf + sizeof(MYSQL_COMPLEX_BIND_PLARRAY);
      my_array->is_null = 0;
      my_array->type_name = NULL;
      my_array->owner_name = NULL;
      my_array->elem_type = ele_type;
      my_array->length = 0;
      my_array->maxrarr_len = array_size;
      if (OB_FAIL(in_out_map_.push_back(true))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(param_.bind_param(*bind_param))) {
        LOG_WARN("failed tp bind param", K(ret), KPC(bind_param));
      } else if (stmt_param_count_ == position + 1
                 && OB_FAIL(param_.bind_param())) {
        LOG_WARN("failed to bind param", K(ret), "info", mysql_stmt_error(stmt_));
      }
    }
  }
#endif
  return ret;
}

int ObMySQLProcStatement::bind_proc_param(ObIAllocator &allocator,
                                          ParamStore &params,
                                          const share::schema::ObRoutineInfo &routine_info,
                                          common::ObIArray<std::pair<int64_t, int64_t>> &basic_out_param,
                                          const ObTimeZoneInfo *tz_info,
                                          ObObj *result,
                                          bool is_sql)
{
  int ret = OB_SUCCESS;
  int64_t start_idx = routine_info.get_param_start_idx();
  if (routine_info.is_function() && !is_sql) {
    if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is NULL", K(ret));
    } else if (OB_FAIL(basic_out_param.push_back(std::make_pair(0, -1)))) {
      LOG_WARN("push back failed", K(ret));
    } else if (OB_FAIL(bind_param(0, 0, true, tz_info, *result, routine_info, allocator))) {
      LOG_WARN("failed to bind param", K(ret));
    }
  }
  int64_t start_pos = (routine_info.is_function() ? (is_sql ? 0 : 1) : 0);
  int64_t skip_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
    ObObjParam &param = params.at(i);
    const share::schema::ObRoutineParam *r_param = routine_info.get_routine_params().at(i + start_idx);
    bool is_output = false;
    if (OB_ISNULL(r_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param is null", K(ret), K(i));
    } else if (param.is_pl_mock_default_param()) {
      skip_cnt++;
    } else {
      is_output = r_param->is_out_sp_param() || r_param->is_inout_sp_param();
      if (is_output && OB_FAIL(basic_out_param.push_back(std::make_pair(i + start_pos - skip_cnt, i)))) {
        LOG_WARN("push back failed", K(ret), K(i));
      } else if (OB_FAIL(bind_param(i + start_pos - skip_cnt, i + (routine_info.is_function() ? 1 : 0),
                                    is_output, tz_info, param, routine_info, allocator))) {
        LOG_WARN("failed to bind param", K(ret));
      }
    }
  }
  if (routine_info.is_function() && is_sql) {
    if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is NULL", K(ret));
    } else if (OB_FAIL(basic_out_param.push_back(std::make_pair(params.count() - skip_cnt, -1)))) {
      LOG_WARN("push back failed", K(ret));
    } else if (OB_FAIL(bind_param(params.count() - skip_cnt, 0, true, tz_info, *result, routine_info, allocator))) {
      LOG_WARN("failed to bind param", K(ret));
    }
  }
  return ret;
}

int ObMySQLProcStatement::convert_proc_output_param_result(int64_t out_param_idx,
                                                           const ObTimeZoneInfo &tz_info,
                                                           const ObBindParam &bind_param,
                                                           ObObj *param,
                                                           const share::schema::ObRoutineInfo &routine_info,
                                                           ObIAllocator &allocator,
                                                           bool is_return_value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param is NULL", K(ret));
  } else {
    if (bind_param.is_null_) {
      param->set_null();
    } else {
      ObObjType obj_type = ObNullType;
      const share::schema::ObRoutineParam *routine_param = routine_info.get_routine_params().at(out_param_idx);
      if (OB_ISNULL(routine_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("routine_param is NULL", K(ret), K(out_param_idx));
      } else {
        const ObDataType &data_type = routine_param->get_param_type();
        if (param->is_null()) {
          param->set_meta_type(data_type.get_meta_type());
          if (!is_return_value) {
            ObObjParam *obj_param = static_cast<ObObjParam *>(param);
            if (OB_ISNULL(obj_param)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("obj_param is NULL", K(ret));
            } else {
              obj_param->set_param_meta();
              obj_param->set_accuracy(data_type.get_accuracy());
            }
          }
        }
        if (FAILEDx(get_ob_type(obj_type, static_cast<obmysql::EMySQLFieldType>(bind_param.buffer_type_)))) {
          LOG_WARN("fail to get ob type", K(ret), K(bind_param));
        } else if (OB_FAIL(ObBindParamDecode::decode_map_[obj_type](bind_param.buffer_type_,
                                                                    tz_info,
                                                                    bind_param,
                                                                    *param,
                                                                    allocator))) {
          LOG_WARN("failed to decode param", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMySQLProcStatement::process_proc_output_params(ObIAllocator &allocator,
                                                     ParamStore &params,
                                                     const share::schema::ObRoutineInfo &routine_info,
                                                     common::ObIArray<std::pair<int64_t, int64_t>> &basic_out_param,
                                                     const ObTimeZoneInfo *tz_info,
                                                     ObObj *result,
                                                     bool is_sql)
{
  int ret = OB_SUCCESS;
  if (basic_out_param.count() > 0) {
    const int64_t params_count = params.count();
    if (OB_FAIL(result_.init())) {
      LOG_WARN("failed to init result_", K(ret));
    } else if (FALSE_IT(result_column_count_ = result_.get_result_column_count())) {
    } else if (result_column_count_ != basic_out_param.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("only support basic type out param", K(ret), K(result_column_count_), K(basic_out_param.count()));
    } else if (result_column_count_ > 0
               && OB_FAIL(alloc_bind_params(result_column_count_, result_params_))) {
      LOG_WARN("failed to alloc bind params", K(ret), K(result_column_count_));
    } else if (result_column_count_ > 0) {
      ObBindParam *in_param = NULL;
      ObBindParam *out_param = NULL;
      MYSQL_BIND *mysql_bind = result_.get_bind();
      int64_t out_idx = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < basic_out_param.count(); i++) {
        if (OB_FAIL(get_bind_param_by_idx(basic_out_param.at(i).first, in_param))) {
          LOG_WARN("failed to get param", K(ret), K(i));
        } else if (OB_ISNULL(in_param)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("in_param is NULL", K(ret), K(i));
        } else if (i >= result_column_count_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("out_idx is error", K(ret), K(out_idx), K(result_column_count_));
        } else if (OB_FAIL(get_bind_result_param_by_idx(i, out_param))) {
          LOG_WARN("fail to get bind result param by idx", K(ret), K(out_idx));
        } else if (OB_ISNULL(out_param) || OB_ISNULL(mysql_bind)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("out_param is NULL", K(ret), K(out_idx), K(mysql_bind));
        } else {
          out_param->assign(*in_param);
          mysql_bind[i].buffer_type = out_param->buffer_type_;
          mysql_bind[i].buffer = out_param->buffer_;
          mysql_bind[i].buffer_length = out_param->buffer_len_;
          mysql_bind[i].length = &out_param->length_;
          mysql_bind[i].error = &mysql_bind[i].error_value;
          mysql_bind[i].is_null = &out_param->is_null_;
          if (OB_ISNULL(mysql_bind[i].buffer)) {
            void *tmp_buf = NULL;
            int64_t tmp_buf_len = 0;
            if (MYSQL_TYPE_DATETIME == out_param->buffer_type_) {
              tmp_buf_len = sizeof(MYSQL_TIME);
            } else if (MYSQL_TYPE_FLOAT == out_param->buffer_type_) {
              tmp_buf_len = sizeof(float);
            } else if (MYSQL_TYPE_DOUBLE ==  out_param->buffer_type_) {
              tmp_buf_len = sizeof(double);
            } else if (MYSQL_TYPE_LONG == out_param->buffer_type_) {
              tmp_buf_len = sizeof(int64_t);
            }
            if (tmp_buf_len > 0) {
              if (OB_ISNULL(tmp_buf = allocator.alloc(tmp_buf_len))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed to alloc memory", K(ret));
              } else {
                mysql_bind[i].buffer = tmp_buf;
                mysql_bind[i].buffer_length = tmp_buf_len;
                out_param->buffer_ = tmp_buf;
                out_param->buffer_len_ =tmp_buf_len;
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        int tmp_ret = 0;
        if (0 != (tmp_ret = mysql_stmt_bind_result(stmt_, mysql_bind))) {
          ret = -mysql_stmt_errno(stmt_);
          LOG_WARN("failed to bind out param", K(ret), "info", mysql_stmt_error(stmt_));
        } else {
          tmp_ret = mysql_stmt_fetch(stmt_);
          if (MYSQL_DATA_TRUNCATED == tmp_ret) {
            if (OB_FAIL(handle_data_truncated(allocator))) {
              LOG_WARN("failed to handler data", K(ret));
            }
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < basic_out_param.count(); i++) {
            const int64_t col_idx = basic_out_param.at(i).first;
            ObBindParam *bind_param = nullptr;
            if (OB_FAIL(get_bind_result_param_by_idx(i, bind_param))) {
              LOG_WARN("fail to get bind param by idx", K(ret), K(col_idx), K_(stmt_param_count));
            } else if (OB_ISNULL(bind_param)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("bind_param is NULL", K(ret));
            } else {
              ObObj *obj = NULL;
              bool is_return_value = false;
              if (routine_info.is_function()
                  && ((is_sql && i == result_column_count_ - 1)
                      ||(!is_sql && (col_idx == 0)))) {
                obj = result;
                is_return_value = true;
              } else {
                obj = &params.at(basic_out_param.at(i).second);
              }
              if (OB_ISNULL(obj)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("obj is NULL", K(ret));
              } else if (OB_FAIL(convert_proc_output_param_result(col_idx, *tz_info, *bind_param, obj,
                                                                  routine_info, allocator, is_return_value))) {
                LOG_WARN("fail to convert proc output param result", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMySQLProcStatement::init(ObMySQLConnection &conn,
                               const char *sql,
                               int64_t param_count)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
#else
  conn_ = &conn;
  stmt_param_count_ = param_count;
  in_out_map_.reset();
  if (OB_ISNULL(proc_ = sql)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql", KCSTRING(sql), K(ret));
  } else if (OB_ISNULL(conn_) || OB_ISNULL(conn_->get_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn_ is NULL", K(ret));
  } else if (FALSE_IT(conn_->get_handler()->field_count = 0)) {
    // After executing the previous statement, libObClient did not set the value of `mysql->filed_count` to 0,
    // which may cause coredump. Set it manually here.
  } else if (OB_ISNULL(stmt_ = mysql_stmt_init(conn_->get_handler()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to init stmt", K(ret));
  } else if (0 != mysql_stmt_prepare_v2(stmt_, sql, STRLEN(sql), &stmt_param_count_)) {
    ret = -mysql_errno(conn_->get_handler());
    LOG_WARN("fail to prepare stmt", "info", mysql_error(conn_->get_handler()), K(ret));
  } else if (OB_FAIL(param_.init())) {
    LOG_WARN("fail to init prepared result", K(ret));
  } else if (stmt_param_count_ > 0 && OB_FAIL(alloc_bind_params(stmt_param_count_, bind_params_))) {
    LOG_WARN("fail to alloc stmt bind params", K(ret));
  }
#endif
  return ret;
}

int ObMySQLProcStatement::execute_proc(ObIAllocator &allocator,
                                       ParamStore &params,
                                       const share::schema::ObRoutineInfo &routine_info,
                                       const ObTimeZoneInfo *tz_info,
                                       ObObj *result,
                                       bool is_sql)
{
  int ret = OB_SUCCESS;
  // pair.first is: out param position in @this.bind_params_
  // pair.second is: out param position in @params, if the routine is a function, the values is -1
  common::ObSEArray<std::pair<int64_t, int64_t>, 8> basic_out_param;
  void* execute_extend_arg = NULL;
  if (OB_ISNULL(tz_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tz info is null", K(ret));
  } else if (OB_FAIL(bind_proc_param(allocator, params, routine_info, basic_out_param, tz_info, result, is_sql))) {
    LOG_WARN("failed to bind proc param", K(ret));
  } else if (OB_FAIL(param_.bind_param())) {
    LOG_WARN("failed to bind prepared input param", "info", mysql_error(conn_->get_handler()), K(ret));
  } else if (OB_FAIL(execute_stmt_v2_interface())) {
    LOG_WARN("failed to execute PL", K(ret));
  } else if (OB_FAIL(process_proc_output_params(allocator, params, routine_info, basic_out_param,
                                                tz_info, result, is_sql))) {
    LOG_WARN("fail to process proc output params", K(ret));
  }
  return ret;
}

int ObMySQLProcStatement::execute_proc()
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
#else
  void* execute_extend_arg = NULL;
  MYSQL_RES *res = NULL;
  uint64_t ps_stmt_checksum = ob_crc64(proc_, STRLEN(proc_));
  LOG_INFO("execute v2 info", K(ret), K(ps_stmt_checksum), K(STRLEN(proc_)), K(proc_));
  if (OB_FAIL(execute_stmt_v2_interface())) {
    LOG_WARN("failed to execute PL", K(ret));
  } else if (OB_ISNULL(res = mysql_stmt_result_metadata(stmt_))) {
    ret = -mysql_stmt_errno(stmt_);
    char errmsg[256] = {0};
    const char *srcmsg = mysql_stmt_error(stmt_);
    MEMCPY(errmsg, srcmsg, MIN(255, STRLEN(srcmsg)));
    TRANSLATE_CLIENT_ERR(ret, errmsg);
    LOG_WARN("failed to execute proc", K(ret), "info", mysql_stmt_error(stmt_));
  } else if (OB_FAIL(result_.init())) {
    LOG_WARN("fail to init prepared result", K(ret));
  } else if (FALSE_IT(result_column_count_ = res->field_count)) {
  } else if (result_column_count_ > 0 && OB_FAIL(alloc_bind_params(result_column_count_, result_params_))) {
    LOG_WARN("fail to alloc result bind params", K(ret), K(result_column_count_));
  } else if (result_column_count_ > 0) {
    ObBindParam *in_param = NULL;
    ObBindParam *out_param = NULL;
    MYSQL_BIND *mysql_bind = result_.get_bind();
    int64_t out_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt_param_count_; i++) {
      if (in_out_map_.at(i)) {
        if (OB_FAIL(get_bind_param_by_idx(i, in_param))) {
          LOG_WARN("fail to get bind param by idx", K(ret), K(i));
        } else if (OB_ISNULL(in_param)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("in_param is NULL", K(ret), K(i), K(stmt_param_count_));
        } else if (out_idx >= result_column_count_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("out_idx is error", K(ret), K(out_idx), K(result_column_count_), K(stmt_param_count_));
        } else if (OB_FAIL(get_bind_result_param_by_idx(out_idx, out_param))) {
          LOG_WARN("fail to get bind result param by idx", K(ret), K(out_idx));
        } else if (OB_ISNULL(out_param) || OB_ISNULL(mysql_bind)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("out_param is NULL", K(ret), K(out_idx), K(stmt_param_count_), K(mysql_bind));
        } else {
          out_param->assign(*in_param);
          mysql_bind[out_idx].buffer_type = in_param->buffer_type_;
          mysql_bind[out_idx].buffer = in_param->buffer_;
          mysql_bind[out_idx].buffer_length = in_param->buffer_len_;
          mysql_bind[out_idx].length = &in_param->length_;
          mysql_bind[out_idx].error = &mysql_bind[out_idx].error_value;
          mysql_bind[out_idx].is_null = &in_param->is_null_;
          out_idx++;
        }
      }
    }
    if (OB_SUCC(ret)) {
      int tmp_ret = 0;
      if (0 != (tmp_ret = mysql_stmt_bind_result(stmt_, mysql_bind))) {
        ret = -mysql_stmt_errno(stmt_);
        LOG_WARN("failed to bind out param", K(ret), "info", mysql_stmt_error(stmt_));
      } else if (0 != (tmp_ret = mysql_stmt_fetch(stmt_))) {
        if (MYSQL_DATA_TRUNCATED != tmp_ret) {
          ret = -mysql_stmt_errno(stmt_);
          LOG_WARN("failed to fetch", K(ret), K(tmp_ret),
                   "info", mysql_stmt_error(stmt_), "info", mysql_error(conn_->get_handler()));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < result_column_count_; i++) {
            MYSQL_BIND &res_bind = mysql_bind[i];
            void *res_buffer = NULL;
            if (OB_ISNULL(res_buffer = alloc_->alloc(*mysql_bind[i].length))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("alloc memory failed", K(ret));
            } else {
              mysql_bind[i].buffer = res_buffer;
              mysql_bind[i].buffer_length = *mysql_bind[i].length;
              if (0 != mysql_stmt_fetch_column(stmt_, &mysql_bind[i], i, 0)) {
                ret = -mysql_stmt_errno(stmt_);
                LOG_WARN("failed to fetch column", K(ret), "info", mysql_stmt_error(stmt_));
              }
            }
          }
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < result_column_count_; i++) {
        if (mysql_bind[i].buffer_type == MYSQL_TYPE_OBJECT) {
          MYSQL_COMPLEX_BIND_ARRAY *pl_array = (MYSQL_COMPLEX_BIND_ARRAY *)mysql_bind[i].buffer;
          MYSQL_COMPLEX_BIND_HEADER *header = NULL;
          char *copy_buff = (char *)result_params_[i].array_buffer_;
          if (OB_ISNULL(pl_array)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("pl_array is NULL", K(ret));
          } else if (OB_ISNULL(header = (MYSQL_COMPLEX_BIND_HEADER *)pl_array->buffer)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("header is NULL", K(ret));
          } else if (OB_ISNULL(copy_buff) || OB_ISNULL(result_params_[i].out_valid_array_size_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("argument is NULL", K(ret), K(result_params_[i].out_valid_array_size_));
          } else {
            *result_params_[i].out_valid_array_size_ = (int32_t)pl_array->length;
          }
          for (int64_t pos = 0; OB_SUCC(ret) && pos < pl_array->length; pos++) {
            if (header->buffer_type == MYSQL_TYPE_LONG) {
              MYSQL_COMPLEX_BIND_BASIC *bb = ((MYSQL_COMPLEX_BIND_BASIC *)pl_array->buffer) + pos;
              if (OB_ISNULL(bb) || OB_ISNULL(bb->buffer)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("argument is NULL", K(ret), K(bb), K(bb->buffer));
              } else {
                *(int *)copy_buff = *(int *)bb->buffer;
              }
            } else if (header->buffer_type == MYSQL_TYPE_VARCHAR) {
              MYSQL_COMPLEX_BIND_STRING *bs = ((MYSQL_COMPLEX_BIND_STRING *)pl_array->buffer) + pos;
              memcpy(copy_buff, (char *)bs->buffer, bs->length);
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not support type", K(ret), K(header->buffer_type));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "some type ");
            }
            if (OB_SUCC(ret)) {
              copy_buff += result_params_[i].ele_size_;
            }
          }
        }
      }
    }
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = close())) {
    LOG_WARN("close proc stmt failed", K(ret));
  }
  ret = (ret == OB_SUCCESS) ? tmp_ret : ret;
#endif
  return ret;
}

int ObMySQLProcStatement::close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(close_mysql_stmt())) {
    LOG_WARN("close mysql stmt failed", K(ret));
  }
  free_resouce();
  return ret;
}

void ObMySQLProcStatement::free_resouce()
{
  if (NULL != bind_params_) {
    alloc_->free(bind_params_);
    bind_params_ = NULL;
    stmt_param_count_ = 0;
  }
  if (NULL != result_params_) {
    alloc_->free(result_params_);
    result_params_ = NULL;
    result_column_count_ = 0;
  }
  param_.close();
  result_.close();
  in_out_map_.reset();
  proc_ = NULL;
}

int ObMySQLProcStatement::close_mysql_stmt()
{
  int ret = OB_SUCCESS;
  if (NULL != stmt_) {
    if (0 != mysql_stmt_close(stmt_)) {
      ret = -mysql_errno(conn_->get_handler());
      LOG_WARN("fail to close stmt", "info", mysql_error(conn_->get_handler()), K(ret));
    }
  }
  return ret;
}

int ObMySQLProcStatement::execute_stmt_v2_interface()
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
#else
  void* execute_extend_arg = NULL;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL", K(ret));
  } else if (OB_ISNULL(proc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proc_ is NULL", K(ret));
  } else if (0 != mysql_stmt_execute_v2(stmt_, proc_, STRLEN(proc_), 1, 0, execute_extend_arg)) {
    ret = -mysql_stmt_errno(stmt_);
    char errmsg[256] = {0};
    const char *srcmsg = mysql_stmt_error(stmt_);
    MEMCPY(errmsg, srcmsg, MIN(255, STRLEN(srcmsg)));
    TRANSLATE_CLIENT_ERR(ret, errmsg);
    LOG_WARN("failed to execute proc", "info", mysql_stmt_error(stmt_), K(ret));
  }
#endif
  return ret;
}

int ObMySQLProcStatement::handle_data_truncated(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  MYSQL_BIND *mysql_bind = result_.get_bind();
  if (OB_ISNULL(mysql_bind)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mysql_bind is NULL", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < result_column_count_; i++) {
    MYSQL_BIND &res_bind = mysql_bind[i];
    if (*res_bind.is_null) {
      result_params_[i].is_null_ = 1;
    } else {
      if (res_bind.buffer_length < *res_bind.length) {
        void *res_buffer = NULL;
        if (OB_ISNULL(res_buffer = allocator.alloc(*res_bind.length))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed", K(ret));
        } else {
          res_bind.buffer = res_buffer;
          res_bind.buffer_length = *res_bind.length;
        }
      }
      if (OB_SUCC(ret)) {
        if (0 != mysql_stmt_fetch_column(stmt_, &res_bind, i, 0)) {
          ret = -mysql_stmt_errno(stmt_);
          LOG_WARN("failed to fetch column", K(ret), "info", mysql_stmt_error(stmt_));
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to fetch column", K(ret), K(res_bind.buffer_type));
        } else {
          result_params_[i].buffer_type_ = res_bind.buffer_type;
          result_params_[i].buffer_ = res_bind.buffer;
          result_params_[i].buffer_len_ = res_bind.buffer_length;
          result_params_[i].length_ = *res_bind.length;
          result_params_[i].is_unsigned_ = res_bind.is_unsigned;
          result_params_[i].is_null_ = *res_bind.is_null;
        }
      }
    }
  }
  return ret;
}

} // end namespace sqlcient
} // end namespace common
} // end namespace oceanbase
