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
#include "lib/mysqlclient/ob_mysql_prepared_statement.h"
#include "share/schema/ob_routine_info.h"
#include "pl/ob_pl_user_type.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{

static const int64_t IN_VALUE_ISNULL = 1;
static const int64_t OUT_VALUE_ISNULL = 2;

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
  bind_param.col_idx_ = col_idx;
  float *buf = NULL;
  OV (OB_NOT_NULL(buf = reinterpret_cast<float *>(allocator.alloc(sizeof(float)))), OB_ALLOCATE_MEMORY_FAILED);
  if (OB_SUCC(ret)) {
    *buf = param.v_.float_;
    bind_param.buffer_type_ = buffer_type;
    bind_param.buffer_ = buf;
    bind_param.buffer_len_ = sizeof(float);
  }
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
  bind_param.col_idx_ = col_idx;
  double *buf = NULL;
  OV (OB_NOT_NULL(buf = reinterpret_cast<double *>(allocator.alloc(sizeof(double)))), OB_ALLOCATE_MEMORY_FAILED);
  if (OB_SUCC(ret)) {
    *buf = param.v_.double_;
    bind_param.buffer_type_ = buffer_type;
    bind_param.buffer_ = buf;
    bind_param.buffer_len_ = sizeof(double);
  }
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
  ObObjType obj_type = ObNullType;
  OZ (ObMySQLPreparedStatement::get_ob_type(obj_type, (obmysql::EMySQLFieldType)field_type));
  OZ (ObMySQLProcStatement::store_string_obj(param, obj_type, allocator,
                                             (int64_t)bind_param.length_,
                                             (char *)bind_param.buffer_));
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
  array_is_null_ = other.array_is_null_;
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

int ObMySQLPreparedStatement::get_mysql_type(ObObjType ob_type, obmysql::EMySQLFieldType &mysql_type)
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

int ObMySQLPreparedStatement::get_ob_type(ObObjType &ob_type, obmysql::EMySQLFieldType mysql_type)
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

int ObMySQLPreparedStatement::init(ObMySQLConnection &conn, const ObString &sql, int64_t param_count)
{
  int ret = OB_SUCCESS;
  conn_ = &conn;
  if (sql.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql", K(sql), K(ret));
  } else if (OB_ISNULL(stmt_ = mysql_stmt_init(conn_->get_handler()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to init stmt", K(ret));
  } else if (0 != mysql_stmt_prepare(stmt_, sql.ptr(), sql.length())) {
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
                                          const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                                          common::ObIArray<std::pair<int64_t, int64_t>> &basic_out_param,
                                          const ObTimeZoneInfo *tz_info,
                                          ObObj *result,
                                          bool is_sql)
{
  int ret = OB_SUCCESS;
  bool has_complex_type = false;
  if (routine_info.is_function()) {
    const ObDataType *ret_type = routine_info.get_ret_type();
    if (OB_ISNULL(ret_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("return type is NULL", K(ret), K(routine_info));
    } else if (ob_is_extend(ret_type->get_obj_type())) {
      has_complex_type = true;
    }
  }
  int64_t start_idx = routine_info.get_param_start_idx();
  const share::schema::ObRoutineParam *r_param = NULL;
  for (int64_t param_idx = 0; OB_SUCC(ret) && !has_complex_type && param_idx < params.count(); ++param_idx) {
    if (OB_ISNULL(r_param = routine_info.get_routine_params().at(start_idx + param_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param is NULL", K(ret), K(param_idx), K(start_idx), K(routine_info));
    } else if (!params.at(param_idx).is_pl_mock_default_param()
               && r_param->is_dblink_type()) {
      has_complex_type = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (has_complex_type) {
    if (OB_FAIL(bind_proc_param_with_composite_type(allocator,
                                                    params,
                                                    routine_info,
                                                    udts,
                                                    tz_info,
                                                    result,
                                                    is_sql,
                                                    basic_out_param))) {
      LOG_WARN("bind parameters failed", K(ret));
    }
  } else {
    int64_t start_idx = routine_info.get_param_start_idx();
    if (routine_info.is_function() && !is_sql) {
      if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is NULL", K(ret));
      } else if (OB_FAIL(basic_out_param.push_back(std::make_pair(0, 0)))) {
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
      } else if (OB_FAIL(basic_out_param.push_back(std::make_pair(params.count() - skip_cnt, 0)))) {
        LOG_WARN("push back failed", K(ret));
      } else if (OB_FAIL(bind_param(params.count() - skip_cnt, 0, true, tz_info, *result, routine_info, allocator))) {
        LOG_WARN("failed to bind param", K(ret));
      }
    }
  }
  return ret;
}

int ObMySQLProcStatement::bind_proc_param_with_composite_type(
                                          ObIAllocator &allocator,
                                          ParamStore &params,
                                          const share::schema::ObRoutineInfo &routine_info,
                                          const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                                          const ObTimeZoneInfo *tz_info,
                                          ObObj *result,
                                          bool is_sql,
                                          common::ObIArray<std::pair<int64_t, int64_t>> &basic_out_param)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
#else
  int64_t position = 0;
  const share::schema::ObRoutineParam *r_param = NULL;
  const pl::ObUserDefinedType *udt = NULL;
  int64_t out_param_start_pos = 0;
  bool is_return_basic = false;
  bool is_func = routine_info.is_function();
  if (is_func) {
    CK (OB_NOT_NULL(r_param = routine_info.get_routine_params().at(0)));
    if (OB_FAIL(ret)) {
    } else if (r_param->is_dblink_type()) {
      OZ (get_udt_by_name(r_param->get_type_name(), udts, udt));
      CK (OB_NOT_NULL(udt));
      OZ (bind_compsite_type(position, allocator, *result, r_param, udts, udt, tz_info));
    } else {
      is_return_basic = true;
      CK (OB_NOT_NULL(result));
      if (!is_sql) {
        OZ (basic_out_param.push_back(std::make_pair(get_basic_return_value_pos(), 0)));
      }
      OZ (bind_param(get_basic_return_value_pos(), 0, true, tz_info, *result, routine_info, allocator));
    }
  }
  int64_t basic_param_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    ObObjParam &obj_param = params.at(i);
    if (!obj_param.is_pl_mock_default_param()) {
      CK (OB_NOT_NULL(r_param = routine_info.get_routine_params().at(i + (is_func ? 1 : 0))));
      if (OB_FAIL(ret)) {
      } else if (r_param->is_dblink_type()) {
        OZ (get_udt_by_name(r_param->get_type_name(), udts, udt));
        CK (OB_NOT_NULL(udt));
        OZ (bind_compsite_type(position, allocator, obj_param, r_param, udts, udt, tz_info));
      } else {
        bool is_out = is_out_param(*r_param);
        if (is_out_param(*r_param)) {
          OZ (basic_out_param.push_back(std::make_pair(get_basic_param_start_pos() + basic_param_cnt, i)));
        }
        OZ (bind_param(get_basic_param_start_pos() + basic_param_cnt,
                       i + (is_func ? 1 : 0),
                       is_out, tz_info, obj_param, routine_info, allocator));
        OX (basic_param_cnt++);
      }
    } // end for
  }
  if (OB_SUCC(ret) && is_return_basic && is_sql) {
    OZ (basic_out_param.push_back(std::make_pair(get_basic_return_value_pos(), 0)));
  }
#endif
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObMySQLProcStatement::bind_compsite_type(int64_t &position,
                                             ObIAllocator &allocator,
                                             ObObj &param,
                                             const share::schema::ObRoutineParam *r_param,
                                             const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                                             const pl::ObUserDefinedType *udt,
                                             const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = position;
  if (OB_ISNULL(udt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udt is NULL", K(ret));
  } else if (udt->is_record_type()) {
    if (OB_FAIL(build_record_meta(allocator, param, r_param,
                                  static_cast<const pl::ObRecordType *>(udt),
                                  position, tz_info))) {
      LOG_WARN("build record meta failed", K(ret), K(position), K(param), K(tmp_pos));
    }
  } else if (udt->is_collection_type()) {
    if (OB_FAIL(build_array_meta(allocator, param, r_param,
                                 static_cast<const pl::ObCollectionType *>(udt),
                                 udts, position, tz_info))) {
      LOG_WARN("build array meta failed", K(ret), K(position), K(param), K(tmp_pos));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected user defined type", K(ret), KPC(udt));
  }
  return ret;
}

int ObMySQLProcStatement::build_record_meta(ObIAllocator &allocator,
                                            ObObj &param,
                                            const share::schema::ObRoutineParam *r_param,
                                            const pl::ObRecordType *record_type,
                                            int64_t &position,
                                            const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(r_param));
  CK (OB_NOT_NULL(record_type));
  if (OB_SUCC(ret)) {
    int64_t in_start_pos = 0;
    int64_t out_start_pos = get_out_param_cur_pos();
    if (is_in_param(*r_param)) {
      in_start_pos = position;
      position += record_type->get_record_member_count();
    }
    if (is_out_param(*r_param)) {
      increase_out_param_cur_pos(record_type->get_record_member_count());
    }
    ObCompositeData com_data(r_param->get_param_position());
    com_data.set_is_record(true);
    com_data.set_is_out(true);
    com_data.set_udt_id(record_type->get_user_type_id());
    for (int64_t i = 0; OB_SUCC(ret) && i < record_type->get_record_member_count(); ++i) {
      const pl::ObPLDataType *pl_type = record_type->get_record_member_type(i);
      ObBindParam *bind_param = NULL;
      CK (OB_NOT_NULL(pl_type));
      CK (pl_type->is_obj_type());
      if (OB_SUCC(ret) && is_in_param(*r_param)) {
        OZ (build_record_element(in_start_pos++, false, i, allocator, param, *pl_type, tz_info, bind_param));
      }
      if (OB_SUCC(ret) && is_out_param(*r_param)) {
        OZ (build_record_element(out_start_pos + i, true, i, allocator, param, *pl_type, tz_info, bind_param));
        CK (OB_NOT_NULL(bind_param));
        OZ (com_data.add_element(bind_param));
      }
    }
    if (OB_SUCC(ret) && is_out_param(*r_param)) {
      OZ (get_com_datas().push_back(com_data));
    }
  }
  return ret;
}

int ObMySQLProcStatement::build_record_element(int64_t position,
                                               bool is_out,
                                               int64_t element_idx,
                                               ObIAllocator &allocator,
                                               ObObj &param,
                                               const pl::ObPLDataType &pl_type,
                                               const ObTimeZoneInfo *tz_info,
                                               ObBindParam *&bind_param)
{
  int ret = OB_SUCCESS;
  ObObj current_obj;
  pl::ObPLRecord *record = reinterpret_cast<pl::ObPLRecord *>(param.get_ext());
  if (NULL != record) {
    OZ (get_current_obj(record, 0, element_idx, current_obj));
  }
  if (OB_SUCC(ret)) {
    const ObObjType obj_type = current_obj.get_type();
    enum_field_types buffer_type = static_cast<enum_field_types>(ob_type_to_mysql_type[pl_type.get_obj_type()]);
    bind_param = nullptr;
    if (OB_FAIL(get_bind_param_by_idx(position, bind_param))) {
      LOG_WARN("get param failed", K(ret), K(position), K(pl_type));
    } else if (OB_ISNULL(bind_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bind param is NULL", K(ret), K(position), K(pl_type));
    } else if (OB_FAIL(ObBindParamEncode::encode_map_[obj_type](position,
                                                                is_out,
                                                                *tz_info,
                                                                current_obj,
                                                                *bind_param,
                                                                allocator,
                                                                buffer_type))) {
      LOG_WARN("encode param failed", K(ret));
    } else if (OB_FAIL(param_.bind_param(*bind_param))) {
      LOG_WARN("bind param failed", K(ret));
    }
  }
  return ret;
}

int ObMySQLProcStatement::get_current_obj(pl::ObPLComposite *composite,
                                          int64_t current_idx,
                                          int64_t element_idx,
                                          ObObj &current_obj)
{
  int ret = OB_SUCCESS;
  ObObj element;
  CK (OB_NOT_NULL(composite));
  if (OB_FAIL(ret)) {
  } else if (composite->is_collection()) {
    pl::ObPLCollection *coll = static_cast<pl::ObPLCollection *>(composite);
    CK (OB_NOT_NULL(coll));
    CK (current_idx < coll->get_count());
    if (OB_FAIL(ret)) {
    } else if (!coll->is_associative_array()) {
      OX (element = coll->get_data()[current_idx]);
    } else {
      pl::ObPLAssocArray *assoc = static_cast<pl::ObPLAssocArray *>(coll);
      CK (OB_NOT_NULL(assoc));
      if (OB_FAIL(ret)) {
      } else if (NULL == assoc->get_sort()) {
        OX (element = coll->get_data()[current_idx]);
      } else {
        element = coll->get_data()[assoc->get_sort(current_idx)];
      }
    }
    if (OB_FAIL(ret)) {
    } else if (pl::PL_OBJ_TYPE == coll->get_element_desc().get_pl_type()) {
      CK (!element.is_ext());
      OX (current_obj = element);
    } else if (pl::PL_RECORD_TYPE == coll->get_element_desc().get_pl_type()) {
      pl::ObPLRecord *record = NULL;
      CK (element.is_ext());
      CK (OB_NOT_NULL(record = reinterpret_cast<pl::ObPLRecord*>(element.get_ext())));
      CK (element_idx < record->get_count());
      OZ (record->get_element(element_idx, current_obj));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pl type of collection element", K(ret), KPC(coll));
    }
  } else if (composite->is_record()) {
    pl::ObPLRecord *record = static_cast<pl::ObPLRecord *>(composite);
    CK (OB_NOT_NULL(record));
    CK (element_idx < record->get_count());
    OZ (record->get_element(element_idx, current_obj));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pl type", K(ret), KPC(composite));
  }
  CK (!current_obj.is_ext());
  return ret;
}


int ObMySQLProcStatement::build_array_meta(ObIAllocator &allocator,
                                           ObObj &param,
                                           const share::schema::ObRoutineParam *r_param,
                                           const pl::ObCollectionType *coll_type,
                                           const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                                           int64_t &position,
                                           const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  int64_t &in_start_pos = position;
  CK (OB_NOT_NULL(r_param));
  CK (OB_NOT_NULL(coll_type));
  if (OB_SUCC(ret)) {
    ObCompositeData com_data(r_param->get_param_position());
    com_data.set_is_out(true);
    com_data.set_udt_id(coll_type->get_user_type_id());
    if (!coll_type->is_associative_array_type()) {
      if (is_in_param(*r_param)) {
        OZ (build_array_isnull(in_start_pos++, true, param));
      }
      if (is_out_param(*r_param)) {
        ObBindParam *bind_param = NULL;
        com_data.set_is_assoc_array(false);
        OZ (build_array_isnull(get_out_param_cur_pos(), false, param));
        OZ (get_bind_param_by_idx(get_out_param_cur_pos(), bind_param));
        CK (OB_NOT_NULL(bind_param));
        OZ (com_data.add_element(bind_param));
        OX (increase_out_param_cur_pos(1));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (coll_type->get_element_type().is_obj_type()) {
      if (OB_SUCC(ret) && is_in_param(*r_param)) {
        // 入参数组
        OZ (build_array_element(in_start_pos++, false, 0, allocator, param,
                                coll_type->get_element_type().get_obj_type(), tz_info));
      }
      if (OB_SUCC(ret) && is_out_param(*r_param)) {
        // 出参数组
        ObBindParam *bind_param = NULL;
        OZ (build_array_element(get_out_param_cur_pos(), true, 0, allocator, param,
                                coll_type->get_element_type().get_obj_type(), tz_info));
        OZ (get_bind_param_by_idx(get_out_param_cur_pos(), bind_param));
        CK (OB_NOT_NULL(bind_param));
        OZ (com_data.add_element(bind_param));
        OX (increase_out_param_cur_pos(1));
      }
    } else if (coll_type->get_element_type().is_record_type()) {
      const pl::ObUserDefinedType *inner = NULL;
      const pl::ObRecordType *record_type = NULL;
      OZ (get_udt_by_id(coll_type->get_element_type().get_user_type_id(), udts, inner));
      CK (OB_NOT_NULL(record_type = static_cast<const pl::ObRecordType*>(inner)));
      for (int64_t i = 0; OB_SUCC(ret) && i < record_type->get_record_member_count(); ++i) {
        CK (OB_NOT_NULL(record_type->get_record_member_type(i)));
        CK (record_type->get_record_member_type(i)->is_obj_type());
        if (OB_SUCC(ret) && is_in_param(*r_param)) {
          // 入参数组
          OZ (build_array_element(in_start_pos++, false, i, allocator, param,
                                  record_type->get_record_member_type(i)->get_obj_type(),
                                  tz_info));
        }
        if (OB_SUCC(ret) && is_out_param(*r_param)) {
          // 出参数组
           ObBindParam *bind_param = NULL;
          OZ (build_array_element(get_out_param_cur_pos(), true, i, allocator, param,
                                  record_type->get_record_member_type(i)->get_obj_type(),
                                  tz_info));
          OZ (get_bind_param_by_idx(get_out_param_cur_pos(), bind_param));
          CK (OB_NOT_NULL(bind_param));
          OZ (com_data.add_element(bind_param));
          OX (increase_out_param_cur_pos(1));
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
        "collection type whose data type is not collection(record(basic))/collection(basic)");
    }
    if (OB_SUCC(ret) && is_out_param(*r_param)) {
      OZ (get_com_datas().push_back(com_data));
    }
  }
  return ret;
}

int ObMySQLProcStatement::build_array_element(int64_t position,
                                              bool is_out,
                                              int64_t element_idx,
                                              ObIAllocator &allocator,
                                              ObObj &param,
                                              ObObjType obj_type,
                                              const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  if (!is_out) {
    if (param.is_null() || !param.is_ext() || 0 == param.get_ext()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support null array now", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "array type in mode paramter is null");
    } else {
      pl::ObPLCollection *coll = reinterpret_cast<pl::ObPLCollection *>(param.get_ext());
      CK (OB_NOT_NULL(coll));
      if (OB_SUCC(ret) && 0 == coll->get_count()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("collection type not support do delete", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "collection type do delete in PL dblink");
      }
      OZ (check_assoc_array(coll));
      OZ (build_array_buffer(position, element_idx, allocator, coll, obj_type, false, tz_info));
    }
  } else {
    OZ (build_array_buffer(position, element_idx, allocator, NULL, obj_type, true, tz_info));
  }
  return ret;
}

int ObMySQLProcStatement::build_array_buffer(int64_t position,
                                             int64_t element_idx,
                                             ObIAllocator &allocator,
                                             pl::ObPLCollection *coll,
                                             ObObjType obj_type,
                                             bool is_out,
                                             const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  ObBindParam *bind_param = NULL;
  if (!is_out) {
    CK (OB_NOT_NULL(coll));
  }
  OZ (get_bind_param_by_idx(position, bind_param));
  CK (OB_NOT_NULL(bind_param));
  if (OB_SUCC(ret)) {
    enum_field_types ele_type = static_cast<enum_field_types>(ob_type_to_mysql_type[(obj_type)]);
    MYSQL_COMPLEX_BIND_HEADER *array_ele = NULL;
    MYSQL_COMPLEX_BIND_PLARRAY *my_array = NULL;
    int64_t ele_cnt = is_out ? 0 : (coll->get_count() < 0 ? 0 : coll->get_count());
    int64_t array_elem_size = convert_type_to_complex(ele_type);
    int64_t buf_len = sizeof(MYSQL_COMPLEX_BIND_PLARRAY)
                      + array_elem_size * (ele_cnt == 0 ? 1 : ele_cnt);
    char *buf = NULL;
    if (OB_ISNULL(buf = (char *)allocator.alloc(buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else {
      MEMSET(buf, 0, buf_len);
      bind_param->col_idx_ = position;
      bind_param->buffer_type_ = MYSQL_TYPE_OBJECT;
      bind_param->buffer_ = buf;
      bind_param->buffer_len_ = buf_len;
      bind_param->is_null_ = 0;
      // bind_param->array_buffer_ = array;
      // bind_param->ele_size_ = ele_size;
      // bind_param->max_array_size_ = array_size;
      // bind_param->out_valid_array_size_ = out_valid_array_size;

      my_array = (MYSQL_COMPLEX_BIND_PLARRAY *)buf;
      my_array->buffer_type = MYSQL_TYPE_PLARRAY;
      my_array->buffer = buf + sizeof(MYSQL_COMPLEX_BIND_PLARRAY);
      my_array->is_null = 0;
      my_array->type_name = NULL;
      my_array->owner_name = NULL;
      my_array->elem_type = ele_type;
      my_array->length = ele_cnt;
      my_array->maxrarr_len = (is_out ? 1024 : ele_cnt);

      array_ele = (MYSQL_COMPLEX_BIND_HEADER *)my_array->buffer;
      for (int64_t i = 0; OB_SUCC(ret) && i < ele_cnt; ++i) {
        ObObj current_obj;
        array_ele->buffer_type = ele_type;
        OZ (get_current_obj(coll, i, element_idx, current_obj));
        OZ (store_array_element(current_obj, array_ele, allocator, tz_info));
        if (OB_SUCC(ret)) {
          skip_param_complex(&array_ele);
        }
      } // end for
      OZ (param_.bind_param(*bind_param));
    }
  }
  return ret;
}

int ObMySQLProcStatement::check_assoc_array(pl::ObPLCollection *coll)
{
  int ret = OB_SUCCESS;
  if (NULL != coll && coll->is_associative_array()) {
    pl::ObPLAssocArray *assoc = static_cast<pl::ObPLAssocArray *>(coll);
    ObObj *keys = NULL;
    CK (OB_NOT_NULL(assoc));
    if (OB_SUCC(ret)
        && assoc->get_count() > 0
        && NULL != (keys = assoc->get_key())
        && NULL != assoc->get_sort()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < assoc->get_count(); i++) {
        ObObj &key = keys[assoc->get_sort(i)];
        if ((int64_t)key.get_int32() != i + 1) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("key must be consecutive positive integers starting from one", K(ret), K(i), K(key));
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
          "index table key must be consecutive positive integers starting from one, other cases");
        }
      }
    }
  }
  return ret;
}
#endif

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
                                                     const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                                                     common::ObIArray<std::pair<int64_t, int64_t>> &basic_out_param,
                                                     const ObTimeZoneInfo *tz_info,
                                                     ObObj *result,
                                                     bool is_sql)
{
  int ret = OB_SUCCESS;
  const int64_t params_count = params.count();
  if (OB_FAIL(result_.init())) {
    LOG_WARN("failed to init result_", K(ret));
  } else if (FALSE_IT(result_column_count_ = result_.get_result_column_count())) {
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
    // process compsite out param
    int64_t idx_in_result = basic_out_param.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < get_com_datas().count(); ++i) {
      ObCompositeData &com_data = get_com_datas().at(i);
      for (int64_t inner_idx = 0; OB_SUCC(ret) && inner_idx < com_data.get_data_array().count(); inner_idx++) {
        CK (OB_NOT_NULL(in_param = com_data.get_data_array().at(inner_idx)));
        OZ (get_bind_result_param_by_idx(idx_in_result, out_param));
        CK (OB_NOT_NULL(out_param));
        if (OB_SUCC(ret)) {
          out_param->assign(*in_param);
          mysql_bind[idx_in_result].buffer_type = out_param->buffer_type_;
          mysql_bind[idx_in_result].buffer = NULL;
          mysql_bind[idx_in_result].buffer_length = 0;
          mysql_bind[idx_in_result].length = &out_param->length_;
          mysql_bind[idx_in_result].error = &mysql_bind[i].error_value;
          mysql_bind[idx_in_result].is_null = &out_param->is_null_;
          if (OB_ISNULL(mysql_bind[idx_in_result].buffer)) {
            void *tmp_buf = NULL;
            int64_t tmp_buf_len = 0;
            if (MYSQL_TYPE_DATETIME == out_param->buffer_type_) {
              tmp_buf_len = sizeof(MYSQL_TIME);
            } else if (MYSQL_TYPE_FLOAT == out_param->buffer_type_) {
              tmp_buf_len = sizeof(float);
            } else if (MYSQL_TYPE_DOUBLE ==  out_param->buffer_type_) {
              tmp_buf_len = sizeof(double);
            } else if (MYSQL_TYPE_LONG == out_param->buffer_type_
                       || MYSQL_TYPE_LONGLONG == out_param->buffer_type_) {
              tmp_buf_len = sizeof(int64_t);
            } else if (MYSQL_TYPE_TINY == out_param->buffer_type_) {
              tmp_buf_len = sizeof(int);
            }
            if (tmp_buf_len > 0) {
              if (OB_ISNULL(tmp_buf = allocator.alloc(tmp_buf_len))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed to alloc memory", K(ret));
              } else {
                mysql_bind[idx_in_result].buffer = tmp_buf;
                mysql_bind[idx_in_result].buffer_length = tmp_buf_len;
                out_param->buffer_ = tmp_buf;
                out_param->buffer_len_ = tmp_buf_len;
              }
            }
          }
        }
        idx_in_result++;
      } // end for
    } // end for
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
        } else {
          ret = -mysql_stmt_errno(stmt_);
          LOG_WARN("failed to fetch", K(ret), K(tmp_ret),
                   "info", mysql_stmt_error(stmt_), "info", mysql_error(conn_->get_handler()));
        }
        if (OB_SUCC(ret)) {
          idx_in_result = basic_out_param.count();
          for (int64_t i = 0; OB_SUCC(ret) && i < get_com_datas().count(); ++i) {
            ObCompositeData &com_data = get_com_datas().at(i);
            for (int64_t inner_idx = 0; OB_SUCC(ret) && inner_idx < com_data.get_data_array().count(); inner_idx++) {
              CK (OB_NOT_NULL(in_param = com_data.get_data_array().at(inner_idx)));
              OX (in_param->assign(result_params_[idx_in_result]));
              idx_in_result++;
            } // end inner for
          } // end outer for
        }
        // process basic out param
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
                    ||(!is_sql && (col_idx == get_basic_return_value_pos())))) {
              obj = result;
              is_return_value = true;
            } else {
              obj = &params.at(basic_out_param.at(i).second);
            }
            if (OB_ISNULL(obj)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("obj is NULL", K(ret));
            } else if (OB_FAIL(convert_proc_output_param_result(basic_out_param.at(i).second, *tz_info,
                                                                *bind_param, obj,
                                                                routine_info, allocator, is_return_value))) {
              LOG_WARN("fail to convert proc output param result", K(ret));
            }
          }
        } // end for
        // process composite out param
        OZ (process_composite_out_param(allocator, params, result, basic_out_param.count(),
                                    routine_info, udts, tz_info));
      }
    }
  }
  return ret;
}

int ObMySQLProcStatement::process_composite_out_param(ObIAllocator &allocator,
                                                      ParamStore &params,
                                                      ObObj *result,
                                                      int64_t start_idx_in_result,
                                                      const share::schema::ObRoutineInfo &routine_info,
                                                      const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                                                      const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
#else
  int64_t idx_in_result = start_idx_in_result;
  bool is_func = routine_info.is_function();
  bool ret_composite = false;
  if (is_func) {
    CK (OB_NOT_NULL(routine_info.get_ret_type()));
    OX (ret_composite = ob_is_extend(routine_info.get_ret_type()->get_obj_type()));
    if (OB_SUCC(ret) && ret_composite) {
      ObCompositeData &com_data = get_com_datas().at(0);
      const pl::ObUserDefinedType *udt = NULL;
      OZ (get_udt_by_id(com_data.get_udt_id(), udts, udt));
      if (OB_FAIL(ret)) {
      } else if (udt->is_record_type()) {
        OZ (process_record_out_param(static_cast<const pl::ObRecordType*>(udt),
                                     com_data,
                                     allocator,
                                     *result,
                                     tz_info));
      } else if (udt->is_collection_type()) {
        OZ (process_array_out_param(static_cast<const pl::ObCollectionType*>(udt),
                                    udts,
                                    com_data,
                                    allocator,
                                    *result,
                                    tz_info));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type is error", K(ret), KPC(udt));
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t i = ret_composite ? 1 : 0;
    // int64_t start_idx = routine_info.get_param_start_idx();
    for (; OB_SUCC(ret) && i < get_com_datas().count(); ++i) {
      ObCompositeData &com_data = get_com_datas().at(i);
      const pl::ObUserDefinedType *udt = NULL;
      OZ (get_udt_by_id(com_data.get_udt_id(), udts, udt));
      if (OB_FAIL(ret)) {
      } else if (udt->is_record_type()) {
        OZ (process_record_out_param(static_cast<const pl::ObRecordType*>(udt),
                                     com_data,
                                     allocator,
                                     params.at(com_data.get_param_position() - 1),
                                     tz_info));
      } else if (udt->is_collection_type()) {
        OZ (process_array_out_param(static_cast<const pl::ObCollectionType*>(udt),
                                    udts,
                                    com_data,
                                    allocator,
                                    params.at(com_data.get_param_position() - 1),
                                    tz_info));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type is error", K(ret), KPC(udt));
      }
    }
  }
#endif
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObMySQLProcStatement::process_record_out_param(const pl::ObRecordType *record_type,
                                                    ObCompositeData &com_data,
                                                    ObIAllocator &allocator,
                                                    ObObj &param,
                                                    const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  pl::ObPLRecord *record = NULL;
  CK (com_data.is_out());
  CK (com_data.is_record());
  CK (OB_NOT_NULL(record_type));
  CK (param.is_null() || param.is_ext());
  CK (OB_NOT_NULL(tz_info));
  if (OB_FAIL(ret)) {
  } else if (param.is_null() || 0 == param.get_ext()) {
    int64_t init_size = pl::ObRecordType::get_init_size(record_type->get_member_count());
    if (OB_ISNULL(record = reinterpret_cast<pl::ObPLRecord*>(allocator.alloc(init_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for record", K(ret), K(init_size));
    } else {
      new(record)pl::ObPLRecord(record_type->get_user_type_id(), record_type->get_member_count());
      OZ (record->init_data(allocator, true));
      OX (param.set_extend(reinterpret_cast<int64_t>(record), pl::PL_RECORD_TYPE, init_size));
    }
  } else {
    OZ (pl::ObUserDefinedType::destruct_obj(param, nullptr, true));
    CK (OB_NOT_NULL(record = reinterpret_cast<pl::ObPLRecord *>(param.get_ext())));
  }
  ObObj *element = NULL;
  CK (OB_NOT_NULL(record->get_allocator()));
  for (int64_t i = 0; OB_SUCC(ret) && i < record_type->get_member_count(); i++) {
    ObObjType obj_type = ObNullType;
    ObBindParam *bind_param = NULL;
    CK (OB_NOT_NULL(bind_param = com_data.get_data_array().at(i)));
    OZ (get_ob_type(obj_type, static_cast<obmysql::EMySQLFieldType>(bind_param->buffer_type_)));
    CK (OB_NOT_NULL(record_type->get_record_member_type(i)));
    OZ (record->get_element(i, element));
    CK (OB_NOT_NULL(element));
    CK (record_type->get_record_member_type(i)->is_obj_type());
    OX (new(element) ObObj(ObNullType));
    if (OB_FAIL(ret)) {
    } else if (bind_param->is_null_) {
      element->set_null();
    } else {
      OZ (ObBindParamDecode::decode_map_[obj_type](bind_param->buffer_type_,
                                                  *tz_info,
                                                  *bind_param,
                                                  *element,
                                                  *record->get_allocator()));
    }
    if (OB_SUCC(ret) && !element->is_null()) {
      CK (element->get_type() == record_type->get_record_member_type(i)->get_obj_type());
      OX (element->set_meta_type(*(record_type->get_record_member_type(i)->get_meta_type())));
    }
  }
  return ret;
}

int ObMySQLProcStatement::process_array_out_param(const pl::ObCollectionType *coll_type,
                                                  const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                                                  ObCompositeData &com_data,
                                                  ObIAllocator &allocator,
                                                  ObObj &param,
                                                  const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  pl::ObPLCollection *coll = NULL;
  ObObj *new_data = NULL;
  bool is_null = false;
  ObIAllocator *local_allocator = &allocator;
  // int64_t array_size = 0;
  CK (com_data.is_out());
  CK (!com_data.is_record());
  CK (OB_NOT_NULL(coll_type));
  if (OB_FAIL(ret)) {
  } else if (param.is_ext() && param.get_ext() != 0) {
    // 出参场景，原来已经有值，直接修改
    OZ (pl::ObUserDefinedType::destruct_obj(param, nullptr, true));
    CK (OB_NOT_NULL(coll = reinterpret_cast<pl::ObPLCollection*>(param.get_ext())));
    OX (coll->set_count(0));
    LOG_INFO("coll allocator is", K(coll->get_allocator()), K(ret));
  } else {
    // 原来无值，重新分配内存
    int64_t init_size = 0;
    void *ptr = NULL;
    OZ (coll_type->get_size(pl::ObPLTypeSize::PL_TYPE_INIT_SIZE, init_size));
    OV (OB_NOT_NULL(ptr = allocator.alloc(init_size)), OB_ALLOCATE_MEMORY_FAILED, init_size);
    OX (MEMSET(ptr, 0, init_size));
    if (OB_FAIL(ret)) {
    } else if (coll_type->is_nested_table_type()) {
      new(ptr)pl::ObPLNestedTable(coll_type->get_user_type_id());
    } else if (coll_type->is_varray_type()) {
      const pl::ObVArrayType *v_type = static_cast<const pl::ObVArrayType*>(coll_type);
      pl::ObPLVArray *varray = reinterpret_cast<pl::ObPLVArray*>(ptr);
      CK (OB_NOT_NULL(v_type));
      CK (OB_NOT_NULL(varray));
      OX (new(varray)pl::ObPLVArray(v_type->get_user_type_id()));
      OX (varray->set_capacity(v_type->get_capacity()));
    } else if (coll_type->is_associative_array_type()) {
      new(ptr)pl::ObPLAssocArray(coll_type->get_user_type_id());
    }
    if (OB_SUCC(ret)) {
      coll = reinterpret_cast<pl::ObPLCollection*>(ptr);
      pl::ObElemDesc elem_desc;
      int64_t field_cnt = OB_INVALID_COUNT;
      if (coll_type->get_element_type().is_obj_type()) {
        CK (OB_NOT_NULL(coll_type->get_element_type().get_data_type()));
        OX (((ObDataType &)elem_desc) = *(coll_type->get_element_type().get_data_type()));
      } else {
        elem_desc.set_obj_type(common::ObExtendType);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(coll->init_allocator(allocator, true))) {
        LOG_WARN("collection init allocator failed", K(ret));
      } else if (coll_type->get_element_type().is_record_type()) {
        const pl::ObUserDefinedType *udt = NULL;
        const pl::ObRecordType *record_type = NULL;
        OZ (get_udt_by_id(coll_type->get_element_type().get_user_type_id(), udts, udt));
        CK (OB_NOT_NULL(record_type = static_cast<const pl::ObRecordType*>(udt)));
        OX (field_cnt = record_type->get_member_count());
      } else {
        field_cnt = 1;
      }
      if (OB_SUCC(ret)) {
        elem_desc.set_field_count(field_cnt);
        elem_desc.set_pl_type(coll_type->get_element_type().get_type());
        elem_desc.set_udt_id(coll_type->get_element_type().get_user_type_id());
        coll->set_element_desc(elem_desc);
        param.set_extend(reinterpret_cast<int64_t>(ptr), coll_type->get_type(), init_size);
      } else if (NULL != ptr) {
        allocator.free(ptr);
      }
    }
  }
  // deal with is null
  if (OB_SUCC(ret) && !coll_type->is_associative_array_type()) {
    ObObj isnull_obj;
    ObBindParam *bind_param = NULL;
    CK (!com_data.is_assoc_array());
    CK (OB_NOT_NULL(bind_param = com_data.get_data_array().at(0)));
    OZ (ObBindParamDecode::decode_map_[ObObjType::ObIntType](bind_param->buffer_type_,
                                                              *tz_info,
                                                              *bind_param,
                                                              isnull_obj,
                                                              allocator));
    OX (is_null = (isnull_obj.get_int() == OUT_VALUE_ISNULL));
  }
  if (OB_FAIL(ret)) {
  } else if (is_null) {
    // 返回的数组为null
  } else if (pl::PL_OBJ_TYPE == coll->get_element_desc().get_pl_type()) {
    ObBindParam *bind_param = NULL;
    if (coll_type->is_associative_array_type()) {
      CK (1 == com_data.get_data_array().count());
      CK (OB_NOT_NULL(bind_param = com_data.get_data_array().at(0)));
    } else {
      CK (2 == com_data.get_data_array().count());
      CK (OB_NOT_NULL(bind_param = com_data.get_data_array().at(1)));
    }
    CK (MYSQL_TYPE_OBJECT == bind_param->buffer_type_);
    if (OB_SUCC(ret)) {
      int64_t array_size = 0;
      MYSQL_COMPLEX_BIND_ARRAY *pl_array = (MYSQL_COMPLEX_BIND_ARRAY *)bind_param->buffer_;
      CK (OB_NOT_NULL(pl_array));
      OX (array_size = (int64_t)pl_array->length);
      OX (local_allocator = coll->get_allocator() != NULL ? coll->get_allocator() : &allocator);
      if (0 == array_size) {
        // do nothing
      } else if (OB_ISNULL(new_data = reinterpret_cast<ObObj*>(local_allocator->alloc(sizeof(ObObj) * array_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed alloc memory for collection data", K(ret), K(array_size));
      } else {
        int64_t i = 0;
        for (; OB_SUCC(ret) && i < array_size; ++i) {
          ObObj *current_obj = new_data + i;
          CK (OB_NOT_NULL(current_obj));
          OX (current_obj->reset());
          OX (current_obj->set_meta_type(coll->get_element_desc().get_meta_type()));
          OZ (process_array_element(i, *local_allocator, pl_array->buffer, *current_obj, tz_info));
        }
        if (OB_SUCC(ret)) {
          coll->set_data(new_data, array_size);
          coll->set_count(array_size);
          coll->set_first(1);
          coll->set_last(array_size);
        } else if (NULL != new_data) {
          for (int64_t j = 0; j < i; j++) {
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = pl::ObUserDefinedType::destruct_objparam(*local_allocator, new_data[j]))) {
              LOG_WARN("dblink destruct_objparam failed", K(ret), K(tmp_ret));
            }
          }
          local_allocator->free(new_data);
        }
      }
    }
  } else if (pl::PL_RECORD_TYPE == coll->get_element_desc().get_pl_type()) {
    const pl::ObUserDefinedType *udt = NULL;
    const pl::ObRecordType *record_type = NULL;
    OZ (get_udt_by_id(coll_type->get_element_type().get_user_type_id(), udts, udt));
    CK (OB_NOT_NULL(record_type = static_cast<const pl::ObRecordType*>(udt)));
    if (OB_SUCC(ret)) {
      ObBindParam *bind_param = NULL;
      int64_t start_idx = 0;
      if (coll_type->is_associative_array_type()) {
        CK (record_type->get_member_count() == com_data.get_data_array().count());
      } else {
        start_idx = 1;
        CK (record_type->get_member_count() + 1 == com_data.get_data_array().count());
      }
      CK (OB_NOT_NULL(bind_param = com_data.get_data_array().at(start_idx)));
      CK (MYSQL_TYPE_OBJECT == bind_param->buffer_type_);
      if (OB_SUCC(ret)) {
        int64_t array_size = 0;
        MYSQL_COMPLEX_BIND_ARRAY *pl_array = (MYSQL_COMPLEX_BIND_ARRAY *)bind_param->buffer_;
        CK (OB_NOT_NULL(pl_array));
        OX (array_size = (int64_t)pl_array->length);
        OX (local_allocator = coll->get_allocator() != NULL ? coll->get_allocator() : &allocator);
        if (0 == array_size) {
          // do nothing
        } else if (OB_ISNULL(new_data = reinterpret_cast<ObObj*>(local_allocator->alloc(sizeof(ObObj) * array_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed alloc memory for collection data", K(ret), K(array_size));
        } else {
          int64_t i = 0;
          for (; OB_SUCC(ret) && i < array_size; i++) {
            ObObj *current_obj = new_data + i;
            int64_t element_cnt = record_type->get_record_member_count();
            int64_t init_size = pl::ObRecordType::get_init_size(element_cnt);
            CK (OB_NOT_NULL(current_obj));
            OX (current_obj->reset());
            if (OB_SUCC(ret)) {
              void *ptr = local_allocator->alloc(init_size);
              pl::ObPLRecord *record = NULL;
              OV (OB_NOT_NULL(ptr), OB_ALLOCATE_MEMORY_FAILED, init_size);
              CK (OB_NOT_NULL(record = reinterpret_cast<pl::ObPLRecord*>(ptr)));
              if (OB_SUCC(ret)) {
                ObObj *element = NULL;
                new(ptr)pl::ObPLRecord(record_type->get_user_type_id(), record_type->get_member_count());
                OZ (record->init_data(*local_allocator, false));
                if (OB_FAIL(ret)) {
                  local_allocator->free(ptr);
                } else {
                  CK (record->get_allocator());
                  for (int64_t j = 0; OB_SUCC(ret) && j < record_type->get_member_count(); ++j) {
                    CK (OB_NOT_NULL(record_type->get_record_member_type(j)));
                    OZ (record->get_element(j, element));
                    CK (OB_NOT_NULL(element));
                    CK (record_type->get_record_member_type(j)->is_obj_type());
                    OX (new (element) ObObj(ObNullType));
                    if (OB_SUCC(ret)) {
                      CK (start_idx + j < com_data.get_data_array().count());
                      CK (OB_NOT_NULL(bind_param = com_data.get_data_array().at(start_idx + j)));
                      CK (MYSQL_TYPE_OBJECT == bind_param->buffer_type_);
                      CK (OB_NOT_NULL(pl_array = (MYSQL_COMPLEX_BIND_ARRAY *)bind_param->buffer_));
                      OX (element->set_meta_type(*(record_type->get_record_member_type(j)->get_meta_type())));
                      OZ (process_array_element(i, *(record->get_allocator()), pl_array->buffer, *element, tz_info));
                    }
                  } // end for
                  current_obj->set_extend(reinterpret_cast<int64_t>(ptr), pl::PL_RECORD_TYPE, init_size);
                  if (OB_FAIL(ret)) {
                    int tmp_ret = OB_SUCCESS;
                    if (OB_SUCCESS != (tmp_ret = pl::ObUserDefinedType::destruct_objparam(*local_allocator, *current_obj))) {
                      LOG_WARN("dblink destruct_objparam failed", K(ret), K(tmp_ret));
                    }
                  }
                }
              }
            }
          } // end for array_size
          if (OB_SUCC(ret)) {
            coll->set_data(new_data, array_size);
            coll->set_count(array_size);
            coll->set_first(1);
            coll->set_last(array_size);
          } else if (NULL != new_data) {
            for (int64_t j = 0; j < i; j++) {
              int tmp_ret = OB_SUCCESS;
              if (OB_SUCCESS != (tmp_ret = pl::ObUserDefinedType::destruct_objparam(*local_allocator, new_data[j]))) {
                LOG_WARN("dblink destruct_objparam failed", K(ret), K(tmp_ret));
              }
            }
            local_allocator->free(new_data);
          }
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is error", K(ret));
  }
  return ret;
}

int ObMySQLProcStatement::process_array_element(int64_t pos,
                                                ObIAllocator &allocator,
                                                void *buffer,
                                                ObObj &param,
                                                const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  MYSQL_COMPLEX_BIND_HEADER *header = NULL;
  MYSQL_COMPLEX_BIND_BASIC *bb = NULL;
  MYSQL_COMPLEX_BIND_STRING *bs = NULL;
  ObObjType obj_type = param.get_type();
  CK (OB_NOT_NULL(buffer));
  CK (OB_NOT_NULL(header = (MYSQL_COMPLEX_BIND_HEADER *)buffer));
  CK (OB_NOT_NULL(bb = ((MYSQL_COMPLEX_BIND_BASIC *)buffer) + pos));
  CK (OB_NOT_NULL(bs = ((MYSQL_COMPLEX_BIND_STRING *)buffer) + pos));
#define SET_FIXED_LEN_TYPE_VALUE(TYPE, TYPE_1)  \
  if (bb->is_null) {  \
    param.set_null(); \
  } else {  \
    param.set_##TYPE(*(reinterpret_cast<TYPE_1*>(bb->buffer))); \
  }
  if (OB_SUCC(ret)) {
    switch (obj_type) {
      case ObNullType:
        param.set_null();
        break;
      case ObTinyIntType:
        SET_FIXED_LEN_TYPE_VALUE(tinyint, int8_t);
        break;
      case ObSmallIntType:
        SET_FIXED_LEN_TYPE_VALUE(smallint, int16_t);
        break;
      case ObMediumIntType:
        SET_FIXED_LEN_TYPE_VALUE(mediumint, int32_t);
        break;
      case ObInt32Type:
        SET_FIXED_LEN_TYPE_VALUE(int32, int32_t);
        break;
      case ObIntType:
        SET_FIXED_LEN_TYPE_VALUE(int, int64_t);
        break;
      case ObFloatType:
        SET_FIXED_LEN_TYPE_VALUE(float, float);
        break;
      case ObDoubleType:
        SET_FIXED_LEN_TYPE_VALUE(double, double);
        break;
      case ObNumberType:
        {
          if (bs->is_null) {
            param.set_null();
          } else {
            number::ObNumber nb;
            OZ (nb.from((char *)bs->buffer, bs->length, allocator));
            OX (param.set_number(nb));
          }
        }
        break;
      case ObNumberFloatType:
        {
          if (bs->is_null) {
            param.set_null();
          } else {
            number::ObNumber nb;
            OZ (nb.from((char *)bs->buffer, bs->length, allocator));
            OX (param.set_number_float(nb));
          }
        }
        break;
      case ObVarcharType:
      case ObCharType:
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
        {
          if (bs->is_null) {
            param.set_null();
          } else {
            OZ (store_string_obj(param, obj_type, allocator, (int64_t)bs->length, (char *)bs->buffer));
          }
        }
        break;
      case ObDateTimeType:
      case ObTimestampType:
        {
          if (bb->is_null) {
            param.set_null();
          } else {
            ObTime ob_time;
            ObPreciseDateTime value;
            MYSQL_TIME *tm = reinterpret_cast<MYSQL_TIME *>(bb->buffer);
            enum_field_types field_type = (enum_field_types)ob_type_to_mysql_type[obj_type];
            // if (0 == bind_param.length_) {
            //   value = 0;
            // } else
            {
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
                if (OB_FAIL(ObTimeConverter::datetime_to_timestamp(value, tz_info, ts_value))) {
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
          }
        }
        break;
      case ObTimestampTZType:
      case ObTimestampLTZType:
      case ObTimestampNanoType:
        {
          if (bb->is_null) {
            param.set_null();
          } else {
            const bool is_tz = (ObTimestampTZType == obj_type);
            ObOTimestampData ot_data;
            ot_data.reset();
            ORACLE_TIME *ora_time = reinterpret_cast<ORACLE_TIME *>(bb->buffer);
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
              ObTimeConvertCtx ctx(tz_info, ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT, true);
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
                if (OB_FAIL(ObTimeConverter::ob_time_to_utc(obj_type, ctx, ob_time))) {
                  LOG_WARN("failed to convert ob_time to utc", K(ret));
                } else if (OB_FAIL(ObTimeConverter::ob_time_to_otimestamp(ob_time, ot_data))) {
                  LOG_WARN("failed to ob_time to otimestamp", K(ret));
                } else {
                  param.set_otimestamp_value(param.get_type(), ot_data);
                }
              }
            }
          }
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type is error", K(ret), K(obj_type));
    }
  }
#undef SET_FIXED_LEN_TYPE_VALUE
  return ret;
}
#endif

int ObMySQLProcStatement::store_string_obj(ObObj &param,
                                           ObObjType obj_type,
                                           ObIAllocator &allocator,
                                           const int64_t length,
                                           char *buffer)
{
  int ret = OB_SUCCESS;
  ObString dst(length, buffer);
  if (OB_FAIL(ob_write_string(allocator, dst, dst))) {
    LOG_WARN("failed to write str", K(ret));
  } else {
    switch (obj_type) {
      case ObVarcharType:
        param.set_varchar(dst);
        break;
      case ObCharType:
        param.set_char(dst);
        break;
      case ObTinyTextType:
        param.set_lob_value(ObTinyTextType, dst.ptr(), dst.length());
        break;
      case ObTextType:
        param.set_lob_value(ObTextType, dst.ptr(), dst.length());
        break;
      case ObMediumTextType:
        param.set_lob_value(ObMediumTextType, dst.ptr(), dst.length());
        break;
      case ObLongTextType:
        param.set_lob_value(ObLongTextType, dst.ptr(), dst.length());
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown type", K(ret), K(obj_type));
        break;
    }
  }
  return ret;
}

int ObMySQLProcStatement::init(ObMySQLConnection &conn,
                               const ObString &sql,
                               int64_t param_count)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
#else
  conn_ = &conn;
  stmt_param_count_ = param_count;
  in_out_map_.reset();
  com_datas_.reset();
  out_param_start_pos_ = 0;
  out_param_cur_pos_ = 0;
  proc_ = sql;
  if (OB_ISNULL(proc_.ptr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql", K(sql), K(ret));
  } else if (OB_ISNULL(conn_) || OB_ISNULL(conn_->get_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn_ is NULL", K(ret));
  } else if (FALSE_IT(conn_->get_handler()->field_count = 0)) {
    // After executing the previous statement, libObClient did not set the value of `mysql->filed_count` to 0,
    // which may cause coredump. Set it manually here.
  } else if (OB_ISNULL(stmt_ = mysql_stmt_init(conn_->get_handler()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to init stmt", K(ret));
  } else if (0 != mysql_stmt_prepare_v2(stmt_, sql.ptr(), sql.length(), &stmt_param_count_)) {
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
                                       const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                                       const ObTimeZoneInfo *tz_info,
                                       ObObj *result,
                                       bool is_sql,
                                       int64_t out_param_start_pos,
                                       int64_t basic_param_start_pos,
                                       int64_t basic_return_value_pos)
{
  int ret = OB_SUCCESS;
  // pair.first is: out param position in @this.bind_params_
  // pair.second is: out param position in @params, if the routine is a function, the values is -1
  common::ObSEArray<std::pair<int64_t, int64_t>, 8> basic_out_param;
  void* execute_extend_arg = NULL;
  out_param_start_pos_ = out_param_start_pos;
  out_param_cur_pos_ = out_param_start_pos;
  basic_param_start_pos_ = basic_param_start_pos;
  basic_return_value_pos_ = basic_return_value_pos;
  if (OB_ISNULL(tz_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tz info is null", K(ret));
  } else if (OB_FAIL(bind_proc_param(allocator, params, routine_info, udts, basic_out_param, tz_info, result, is_sql))) {
    LOG_WARN("failed to bind proc param", K(ret));
  } else if (OB_FAIL(param_.bind_param())) {
    LOG_WARN("failed to bind prepared input param", "info", mysql_error(conn_->get_handler()), K(ret));
  } else if (OB_FAIL(execute_stmt_v2_interface())) {
    LOG_WARN("failed to execute PL", K(ret));
  } else if (OB_FAIL(process_proc_output_params(allocator, params, routine_info, udts, basic_out_param,
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
  uint64_t ps_stmt_checksum = ob_crc64(proc_.ptr(), proc_.length());
  LOG_INFO("execute v2 info", K(ret), K(ps_stmt_checksum), K(proc_));
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
  if (NULL != res) {
    mysql_free_result(res);
    res = NULL;
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
  out_param_start_pos_ = 0;
  out_param_cur_pos_ = 0;
  com_datas_.reset();
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
  } else if (OB_ISNULL(proc_.ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proc_ is NULL", K(ret));
  } else if (0 != mysql_stmt_execute_v2(stmt_, proc_.ptr(), proc_.length(), 1, 0, execute_extend_arg)) {
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

int ObMySQLProcStatement::get_udt_by_id(uint64_t user_type_id,
                                        const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                                        const pl::ObUserDefinedType *&udt)
{
  int ret = OB_SUCCESS;
  udt = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < udts.count(); ++i) {
    if (udts.at(i)->get_user_type_id() == user_type_id) {
      udt = udts.at(i);
      break;
    }
  }
  OV (OB_NOT_NULL(udt), OB_ERR_UNEXPECTED, K(user_type_id), K(udts));
  return ret;
}

int ObMySQLProcStatement::get_udt_by_name(const ObString &name,
                                          const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                                          const pl::ObUserDefinedType *&udt)
{
  int ret = OB_SUCCESS;
  udt = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < udts.count(); ++i) {
    if (name.case_compare(udts.at(i)->get_name()) == 0) {
      udt = udts.at(i);
      break;
    }
  }
  CK (OB_NOT_NULL(udt));
  return ret;
}

bool ObMySQLProcStatement::is_in_param(const share::schema::ObRoutineParam &r_param)
{
  return r_param.is_in_param() || r_param.is_inout_param();
}

bool ObMySQLProcStatement::is_out_param(const share::schema::ObRoutineParam &r_param) {
  return r_param.is_out_param() || r_param.is_inout_param();
}

int ObMySQLProcStatement::get_anonymous_param_count(ParamStore &params,
                                                    const share::schema::ObRoutineInfo &routine_info,
                                                    const common::ObIArray<const pl::ObUserDefinedType *> &udts,
                                                    bool is_sql,
                                                    int64_t &param_cnt,
                                                    int64_t &out_param_start_pos,
                                                    int64_t &basic_param_start_pos,
                                                    int64_t &basic_return_value_pos)
{
  int ret = OB_SUCCESS;
  param_cnt = 0;
  out_param_start_pos = 0;
  basic_param_start_pos = 0;
  basic_return_value_pos = 0;
  const share::schema::ObRoutineParam *r_param = NULL;
  const pl::ObUserDefinedType *udt = NULL;
  bool return_basic = false;
  if (routine_info.is_function()) {
    const share::schema::ObRoutineParam *r_param = routine_info.get_routine_params().at(0);
    CK (OB_NOT_NULL(r_param));
    if (OB_FAIL(ret)) {
    } else if (r_param->is_dblink_type()) {
      OZ (ObMySQLProcStatement::get_udt_by_name(r_param->get_type_name(), udts, udt));
      CK (OB_NOT_NULL(udt));
      if (OB_SUCC(ret)) {
        if (udt->is_record_type()) {
          param_cnt += udt->get_member_count();
        } else if (udt->is_collection_type()) {
          const pl::ObCollectionType *coll_type = static_cast<const pl::ObCollectionType *>(udt);
          CK (OB_NOT_NULL(coll_type));
          if (OB_FAIL(ret)) {
          } else if (coll_type->get_element_type().is_obj_type()) {
            param_cnt++;
            if (!coll_type->is_associative_array_type()) {
              param_cnt++;
            }
          } else if (coll_type->get_element_type().is_record_type()) {
            const pl::ObUserDefinedType *inner = NULL;
            OZ (get_udt_by_id(coll_type->get_element_type().get_user_type_id(), udts, inner));
            CK (OB_NOT_NULL(inner));
            if (OB_SUCC(ret)) {
              param_cnt += inner->get_member_count();
              if (!coll_type->is_associative_array_type()) {
                param_cnt++;
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("type is error", K(ret), KPC(coll_type));
          }
        }
      }
    } else {
      param_cnt++;
      out_param_start_pos++;
      return_basic = true;
    }
  }
  int64_t start_idx = routine_info.is_function() ? 1 : 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
    if (!params.at(i).is_pl_mock_default_param()) {
      r_param = routine_info.get_routine_params().at(start_idx + i);
      CK (OB_NOT_NULL(r_param));
      if (OB_SUCC(ret)) {
        if (r_param->is_dblink_type()) {
          OZ (ObMySQLProcStatement::get_udt_by_name(r_param->get_type_name(), udts, udt));
          CK (OB_NOT_NULL(udt));
          if (OB_SUCC(ret)) {
            if (udt->is_record_type()) {
              if (ObMySQLProcStatement::is_in_param(*r_param)) {
                param_cnt += udt->get_member_count();
                out_param_start_pos += udt->get_member_count();
                basic_param_start_pos += udt->get_member_count();
              }
              if (ObMySQLProcStatement::is_out_param(*r_param)) {
                param_cnt += udt->get_member_count();
              }
            } else if (udt->is_collection_type()) {
              const pl::ObCollectionType *coll_type = static_cast<const pl::ObCollectionType *>(udt);
              if (OB_ISNULL(coll_type)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("coll type is NULL", K(ret));
              } else if (coll_type->get_element_type().is_obj_type()) {
                if (is_in_param(*r_param)) {
                  param_cnt++;
                  out_param_start_pos++;
                  basic_param_start_pos++;
                  if (!coll_type->is_associative_array_type()) {
                    param_cnt++;
                    out_param_start_pos++;
                    basic_param_start_pos++;
                  }
                }
                if (is_out_param(*r_param)) {
                  param_cnt++;
                  if (!coll_type->is_associative_array_type()) {
                    param_cnt++;
                  }
                }
              } else if (coll_type->get_element_type().is_record_type()) {
                const pl::ObUserDefinedType *inner = NULL;
                OZ (get_udt_by_id(coll_type->get_element_type().get_user_type_id(), udts, inner));
                CK (OB_NOT_NULL(inner));
                if (OB_SUCC(ret)) {
                  if (is_in_param(*r_param)) {
                    param_cnt += inner->get_member_count();
                    out_param_start_pos += inner->get_member_count();
                    basic_param_start_pos += inner->get_member_count();
                    if (!coll_type->is_associative_array_type()) {
                      param_cnt++;
                      out_param_start_pos++;
                      basic_param_start_pos++;
                    }
                  }
                  if (is_out_param(*r_param)) {
                    param_cnt += inner->get_member_count();
                    if (!coll_type->is_associative_array_type()) {
                      param_cnt++;
                    }
                  }
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("type is error", K(ret), KPC(coll_type));
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("type is error", K(ret), KPC(udt));
            }
          }
        } else {
          param_cnt++;
          out_param_start_pos++;
        }
      }
    }
  }
  if (OB_SUCC(ret) && return_basic) {
    if (is_sql) {
      basic_return_value_pos = out_param_start_pos - 1;
    } else {
      basic_return_value_pos = basic_param_start_pos;
    }
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObMySQLProcStatement::build_array_isnull(int64_t position,
                                             bool is_in,
                                             ObObj &param)
{
  int ret = OB_SUCCESS;
  ObBindParam *bind_param = NULL;
  enum_field_types buffer_type = MYSQL_TYPE_LONGLONG;
  OZ (get_bind_param_by_idx(position, bind_param));
  CK (OB_NOT_NULL(bind_param));
  if (OB_SUCC(ret)) {
    bind_param->col_idx_ = position;
    bind_param->buffer_type_ = buffer_type;
    bind_param->buffer_len_ = sizeof(bind_param->array_is_null_);
    if (is_in) {
      pl::ObPLCollection *coll = reinterpret_cast<pl::ObPLCollection *>(param.get_ext());
      if (OB_ISNULL(coll) || -1 == coll->get_count()) {
        bind_param->array_is_null_ = 1;
      }
    }
    bind_param->buffer_ = &bind_param->array_is_null_;
    OZ (param_.bind_param(*bind_param));
  }
  return ret;
}

int ObMySQLProcStatement::store_array_element(ObObj &obj,
                                              MYSQL_COMPLEX_BIND_HEADER *header,
                                              ObIAllocator &allocator,
                                              const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  if (obj.is_null()) {
    header->is_null = 1;
  } else {
    switch (obj.get_type()) {
      case ObVarcharType:
      case ObCharType:
        {
          MYSQL_COMPLEX_BIND_STRING *bs = (MYSQL_COMPLEX_BIND_STRING *)header;
          bs->length = obj.val_len_;
          bs->buffer = (void *)obj.v_.string_;
        }
        break;
      case ObIntType:
      case ObInt32Type:
        {
          MYSQL_COMPLEX_BIND_BASIC *bb = (MYSQL_COMPLEX_BIND_BASIC *)header;
          bb->buffer = &(obj.v_.int64_);
        }
        break;
      case ObFloatType:
        {
          float *buf = NULL;
          OV (OB_NOT_NULL(buf = reinterpret_cast<float *>(allocator.alloc(sizeof(float)))), OB_ALLOCATE_MEMORY_FAILED);
          if (OB_SUCC(ret)) {
            *buf = obj.v_.float_;
            header->buffer = buf;
          }
        }
        break;
      case ObDoubleType:
        {
          // double *buf = NULL;
          // OV (OB_NOT_NULL(buf = reinterpret_cast<double *>(allocator.alloc(sizeof(double)))), OB_ALLOCATE_MEMORY_FAILED);
          // if (OB_SUCC(ret)) {
          //   *buf = obj.v_.double_;
          //   header->buffer = buf;
          // }
          // 远程协议端对于Oracle 模式的Double, 在 parse_basic_param_value 函数中映射到了number，导致后面计算
          // ObExprObjAccess::ExtraInfo::calc 是number 类型和 binary_double不匹配报错，这里先报错不支持
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "composite type's element type is BINARY_DOUBLE");
        }
        break;
      case ObNumberType:
        {
          MYSQL_COMPLEX_BIND_DECIMAL *bd = (MYSQL_COMPLEX_BIND_DECIMAL *)header;
          int64_t pos = 0;
          char *buf = NULL;
          number::ObNumber num;
          const int64_t buf_len = OB_CAST_TO_VARCHAR_MAX_LENGTH;
          OV (OB_NOT_NULL(buf = reinterpret_cast<char *>(allocator.alloc(buf_len))), OB_ALLOCATE_MEMORY_FAILED);
          OZ (obj.get_number(num));
          OZ (num.format(buf, buf_len, pos, obj.get_scale()));
          if (OB_SUCC(ret)) {
            bd->length = pos;
            bd->buffer = buf;
          }
        }
        break;
      case ObNumberFloatType:
        {
          int64_t pos = 0;
          void *buf = NULL;
          int64_t buf_len = 41;
          OV (OB_NOT_NULL(buf = reinterpret_cast<char *>(allocator.alloc(buf_len))), OB_ALLOCATE_MEMORY_FAILED);
          OZ (obj.get_number().format_v1((char *)buf, buf_len, pos, obj.get_scale()));
          if (OB_SUCC(ret)) {
            MYSQL_COMPLEX_BIND_STRING *bs = (MYSQL_COMPLEX_BIND_STRING *)header;
            bs->length = pos;
            bs->buffer = buf;
          }
        }
        break;
      case ObDateTimeType:
      case ObTimestampType:
        {
          MYSQL_COMPLEX_BIND_BASIC *bb = (MYSQL_COMPLEX_BIND_BASIC *)header;
          ObTime ob_time;
          MYSQL_TIME *tm = nullptr;
          const ObObjType obj_type = obj.get_type();
          const ObTimeZoneInfo *tmp_tz = (obj_type == ObObjType::ObDateTimeType ? NULL : tz_info);
          OV (OB_NOT_NULL(tm = reinterpret_cast<MYSQL_TIME *>(allocator.alloc(sizeof(MYSQL_TIME)))),
                          OB_ALLOCATE_MEMORY_FAILED);
          OZ (ObTimeConverter::datetime_to_ob_time(obj.get_datetime(), tmp_tz, ob_time));
          if (OB_SUCC(ret)) {
            MEMSET(tm, 0, sizeof(MYSQL_TIME));
            tm->year = ob_time.parts_[DT_YEAR];
            tm->month = ob_time.parts_[DT_MON];
            tm->day = ob_time.parts_[DT_MDAY];
            tm->hour = ob_time.parts_[DT_HOUR];
            tm->minute = ob_time.parts_[DT_MIN];
            tm->second = ob_time.parts_[DT_SEC];
            tm->second_part = ob_time.parts_[DT_USEC];
            tm->neg = DT_MODE_NEG & ob_time.mode_;
            bb->buffer = (void *)tm;
          }
        }
        break;
      case ObTimestampTZType:
      case ObTimestampLTZType:
      case ObTimestampNanoType:
        {
          const ObObjType obj_type = obj.get_type();
          const bool is_tz = (ObTimestampTZType == obj_type);
          ORACLE_TIME *ora_time = NULL;
          int64_t len = sizeof(ORACLE_TIME);
          const ObOTimestampData &ot_data = obj.get_otimestamp_value();
          ObTime ob_time(DT_TYPE_ORACLE_TIMESTAMP);
          OZ (ObTimeConverter::otimestamp_to_ob_time(obj_type, ot_data, tz_info, ob_time, false));
          OV (ObTimeConverter::valid_oracle_year(ob_time), OB_ERR_DATETIME_INTERVAL_INTERNAL_ERROR);
          OV (OB_NOT_NULL(ora_time = reinterpret_cast<ORACLE_TIME *>(allocator.alloc(len))), OB_ALLOCATE_MEMORY_FAILED);
          if (OB_SUCC(ret)) {
            MEMSET(ora_time, 0, sizeof(ORACLE_TIME));
            const int32_t unsigned_year = ob_time.parts_[DT_YEAR] >= 0 ?
                                          ob_time.parts_[DT_YEAR] : (0 - ob_time.parts_[DT_YEAR]);
            int32_t century = static_cast<int32_t>(unsigned_year / YEARS_PER_CENTURY
                                          * (ob_time.parts_[DT_YEAR] >= 0 ? 1 : -1));
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
            ora_time->scale = obj.get_scale();
            if (is_tz) {
              if (!ob_time.get_tz_name_str().empty()) {
                ora_time->tz_name = ob_time.get_tz_name_str().ptr();
              }
              if (!ob_time.get_tzd_abbr_str().empty()) {
                ora_time->tz_abbr = ob_time.get_tzd_abbr_str().ptr();
              }
              const int32_t unsigned_offset = (ob_time.parts_[DT_OFFSET_MIN] >= 0 ?
                                      ob_time.parts_[DT_OFFSET_MIN] : (0 - ob_time.parts_[DT_OFFSET_MIN]));
              int32_t offset_hour = static_cast<int32_t>(unsigned_offset / MINS_PER_HOUR
                                    * (ob_time.parts_[DT_OFFSET_MIN] >= 0 ? 1 : -1));
              int32_t offset_minute = static_cast<int32_t>(unsigned_offset % MINS_PER_HOUR);
              if (0 == offset_hour) {
                offset_minute *= static_cast<int32_t>(ob_time.parts_[DT_OFFSET_MIN] >= 0 ? 1 : -1);
              }
              ora_time->offset_hour = offset_hour;
              ora_time->offset_minute = offset_minute;
            }
            header->buffer = ora_time;
          }
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type is error", K(ret), K(obj));
    }
  }
  return ret;
}
#endif

} // end namespace sqlcient
} // end namespace common
} // end namespace oceanbase
