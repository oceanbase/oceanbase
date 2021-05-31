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

#include "obsm_utils.h"

#include "lib/time/ob_time_utility.h"
#include "lib/charset/ob_dtoa.h"
#include "common/ob_field.h"
#include "share/schema/ob_schema_getter_guard.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::share::schema;

struct ObMySQLTypeMap {
  /* oceanbase::common::ObObjType ob_type; */
  EMySQLFieldType mysql_type;
  uint16_t flags;  /* flags if Field */
  uint64_t length; /* other than varchar type */
};

// @todo
// reference: https://dev.mysql.com/doc/refman/5.6/en/c-api-data-structures.html
// reference: http://dev.mysql.com/doc/internals/en/client-server-protocol.html
static const ObMySQLTypeMap type_maps_[ObMaxType] = {
    /* ObMinType */
    {EMySQLFieldType::MYSQL_TYPE_NULL, OB_MYSQL_BINARY_FLAG, 0},                                /* ObNullType */
    {EMySQLFieldType::MYSQL_TYPE_TINY, OB_MYSQL_BINARY_FLAG, 0},                                /* ObTinyIntType */
    {EMySQLFieldType::MYSQL_TYPE_SHORT, OB_MYSQL_BINARY_FLAG, 0},                               /* ObSmallIntType */
    {EMySQLFieldType::MYSQL_TYPE_INT24, OB_MYSQL_BINARY_FLAG, 0},                               /* ObMediumIntType */
    {EMySQLFieldType::MYSQL_TYPE_LONG, OB_MYSQL_BINARY_FLAG, 0},                                /* ObInt32Type */
    {EMySQLFieldType::MYSQL_TYPE_LONGLONG, OB_MYSQL_BINARY_FLAG, 0},                            /* ObIntType */
    {EMySQLFieldType::MYSQL_TYPE_TINY, OB_MYSQL_BINARY_FLAG | OB_MYSQL_UNSIGNED_FLAG, 0},       /* ObUTinyIntType */
    {EMySQLFieldType::MYSQL_TYPE_SHORT, OB_MYSQL_BINARY_FLAG | OB_MYSQL_UNSIGNED_FLAG, 0},      /* ObUSmallIntType */
    {EMySQLFieldType::MYSQL_TYPE_INT24, OB_MYSQL_BINARY_FLAG | OB_MYSQL_UNSIGNED_FLAG, 0},      /* ObUMediumIntType */
    {EMySQLFieldType::MYSQL_TYPE_LONG, OB_MYSQL_BINARY_FLAG | OB_MYSQL_UNSIGNED_FLAG, 0},       /* ObUInt32Type */
    {EMySQLFieldType::MYSQL_TYPE_LONGLONG, OB_MYSQL_BINARY_FLAG | OB_MYSQL_UNSIGNED_FLAG, 0},   /* ObUInt64Type */
    {EMySQLFieldType::MYSQL_TYPE_FLOAT, OB_MYSQL_BINARY_FLAG, 0},                               /* ObFloatType */
    {EMySQLFieldType::MYSQL_TYPE_DOUBLE, OB_MYSQL_BINARY_FLAG, 0},                              /* ObDoubleType */
    {EMySQLFieldType::MYSQL_TYPE_FLOAT, OB_MYSQL_BINARY_FLAG | OB_MYSQL_UNSIGNED_FLAG, 0},      /* ObUFloatType */
    {EMySQLFieldType::MYSQL_TYPE_DOUBLE, OB_MYSQL_BINARY_FLAG | OB_MYSQL_UNSIGNED_FLAG, 0},     /* ObUDoubleType */
    {EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL, OB_MYSQL_BINARY_FLAG, 0},                          /* ObNumberType */
    {EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL, OB_MYSQL_BINARY_FLAG | OB_MYSQL_UNSIGNED_FLAG, 0}, /* ObUNumberType */
    {EMySQLFieldType::MYSQL_TYPE_DATETIME, OB_MYSQL_BINARY_FLAG, 0},                            /* ObDateTimeType */
    {EMySQLFieldType::MYSQL_TYPE_TIMESTAMP, OB_MYSQL_BINARY_FLAG | OB_MYSQL_TIMESTAMP_FLAG, 0}, /* ObTimestampType */
    {EMySQLFieldType::MYSQL_TYPE_DATE, OB_MYSQL_BINARY_FLAG, 0},                                /* ObDateType */
    {EMySQLFieldType::MYSQL_TYPE_TIME, OB_MYSQL_BINARY_FLAG, 0},                                /* ObTimeType */
    {EMySQLFieldType::MYSQL_TYPE_YEAR, OB_MYSQL_UNSIGNED_FLAG | OB_MYSQL_ZEROFILL_FLAG, 0},     /* ObYearType */
    {EMySQLFieldType::MYSQL_TYPE_VAR_STRING, 0, 0},                                             /* ObVarcharType */
    {EMySQLFieldType::MYSQL_TYPE_STRING, 0, 0},                                                 /* ObCharType */
    {EMySQLFieldType::MYSQL_TYPE_VAR_STRING, OB_MYSQL_BINARY_FLAG, 0},                          /* ObHexStringType */
    {EMySQLFieldType::MYSQL_TYPE_COMPLEX, 0, 0},                                                /* ObExtendType */
    {EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED, 0, 0},                                            /* ObUnknownType */
    {EMySQLFieldType::MYSQL_TYPE_TINY_BLOB, OB_MYSQL_BLOB_FLAG, 0},                             /* ObTinyTextType */
    {EMySQLFieldType::MYSQL_TYPE_BLOB, OB_MYSQL_BLOB_FLAG, 0},                                  /* ObTextType */
    {EMySQLFieldType::MYSQL_TYPE_MEDIUM_BLOB, OB_MYSQL_BLOB_FLAG, 0},                           /* ObMediumTextType */
    {EMySQLFieldType::MYSQL_TYPE_LONG_BLOB, OB_MYSQL_BLOB_FLAG, 0},                             /* ObLongTextType */
    {EMySQLFieldType::MYSQL_TYPE_BIT, OB_MYSQL_UNSIGNED_FLAG, 0},                               /* ObBitType */
    {EMySQLFieldType::MYSQL_TYPE_STRING, OB_MYSQL_ENUM_FLAG, 0},                                /* ObEnumType */
    {EMySQLFieldType::MYSQL_TYPE_STRING, OB_MYSQL_SET_FLAG, 0},                                 /* ObSetType */
    {EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED, 0, 0},                                            /* ObEnumInnerType */
    {EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED, 0, 0},                                            /* ObSetInnerType */
    {EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE,
        OB_MYSQL_BINARY_FLAG | OB_MYSQL_TIMESTAMP_FLAG,
        0}, /* ObTimestampTZType */
    {EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        OB_MYSQL_BINARY_FLAG | OB_MYSQL_TIMESTAMP_FLAG,
        0}, /* ObTimestampLTZType */
    {EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_NANO,
        OB_MYSQL_BINARY_FLAG | OB_MYSQL_TIMESTAMP_FLAG,
        0},                                                                 /* ObTimestampNanoType */
    {EMySQLFieldType::MYSQL_TYPE_OB_RAW, OB_MYSQL_BINARY_FLAG, 0},          /* ObRawType */
    {EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_YM, OB_MYSQL_BINARY_FLAG, 0},  /* ObIntervalYMType */
    {EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_DS, OB_MYSQL_BINARY_FLAG, 0},  /* ObIntervalDSType */
    {EMySQLFieldType::MYSQL_TYPE_OB_NUMBER_FLOAT, OB_MYSQL_BINARY_FLAG, 0}, /* ObNumberFloatType */
    {EMySQLFieldType::MYSQL_TYPE_OB_NVARCHAR2, 0, 0},                       /* ObNVarchar2Type */
    {EMySQLFieldType::MYSQL_TYPE_OB_NCHAR, 0, 0},                           /* ObNCharType */
    {EMySQLFieldType::MYSQL_TYPE_OB_UROWID, 0, 0},
    {EMySQLFieldType::MYSQL_TYPE_ORA_BLOB, 0, 0}, /* ObLobType */
                                                  /* ObMaxType */
};

// called by handle OB_MYSQL_COM_STMT_EXECUTE offset is 0
bool ObSMUtils::update_from_bitmap(ObObj& param, const char* bitmap, int64_t field_index)
{
  bool ret = false;
  int byte_pos = static_cast<int>(field_index / 8);
  int bit_pos = static_cast<int>(field_index % 8);
  char value = bitmap[byte_pos];
  if (value & (1 << bit_pos)) {
    ret = true;
    param.set_null();
  }
  return ret;
}

int ObSMUtils::cell_str(char* buf, const int64_t len, const ObObj& obj, MYSQL_PROTOCOL_TYPE type, int64_t& pos,
    int64_t cell_idx, char* bitmap, const ObDataTypeCastParams& dtc_params, const ObField* field,
    ObSchemaGetterGuard* schema_guard, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  ObScale scale = 0;
  ObPrecision precision = 0;
  bool zerofill = false;
  int32_t zflength = 0;
  bool is_oracle_raw = false;
  if (NULL == field) {
    if (OB_UNLIKELY(obj.is_invalid_type())) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      scale = ObAccuracy::DML_DEFAULT_ACCURACY[obj.get_type()].get_scale();
    }
  } else {
    scale = field->accuracy_.get_scale();
    precision = field->accuracy_.get_precision();
    zerofill = field->flags_ & OB_MYSQL_ZEROFILL_FLAG;
    zflength = field->length_;
    OB_LOG(DEBUG,
        "get field accuracy_",
        K(field->accuracy_),
        K(zerofill),
        KPC(field),
        K(obj),
        K(schema_guard),
        K(tenant_id));
  }
  if (OB_SUCC(ret)) {
    switch (obj.get_type_class()) {
      case ObNullTC:
        ret = ObMySQLUtil::null_cell_str(buf, len, type, pos, cell_idx, bitmap);
        break;
      case ObIntTC:
        ret = ObMySQLUtil::int_cell_str(buf, len, obj.get_int(), obj.get_type(), false, type, pos, zerofill, zflength);
        break;
      case ObUIntTC:
        ret = ObMySQLUtil::int_cell_str(buf, len, obj.get_int(), obj.get_type(), true, type, pos, zerofill, zflength);
        break;
      case ObFloatTC:
        ret = ObMySQLUtil::float_cell_str(buf, len, obj.get_float(), type, pos, scale, zerofill, zflength);
        break;
      case ObDoubleTC:
        ret = ObMySQLUtil::double_cell_str(buf, len, obj.get_double(), type, pos, scale, zerofill, zflength);
        break;
      case ObNumberTC:
        ret = ObMySQLUtil::number_cell_str(buf, len, obj.get_number(), pos, scale, zerofill, zflength);
        break;
      case ObDateTimeTC:
        ret = ObMySQLUtil::datetime_cell_str(
            buf, len, obj.get_datetime(), type, pos, (obj.is_timestamp() ? dtc_params.tz_info_ : NULL), scale);
        break;
      case ObDateTC:
        ret = ObMySQLUtil::date_cell_str(buf, len, obj.get_date(), type, pos);
        break;
      case ObTimeTC:
        ret = ObMySQLUtil::time_cell_str(buf, len, obj.get_time(), type, pos, scale);
        break;
      case ObYearTC:
        ret = ObMySQLUtil::year_cell_str(buf, len, obj.get_year(), type, pos);
        break;
      case ObOTimestampTC:
        ret = ObMySQLUtil::otimestamp_cell_str(
            buf, len, obj.get_otimestamp_value(), type, pos, dtc_params, scale, obj.get_type());
        break;
      case ObRawTC:
      case ObTextTC:  // TODO texttc share the stringtc temporarily
      case ObStringTC:
      // lob locator will encode to varchar, first encode LobLocator length, then whole lob locator.
      case ObLobTC: {
        ret = ObMySQLUtil::varchar_cell_str(buf, len, obj.get_string(), is_oracle_raw, pos);
        break;
      }
      case ObBitTC: {
        int32_t bit_len = 0;
        if (OB_LIKELY(precision > 0)) {
          bit_len = precision;
        } else {
          bit_len = ObAccuracy::MAX_ACCURACY[obj.get_type()].precision_;
          _OB_LOG(WARN, "max precision is used. origin precision is %d", precision);
        }
        ret = ObMySQLUtil::bit_cell_str(buf, len, obj.get_bit(), bit_len, type, pos);
        break;
      }
      case ObIntervalTC: {
        ret = obj.is_interval_ym()
                  ? ObMySQLUtil::interval_ym_cell_str(buf, len, obj.get_interval_ym(), type, pos, scale)
                  : ObMySQLUtil::interval_ds_cell_str(buf, len, obj.get_interval_ds(), type, pos, scale);
        break;
      }
      case ObRowIDTC: {
        if (obj.is_urowid()) {
          ret = ObMySQLUtil::urowid_cell_str(buf, len, obj.get_urowid(), pos);
        } else {
          ret = OB_NOT_SUPPORTED;
          OB_LOG(WARN, "not support rowid type for now", K(ret));
        }
        break;
      }
      default:
        _OB_LOG(ERROR, "invalid ob type=%d", obj.get_type());
        ret = OB_ERROR;
        break;
    }
  }
  return ret;
}

int get_map(ObObjType ob_type, const ObMySQLTypeMap*& map)
{
  int ret = OB_SUCCESS;
  if (ob_type >= ObMaxType) {
    ret = OB_OBJ_TYPE_ERROR;
  }

  if (OB_SUCC(ret)) {
    map = type_maps_ + ob_type;
  }

  return ret;
}

int ObSMUtils::get_type_length(ObObjType ob_type, int64_t& length)
{
  const ObMySQLTypeMap* map = NULL;
  int ret = OB_SUCCESS;

  if ((ret = get_map(ob_type, map)) == OB_SUCCESS) {
    length = map->length;
  }
  return ret;
}

int ObSMUtils::get_mysql_type(ObObjType ob_type, EMySQLFieldType& mysql_type, uint16_t& flags, ObScale& num_decimals)
{
  const ObMySQLTypeMap* map = NULL;
  int ret = OB_SUCCESS;

  if ((ret = get_map(ob_type, map)) == OB_SUCCESS) {
    mysql_type = map->mysql_type;
    flags |= map->flags;
    // batch fixup num_decimal values
    // so as to be compatible with mysql metainfo
    switch (mysql_type) {
      case EMySQLFieldType::MYSQL_TYPE_LONGLONG:
      case EMySQLFieldType::MYSQL_TYPE_LONG:
      case EMySQLFieldType::MYSQL_TYPE_INT24:
      case EMySQLFieldType::MYSQL_TYPE_SHORT:
      case EMySQLFieldType::MYSQL_TYPE_TINY:
      case EMySQLFieldType::MYSQL_TYPE_NULL:
      case EMySQLFieldType::MYSQL_TYPE_DATE:
      case EMySQLFieldType::MYSQL_TYPE_YEAR:
      case EMySQLFieldType::MYSQL_TYPE_BIT:
        num_decimals = 0;
        break;

      case EMySQLFieldType::MYSQL_TYPE_TINY_BLOB:
      case EMySQLFieldType::MYSQL_TYPE_BLOB:
      case EMySQLFieldType::MYSQL_TYPE_MEDIUM_BLOB:
      case EMySQLFieldType::MYSQL_TYPE_LONG_BLOB:
      case EMySQLFieldType::MYSQL_TYPE_VAR_STRING:
      case EMySQLFieldType::MYSQL_TYPE_STRING:
      case EMySQLFieldType::MYSQL_TYPE_OB_RAW:
      case EMySQLFieldType::MYSQL_TYPE_COMPLEX:
      case EMySQLFieldType::MYSQL_TYPE_OB_NVARCHAR2:
      case EMySQLFieldType::MYSQL_TYPE_OB_NCHAR:
      case EMySQLFieldType::MYSQL_TYPE_OB_UROWID:
      case EMySQLFieldType::MYSQL_TYPE_ORA_BLOB:
      case EMySQLFieldType::MYSQL_TYPE_ORA_CLOB:
        // for compatible with MySQL, ugly convention.
        num_decimals = static_cast<ObScale>(lib::is_oracle_mode() ? ORACLE_NOT_FIXED_DEC : NOT_FIXED_DEC);
        break;
      case EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:
      case EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_NANO:
      case EMySQLFieldType::MYSQL_TYPE_TIMESTAMP:
      case EMySQLFieldType::MYSQL_TYPE_DATETIME:
      case EMySQLFieldType::MYSQL_TYPE_TIME:
      case EMySQLFieldType::MYSQL_TYPE_FLOAT:
      case EMySQLFieldType::MYSQL_TYPE_DOUBLE:
      case EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL:
      case EMySQLFieldType::MYSQL_TYPE_OB_NUMBER_FLOAT:
      case EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_YM:
      case EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_DS:
        num_decimals = static_cast<ObScale>(
            (num_decimals == -1) ? (lib::is_oracle_mode() ? ORACLE_NOT_FIXED_DEC : NOT_FIXED_DEC) : num_decimals);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        _OB_LOG(WARN, "unexpected mysql_type=%d", mysql_type);
        break;
    }  // end switch
  }
  return ret;
}

int ObSMUtils::get_ob_type(ObObjType& ob_type, EMySQLFieldType mysql_type)
{
  int ret = OB_SUCCESS;
  switch (mysql_type) {
    case EMySQLFieldType::MYSQL_TYPE_NULL:
      ob_type = ObNullType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_TINY:
      ob_type = ObTinyIntType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_SHORT:
      ob_type = ObSmallIntType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_LONG:
      ob_type = ObInt32Type;
      break;
    case EMySQLFieldType::MYSQL_TYPE_LONGLONG:
      ob_type = ObIntType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_FLOAT:
      ob_type = ObFloatType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_DOUBLE:
      ob_type = ObDoubleType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_TIMESTAMP:
      ob_type = ObTimestampType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_DATETIME:
      ob_type = ObDateTimeType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_TIME:
      ob_type = ObTimeType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_DATE:
      ob_type = ObDateType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_YEAR:
      ob_type = ObYearType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_VARCHAR:
    case EMySQLFieldType::MYSQL_TYPE_STRING:
    case EMySQLFieldType::MYSQL_TYPE_VAR_STRING:
      ob_type = ObVarcharType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_TINY_BLOB:
      ob_type = ObTinyTextType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_BLOB:
      ob_type = ObTextType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_MEDIUM_BLOB:
      ob_type = ObMediumTextType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_LONG_BLOB:
      ob_type = ObLongTextType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:
      ob_type = ObTimestampTZType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      ob_type = ObTimestampLTZType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_NANO:
      ob_type = ObTimestampNanoType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_OB_RAW:
      ob_type = ObRawType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL:
      ob_type = ObNumberType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_BIT:
      ob_type = ObBitType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_ENUM:
      ob_type = ObEnumType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_SET:
      ob_type = ObSetType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_COMPLEX:
      ob_type = ObExtendType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_YM:
      ob_type = ObIntervalYMType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_DS:
      ob_type = ObIntervalDSType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_OB_NUMBER_FLOAT:
      ob_type = ObNumberFloatType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_OB_NVARCHAR2:
      ob_type = ObNVarchar2Type;
      break;
    case EMySQLFieldType::MYSQL_TYPE_OB_NCHAR:
      ob_type = ObNCharType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_OB_UROWID:
      ob_type = ObURowIDType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_ORA_BLOB:
    case EMySQLFieldType::MYSQL_TYPE_ORA_CLOB:
      ob_type = ObLobType;
      break;
    default:
      _OB_LOG(WARN, "unsupport MySQL type %d", mysql_type);
      ret = OB_OBJ_TYPE_ERROR;
  }
  return ret;
}
