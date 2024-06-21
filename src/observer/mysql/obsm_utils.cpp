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
#include "share/schema/ob_udt_info.h"
#include "pl/ob_pl_user_type.h"
#include "pl/ob_pl_stmt.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/sys_package/ob_sdo_geometry.h"
#endif

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::share::schema;
using namespace oceanbase::pl;

struct ObMySQLTypeMap
{
  /* oceanbase::common::ObObjType ob_type; */
  EMySQLFieldType mysql_type;
  uint16_t flags;         /* flags if Field */
  uint64_t length;        /* other than varchar type */
};

// @todo
// reference: https://dev.mysql.com/doc/refman/5.6/en/c-api-data-structures.html
// reference: http://dev.mysql.com/doc/internals/en/client-server-protocol.html
static const ObMySQLTypeMap type_maps_[ObMaxType] =
{
  /* ObMinType */
  {EMySQLFieldType::MYSQL_TYPE_NULL,      BINARY_FLAG, 0},                        /* ObNullType */
  {EMySQLFieldType::MYSQL_TYPE_TINY,      0, 0},                                  /* ObTinyIntType */
  {EMySQLFieldType::MYSQL_TYPE_SHORT,     0, 0},                                  /* ObSmallIntType */
  {EMySQLFieldType::MYSQL_TYPE_INT24,     0, 0},                                  /* ObMediumIntType */
  {EMySQLFieldType::MYSQL_TYPE_LONG,      0, 0},                                  /* ObInt32Type */
  {EMySQLFieldType::MYSQL_TYPE_LONGLONG,  0, 0},                                  /* ObIntType */
  {EMySQLFieldType::MYSQL_TYPE_TINY,      UNSIGNED_FLAG, 0},                      /* ObUTinyIntType */
  {EMySQLFieldType::MYSQL_TYPE_SHORT,     UNSIGNED_FLAG, 0},                      /* ObUSmallIntType */
  {EMySQLFieldType::MYSQL_TYPE_INT24,     UNSIGNED_FLAG, 0},                      /* ObUMediumIntType */
  {EMySQLFieldType::MYSQL_TYPE_LONG,      UNSIGNED_FLAG, 0},                      /* ObUInt32Type */
  {EMySQLFieldType::MYSQL_TYPE_LONGLONG,  UNSIGNED_FLAG, 0},                      /* ObUInt64Type */
  {EMySQLFieldType::MYSQL_TYPE_FLOAT,     0, 0},                                  /* ObFloatType */
  {EMySQLFieldType::MYSQL_TYPE_DOUBLE,    0, 0},                                  /* ObDoubleType */
  {EMySQLFieldType::MYSQL_TYPE_FLOAT,     UNSIGNED_FLAG, 0},                      /* ObUFloatType */
  {EMySQLFieldType::MYSQL_TYPE_DOUBLE,    UNSIGNED_FLAG, 0},                      /* ObUDoubleType */
  {EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL,0, 0},                                  /* ObNumberType */
  {EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL,UNSIGNED_FLAG, 0},                      /* ObUNumberType */
  {EMySQLFieldType::MYSQL_TYPE_DATETIME,  BINARY_FLAG, 0},                        /* ObDateTimeType */
  {EMySQLFieldType::MYSQL_TYPE_TIMESTAMP, BINARY_FLAG | TIMESTAMP_FLAG, 0},       /* ObTimestampType */
  {EMySQLFieldType::MYSQL_TYPE_DATE,      BINARY_FLAG, 0},                        /* ObDateType */
  {EMySQLFieldType::MYSQL_TYPE_TIME,      BINARY_FLAG, 0},                        /* ObTimeType */
  {EMySQLFieldType::MYSQL_TYPE_YEAR,      UNSIGNED_FLAG | ZEROFILL_FLAG, 0},      /* ObYearType */
  {EMySQLFieldType::MYSQL_TYPE_VAR_STRING,   0, 0},                               /* ObVarcharType */
  {EMySQLFieldType::MYSQL_TYPE_STRING,       0, 0},                               /* ObCharType */
  {EMySQLFieldType::MYSQL_TYPE_VAR_STRING,   BINARY_FLAG, 0},                     /* ObHexStringType */
  {EMySQLFieldType::MYSQL_TYPE_COMPLEX,      0, 0},                               /* ObExtendType */
  {EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,  0, 0},                               /* ObUnknownType */
  {EMySQLFieldType::MYSQL_TYPE_TINY_BLOB,    BLOB_FLAG, 0},                       /* ObTinyTextType */
  {EMySQLFieldType::MYSQL_TYPE_BLOB,         BLOB_FLAG, 0},                       /* ObTextType */
  {EMySQLFieldType::MYSQL_TYPE_MEDIUM_BLOB,  BLOB_FLAG, 0},                       /* ObMediumTextType */
  {EMySQLFieldType::MYSQL_TYPE_LONG_BLOB,    BLOB_FLAG, 0},                       /* ObLongTextType */
  {EMySQLFieldType::MYSQL_TYPE_BIT,          UNSIGNED_FLAG, 0},                   /* ObBitType */
  {EMySQLFieldType::MYSQL_TYPE_STRING,       ENUM_FLAG, 0},                       /* ObEnumType */
  {EMySQLFieldType::MYSQL_TYPE_STRING,       SET_FLAG, 0},                        /* ObSetType */
  {EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,  0, 0},                               /* ObEnumInnerType */
  {EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,  0, 0},                               /* ObSetInnerType */
  {EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE,       BINARY_FLAG | TIMESTAMP_FLAG, 0}, /* ObTimestampTZType */
  {EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE, BINARY_FLAG | TIMESTAMP_FLAG, 0}, /* ObTimestampLTZType */
  {EMySQLFieldType::MYSQL_TYPE_OB_TIMESTAMP_NANO,                 BINARY_FLAG | TIMESTAMP_FLAG, 0},  /* ObTimestampNanoType */
  {EMySQLFieldType::MYSQL_TYPE_OB_RAW,                            BINARY_FLAG,                  0},  /* ObRawType */
  {EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_YM,                    BINARY_FLAG,                  0},  /* ObIntervalYMType */
  {EMySQLFieldType::MYSQL_TYPE_OB_INTERVAL_DS,                    BINARY_FLAG,                  0},  /* ObIntervalDSType */	
  {EMySQLFieldType::MYSQL_TYPE_OB_NUMBER_FLOAT,                   BINARY_FLAG,                  0},  /* ObNumberFloatType */
  {EMySQLFieldType::MYSQL_TYPE_OB_NVARCHAR2, 0, 0},                               /* ObNVarchar2Type */
  {EMySQLFieldType::MYSQL_TYPE_OB_NCHAR,     0, 0},                               /* ObNCharType */
  {EMySQLFieldType::MYSQL_TYPE_OB_UROWID,    0, 0},
  {EMySQLFieldType::MYSQL_TYPE_ORA_BLOB,       0, 0},                       /* ObLobType */
  {EMySQLFieldType::MYSQL_TYPE_JSON,       BLOB_FLAG | BINARY_FLAG, 0}, /* ObJsonType */
  {EMySQLFieldType::MYSQL_TYPE_GEOMETRY,   BLOB_FLAG | BINARY_FLAG, 0}, /* ObGeometryType */
  {EMySQLFieldType::MYSQL_TYPE_COMPLEX,   0, 0}, /* ObUserDefinedSQLType */
  {EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL, 0, 0},                           /* ObDecimalIntType */
  {EMySQLFieldType::MYSQL_TYPE_STRING,     0, 0},   /* ObCollectionSQLType, will cast to string */
  {EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,     0, 0},   /* reserved for ObMySQLDateType */
  {EMySQLFieldType::MYSQL_TYPE_NOT_DEFINED,     0, 0},   /* reserved for ObMySQLDateTimeType */
  {EMySQLFieldType::MYSQL_TYPE_BLOB,       BLOB_FLAG, 0},                         /* ObRoaringBitmapType */
  /* ObMaxType */
};

//called by handle COM_STMT_EXECUTE offset is 0
bool ObSMUtils::update_from_bitmap(ObObj &param, const char *bitmap, int64_t field_index)
{
  bool ret = false;
  if (update_from_bitmap(bitmap, field_index)) {
    param.set_null();
    ret = true;
  }
  return ret;
}

bool ObSMUtils::update_from_bitmap(const char *bitmap, int64_t field_index)
{
  bool ret = false;
  int byte_pos = static_cast<int>(field_index / 8);
  int bit_pos  = static_cast<int>(field_index % 8);
  if (NULL != bitmap) {
    char value = bitmap[byte_pos];
    if (value & (1 << bit_pos)) {
      ret = true;
    }
  }
  return ret;
}

int ObSMUtils::cell_str(
    char *buf, const int64_t len,
    const ObObj &obj,
    MYSQL_PROTOCOL_TYPE type, int64_t &pos,
    int64_t cell_idx, char *bitmap,
    const ObDataTypeCastParams &dtc_params,
    const ObField *field,
    ObSchemaGetterGuard *schema_guard,
    uint64_t tenant_id)
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
    zerofill = field->flags_ & ZEROFILL_FLAG;
    zflength = field->length_;
    OB_LOG(DEBUG, "get field accuracy_", K(field->accuracy_), K(zerofill), K(obj));
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
        ret = ObMySQLUtil::datetime_cell_str(buf, len, obj.get_datetime(), type, pos, (obj.is_timestamp() ? dtc_params.tz_info_ : NULL), scale);
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
        ret = ObMySQLUtil::otimestamp_cell_str(buf, len, obj.get_otimestamp_value(), type, pos, dtc_params, scale, obj.get_type());
        break;
      case ObRawTC:
      case ObTextTC: // TODO@hanhui texttc share the stringtc temporarily
      case ObStringTC:
      // lob locator也会按varchar方式进行encode, 客户端往server端传输数据时,
      // 也是将lob locator按varchar传输, 先编码LobLocator length, 然后再编码整个lob Locator
      case ObLobTC:
      case ObRoaringBitmapTC: {
        ret = ObMySQLUtil::varchar_cell_str(buf, len, obj.get_string(), is_oracle_raw, pos);
        break;
      }
      case ObJsonTC:{
        ret = ObMySQLUtil::json_cell_str(MTL_ID(), buf, len, obj.get_string(), pos);
        break;
      }
      case ObGeometryTC: {
        if (lib::is_oracle_mode() && type == MYSQL_PROTOCOL_TYPE::TEXT) {
#ifdef OB_BUILD_ORACLE_PL
          common::ObArenaAllocator allocator;
          ObStringBuffer geo_str(&allocator);
          if (OB_FAIL(pl::ObSdoGeometry::wkb_to_sdo_geometry_text(obj.get_string(), geo_str))) {
            OB_LOG(WARN, "wkb to sdo geometry text failed", K(ret));
          } else {
            ret = ObMySQLUtil::varchar_cell_str(buf, len, geo_str.string(), is_oracle_raw, pos);
          }
#else
          ret = OB_NOT_SUPPORTED;
#endif
        } else {
          ret = ObMySQLUtil::geometry_cell_str(buf, len, obj.get_string(), pos);
        }
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
      case ObExtendTC: {
        common::ObArenaAllocator allocator;
        const pl::ObUserDefinedType *user_type = NULL;
        const ObUDTTypeInfo *udt_info = NULL;
        ObObj shadow_obj = obj;
        char *src = reinterpret_cast<char*>(&shadow_obj);
        uint64_t database_id = OB_INVALID_ID;
        if (OB_ISNULL(field) || OB_ISNULL(schema_guard)) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "complex type need field and schema guard not null", K(ret));
        } else if (BINARY == type && field->type_.get_type() != ObExtendType &&
                   !(field->type_.is_geometry() && lib::is_oracle_mode()) && // oracle gis will cast to extend in ps mode
                   !field->type_.is_user_defined_sql_type() &&
                   !field->type_.is_collection_sql_type()) { // sql udt will cast to extend in ps mode
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "field type is not ObExtended", K(ret));
        } else if (field->type_.is_user_defined_sql_type() || field->type_.is_collection_sql_type()
                   || (field->type_.get_type() == ObExtendType && field->accuracy_.get_accuracy() == T_OBJ_SDO_GEOMETRY)) {
          const uint64_t udt_id = field->accuracy_.get_accuracy();
          const uint64_t tenant_id = pl::get_tenant_id_by_object_id(udt_id);
          if (OB_FAIL(schema_guard->get_udt_info(tenant_id, udt_id, udt_info))) {
            OB_LOG(WARN, "failed to get sys udt info", K(ret), K(field));
          } else if (OB_ISNULL(udt_info)) {
            ret = OB_ERR_UNEXPECTED;
            OB_LOG(WARN, "udt info is null", K(ret));
          } else if (OB_FAIL(udt_info->transform_to_pl_type(allocator, user_type))) {
            OB_LOG(WARN, "faild to transform to pl type", K(ret));
          } else if (OB_ISNULL(user_type)) {
            ret = OB_ERR_UNEXPECTED;
            OB_LOG(WARN, "user type is null", K(ret));
          } else if (OB_FAIL(user_type->serialize(*schema_guard, dtc_params.tz_info_, type, src, buf, len, pos))) {
            OB_LOG(WARN, "failed to serialize", K(ret));
          }
        } else if (field->type_owner_.empty() || field->type_name_.empty()) {
          if (0 == field->type_name_.case_compare("SYS_REFCURSOR")) {
            ObPLCursorInfo *cursor = reinterpret_cast<ObPLCursorInfo*>(obj.get_int());
            if (OB_ISNULL(cursor)) {
              // cursor may null, we cell null to client.
              ret = ObMySQLUtil::null_cell_str(buf, len, type, pos, cell_idx, bitmap);
            } else if (OB_FAIL(ObMySQLUtil::int_cell_str(buf,
                                                         len,
                                                         cursor->get_id(),
                                                         ObInt32Type,
                                                         false,
                                                         type,
                                                         pos,
                                                         zerofill,
                                                         zflength))) {
              OB_LOG(WARN,
                     "int cell str filled",
                     K(cursor->get_id()),
                     K(ObString(len, buf)),
                     K(ret));
            } else { /*do nothing*/ }
#ifdef OB_BUILD_ORACLE_PL
          } else if (obj.is_pl_extend()) {
            if (TEXT == type && (PL_VARRAY_TYPE == obj.get_meta().get_extend_type()
                        || PL_NESTED_TABLE_TYPE == obj.get_meta().get_extend_type()
                        || PL_ASSOCIATIVE_ARRAY_TYPE == obj.get_meta().get_extend_type()
                        || PL_RECORD_TYPE == obj.get_meta().get_extend_type())) {
              if (OB_FAIL(extend_cell_str(buf, len, src, type, pos,
                                 dtc_params, field, schema_guard, tenant_id))) {
                OB_LOG(WARN, "extend type cell string fail.", K(ret));
              }
            } else if (BINARY == type && PL_NESTED_TABLE_TYPE == obj.get_meta().get_extend_type()) {
              // anonymous collection
              ObPLCollection *coll = reinterpret_cast<ObPLCollection *>(obj.get_ext());
              ObNestedTableType *nested_type = NULL;
              ObPLDataType element_type;
              if (OB_ISNULL(nested_type =
                reinterpret_cast<ObNestedTableType*>(allocator.alloc(sizeof(ObNestedTableType))))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                OB_LOG(WARN, "failed to alloc memory for ObNestedTableType", K(ret));
              } else if (OB_ISNULL(coll)) {
                ret = OB_ERR_UNEXPECTED;
                OB_LOG(WARN, "coll is null", K(ret));
              } else if (FALSE_IT(new (nested_type) ObNestedTableType())) {
              } else if (FALSE_IT(element_type.reset())) {
              } else if (FALSE_IT(element_type.set_data_type(coll->get_element_type()))) {
              } else if (FALSE_IT(nested_type->set_element_type(element_type))) {
              } else if (OB_FAIL(nested_type->serialize(
                                  *schema_guard, dtc_params.tz_info_, type, src, buf, len, pos))) {
                OB_LOG(WARN, "failed to serialize anonymous collection", K(ret));
              } else {
                OB_LOG(DEBUG, "success to serialize anonymous collection", K(ret));
              }
            } else {
              ret = OB_NOT_SUPPORTED;
              OB_LOG(WARN, "this extend type is not support", KPC(field), K(type), K(obj.get_meta().get_extend_type()), K(ret));
            }
#endif
          } else {
            ret = OB_ERR_UNEXPECTED;
            OB_LOG(WARN, "type owner or type name is empty", KPC(field), K(ret));
          }
        } else {
          if (OB_FAIL(schema_guard->get_database_id(tenant_id, field->type_owner_, database_id))) {
            OB_LOG(WARN, "failed to get database id", K(ret));
          } else if (OB_FAIL(schema_guard->get_udt_info(tenant_id, database_id, OB_INVALID_ID,
                                                        field->type_name_, udt_info))) {
            OB_LOG(WARN, "failed to get udt info", K(ret), K(field));
          }
          if (OB_ISNULL(udt_info)) {
            // overwrite ret
            // try system udt
            if (0 == field->type_owner_.case_compare(OB_SYS_DATABASE_NAME)
                || 0 == field->type_owner_.case_compare("SYS")) {
              if (OB_FAIL(schema_guard->get_udt_info(
                  OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID,
                  OB_INVALID_ID, field->type_name_, udt_info))) {
                OB_LOG(WARN, "failed to get sys udt info", K(ret), K(field));
              }
            }
            if (OB_SUCC(ret) && OB_ISNULL(udt_info)) {
              ret = OB_ERR_UNEXPECTED;
              OB_LOG(WARN, "udt info is null", K(ret));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(udt_info->transform_to_pl_type(allocator, user_type))) {
            OB_LOG(WARN, "faild to transform to pl type", K(ret));
          } else if (OB_ISNULL(user_type)) {
            ret = OB_ERR_UNEXPECTED;
            OB_LOG(WARN, "user type is null", K(ret));
          } else if (OB_FAIL(user_type->serialize(*schema_guard, dtc_params.tz_info_, type, src, buf, len, pos))) {
            OB_LOG(WARN, "failed to serialize", K(ret));
          }
        }
        break;
      }
      case ObIntervalTC: {
        ret = obj.is_interval_ym() ?
              ObMySQLUtil::interval_ym_cell_str(buf, len, obj.get_interval_ym(), type, pos, scale) :
              ObMySQLUtil::interval_ds_cell_str(buf, len, obj.get_interval_ds(), type, pos, scale);
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
      case ObUserDefinedSQLTC:
      case ObCollectionSQLTC: {
        if (obj.get_udt_subschema_id() == 0) { // xml
          ret = ObMySQLUtil::sql_utd_cell_str(MTL_ID(), buf, len, obj.get_string(), pos);
        } else if (type == MYSQL_PROTOCOL_TYPE::TEXT) { // common sql udt text protocal
          ret = ObMySQLUtil::varchar_cell_str(buf, len, obj.get_string(), is_oracle_raw, pos);
        } else {
          // ToDo: sql udt binary protocal (result should be the same as extend type)
          ret = OB_NOT_IMPLEMENT;
          OB_LOG(WARN, "UDTSQLType binary protocal not implemented", K(ret));
        }
        break;
      }
      case ObDecimalIntTC: {
        ret = ObMySQLUtil::decimalint_cell_str(buf, len, obj.get_decimal_int(), obj.get_int_bytes(),
                                               obj.get_scale(), pos, zerofill, zflength);
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

int get_map(ObObjType ob_type, const ObMySQLTypeMap *&map)
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

int ObSMUtils::get_type_length(ObObjType ob_type, int64_t &length)
{
  const ObMySQLTypeMap *map = NULL;
  int ret = OB_SUCCESS;

  if ((ret = get_map(ob_type, map)) == OB_SUCCESS) {
    length = map->length;
  }
  return ret;
}

int ObSMUtils::get_mysql_type(ObObjType ob_type, EMySQLFieldType &mysql_type,
                              uint16_t &flags, ObScale &num_decimals)
{
  const ObMySQLTypeMap *map = NULL;
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
      case EMySQLFieldType::MYSQL_TYPE_JSON: // mysql json and long text decimals are 0, we do not need it?
      case EMySQLFieldType::MYSQL_TYPE_GEOMETRY:
      case EMySQLFieldType::MYSQL_TYPE_ORA_XML:
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
        num_decimals = static_cast<ObScale>(lib::is_oracle_mode()
        ? ORACLE_NOT_FIXED_DEC
        : 0);
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
        num_decimals = static_cast<ObScale>((num_decimals == -1)
            ? (lib::is_oracle_mode() ? ORACLE_NOT_FIXED_DEC : NOT_FIXED_DEC)
            : num_decimals);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        _OB_LOG(WARN, "unexpected mysql_type=%d", mysql_type);
        break;
    } // end switch
  }
  return ret;
}

int ObSMUtils::get_ob_type(ObObjType &ob_type, EMySQLFieldType mysql_type, const bool is_unsigned)
{
  int ret = OB_SUCCESS;
  switch (mysql_type) {
    case EMySQLFieldType::MYSQL_TYPE_NULL:
      ob_type = ObNullType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_TINY:
      ob_type = is_unsigned ? ObUTinyIntType : ObTinyIntType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_SHORT:
      ob_type = is_unsigned ? ObUSmallIntType : ObSmallIntType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_LONG:
      ob_type = is_unsigned ? ObUInt32Type : ObInt32Type;
      break;
    case EMySQLFieldType::MYSQL_TYPE_LONGLONG:
      ob_type = is_unsigned ? ObUInt64Type : ObIntType;
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
    case EMySQLFieldType::MYSQL_TYPE_JSON:
      ob_type = ObJsonType;
      break;
    case EMySQLFieldType::MYSQL_TYPE_GEOMETRY:
      ob_type = ObGeometryType;
      break;
    default:
      _OB_LOG(WARN, "unsupport MySQL type %d", mysql_type);
      ret = OB_OBJ_TYPE_ERROR;
  }
  return ret;
}

int ObSMUtils::extend_cell_str(char *buf, const int64_t len,
                               char *src,
                               MYSQL_PROTOCOL_TYPE type, int64_t &pos,
                               const ObDataTypeCastParams &dtc_params,
                               const ObField *field,
                               ObSchemaGetterGuard *schema_guard,
                               uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator;
  const pl::ObUserDefinedType *user_type = NULL;
  const ObUDTTypeInfo *udt_info = NULL;
  const int64_t type_id = field->accuracy_.get_accuracy();
  if (OB_FAIL(schema_guard->get_udt_info(tenant_id, type_id, udt_info))) {
    OB_LOG(WARN, "get user type fail.", K(type_id), K(ret));
  } else if (NULL == udt_info) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "faild to get udt info.", K(ret));
  } else if (OB_FAIL(udt_info->transform_to_pl_type(allocator, user_type))) {
    OB_LOG(WARN, "faild to transform to pl type", K(ret));
  } else if (NULL == user_type) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "faild to get user type.", K(ret));
  } else if (len - pos < user_type->get_name().length() + 12) {
    // 12 is the length of string length and '(' and ')'
    // string length is reserve a const value of 10
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "size over flow.", K(ret), K(len), K(user_type->get_name()));
  } else {
    ObArenaAllocator alloc;
    char* tmp_buf = static_cast<char*>(alloc.alloc(len - pos - 12));
    int64_t tmp_pos = 0;
    MEMCPY(tmp_buf + tmp_pos, user_type->get_name().ptr(), user_type->get_name().length());
    tmp_pos += user_type->get_name().length();
    MEMCPY(tmp_buf + tmp_pos, "(", 1);
    tmp_pos += 1;
    const_cast<pl::ObUserDefinedType *>(user_type)->set_charset(static_cast<ObCollationType>(field->charsetnr_));
    if (OB_FAIL(user_type->serialize(*schema_guard, dtc_params.tz_info_, type, src, tmp_buf, len, tmp_pos))) {
      OB_LOG(WARN, "failed to serialize", K(ret));
    } else if (len - pos > tmp_pos + 1) {
      MEMCPY(tmp_buf + tmp_pos, ")", 1);
      tmp_pos += 1;
    } else {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "tmp_buf length is not enough.", K(ret), K(pos), K(len), K(tmp_pos));
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ObMySQLUtil::store_length(buf, len, tmp_pos, pos))) {
      OB_LOG(WARN, "store length fail.", K(ret), K(pos), K(len), K(tmp_pos));
    } else {
      MEMCPY(buf + pos, tmp_buf, tmp_pos);
      pos += tmp_pos;
    }
    const_cast<pl::ObUserDefinedType *>(user_type)->reset_charset();
  }
  return ret;
}
