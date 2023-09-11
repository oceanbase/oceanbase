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

#ifndef OCEANBASE_COMMON_OB_OBJECT_H_
#define OCEANBASE_COMMON_OB_OBJECT_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "common/ob_action_flag.h"
#include "common/object/ob_obj_type.h"
#include "common/ob_accuracy.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/number/ob_number_v2.h"
#include "lib/charset/ob_charset.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/hash_func/ob_hash_func.h"
#include "lib/charset/ob_dtoa.h"
#include "lib/rowid/ob_urowid.h"

namespace oceanbase
{
namespace tests
{
namespace common
{
class ObjTest;
class ObSqlString;
}
}
namespace sql
{
class ObExecContext;
}
namespace common
{

struct ObCompareCtx;
enum ObCmpNullPos
{
  NULL_LAST = 1,
  NULL_FIRST,
  MAX_NULL_POS
};

inline ObCmpNullPos default_null_pos()
{
  return lib::is_oracle_mode() ? NULL_LAST : NULL_FIRST;
}

struct ObEnumSetInnerValue
{
  OB_UNIS_VERSION_V(1);
public:
  ObEnumSetInnerValue() : numberic_value_(0), string_value_() {}
  ObEnumSetInnerValue(uint64_t numberic_value, common::ObString &string_value)
      : numberic_value_(numberic_value), string_value_(string_value) {}
  virtual ~ObEnumSetInnerValue() {}

  TO_STRING_KV(K_(numberic_value), K_(string_value));
  uint64_t numberic_value_;
  common::ObString string_value_;
};

struct ObLobScale
{
  static const uint8_t LOB_SCALE_MASK = 0xF;
  enum StorageType
  {
    STORE_IN_ROW = 0,
    STORE_OUT_ROW,
    STORE_HAS_LOB_HEADER,
  };
  union
  {
    int8_t scale_;
    struct
    {
      uint8_t reserve_: 4;
      uint8_t type_: 4;
    };
  };
  ObLobScale() : scale_(0) {}
  ObLobScale(const ObScale scale) : scale_(static_cast<const int8_t>(scale)) { reserve_ = 0; }
  OB_INLINE void reset() { scale_ = -1; }
  OB_INLINE bool is_valid() { return STORE_IN_ROW == type_ || STORE_OUT_ROW == type_ || STORE_HAS_LOB_HEADER == type_; }
  OB_INLINE void set_in_row() { reserve_ = 0; type_ = STORE_IN_ROW; }
  OB_INLINE void set_out_row() { reserve_ = 0; type_ = STORE_OUT_ROW; }
  OB_INLINE void set_has_lob_header() { reserve_ = 0; type_ = STORE_HAS_LOB_HEADER; }
  OB_INLINE bool is_in_row() const { return type_ == STORE_IN_ROW; }
  OB_INLINE bool is_out_row() const { return type_ == STORE_OUT_ROW; }
  OB_INLINE bool has_lob_header() const { return type_ == STORE_HAS_LOB_HEADER; }
  OB_INLINE ObScale get_scale() const { return static_cast<ObScale>(scale_); }
  TO_STRING_KV(K_(scale));
};

class ObObjMeta
{
public:
  ObObjMeta()
      :type_(ObNullType),
       cs_level_(CS_LEVEL_INVALID),
       cs_type_(CS_TYPE_INVALID),
       scale_(-1)
  {}

  OB_INLINE bool operator ==(const ObObjMeta &other) const
  {
    return (type_ == other.type_ && cs_level_ == other.cs_level_&& cs_type_ == other.cs_type_);
  }
  OB_INLINE bool operator !=(const ObObjMeta &other) const { return !this->operator ==(other); }
  // this method is inefficient, you'd better use set_tinyint() etc. instead
  OB_INLINE void set_type(const ObObjType &type)
  {
    type_ = static_cast<uint8_t>(type);
    if (ObNullType == type_) {
      set_collation_level(CS_LEVEL_IGNORABLE);
      set_collation_type(CS_TYPE_BINARY);
    } else if (ObUnknownType == type_
               || ObExtendType == type_) {
      set_collation_level(CS_LEVEL_INVALID);
      set_collation_type(CS_TYPE_INVALID);
    } else if (ObHexStringType == type_) {
      set_collation_type(CS_TYPE_BINARY);
    } else if (ObJsonType == type_) {
      set_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else if (!ob_is_string_type(static_cast<ObObjType>(type_))
               && !ob_is_lob_locator(static_cast<ObObjType>(type_))
               && !ob_is_raw(static_cast<ObObjType>(type_))
               && !ob_is_enum_or_set_type(static_cast<ObObjType>(type_))
               && !ob_is_geometry(static_cast<ObObjType>(type_))) {
      set_collation_level(CS_LEVEL_NUMERIC);
      set_collation_type(CS_TYPE_BINARY);
    } else if (ObUserDefinedSQLType == type_) {
      set_subschema_id(UINT_MAX16);
    }
  }
  OB_INLINE void set_type_simple(const ObObjType &type)
  {
    type_ = static_cast<uint8_t>(type);
  }
  // in greatest case need manually set numeric collation,
  // e.g greatest(2, 'x') => type=varchar,collation=binary,cmp_collation=utf8
  OB_INLINE void set_numeric_collation() {
    set_collation_level(CS_LEVEL_NUMERIC);
    set_collation_type(CS_TYPE_BINARY);
  }
  OB_INLINE void set_meta(const ObObjMeta &meta)
  {
    type_ = meta.type_;
    cs_level_ = meta.cs_level_;
    cs_type_ = meta.cs_type_;
    scale_ = meta.scale_;
  }
  OB_INLINE void reset()
  {
    type_ = ObNullType;
    cs_level_ = CS_LEVEL_INVALID;
    cs_type_ = CS_TYPE_INVALID;
    scale_ = -1;
  }

  OB_INLINE ObObjType get_type() const { return static_cast<ObObjType>(type_); }
  OB_INLINE ObObjOType get_oracle_type() const { return ob_obj_type_to_oracle_type(static_cast<ObObjType>(type_)); }
  OB_INLINE const ObObjMeta &get_obj_meta() const { return *this; }
  OB_INLINE ObObjTypeClass get_type_class() const { return ob_obj_type_class(get_type()); }
  OB_INLINE ObObjTypeClass get_type_class_for_oracle() const { return ob_oracle_type_class(get_type()); }

  OB_INLINE void set_null() { type_ = static_cast<uint8_t>(ObNullType); set_collation_level(CS_LEVEL_IGNORABLE); set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_tinyint() { type_ = static_cast<uint8_t>(ObTinyIntType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_smallint() { type_ = static_cast<uint8_t>(ObSmallIntType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_mediumint() { type_ = static_cast<uint8_t>(ObMediumIntType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_int32() { type_ = static_cast<uint8_t>(ObInt32Type); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_int() { type_ = static_cast<uint8_t>(ObIntType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_utinyint() { type_ = static_cast<uint8_t>(ObUTinyIntType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_usmallint() { type_ = static_cast<uint8_t>(ObUSmallIntType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_umediumint() { type_ = static_cast<uint8_t>(ObUMediumIntType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_uint32() { type_ = static_cast<uint8_t>(ObUInt32Type); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_uint64() { type_ = static_cast<uint8_t>(ObUInt64Type); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_float() { type_ = static_cast<uint8_t>(ObFloatType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_double() { type_ = static_cast<uint8_t>(ObDoubleType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_ufloat() { type_ = static_cast<uint8_t>(ObUFloatType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_udouble() { type_ = static_cast<uint8_t>(ObUDoubleType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_number() { type_ = static_cast<uint8_t>(ObNumberType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_unumber() { type_ = static_cast<uint8_t>(ObUNumberType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_number_float() { type_ = static_cast<uint8_t>(ObNumberFloatType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_datetime() { type_ = static_cast<uint8_t>(ObDateTimeType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_timestamp() { type_ = static_cast<uint8_t>(ObTimestampType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_year() { type_ = static_cast<uint8_t>(ObYearType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_date() { type_ = static_cast<uint8_t>(ObDateType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_time() { type_ = static_cast<uint8_t>(ObTimeType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_varchar() { type_ = static_cast<uint8_t>(ObVarcharType); }
  OB_INLINE void set_char() { type_ = static_cast<uint8_t>(ObCharType); }
  OB_INLINE void set_varbinary() { type_ = static_cast<uint8_t>(ObVarcharType); set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_binary() { type_ = static_cast<uint8_t>(ObCharType); set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_hex_string() { type_ = static_cast<uint8_t>(ObHexStringType); set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_raw() { type_ = static_cast<uint8_t>(ObRawType); set_collation_level(common::CS_LEVEL_IMPLICIT); set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_ext() { type_ = static_cast<uint8_t>(ObExtendType); set_collation_level(CS_LEVEL_INVALID); set_collation_type(CS_TYPE_INVALID); set_extend_type(0); }
  OB_INLINE void set_unknown() { type_ = static_cast<uint8_t>(ObUnknownType); set_collation_level(CS_LEVEL_INVALID); set_collation_type(CS_TYPE_INVALID); }
  OB_INLINE void set_bit() { type_ = static_cast<uint8_t>(ObBitType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  //TODO():collation_level, collation_type
  OB_INLINE void set_enum() { type_ = static_cast<uint8_t>(ObEnumType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_set() { type_ = static_cast<uint8_t>(ObSetType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_enum_inner() { type_ = static_cast<uint8_t>(ObEnumInnerType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_set_inner() { type_ = static_cast<uint8_t>(ObSetInnerType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }

  OB_INLINE void set_clob() { type_ = static_cast<uint8_t>(ObLongTextType); lob_scale_.set_in_row(); set_collation_level(CS_LEVEL_IMPLICIT); set_default_collation_type(); }
  OB_INLINE void set_blob() { type_ = static_cast<uint8_t>(ObLongTextType); lob_scale_.set_in_row(); set_collation_level(CS_LEVEL_IMPLICIT); set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_inrow() { lob_scale_.set_in_row(); }
  OB_INLINE void set_outrow() { lob_scale_.set_out_row(); }
  OB_INLINE void set_has_lob_header() { lob_scale_.set_has_lob_header(); }
  OB_INLINE void set_json()
  {
    type_ = static_cast<uint8_t>(ObJsonType);
    lob_scale_.set_in_row();
    set_collation_level(CS_LEVEL_IMPLICIT);
    set_collation_type(CS_TYPE_UTF8MB4_BIN);
  }
  OB_INLINE void set_geometry()
  {
    type_ = static_cast<uint8_t>(ObGeometryType);
    lob_scale_.set_in_row();
    set_collation_level(CS_LEVEL_IMPLICIT);
    set_collation_type(CS_TYPE_BINARY);
  }
  OB_INLINE void set_otimestamp_type(const ObObjType type) { type_ = static_cast<uint8_t>(type); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_timestamp_tz() { set_otimestamp_type(ObTimestampTZType); }
  OB_INLINE void set_timestamp_ltz() { set_otimestamp_type(ObTimestampLTZType); }
  OB_INLINE void set_timestamp_nano() { set_otimestamp_type(ObTimestampNanoType); }
  OB_INLINE void set_interval_ym() { type_ = static_cast<uint8_t>(ObIntervalYMType); set_collation_level(CS_LEVEL_NUMERIC); set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_interval_ds() { type_ = static_cast<uint8_t>(ObIntervalDSType); set_collation_level(CS_LEVEL_NUMERIC); set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_nvarchar2() { type_ = static_cast<uint8_t>(ObNVarchar2Type); }
  OB_INLINE void set_nchar() { type_ = static_cast<uint8_t>(ObNCharType); }
  OB_INLINE void set_urowid()
  {
    type_ = static_cast<uint8_t>(ObURowIDType);
    set_collation_level(CS_LEVEL_NUMERIC);
    set_collation_type(CS_TYPE_BINARY);
  }

  OB_INLINE void set_clob_locator() { type_ = static_cast<uint8_t>(ObLobType); set_default_collation_type(); }
  OB_INLINE void set_blob_locator() { type_ = static_cast<uint8_t>(ObLobType); set_collation_type(CS_TYPE_BINARY); }

  OB_INLINE bool is_valid() const { return ob_is_valid_obj_type(static_cast<ObObjType>(type_)); }
  OB_INLINE bool is_invalid() const { return !ob_is_valid_obj_type(static_cast<ObObjType>(type_)); }

  OB_INLINE bool is_null() const { return type_ == static_cast<uint8_t>(ObNullType); }
  OB_INLINE bool is_tinyint() const { return type_ == static_cast<uint8_t>(ObTinyIntType); }
  OB_INLINE bool is_smallint() const { return type_ == static_cast<uint8_t>(ObSmallIntType); }
  OB_INLINE bool is_mediumint() const { return type_ == static_cast<uint8_t>(ObMediumIntType); }
  OB_INLINE bool is_int32() const { return type_ == static_cast<uint8_t>(ObInt32Type); }
  OB_INLINE bool is_int() const { return type_ == static_cast<uint8_t>(ObIntType); }
  OB_INLINE bool is_utinyint() const { return type_ == static_cast<uint8_t>(ObUTinyIntType); }
  OB_INLINE bool is_usmallint() const { return type_ == static_cast<uint8_t>(ObUSmallIntType); }
  OB_INLINE bool is_umediumint() const { return type_ == static_cast<uint8_t>(ObUMediumIntType); }
  OB_INLINE bool is_uint32() const { return type_ == static_cast<uint8_t>(ObUInt32Type); }
  OB_INLINE bool is_uint64() const { return type_ == static_cast<uint8_t>(ObUInt64Type); }
  OB_INLINE bool is_float() const { return type_ == static_cast<uint8_t>(ObFloatType); }
  OB_INLINE bool is_double() const { return type_ == static_cast<uint8_t>(ObDoubleType); }
  OB_INLINE bool is_ufloat() const { return type_ == static_cast<uint8_t>(ObUFloatType); }
  OB_INLINE bool is_udouble() const { return type_ == static_cast<uint8_t>(ObUDoubleType); }
  OB_INLINE bool is_number() const { return type_ == static_cast<uint8_t>(ObNumberType); }
  OB_INLINE bool is_unumber() const { return type_ == static_cast<uint8_t>(ObUNumberType); }
  OB_INLINE bool is_number_float() const { return type_ == static_cast<uint8_t>(ObNumberFloatType); }
  OB_INLINE bool is_datetime() const { return type_ == static_cast<uint8_t>(ObDateTimeType); }
  OB_INLINE bool is_timestamp() const { return type_ == static_cast<uint8_t>(ObTimestampType); }
  OB_INLINE bool is_year() const { return type_ == static_cast<uint8_t>(ObYearType); }
  OB_INLINE bool is_date() const { return type_ == static_cast<uint8_t>(ObDateType); }
  OB_INLINE bool is_time() const { return type_ == static_cast<uint8_t>(ObTimeType); }
  OB_INLINE bool is_timestamp_tz() const { return type_ == static_cast<uint8_t>(ObTimestampTZType); }
  OB_INLINE bool is_timestamp_ltz() const { return type_ == static_cast<uint8_t>(ObTimestampLTZType); }
  OB_INLINE bool is_timestamp_nano() const { return type_ == static_cast<uint8_t>(ObTimestampNanoType); }
  OB_INLINE bool is_interval_ym() const {  return type_ == static_cast<uint8_t>(ObIntervalYMType); }
  OB_INLINE bool is_interval_ds() const {  return type_ == static_cast<uint8_t>(ObIntervalDSType); }
  OB_INLINE bool is_nvarchar2() const { return type_ == static_cast<uint8_t>(ObNVarchar2Type); }
  OB_INLINE bool is_nchar() const { return type_ == static_cast<uint8_t>(ObNCharType); }
  OB_INLINE bool is_varchar() const
  {
    return ((type_ == static_cast<uint8_t>(ObVarcharType)) && (CS_TYPE_BINARY != cs_type_));
  }
  OB_INLINE bool is_char() const
  {
    return ((type_ == static_cast<uint8_t>(ObCharType)) && (CS_TYPE_BINARY != cs_type_));
  }
  OB_INLINE bool is_varbinary() const
  {
    return (type_ == static_cast<uint8_t>(ObVarcharType) && CS_TYPE_BINARY == cs_type_);
  }
  static bool is_binary(const ObObjType type, const ObCollationType cs_type)
  {
    return (ObCharType == type && CS_TYPE_BINARY == cs_type);
  }
  OB_INLINE bool is_binary() const
  {
    return is_binary(static_cast<ObObjType>(type_), static_cast<ObCollationType>(cs_type_));
  }
  OB_INLINE bool is_cs_collation_free() const
  {
    return cs_type_ == CS_TYPE_UTF8MB4_GENERAL_CI || cs_type_ == CS_TYPE_UTF8MB4_BIN;
  }
  OB_INLINE bool is_hex_string() const { return type_ == static_cast<uint8_t>(ObHexStringType); }
  OB_INLINE bool is_raw() const { return type_ == static_cast<uint8_t>(ObRawType); }
  OB_INLINE bool is_ext() const { return type_ == static_cast<uint8_t>(ObExtendType); }
  OB_INLINE bool is_ext_sql_array() const { return is_ext() && get_extend_type() == T_EXT_SQL_ARRAY; }
  OB_INLINE bool is_unknown() const { return type_ == static_cast<uint8_t>(ObUnknownType); }
  OB_INLINE bool is_bit() const { return type_ == static_cast<uint8_t>(ObBitType); }
  OB_INLINE bool is_enum() const { return type_ == static_cast<uint8_t>(ObEnumType); }
  OB_INLINE bool is_set() const { return type_ == static_cast<uint8_t>(ObSetType); }
  OB_INLINE bool is_enum_or_set() const { return type_ == static_cast<uint8_t>(ObEnumType)
    || type_ == static_cast<uint8_t>(ObSetType); }
  OB_INLINE bool is_text() const
  {
    return (ob_is_text_tc(get_type()) && CS_TYPE_BINARY != cs_type_);
  }
  /*OB_INLINE bool is_oracle_clob() const
  {
    return (lib::is_oracle_mode() && ObLongTextType == get_type() && CS_TYPE_BINARY != cs_type_);
  }*/
  OB_INLINE bool is_clob() const
  {
    return (lib::is_oracle_mode() && ObLongTextType == get_type() && CS_TYPE_BINARY != cs_type_);
  }
  /*OB_INLINE bool is_oracle_blob() const
  {
    return (lib::is_oracle_mode() && ObLongTextType == get_type() && CS_TYPE_BINARY == cs_type_);
  }*/
  OB_INLINE bool is_blob() const
  {
    return (ob_is_text_tc(get_type()) && CS_TYPE_BINARY == cs_type_);
  }
  OB_INLINE bool is_lob_storage() const { return ob_is_large_text(get_type()) || ob_is_json_tc(get_type()) || ob_is_geometry_tc(get_type()); }
  OB_INLINE bool is_lob() const { return ob_is_text_tc(get_type()); }
  OB_INLINE bool is_inrow() const { return is_lob() && lob_scale_.is_in_row(); }
  OB_INLINE bool is_outrow() const { return is_lob() && lob_scale_.is_out_row(); }
  OB_INLINE bool has_lob_header() const { return is_lob_storage() && lob_scale_.has_lob_header(); }

  OB_INLINE bool is_json() const { return type_ == static_cast<uint8_t>(ObJsonType); }
  OB_INLINE bool is_json_inrow() const { return is_json() && lob_scale_.is_in_row(); }
  OB_INLINE bool is_json_outrow() const { return is_json() && lob_scale_.is_out_row(); }

  OB_INLINE bool is_geometry() const { return type_ == static_cast<uint8_t>(ObGeometryType); }
  OB_INLINE bool is_geometry_inrow() const { return is_geometry() && lob_scale_.is_in_row(); }
  OB_INLINE bool is_geometry_outrow() const { return is_geometry() && lob_scale_.is_out_row(); }

  // combination of above functions.
  OB_INLINE bool is_varbinary_or_binary() const { return is_varbinary() || is_binary(); }
  OB_INLINE bool is_varchar_or_char() const { return is_varchar() || is_char(); }
  OB_INLINE bool is_nstring() const { return is_nvarchar2() || is_nchar(); }
  OB_INLINE bool is_fixed_len_char_type() const { return is_char() || is_nchar(); }
  OB_INLINE bool is_varying_len_char_type() const { return is_varchar() || is_nvarchar2(); }
  OB_INLINE bool is_character_type() const { return is_nstring() || is_varchar_or_char(); }
  OB_INLINE bool is_collation_free_compatible() const { return is_character_type() && is_cs_collation_free(); }
  OB_INLINE bool is_numeric_type() const { return ob_is_numeric_type(get_type()); }
  OB_INLINE bool is_integer_type() const { return ob_is_integer_type(get_type()); }
  OB_INLINE bool is_string_type() const { return ob_is_string_tc(get_type()) || ob_is_text_tc(get_type()); }
  OB_INLINE bool is_string_or_lob_locator_type() const {
    return ob_is_string_tc(get_type()) || ob_is_text_tc(get_type()) || is_lob_locator();
  }
  OB_INLINE bool is_temporal_type() const { return ob_is_temporal_type(get_type()); }
  OB_INLINE bool is_unsigned_integer() const
  {
    return (static_cast<uint8_t>(ObUTinyIntType) <= type_
         && static_cast<uint8_t>(ObUInt64Type)   >= type_);
  }
  OB_INLINE bool is_signed_integer() const
  {
    return (static_cast<uint8_t>(ObTinyIntType) <= type_
        && static_cast<uint8_t>(ObIntType)   >= type_);
  }
  OB_INLINE bool is_unsigned() const {
    return (static_cast<uint8_t>(ObUTinyIntType) <= type_
         && static_cast<uint8_t>(ObUInt64Type)   >= type_)
        || static_cast<uint8_t>(ObUNumberType) == type_
        || static_cast<uint8_t>(ObUDoubleType) == type_
        || static_cast<uint8_t>(ObUFloatType) == type_;
  }
  OB_INLINE bool is_enumset_inner_type() const { return static_cast<uint8_t>(ObEnumInnerType) == type_
    || static_cast<uint8_t>(ObSetInnerType) == type_;}
  OB_INLINE bool is_otimestamp_type() const { return ObTimestampTZType <= get_type() && get_type() <= ObTimestampNanoType; }
  OB_INLINE bool is_oracle_decimal() const { return ObNumberType == type_ || ObFloatType == type_ || ObDoubleType == type_; }
  OB_INLINE bool is_urowid() const { return ObURowIDType == type_; }
  OB_INLINE bool is_blob_locator() const { return (ObLobType == type_ && CS_TYPE_BINARY == cs_type_); }
  OB_INLINE bool is_clob_locator() const { return (ObLobType == type_ && CS_TYPE_BINARY != cs_type_); }
  OB_INLINE bool is_lob_locator() const { return ObLobType == type_; }

  OB_INLINE bool is_interval_type() const { return is_interval_ds() || is_interval_ym(); }
  OB_INLINE bool is_oracle_temporal_type() const { return is_datetime() || is_otimestamp_type() || is_interval_type(); }

  OB_INLINE void set_collation_level(ObCollationLevel cs_level) { cs_level_ = cs_level; }
  OB_INLINE void set_collation_type(ObCollationType cs_type) { cs_type_ = cs_type; }
  OB_INLINE void set_default_collation_type() { set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset())); }
  OB_INLINE ObCollationLevel get_collation_level() const { return static_cast<ObCollationLevel>(cs_level_); }
  OB_INLINE ObCollationType get_collation_type() const {
    // ObUserDefinedSQLType reused cs_type as part of sub schema id, therefore always return CS_TYPE_BINARY
    return is_user_defined_sql_type() ? CS_TYPE_BINARY : static_cast<ObCollationType>(cs_type_);
  }
  OB_INLINE ObCharsetType get_charset_type() const {
    return ObCharset::charset_type_by_coll(get_collation_type());
  }
  OB_INLINE bool is_collation_invalid() const
  {
    return CS_LEVEL_INVALID == cs_level_ && CS_TYPE_INVALID == cs_type_;
  }
  OB_INLINE void set_collation(const ObObjMeta &other)
  {
    cs_level_ = other.cs_level_;
    cs_type_ = other.cs_type_;
  }
  OB_INLINE void set_scale(const ObScale scale) { scale_ = static_cast<int8_t>(scale); }
  OB_INLINE ObScale get_scale() const { return static_cast<ObScale>(scale_); }
  OB_INLINE void set_extend_type(uint8_t type) { extend_type_ = type; }
  OB_INLINE uint8_t get_extend_type() const { return is_ext() ? extend_type_ : -1; }

  TO_STRING_KV(N_TYPE, ob_obj_type_str(static_cast<ObObjType>(type_)),
               N_COLLATION, ObCharset::collation_name(get_collation_type()),
               N_COERCIBILITY, ObCharset::collation_level(get_collation_level()));
  NEED_SERIALIZE_AND_DESERIALIZE;

  static uint32_t type_offset_bits() { return offsetof(ObObjMeta, type_) * 8; }
  static uint32_t cs_level_offset_bits() { return offsetof(ObObjMeta, cs_level_) * 8; }
  static uint32_t cs_type_offset_bits() { return offsetof(ObObjMeta, cs_type_) * 8; }
  static uint32_t scale_offset_bits() { return offsetof(ObObjMeta, scale_) * 8; }

  OB_INLINE uint16_t get_subschema_id() const { return ((static_cast<uint16_t>(cs_type_)) << 8) | cs_level_; }
  OB_INLINE void set_subschema_id(const uint16_t subschema_id) {
    cs_type_ = (subschema_id >> 8) & UINT_MAX8;
    cs_level_ = subschema_id & UINT_MAX8;
  }
  OB_INLINE uint8_t get_udt_flags() const { return scale_; }
  OB_INLINE void set_udt_flags(uint8_t udt_flag) { scale_  = udt_flag; }
  // UDTs are not lobs, but they may contain lob types
  OB_INLINE void set_sql_udt(const uint16_t subschema_id) {
    type_ = static_cast<uint8_t>(ObUserDefinedSQLType);
    set_subschema_id(subschema_id);
  }
  OB_INLINE bool is_user_defined_sql_type() const { return ObUserDefinedSQLType == type_; }
  OB_INLINE bool is_xml_sql_type() const { return (ObUserDefinedSQLType == type_ && get_subschema_id() == ObXMLSqlType); }

protected:
  uint8_t type_;
  uint8_t cs_level_;    // collation level, low bits of sub schema id if type_ is UDT
  uint8_t cs_type_;     // collation type, high bits of sub schema id if type_ is UDT
  union {
    int8_t scale_;  // scale, length of bit if type_ is ObBitType; reserved flags if type_ is UDT
    ObLobScale lob_scale_;
    uint8_t extend_type_; //detailed extend value(enum ObPLType), if type_ is ObExtendType
  };
};

struct ObLobId
{
  ObLobId()
    : tablet_id_(0 /* ObTabletID::INVALID_TABLET_ID */), lob_id_(0)
  {}
  bool operator ==(const ObLobId &other) const;
  bool operator !=(const ObLobId &other) const;
  bool operator <(const ObLobId &other) const;
  bool operator >(const ObLobId &other) const;
  TO_STRING_KV(K_(tablet_id), K_(lob_id));
  void reset();
  uint64_t tablet_id_;
  uint64_t lob_id_;
};

struct ObLobDataOutRowCtx
{
  enum OpType
  {
    SQL = 0, // all sql op
    APPEND, // dbms lob op
    INSERT,
    WRITE,
    ERASE,
    EMPTY_SQL, // lob col not change in full sql update, out row ctx is empty
  };
  ObLobDataOutRowCtx()
    : is_full_(0), op_(0), offset_(0), check_sum_(0), seq_no_st_(0), seq_no_cnt_(0),
      del_seq_no_cnt_(0), modified_len_(0), first_meta_offset_(0)
  {}
  TO_STRING_KV(K_(is_full), K_(op), K_(offset), K_(check_sum), K_(seq_no_st), K_(seq_no_cnt),
    K_(del_seq_no_cnt), K_(modified_len), K_(first_meta_offset));
  uint64_t is_full_ : 1;
  uint64_t op_ : 8;
  uint64_t offset_ : 55;
  uint64_t check_sum_;
  int64_t seq_no_st_;
  uint32_t seq_no_cnt_;
  uint32_t del_seq_no_cnt_; // for sql update
  uint64_t modified_len_;
  uint32_t first_meta_offset_;
};

struct ObLobData
{
  ObLobData()
    : id_(), byte_size_(0)
  {}
  bool operator ==(const ObLobData &other) const;
  bool operator !=(const ObLobData &other) const;
  TO_STRING_KV(K_(id), K_(byte_size));
  void reset();
  ObLobId id_;
  uint64_t byte_size_;
  char buffer_[0];
};

struct ObLobCommon
{
  static const int64_t LOB_DATA_VERSION = 1;
  ObLobCommon()
    : version_(LOB_DATA_VERSION), is_init_(0), is_empty_(0), in_row_(1),
      opt_encrypt_(0), opt_compress_(0), opt_deduplicate_(0), has_content_type_(0),
      use_big_endian_(1), is_mem_loc_(0), reserve_(0)
  {}
  bool operator ==(const ObLobCommon &other) const;
  bool operator !=(const ObLobCommon &other) const;
  TO_STRING_KV(K_(version), K_(is_init), K_(is_empty), K_(in_row), K_(opt_encrypt),
      K_(opt_compress), K_(opt_deduplicate), K_(has_content_type), K_(use_big_endian), K_(is_mem_loc),
      K_(reserve));
  int64_t get_handle_size(int64_t byte_size) const
  {
    int64_t s = static_cast<int64_t>(offsetof(ObLobCommon, buffer_));
    if (is_init_) {
      s += sizeof(ObLobData);
    }
    if (in_row_) {
      s += byte_size;
    } else {
      s += sizeof(ObLobDataOutRowCtx) + sizeof(uint64_t);
    }
    return s;
  }
  int64_t get_byte_size(int64_t handle_size) const
  {
    int64_t s = handle_size;
    if (is_init_) {
      const ObLobData *lob_data = reinterpret_cast<const ObLobData*>(buffer_);
      s = lob_data->byte_size_;
    } else {
      s -= static_cast<int64_t>(offsetof(ObLobCommon, buffer_));
    }
    return s;
  }
  const char* get_inrow_data_ptr() const
  {
    const char* ptr = buffer_;
    if (is_init_) {
      const ObLobData *lob_data = reinterpret_cast<const ObLobData*>(buffer_);
      ptr = lob_data->buffer_;
    }
    return ptr;
  }

  // Get full size of outrow lobs if it is built inrow, only used in reading of systables
  static int64_t calc_inrow_handle_size(bool is_init, int64_t byte_size)
  {
    int64_t s = static_cast<int64_t>(offsetof(ObLobCommon, buffer_));
    if (is_init) {
      s += sizeof(ObLobData);
    }
    s += byte_size;
    return s;
  }
  void reset();
  OB_INLINE bool is_valid() const
  {
    bool bret = (version_ == LOB_DATA_VERSION) && (reserve_ == 0);
    if (!in_row_) {
      bret = (bret && is_init_);
    }
    return bret;
  }
  uint32_t version_ : 8;
  /*flag start*/
  uint32_t is_init_ : 1;
  uint32_t is_empty_ : 1;
  uint32_t in_row_ : 1;
  uint32_t opt_encrypt_ : 1;
  uint32_t opt_compress_ : 1;
  uint32_t opt_deduplicate_ : 1;
  uint32_t has_content_type_ : 1;
  uint32_t use_big_endian_ : 1;
  uint32_t is_mem_loc_ : 1;
  uint32_t reserve_ : 15;
  /*flag end*/
  char buffer_[0];
};

//We always get/convert/build locator from existing buffer
#define LOB_DEFAULT_FLAGS UINT16_C(0)
#define LOB_COMPAT_MODE_FLAG (INT64_C(1) << 0) // 1 means old heap table without rowid, fake locator
#define LOB_OPEN_MODE_FLAG (INT64_C(1) << 1) // 0 means lob_readwrite, 1 means lob_readonly
#define LOB_IS_OPEN_FLAG (INT64_C(1) << 2) // 0 means is close, 1 means is open

struct ObLobLocator
{
  static const uint32_t MAGIC_CODE = 0x4C4F4221; //LOB!
  static const uint32_t LOB_LOCATOR_VERSION = 1;
  ObLobLocator() = delete;
  ~ObLobLocator() = delete;
  OB_INLINE bool is_valid() const
  {
    return magic_code_ == MAGIC_CODE && version_ == LOB_LOCATOR_VERSION && snapshot_version_ >= 0
           && is_valid_id(table_id_) && is_valid_id(column_id_)
           && option_ == 0;
  }
  OB_INLINE bool is_fake_locator() const { return !is_inline_mode(); }
  int init(const uint64_t table_id,
           const uint32_t column_id,
           const int64_t snapshot_version,
           const uint16_t flags,
           const ObString &rowid,
           const ObString &payload);
  int init(const ObString &payload); // init a lob locator with fake rowid/table_id and etc.
  int get_rowid(ObString &rowid) const;
  int get_payload(ObString &payload) const;
  OB_INLINE int64_t get_data_length() const { return (payload_offset_ + payload_size_); }
  OB_INLINE int64_t get_total_size() const { return offsetof(ObLobLocator, data_) + get_data_length(); }
  OB_INLINE uint32_t get_payload_length() const { return payload_size_; }
  //TODO @shanting to be deleted
  OB_INLINE const char *get_payload_ptr() const { return &data_[payload_offset_]; }
  OB_INLINE void add_flag(int64_t flag) { flags_ |= flag; }
  OB_INLINE void del_flag(int64_t flag) { flags_ &= ~flag; }
  OB_INLINE void set_inline_mode() { del_flag(LOB_COMPAT_MODE_FLAG); }
  OB_INLINE void set_compat_mode() { add_flag(LOB_COMPAT_MODE_FLAG); }
  OB_INLINE bool is_inline_mode() const { return !(flags_ & LOB_COMPAT_MODE_FLAG); }
  OB_INLINE void set_readwrite() { del_flag(LOB_OPEN_MODE_FLAG); }
  OB_INLINE void set_readonly() { add_flag(LOB_OPEN_MODE_FLAG); }
  OB_INLINE bool is_readwrite() const { return !(flags_ & LOB_OPEN_MODE_FLAG); }
  OB_INLINE void set_is_close() { del_flag(LOB_IS_OPEN_FLAG); }
  OB_INLINE void set_is_open() { add_flag(LOB_IS_OPEN_FLAG); }
  OB_INLINE bool is_open() const { return (flags_ & LOB_IS_OPEN_FLAG); }

  DECLARE_TO_STRING;

  uint32_t magic_code_;
  uint32_t version_;
  int64_t  snapshot_version_;
  uint64_t table_id_;
  uint32_t column_id_;
  uint16_t flags_;
  uint16_t option_; // storage option: compress/ encrypt / dedup
  uint32_t payload_offset_; //  == rowid_size; payload = data_ + payload_offset_
  uint32_t payload_size_;
  char data_[0]; // rowid + varchar
};

// New In-Memory LobLocator
enum ObMemLobType
{
  INVALID_LOB = 0,
  PERSISTENT_LOB = 1,
  TEMP_FULL_LOB = 2,
  TEMP_DELTA_LOB = 3,
  MAX_LOB_TYPE
};

// Memory Locator V2, Common Header, sz/mo6g8w
// Notice: Do not add or remove fields from this sturct!
// 8 bytes
struct ObMemLobCommon
{
  // locator version 2, the old was 1
  static const uint8_t MEM_LOB_LOCATOR_VERSION = 2;
  static const uint32_t MAGIC_CODE2 = 0x7F7FABCD; // LOB magic for debug

  ObMemLobCommon(ObMemLobType type, bool is_simple) :
    lob_common_(), version_(MEM_LOB_LOCATOR_VERSION), type_(type), read_only_(0),
    has_inrow_data_(1), is_open_(0), is_simple_(is_simple), has_extern_(0), is_freed_(0), reserved_(0)
  { lob_common_.is_mem_loc_ = 1; }

  OB_INLINE void set_extern(bool has_extern) { has_extern_ = has_extern ? 1 : 0; };
  OB_INLINE void set_read_only(bool is_read_only) { read_only_ = is_read_only ? 1 : 0; }
  OB_INLINE void set_has_inrow_data(bool has_inrow_data) { has_inrow_data_ =  has_inrow_data ? 1 : 0 ; }
  OB_INLINE void set_open(bool is_open) { is_open_ = is_open ? 1 : 0; }
  OB_INLINE void set_simple(bool is_simple) { is_simple_ = is_simple ? 1 : 0; }
  OB_INLINE void set_freed(bool freed) { is_freed_ = freed ? 1 : 0; };

  OB_INLINE bool is_read_only() { return read_only_ == 1; }
  OB_INLINE bool has_inrow_data() { return has_inrow_data_ == 1; }
  OB_INLINE bool is_open() { return is_open_ == 1; }
  OB_INLINE bool is_simple() { return is_simple_ == 1; }
  OB_INLINE bool has_extern() {return has_extern_ == 1; }
  OB_INLINE bool is_freed() {return is_freed_ == 1; }
  OB_INLINE bool is_persist() { return type_ == PERSISTENT_LOB; }
  OB_INLINE bool is_temporary_full() { return type_ == TEMP_FULL_LOB; }
  OB_INLINE bool is_temporary_delta() { return type_ == TEMP_DELTA_LOB; }

  OB_INLINE bool has_rowkey_addr() { return has_extern(); } // Notice: rowkey maybe empty string

  OB_INLINE bool is_valid() const
  {
    bool bret = (lob_common_.is_valid() && lob_common_.is_mem_loc_ == 1)
                && (version_ == MEM_LOB_LOCATOR_VERSION)
                && (type_ > INVALID_LOB && type_ <= TEMP_DELTA_LOB)
                && ((is_simple_ & has_extern_) != 1);
    if (!bret) {
      COMMON_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "Invalid lob locator v2!",
        K(lob_common_), K(version_), K(type_), K(is_simple_), K(has_extern_), K(lbt()));
    }
    return bret;
  }

  TO_STRING_KV(K_(lob_common), K_(type), K_(read_only), K_(has_inrow_data), K_(is_open), K_(is_simple),
               K_(has_extern), K_(is_freed), K_(reserved), K_(version));

  ObLobCommon lob_common_;

  // version, type, flags total 4 bytes, correspoinding to the old locator version
  uint32_t version_ : 8;
  uint32_t type_ : 4;     // ObMemLobType (Persistent/TmpFull/TmpDelta)

  // flags, 20 bits
  uint32_t read_only_ : 1;
  uint32_t has_inrow_data_ : 1;  // Indicate whether the lob is in-Memory inrow
                                 // However, it may not be the same with the flag in disk locator.
                                 // compatibility flag for client
  uint32_t is_open_ : 1;   // Indicate whether the persist lob is open, compatible with oracle
  uint32_t is_simple_ : 1; // Indicate whether the lob has this common part only. Used for inrow lobs
                           // which do not need rowkey (Only used in mysql modes now)
  uint32_t has_extern_ : 1; // Indicate whether the lob locator has extern segment
  uint32_t is_freed_ : 1; // Indicate whether the temp lob locator has been freed
  uint32_t reserved_  : 14;


  char data_[0];
};

struct ObMemLobExternFlags
{
  ObMemLobExternFlags() :
    has_tx_info_(1), has_location_info_(1), reserved_(0)
  {}

  ObMemLobExternFlags(bool enable) :
    has_tx_info_(enable), has_location_info_(enable), reserved_(0)
  {}

  ObMemLobExternFlags(const ObMemLobExternFlags &flags) { *this = flags; }

  OB_INLINE bool is_empty() const
  {
    return (*(reinterpret_cast<const uint16_t *>(this)) == 0);
  }

  OB_INLINE bool set_empty()
  {
    return (*(reinterpret_cast<uint16_t *>(this)) = 0);
  }

  TO_STRING_KV(K_(has_tx_info), K_(has_location_info), K_(reserved));

  uint16_t has_tx_info_ : 1; // Indicate whether tx info exists
  uint16_t has_location_info_ : 1; // Indicate whether has cid exists (reserved)
  uint16_t reserved_ : 14;
};

// Memory Locator V2, Extern Header:
// Notice: Do not add or remove fields from this sturct!
// 32 bytes
struct ObMemLobExternHeader
{
  ObMemLobExternHeader(const ObMemLobExternFlags &flags, uint16_t rowkey_size) :
    reserved_(0), table_id_(0), column_idx_(0), flags_(flags),
    rowkey_size_(rowkey_size), payload_offset_(0), payload_size_(0)
  {}

  ObMemLobExternHeader(uint64_t table_id, uint32_t column_idx, uint16_t rowkey_size,
                       uint32_t payload_offset, uint32_t payload_size) :
    reserved_(0), table_id_(table_id), column_idx_(column_idx), flags_(),
    rowkey_size_(rowkey_size),
    payload_offset_(payload_offset), payload_size_(payload_size)
  {}

  TO_STRING_KV(K_(reserved), K_(table_id), K_(column_idx), K_(flags), K_(rowkey_size),
               K_(payload_offset), K_(payload_size));

  int64_t reserved_; // 8 bytes snpahot version is not enough for read, just reserved
  uint64_t table_id_;
  uint32_t column_idx_;

  // correspoinding to ObLobLocator(v1) uint16_t mode_
  ObMemLobExternFlags flags_;

  // correspoinding to ObLobLocator(v1) uint16_t option_
  uint16_t rowkey_size_; // Max rowkey size is 16KB

  // correspoinding to ObLobLocator(v1) payload_offset_ and payload_size_
  uint32_t payload_offset_;  // total length of extern fields, rowkey, and disk locator
  uint32_t payload_size_;    // total inrow data size, <= 4GB

  char data_[0];
};

// Memory Locator V2, Extern Body Structs
struct ObMemLobTxInfo
{
  ObMemLobTxInfo(){}
  ObMemLobTxInfo(int64_t snapshot_ver, int64_t tx_id, int64_t scn) :
    snapshot_version_(snapshot_ver), snapshot_tx_id_(tx_id), snapshot_seq_(scn)
  {}
  TO_STRING_KV(K_(snapshot_version), K_(snapshot_tx_id), K_(snapshot_seq));

  int64_t snapshot_version_;
  int64_t snapshot_tx_id_;
  int64_t snapshot_seq_;
  char data_[0];
};

struct ObMemLobLocationInfo
{
  ObMemLobLocationInfo(){}
  ObMemLobLocationInfo(int64_t table_id, int64_t ls_id, ObCollationType collation_id) :
    tablet_id_(table_id), ls_id_(ls_id), cid_(collation_id)
  {}
  TO_STRING_KV(K_(tablet_id), K_(ls_id), K_(cid));

  int64_t tablet_id_;
  int64_t ls_id_;
  ObCollationType cid_; // charset for dbmslob
  char data_[0];
};

OB_INLINE void validate_has_lob_header(const bool &has_header)
{
#ifdef VALIDATE_LOB_HEADER
  // Debug only, disabled by default
  OB_ASSERT(has_header == true);
#endif
};

// In-Memory LobLocator Content:
// ObMemLobCommon       (all lobs)|
// ObMemLobExternHeader (has extern flag) |
// externs length       (uint16_t, extern flags) |
// ObMemLobExterns      (extern flags) |
// Rowkey & Addr        (has extern flag) |
// Disk locator         (all non-simple lobs)|
// inline buffer
struct ObLobLocatorV2
{
OB_UNIS_VERSION(1);
public:
  static const uint32_t MEM_LOB_COMMON_HEADER_LEN = sizeof(ObMemLobCommon);
  static const uint32_t MEM_LOB_EXTERN_HEADER_LEN = sizeof(ObMemLobExternHeader);
  static const uint32_t MEM_LOB_EXTERN_TXINFO_LEN = sizeof(ObMemLobTxInfo);
  static const uint32_t MEM_LOB_EXTERN_LOCATIONINFO_LEN = sizeof(ObMemLobLocationInfo);
  static const uint16_t MEM_LOB_EXTERN_SIZE_LEN = sizeof(uint16_t);
  static const uint32_t MEM_LOB_ADDR_LEN = 0; // reserved for temp lob address

  ObLobLocatorV2() : ptr_(NULL), size_(0), has_lob_header_(true) {}
  ObLobLocatorV2(char *loc_ptr, uint32_t loc_size, uint32_t has_lob_header = true) :
    ptr_(loc_ptr), size_(loc_size), has_lob_header_(has_lob_header)
  {
    if (loc_ptr == NULL || loc_size == 0) {
    } else {
      validate_has_lob_header(has_lob_header_);
    }
  }
  ObLobLocatorV2(ObString &lob_str, uint32_t has_lob_header = true) :
    ptr_(lob_str.ptr()), size_(lob_str.length()), has_lob_header_(has_lob_header)
  {
    if (lob_str.empty()) {
    } else {
      validate_has_lob_header(has_lob_header_);
    }
  }
  ObLobLocatorV2(const ObString &lob_str, uint32_t has_lob_header = true) :
    ptr_(const_cast<char *>(lob_str.ptr())), size_(lob_str.length()), has_lob_header_(has_lob_header)
  {
    if (lob_str.empty()) {
    } else {
      validate_has_lob_header(has_lob_header_);
    }
  }

  ~ObLobLocatorV2() {}

  DECLARE_TO_STRING;

  OB_INLINE bool is_lob_locator_v1() const
  {
    // Notice: should be called only when ptr_ is from ObLobType data
    ObLobLocator *loc_v1 = reinterpret_cast<ObLobLocator *>(ptr_);
    return size_ >= sizeof(ObLobLocator) && loc_v1->is_valid();
  }

  OB_INLINE bool is_lob_disk_locator() const
  {
    // Notice: should be called only when ptr_ is not null
    ObLobCommon *loc = reinterpret_cast<ObLobCommon *>(ptr_);
    return has_lob_header_ && (loc != nullptr) && (size_ >= sizeof(ObLobCommon)) &&
           (loc->is_valid()) && (loc->is_mem_loc_ == 0);
  }

  OB_INLINE bool is_valid(bool is_assert = true) const
  {
    bool bret = true;
    if (is_assert) {
      validate_has_lob_header(has_lob_header_); // remove later
    }
    if (OB_UNLIKELY(!has_lob_header_ || size_ == 0)) {
      // cannot validate without header or len is zero
    } else if (OB_NOT_NULL(ptr_) && is_lob_disk_locator()) {
      // cannot validate with disk
    } else {
      ObMemLobCommon *loc = reinterpret_cast<ObMemLobCommon *>(ptr_);
      bret = (OB_NOT_NULL(loc) && size_ >= MEM_LOB_COMMON_HEADER_LEN) ? loc->is_valid() : false;
      if (bret && loc->has_extern() && size_ < MEM_LOB_COMMON_HEADER_LEN + MEM_LOB_EXTERN_HEADER_LEN) {
        bret = false;
      }
      if (!bret) {
        COMMON_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "Invalid lob locator!", KP(ptr_), K(size_));
        if (OB_NOT_NULL(loc) && is_assert) {
          OB_ASSERT(0);
        }
      }
    }
    return bret;
  }

  // remove if not used
  void assign_ptr(const ObMemLobCommon *lobv2, uint32 len, uint32_t has_lob_header = true)
  {
    ptr_ = reinterpret_cast<char *>(const_cast<ObMemLobCommon *>(lobv2));
    size_ = len;
    has_lob_header_ = has_lob_header;
  }

  void assign_buffer(char *loc_buff, uint32 len, uint32_t has_lob_header = true)
  {
    ptr_ = loc_buff;
    size_ = len;
    has_lob_header_ = has_lob_header;
  }

  // Notice: disk_lob_full_size = (disk locator header size if any) + inline buffer
  static uint32_t calc_locator_full_len(const ObMemLobExternFlags &flags,
                                        uint32_t rowkey_size,
                                        uint32_t disk_lob_full_size,
                                        bool is_simple);

  // interfaces for write
  // fill empty lob locator
  int fill(ObMemLobType type,
           const ObMemLobExternFlags &flags,
           const ObString &rowkey_str,
           const ObLobCommon *disk_loc,
           uint32_t disk_lob_full_size,
           uint32_t disk_lob_header_size,
           bool is_simple);

  int copy(const ObLobLocatorV2* src_locator) const;
  int set_table_info(const uint64_t &table_id, const uint32_t &column_idx);
  int set_payload_data(const ObString& payload);
  int set_payload_data(const ObLobCommon *lob_comm, const ObString& payload);
  int set_tx_info(const ObMemLobTxInfo &tx_info);
  int set_location_info(const ObMemLobLocationInfo &location_info);

  // interfaces for read
  // Notice: all the following functions should be called after is_valid() or fill()
  int get_mem_locator(ObMemLobCommon *&mem_loc) const;
  int get_extern_header(ObMemLobExternHeader *&extern_header) const;
  int get_rowkey(ObString &rowkey_str) const;
  int get_disk_locator(ObLobCommon *&disk_loc) const;
  int get_disk_locator(ObString &disc_loc_buff) const;

  int get_inrow_data(ObString &inrow_data) const;
  int get_lob_data_byte_len(int64_t &len) const;
  int get_table_info(uint64_t &table_id, uint32_t &column_idex);
  int get_tx_info(ObMemLobTxInfo *&tx_info) const;
  int get_location_info(ObMemLobLocationInfo *&location_info) const;
  int get_real_locator_len(int64_t &real_len) const;

  bool is_empty_lob() const;
  bool is_inrow() const;
  bool is_null() const { return ptr_ == NULL || size_ == 0; }

  OB_INLINE bool is_readonly() const
  {
    validate_has_lob_header(has_lob_header_);
    return has_lob_header_ && OB_NOT_NULL(ptr_) &&
           !is_lob_disk_locator() && size_ >= MEM_LOB_COMMON_HEADER_LEN &&
           (reinterpret_cast<ObMemLobCommon *>(ptr_))->is_read_only();
  }

  OB_INLINE bool is_freed() const
  {
    validate_has_lob_header(has_lob_header_);
    return has_lob_header_ && OB_NOT_NULL(ptr_) &&
           !is_lob_disk_locator() && size_ >= MEM_LOB_COMMON_HEADER_LEN &&
           (reinterpret_cast<ObMemLobCommon *>(ptr_))->is_freed();
  }

  OB_INLINE bool has_inrow_data() const
  {
    bool bret = false;
    if (!has_lob_header_ || size_ == 0) {
      bret = true;
    } else if (OB_NOT_NULL(ptr_)) {
      if (is_lob_disk_locator()) {
        bret = (reinterpret_cast<ObLobCommon *>(ptr_))->in_row_;
      } else if (size_ >= MEM_LOB_COMMON_HEADER_LEN) {
        bret = (reinterpret_cast<ObMemLobCommon *>(ptr_))->has_inrow_data();
      }
    }
    return bret;
  }

  OB_INLINE bool is_open() const
  {
    validate_has_lob_header(has_lob_header_);
    return has_lob_header_ && OB_NOT_NULL(ptr_) &&
           !is_lob_disk_locator() && size_ >= MEM_LOB_COMMON_HEADER_LEN &&
           (reinterpret_cast<ObMemLobCommon *>(ptr_))->is_open();
  }

  OB_INLINE bool is_simple() const
  {
    validate_has_lob_header(has_lob_header_);
    return has_lob_header_ && OB_NOT_NULL(ptr_) &&
           !is_lob_disk_locator() && size_ >= MEM_LOB_COMMON_HEADER_LEN &&
           (reinterpret_cast<ObMemLobCommon *>(ptr_))->is_simple();
  }

  OB_INLINE bool has_extern() const
  {
    validate_has_lob_header(has_lob_header_);
    return has_lob_header_ && OB_NOT_NULL(ptr_) &&
           !is_lob_disk_locator() && size_ >= MEM_LOB_COMMON_HEADER_LEN &&
           (reinterpret_cast<ObMemLobCommon *>(ptr_))->has_extern();
  }

  // all lob types without header are treated as full temp lob
  OB_INLINE bool is_persist_lob() const
  {
    validate_has_lob_header(has_lob_header_);
    return has_lob_header_ && OB_NOT_NULL(ptr_) &&
           !is_lob_disk_locator() && size_ >= MEM_LOB_COMMON_HEADER_LEN &&
           (reinterpret_cast<ObMemLobCommon *>(ptr_))->is_persist();
  }

  OB_INLINE bool is_full_temp_lob() const
  {
    validate_has_lob_header(has_lob_header_);
    return (!has_lob_header_)
           || (has_lob_header_ && OB_NOT_NULL(ptr_) &&
              !is_lob_disk_locator() && size_ >= MEM_LOB_COMMON_HEADER_LEN &&
              (reinterpret_cast<ObMemLobCommon *>(ptr_))->is_temporary_full());
  }

  OB_INLINE bool is_delta_temp_lob() const
  {
    validate_has_lob_header(has_lob_header_);
    return has_lob_header_ && OB_NOT_NULL(ptr_) &&
           !is_lob_disk_locator() && size_ >= MEM_LOB_COMMON_HEADER_LEN &&
           (reinterpret_cast<ObMemLobCommon *>(ptr_))->is_temporary_delta();
  }

  OB_INLINE bool has_lob_header() const { return has_lob_header_; }

  char *ptr_; // if has_lob_header ptr_ is ObMemLobCommon, else it is data content
  uint32_t size_; // full MemLobLocator size;
  bool has_lob_header_; // for observer 4.0 compatibility
};

struct ObObjPrintParams
{
  ObObjPrintParams (const ObTimeZoneInfo *tz_info, ObCollationType cs_type):
    tz_info_(tz_info),
    cs_type_(cs_type),
    print_flags_(0),
    exec_ctx_(NULL),
    ob_obj_type_(ObNullType)
  {}
  ObObjPrintParams (const ObTimeZoneInfo *tz_info):
    tz_info_(tz_info),
    cs_type_(CS_TYPE_UTF8MB4_GENERAL_CI),
    print_flags_(0),
    exec_ctx_(NULL),
    ob_obj_type_(ObNullType)
  {}
  ObObjPrintParams ():
    tz_info_(NULL),
    cs_type_(CS_TYPE_UTF8MB4_GENERAL_CI),
    print_flags_(0),
    exec_ctx_(NULL),
    ob_obj_type_(ObNullType)
  {}
  TO_STRING_KV(K_(tz_info), K_(cs_type),K_(print_flags), K_(ob_obj_type));
  const ObTimeZoneInfo *tz_info_;
  ObCollationType cs_type_;
  union {
    uint32_t print_flags_;
    struct {
      uint32_t need_cast_expr_:1;
      uint32_t print_origin_stmt_:1;
      uint32_t use_memcpy_:1;
      uint32_t skip_escape_:1;
      uint32_t beginning_space_:1;
      uint32_t for_dblink_:1;
      uint32_t binary_string_print_hex_:1;
      uint32_t need_print_converter_:1;
      uint32_t print_const_expr_type_:1;
      uint32_t print_null_string_value_:1;
      uint32_t reserved_:22;
    };
  };

  /**
   * MySQL only
   * The limit expr may no longer be a constant number after transformer.
   * So, set exec_ctx_ if the stmt is rewritten.
   * If exec_ctx_ is not null, Printer will calculate the value of limit expr and output.
   *  e.g. "limit (1 + 1)" will be output as "limit 2"
   *
  */
  sql::ObExecContext *exec_ctx_;
  ObObjType ob_obj_type_;
};

// sizeof(ObObjValue)=8
union ObObjValue
{
  int64_t int64_;
  uint64_t uint64_;

  float float_;
  double double_;

  const char *string_;

  uint32_t *nmb_digits_;

  int64_t datetime_;
  int32_t date_;
  int64_t time_;
  uint8_t year_;

  int64_t ext_;
  int64_t unknown_;
  const ObLobCommon *lob_;
  const ObLobLocator *lob_locator_;
  const ObMemLobCommon *lob_locator_v2_;
  int64_t nmonth_; //for interval year to month
  int64_t nsecond_; //for interval day to second
  void *ptr_;
};

extern int (*serialize_composite_callback)(const ObObj&, char*, const int64_t, int64_t&);
extern int (*deserialize_composite_callback)(ObObj&, const char*, const int64_t, int64_t&);
extern int64_t (*composite_serialize_size_callback)(const ObObj&);

class ObBatchChecksum;
class ObObj
{
public:
  // min, max extend value
  static const int64_t MIN_OBJECT_VALUE         = UINT64_MAX - 2;
  static const int64_t MAX_OBJECT_VALUE         = UINT64_MAX - 1;
  // WARNING: (xiaochu.yh) used only in RootTable, other user should not use this
  // to represent a (min, max) range in Roottable impl, we need this
  static const char *MIN_OBJECT_VALUE_STR;
  static const char *MAX_OBJECT_VALUE_STR;
  static const char *NOP_VALUE_STR;
public:
  ObObj();
  ObObj(const ObObj &other);
  explicit ObObj(bool val);
  explicit ObObj(int32_t val);
  explicit ObObj(int64_t val);
  explicit ObObj(ObObjType type);
  inline void reset();
  //when in not strict sql mode, build default value refer to data type
  int build_not_strict_default_value();
  static ObObj make_min_obj();
  static ObObj make_max_obj();
  static ObObj make_nop_obj();
  static bool is_ext_val(int64_t v)
  {
    return (ObActionFlag::OP_MIN_OBJ <= v
        && ObActionFlag::OP_LOCK_ROW >= v)
        || MIN_OBJECT_VALUE == v
        || MAX_OBJECT_VALUE == v;
  }

  OB_INLINE void copy_meta_type(const ObObjMeta &meta) {
    meta_.set_type_simple(meta.get_type());
    meta_.set_collation_type(meta.get_collation_type());
    if (ObCharType == get_type() || ObVarcharType == get_type() || ob_is_text_tc(get_type())
        || ob_is_lob_locator(get_type()) || ob_is_json(get_type()) || ob_is_geometry(get_type())) {
      meta_.set_collation_level(ObCollationLevel::CS_LEVEL_IMPLICIT);
    } else {
      meta_.set_collation_level(meta.get_collation_level());
    }
    if (meta.has_lob_header() && oceanbase::is_lob_storage(get_type())) {
      set_has_lob_header();
    } else if (meta.is_user_defined_sql_type()) {
      meta_.set_udt_flags(meta.get_udt_flags());
    }
  }

  OB_INLINE void copy_value_to(ObObj &obj, bool &has_null) const {
    switch (get_type()) {
      case ObNullType:
      case ObExtendType: {
        obj = *this;
        has_null = true;
        break;
      }
      case ObCharType:
      case ObVarcharType:
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObLongTextType:
      case ObLobType:
      case ObJsonType:
      case ObGeometryType:
      case ObRawType: {
        obj.meta_.set_collation_level(meta_.get_collation_level());
        obj.meta_.set_scale(meta_.get_scale());
        obj.val_len_ = val_len_;
        obj.v_ = v_;
        break;
      }
      default: {
        obj.meta_.set_scale(meta_.get_scale());
        obj.val_len_ = val_len_;
        obj.v_ = v_;
        break;
      }
    }
  }

  OB_INLINE void copy_value_or_obj(ObObj &obj, bool is_copy_all) const {
    if (is_copy_all) {
      obj = *this;
    } else {
      obj.meta_.set_scale(meta_.get_scale());
      obj.val_len_ = val_len_;
      obj.v_ = v_;
    }
  }
  //@{ setters
  OB_INLINE void set_type(const ObObjType &type)
  {
    if (OB_UNLIKELY(ObNullType > type || ObMaxType < type)) {
      COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "invalid type", K(type));
      meta_.set_type(ObUnknownType);
    } else {
      meta_.set_type(type);
    }
  }
  static int64_t get_otimestamp_store_size(const bool is_timestamp_tz)
  {
    return static_cast<int64_t>(sizeof(int64_t)
        + (is_timestamp_tz
           ? sizeof(uint32_t)
           : sizeof(uint16_t)));
  }
  template<typename T>
  void set_obj_value(const T &v);
  void set_collation_level(const ObCollationLevel &cs_level) { meta_.set_collation_level(cs_level); }
  void set_collation_type(const ObCollationType &cs_type) { meta_.set_collation_type(cs_type); }
  void set_default_collation_type() { meta_.set_default_collation_type(); }
  void set_meta_type(const ObObjMeta &type) { meta_ = type; }
  void set_collation(const ObObjMeta &type) { meta_.set_collation(type); }
  void set_scale(ObScale scale) { meta_.set_scale(scale); }

  void set_int(const ObObjType type, const int64_t value);
  void set_tinyint(const int8_t value);
  void set_smallint(const int16_t value);
  void set_mediumint(const int32_t value);
  void set_int32(const int32_t value);
  void set_int(const int64_t value);  // aka bigint

  void set_tinyint_value(const int8_t value);
  void set_smallint_value(const int16_t value);
  void set_mediumint_value(const int32_t value);
  void set_int32_value(const int32_t value);
  void set_int_value(const int64_t value);  // aka bigint

  void set_uint(const ObObjType type, const uint64_t value);
  void set_utinyint(const uint8_t value);
  void set_utinyint_value(const uint8_t value);
  void set_usmallint(const uint16_t value);
  void set_usmallint_value(const uint16_t value);
  void set_umediumint(const uint32_t value);
  void set_umediumint_value(const uint32_t value);
  void set_uint32(const uint32_t value);
  void set_uint32_value(const uint32_t value);
  void set_uint64(const uint64_t value);
  void set_uint64_value(const uint64_t value);

  void set_float(const ObObjType type, const float value);
  void set_float(const float value);
  void set_float_value(const float value);
  void set_ufloat(const float value);
  void set_ufloat_value(const float value);

  void set_double(const ObObjType type, const double value);
  void set_double(const double value);
  void set_double_value(const double value);
  void set_udouble(const double value);
  void set_udouble_value(const double value);

  void set_number(const ObObjType type, const number::ObNumber &num);
  void set_number(const ObObjType type, const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits);
  void set_number(const number::ObNumber &num);
  void set_number_value(const number::ObNumber &num);
  void set_number_value(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits);
  void set_number(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits);

  void set_unumber(const number::ObNumber &num);
  void set_unumber(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits);
  void set_unumber_value(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits);

  void set_number_float(const number::ObNumber &num);
  void set_number_float(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits);
  void set_number_float_value(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits);

  void set_datetime(const ObObjType type, const int64_t value);
  void set_datetime(const int64_t value);
  void set_timestamp(const int64_t value);
  void set_date(const int32_t value);
  void set_time(const int64_t value);
  void set_year(const uint8_t value);

  void set_datetime_value(const int64_t value);
  void set_timestamp_value(const int64_t value);
  void set_date_value(const int32_t value);
  void set_time_value(const int64_t value);
  void set_year_value(const uint8_t value);

  //only set v_.string_ and length
  void set_common_value(const ObString &value);

  void set_string(const ObObjType type, const char *ptr, const ObString::obstr_size_t size);
  void set_string(const ObObjType type, const ObString &value);
  void set_varchar(const ObString &value);
  void set_varchar(const char *ptr, const ObString::obstr_size_t size);
  void set_varchar_value(const char *ptr, const ObString::obstr_size_t size);
  void set_varchar(const char *cstr);
  void set_char(const ObString &value);
  void set_char_value(const char *ptr, const ObString::obstr_size_t size);
  void set_varbinary(const ObString &value);
  void set_binary(const ObString &value);
  void set_raw(const ObString &value);
  void set_raw(const char *ptr, const ObString::obstr_size_t size);
  void set_raw_value(const char *ptr, const ObString::obstr_size_t size);
  void set_hex_string(const ObString &value);
  void set_hex_string_value(const ObString &value);
  void set_hex_string_value(const char *ptr, const ObString::obstr_size_t size);
  void set_enum(const uint64_t value);
  void set_enum_value(const uint64_t value);
  void set_set(const uint64_t value);
  void set_set_value(const uint64_t value);
  void set_enum_inner(const ObString &value);
  void set_enum_inner(const char *ptr, const ObString::obstr_size_t size);
  void set_set_inner(const ObString &value);
  void set_set_inner(const char *ptr, const ObString::obstr_size_t size);
  void set_lob_value(const ObObjType type, const ObLobCommon *value, const int32_t length);
  void set_lob_value(const ObObjType type, const char *ptr, const int32_t length);
  void set_json_value(const ObObjType type, const ObLobCommon *value, const int32_t length);
  void set_json_value(const ObObjType type, const char *ptr, const int32_t length);
  void set_geometry_value(const ObObjType type, const ObLobCommon *value, const int32_t length);
  void set_geometry_value(const ObObjType type, const char *ptr, const int32_t length);
  void set_lob_locator(const ObLobLocator &value);
  void set_lob_locator(const ObObjType type, const ObLobLocator &value);
  inline void set_inrow() { meta_.set_inrow(); }
  inline void set_outrow() { meta_.set_outrow(); }
  inline void set_has_lob_header() { meta_.set_has_lob_header(); }
  void set_otimestamp_value(const ObObjType type, const ObOTimestampData &value);
  void set_otimestamp_value(const ObObjType type, const int64_t time_us, const uint32_t time_ctx_desc);
  void set_otimestamp_value(const ObObjType type, const int64_t time_us, const uint16_t time_desc);
  void set_otimestamp_null(const ObObjType type);

  void set_timestamp_tz(const int64_t time_us, const uint32_t time_ctx_desc) { set_otimestamp_value(ObTimestampTZType, time_us, time_ctx_desc); }
  void set_timestamp_ltz(const int64_t time_us, const uint16_t time_desc) { set_otimestamp_value(ObTimestampLTZType, time_us, time_desc); }
  void set_timestamp_nano(const int64_t time_us, const uint16_t time_desc) { set_otimestamp_value(ObTimestampNanoType, time_us, time_desc); }
  void set_timestamp_tz(const ObOTimestampData &value) { set_otimestamp_value(ObTimestampTZType, value); }
  void set_timestamp_ltz(const ObOTimestampData &value) { set_otimestamp_value(ObTimestampLTZType, value); }
  void set_timestamp_nano(const ObOTimestampData &value) { set_otimestamp_value(ObTimestampNanoType, value); }

  inline void set_bool(const bool value);
  inline void set_ext(const int64_t value);
  inline void set_extend(const int64_t value, uint8 extend_type, int32_t size = 0);
  inline void set_unknown(const int64_t value);

  inline void set_bit(const uint64_t value);
  inline void set_bit_value(const uint64_t value);

  inline void set_null();
  inline void set_min_value();
  inline void set_max_value();
  inline void set_nop_value();

  inline void set_lob(const char* ptr, const int32_t size, const ObLobScale &lob_scale);

  void set_val_len(const int32_t val_len);
  void set_null_meta(const ObObjMeta meta);

  void set_interval_ym(const ObIntervalYMValue &value);
  void set_interval_ds(const ObIntervalDSValue &value);
  void set_interval_ym(const int64_t value) { set_interval_ym(ObIntervalYMValue(value)); }
  void set_nvarchar2(const ObString &value);
  void set_nvarchar2_value(const char *ptr, const ObString::obstr_size_t size);
  void set_nchar(const ObString &value);
  void set_nchar_value(const char *ptr, const ObString::obstr_size_t size);

  void set_urowid(const ObURowIDData &urowid)
  {
    meta_.set_urowid();
    v_.string_ = (const char *)urowid.rowid_content_;
    val_len_ = static_cast<int32_t>(urowid.rowid_len_);
  }
  void set_urowid(const char *ptr, const int64_t size)
  {
    meta_.set_urowid();
    v_.string_ = ptr;
    val_len_ = static_cast<int32_t>(size);
  }

  inline void set_sql_udt(const char* ptr, int32_t size, uint16_t subschema_id = 0, uint8_t udt_flags = 0);
  inline void set_udt_value(const char* ptr, int32_t size);
  //@}

  //@{ getters
  OB_INLINE ObObjType get_type() const { return meta_.get_type(); }
  OB_INLINE ObObjTypeClass get_type_class() const { return meta_.get_type_class(); }
  OB_INLINE ObCollationLevel get_collation_level() const { return meta_.get_collation_level(); }
  OB_INLINE ObCollationType get_collation_type() const { return meta_.get_collation_type(); }
  OB_INLINE ObScale get_scale() const { return meta_.get_scale(); }
  inline const ObObjMeta& get_meta() const { return meta_; }
  inline const ObObjMeta& get_null_meta() const { return null_meta_; }

  inline int get_tinyint(int8_t &value) const;
  inline int get_smallint(int16_t &value) const;
  inline int get_mediumint(int32_t &value) const;
  inline int get_int32(int32_t &value) const;
  inline int get_int(int64_t &value) const;

  inline int get_utinyint(uint8_t &value) const;
  inline int get_usmallint(uint16_t &value) const;
  inline int get_umediumint(uint32_t &value) const;
  inline int get_uint32(uint32_t &value) const;
  inline int get_uint64(uint64_t &value) const;

  inline int get_float(float &value) const;
  inline int get_double(double &value) const;
  inline int get_ufloat(float &value) const;
  inline int get_udouble(double &value) const;

  inline int get_number(number::ObNumber &num) const;
  inline int get_unumber(number::ObNumber &num) const;
  inline int get_number_float(number::ObNumber &num) const;

  inline int get_datetime(int64_t &value) const;
  inline int get_timestamp(int64_t &value) const;
  inline int get_date(int32_t &value) const;
  inline int get_time(int64_t &value) const;
  inline int get_year(uint8_t &value) const;

  inline int get_string(ObString &value) const;
  inline int get_varchar(ObString &value) const;
  inline int get_char(ObString &value) const;
  inline int get_nvarchar2(ObString &value) const;
  inline int get_nchar(ObString &value) const;
  inline int get_varbinary(ObString &value) const;
  inline int get_raw(ObString &value) const;
  inline int get_binary(ObString &value) const;
  inline int get_hex_string(ObString &value) const;
  inline int get_print_string(ObString &value) const;

  inline int get_bool(bool &value) const;
  inline int get_ext(int64_t &value) const;
  inline int get_unknown(int64_t &value) const;
  inline int get_bit(uint64_t &value) const;
  inline int get_enum(uint64_t &value) const;
  inline int get_set(uint64_t &value) const;
  int get_enum_str_val(ObSqlString &str_val, const ObIArray<ObString> &type_infos) const;
  int get_set_str_val(ObSqlString &str_val, const ObIArray<ObString> &type_infos) const;
  inline int32_t get_val_len() const { return val_len_; }
  inline int get_enumset_inner_value(ObEnumSetInnerValue &inner_value) const;

  int get_interval_ym(ObIntervalYMValue &value) const;
  int get_interval_ds(ObIntervalDSValue &value) const;

  int get_urowid(ObURowIDData &urowid_data) const;

  /// the follow getters do not check type, use them when you already known the type
  OB_INLINE int8_t get_tinyint() const { return static_cast<int8_t>(v_.int64_); }
  OB_INLINE int16_t get_smallint() const { return static_cast<int16_t>(v_.int64_); }
  OB_INLINE int32_t get_mediumint() const { return static_cast<int32_t>(v_.int64_); }
  OB_INLINE int32_t get_int32() const { return static_cast<int32_t>(v_.int64_); }
  OB_INLINE int64_t get_int() const { return static_cast<int64_t>(v_.int64_); }

  OB_INLINE uint8_t get_utinyint() const { return static_cast<uint8_t>(v_.uint64_); }
  OB_INLINE uint16_t get_usmallint() const { return static_cast<uint16_t>(v_.uint64_); }
  OB_INLINE uint32_t get_umediumint() const { return static_cast<uint32_t>(v_.uint64_); }
  OB_INLINE uint32_t get_uint32() const { return static_cast<uint32_t>(v_.uint64_); }
  OB_INLINE uint64_t get_uint64() const { return static_cast<uint64_t>(v_.uint64_); }

  OB_INLINE float get_float() const { return v_.float_; }
  OB_INLINE double get_double() const { return v_.double_; }
  OB_INLINE float get_ufloat() const { return v_.float_; }
  OB_INLINE double get_udouble() const { return v_.double_; }

  OB_INLINE bool is_negative_number() const { return number::ObNumber::is_negative_number(nmb_desc_); }
  OB_INLINE bool is_zero_number() const { return number::ObNumber::is_zero_number(nmb_desc_); }
  OB_INLINE int64_t get_number_digit_length() const { return nmb_desc_.len_; }
  OB_INLINE int64_t get_number_byte_length() const { return nmb_desc_.len_ * sizeof(uint32_t); }
  OB_INLINE number::ObNumber get_number() const { return number::ObNumber(nmb_desc_.desc_, v_.nmb_digits_); }
  OB_INLINE number::ObNumber get_unumber() const { return number::ObNumber(nmb_desc_.desc_, v_.nmb_digits_); }
  OB_INLINE number::ObNumber get_number_float() const { return number::ObNumber(nmb_desc_.desc_, v_.nmb_digits_); }

  OB_INLINE int64_t get_datetime() const { return v_.datetime_; }
  OB_INLINE int64_t get_timestamp() const { return v_.datetime_; }
  OB_INLINE int32_t get_date() const { return v_.date_; }
  OB_INLINE int64_t get_time() const { return v_.time_; }
  OB_INLINE uint8_t get_year() const { return v_.year_; }

  OB_INLINE ObString get_string() const {
    return ObString(val_len_, v_.string_);
  }
  OB_INLINE ObString get_varchar() const { return ObString(val_len_, v_.string_); }
  OB_INLINE ObString get_char() const { return ObString(val_len_, v_.string_); }
  OB_INLINE ObString get_varbinary() const { return ObString(val_len_, v_.string_); }
  OB_INLINE ObString get_raw() const { return ObString(val_len_, v_.string_); }
  OB_INLINE ObString get_binary() const { return ObString(val_len_, v_.string_); }
  OB_INLINE ObString get_hex_string() const { return ObString(val_len_, v_.string_); }
  OB_INLINE ObString get_print_string(const int64_t max_len) const { return ObString(MIN(val_len_, max_len), v_.string_); }
  OB_INLINE ObString get_lob_print_string(const int64_t max_len) const {
    return ObString(MIN(v_.lob_locator_->payload_size_, max_len),
                        v_.lob_locator_->get_payload_ptr());
  }
  OB_INLINE ObString get_text_print_string(const int64_t max_len) const {
    int ret = OB_SUCCESS;
    ObString inrow_data;
    if (val_len_ == 0 || !has_lob_header()) {
      COMMON_LOG(DEBUG, "Lob: get string of null text obj", K(*this));
      inrow_data.assign_ptr(v_.string_, val_len_);
    } else {
      ObLobLocatorV2 loc(reinterpret_cast<char *>(v_.ptr_), val_len_, has_lob_header());
      if (OB_UNLIKELY(!loc.is_valid(false))) {
        inrow_data.assign_ptr(v_.string_, val_len_);
      } else if (!loc.has_inrow_data()) {
        inrow_data.assign_ptr("outrow", 6);
      } else if (OB_FAIL(loc.get_inrow_data(inrow_data))) {
        COMMON_LOG(WARN, "Lob: get inrow data failed in obobj", K(*this));
      } else {
        inrow_data.assign_ptr(inrow_data.ptr(), MIN(inrow_data.length(), max_len));
      }
    }
    return inrow_data;
  }

  OB_INLINE int get_json_print_data(ObString &json_data, char *buf, int64_t buf_len, int64_t &pos) const {
    int ret = OB_SUCCESS;
    json_data = get_string();
    ObLobCommon* lob_comm = reinterpret_cast<ObLobCommon*>(json_data.ptr());
    bool bret = has_lob_header() ||
                (json_data.length() >= sizeof(ObLobCommon) && lob_comm->is_valid());
    if (!bret) {
      // if it called in log params, like K(json), it maybe a json with disk lob header only
      // cannot judge here is a plain json data or json data with disk lob header
      COMMON_LOG(DEBUG, "Lob: get json data without mem lob header", K(*this));
    } else {
      ObLobLocatorV2 loc(reinterpret_cast<char *>(v_.ptr_), val_len_, bret);
      if (OB_UNLIKELY(!loc.is_valid(false))) {
        // do nothing, warn log inside
        COMMON_LOG(WARN, "Lob: invalid json lob", K(ret), K(json_data));
      } else if (!loc.has_inrow_data()) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", "'outrow json'"))) {
          COMMON_LOG(WARN, "Lob: fail to print \"\'outrow json\'\"", K(ret), K(buf_len), K(pos));
        }
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(loc.get_inrow_data(json_data))) {
        COMMON_LOG(WARN, "Lob: get inrow data failed in obobj", K(*this));
      }
    }
    return ret;
  }

  OB_INLINE int get_udt_print_data(ObString &udt_data, char *buf, int64_t buf_len, int64_t &pos, bool is_lob) const {
    int ret = OB_SUCCESS;
    udt_data = get_string();
    if (is_lob) {
      ObLobLocatorV2 loc(reinterpret_cast<char *>(v_.ptr_), val_len_, true);
      if (OB_UNLIKELY(!loc.is_valid(false))) {
        // do nothing, warn log inside
        COMMON_LOG(WARN, "Lob: invalid udt lob", K(ret), K(udt_data));
      } else if (!loc.is_inrow()) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", "'outrow udt'"))) {
          COMMON_LOG(WARN, "Lob: fail to print \"\'outrow json\'\"", K(ret), K(buf_len), K(pos));
        }
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(loc.get_inrow_data(udt_data))) {
        COMMON_LOG(WARN, "Lob: get inrow data failed in obobj", K(*this));
      }
    }
    return ret;
  }

  OB_INLINE bool get_bool() const { return (0 != v_.int64_); }
  inline int64_t get_ext() const;
  OB_INLINE int64_t get_unknown() const { return v_.unknown_; }
  OB_INLINE uint64_t get_bit() const { return v_.uint64_; }
  OB_INLINE uint64_t get_enum() const { return v_.uint64_; }
  OB_INLINE uint64_t get_set() const { return v_.uint64_; }
  OB_INLINE ObString get_set_inner() const { return ObString(val_len_, v_.string_); }
  OB_INLINE ObString get_enum_inner() const { return ObString(val_len_, v_.string_); }

  inline const number::ObNumber::Desc& get_number_desc() const { return nmb_desc_; }
  inline const uint32_t *get_number_digits() const { return v_.nmb_digits_; }

  inline const char* get_string_ptr() const { return v_.string_; }
  inline int32_t get_string_len() const { return val_len_; }
  inline const ObLobCommon *get_lob_value() const { return v_.lob_; }
  inline int get_lob_value(const ObLobCommon *&lob) const;

  inline int get_lob_locator(ObLobLocator *&lob_locator) const;
  inline const ObLobLocator* get_lob_locator() const { return v_.lob_locator_; }
  inline int get_lob_locatorv2(ObLobLocatorV2 &lob_locator) const;
  inline int get_lob_locatorv2(ObString &lob_locator) const;
  //TODO @shanting dangerous interface
  inline uint32_t get_lob_payload_size() const { return v_.lob_locator_->payload_size_; }
  inline const char *get_lob_payload_ptr() const
  {
    return v_.lob_locator_ == nullptr ? nullptr : v_.lob_locator_->get_payload_ptr();
  }

  inline ObOTimestampData::UnionTZCtx get_tz_desc() const { return time_ctx_; }
  inline ObOTimestampData get_otimestamp_value() const { return ObOTimestampData(v_.datetime_, time_ctx_); }
  inline int64_t get_otimestamp_store_size() const
  {
    return get_otimestamp_store_size(is_timestamp_tz());
  }
  inline int write_otimestamp(char *buf, const int64_t len, int64_t &pos) const;
  inline int read_otimestamp(const char *buf, const int64_t len);

  inline int64_t get_interval_store_size() const;
  inline int write_interval(char *buf) const;
  inline int read_interval(const char *buf);

  inline ObIntervalYMValue get_interval_ym() const { return ObIntervalYMValue(v_.nmonth_); }
  inline ObIntervalDSValue get_interval_ds() const { return ObIntervalDSValue(v_.nsecond_, interval_fractional_); }

  inline ObString get_nvarchar2() const { return ObString(val_len_, v_.string_); }
  inline ObString get_nchar() const { return ObString(val_len_, v_.string_); }

  inline ObURowIDData get_urowid() const
  {
    return ObURowIDData(val_len_, (const uint8_t *)v_.string_);
  }

  inline uint16_t get_udt_subschema_id() const { return meta_.get_subschema_id(); }
  inline uint8_t get_udt_flags() const { return meta_.get_udt_flags(); }
  inline int get_udt(ObString &udt_data) const;

  //@}

  //@{ test functions
  OB_INLINE bool is_valid_type() const { return meta_.is_valid(); }
  OB_INLINE bool is_invalid_type() const { return meta_.is_invalid(); }

  OB_INLINE bool is_null() const { return meta_.is_null(); }
  OB_INLINE bool is_null_oracle() const { return meta_.is_null()
      || (meta_.is_character_type() && (0 == get_string_len())); }
  OB_INLINE bool is_tinyint() const { return meta_.is_tinyint(); }
  OB_INLINE bool is_smallint() const { return meta_.is_smallint(); }
  OB_INLINE bool is_mediumint() const { return meta_.is_mediumint(); }
  OB_INLINE bool is_int32() const { return meta_.is_int32(); }
  OB_INLINE bool is_int() const { return meta_.is_int(); }
  OB_INLINE bool is_utinyint() const { return meta_.is_utinyint(); }
  OB_INLINE bool is_usmallint() const { return meta_.is_usmallint(); }
  OB_INLINE bool is_umediumint() const { return meta_.is_umediumint(); }
  OB_INLINE bool is_uint32() const { return meta_.is_uint32(); }
  OB_INLINE bool is_uint64() const { return meta_.is_uint64(); }
  OB_INLINE bool is_float() const { return meta_.is_float(); }
  OB_INLINE bool is_double() const { return meta_.is_double(); }
  OB_INLINE bool is_ufloat() const { return meta_.is_ufloat(); }
  OB_INLINE bool is_udouble() const { return meta_.is_udouble(); }
  OB_INLINE bool is_number() const { return meta_.is_number(); }
  OB_INLINE bool is_unumber() const { return meta_.is_unumber(); }
  OB_INLINE bool is_number_float() const { return meta_.is_number_float(); }
  OB_INLINE bool is_oracle_decimal() const { return meta_.is_oracle_decimal(); }
  OB_INLINE bool is_datetime() const { return meta_.is_datetime(); }
  OB_INLINE bool is_timestamp() const { return meta_.is_timestamp(); }
  OB_INLINE bool is_otimestamp_type() const { return meta_.is_otimestamp_type(); }
  OB_INLINE bool is_year() const { return meta_.is_year(); }
  OB_INLINE bool is_date() const { return meta_.is_date(); }
  OB_INLINE bool is_time() const { return meta_.is_time(); }
  OB_INLINE bool is_varchar() const { return meta_.is_varchar(); }
  OB_INLINE bool is_char() const { return meta_.is_char(); }
  OB_INLINE bool is_varbinary() const { return meta_.is_varbinary(); }
  OB_INLINE bool is_raw() const { return meta_.is_raw(); }
  OB_INLINE bool is_binary() const { return meta_.is_binary(); }
  OB_INLINE bool is_hex_string() const { return meta_.is_hex_string(); }
  OB_INLINE bool is_ext() const { return meta_.is_ext(); }
  OB_INLINE bool is_unknown() const { return meta_.is_unknown(); }
  OB_INLINE bool is_bit() const { return meta_.is_bit(); }
  OB_INLINE bool is_enum() const { return meta_.is_enum(); }
  OB_INLINE bool is_set() const { return meta_.is_set(); }
  OB_INLINE bool is_text() const { return meta_.is_text(); }
  OB_INLINE bool is_clob() const { return meta_.is_clob(); }
  //OB_INLINE bool is_oracle_clob() const { return meta_.is_oracle_clob(); }
  OB_INLINE bool is_blob() const { return meta_.is_blob(); }
  //OB_INLINE bool is_oracle_blob() const { return meta_.is_oracle_blob(); }
  OB_INLINE bool is_lob_storage() const { return meta_.is_lob_storage(); }
  OB_INLINE bool is_lob() const { return meta_.is_lob(); }
  OB_INLINE bool is_outrow_lob() const;
  OB_INLINE bool is_delta_tmp_lob() const;
  OB_INLINE bool is_persist_lob() const;
  OB_INLINE bool is_inrow() const { return meta_.is_inrow(); }
  OB_INLINE bool is_outrow() const { return meta_.is_outrow(); }
  OB_INLINE bool has_lob_header() const { return meta_.has_lob_header(); }

  OB_INLINE bool is_json() const { return meta_.is_json(); }
  OB_INLINE bool is_json_inrow() const { return meta_.is_json_inrow(); }
  OB_INLINE bool is_json_outrow() const { return meta_.is_json_outrow(); }

  OB_INLINE bool is_geometry() const { return meta_.is_geometry(); }
  OB_INLINE bool is_geometry_inrow() const { return meta_.is_geometry_inrow(); }
  OB_INLINE bool is_geometry_outrow() const { return meta_.is_geometry_outrow(); }
  OB_INLINE bool is_user_defined_sql_type() const { return meta_.is_user_defined_sql_type(); }
  OB_INLINE bool is_xml_sql_type() const { return meta_.is_user_defined_sql_type(); }

  OB_INLINE bool is_timestamp_tz() const { return meta_.is_timestamp_tz(); }
  OB_INLINE bool is_timestamp_ltz() const { return meta_.is_timestamp_ltz(); }
  OB_INLINE bool is_timestamp_nano() const { return meta_.is_timestamp_nano(); }
  OB_INLINE bool is_interval_ym() const { return meta_.is_interval_ym(); }
  OB_INLINE bool is_interval_ds() const { return meta_.is_interval_ds(); }

  // including int only
  OB_INLINE bool is_unsigned_integer() const { return meta_.is_unsigned_integer(); }
  OB_INLINE bool is_signed_integer() const { return meta_.is_signed_integer(); }
  // including int, number,float,double
  OB_INLINE bool is_unsigned() const { return meta_.is_unsigned(); }
  OB_INLINE bool is_integer_type() const { return meta_.is_integer_type(); }
  OB_INLINE bool is_numeric_type() const { return meta_.is_numeric_type(); }
  OB_INLINE bool is_string_type() const { return meta_.is_string_type(); }
  OB_INLINE bool is_temporal_type() const { return meta_.is_temporal_type(); }
  OB_INLINE bool is_varchar_or_char() const { return meta_.is_varchar_or_char(); }
  OB_INLINE bool is_varbinary_or_binary() const { return meta_.is_varbinary_or_binary(); }
  OB_INLINE bool is_nvarchar2() const { return meta_.is_nvarchar2(); }
  OB_INLINE bool is_nchar() const { return meta_.is_nchar(); }
  OB_INLINE bool is_nstring() const { return meta_.is_nstring(); }
  OB_INLINE bool is_fixed_len_char_type() const { return meta_.is_fixed_len_char_type(); }
  OB_INLINE bool is_varying_len_char_type() const { return meta_.is_varying_len_char_type(); }
  OB_INLINE bool is_character_type() const { return meta_.is_character_type(); }
  OB_INLINE bool is_collation_free_compatible() const { return meta_.is_collation_free_compatible(); }

  OB_INLINE bool is_urowid() const { return meta_.is_urowid(); }
  OB_INLINE bool is_blob_locator() const { return meta_.is_blob_locator(); }
  OB_INLINE bool is_clob_locator() const { return meta_.is_clob_locator(); }
  OB_INLINE bool is_lob_locator() const { return meta_.is_lob_locator(); }
  OB_INLINE bool is_fixed_double() const { return ob_is_double_type(meta_.get_type()) &&
    SCALE_UNKNOWN_YET < meta_.get_scale() && OB_MAX_DOUBLE_FLOAT_SCALE >= meta_.get_scale(); }
  OB_INLINE bool is_string_or_lob_locator_type() const {
    return meta_.is_string_or_lob_locator_type();
  }
  OB_INLINE bool is_pl_extend() const
  {
    return is_ext()
        && !ObObj::is_ext_val(get_ext())
        && meta_.get_extend_type() > 0
        && meta_.get_extend_type() < T_EXT_SQL_ARRAY;
  }
  OB_INLINE bool is_ext_sql_array() const { return meta_.is_ext_sql_array(); }
  OB_INLINE bool is_overflow_integer(ObObjType type) const
  {
#define CHECK_VAL_OVERFLOW(min, max)                            \
    if (is_signed_integer()) {                                  \
      bret = !(get_int() >= min && get_int() <= max);           \
    } else if (is_unsigned_integer()) {                         \
      bret = !(get_uint64() <= max);                            \
    } else {                                                    \
      bret = true;                                              \
    }
    bool bret = false;
    switch (type) {
    case ObTinyIntType: {
      CHECK_VAL_OVERFLOW(INT8_MIN, INT8_MAX);
      break;
    }
    case ObSmallIntType: {
      CHECK_VAL_OVERFLOW(INT16_MIN, INT16_MAX);
      break;
    }
    case ObMediumIntType: {
      CHECK_VAL_OVERFLOW(-8388608, 8388607);
      break;
    }
    case ObInt32Type: {
      CHECK_VAL_OVERFLOW(INT32_MIN, INT32_MAX);
      break;
    }
    case ObIntType: {
      CHECK_VAL_OVERFLOW(INT64_MIN, INT64_MAX);
      break;
    }
    case ObUTinyIntType: {
      CHECK_VAL_OVERFLOW(0, UINT8_MAX);
      break;
    }
    case ObUSmallIntType: {
      CHECK_VAL_OVERFLOW(0, UINT16_MAX);
      break;
    }
    case ObUMediumIntType: {
      CHECK_VAL_OVERFLOW(0, 16777215);
      break;
    }
    case ObUInt32Type: {
      CHECK_VAL_OVERFLOW(0, UINT32_MAX);
      break;
    }
    case ObUInt64Type: {
      CHECK_VAL_OVERFLOW(0, UINT64_MAX);
      break;
    }
    default: {
      bret = true;
      break;
    }
    }
#undef CHECK_VAL_OVERFLOW
    return bret;
  }

  inline bool is_min_value() const;
  inline bool is_max_value() const;
  inline bool is_nop_value() const;
  inline bool is_true() const;
  inline bool is_false() const;

  bool is_zero() const;
  //@}

  /// apply mutation to this obj
  int apply(const ObObj &mutation);

  //@{ comparison
  //
  // ATTENTION:
  //
  // When < > <= >= == != compare is_equal is called,
  // that_obj MUST have same type with this obj (*this)
  // or can_compare.
  // For Sql, to get diff type comparing result(varchar '123' compare with int 123),
  // call static function ObExprEqual::calc or other ObExprXX::calc.
  bool operator<(const ObObj &that_obj) const;
  bool operator>(const ObObj &that_obj) const;
  bool operator<=(const ObObj &that_obj) const;
  bool operator>=(const ObObj &that_obj) const;
  bool operator==(const ObObj &that_obj) const;
  bool operator!=(const ObObj &that_obj) const;
  bool can_compare(const ObObj &other) const;
  inline bool strict_equal(const ObObj &other) const;
  int check_collation_free_and_compare(const ObObj &other, int &cmp) const;
  int check_collation_free_and_compare(const ObObj &other) const;
  int compare(const ObObj &other, int &cmp) const;
  int compare(const ObObj &other) const;
  int compare(const ObObj &other, ObCollationType cs_type, int &cmp) const;
  int compare(const ObObj &other, ObCollationType cs_type) const;
  int compare(const ObObj &other, common::ObCompareCtx &cmp_ctx, int &cmp) const;
  int compare(const ObObj &other, common::ObCompareCtx &cmp_ctx) const;
  int compare(const ObObj &other, ObCollationType cs_type, const ObCmpNullPos null_pos) const;
  int equal(const ObObj &other, bool &is_equal) const;
  bool is_equal(const ObObj &other) const;
  int equal(const ObObj &other, ObCollationType cs_type, bool &is_equal) const;
  bool is_equal(const ObObj &other, ObCollationType cs_type) const;
  //@}

  //@{ print utilities
  /// print as JSON style
  int64_t to_string(char *buffer, const int64_t length, const ObObjPrintParams &params = ObObjPrintParams()) const;
  /// print as SQL literal style, e.g. used to show column default value
  int print_sql_literal(char *buffer, int64_t length, int64_t &pos, const ObObjPrintParams &params = ObObjPrintParams()) const;
  /// print as SQL literal sytle and the length can be auto extended, e.g. used to show column default value
  int print_sql_literal(char *&buffer, int64_t &length,
                        int64_t &pos, ObIAllocator &alloc,
                        const ObObjPrintParams &params = ObObjPrintParams()) const;
  /// print as SQL VARCHAR literal
  int print_varchar_literal(char *buffer, int64_t length, int64_t &pos, const ObObjPrintParams &params = ObObjPrintParams()) const;
  //used for enum and set
  int print_varchar_literal(const ObIArray<ObString> &type_infos, char *buffer, int64_t length, int64_t &pos) const;
  /// print as plain string
  int print_plain_str_literal(char *buffer, int64_t length, int64_t &pos, const ObObjPrintParams &params = ObObjPrintParams()) const;
  //used for enum and set
  int print_plain_str_literal(const ObIArray<ObString> &type_infos, char *buffer, int64_t length, int64_t &pos) const;

  void print_range_value(char *buffer, int64_t length, int64_t &pos) const;
  void print_str_with_repeat(char *buffer, int64_t length, int64_t &pos) const;

  //print_smart and print_format are for log_tool use
  int print_smart(char *buffer, int64_t length, int64_t &pos) const;
  int print_format(char *buffer, int64_t length, int64_t &pos) const;
  /// dump into log
  void dump(const int32_t log_level = OB_LOG_LEVEL_DEBUG) const;
  //@}

  //@{  deep copy
  bool need_deep_copy()const;
  OB_INLINE int64_t get_deep_copy_size() const;
  int deep_copy(const ObObj &src, char *buf, const int64_t size, int64_t &pos);
  void* get_deep_copy_obj_ptr();
  void set_data_ptr(void *data_ptr);
  const void *get_data_ptr() const;
  //return byte length
  int64_t get_data_length() const;

  const void *get_tight_data_ptr() const;
  int64_t get_tight_data_len() const;

  template <typename Allocator>
  int to_collation_free_obj(ObObj &dst, bool &is_valid_collation_free, Allocator &allocator);
  //@}

  //@{ checksum
  // CRC64
  int64_t checksum(const int64_t current) const;
  int64_t checksum_v2(const int64_t current) const;
  void checksum(ObBatchChecksum &bc) const;
  // mysql hash for string, murmurhash for others
  int hash(uint64_t &res) const;
  int hash(uint64_t &res, uint64_t seed) const;
  int hash_v1(uint64_t &res, uint64_t seed = 0) const;  // for compatible purpose, use hash() instead
  int hash_murmur(uint64_t &res, uint64_t seed = 0) const;
  // wyhash for all types
  int hash_wy(uint64_t &res, uint64_t seed = 0) const;
  // xx hash
  int hash_xx(uint64_t &res, uint64_t seed = 0) const;
  // mysql hash
  uint64_t varchar_hash(ObCollationType cs_type, uint64_t seed = 0) const;
  uint64_t varchar_murmur_hash(ObCollationType cs_type, uint64_t seed = 0) const;
  uint64_t varchar_wy_hash(ObCollationType cs_type, uint64_t seed = 0) const;
  uint64_t varchar_xx_hash(ObCollationType cs_type, uint64_t seed = 0) const;
  bool check_collation_integrity() const;
  //@}

  NEED_SERIALIZE_AND_DESERIALIZE;

  static uint32_t meta_offset_bits() { return offsetof(ObObj, meta_) * 8; }
  static uint32_t val_len_offset_bits() { return offsetof(ObObj, val_len_) * 8; }
  static uint32_t nmb_desc_offset_bits() { return offsetof(ObObj, nmb_desc_) * 8; }
  static uint32_t v_offset_bits() { return offsetof(ObObj, v_) * 8; }
  int get_char_length(const ObAccuracy accuracy, int32_t &char_len, bool is_oracle_mode) const;
  int convert_string_value_charset(ObCharsetType charset_type, ObIAllocator &allocator);
private:
  friend class tests::common::ObjTest;
  friend class ObCompactCellWriter;
  friend class ObCompactCellIterator;

public:
  ObObjMeta meta_;  // sizeof = 4
  union
  {
    int32_t val_len_;
    int32_t interval_fractional_; //values for intervalds type
    number::ObNumber::Desc nmb_desc_;
    ObOTimestampData::UnionTZCtx time_ctx_;
    ObObjMeta null_meta_;
  };  // sizeof = 4
  ObObjValue v_;  // sizeof = 8
};

//为每个hash函数创建一个类，其中都包含static方法，方法名为hash，参数类型为const void*, uint64_t, uint64_t.
//is_varchar_hash是为了兼容原来实现murmurhash处理varchar类型时，底层并不是使用murmurhash
struct ObjHashBase
{
  static const bool is_varchar_hash = true;
};

// default hash method: same with ObObj::hash())
//  murmurhash for non string types.
//  mysql string hash for string types.
struct ObDefaultHash : public ObjHashBase
{
  static const bool is_varchar_hash = false;
  static uint64_t hash(const void *data, uint64_t len, uint64_t seed)
  {
    return murmurhash64A(data, static_cast<int32_t>(len), seed);
  }
};

struct ObMurmurHash : public ObjHashBase
{
  OB_INLINE static uint64_t hash(const void *data, uint64_t len, uint64_t seed)
  {
    return murmurhash64A(data, static_cast<int32_t>(len), seed);
  }
};

struct ObWyHash : public ObjHashBase
{
  static uint64_t hash(const void *data, uint64_t len, uint64_t seed)
  {
    return wyhash(data, len, seed);
  }
};

struct ObXxHash : public ObjHashBase
{
  static uint64_t hash(const void *data, uint64_t len, uint64_t seed)
  {
    return XXH64(data, static_cast<size_t>(len), seed);
  }
};

template <ObObjType type, typename T, typename P>
struct ObjHashCalculator
{
  static int calc_hash_value(const P &param, const uint64_t hash, uint64_t &res) {
    UNUSED(param);
    UNUSED(hash);
    res = 0;
    return OB_SUCCESS;
  }
};

inline ObObj::ObObj()
{
  reset();
}

inline ObObj::ObObj(bool val)
{
  set_bool(val);
  val_len_ = 0;
}

inline ObObj::ObObj(int32_t val)
{
  set_int32(val);
  val_len_ = 0;
}

inline ObObj::ObObj(int64_t val)
{
  set_int(val);
  val_len_ = 0;
}

inline ObObj::ObObj(ObObjType type)
{
  meta_.set_type(type);
  val_len_ = 0;
}

inline ObObj::ObObj(const ObObj &other)
{
  *this = other;
}

inline void ObObj::reset()
{
  meta_.reset();
  val_len_ = 0;
  v_.int64_ = 0;
}

inline ObObj ObObj::make_min_obj()
{
  ObObj obj;
  obj.set_min_value();
  return obj;
}

inline ObObj ObObj::make_max_obj()
{
  ObObj obj;
  obj.set_max_value();
  return obj;
}

inline ObObj ObObj::make_nop_obj()
{
  ObObj obj;
  obj.set_nop_value();
  return obj;
}

inline void ObObj::set_int(const ObObjType type, const int64_t value)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_NUMERIC);
  v_.int64_ = value;
}

inline void ObObj::set_tinyint(const int8_t value)
{
  meta_.set_tinyint();
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_tinyint_value(const int8_t value)
{
//  meta_.set_tinyint();
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_smallint(const int16_t value)
{
  meta_.set_smallint();
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_smallint_value(const int16_t value)
{
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_mediumint(const int32_t value)
{
  meta_.set_mediumint();
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_mediumint_value(const int32_t value)
{
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_int32(const int32_t value)
{
  meta_.set_int32();
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_int32_value(const int32_t value)
{
//  meta_.set_int32();
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_int(const int64_t value)
{
  meta_.set_int();
  v_.int64_ = value;
}

inline void ObObj::set_int_value(const int64_t value)
{
  v_.int64_ = value;
}

inline void ObObj::set_uint(const ObObjType type, const uint64_t value)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_NUMERIC);
  v_.uint64_ = value;
}

inline void ObObj::set_utinyint(const uint8_t value)
{
  meta_.set_utinyint();
  v_.uint64_ = static_cast<uint64_t>(value);
}

inline void ObObj::set_utinyint_value(const uint8_t value)
{
  v_.uint64_ = static_cast<uint64_t>(value);
}

inline void ObObj::set_usmallint(const uint16_t value)
{
  meta_.set_usmallint();
  v_.uint64_ = static_cast<uint64_t>(value);
}

inline void ObObj::set_usmallint_value(const uint16_t value)
{
  v_.uint64_ = static_cast<uint64_t>(value);
}

inline void ObObj::set_umediumint(const uint32_t value)
{
  meta_.set_umediumint();
  v_.uint64_ = static_cast<uint64_t>(value);
}

inline void ObObj::set_umediumint_value(const uint32_t value)
{
  v_.uint64_ = static_cast<uint64_t>(value);
}

inline void ObObj::set_uint32(const uint32_t value)
{
  meta_.set_uint32();
  v_.uint64_ = static_cast<uint64_t>(value);
}

inline void ObObj::set_uint32_value(const uint32_t value)
{
  v_.uint64_ = static_cast<uint64_t>(value);
}

inline void ObObj::set_uint64(const uint64_t value)
{
  meta_.set_uint64();
  v_.uint64_ = value;
}

inline void ObObj::set_uint64_value(const uint64_t value)
{
  v_.uint64_ = value;
}

inline void ObObj::set_float(const ObObjType type, const float value)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_NUMERIC);
  v_.uint64_ = 0;
  v_.float_ = value;
}

inline void ObObj::set_float(const float value)
{
  meta_.set_float();
  v_.uint64_ = 0;
  v_.float_ = value;
}

inline void ObObj::set_float_value(const float value)
{
//  meta_.set_float();
  v_.uint64_ = 0;
  v_.float_ = value;
}

inline void ObObj::set_ufloat(const float value)
{
  meta_.set_ufloat();
  v_.uint64_ = 0;
  v_.float_ = value;
}

inline void ObObj::set_ufloat_value(const float value)
{
  v_.uint64_ = 0;
  v_.float_ = value;
}

inline void ObObj::set_double(const ObObjType type, const double value)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_NUMERIC);
  v_.double_ = value;
}

inline void ObObj::set_double(const double value)
{
  meta_.set_double();
  v_.double_ = value;
}

inline void ObObj::set_double_value(const double value)
{
  v_.double_ = value;
}

inline void ObObj::set_udouble(const double value)
{
  meta_.set_udouble();
  v_.double_ = value;
}

inline void ObObj::set_udouble_value(const double value)
{
  v_.double_ = value;
}

inline void ObObj::set_number(const ObObjType type, const number::ObNumber &num)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_NUMERIC);
  nmb_desc_.desc_ = num.get_desc_value();
  v_.nmb_digits_ = num.get_digits();
}

inline void ObObj::set_number(const ObObjType type, const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_NUMERIC);
  nmb_desc_ = nmb_desc;
  v_.nmb_digits_ = nmb_digits;
}

inline void ObObj::set_number(const number::ObNumber &num)
{
  meta_.set_number();
  nmb_desc_.desc_ = num.get_desc_value();
  v_.nmb_digits_ = num.get_digits();
}

inline void ObObj::set_number_value(const number::ObNumber &num)
{
  nmb_desc_.desc_ = num.get_desc_value();
  v_.nmb_digits_ = num.get_digits();
}

inline void ObObj::set_number_value(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits)
{
  nmb_desc_ = nmb_desc;
  v_.nmb_digits_ = nmb_digits;
}

inline void ObObj::set_number(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits)
{
  meta_.set_number();
  nmb_desc_ = nmb_desc;
  v_.nmb_digits_ = nmb_digits;
}

inline void ObObj::set_unumber(const number::ObNumber &num)
{
  meta_.set_unumber();
  nmb_desc_.desc_ = num.get_desc_value();
  v_.nmb_digits_ = num.get_digits();
}

inline void ObObj::set_unumber(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits)
{
  meta_.set_unumber();
  nmb_desc_ = nmb_desc;
  v_.nmb_digits_ = nmb_digits;
}

inline void ObObj::set_unumber_value(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits)
{
  nmb_desc_ = nmb_desc;
  v_.nmb_digits_ = nmb_digits;
}

inline void ObObj::set_number_float(const number::ObNumber &num)
{
  meta_.set_number_float();
  nmb_desc_.desc_ = num.get_desc_value();
  v_.nmb_digits_ = num.get_digits();
}

inline void ObObj::set_number_float(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits)
{
  meta_.set_number_float();
  nmb_desc_ = nmb_desc;
  v_.nmb_digits_ = nmb_digits;
}

inline void ObObj::set_number_float_value(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits)
{
  nmb_desc_ = nmb_desc;
  v_.nmb_digits_ = nmb_digits;
}

inline void ObObj::set_datetime(const ObObjType type, const int64_t value)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_NUMERIC);
  v_.datetime_ = value;
}

inline void ObObj::set_datetime(const int64_t value)
{
  meta_.set_datetime();
  v_.datetime_ = value;
}
inline void ObObj::set_datetime_value(const int64_t value)
{

  v_.datetime_ = value;
}

inline void ObObj::set_timestamp(const int64_t value)
{
  meta_.set_timestamp();
  v_.datetime_ = value;
}

inline void ObObj::set_timestamp_value(const int64_t value)
{
  v_.datetime_ = value;
}

inline void ObObj::set_date(const int32_t value)
{
  meta_.set_date();
  v_.uint64_ = 0;
  v_.date_ = value;
}

inline void ObObj::set_time(const int64_t value)
{
  meta_.set_time();
  v_.time_ = value;
}
inline void ObObj::set_date_value(const int32_t value)
{
  v_.uint64_ = 0;
  v_.date_ = value;
}

inline void ObObj::set_time_value(const int64_t value)
{
  v_.time_ = value;
}

inline void ObObj::set_year(const uint8_t value)
{
  meta_.set_year();
  v_.uint64_ = 0;
  v_.year_ = value;
}

inline void ObObj::set_year_value(const uint8_t value)
{
  v_.uint64_ = 0;
  v_.year_ = value;
}

inline void ObObj::set_common_value(const ObString &value)
{
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_string(const ObObjType type, const char *ptr, const ObString::obstr_size_t size)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_string(const ObObjType type, const ObString &value)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_varchar(const ObString &value)
{
  meta_.set_varchar();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_varchar(const char *ptr, const ObString::obstr_size_t size)
{
  meta_.set_varchar();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_varchar_value(const char *ptr, const ObString::obstr_size_t size)
{
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_varchar(const char *cstr)
{
  meta_.set_varchar();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = cstr;
  val_len_ = static_cast<int32_t>(strlen(cstr));
}

inline void ObObj::set_char(const ObString &value)
{
  meta_.set_char();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_char_value(const char *ptr, const ObString::obstr_size_t size)
{
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_varbinary(const ObString &value)
{
  meta_.set_varchar();
  meta_.set_collation_type(CS_TYPE_BINARY);
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_binary(const ObString &value)
{
  meta_.set_char();
  meta_.set_collation_type(CS_TYPE_BINARY);
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_raw(const ObString &value)
{
  meta_.set_raw();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_raw(const char *ptr, const ObString::obstr_size_t size)
{
  meta_.set_raw();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_raw_value(const char *ptr, const ObString::obstr_size_t size)
{
  meta_.set_collation_type(CS_TYPE_BINARY);
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_hex_string(const ObString &value)
{
  meta_.set_hex_string();
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_hex_string_value(const ObString &value)
{
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_hex_string_value(const char *ptr, const ObString::obstr_size_t size)
{
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_enum(const uint64_t value)
{
  meta_.set_enum();
  v_.uint64_ = value;
}

inline void ObObj::set_set(const uint64_t value)
{
  meta_.set_set();
  v_.uint64_ = value;
}

inline void ObObj::set_enum_value(const uint64_t value)
{
  v_.uint64_ = value;
}

inline void ObObj::set_set_value(const uint64_t value)
{
  v_.uint64_ = value;
}

inline void ObObj::set_enum_inner(const ObString &value)
{
  meta_.set_enum_inner();
  //meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_enum_inner(const char *ptr, const ObString::obstr_size_t size)
{
  meta_.set_enum_inner();
  //meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_set_inner(const ObString &value)
{
  meta_.set_set_inner();
  //meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_set_inner(const char *ptr, const ObString::obstr_size_t size)
{
  meta_.set_set_inner();
  //meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = size;
}

// used to set lob value without lob header
// Notice: use set_null for null input!
inline void ObObj::set_lob_value(const ObObjType type, const ObLobCommon *value, const int32_t length)
{
  meta_.set_type(type);
  meta_.set_inrow();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.lob_ = value;
  val_len_ = length;
}

inline void ObObj::set_lob_value(const ObObjType type, const char *ptr, const int32_t length)
{
  meta_.set_type(type);
  meta_.set_inrow();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = length;
}

inline void ObObj::set_json_value(const ObObjType type, const ObLobCommon *value, const int32_t length)
{
  set_lob_value(type, value, length);
  meta_.set_collation_type(CS_TYPE_UTF8MB4_BIN); // for oracle it is decided by sys collation.
}

inline void ObObj::set_json_value(const ObObjType type, const char *ptr, const int32_t length)
{
  set_lob_value(type, ptr, length);
  meta_.set_collation_type(CS_TYPE_UTF8MB4_BIN); // for oracle it is decided by sys collation.
}

inline void ObObj::set_geometry_value(const ObObjType type, const ObLobCommon *value, const int32_t length)
{
  set_lob_value(type, value, length);
  meta_.set_collation_type(CS_TYPE_BINARY);
}

inline void ObObj::set_geometry_value(const ObObjType type, const char *ptr, const int32_t length)
{
  set_lob_value(type, ptr, length);
  meta_.set_collation_type(CS_TYPE_BINARY);
}

inline void ObObj::set_lob_locator(const ObLobLocator &value)
{
  meta_.set_type(ObLobType);
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.lob_locator_ = &value;
  val_len_ = static_cast<int32_t>(value.get_total_size());
}

inline void ObObj::set_lob_locator(const ObObjType type, const ObLobLocator &value)
{
  UNUSED(type);
  meta_.set_type(ObLobType);
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.lob_locator_ = &value;
  val_len_ = static_cast<int32_t>(value.get_total_size());
}

inline void ObObj::set_otimestamp_value(const ObObjType type, const ObOTimestampData &value)
{
  meta_.set_otimestamp_type(type);
  time_ctx_ = value.time_ctx_;
  v_.datetime_ = value.time_us_;
}

inline void ObObj::set_otimestamp_value(const ObObjType type, const int64_t time_us, const uint32_t time_ctx_desc)
{
  meta_.set_otimestamp_type(type);
  time_ctx_.desc_ = time_ctx_desc;
  v_.datetime_ = time_us;
}

inline void ObObj::set_otimestamp_value(const ObObjType type, const int64_t time_us, const uint16_t time_desc)
{
  meta_.set_otimestamp_type(type);
  time_ctx_.tz_desc_ = 0;
  time_ctx_.time_desc_ = time_desc;
  v_.datetime_ = time_us;
}

inline void ObObj::set_otimestamp_null(const ObObjType type)
{
  meta_.set_otimestamp_type(type);
  time_ctx_.tz_desc_ = 0;
  time_ctx_.time_desc_ = 0;
  time_ctx_.is_null_ = 1;
}

inline void ObObj::set_interval_ym(const ObIntervalYMValue &value)
{
  meta_.set_interval_ym();
  v_.nmonth_ = value.nmonth_;
  interval_fractional_ = 0;
}

inline void ObObj::set_interval_ds(const ObIntervalDSValue &value)
{
  meta_.set_interval_ds();
  v_.nsecond_ = value.nsecond_;
  interval_fractional_ = value.fractional_second_;
}

inline void ObObj::set_nvarchar2(const ObString &value)
{
  meta_.set_nvarchar2();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_nvarchar2_value(const char *ptr, const ObString::obstr_size_t size)
{
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = size;
}


inline void ObObj::set_nchar(const ObString &value)
{
  meta_.set_nchar();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_nchar_value(const char *ptr, const ObString::obstr_size_t size)
{
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_null()
{
  meta_.set_null();
}

inline void ObObj::set_bool(const bool value)
{
  meta_.set_tinyint();
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_ext(const int64_t value)
{
  meta_.set_ext();
  val_len_ = 0;
  v_.ext_ = value;
}

inline void ObObj::set_extend(const int64_t value, uint8 extend_type, int32_t size)
{
  set_ext(value);
  meta_.set_extend_type(extend_type);
  set_val_len(size);
}

inline void ObObj::set_unknown(const int64_t value)
{
  meta_.set_unknown();
  v_.unknown_ = value;
}

inline void ObObj::set_bit(const uint64_t value)
{
  meta_.set_bit();
  v_.uint64_ = value;
}

inline void ObObj::set_bit_value(const uint64_t value)
{
  v_.uint64_ = value;
}

inline void ObObj::set_min_value()
{
  set_ext(MIN_OBJECT_VALUE);
}

inline void ObObj::set_max_value()
{
  set_ext(MAX_OBJECT_VALUE);
}

inline void ObObj::set_nop_value()
{
  set_ext(ObActionFlag::OP_NOP);
}

inline void ObObj::set_lob(const char* ptr, const int32_t size, const ObLobScale &lob_scale)
{
  meta_.set_scale(lob_scale.get_scale());
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_sql_udt(const char* ptr, int32_t size, uint16_t subschema_id, uint8_t udt_flags)
{
  meta_.set_sql_udt(subschema_id);
  meta_.set_udt_flags(udt_flags);
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_udt_value(const char* ptr, int32_t size)
{
  v_.string_ = ptr;
  val_len_ = size;
}

inline bool ObObj::is_min_value() const
{
  return meta_.get_type() == ObExtendType && v_.ext_ == MIN_OBJECT_VALUE;
}

inline bool ObObj::is_max_value() const
{
  return meta_.get_type() == ObExtendType && v_.ext_ == MAX_OBJECT_VALUE;
}

inline bool ObObj::is_nop_value() const
{
  return meta_.get_type() == ObExtendType && v_.ext_ == ObActionFlag::OP_NOP;
}

inline bool ObObj::is_true() const
{
  return ob_is_int_tc(meta_.get_type()) && 0 != v_.int64_;
}

inline bool ObObj::is_false() const
{
  return ob_is_int_tc(meta_.get_type()) && 0 == v_.int64_;
}

inline bool ObObj::need_deep_copy()const
{
  return (((ob_is_string_type(meta_.get_type())
            || ob_is_lob_locator(meta_.get_type())
            || ob_is_json(meta_.get_type())
            || ob_is_geometry(meta_.get_type())
            || ob_is_raw(meta_.get_type())
            || ob_is_user_defined_sql_type(meta_.get_type())
            || ob_is_rowid_tc(meta_.get_type())) && 0 != val_len_ && NULL != get_string_ptr())
            || (ob_is_number_tc(meta_.get_type())
             && 0 != nmb_desc_.len_ && NULL != get_number_digits()));
}

inline int64_t ObObj::get_ext() const
{
  int64_t res = 0;
  if (ObExtendType == meta_.get_type()) {
    res = v_.ext_;
  }
  return res;
}

inline void ObObj::set_val_len(const int32_t val_len)
{
  val_len_ = val_len;
}

inline void ObObj::set_null_meta(const ObObjMeta meta)
{
  null_meta_ = meta;
}

////////////////////////////////////////////////////////////////
inline int ObObj::get_tinyint(int8_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_tinyint()) {
    v = static_cast<int8_t>(v_.int64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_smallint(int16_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_smallint()) {
    v = static_cast<int16_t>(v_.int64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_mediumint(int32_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_mediumint()) {
    v = static_cast<int32_t>(v_.int64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_int32(int32_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_int32()) {
    v = static_cast<int32_t>(v_.int64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_int(int64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_int()) {
    v = v_.int64_;
    ret = OB_SUCCESS;
  }
  return ret;
}


inline int ObObj::get_utinyint(uint8_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_utinyint()) {
    v = static_cast<uint8_t>(v_.uint64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_usmallint(uint16_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_usmallint()) {
    v = static_cast<uint16_t>(v_.uint64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_umediumint(uint32_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_umediumint()) {
    v = static_cast<uint32_t>(v_.uint64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_uint32(uint32_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_uint32()) {
    v = static_cast<uint32_t>(v_.uint64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_uint64(uint64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_uint64()) {
    v = v_.uint64_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_float(float &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_float()) {
    v = v_.float_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_double(double &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_double()) {
    v = v_.double_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_ufloat(float &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_ufloat()) {
    v = v_.float_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_udouble(double &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_udouble()) {
    v = v_.double_;
    ret = OB_SUCCESS;
  }
  return ret;
}


inline int ObObj::get_number(number::ObNumber &num) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_number()) {
    num.assign(nmb_desc_.desc_, v_.nmb_digits_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_unumber(number::ObNumber &num) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_unumber()) {
    num.assign(nmb_desc_.desc_, v_.nmb_digits_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_number_float(number::ObNumber &num) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_number_float()) {
    num.assign(nmb_desc_.desc_, v_.nmb_digits_);
    ret = OB_SUCCESS;
  }
  return ret;
}


inline int ObObj::get_datetime(int64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_datetime()) {
    v = v_.datetime_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_timestamp(int64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_timestamp()) {
    v = v_.datetime_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_date(int32_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_date()) {
    v = v_.date_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_time(int64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_time()) {
    v = v_.time_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_year(uint8_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_year()) {
    v = v_.year_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_string(ObString &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_string_or_lob_locator_type() || is_lob_storage()) {
    if (ObLobType == meta_.get_type()) {
      if (OB_ISNULL(v_.lob_locator_)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "Unexpected null lob locator", K(*this));
      } else if (OB_FAIL(v_.lob_locator_->get_payload(v))) {
        OB_LOG(WARN, "Failed to get payload from lob locator", K(ret), KPC(v_.lob_locator_));
      }
    } else if (is_lob_storage()) {
      if (val_len_ == 0) {
        OB_LOG(DEBUG, "Lob: get string of null text obj", K(*this));
        v.assign_ptr(v_.string_, val_len_);
      } else {
        ObLobLocatorV2 loc(reinterpret_cast<char *>(v_.ptr_), val_len_, has_lob_header());
        if (OB_UNLIKELY(!loc.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "Unexpected invalid lob locator", K(*this), K(loc));
        } else if (OB_FAIL(loc.get_inrow_data(v))) {
          OB_LOG(WARN, "Lob: get inrow data failed", K(*this), K(loc));
        }
      }
    } else {
      v.assign_ptr(v_.string_, val_len_);
    }
    ret = OB_SUCCESS;
  } else if (meta_.is_null()) {
    v.assign_ptr(NULL, 0);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline bool ObObj::is_outrow_lob() const
{
  bool bret = false;
  if (is_lob_storage()) {
    if (val_len_ == 0) {
    } else if (!has_lob_header()) {
    } else {
      ObLobLocatorV2 loc(reinterpret_cast<char *>(v_.ptr_), val_len_, has_lob_header());
      if (loc.is_valid()) {
        bret = !loc.has_inrow_data();
      }
    }
  }
  return bret;
}

inline bool ObObj::is_delta_tmp_lob() const
{
  bool bret = false;
  if (is_lob_storage()) {
    if (val_len_ == 0) {
    } else if (!has_lob_header()) {
    } else {
      ObLobLocatorV2 loc(reinterpret_cast<char *>(v_.ptr_), val_len_, has_lob_header());
      if (loc.is_valid()) {
        bret = loc.is_delta_temp_lob();
      }
    }
  }
  return bret;
}

inline bool ObObj::is_persist_lob() const
{
  bool bret = false;
  if (ob_is_text_tc(meta_.get_type()) || ob_is_json_tc(meta_.get_type())) {
    if (val_len_ == 0) {
    } else if (!has_lob_header()) {
    } else {
      ObLobLocatorV2 loc(reinterpret_cast<char *>(v_.ptr_), val_len_, has_lob_header());
      if (loc.is_valid()) {
        bret = loc.is_persist_lob();
      }
    }
  }
  return bret;
}

inline int ObObj::get_print_string(ObString &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_lob()) {
    OB_LOG(WARN, "Lob: using get_print_string for text types", K(*this), K(lbt()));
  }
  if (meta_.is_string_type()) {
    v.assign_ptr(v_.string_, MIN(val_len_, OB_MAX_VARCHAR_LENGTH));
    ret = OB_SUCCESS;
  } else if (meta_.is_null()) {
    v.assign_ptr(NULL, 0);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_varchar(ObString &v) const
{
  //int ret = OB_OBJ_TYPE_ERROR;
  //if (meta_.is_varchar()) {
  //  v.assign_ptr(v_.string_, val_len_);
  //  ret = OB_SUCCESS;
  //}
  //return ret;
  //
  ////// todo  区分varchar和varbinary ;
  return get_string(v);
}

inline int ObObj::get_char(ObString &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_char()) {
    v.assign_ptr(v_.string_, val_len_);
    ret = OB_SUCCESS;
  } else if (meta_.is_null()) {
    v.assign_ptr(NULL, 0);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_nvarchar2(ObString &v) const
{
  return get_string(v);
}


inline int ObObj::get_nchar(ObString &v) const
{
  return get_string(v);
}

inline int ObObj::get_varbinary(ObString &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_varbinary()) {
    v.assign_ptr(v_.string_, val_len_);
    ret = OB_SUCCESS;
  } else if (meta_.is_null()) {
    v.assign_ptr(NULL, 0);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_raw(ObString &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_raw()) {
    v.assign_ptr(v_.string_, val_len_);
    ret = OB_SUCCESS;
  } else if (meta_.is_null()) {
    v.assign_ptr(NULL, 0);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_binary(ObString &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_binary()) {
    v.assign_ptr(v_.string_, val_len_);
    ret = OB_SUCCESS;
  } else if (meta_.is_null()) {
    v.assign_ptr(NULL, 0);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_hex_string(ObString &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_hex_string()) {
    v.assign_ptr(v_.string_, val_len_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_lob_value(const ObLobCommon *&value) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (is_lob_storage()) {
    value = v_.lob_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_lob_locator(ObLobLocator *&lob_locator) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (is_lob_locator()) {
    lob_locator = const_cast<ObLobLocator *>(v_.lob_locator_);
    if (!lob_locator->is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid lob locator", K(*this));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

inline int ObObj::get_lob_locatorv2(ObLobLocatorV2 &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (is_lob_storage()) {
    v.assign_ptr(v_.lob_locator_v2_, val_len_, has_lob_header());
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_lob_locatorv2(ObString &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (is_lob_storage()) {
    v.assign_ptr(v_.string_, val_len_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_bool(bool &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_tinyint()) {
    v = (0 != v_.int64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_ext(int64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_ext()) {
    v = v_.ext_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_unknown(int64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_unknown()) {
    v = v_.unknown_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_bit(uint64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_bit()) {
    v = v_.uint64_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_enum(uint64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_enum()) {
    v = v_.uint64_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_set(uint64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_set()) {
    v = v_.uint64_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_enumset_inner_value(ObEnumSetInnerValue &inner_value) const
{
  int ret = OB_SUCCESS;
  if (!ob_is_enumset_inner_tc(get_type())) {
    ret = OB_OBJ_TYPE_ERROR;
  } else {
    int64_t pos = 0;
    if (OB_FAIL(inner_value.deserialize(v_.string_, val_len_, pos))) {
    }
  }
  return ret;
}

inline int ObObj::get_interval_ds(ObIntervalDSValue &value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!meta_.is_interval_ds())) {
    ret = OB_OBJ_TYPE_ERROR;
  } else {
    value = get_interval_ds();
  }
  return ret;
}

inline int ObObj::get_interval_ym(ObIntervalYMValue &value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!meta_.is_interval_ym())) {
    ret = OB_OBJ_TYPE_ERROR;
  } else {
    value = get_interval_ym();
  }
  return ret;
}

inline int ObObj::get_urowid(ObURowIDData &urowid_data) const
{
    int ret = common::OB_SUCCESS;
    urowid_data.rowid_content_ = (const uint8_t *)v_.string_;
    urowid_data.rowid_len_ = val_len_;
    return ret;
}

inline int ObObj::get_udt(ObString &udt_data) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_user_defined_sql_type()) {
    udt_data.assign_ptr(v_.string_, val_len_);
    ret = OB_SUCCESS;
  }
  return ret;
}

//Obj中所有public的varchar hash方法，以及varchar类型的obj调用hash方法时，都调用了此方法
OB_INLINE static uint64_t varchar_hash_with_collation(const ObObj &obj,
                                                      const ObCollationType cs_type,
                                                      const uint64_t hash, hash_algo hash_al)
{
  return ObCharset::hash(cs_type, obj.get_string_ptr(), obj.get_string_len(), hash,
           obj.is_varying_len_char_type() && lib::is_oracle_mode(), hash_al);
}

inline uint64_t ObObj::varchar_hash(ObCollationType cs_type, uint64_t seed) const
{
  check_collation_integrity();
  return varchar_hash_with_collation(*this, cs_type, seed, NULL);
}

inline uint64_t ObObj::varchar_murmur_hash(ObCollationType cs_type, uint64_t seed) const
{
  check_collation_integrity();
  return varchar_hash_with_collation(*this, cs_type, seed, ObMurmurHash::hash);
}

inline uint64_t ObObj::varchar_wy_hash(ObCollationType cs_type, uint64_t seed) const
{
  check_collation_integrity();
  return varchar_hash_with_collation(*this, cs_type, seed, ObWyHash::hash);
}

inline uint64_t ObObj::varchar_xx_hash(ObCollationType cs_type, uint64_t seed) const
{
  check_collation_integrity();
  return varchar_hash_with_collation(*this, cs_type, seed, ObXxHash::hash);
}

inline const void *ObObj::get_data_ptr() const
{
  const void *ret = NULL;
  if (ob_is_string_type(get_type()) || ob_is_raw(get_type()) || ob_is_rowid_tc(get_type()) || ob_is_json(get_type())
      || ob_is_geometry(get_type()) || ob_is_user_defined_sql_type(get_type())) {
    ret = const_cast<char *>(v_.string_);
  } else if (ob_is_number_tc(get_type())) {
    ret = const_cast<uint32_t *>(v_.nmb_digits_);
  } else if (ob_is_lob_locator(get_type())) {
    ret = const_cast<ObLobLocator *>(v_.lob_locator_);
  } else {
    ret = &v_;
  }
  return ret;
};

inline void ObObj::set_data_ptr(void *data_ptr)
{
  if (ob_is_string_type(get_type()) || ob_is_raw(get_type()) || ob_is_rowid_tc(get_type()) || ob_is_json(get_type())
      || ob_is_geometry(get_type()) || ob_is_user_defined_sql_type(get_type())) {
    v_.string_ = static_cast<char*>(data_ptr);
  } else if (ob_is_number_tc(get_type())) {
    v_.nmb_digits_ = static_cast<uint32_t*>(data_ptr);
  } else if (ob_is_lob_locator(get_type())) {
    v_.lob_locator_ = static_cast<ObLobLocator *>(data_ptr);
  } else {
    //@TODO other value pointer
  }
};

template <typename Allocator>
int ObObj::to_collation_free_obj(ObObj &dst, bool &is_valid_collation_free, Allocator &allocator)
{
  int ret = OB_SUCCESS;
  const int32_t len = get_string_len();
  const bool is_copy_all = true;
  const int32_t buf_len = len * 2; // 对于变长类型的sortkey，最多会比原来的字符串占用2倍的空间
  char *buf = NULL;
  is_valid_collation_free = true;
  if (!is_character_type()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument, only varchar or char can be transformed to collation free obj",
        K(ret), "obj type", get_type());
  } else { // 对于字符串类型，目前仅考虑char或varchar作为rowkey的场景
    if (0 == len || NULL == get_string_ptr()) {
      copy_value_or_obj(dst, is_copy_all);
      dst.set_collation_type(CS_TYPE_COLLATION_FREE); // TODO: 等九仞把是否比较空格的collation加进来之后，还需要修改
    } else {
      if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "fail to allocate memory", K(ret), K(buf_len));
      } else {
        size_t size = ObCharset::sortkey(get_collation_type(), get_string_ptr(),
            static_cast<int64_t>(len), buf, static_cast<int64_t>(buf_len), is_valid_collation_free);
        copy_value_or_obj(dst, is_copy_all);
        if (is_varchar()) {
          dst.set_varchar_value(buf, static_cast<int32_t>(size));
        } else {
          dst.set_char_value(buf, static_cast<int32_t>(size));
        }
        dst.set_collation_type(CS_TYPE_COLLATION_FREE);
      }
    }
  }
  return ret;
}

//return byte length
inline int64_t ObObj::get_data_length() const
{
  int64_t ret = sizeof(v_);
  if (ob_is_string_type(get_type()) ||
      ob_is_raw(get_type()) ||
      ob_is_rowid_tc(get_type()) ||
      ob_is_lob_locator(get_type()) ||
      ob_is_json(get_type()) ||
      ob_is_geometry(get_type()) ||
      ob_is_user_defined_sql_type(get_type())) {
    ret = val_len_;
  } else if (ob_is_number_tc(get_type())) {
    ret = nmb_desc_.len_ * sizeof(uint32_t);
  }
  return ret;
};

inline int64_t ObObj::get_tight_data_len() const
{
  int64 len = 0;
  const ObObjType obj_type = get_type();
  if (ob_is_var_len_type(obj_type)) {
    len = val_len_;
  } else if (ob_is_number_tc(obj_type)) {
    len = nmb_desc_.len_ * sizeof(uint32_t);
  } else {  // fixed-length types
    switch(obj_type) {
      case ObNullType:
      case ObTinyIntType:
      case ObUTinyIntType:
      case ObYearType:
        len = 1;
        break;
      case ObSmallIntType:
      case ObUSmallIntType:
        len = 2;
        break;
      case ObMediumIntType:
      case ObUMediumIntType:
      case ObInt32Type:
      case ObUInt32Type:
      case ObFloatType:
      case ObUFloatType:
      case ObDateType:
        len = 4;
        break;
      case ObIntType:
      case ObUInt64Type:
      case ObDoubleType:
      case ObUDoubleType:
      case ObDateTimeType:
      case ObTimestampType:
      case ObTimeType:
      case ObBitType:
      case ObEnumType:  // @todo according to mysql doc, enum only need 2 bytes to store
      case ObSetType:
      case ObIntervalYMType:
        len = 8;
        break;
      case ObTimestampLTZType:
      case ObTimestampNanoType:
        len = 10;
        break;
      case ObTimestampTZType:
      case ObIntervalDSType:
        len = 12;
        break;
      default:
        OB_ASSERT(0);
        break;
    }
  }
  return len;
}

inline const void *ObObj::get_tight_data_ptr() const
{
  const void *ret = NULL;
  const ObObjType obj_type = get_type();
  if (ob_is_var_len_type(obj_type)
      || ob_is_number_tc(obj_type)) {
    ret = v_.ptr_;
  } else {  // fixed-length types
    switch(obj_type) {
      case ObNullType:
      case ObTinyIntType:
      case ObUTinyIntType:
      case ObYearType:
      case ObSmallIntType:
      case ObUSmallIntType:
      case ObMediumIntType:
      case ObUMediumIntType:
      case ObInt32Type:
      case ObUInt32Type:
      case ObFloatType:
      case ObUFloatType:
      case ObDateType:
      case ObIntType:
      case ObUInt64Type:
      case ObDoubleType:
      case ObUDoubleType:
      case ObDateTimeType:
      case ObTimestampType:
      case ObTimeType:
      case ObBitType:
      case ObEnumType:  // @todo according to mysql doc, enum only need 2 bytes to store
      case ObSetType:
      case ObIntervalYMType:
        ret = &v_;
        break;
      case ObTimestampLTZType:
      case ObTimestampNanoType:      // @FIXME, time_desc_ is the first field of time_ctx_
        ret = &time_ctx_.time_desc_;  // 2 + 8 continuous bytes
        break;
      case ObTimestampTZType:
      case ObIntervalDSType:
        ret = &interval_fractional_;  // 4 + 8 bytes
        break;
      default:
        OB_ASSERT(0);
    }
  }
  return ret;
};

template <typename AllocatorT>
    int ob_write_obj(AllocatorT &allocator, const ObObj &src, ObObj &dst)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(src.need_deep_copy())) {
    int64_t deep_copy_size = src.get_deep_copy_size();
    char *buf = static_cast<char*>(allocator.alloc(deep_copy_size));
    int64_t pos = 0;
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "allocate memory failed", K(ret), K(deep_copy_size));
    } else if (OB_FAIL(dst.deep_copy(src, buf, deep_copy_size, pos))) {
      LIB_LOG(WARN, "deep copy src obj failed", K(ret), K(deep_copy_size), K(pos));
    }
  } else {
    dst = src;
  }
  return ret;
}

inline int ObObj::write_otimestamp(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len < get_otimestamp_store_size())) {
    ret= common::OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf), K(len));
  } else {
    const ObOTimestampData &ot_data = get_otimestamp_value();
    *reinterpret_cast<int64_t *>(buf) = ot_data.time_us_;
    if (is_timestamp_tz()) {
      *reinterpret_cast<uint32_t *>(buf + sizeof(int64_t)) = ot_data.time_ctx_.desc_;
      pos += static_cast<int64_t>(sizeof(int64_t) + sizeof(uint32_t));
    } else {
      *reinterpret_cast<uint16_t *>(buf + sizeof(int64_t)) = ot_data.time_ctx_.time_desc_;
      pos += static_cast<int64_t>(sizeof(int64_t) + sizeof(uint16_t));
    }
  }
  return ret;
}

inline int ObObj::read_otimestamp(const char *buf, const int64_t len)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len < get_otimestamp_store_size())) {
    ret= common::OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf), K(len));
  } else {
    const int64_t time_us = *reinterpret_cast<int64_t *>(const_cast<char *>(buf));
    if (is_timestamp_tz()) {
      const uint32_t time_ctx_desc = *reinterpret_cast<const uint32_t *>(buf + sizeof(int64_t));
      set_otimestamp_value(get_type(), time_us, time_ctx_desc);
    } else {
      const uint16_t time_desc = *reinterpret_cast<const uint16_t *>(buf + sizeof(int64_t));
      set_otimestamp_value(get_type(), time_us, time_desc);
    }
  }
  return ret;
}

int ObObj::read_interval(const char *buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret= OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf));
  } else if (is_interval_ym()) {
    ObIntervalYMValue value;
    if (OB_FAIL(value.decode(buf))) {
    } else {
      set_interval_ym(value);
    }
  } else {
    ObIntervalDSValue value;
    if (OB_FAIL(value.decode(buf))) {
    } else {
      set_interval_ds(value);
    }
  }
  return ret;
}

int64_t ObObj::get_interval_store_size() const
{
  return is_interval_ym() ? ObIntervalYMValue::get_store_size() : ObIntervalDSValue::get_store_size();
}

int ObObj::write_interval(char *buf) const
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret= common::OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KP(buf));
  } else {
    ret = is_interval_ym() ? get_interval_ym().encode(buf)
                           : get_interval_ds().encode(buf);
  }
  return ret;
}

inline bool ObObj::strict_equal(const ObObj &other) const
{
  bool bret = true;
  if (OB_UNLIKELY(get_type() != other.get_type())) {
    bret = false;
  } else {
    //here must use CS_TYPE_BINARY to compare, avoid spaces at the end of the string be ignored
    bret = (0 == compare(other, CS_TYPE_BINARY));
    if (bret && is_timestamp_tz()) {
      //for the data type of timestamp with time zone,
      //obj meaningful info includes v_.datetime_ and time_ctx_.desc_
      //so we must compare time_ctx_.desc_ here
      bret = (time_ctx_.desc_ == other.time_ctx_.desc_);
    }
  }
  return bret;
}

#define DEFINE_SET_COMMON_OBJ_VALUE(VTYPE, OBJTYPE)             \
template<>                                                      \
inline void ObObj::set_obj_value<OBJTYPE>(const OBJTYPE & v)    \
{                                                               \
  v_.VTYPE##_ = static_cast<__typeof__(v_.VTYPE##_)>(v);            \
}

DEFINE_SET_COMMON_OBJ_VALUE(int64, int8_t);
DEFINE_SET_COMMON_OBJ_VALUE(uint64, uint8_t);
DEFINE_SET_COMMON_OBJ_VALUE(int64, int16_t);
DEFINE_SET_COMMON_OBJ_VALUE(uint64, uint16_t);
DEFINE_SET_COMMON_OBJ_VALUE(int64, int32_t);
DEFINE_SET_COMMON_OBJ_VALUE(uint64, uint32_t);
DEFINE_SET_COMMON_OBJ_VALUE(float, float);
DEFINE_SET_COMMON_OBJ_VALUE(double, double);
DEFINE_SET_COMMON_OBJ_VALUE(int64, int64_t);
DEFINE_SET_COMMON_OBJ_VALUE(uint64, uint64_t);

template<>
inline void ObObj::set_obj_value<ObString>(const ObString &v)
{
  v_.string_ = v.ptr();
  val_len_ = v.length();
}

template<>
inline void ObObj::set_obj_value<ObIntervalYMValue>(const ObIntervalYMValue &v)
{
  v_.nmonth_ = v.nmonth_;
  interval_fractional_ = 0;
}

template<>
inline void ObObj::set_obj_value<ObIntervalDSValue>(const ObIntervalDSValue &v)
{
  v_.nsecond_ = v.nsecond_;
  interval_fractional_ = v.fractional_second_;
}

template<>
inline void ObObj::set_obj_value<ObURowIDData>(const ObURowIDData &urowid)
{
  v_.string_ = (const char *)urowid.rowid_content_;
  val_len_ = static_cast<int32_t>(urowid.rowid_len_);
}

template<>
inline void ObObj::set_obj_value<ObOTimestampData>(const ObOTimestampData &otimestamp)
{
  time_ctx_ = otimestamp.time_ctx_;
  v_.datetime_ = otimestamp.time_us_;
}

struct ParamFlag
{
  ParamFlag() : need_to_check_type_(true),
                need_to_check_bool_value_(false),
                expected_bool_value_(false),
                need_to_check_extend_type_(true),
                is_ref_cursor_type_(false),
                is_pl_mock_default_param_(false),
                is_boolean_(false),
                is_batch_parameter_(0),
                ignore_scale_check_(false),
                reserved_(0)
  { }
  TO_STRING_KV(K_(need_to_check_type), K_(need_to_check_bool_value),
               K_(expected_bool_value), K_(is_pl_mock_default_param), K_(need_to_check_extend_type), K_(is_batch_parameter),
               K_(ignore_scale_check));
  void reset();

  static uint32_t flag_offset_bits() { return offsetof(ParamFlag, flag_) * 8; }

  union
  {
    uint8_t flag_;
    struct {
      uint8_t need_to_check_type_: 1; //TRUE if the type need to be checked by plan cache, FALSE otherwise
      uint8_t need_to_check_bool_value_ : 1;//TRUE if the bool value need to be checked by plan cache, FALSE otherwise
      uint8_t expected_bool_value_ : 1;//bool value, effective only when need_to_check_bool_value_ is true
      uint8_t need_to_check_extend_type_ : 1; // True if the extended type needs to be checked
      uint8_t is_ref_cursor_type_ : 1; // in pl/sql context, this will be true if the local var is a ref cursor
      uint8_t is_pl_mock_default_param_ : 1; // TRUE if ObObjParam is pl default param
      uint8_t is_boolean_ : 1; // to distinguish T_BOOL and T_TINYINT
      uint8_t is_batch_parameter_ : 1; // 表示是batch参数
    };
  };

  union
  {
    uint8_t extend_flag_;
    struct {
      uint8_t ignore_scale_check_ : 1; //TRUE if plan cache can reuse by different scale numbers, FALSE otherwise
      uint8_t reserved_ : 7;
    };
  };
  OB_UNIS_VERSION_V(1);
};

class ObObjParam : public ObObj
{
public:
  ObObjParam() : ObObj(), accuracy_(), res_flags_(0),
                          raw_text_pos_(-1), raw_text_len_(-1)
  {
  }
  ObObjParam(const ObObj &other) : ObObj(other), accuracy_(), res_flags_(0),
                                                 raw_text_pos_(-1), raw_text_len_(-1)
  {
  }
public:
  void reset();
  // accuracy.
  OB_INLINE void set_accuracy(const common::ObAccuracy &accuracy) { accuracy_.set_accuracy(accuracy); }
  OB_INLINE void set_length(common::ObLength length) { accuracy_.set_length(length); }
  OB_INLINE void set_precision(common::ObPrecision precision) { accuracy_.set_precision(precision); }
  OB_INLINE void set_length_semantics(common::ObLengthSemantics length_semantics) { accuracy_.set_length_semantics(length_semantics); }
  OB_INLINE void set_scale(common::ObScale scale) {
    ObObj::set_scale(scale);
    accuracy_.set_scale(scale);
  }
  OB_INLINE void set_udt_id(uint64_t id) {
    accuracy_.set_accuracy(id);
  }
  OB_INLINE const common::ObAccuracy &get_accuracy() const { return accuracy_; }
  OB_INLINE common::ObLength get_length() const { return accuracy_.get_length(); }
  OB_INLINE common::ObPrecision get_precision() const { return accuracy_.get_precision(); }
  OB_INLINE common::ObScale get_scale() const { return accuracy_.get_scale(); }
  OB_INLINE uint64_t get_udt_id() const { return is_ext() ? accuracy_.get_accuracy() : OB_INVALID_INDEX; }
  OB_INLINE bool is_extend_xml_type() const { return get_udt_id() == T_OBJ_XML; }
  OB_INLINE void set_result_flag(uint32_t flag) { res_flags_ |= flag; }
  OB_INLINE void unset_result_flag(uint32_t flag) { res_flags_ &= (~flag); }
  OB_INLINE bool has_result_flag(uint32_t flag) const { return res_flags_ & flag; }
  OB_INLINE uint32_t get_result_flag() const { return res_flags_; }

  OB_INLINE const ParamFlag &get_param_flag() const { return flag_; }
  OB_INLINE ParamFlag &get_param_flag() { return flag_; }
  OB_INLINE bool is_batch_parameters() const { return flag_.is_batch_parameter_; }
  OB_INLINE void set_param_flag(const ParamFlag flag) { flag_ = flag; }
  OB_INLINE void set_need_to_check_type(bool flag) { flag_.need_to_check_type_ = flag; }
  OB_INLINE bool need_to_check_type() const { return flag_.need_to_check_type_; }
  OB_INLINE void set_need_to_check_extend_type(bool flag) { flag_.need_to_check_extend_type_ = flag; }
  OB_INLINE bool need_to_check_extend_type() const { return flag_.need_to_check_extend_type_; }

  OB_INLINE void set_need_to_check_bool_value(bool flag) { flag_.need_to_check_bool_value_ = flag; }
  OB_INLINE bool need_to_check_bool_value() const { return flag_.need_to_check_bool_value_; }

  OB_INLINE void set_expected_bool_value(bool b_value) { flag_.expected_bool_value_ = b_value; }
  OB_INLINE bool expected_bool_value() const { return flag_.expected_bool_value_; }

  OB_INLINE void set_is_ref_cursor_type(bool flag) { flag_.is_ref_cursor_type_ = flag; }
  OB_INLINE bool is_ref_cursor_type() const { return flag_.is_ref_cursor_type_; }
  OB_INLINE void set_is_pl_mock_default_param(bool flag) { flag_.is_pl_mock_default_param_= flag; }
  OB_INLINE bool is_pl_mock_default_param() const { return flag_.is_pl_mock_default_param_; }
  OB_INLINE void set_is_boolean(bool flag) { flag_.is_boolean_ = flag; }
  OB_INLINE bool is_boolean() const { return flag_.is_boolean_; }

  OB_INLINE void set_ignore_scale_check(bool flag) { flag_.ignore_scale_check_ = flag; }
  OB_INLINE bool ignore_scale_check() const { return flag_.ignore_scale_check_; }

  OB_INLINE void set_raw_text_info(int32_t pos, int32_t len)
  {
    raw_text_pos_ = pos;
    raw_text_len_ = len;
  }
  OB_INLINE int32_t get_raw_text_pos() const { return raw_text_pos_; }
  OB_INLINE int32_t get_raw_text_len() const { return raw_text_len_; }
  OB_INLINE void set_param_meta() { param_meta_ = get_meta(); }
  OB_INLINE void set_param_meta(const ObObjMeta &meta) { param_meta_ = meta; }
  OB_INLINE const ObObjMeta &get_param_meta() const
  {
    return param_meta_;
  }
  // others.
  INHERIT_TO_STRING_KV(N_OBJ, ObObj, N_ACCURACY, accuracy_,
                       N_FLAG, res_flags_, K_(raw_text_pos), K_(raw_text_len), K_(param_meta));
  NEED_SERIALIZE_AND_DESERIALIZE;

  static uint32_t accuracy_offset_bits() { return offsetof(ObObjParam, accuracy_) * 8; }
  static uint32_t res_flags_offset_bits() { return offsetof(ObObjParam, res_flags_) * 8; }
  static uint32_t flag_offset_bits() { return offsetof(ObObjParam, flag_) * 8; }

private:
  ObAccuracy accuracy_;
  uint32_t res_flags_;  // BINARY, NUM, NOT_NULL, TIMESTAMP, etc
                        // reference: src/lib/regex/include/mysql_com.h
  ParamFlag flag_;
  int32_t raw_text_pos_;
  int32_t raw_text_len_;
  ObObjMeta param_meta_; //meta for objparma, to solve Oracle NULL/'' problem
};

struct ObDataType
{
  OB_UNIS_VERSION(1);
public:
  ObDataType()
    : meta_(),
      accuracy_(),
      charset_(CHARSET_UTF8MB4),
      is_binary_collation_(false),
      is_zero_fill_(false)
  {}
  TO_STRING_KV(K_(meta),
               K_(accuracy),
               K_(charset),
               K_(is_binary_collation),
               K_(is_zero_fill));
  inline void reset()
  {
    meta_.reset();
    accuracy_.reset();
    charset_ = CHARSET_UTF8MB4;
    is_binary_collation_ = false;
    is_zero_fill_ = false;
  }
  inline bool operator==(const ObDataType &other) const
  {
    return meta_ == other.meta_
        && accuracy_ == other.accuracy_
        && charset_ == other.charset_
        && is_binary_collation_ == other.is_binary_collation_
        && is_zero_fill_ == other.is_zero_fill_;
  }
  inline ObObjType get_obj_type() const { return meta_.get_type(); }
  inline ObObjTypeClass get_type_class() const { return meta_.get_type_class(); }
  inline ObLength get_length() const { return accuracy_.get_length(); }
  inline ObPrecision get_precision() const { return accuracy_.get_precision(); }
  inline ObLengthSemantics get_length_semantics() const { return accuracy_.get_length_semantics(); }
  inline ObScale get_scale() const { return accuracy_.get_scale(); }
  inline ObCharsetType get_charset_type() const { return charset_; }
  inline ObCollationType get_collation_type() const { return meta_.get_collation_type(); }
  inline ObCollationLevel get_collation_level() const { return meta_.get_collation_level(); }
  inline bool is_binary_collation() const { return is_binary_collation_; }
  inline bool is_zero_fill() const { return is_zero_fill_; }
  inline void set_obj_type(const ObObjType &type) { return meta_.set_type(type); }
  inline void set_length(const ObLength length) { accuracy_.set_length(length); }
  inline void set_precision(const ObPrecision precision) { accuracy_.set_precision(precision); }
  inline void set_length_semantics(const ObLengthSemantics length_semantics) { accuracy_.set_length_semantics(length_semantics); }
  inline void set_scale(const ObScale scale) { accuracy_.set_scale(scale); }
  inline void set_charset_type(const ObCharsetType charset_type) { charset_ = charset_type; }
  inline void set_collation_type(const ObCollationType coll_type) { meta_.set_collation_type(coll_type); }
  inline void set_collation_level(const ObCollationLevel coll_level) { meta_.set_collation_level(coll_level); }
  inline void set_binary_collation(const bool is_binary_collation) { is_binary_collation_ = is_binary_collation; }
  inline void set_zero_fill(const bool is_zero_fill) { is_zero_fill_ = is_zero_fill; }
  inline const ObObjMeta &get_meta_type() const { return meta_; }
  inline void set_meta_type(const ObObjMeta &meta_type) { meta_ = meta_type; }
  inline const ObAccuracy &get_accuracy() const { return accuracy_; }
  inline void set_accuracy(const ObAccuracy &accuracy) { accuracy_ = accuracy; }
  inline int64_t get_accuracy_value() const { return accuracy_.accuracy_; }
  inline void set_int() { meta_.set_int(); }
  inline uint64_t get_udt_id() const { return accuracy_.get_accuracy(); }
  inline void set_udt_id(uint64_t udt_id) { accuracy_.set_accuracy(udt_id); }
  inline void set_subschema_id(const uint16_t subschema_id) { meta_.set_subschema_id(subschema_id); }
  ObObjMeta meta_;
  ObAccuracy accuracy_;
  ObCharsetType charset_;
  bool is_binary_collation_;
  bool is_zero_fill_;
};


struct ObSqlArrayObj
{
  ObSqlArrayObj()
    : data_(nullptr),
      count_(0),
      element_()
  {
  }
  typedef common::ObArrayWrap<common::ObObjParam> DataArray;
  static ObSqlArrayObj *alloc(common::ObIAllocator &allocator, int64_t count);
  TO_STRING_KV("data", DataArray(data_, count_), K_(count), K_(element));
  common::ObObjParam *data_;
  int64_t count_;
  common::ObDataType element_;
};

OB_INLINE ObSqlArrayObj *ObSqlArrayObj::alloc(common::ObIAllocator &allocator, int64_t count)
{
  ObSqlArrayObj *array_obj = nullptr;
  void *array_buf = nullptr;
  void *data_buf = nullptr;
  int64_t array_size = sizeof(ObSqlArrayObj) + sizeof(common::ObObjParam) * count;
  if (OB_NOT_NULL(array_buf = allocator.alloc(array_size))) {
    array_obj = new (array_buf) ObSqlArrayObj();
    data_buf = static_cast<char*>(array_buf) + sizeof(ObSqlArrayObj);
    array_obj->data_ = new (data_buf) common::ObObjParam[count];
    array_obj->count_ = count;
  }
  return array_obj;
}

OB_INLINE int64_t ObObj::get_deep_copy_size() const
{
  int64_t ret = 0;
  if (is_string_type() || is_raw() || ob_is_rowid_tc(get_type()) || is_lob_locator() || is_json()
      || is_geometry() || is_user_defined_sql_type()) {
    ret += val_len_;
  } else if (ob_is_number_tc(get_type())) {
    ret += (sizeof(uint32_t) * nmb_desc_.len_);
  }
  return ret;
}

typedef int (*ob_obj_print)(const ObObj &obj, char *buffer, int64_t length, int64_t &pos, const ObObjPrintParams &params);
typedef int64_t (*ob_obj_crc64)(const ObObj &obj, const int64_t current);
typedef void (*ob_obj_batch_checksum)(const ObObj &obj, ObBatchChecksum &bc);
typedef int (*ob_obj_hash)(const ObObj &obj, const uint64_t hash, uint64_t& res);
typedef int (*ob_obj_value_serialize)(const ObObj &obj, char* buf, const int64_t buf_len, int64_t& pos);
typedef int (*ob_obj_value_deserialize)(ObObj &obj, const char* buf, const int64_t data_len, int64_t& pos);
typedef int64_t (*ob_obj_value_get_serialize_size)(const ObObj &obj);
typedef uint64_t (*ob_obj_crc64_v3)(const ObObj &obj, const uint64_t hash);

class ObObjUtil
{
public:
  static ob_obj_hash get_murmurhash_v3(ObObjType type);
  static ob_obj_hash get_murmurhash_v2(ObObjType type);
  static ob_obj_crc64_v3 get_crc64_v3(ObObjType type);
  static ob_obj_hash get_xxhash64(ObObjType type);
  static ob_obj_hash get_wyhash(ObObjType type);
};

class ObHexEscapeSqlStr
{
public:
  ObHexEscapeSqlStr(const common::ObString &str) : str_(str),
                                                   skip_escape_(false),
                                                   do_oracle_mode_escape_(lib::is_oracle_mode()) { }
  ObHexEscapeSqlStr(const common::ObString &str, const bool skip_escape) : str_(str),
                                                                           skip_escape_(skip_escape),
                                                                           do_oracle_mode_escape_(lib::is_oracle_mode()){ }
  ObHexEscapeSqlStr(const common::ObString &str,
                    const bool skip_escape,
                    const bool do_oracle_mode_escape)
    : str_(str), skip_escape_(skip_escape), do_oracle_mode_escape_(do_oracle_mode_escape){ }
  ObString str() const { return str_; }
  int64_t get_extra_length() const;
  DECLARE_TO_STRING;
private:
  ObString str_;
  bool skip_escape_;
  bool do_oracle_mode_escape_;
};

typedef Ob2DArray<ObObjParam, OB_MALLOC_BIG_BLOCK_SIZE,
                          ObWrapperAllocator,false,
                          ObSEArray<ObObjParam *, 1, ObWrapperAllocator, false>
                          > ParamStore;
}
}

#endif //
