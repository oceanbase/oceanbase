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

#define USING_LOG_PREFIX SHARE

#include "ob_datum.h"
#include "ob_datum_util.h"

namespace oceanbase
{
namespace common
{

ObObjDatumMapType ObDatum::get_obj_datum_map_type(const ObObjType type)
{

  const static ObObjDatumMapType maps[] = {
    OBJ_DATUM_NULL,           // ObNullType
    OBJ_DATUM_8BYTE_DATA,     // ObTinyIntType
    OBJ_DATUM_8BYTE_DATA,     // ObSmallIntType
    OBJ_DATUM_8BYTE_DATA,     // ObMediumIntType
    OBJ_DATUM_8BYTE_DATA,     // ObInt32Type
    OBJ_DATUM_8BYTE_DATA,     // ObIntType
    OBJ_DATUM_8BYTE_DATA,     // ObUTinyIntType
    OBJ_DATUM_8BYTE_DATA,     // ObUSmallIntType
    OBJ_DATUM_8BYTE_DATA,     // ObUMediumIntType
    OBJ_DATUM_8BYTE_DATA,     // ObUInt32Type
    OBJ_DATUM_8BYTE_DATA,     // ObUInt64Type
    OBJ_DATUM_4BYTE_DATA,     // ObFloatType
    OBJ_DATUM_8BYTE_DATA,     // ObDoubleType
    OBJ_DATUM_4BYTE_DATA,     // ObUFloatType
    OBJ_DATUM_8BYTE_DATA,     // ObUDoubleType
    OBJ_DATUM_NUMBER,         // ObNumberType
    OBJ_DATUM_NUMBER,         // ObUNumberType
    OBJ_DATUM_8BYTE_DATA,     // ObDateTimeType
    OBJ_DATUM_8BYTE_DATA,     // ObTimestampType
    OBJ_DATUM_4BYTE_DATA,     // ObDateType
    OBJ_DATUM_8BYTE_DATA,     // ObTimeType
    OBJ_DATUM_1BYTE_DATA,     // ObYearType
    OBJ_DATUM_STRING,         // ObVarcharType
    OBJ_DATUM_STRING,         // ObCharType
    OBJ_DATUM_STRING,         // ObHexStringType
    OBJ_DATUM_FULL,           // ObExtendType
    OBJ_DATUM_8BYTE_DATA,     // ObUnknownType
    OBJ_DATUM_STRING,         // ObTinyTextType
    OBJ_DATUM_STRING,         // ObTextType
    OBJ_DATUM_STRING,         // ObMediumTextType
    OBJ_DATUM_STRING,         // ObLongTextType
    OBJ_DATUM_8BYTE_DATA,     // ObBitType
    OBJ_DATUM_8BYTE_DATA,     // ObEnumType
    OBJ_DATUM_8BYTE_DATA,     // ObSetType
    OBJ_DATUM_STRING,         // ObEnumInnerType
    OBJ_DATUM_STRING,         // ObSetInnerType
    OBJ_DATUM_4BYTE_LEN_DATA, // ObTimestampTZType
    OBJ_DATUM_2BYTE_LEN_DATA, // ObTimestampLTZType
    OBJ_DATUM_2BYTE_LEN_DATA, // ObTimestampNanoType
    OBJ_DATUM_STRING,         // ObRawType
    OBJ_DATUM_8BYTE_DATA    , // ObIntervalYMType
    OBJ_DATUM_4BYTE_LEN_DATA, // ObIntervalDSType
    OBJ_DATUM_NUMBER,         // ObNumberFloatType
    OBJ_DATUM_STRING,         // ObNVarchar2Type
    OBJ_DATUM_STRING,         // ObNCharType
    OBJ_DATUM_STRING,         // ObURowID
    OBJ_DATUM_STRING,         // ObLobType
    OBJ_DATUM_STRING,         // ObJsonType
    OBJ_DATUM_STRING,         // ObGeometryType
    OBJ_DATUM_STRING,         // ObUserDefinedSQLType
  };
  static_assert(sizeof(maps) / sizeof(maps[0]) == ObMaxType,
      "new added type should extend this map");
  ObObjDatumMapType t = OBJ_DATUM_MAPPING_MAX;
  if (type < 0 || type >= ObMaxType) {
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid obj type", K(type));
  } else {
    t = maps[type];
  }

  return t;
}

uint32_t ObDatum::get_reserved_size(const ObObjDatumMapType type)
{
  static const uint32_t OBOBJ_DATUM_MAP_TYPE_TO_RES_SIZE_MAP[] =
  {
    OBJ_DATUM_NULL_RES_SIZE,           // OBJ_DATUM_NULL
    OBJ_DATUM_STRING_RES_SIZE,         // OBJ_DATUM_STRING
    OBJ_DATUM_NUMBER_RES_SIZE,         // OBJ_DATUM_NUMBER
    OBJ_DATUM_8BYTE_DATA_RES_SIZE,     // OBJ_DATUM_8BYTE_DATA
    OBJ_DATUM_4BYTE_DATA_RES_SIZE,     // OBJ_DATUM_4BYTE_DATA
    OBJ_DATUM_1BYTE_DATA_RES_SIZE,     // OBJ_DATUM_1BYTE_DATA
    OBJ_DATUM_4BYTE_LEN_DATA_RES_SIZE, // OBJ_DATUM_4BYTE_LEN_DATA
    OBJ_DATUM_2BYTE_LEN_DATA_RES_SIZE, // OBJ_DATUM_2BYTE_LEN_DATA
    OBJ_DATUM_FULL_DATA_RES_SIZE,      // OBJ_DATUM_FULL
  };
  static_assert(sizeof(OBOBJ_DATUM_MAP_TYPE_TO_RES_SIZE_MAP)
                / sizeof(OBOBJ_DATUM_MAP_TYPE_TO_RES_SIZE_MAP[0]) == OBJ_DATUM_MAPPING_MAX,
      "new added ObObjDatumMapType should extend this map");

  uint32_t res_size = 0;
  if (type >= OBJ_DATUM_MAPPING_MAX) {
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid obj type", K(type));
  } else {
    res_size = OBOBJ_DATUM_MAP_TYPE_TO_RES_SIZE_MAP[type];
  }

  return res_size;
}

DEF_TO_STRING(ObDatum)
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  const static int64_t STR_MAX_PRINT_LEN = 128L;
  if (null_) {
    J_NULL();
  } else {
    J_OBJ_START();
    BUF_PRINTF("len: %d, flag: %d, null: %d, ptr: %p", len_, flag_, null_, ptr_);
    if (len_ > 0) {
      OB_ASSERT(NULL != ptr_);
      const int64_t plen = std::min(static_cast<int64_t>(len_),
                                    static_cast<int64_t>(STR_MAX_PRINT_LEN));
      // print hex value
      BUF_PRINTF(", hex: ");
      if (OB_FAIL(hex_print(ptr_, plen, buf, buf_len, pos))) {
        // no logging in to_string function.
      } else {
        // maybe ObIntTC
        if (sizeof(int64_t) == len_) {
          BUF_PRINTF(", int: %ld", *int_);
          // maybe number with one digit
          if (1 == num_->desc_.len_) {
            BUF_PRINTF(", num_digit0: %u", num_->digits_[0]);
          }
        }
        // maybe printable C string
        int64_t idx = 0;
        while (idx < plen && isprint(ptr_[idx])) {
          idx++;
        }
        if (idx >= plen) {
          BUF_PRINTF(", cstr: %.*s", static_cast<int>(plen), ptr_);
        }
      }
    }
    J_OBJ_END();
  }
  return pos;
}

OB_DEF_SERIALIZE(ObDatum)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(pack_);
  if (OB_NOT_NULL(ptr_)) {
    if (len_ > 0) {
      MEMCPY(buf + pos, ptr_, len_);
      pos += len_;
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDatum)
{
  int ret = OB_SUCCESS;
  uint32_t pack = pack_;
  OB_UNIS_DECODE(pack);
  if (OB_SUCC(ret)) {
    pack_ = pack;
    ptr_ = buf + pos;
    pos += len_;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDatum)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(pack_);
  len += len_;
  return len;
}


} // end namespace common
} // end namespace oceanbase
