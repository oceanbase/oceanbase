/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_BLOCKSSTABLE_OB_INDEX_BLOCK_UTIL_
#define OCEANBASE_BLOCKSSTABLE_OB_INDEX_BLOCK_UTIL_

#include "share/datum/ob_datum.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
struct ObSkipIndexColumnAttr;
}
}
namespace blocksstable
{

// Only MIN_MAX skipping index is supported now.
enum ObSkipIndexType : uint8_t
{
  MIN_MAX,
  BLOOM_FILTER,
  NGRAM_BLOOM_FILTER,
  MAX_TYPE
};

enum ObSkipIndexColType : uint8_t
{
  // NOTICE: order of this enum will affect order of stored pre-aggregate columns
  SK_IDX_MIN = 0,
  SK_IDX_MAX,
  SK_IDX_NULL_COUNT,
  SK_IDX_SUM,
  SK_IDX_MAX_COL_TYPE
};

struct ObSkipIndexColMeta
{
  // For data with length larger than 40 bytes(normally string), we will store the prefix as min/max
  static constexpr int64_t MAX_SKIP_INDEX_COL_LENGTH = 40;
  static constexpr int64_t SKIP_INDEX_ROW_SIZE_LIMIT = 1 << 10; // 1kb
  static constexpr int64_t MAX_AGG_COLUMN_PER_ROW = 4; // min / max / null count / sum
  static constexpr ObObjDatumMapType NULL_CNT_COL_TYPE = OBJ_DATUM_8BYTE_DATA;
  static_assert(common::OBJ_DATUM_NUMBER_RES_SIZE == MAX_SKIP_INDEX_COL_LENGTH,
      "Buffer size of ObStorageDatum and maximum size of skip index data is equal to maximum size of ObNumber");
  ObSkipIndexColMeta() : pack_(0) {}
  ObSkipIndexColMeta(const uint32_t col_idx, const ObSkipIndexColType col_type)
      : col_idx_(col_idx), col_type_(col_type) {}
  bool is_valid() const { return col_type_ < SK_IDX_MAX_COL_TYPE; }
  bool operator <(const ObSkipIndexColMeta &rhs) const
  {
    bool ret = false;
    if (col_idx_ != rhs.col_idx_) {
      ret = col_idx_ < rhs.col_idx_;
    } else {
      ret = col_type_ < rhs.col_type_;
    }
    return ret;
  }

  bool operator !=(const ObSkipIndexColMeta &rhs) const
  {
    return pack_ != rhs.pack_;
  }

  static int append_skip_index_meta(
      const share::schema::ObSkipIndexColumnAttr &skip_idx_attr,
      const int64_t col_idx,
      common::ObIArray<ObSkipIndexColMeta> &skip_idx_metas);
  static int calc_skip_index_maximum_size(
      const share::schema::ObSkipIndexColumnAttr &skip_idx_attr,
      const ObObjType obj_type,
      const int16_t precision,
      int64_t &max_size);

  TO_STRING_KV(K_(pack), K_(col_idx), K_(col_type));

  union
  {
    uint32_t pack_;
    struct
    {
      uint32_t col_idx_   : 24; // Column index this skip index column refer to
      uint32_t col_type_  : 8;  // Skip index column type
    };
  };
};

enum ObSkipIndexUpperStoreSize {
  SKIP_INDEX_NULL_UPPER_SIZE = 0,
  SKIP_INDEX_8BYTE_UPPER_SIZE = 8,
  SKIP_INDEX_2BYTE_LEN_DATA_UPPER_SIZE = 10,
  SKIP_INDEX_4BYTE_LEN_DATA_UPPER_SIZE = 12,
  SKIP_INDEX_VAR_LENGTH_UPPER_SIZE = ObSkipIndexColMeta::MAX_SKIP_INDEX_COL_LENGTH,
};

OB_INLINE static int get_skip_index_store_upper_size(
    const ObObjDatumMapType type,
    const int16_t precision,
    uint32_t &upper_size)
{
  static const uint32_t OBOBJ_DATUM_MAP_TYPE_TO_AGG_UPPER_SIZE_MAP[] = {
    SKIP_INDEX_NULL_UPPER_SIZE,  // OBJ_DATUM_NULL
    SKIP_INDEX_VAR_LENGTH_UPPER_SIZE,  // OBJ_DATUM_STRING
    SKIP_INDEX_VAR_LENGTH_UPPER_SIZE,  // OBJ_DATUM_NUMBER
    SKIP_INDEX_8BYTE_UPPER_SIZE,  // OBJ_DATUM_8BYTE_DATA
    SKIP_INDEX_8BYTE_UPPER_SIZE,  // OBJ_DATUM_4BYTE_DATA
    SKIP_INDEX_8BYTE_UPPER_SIZE,  // OBJ_DATUM_1BYTE_DATA
    SKIP_INDEX_4BYTE_LEN_DATA_UPPER_SIZE, // OBJ_DATUM_4BYTE_LEN_DATA
    SKIP_INDEX_2BYTE_LEN_DATA_UPPER_SIZE, // OBJ_DATUM_2BYTE_LEN_DATA
    SKIP_INDEX_8BYTE_UPPER_SIZE,  // OBJ_DATUM_FULL
    SKIP_INDEX_VAR_LENGTH_UPPER_SIZE, // OBJ_DATUM_DECIMALINT
  };
  static_assert(sizeof(OBOBJ_DATUM_MAP_TYPE_TO_AGG_UPPER_SIZE_MAP)
                / sizeof(OBOBJ_DATUM_MAP_TYPE_TO_AGG_UPPER_SIZE_MAP[0]) == OBJ_DATUM_MAPPING_MAX,
      "new added ObObjDatumMapType should extend this map");
  static const uint32_t OBOBJ_DECIMAL_INT_TO_AGG_UPPER_SIZE_MAP[] =
  {
    sizeof(int32_t),   // DECIMAL_INT_32
    sizeof(int64_t),   // DECIMAL_INT_64
    sizeof(int128_t),  // DECIMAL_INT_128
    sizeof(int256_t),  // DECIMAL_INT_256
    sizeof(int512_t),  // DECIMAL_INT_512
  };
  int ret = OB_SUCCESS;
  upper_size = 0;
  if (OB_UNLIKELY(type >= OBJ_DATUM_MAPPING_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid obj type", K(type));
  } else if (OBJ_DATUM_DECIMALINT == type) {
    if (OB_UNLIKELY(precision < 0 || precision > OB_MAX_DECIMAL_PRECISION)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid decimal int precision",
                      K(type), K(precision));
    } else {
      const ObDecimalIntWideType decimalint_type = get_decimalint_type(precision);
      upper_size = OBOBJ_DECIMAL_INT_TO_AGG_UPPER_SIZE_MAP[decimalint_type];
    }
  } else {
    upper_size = OBOBJ_DATUM_MAP_TYPE_TO_AGG_UPPER_SIZE_MAP[type];
  }
  return ret;
}

OB_INLINE static bool is_skip_index_black_list_type(const ObObjType &obj_type)
{
  return ObNullType == obj_type || ob_is_json_tc(obj_type) || ob_is_geometry_tc(obj_type)
      || ob_is_user_defined_sql_type(obj_type) || ob_is_roaringbitmap_tc(obj_type);
}

OB_INLINE static bool is_skip_index_while_list_type(const ObObjType &obj_type)
{
  const ObObjTypeClass tc = ob_obj_type_class(obj_type);
  return (ObIntTC <= tc && tc <= ObLobTC) || ObDecimalIntTC == tc;
}

OB_INLINE static bool can_agg_sum(const ObObjType &obj_type)
{
  return is_skip_index_while_list_type(obj_type) && ob_is_numeric_type(obj_type)
      && ob_obj_type_class(obj_type) != ObObjTypeClass::ObBitTC;
}

OB_INLINE static int get_sum_store_size(const ObObjType &obj_type, uint32_t &sum_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNullType > obj_type || ObMaxType < obj_type || !can_agg_sum(obj_type))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "invalid type from sum", K(obj_type));
  } else {
    switch(ob_obj_type_class(obj_type)) {
      case ObObjTypeClass::ObIntTC:
      case ObObjTypeClass::ObUIntTC:
      case ObObjTypeClass::ObDecimalIntTC:
      case ObObjTypeClass::ObNumberTC:
        sum_size = common::OBJ_DATUM_NUMBER_RES_SIZE;
        break;
      case ObObjTypeClass::ObFloatTC: {
        sum_size = sizeof(float);
        break;
      }
      case ObObjTypeClass::ObDoubleTC: {
        sum_size = sizeof(double);
        break;
      }
      default: {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "invalid type from sum", K(obj_type));
    }
    }
  }
  return ret;
}

} // blocksstable
} // oceanbase

#endif
