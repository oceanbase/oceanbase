
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

#ifndef OCEANBASE_ENCODING_OB_CS_DECODING_UTIL_H_
#define OCEANBASE_ENCODING_OB_CS_DECODING_UTIL_H_

#if defined ( __ARM_NEON )
#include <arm_neon.h>
#endif

#include "ob_stream_encoding_struct.h"
#include "ob_integer_stream_decoder.h"
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"
#include "storage/blocksstable/encoding/neon/ob_encoding_neon_util.h"
#include "storage/blocksstable/encoding/ob_encoding_util.h"

namespace oceanbase
{
namespace blocksstable
{

typedef void (*cs_dict_fast_cmp_function) (
            const int64_t dict_ref_val,
            const int64_t dict_val_cnt,
            const char *dict_ref_buf,
            const sql::PushdownFilterInfo &pd_filter_info,
            sql::ObBitVector &result);

// if lval >= rval, diff = lval - rval; otherwise, diff is useless.
typedef bool (*cs_filter_is_less_than) (
            const uint64_t lval,
            const int64_t lval_size,
            const uint64_t rval,
            const int64_t rval_size,
            uint64_t &diff);

typedef int (*cs_integer_compare_tranverse) (
            const char *buf,
            const uint64_t datum_val,
            const int64_t row_start,
            const int64_t row_count,
            const sql::ObPushdownFilterExecutor *parent,
            ObBitmap &result_bitmap);

typedef int (*cs_integer_bt_tranverse) (
            const char *buf,
            const uint64_t *datums_val,
            const int64_t row_start,
            const int64_t row_count,
            const sql::ObPushdownFilterExecutor *parent,
            ObBitmap &result_bitmap);

typedef int (*cs_integer_bt_tranverse_with_null) (
            const char *buf,
            const uint64_t *datums_val,
            const uint64_t null_replaced_val,
            const int64_t row_start,
            const int64_t row_count,
            const sql::ObPushdownFilterExecutor *parent,
            ObBitmap &result_bitmap);

typedef int (*cs_integer_in_tranverse) (
            const char *buf,
            const bool *filter_vals_valid,
            const uint64_t *filter_vals,
            const int64_t filter_val_cnt,
            const int64_t row_start,
            const int64_t row_count,
            const uint64_t base_val,
            const sql::ObPushdownFilterExecutor *parent,
            ObBitmap &result_bitmap);

typedef int (*cs_integer_in_tranverse_with_null) (
            const char *buf,
            const uint64_t null_replaced_val,
            const bool *filter_vals_valid,
            const uint64_t *filter_vals,
            const int64_t filter_val_cnt,
            const int64_t row_start,
            const int64_t row_count,
            const uint64_t base_val,
            const sql::ObPushdownFilterExecutor *parent,
            ObBitmap &result_bitmap);

typedef void (*cs_dict_val_compare_tranverse) (
            const char *dict_val_buf,
            const uint64_t datum_val,
            const int64_t dict_val_cnt,
            int64_t &matched_ref_cnt,
            sql::ObBitVector *ref_bitset);

typedef void (*cs_dict_val_bt_tranverse) (
            const char *dict_val_buf,
            const uint64_t *datums_val,
            const int64_t dict_val_cnt,
            int64_t &matched_ref_cnt,
            sql::ObBitVector *ref_bitset);

typedef int (*cs_dict_ref_sort_bt_tranverse) (
            const char *dict_ref_buf,
            const uint64_t dict_val_cnt,
            const int64_t *refs_val,
            const int64_t row_start,
            const int64_t row_count,
            const sql::ObPushdownFilterExecutor *parent,
            common::ObBitmap &result_bitmap);

typedef void (*cs_dict_val_in_tranverse) (
            const char *dict_val_buf,
            const uint64_t dict_val_base,
            const bool *vals_valid,
            const uint64_t *vals,
            const int64_t datums_cnt,
            const int64_t dict_val_cnt,
            int64_t &matched_ref_cnt,
            sql::ObBitVector *ref_bitset);

typedef int (*cs_dict_tranverse_ref) (
            const char *dict_ref_buf,
            const int64_t row_start,
            const int64_t row_count,
            const common::ObBitmap *ref_bitmap,
            const sql::ObPushdownFilterExecutor *parent,
            common::ObBitmap &result_bitmap);

template <typename DataType>
struct CSEqualsOp
{
  OB_INLINE static bool apply(DataType a, DataType b) { return a == b; }
};

template <typename DataType>
struct CSNotEqualsOp
{
  OB_INLINE static bool apply(DataType a, DataType b) { return a != b; }
};

template <typename DataType>
struct CSGreaterOp
{
  OB_INLINE static bool apply(DataType a, DataType b) { return a > b; }
};

template <typename DataType>
struct CSLessOp
{
  OB_INLINE static bool apply(DataType a, DataType b) { return a < b; }
};

template <typename DataType>
struct CSGreaterOrEqualsOp
{
  OB_INLINE static bool apply(DataType a, DataType b) { return a >= b; }
};

template <typename DataType>
struct CSLessOrEqualsOp
{
  OB_INLINE static bool apply(DataType a, DataType b) { return a <= b; }
};

template <typename DataType>
struct CSBetweenOp
{
  OB_INLINE static bool apply(DataType a, DataType b, DataType c) { return a >= b && a <= c; }
};

template <int L_IS_SIGNED, int32_t L_WIDTH_TAG, int R_IS_SIGNED, int32_t R_WIDTH_TAG>
class ObCSFilterCommonFunction
{
public:
  OB_INLINE static bool less_than_func(
    const uint64_t lval,
    const int64_t lval_size,
    const uint64_t rval,
    const int64_t rval_size,
    uint64_t &diff)
  {
    // default: lval amd rval has same sign bit
    typedef typename ObEncodingTypeInference<L_IS_SIGNED, L_WIDTH_TAG>::Type LDataType;
    typedef typename ObEncodingTypeInference<R_IS_SIGNED, R_WIDTH_TAG>::Type RDataType;

    const uint64_t valid_lval = lval & INTEGER_MASK_TABLE[lval_size];
    const LDataType cast_lval = (LDataType)valid_lval;
    const uint64_t valid_rval = rval & INTEGER_MASK_TABLE[rval_size];
    const RDataType cast_rval = (RDataType)valid_rval;

    bool bool_ret = (cast_lval < cast_rval);
    if (!bool_ret) {
      diff = (uint64_t)(cast_lval - cast_rval);
    }
    return bool_ret;
  }
};

template <int32_t L_WIDTH_TAG, int32_t R_WIDTH_TAG>
class ObCSFilterCommonFunction<0/*L_IS_SIGNED*/, L_WIDTH_TAG, 1/*R_IS_SIGNED*/, R_WIDTH_TAG>
{
public:
  OB_INLINE static bool less_than_func(
    const uint64_t lval,
    const int64_t lval_size,
    const uint64_t rval,
    const int64_t rval_size,
    uint64_t &diff)
  {
    typedef typename ObEncodingTypeInference<0, L_WIDTH_TAG>::Type LDataType;
    typedef typename ObEncodingTypeInference<1, R_WIDTH_TAG>::Type RDataType;

    const uint64_t valid_lval = lval & INTEGER_MASK_TABLE[lval_size];
    const LDataType cast_lval = (LDataType)valid_lval;
    const uint64_t valid_rval = rval & INTEGER_MASK_TABLE[rval_size];
    const RDataType cast_rval = (RDataType)valid_rval;
    bool bool_ret = false;
    if (cast_rval > 0) {
      bool_ret = (cast_lval < cast_rval);
    } // else {cast_rval <= 0, cast_lval(unsigned) < cast_rval(signed) must be false}
    if (!bool_ret) {
      diff = (uint64_t)(cast_lval - cast_rval);
    }
    return bool_ret;
  }
};

template <int32_t L_WIDTH_TAG, int32_t R_WIDTH_TAG>
class ObCSFilterCommonFunction<1/*L_IS_SIGNED*/, L_WIDTH_TAG, 0/*R_IS_SIGNED*/, R_WIDTH_TAG>
{
public:
  OB_INLINE static bool less_than_func(
    const uint64_t lval,
    const int64_t lval_size,
    const uint64_t rval,
    const int64_t rval_size,
    uint64_t &diff)
  {
    typedef typename ObEncodingTypeInference<1, L_WIDTH_TAG>::Type LDataType;
    typedef typename ObEncodingTypeInference<0, R_WIDTH_TAG>::Type RDataType;

    const uint64_t valid_lval = lval & INTEGER_MASK_TABLE[lval_size];
    const LDataType cast_lval = (LDataType)valid_lval;
    const uint64_t valid_rval = rval & INTEGER_MASK_TABLE[rval_size];
    const RDataType cast_rval = (RDataType)valid_rval;
    bool bool_ret = true;
    if (cast_lval >= 0) {
      bool_ret = (cast_lval < cast_rval);
    } // else {cast_lval < 0, cast_lval(signed) < cast_rval(unsigned) must be true}
    if (!bool_ret) {
      diff = (uint64_t)(cast_lval - cast_rval);
    }
    return bool_ret;
  }
};

template <typename ValDataType, typename Op, bool ExistParent, bool ExistNullBitmap>
class ObCSIntegerFilterOpFunc
{
public:
  static int compare_op_tranverse(const char *buf, const uint64_t datum_val,
    const int64_t row_start, const int64_t row_count,
    const sql::ObPushdownFilterExecutor *parent, ObBitmap &result_bitmap)
  {
    int ret = common::OB_SUCCESS;
    const ValDataType cast_datum_val = *reinterpret_cast<const ValDataType *>(&datum_val);
    const ValDataType *start_pos = reinterpret_cast<const ValDataType *>(buf);
    start_pos += row_start;
    for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
      if (ExistParent && parent->can_skip_filter(i)) {
        // skip
      } else if (ExistNullBitmap && result_bitmap.test(i)) {
        if (OB_FAIL(result_bitmap.set(i, false))) {
          STORAGE_LOG(WARN, "fail to set", KR(ret), K(i), K(row_start));
        }
      } else {
        ValDataType cur_val = *start_pos;
        if (Op::apply(cur_val, cast_datum_val)) {
          if (OB_FAIL(result_bitmap.set(i))) {
            STORAGE_LOG(WARN, "fail to set bitmap", KR(ret), K(i), K(row_start));
          }
        }
      }
      ++start_pos;
    }
    return ret;
  }

  static int between_op_tranverse(const char *buf, const uint64_t *datums_val,
    const int64_t row_start, const int64_t row_count,
    const sql::ObPushdownFilterExecutor *parent, ObBitmap &result_bitmap)
  {
    int ret = common::OB_SUCCESS;
    const ValDataType left_boundary = *reinterpret_cast<const ValDataType *>(datums_val);
    const ValDataType right_boundary = *reinterpret_cast<const ValDataType *>(datums_val + 1);
    const ValDataType *start_pos = reinterpret_cast<const ValDataType *>(buf);
    start_pos += row_start;
    for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
      if (ExistParent && parent->can_skip_filter(i)) {
        // skip
      } else if (ExistNullBitmap && result_bitmap.test(i)) {
        if (OB_FAIL(result_bitmap.set(i, false))) {
          STORAGE_LOG(WARN, "fail to set", KR(ret), K(i), K(row_start));
        }
      } else {
        ValDataType cur_val = *start_pos;
        if ((cur_val >= left_boundary) && (cur_val <= right_boundary)) {
          if (OB_FAIL(result_bitmap.set(i))) {
            STORAGE_LOG(WARN, "fail to set bitmap", KR(ret), K(i), K(row_start));
          }
        }
      }
      ++start_pos;
    }
    return ret;
  }

  static int between_op_tranverse_with_null(const char *buf, const uint64_t *datums_val,
    const uint64_t null_replaced_val, const int64_t row_start, const int64_t row_count,
    const sql::ObPushdownFilterExecutor *parent, ObBitmap &result_bitmap)
  {
    UNUSED(ExistNullBitmap);
    int ret = common::OB_SUCCESS;
    const ValDataType left_boundary = *reinterpret_cast<const ValDataType *>(datums_val);
    const ValDataType right_boundary = *reinterpret_cast<const ValDataType *>(datums_val + 1);
    const ValDataType cast_null_val = *reinterpret_cast<const ValDataType *>(&null_replaced_val);
    const ValDataType *start_pos = reinterpret_cast<const ValDataType *>(buf);
    start_pos += row_start;
    for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
      if (ExistParent && parent->can_skip_filter(i)) {
        // skip
      } else {
        ValDataType cur_val = *start_pos;
        if ((cur_val != cast_null_val) && (cur_val >= left_boundary) && (cur_val <= right_boundary)) {
          if (OB_FAIL(result_bitmap.set(i))) {
            STORAGE_LOG(WARN, "fail to set bitmap", KR(ret), K(i), K(row_start));
          }
        }
      }
      ++start_pos;
    }
    return ret;
  }

  // For in op, if the filter_datum count and row_count are both large, we will create hash_set
  // to optimize the performance
  #define CHECK_USE_HASHSET_FOR_IN_OP(datum_cnt, row_cnt) \
    const int64_t UPPER_FILTER_DATUM_CNT = 170; \
    const int64_t UPPER_ROW_CNT = 1000; \
    const bool use_hash_set = ((datum_cnt >= UPPER_FILTER_DATUM_CNT) && (row_count >= UPPER_ROW_CNT)); \

  static int in_op_tranverse(const char *buf, const bool *filter_vals_valid, const uint64_t *filter_vals,
    const int64_t filter_val_cnt, const int64_t row_start, const int64_t row_count,
    const uint64_t base_val, const sql::ObPushdownFilterExecutor *parent, ObBitmap &result_bitmap)
  {
    int ret = common::OB_SUCCESS;
    const ValDataType *start_pos = reinterpret_cast<const ValDataType *>(buf);
    start_pos += row_start;

    CHECK_USE_HASHSET_FOR_IN_OP(filter_val_cnt, row_count);
    if (use_hash_set) {
      common::hash::ObHashSet<ValDataType> datums_val;
      if (OB_FAIL(datums_val.create(filter_val_cnt))) {
        STORAGE_LOG(WARN, "fail to create hashset", KR(ret), K(filter_val_cnt));
      }

      for (int64_t i = 0; OB_SUCC(ret) && (i < filter_val_cnt); ++i) {
        if (filter_vals_valid[i]) {
          const uint64_t datum_val = filter_vals[i] - base_val;
          const ValDataType cast_datum_val = *reinterpret_cast<const ValDataType *>(&datum_val);
          if (OB_FAIL(datums_val.set_refactored(cast_datum_val))) {
            STORAGE_LOG(WARN, "fail to set refactored", KR(ret), K(cast_datum_val), K(filter_val_cnt));
          }
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
        if (ExistParent && parent->can_skip_filter(i)) {
          // skip
        } else if (ExistNullBitmap && result_bitmap.test(i)) {
          if (OB_FAIL(result_bitmap.set(i, false))) {
            STORAGE_LOG(WARN, "fail to set", KR(ret), K(i), K(row_start));
          }
        } else {
          ValDataType cur_val = *start_pos;
          if (datums_val.exist_refactored(cur_val) == OB_HASH_EXIST) {
            if (OB_FAIL(result_bitmap.set(i))) {
              STORAGE_LOG(WARN, "fail to set bitmap", KR(ret), K(i), K(row_start));
            }
          }
        }
        ++start_pos;
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
        if (ExistParent && parent->can_skip_filter(i)) {
          // skip
        } else if (ExistNullBitmap && result_bitmap.test(i)) {
          if (OB_FAIL(result_bitmap.set(i, false))) {
            STORAGE_LOG(WARN, "fail to set", KR(ret), K(i), K(row_start));
          }
        } else {
          ValDataType cur_val = *start_pos;
          for (int64_t j = 0; OB_SUCC(ret) && j < filter_val_cnt; ++j) {
            if (filter_vals_valid[j]) {
              const uint64_t datum_val = filter_vals[j] - base_val;
              const ValDataType cast_datum_val = *reinterpret_cast<const ValDataType *>(&datum_val);
              if (cur_val == cast_datum_val) {
                if (OB_FAIL(result_bitmap.set(i))) {
                  STORAGE_LOG(WARN, "fail to set bitmap", KR(ret), K(i), K(row_start));
                }
                break;
              }
            }
          }
        }
        ++start_pos;
      }
    }
    return ret;
  }

  static int in_op_tranverse_with_null(const char *buf, const uint64_t null_replaced_val,
    const bool *filter_vals_valid, const uint64_t *filter_vals, const int64_t filter_val_cnt,
    const int64_t row_start, const int64_t row_count, const uint64_t base_val,
    const sql::ObPushdownFilterExecutor *parent, ObBitmap &result_bitmap)
  {
    UNUSED(ExistNullBitmap);
    int ret = common::OB_SUCCESS;
    const ValDataType cast_null_val = *reinterpret_cast<const ValDataType *>(&null_replaced_val);
    const ValDataType *start_pos = reinterpret_cast<const ValDataType *>(buf);
    start_pos += row_start;

    CHECK_USE_HASHSET_FOR_IN_OP(filter_val_cnt, row_count);
    if (use_hash_set) {
      common::hash::ObHashSet<uint64_t> datums_val;
      if (OB_FAIL(datums_val.create(filter_val_cnt))) {
        STORAGE_LOG(WARN, "fail to create hash set", KR(ret), K(filter_val_cnt));
      }

      for (int64_t i = 0; OB_SUCC(ret) && (i < filter_val_cnt); ++i) {
        if (filter_vals_valid[i]) {
          const uint64_t datum_val = filter_vals[i] - base_val;
          const ValDataType cast_datum_val = *reinterpret_cast<const ValDataType *>(&datum_val);
          if (OB_FAIL(datums_val.set_refactored(cast_datum_val))) {
            STORAGE_LOG(WARN, "fail to set refactored", KR(ret), K(cast_datum_val), K(filter_val_cnt));
          }
        }
      }

      ValDataType cur_val = 0;
      for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
        if (ExistParent && parent->can_skip_filter(i)) {
          // skip
        } else if ((cur_val = *start_pos) == cast_null_val) {
          // skip null
        } else if (datums_val.exist_refactored(cur_val) == OB_HASH_EXIST) {
          if (OB_FAIL(result_bitmap.set(i))) {
            STORAGE_LOG(WARN, "fail to set bitmap", KR(ret), K(i), K(row_start));
          }
        }
        ++start_pos;
      }
    } else {
      ValDataType cur_val = 0;
      for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
        if (ExistParent && parent->can_skip_filter(i)) {
          // skip
        } else if ((cur_val = *start_pos) == cast_null_val) {
          // skip null
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < filter_val_cnt; ++j) {
            if (filter_vals_valid[j]) {
              const uint64_t datum_val = filter_vals[j] - base_val;
              const ValDataType cast_datum_val = *reinterpret_cast<const ValDataType *>(&datum_val);
              if (cur_val == cast_datum_val) {
                if (OB_FAIL(result_bitmap.set(i))) {
                  STORAGE_LOG(WARN, "fail to set bitmap", KR(ret), K(i), K(row_start));
                }
                break;
              }
            }
          }
        }
        ++start_pos;
      }
    }
    return ret;
  }
};

template <typename ValDataType, typename Op, bool ExistParent>
class ObCSDictFilterOpFunc
{
public:
  static void dict_val_compare_tranverse(const char *dict_val_buf,
    const uint64_t datum_val, const int64_t dict_val_cnt,
    int64_t &matched_ref_cnt, sql::ObBitVector *ref_bitset)
  {
    UNUSED(ExistParent);
    const ValDataType cast_datum_val = *reinterpret_cast<const ValDataType *>(&datum_val);
    const ValDataType *start_pos = reinterpret_cast<const ValDataType *>(dict_val_buf);
    for (int64_t i = 0; i < dict_val_cnt; ++i) {
      ValDataType cur_val = *start_pos;
      if (Op::apply(cur_val, cast_datum_val)) {
        ref_bitset->set(i);
        ++matched_ref_cnt;
      }
      ++start_pos;
    }
  }

  static void dict_val_bt_tranverse(const char *dict_val_buf,
    const uint64_t *datums_val, const int64_t dict_val_cnt,
    int64_t &matched_ref_cnt, sql::ObBitVector *ref_bitset)
  {
    UNUSED(ExistParent);
    const ValDataType cast_left_boundary = *reinterpret_cast<const ValDataType *>(datums_val);
    const ValDataType cast_right_boundary = *reinterpret_cast<const ValDataType *>(datums_val + 1);
    const ValDataType *start_pos = reinterpret_cast<const ValDataType *>(dict_val_buf);
    for (int64_t i = 0; i < dict_val_cnt; ++i) {
      ValDataType cur_val = *start_pos;
      if ((cur_val >= cast_left_boundary) && (cur_val <= cast_right_boundary)) {
        ref_bitset->set(i);
        ++matched_ref_cnt;
      }
      ++start_pos;
    }
  }

  static int dict_ref_sort_bt_tranverse(const char *dict_ref_buf, const uint64_t dict_val_cnt,
    const int64_t *refs_val, const int64_t row_start, const int64_t row_count,
    const sql::ObPushdownFilterExecutor *parent, common::ObBitmap &result_bitmap)
  {
    int ret = OB_SUCCESS;
    const ValDataType cast_left_inclusive = *reinterpret_cast<const ValDataType *>(refs_val);
    const ValDataType cast_right_exclusive = *reinterpret_cast<const ValDataType *>(refs_val + 1);
    const ValDataType *start_pos = reinterpret_cast<const ValDataType *>(dict_ref_buf);
    start_pos += row_start;
    for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
      if (ExistParent && parent->can_skip_filter(i)) {
        // skip
      } else {
        ValDataType cur_val = *start_pos;
        if ((cur_val >= cast_left_inclusive) && (cur_val < cast_right_exclusive) && (cur_val < dict_val_cnt)) {
          if (OB_FAIL(result_bitmap.set(i))) {
            STORAGE_LOG(WARN, "fail to set bitmap", KR(ret), K(i), K(row_start));
          }
        }
      }
      ++start_pos;
    }
    return ret;
  }

  static void dict_val_in_tranverse(const char *dict_val_buf, const uint64_t dict_val_base,
    const bool *vals_valid, const uint64_t *vals, const int64_t datums_cnt,
    const int64_t dict_val_cnt, int64_t &matched_ref_cnt, sql::ObBitVector *ref_bitset)
  {
    UNUSED(ExistParent);
    const ValDataType *start_pos = reinterpret_cast<const ValDataType *>(dict_val_buf);
    for (int64_t i = 0; i < dict_val_cnt; ++i) {
      ValDataType cur_val = *start_pos;
      for (int64_t j = 0; j < datums_cnt; ++j) {
        if (vals_valid[j]) {
          const ValDataType cast_ref_val = (ValDataType)(vals[j] - dict_val_base);
          if (cur_val == cast_ref_val) {
            ref_bitset->set(i);
            ++matched_ref_cnt;
            break;
          }
        }
      }
      ++start_pos;
    }
  }

  static int dict_tranverse_ref(const char *dict_ref_buf, const int64_t row_start,
    const int64_t row_count, const common::ObBitmap *ref_bitmap,
    const sql::ObPushdownFilterExecutor *parent, common::ObBitmap &result_bitmap)
  {
    int ret = OB_SUCCESS;
    const ValDataType *start_pos = reinterpret_cast<const ValDataType *>(dict_ref_buf);
    start_pos += row_start;
    for (int64_t offset = 0; OB_SUCC(ret) && (offset < row_count); offset++) {
      if (ExistParent && parent->can_skip_filter(offset)) {
        // skip
      } else {
        ValDataType cur_ref = *start_pos;
        if (ref_bitmap->test(cur_ref) && OB_FAIL(result_bitmap.set(offset))) {
          STORAGE_LOG(WARN, "fail to set result bitmap", KR(ret), K(offset), K(cur_ref), K(row_start));
        }
      }
      ++start_pos;
    }
    return ret;
  }
};

template <bool EXIST_NULL_BITMAP, bool EXIST_PARENT, int32_t VAL_WIDTH_TAG>
struct ObCSIntegerFilterFuncProducer
{
  static cs_integer_compare_tranverse produce_integer_cmp_tranverse(
    const sql::ObWhiteFilterOperatorType op_type)
  {
    typedef typename ObEncodingTypeInference<false, VAL_WIDTH_TAG>::Type ValDataType;
    cs_integer_compare_tranverse func = nullptr;
    switch (op_type) {
      case sql::ObWhiteFilterOperatorType::WHITE_OP_EQ:
        func = ObCSIntegerFilterOpFunc<ValDataType, CSEqualsOp<ValDataType>, EXIST_PARENT, EXIST_NULL_BITMAP>::compare_op_tranverse;
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_LE:
        func = ObCSIntegerFilterOpFunc<ValDataType, CSLessOrEqualsOp<ValDataType>, EXIST_PARENT, EXIST_NULL_BITMAP>::compare_op_tranverse;
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_LT:
        func = ObCSIntegerFilterOpFunc<ValDataType, CSLessOp<ValDataType>, EXIST_PARENT, EXIST_NULL_BITMAP>::compare_op_tranverse;
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_GE:
        func = ObCSIntegerFilterOpFunc<ValDataType, CSGreaterOrEqualsOp<ValDataType>, EXIST_PARENT, EXIST_NULL_BITMAP>::compare_op_tranverse;
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_GT:
        func = ObCSIntegerFilterOpFunc<ValDataType, CSGreaterOp<ValDataType>, EXIST_PARENT, EXIST_NULL_BITMAP>::compare_op_tranverse;
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_NE:
        func = ObCSIntegerFilterOpFunc<ValDataType, CSNotEqualsOp<ValDataType>, EXIST_PARENT, EXIST_NULL_BITMAP>::compare_op_tranverse;
        break;
      default:
        func = nullptr;
        break;
    }
    return func;
  }

  static cs_integer_bt_tranverse produce_integer_bt_tranverse()
  {
    typedef typename ObEncodingTypeInference<false, VAL_WIDTH_TAG>::Type ValDataType;
    cs_integer_bt_tranverse func = nullptr;
    func = ObCSIntegerFilterOpFunc<ValDataType, CSBetweenOp<ValDataType>, EXIST_PARENT, EXIST_NULL_BITMAP>::between_op_tranverse;
    return func;
  }

  static cs_integer_in_tranverse produce_integer_in_tranverse()
  {
    typedef typename ObEncodingTypeInference<false, VAL_WIDTH_TAG>::Type ValDataType;
    cs_integer_in_tranverse func = nullptr;
    func = ObCSIntegerFilterOpFunc<ValDataType, CSEqualsOp<ValDataType>, EXIST_PARENT, EXIST_NULL_BITMAP>::in_op_tranverse;
    return func;
  }
};

template <bool EXIST_PARENT, int32_t VAL_WIDTH_TAG>
struct ObCSIntegerFilterFuncProducerWithNull
{
  static cs_integer_bt_tranverse_with_null produce_integer_bt_tranverse_with_null()
  {
    typedef typename ObEncodingTypeInference<false, VAL_WIDTH_TAG>::Type ValDataType;
    cs_integer_bt_tranverse_with_null func = nullptr;
    func = ObCSIntegerFilterOpFunc<ValDataType, CSBetweenOp<ValDataType>, EXIST_PARENT, false>::between_op_tranverse_with_null;
    return func;
  }

  static cs_integer_in_tranverse_with_null produce_integer_in_tranverse_with_null()
  {
    typedef typename ObEncodingTypeInference<false, VAL_WIDTH_TAG>::Type ValDataType;
    cs_integer_in_tranverse_with_null func = nullptr;
    func = ObCSIntegerFilterOpFunc<ValDataType, CSEqualsOp<ValDataType>, EXIST_PARENT, false>::in_op_tranverse_with_null;
    return func;
  }
};

template <bool EXIST_PARENT, int32_t VAL_WIDTH_TAG>
struct ObCSDictRefFilterFuncProducer
{
  static cs_dict_ref_sort_bt_tranverse produce_dict_ref_sort_bt_tranverse()
  {
    typedef typename ObEncodingTypeInference<false, VAL_WIDTH_TAG>::Type ValDataType;
    cs_dict_ref_sort_bt_tranverse func = nullptr;
    func = ObCSDictFilterOpFunc<ValDataType, CSBetweenOp<ValDataType>, EXIST_PARENT>::dict_ref_sort_bt_tranverse;
    return func;
  }

  static cs_dict_tranverse_ref produce_dict_tranverse_ref()
  {
    typedef typename ObEncodingTypeInference<false, VAL_WIDTH_TAG>::Type ValDataType;
    cs_dict_tranverse_ref func = nullptr;
    func = ObCSDictFilterOpFunc<ValDataType, CSEqualsOp<ValDataType>, EXIST_PARENT>::dict_tranverse_ref;
    return func;
  }
};

template <int32_t VAL_WIDTH_TAG>
struct ObCSDictFilterFuncProducer
{
  OB_INLINE static cs_dict_val_compare_tranverse produce_dict_val_cmp_tranverse(
    const sql::ObWhiteFilterOperatorType op_type)
  {
    typedef typename ObEncodingTypeInference<false, VAL_WIDTH_TAG>::Type ValDataType;
    cs_dict_val_compare_tranverse func = nullptr;
    switch (op_type) {
      case sql::ObWhiteFilterOperatorType::WHITE_OP_EQ:
        func = ObCSDictFilterOpFunc<ValDataType, CSEqualsOp<ValDataType>, false>::dict_val_compare_tranverse;
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_LE:
        func = ObCSDictFilterOpFunc<ValDataType, CSLessOrEqualsOp<ValDataType>, false>::dict_val_compare_tranverse;
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_LT:
        func = ObCSDictFilterOpFunc<ValDataType, CSLessOp<ValDataType>, false>::dict_val_compare_tranverse;
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_GE:
        func = ObCSDictFilterOpFunc<ValDataType, CSGreaterOrEqualsOp<ValDataType>, false>::dict_val_compare_tranverse;
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_GT:
        func = ObCSDictFilterOpFunc<ValDataType, CSGreaterOp<ValDataType>, false>::dict_val_compare_tranverse;
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_NE:
        func = ObCSDictFilterOpFunc<ValDataType, CSNotEqualsOp<ValDataType>, false>::dict_val_compare_tranverse;
        break;
      default:
        func = nullptr;
        break;
    }
    return func;
  }

  OB_INLINE static cs_dict_val_bt_tranverse produce_dict_val_bt_tranverse()
  {
    typedef typename ObEncodingTypeInference<false, VAL_WIDTH_TAG>::Type ValDataType;
    cs_dict_val_bt_tranverse func = nullptr;
    func = ObCSDictFilterOpFunc<ValDataType, CSBetweenOp<ValDataType>, false>::dict_val_bt_tranverse;
    return func;
  }

  OB_INLINE static cs_dict_val_in_tranverse produce_dict_val_in_tranverse()
  {
    typedef typename ObEncodingTypeInference<false, VAL_WIDTH_TAG>::Type ValDataType;
    cs_dict_val_in_tranverse func = nullptr;
    func = ObCSDictFilterOpFunc<ValDataType, CSEqualsOp<ValDataType>, false>::dict_val_in_tranverse;
    return func;
  }
};

class ObCSFilterFunctionFactory {
public:
  static ObCSFilterFunctionFactory &instance();

  OB_INLINE int integer_compare_tranverse(const char *buf, const uint32_t val_width_size,
    const uint64_t datum_val, const int64_t row_start, const int64_t row_count,
    const bool exist_null_bitmap, const sql::ObWhiteFilterOperatorType op_type,
    const sql::ObPushdownFilterExecutor *parent, ObBitmap &result_bitmap)
  {
    int32_t val_width_tag = get_value_len_tag_map()[val_width_size];
    const bool exist_parent = (nullptr != parent);
    cs_integer_compare_tranverse func = integer_cmp_funcs_[exist_null_bitmap][exist_parent][val_width_tag][op_type];
    return func(buf, datum_val, row_start, row_count, parent, result_bitmap);
  }

  OB_INLINE int integer_bt_tranverse(const char *buf, const uint32_t val_width_size,
    const uint64_t *datums_val, const int64_t row_start, const int64_t row_count,
    const bool exist_null_bitmap, const sql::ObPushdownFilterExecutor *parent,
    ObBitmap &result_bitmap)
  {
    int32_t val_width_tag = get_value_len_tag_map()[val_width_size];
    const bool exist_parent = (nullptr != parent);
    cs_integer_bt_tranverse func = integer_bt_funcs_[exist_null_bitmap][exist_parent][val_width_tag];
    return func(buf, datums_val, row_start, row_count, parent, result_bitmap);
  }


  OB_INLINE int integer_bt_tranverse_with_null(const char *buf, const uint32_t val_width_size,
    const uint64_t *datums_val, const uint64_t null_replaced_val, const int64_t row_start,
    const int64_t row_count, const sql::ObPushdownFilterExecutor *parent, ObBitmap &result_bitmap)
  {
    int32_t val_width_tag = get_value_len_tag_map()[val_width_size];
    const bool exist_parent = (nullptr != parent);
    cs_integer_bt_tranverse_with_null func = integer_bt_null_funcs_[exist_parent][val_width_tag];
    return func(buf, datums_val, null_replaced_val, row_start, row_count, parent, result_bitmap);
  }

  OB_INLINE int integer_in_tranverse(const char *buf, const uint32_t val_width_size,
    const bool *filter_vals_valid, const uint64_t *filter_vals, const int64_t filter_val_cnt,
    const int64_t row_start, const int64_t row_count, const uint64_t base_val, const bool exist_null_bitmap,
    const sql::ObPushdownFilterExecutor *parent, ObBitmap &result_bitmap)
  {
    int32_t val_width_tag = get_value_len_tag_map()[val_width_size];
    const bool exist_parent = (nullptr != parent);
    cs_integer_in_tranverse func = integer_in_funcs_[exist_null_bitmap][exist_parent][val_width_tag];
    return func(buf, filter_vals_valid, filter_vals, filter_val_cnt,
        row_start, row_count, base_val, parent, result_bitmap);
  }

  OB_INLINE int integer_in_tranverse_with_null(const char *buf, const uint32_t val_width_size,
    const uint64_t null_replaced_val, const bool *filter_vals_valid, const uint64_t *filter_vals,
    const int64_t filter_val_cnt, const int64_t row_start, const int64_t row_count, const uint64_t base_val,
    const sql::ObPushdownFilterExecutor *parent, ObBitmap &result_bitmap)
  {
    int32_t val_width_tag = get_value_len_tag_map()[val_width_size];
    const bool exist_parent = (nullptr != parent);
    cs_integer_in_tranverse_with_null func = integer_in_null_funcs_[exist_parent][val_width_tag];
    return func(buf, null_replaced_val, filter_vals_valid, filter_vals, filter_val_cnt,
        row_start, row_count, base_val, parent, result_bitmap);
  }

  OB_INLINE void dict_val_compare_tranverse(const char *dict_val_buf, const uint32_t val_width_size,
    const uint64_t datum_val, const int64_t dict_val_cnt, const sql::ObWhiteFilterOperatorType op_type,
    int64_t &matched_ref_cnt, sql::ObBitVector *ref_bitset)
  {
    const int32_t val_width_tag = get_value_len_tag_map()[val_width_size];
    cs_dict_val_compare_tranverse func = dict_val_cmp_funcs_[val_width_tag][op_type];
    func(dict_val_buf, datum_val, dict_val_cnt, matched_ref_cnt, ref_bitset);
  }

  OB_INLINE void dict_val_bt_tranverse(const char *dict_val_buf, const uint32_t val_width_size,
    const uint64_t *datums_val, const int64_t dict_val_cnt, int64_t &matched_ref_cnt,
    sql::ObBitVector *ref_bitset)
  {
    const int32_t val_width_tag = get_value_len_tag_map()[val_width_size];
    cs_dict_val_bt_tranverse func = dict_val_bt_funcs_[val_width_tag];
    func(dict_val_buf, datums_val, dict_val_cnt, matched_ref_cnt, ref_bitset);
  }

  OB_INLINE int dict_ref_sort_bt_tranverse(const char *dict_ref_buf,
    const uint64_t dict_val_cnt, const int64_t *refs_val, const int64_t row_start,
    const int64_t row_count, const sql::ObPushdownFilterExecutor *parent,
    const uint32_t ref_width_size, common::ObBitmap &result_bitmap)
  {
    const int32_t ref_width_tag = get_value_len_tag_map()[ref_width_size];
    const bool exist_parent = (nullptr != parent);
    cs_dict_ref_sort_bt_tranverse func = dict_ref_sort_bt_funcs_[exist_parent][ref_width_tag];
    return func(dict_ref_buf, dict_val_cnt, refs_val, row_start, row_count, parent, result_bitmap);
  }

  OB_INLINE void dict_val_in_tranverse(const char *dict_val_buf, const uint32_t val_width_size,
    const uint64_t dict_val_base, const bool *vals_valid, const uint64_t *vals,
    const int64_t datums_cnt, const int64_t dict_val_cnt, int64_t &matched_ref_cnt, sql::ObBitVector *ref_bitset)
  {
    const int32_t val_width_tag = get_value_len_tag_map()[val_width_size];
    cs_dict_val_in_tranverse func = dict_val_in_funcs_[val_width_tag];
    func(dict_val_buf, dict_val_base, vals_valid, vals, datums_cnt, dict_val_cnt, matched_ref_cnt, ref_bitset);
  }

  OB_INLINE int dict_tranverse_ref(const char *dict_ref_buf, const uint32_t ref_width_size,
    const int64_t row_start, const int64_t row_count, const common::ObBitmap *ref_bitmap,
    const sql::ObPushdownFilterExecutor *parent, common::ObBitmap &result_bitmap)
  {
    const int32_t ref_width_tag = get_value_len_tag_map()[ref_width_size];
    const bool exist_parent = (nullptr != parent);
    cs_dict_tranverse_ref func = dict_scan_ref_funcs_[exist_parent][ref_width_tag];
    return func(dict_ref_buf, row_start, row_count, ref_bitmap, parent, result_bitmap);
  }

private:
  ObCSFilterFunctionFactory();
  ~ObCSFilterFunctionFactory() = default;
  DISALLOW_COPY_AND_ASSIGN(ObCSFilterFunctionFactory);
private:
  ObMultiDimArray_T<cs_integer_compare_tranverse, 2/*exist_null_bitmap*/, 2/*exist_parent*/, 4/*val_tag*/, 6/*op*/> integer_cmp_funcs_;
  ObMultiDimArray_T<cs_integer_bt_tranverse, 2/*exist_null_bitmap*/, 2/*exist_parent*/, 4/*val_tag*/> integer_bt_funcs_;
  ObMultiDimArray_T<cs_integer_bt_tranverse_with_null, 2/*exist_parent*/, 4/*val_tag*/> integer_bt_null_funcs_;
  ObMultiDimArray_T<cs_integer_in_tranverse, 2/*exist_null_bitmap*/, 2/*exist_parent*/, 4/*val_tag*/> integer_in_funcs_;
  ObMultiDimArray_T<cs_integer_in_tranverse_with_null, 2/*exist_parent*/, 4/*val_tag*/> integer_in_null_funcs_;

  ObMultiDimArray_T<cs_dict_val_compare_tranverse, 4/*val_tag*/, 6/*op*/> dict_val_cmp_funcs_;
  ObMultiDimArray_T<cs_dict_val_bt_tranverse, 4/*val_tag*/> dict_val_bt_funcs_;
  ObMultiDimArray_T<cs_dict_ref_sort_bt_tranverse, 2/*exist_parent*/, 4/*val_tag*/> dict_ref_sort_bt_funcs_;
  ObMultiDimArray_T<cs_dict_val_in_tranverse, 4/*val_tag*/> dict_val_in_funcs_;
  ObMultiDimArray_T<cs_dict_tranverse_ref, 2/*exist_parent*/, 4/*val_tag*/> dict_scan_ref_funcs_;
};

class ObCSDecodingUtil
{
public:
  static ObMultiDimArray_T<cs_filter_is_less_than, 2/*l_signed*/, 4/*l_tag*/, 2/*r_signed*/, 4/*r_tag*/> less_than_funcs;

public:
  static OB_INLINE bool is_signed_object_type(const ObObjType &col_type)
  {
    bool is_signed = false;
    switch(ob_obj_type_class(col_type)) {
      case ObIntTC:
      case ObDateTimeTC:
      case ObDateTC:
      case ObTimeTC:
      case ObDecimalIntTC: {
        is_signed = true;
        break;
      }
      default: {
        break;
      }
    }
    return is_signed;
  }

  static OB_INLINE bool test_bit(const char *bitmap, const int64_t idx)
  {
    return ((*(uint8_t *)(bitmap + idx/8)) & (1 << (7 - idx%8))) > 0;
  }

  static OB_INLINE bool can_convert_to_integer(const ObObjType col_type, bool &is_signed_data)
  {
    bool can_convert = false;
    switch(ob_obj_type_class(col_type)) {
      case ObIntTC:
      case ObDateTimeTC:
      case ObDateTC:
      case ObTimeTC:
      case ObDecimalIntTC: {
        can_convert = true;
        is_signed_data = true;
        break;
      }
      case ObUIntTC:
      case ObYearTC: {
        can_convert = true;
        is_signed_data = false;
        break;
      }
      default: {
        break;
      }
    }
    return can_convert;
  };

  static OB_INLINE uint64_t get_uint64_from_buf_by_len(const char *buf, const int64_t len)
  {
    uint64_t result = 0;
    ENCODING_ADAPT_MEMCPY(&result, buf, len);
    return result;
  }

  static OB_INLINE bool check_datum_not_over_8bytes(const ObObjType obj_type,
                                                    const ObDatum &datum,
                                                    bool &is_signed,
                                                    uint64_t &value,
                                                    int64_t &value_size)
  {
    bool not_over = true;
    if (ob_obj_type_class(obj_type) == ObDecimalIntTC) {
      const int64_t int64_min = INT64_MIN;
      const int64_t int64_max = INT64_MAX;
      decint_cmp_fp cmp = wide::ObDecimalIntCmpSet::get_decint_decint_cmp_func(datum.len_, sizeof(int64_t));
      value_size = sizeof(int64_t);
      is_signed = true;
      if (cmp(datum.get_decimal_int(), (ObDecimalInt*)&int64_min) < 0) {
        not_over = false;
        value = int64_min;  //cast
      } else if (cmp(datum.get_decimal_int(), (ObDecimalInt*)&int64_max) > 0) {
        not_over = false;
        value = int64_max; // cast
      } else {
        value = datum.len_ > sizeof(int32_t) ? datum.get_decimal_int64() : datum.get_decimal_int32();
      }
    } else {
      is_signed = is_signed_object_type(obj_type);
      value_size = get_type_size_map()[obj_type];
      value = get_uint64_from_buf_by_len(datum.ptr_, value_size);
    }

    return not_over;
  }
  static OB_INLINE void get_datum_sign_and_size(const ObObjType obj_type,
                                                const ObDatum &datum,
                                                bool &is_signed,
                                                uint64_t &value,
                                                int64_t &value_size)
  {
    if (ob_obj_type_class(obj_type) == ObDecimalIntTC) {
      value_size = sizeof(int64_t);
      is_signed = true;
      value = datum.len_ > sizeof(int32_t) ? datum.get_decimal_int64() : datum.get_decimal_int32();
    } else {
      is_signed = is_signed_object_type(obj_type);
      value_size = get_type_size_map()[obj_type];
      value = get_uint64_from_buf_by_len(datum.ptr_, value_size);
    }
  }
  static OB_INLINE bool is_less_than(const uint64_t lval, const int64_t lval_size, const bool is_lval_signed,
                                     const uint64_t rval, const int64_t rval_size, const bool is_rval_signed)
  {
    int32_t l_width_tag = get_value_len_tag_map()[lval_size];
    int32_t r_width_tag = get_value_len_tag_map()[rval_size];
    cs_filter_is_less_than func = less_than_funcs[is_lval_signed][l_width_tag][is_rval_signed][r_width_tag];
    uint64_t diff = 0;
    return func(lval, lval_size, rval, rval_size, diff);
  }

  static OB_INLINE bool is_less_than_with_diff(const uint64_t lval, const int64_t lval_size, const bool is_lval_signed,
    const uint64_t rval, const int64_t rval_size, const bool is_rval_signed, uint64_t &diff)
  {
    int32_t l_width_tag = get_value_len_tag_map()[lval_size];
    int32_t r_width_tag = get_value_len_tag_map()[rval_size];
    cs_filter_is_less_than func = less_than_funcs[is_lval_signed][l_width_tag][is_rval_signed][r_width_tag];
    return func(lval, lval_size, rval, rval_size, diff);
  }

};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif
