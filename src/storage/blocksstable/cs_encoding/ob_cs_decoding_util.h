
/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
#include "lib/container/ob_bit_simd.h"

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
            ObBitmap &result_bitmap,
            const sql::ObWhiteFilterExecutor *filter);

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
            ObBitmap &result_bitmap,
            const sql::ObWhiteFilterExecutor *filter);

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

typedef void (*cs_dict_ref_sort_bt_tranverse) (
            const char *dict_ref_buf,
            const uint64_t dict_val_cnt,
            const int64_t *refs_val,
            const int64_t row_start,
            const int64_t row_count,
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

typedef void (*cs_dict_tranverse_ref) (
            const char *dict_ref_buf,
            const int64_t row_start,
            const int64_t row_count,
            const common::ObBitmap *ref_bitmap,
            common::ObBitmap &result_bitmap);

typedef int (*cs_dict_set_bitmap_with_bitset) (
            const char *ref_buf,
            sql::ObBitVector *ref_bitset,
            const int64_t ref_bitset_size,
            const int64_t row_start,
            const int64_t row_cnt,
            ObBitmap &result_bitmap);

typedef int (*cs_dict_set_bitmap_with_bitset_const) (
            const char *exception_row_id_buf,
            const char *exception_ref_buf,
            const int64_t exception_cnt,
            sql::ObBitVector *ref_bitset,
            const int64_t ref_bitset_size,
            const int64_t row_start,
            const int64_t row_cnt,
            ObBitmap &result_bitmap);

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
    const ValDataType *__restrict__ val_arr = reinterpret_cast<const ValDataType *>(buf) + row_start;
    uint8_t *__restrict__ result_data = result_bitmap.get_data();
    for (int64_t i = 0; i < row_count; ++i) {
      if constexpr (ExistNullBitmap) {
        result_data[i] = (result_data[i] ^ 1) & static_cast<uint8_t>(Op::apply(val_arr[i], cast_datum_val));
      } else {
        result_data[i] = static_cast<uint8_t>(Op::apply(val_arr[i], cast_datum_val));
      }
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
    const ValDataType *__restrict__ val_arr = reinterpret_cast<const ValDataType *>(buf) + row_start;
    uint8_t *__restrict__ result_data = result_bitmap.get_data();
    const ValDataType delta = right_boundary - left_boundary;
    for (int64_t i = 0; i < row_count; ++i) {
      if constexpr (ExistNullBitmap) {
        result_data[i] = (result_data[i] ^ 1) & static_cast<uint8_t>(static_cast<ValDataType>(val_arr[i] - left_boundary) <= delta);
      } else {
        result_data[i] = static_cast<uint8_t>(static_cast<ValDataType>(val_arr[i] - left_boundary) <= delta);
      }
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
    const ValDataType *__restrict__ val_arr = reinterpret_cast<const ValDataType *>(buf) + row_start;
    uint8_t *__restrict__ result_data = result_bitmap.get_data();
    const ValDataType delta = right_boundary - left_boundary;
    const bool null_in_range = static_cast<ValDataType>(cast_null_val - left_boundary) <= delta;
    if (!null_in_range) {
      for (int64_t i = 0; i < row_count; ++i) {
        result_data[i] = static_cast<uint8_t>(static_cast<ValDataType>(val_arr[i] - left_boundary) <= delta);
      }
    } else {
      for (int64_t i = 0; i < row_count; ++i) {
        const ValDataType cur_val = val_arr[i];
        result_data[i] =
          static_cast<uint8_t>(cur_val != cast_null_val) &
          static_cast<uint8_t>(static_cast<ValDataType>(cur_val - left_boundary) <= delta);
      }
    }
    return ret;
  }

  static const int64_t MAX_STACK_BYTES = 256;
  static const int64_t MAX_STACK_COUNT = MAX_STACK_BYTES / sizeof(ValDataType);

  // For in op, if the filter_datum count and row_count are both large, we will create hash_set
  // to optimize the performance
  #define CHECK_USE_HASHSET_FOR_IN_OP(datum_cnt, row_cnt) \
    const int64_t UPPER_FILTER_DATUM_CNT = 170; \
    const int64_t UPPER_ROW_CNT = 1000; \
    const bool use_hash_set = ((datum_cnt >= UPPER_FILTER_DATUM_CNT) && (row_count >= UPPER_ROW_CNT)); \

  static int in_op_tranverse(const char *buf, const bool *filter_vals_valid, const uint64_t *filter_vals,
    const int64_t filter_val_cnt, const int64_t row_start, const int64_t row_count,
    const uint64_t base_val, const sql::ObPushdownFilterExecutor *parent, ObBitmap &result_bitmap, const sql::ObWhiteFilterExecutor *filter)
  {
    int ret = common::OB_SUCCESS;
    const ValDataType *__restrict__ val_arr = reinterpret_cast<const ValDataType *>(buf) + row_start;
    uint8_t *__restrict__ result_data = result_bitmap.get_data();

    CHECK_USE_HASHSET_FOR_IN_OP(filter_val_cnt, row_count);
    if (use_hash_set) {
      if (filter->is_filter_dynamic_node()) {
        sql::ObWhiteFilterSmallHashSet datums_val;
        if (OB_FAIL(datums_val.create(filter_val_cnt, static_cast<const sql::ObDynamicFilterExecutor *>(filter)->hash_func_))) {
          STORAGE_LOG(WARN, "fail to create small hash set", KR(ret), K(filter_val_cnt));
        }

        ObStorageDatum cast_datum_val;
        for (int64_t i = 0; OB_SUCC(ret) && (i < filter_val_cnt); ++i) {
          if (filter_vals_valid[i]) {
            const uint64_t datum_val = filter_vals[i] - base_val;
            cast_datum_val.set_uint(datum_val);
            if (OB_FAIL(datums_val.insert_datum(cast_datum_val))) {
              STORAGE_LOG(WARN, "fail to insert datum", KR(ret), K(cast_datum_val), K(filter_val_cnt));
            }
          }
        }
        ObStorageDatum cast_cur_val;
        for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
          if (ExistParent && parent->can_skip_filter(i)) {
            // skip
          } else if (ExistNullBitmap && result_bitmap.test(i)) {
            if (OB_FAIL(result_bitmap.set(i, false))) {
              STORAGE_LOG(WARN, "fail to set", KR(ret), K(i), K(row_start));
            }
          } else {
            const ValDataType cur_val = val_arr[i];
            cast_cur_val.set_uint(cur_val);
            bool is_exist;
            if (OB_FAIL(datums_val.exist_datum(cast_cur_val, is_exist))) {
              STORAGE_LOG(WARN, "fail to search datum in small set", KR(ret), K(i), K(cast_cur_val));
            } else if (is_exist && OB_FAIL(result_bitmap.set(i))) {
              STORAGE_LOG(WARN, "fail to set bitmap", KR(ret), K(i), K(row_start));
            }
          }
        }
      } else {
        common::hash::ObHashSet<ValDataType, common::hash::NoPthreadDefendMode> datums_val;
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
            const ValDataType cur_val = val_arr[i];
            if (datums_val.exist_refactored(cur_val) == OB_HASH_EXIST) {
              if (OB_FAIL(result_bitmap.set(i))) {
                STORAGE_LOG(WARN, "fail to set bitmap", KR(ret), K(i), K(row_start));
              }
            }
          }
        }
      }
    } else if (filter_val_cnt <= MAX_STACK_COUNT) {
      ValDataType valid_vals[MAX_STACK_COUNT];
      int64_t valid_cnt = 0;
      for (int64_t i = 0; i < filter_val_cnt; ++i) {
        if (filter_vals_valid[i]) {
          const uint64_t datum_val = filter_vals[i] - base_val;
          const ValDataType cast_datum_val = *reinterpret_cast<const ValDataType *>(&datum_val);
          valid_vals[valid_cnt++] = cast_datum_val;
        }
      }
      if (valid_cnt == 0) {
        if constexpr (ExistNullBitmap) {
          result_bitmap.set_bitmap_batch(0, row_count, false);
        }
      } else if (valid_cnt == 1) {
        const ValDataType valid_val = valid_vals[0];
        for (int64_t i = 0; i < row_count; ++i) {
          if constexpr (ExistNullBitmap) {
            result_data[i] = (result_data[i] ^ 1) & static_cast<uint8_t>(val_arr[i] == valid_val);
          } else {
            result_data[i] = static_cast<uint8_t>(val_arr[i] == valid_val);
          }
        }
      } else if (valid_cnt <= 4) {
        for (int64_t i = 0; i < row_count; ++i) {
          const ValDataType cur_val = val_arr[i];
          bool is_matched = false;
          for (int64_t j = 0; j < valid_cnt; ++j) {
            is_matched |= cur_val == valid_vals[j];
          }
          if constexpr (ExistNullBitmap) {
            result_data[i] = (result_data[i] ^ 1) & static_cast<uint8_t>(is_matched);
          } else {
            result_data[i] = static_cast<uint8_t>(is_matched);
          }
        }
      } else {
        for (int64_t i = 0; i < row_count; ++i) {
          if (ExistNullBitmap && result_data[i]) {
            result_data[i] = 0;
          } else {
            const ValDataType cur_val = val_arr[i];
            for (int64_t j = 0; j < valid_cnt; ++j) {
              if (cur_val == valid_vals[j]) {
                result_data[i] = 1;
                break;
              }
            }
          }
        }
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
          const ValDataType cur_val = val_arr[i];
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
      }
    }
    return ret;
  }

  static int in_op_tranverse_with_null(const char *buf, const uint64_t null_replaced_val,
    const bool *filter_vals_valid, const uint64_t *filter_vals, const int64_t filter_val_cnt,
    const int64_t row_start, const int64_t row_count, const uint64_t base_val,
    const sql::ObPushdownFilterExecutor *parent, ObBitmap &result_bitmap, const sql::ObWhiteFilterExecutor *filter)
  {
    UNUSED(ExistNullBitmap);
    int ret = common::OB_SUCCESS;
    const ValDataType cast_null_val = *reinterpret_cast<const ValDataType *>(&null_replaced_val);
    const ValDataType *__restrict__ val_arr = reinterpret_cast<const ValDataType *>(buf) + row_start;
    uint8_t *__restrict__ result_data = result_bitmap.get_data();

    CHECK_USE_HASHSET_FOR_IN_OP(filter_val_cnt, row_count);
    if (use_hash_set) {
      if (filter->is_filter_dynamic_node()) {
        sql::ObWhiteFilterSmallHashSet datums_val;
        if (OB_FAIL(datums_val.create(filter_val_cnt, static_cast<const sql::ObDynamicFilterExecutor *>(filter)->hash_func_))) {
          STORAGE_LOG(WARN, "fail to create small hash set", KR(ret), K(filter_val_cnt));
        }

        ObStorageDatum cast_datum_val;
        for (int64_t i = 0; OB_SUCC(ret) && (i < filter_val_cnt); ++i) {
          if (filter_vals_valid[i]) {
            const uint64_t datum_val = filter_vals[i] - base_val;
            cast_datum_val.set_uint(datum_val);
            if (OB_FAIL(datums_val.insert_datum(cast_datum_val))) {
              STORAGE_LOG(WARN, "fail to insert datum", KR(ret), K(cast_datum_val), K(filter_val_cnt));
            }
          }
        }
        ObStorageDatum cast_cur_val;
        ValDataType cur_val = 0;
        for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
          if (ExistParent && parent->can_skip_filter(i)) {
            // skip
          } else if ((cur_val = val_arr[i]) == cast_null_val) {
            // skip null
          } else {
            cast_cur_val.set_uint(cur_val);
            bool is_exist;
            if (OB_FAIL(datums_val.exist_datum(cast_cur_val, is_exist))) {
              STORAGE_LOG(WARN, "fail to search datum in small set", KR(ret), K(i), K(cast_cur_val));
            } else if (is_exist && OB_FAIL(result_bitmap.set(i))) {
              STORAGE_LOG(WARN, "fail to set bitmap", KR(ret), K(i), K(row_start));
            }
          }
        }
      } else {
        common::hash::ObHashSet<uint64_t, common::hash::NoPthreadDefendMode> datums_val;
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
          } else if ((cur_val = val_arr[i]) == cast_null_val) {
            // skip null
          } else if (datums_val.exist_refactored(cur_val) == OB_HASH_EXIST) {
            if (OB_FAIL(result_bitmap.set(i))) {
              STORAGE_LOG(WARN, "fail to set bitmap", KR(ret), K(i), K(row_start));
            }
          }
        }
      }
    } else if (filter_val_cnt <= MAX_STACK_COUNT) {
      ValDataType valid_vals[MAX_STACK_COUNT];
      int64_t valid_cnt = 0;
      for (int64_t i = 0; i < filter_val_cnt; ++i) {
        if (filter_vals_valid[i]) {
          const uint64_t datum_val = filter_vals[i] - base_val;
          const ValDataType cast_datum_val = *reinterpret_cast<const ValDataType *>(&datum_val);
          if (cast_datum_val != cast_null_val) {
            valid_vals[valid_cnt++] = cast_datum_val;
          }
        }
      }
      if (valid_cnt == 0) {
      } else if (valid_cnt == 1) {
        const ValDataType valid_val = valid_vals[0];
        for (int64_t i = 0; i < row_count; ++i) {
          result_data[i] = static_cast<uint8_t>(val_arr[i] == valid_val);
        }
      } else if (valid_cnt <= 4) {
        for (int64_t i = 0; i < row_count; ++i) {
          const ValDataType cur_val = val_arr[i];
          bool is_match = false;
          for (int64_t j = 0; j < valid_cnt; ++j) {
            is_match |= cur_val == valid_vals[j];
          }
          result_data[i] = static_cast<uint8_t>(is_match);
        }
      } else {
        for (int64_t i = 0; i < row_count; ++i) {
          const ValDataType cur_val = val_arr[i];
          for (int64_t j = 0; j < valid_cnt; ++j) {
            if (cur_val == valid_vals[j]) {
              result_data[i] = 1;
              break;
            }
          }
        }
      }
    } else {
      ValDataType cur_val = 0;
      for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
        if (ExistParent && parent->can_skip_filter(i)) {
          // skip
        } else if ((cur_val = val_arr[i]) == cast_null_val) {
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
      }
    }
    return ret;
  }
};

template <typename ValDataType, typename Op>
class ObCSDictFilterOpFunc
{
public:
  static void dict_val_compare_tranverse(const char *dict_val_buf,
    const uint64_t datum_val, const int64_t dict_val_cnt,
    int64_t &matched_ref_cnt, sql::ObBitVector *ref_bitset)
  {
    const ValDataType cast_datum_val = *reinterpret_cast<const ValDataType *>(&datum_val);
    const ValDataType *__restrict__ val_arr = reinterpret_cast<const ValDataType *>(dict_val_buf);
    ref_bitset->bit_assign(0, dict_val_cnt, [&](int64_t i) {
      return Op::apply(val_arr[i], cast_datum_val);
    });
    matched_ref_cnt += ref_bitset->accumulate_bit_cnt(dict_val_cnt);
  }

  static void dict_val_bt_tranverse(const char *dict_val_buf,
    const uint64_t *datums_val, const int64_t dict_val_cnt,
    int64_t &matched_ref_cnt, sql::ObBitVector *ref_bitset)
  {
    const ValDataType cast_left_boundary = *reinterpret_cast<const ValDataType *>(datums_val);
    const ValDataType cast_right_boundary = *reinterpret_cast<const ValDataType *>(datums_val + 1);
    const ValDataType *__restrict__ val_arr = reinterpret_cast<const ValDataType *>(dict_val_buf);
    const ValDataType delta = cast_right_boundary - cast_left_boundary;
    ref_bitset->bit_assign(0, dict_val_cnt, [&](int64_t i) {
      return static_cast<ValDataType>(val_arr[i] - cast_left_boundary) <= delta;
    });
    matched_ref_cnt += ref_bitset->accumulate_bit_cnt(dict_val_cnt);
  }

  static void dict_ref_sort_bt_tranverse(const char *dict_ref_buf, const uint64_t dict_val_cnt,
    const int64_t *refs_val, const int64_t row_start, const int64_t row_count,
    common::ObBitmap &result_bitmap)
  {
    const ValDataType cast_left_inclusive = *reinterpret_cast<const ValDataType *>(refs_val);
    const ValDataType cast_right_inclusive = *reinterpret_cast<const ValDataType *>(refs_val + 1);
    OB_ASSERT(cast_right_inclusive < dict_val_cnt);
    const ValDataType *__restrict__ ref_arr = reinterpret_cast<const ValDataType *>(dict_ref_buf) + row_start;
    const ValDataType delta = cast_right_inclusive - cast_left_inclusive;
    uint8_t *__restrict__ result_data = result_bitmap.get_data();
    for (int64_t i = 0; i < row_count; ++i) {
      const ValDataType d = static_cast<ValDataType>(ref_arr[i] - cast_left_inclusive);
      result_data[i] = static_cast<uint8_t>(d <= delta);
    }
  }

  // unused
  static void dict_val_in_tranverse(const char *dict_val_buf, const uint64_t dict_val_base,
    const bool *vals_valid, const uint64_t *vals, const int64_t datums_cnt,
    const int64_t dict_val_cnt, int64_t &matched_ref_cnt, sql::ObBitVector *ref_bitset)
  {
    static const int64_t MAX_STACK_BYTES = 256;
    static const int64_t MAX_STACK_COUNT = MAX_STACK_BYTES / sizeof(ValDataType);
    const ValDataType *__restrict__ val_arr = reinterpret_cast<const ValDataType *>(dict_val_buf);
    if (datums_cnt <= MAX_STACK_COUNT) {
      ValDataType valid_vals[MAX_STACK_COUNT];
      int64_t valid_cnt = 0;
      for (int64_t i = 0; i < datums_cnt; ++i) {
        if (vals_valid[i]) {
          valid_vals[valid_cnt++] = (ValDataType) (vals[i] - dict_val_base);
        }
      }
      if (valid_cnt == 0) {
      } else if (valid_cnt == 1) {
        const ValDataType valid_val = valid_vals[0];
        ref_bitset->bit_assign(0, dict_val_cnt, [&](int64_t i) {
          return val_arr[i] == valid_val;
        });
      } else if (valid_cnt <= 4) {
        ref_bitset->bit_assign(0, dict_val_cnt, [&](int64_t i) {
          bool is_match = false;
          for (int64_t j = 0; j < valid_cnt; ++j) {
            is_match |= val_arr[i] == valid_vals[j];
          }
          return is_match;
        });
      } else {
        ref_bitset->bit_assign(0, dict_val_cnt, [&](int64_t i) {
          for (int64_t j = 0; j < valid_cnt; ++j) {
            if (val_arr[i] == valid_vals[j]) {
              return true;
            }
          }
          return false;
        });
      }
    } else {
      ref_bitset->bit_assign(0, dict_val_cnt, [&](int64_t i) {
        for (int64_t j = 0; j < datums_cnt; ++j) {
          if (vals_valid[j]) {
            const ValDataType cast_ref_val = (ValDataType)(vals[j] - dict_val_base);
            if (val_arr[i] == cast_ref_val) {
              return true;
            }
          }
        }
        return false;
      });
    }
    matched_ref_cnt += ref_bitset->accumulate_bit_cnt(dict_val_cnt);
  }

  static void dict_tranverse_ref(const char *dict_ref_buf, const int64_t row_start,
    const int64_t row_count, const common::ObBitmap *ref_bitmap,
    common::ObBitmap &result_bitmap)
  {
    const ValDataType *ref_arr = reinterpret_cast<const ValDataType *>(dict_ref_buf) + row_start;
    const uint8_t *__restrict__ ref_data = ref_bitmap->get_data();
    uint8_t *__restrict__ result_data = result_bitmap.get_data();
    if constexpr (sizeof(ValDataType) == 1) {
      if (ref_bitmap->size() <= 64) {
        uint64_t word = 0;
        ref_bitmap->to_bits_mask(0, ref_bitmap->size(), false, reinterpret_cast<uint8_t *>(&word));
        if (word != 0) {
          for (int64_t i = 0; i < row_count; ++i) {
            result_data[i] = (word >> ref_arr[i]) & 1;
          }
        }
      } else if (ref_bitmap->size() <= 128) {
        uint64_t words[2] = {0};
        ref_bitmap->to_bits_mask(0, ref_bitmap->size(), false, reinterpret_cast<uint8_t *>(words));
        if (words[0] != 0 || words[1] != 0) {
          for (int64_t i = 0; i < row_count; ++i) {
            const ValDataType ref = ref_arr[i];
            const uint64_t word = (ref < 64) ? words[0] : words[1];
            result_data[i] = (word >> (ref & 63)) & 1;
          }
        }
      } else {
        for (int64_t i = 0; i < row_count; ++i) {
          result_data[i] = ref_data[ref_arr[i]];
        }
      }
    } else {
      for (int64_t i = 0; i < row_count; ++i) {
        result_data[i] = ref_data[ref_arr[i]];
      }
    }
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

template <int32_t VAL_WIDTH_TAG>
struct ObCSDictRefFilterFuncProducer
{
  static cs_dict_ref_sort_bt_tranverse produce_dict_ref_sort_bt_tranverse()
  {
    typedef typename ObEncodingTypeInference<false, VAL_WIDTH_TAG>::Type ValDataType;
    cs_dict_ref_sort_bt_tranverse func = nullptr;
    func = ObCSDictFilterOpFunc<ValDataType, CSBetweenOp<ValDataType>>::dict_ref_sort_bt_tranverse;
    return func;
  }

  static cs_dict_tranverse_ref produce_dict_tranverse_ref()
  {
    typedef typename ObEncodingTypeInference<false, VAL_WIDTH_TAG>::Type ValDataType;
    cs_dict_tranverse_ref func = nullptr;
    func = ObCSDictFilterOpFunc<ValDataType, CSEqualsOp<ValDataType>>::dict_tranverse_ref;
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
        func = ObCSDictFilterOpFunc<ValDataType, CSEqualsOp<ValDataType>>::dict_val_compare_tranverse;
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_LE:
        func = ObCSDictFilterOpFunc<ValDataType, CSLessOrEqualsOp<ValDataType>>::dict_val_compare_tranverse;
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_LT:
        func = ObCSDictFilterOpFunc<ValDataType, CSLessOp<ValDataType>>::dict_val_compare_tranverse;
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_GE:
        func = ObCSDictFilterOpFunc<ValDataType, CSGreaterOrEqualsOp<ValDataType>>::dict_val_compare_tranverse;
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_GT:
        func = ObCSDictFilterOpFunc<ValDataType, CSGreaterOp<ValDataType>>::dict_val_compare_tranverse;
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_NE:
        func = ObCSDictFilterOpFunc<ValDataType, CSNotEqualsOp<ValDataType>>::dict_val_compare_tranverse;
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
    func = ObCSDictFilterOpFunc<ValDataType, CSBetweenOp<ValDataType>>::dict_val_bt_tranverse;
    return func;
  }

  OB_INLINE static cs_dict_val_in_tranverse produce_dict_val_in_tranverse()
  {
    typedef typename ObEncodingTypeInference<false, VAL_WIDTH_TAG>::Type ValDataType;
    cs_dict_val_in_tranverse func = nullptr;
    func = ObCSDictFilterOpFunc<ValDataType, CSEqualsOp<ValDataType>>::dict_val_in_tranverse;
    return func;
  }
};

// =================== set_bitmap_with_bitset ===================
template <int32_t REF_WIDTH_TAG, bool IS_CONVERSELY>
struct ObCSDictSetBitmapOpFunc
{
  typedef typename ObCSEncodingStoreTypeInference<REF_WIDTH_TAG>::Type RefIntType;

  static int set_bitmap_with_bitset(
    const char *ref_buf,
    sql::ObBitVector *ref_bitset,
    const int64_t ref_bitset_size,
    const int64_t row_start,
    const int64_t row_cnt,
    ObBitmap &result_bitmap)
  {
    const RefIntType *__restrict__ ref_arr = reinterpret_cast<const RefIntType *>(ref_buf) + row_start;
    uint8_t *__restrict__ result_data = result_bitmap.get_data();
    if constexpr (sizeof(RefIntType) == 1) { // tiny dict
      if (ref_bitset_size <= 64) {
        const uint64_t ref_word = IS_CONVERSELY ? ~(*ref_bitset->align_at(0)) : *ref_bitset->align_at(0);
        for (int i = 0; i < row_cnt; ++i) {
          result_data[i] = ((ref_word >> ref_arr[i]) & 1);
        }
      } else if (ref_bitset_size <= 128) {
        const uint64_t *ref_words = ref_bitset->reinterpret_data<uint64_t>();
        const uint64_t w0 = IS_CONVERSELY ? ~ref_words[0] : ref_words[0];
        const uint64_t w1 = IS_CONVERSELY ? ~ref_words[1] : ref_words[1];
        for (int i = 0; i < row_cnt; ++i) {
          const RefIntType ref = ref_arr[i];
          const uint64_t word = (ref < 64) ? w0 : w1;
          result_data[i] = ((word >> (ref & 63)) & 1);
        }
      } else {
        alignas(64) uint8_t lut[256];
        unpack_bits_to_bytes<IS_CONVERSELY>(ref_bitset->reinterpret_data<uint64_t>(), lut, ref_bitset_size);
        for (int i = 0; i < row_cnt; ++i) {
          result_data[i] = lut[ref_arr[i]];
        }
      }
    } else if constexpr (sizeof(RefIntType) == 2) {
      if (ref_bitset_size <= 4096) {
        alignas(64) uint8_t lut[4096];
        unpack_bits_to_bytes<IS_CONVERSELY>(ref_bitset->reinterpret_data<uint64_t>(), lut, ref_bitset_size);
        for (int i = 0; i < row_cnt; ++i) {
          result_data[i] = lut[ref_arr[i]];
        }
      } else {
        for (int i = 0; i < row_cnt; ++i) {
          ref_bitset->get_bit_as_u8<IS_CONVERSELY>(ref_arr[i], result_data[i]);
        }
      }
    } else {
      for (int i = 0; i < row_cnt; ++i) {
        ref_bitset->get_bit_as_u8<IS_CONVERSELY>(ref_arr[i], result_data[i]);
      }
    }
    return common::OB_SUCCESS;
  }

  static int set_bitmap_with_bitset_const(
    const char *exception_row_id_buf,
    const char *exception_ref_buf,
    const int64_t exception_cnt,
    sql::ObBitVector *ref_bitset,
    const int64_t ref_bitset_size,
    const int64_t row_start,
    const int64_t row_cnt,
    ObBitmap &result_bitmap)
  {
    // MAX_EXCEPTION_COUNT = 64, ref_bitset_size include const ref and NULL max is 66
    OB_ASSERT(ref_bitset_size <= 66);
    const RefIntType *__restrict__ exception_row_ids = reinterpret_cast<const RefIntType *>(exception_row_id_buf);
    const RefIntType *__restrict__ exception_refs = reinterpret_cast<const RefIntType *>(exception_ref_buf);
    uint8_t *__restrict__ result_data = result_bitmap.get_data();
    const RefIntType *lo = std::lower_bound(exception_row_ids, exception_row_ids + exception_cnt,
                                            static_cast<uint64_t>(row_start));
    const RefIntType *hi = std::lower_bound(lo, exception_row_ids + exception_cnt,
                                            static_cast<uint64_t>(row_start + row_cnt));
    const int begin_idx = lo - exception_row_ids;
    const int end_idx = hi - exception_row_ids;
    if (begin_idx == end_idx) {
    } else if (OB_LIKELY(ref_bitset_size <= 64)) {
      const uint64_t ref_word =
        IS_CONVERSELY ? ~(*ref_bitset->align_at(0)) : *ref_bitset->align_at(0);
      for (int i = begin_idx; i < end_idx; ++i) {
        result_data[exception_row_ids[i] - row_start] = ((ref_word >> exception_refs[i]) & 1);
      }
    } else {
      const uint64_t *ref_words = ref_bitset->reinterpret_data<uint64_t>();
      const uint64_t w0 = IS_CONVERSELY ? ~(ref_words[0]) : ref_words[0];
      const uint64_t w1 = IS_CONVERSELY ? ~(ref_words[1]) : ref_words[1];
      for (int i = begin_idx; i < end_idx; ++i) {
        const RefIntType ref = exception_refs[i];
        const uint64_t word = (ref < 64) ? w0 : w1;
        result_data[exception_row_ids[i] - row_start] = ((word >> (ref & 63)) & 1);
      }
    }
    return common::OB_SUCCESS;
  }
};

// FLAG semantics:
//   FLAG == 1 (true)  -> NOT conversely (set rows whose ref is in @ref_bitset)
//   FLAG == 0 (false) -> conversely     (set rows whose ref is NOT in @ref_bitset)
template <int32_t REF_WIDTH_TAG, int32_t FLAG>
struct ObCSDictSetBitmapFuncProducer
{
  OB_INLINE static cs_dict_set_bitmap_with_bitset produce_set_bitmap_with_bitset()
  {
    return ObCSDictSetBitmapOpFunc<REF_WIDTH_TAG, !static_cast<bool>(FLAG)>::set_bitmap_with_bitset;
  }

  OB_INLINE static cs_dict_set_bitmap_with_bitset_const produce_set_bitmap_with_bitset_const()
  {
    return ObCSDictSetBitmapOpFunc<REF_WIDTH_TAG, !static_cast<bool>(FLAG)>::set_bitmap_with_bitset_const;
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
    const sql::ObPushdownFilterExecutor *parent, ObBitmap &result_bitmap, const sql::ObWhiteFilterExecutor *filter)
  {
    int32_t val_width_tag = get_value_len_tag_map()[val_width_size];
    const bool exist_parent = (nullptr != parent);
    cs_integer_in_tranverse func = integer_in_funcs_[exist_null_bitmap][exist_parent][val_width_tag];
    return func(buf, filter_vals_valid, filter_vals, filter_val_cnt,
        row_start, row_count, base_val, parent, result_bitmap, filter);
  }

  OB_INLINE int integer_in_tranverse_with_null(const char *buf, const uint32_t val_width_size,
    const uint64_t null_replaced_val, const bool *filter_vals_valid, const uint64_t *filter_vals,
    const int64_t filter_val_cnt, const int64_t row_start, const int64_t row_count, const uint64_t base_val,
    const sql::ObPushdownFilterExecutor *parent, ObBitmap &result_bitmap, const sql::ObWhiteFilterExecutor *filter)
  {
    int32_t val_width_tag = get_value_len_tag_map()[val_width_size];
    const bool exist_parent = (nullptr != parent);
    cs_integer_in_tranverse_with_null func = integer_in_null_funcs_[exist_parent][val_width_tag];
    return func(buf, null_replaced_val, filter_vals_valid, filter_vals, filter_val_cnt,
        row_start, row_count, base_val, parent, result_bitmap, filter);
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

  OB_INLINE void dict_ref_sort_bt_tranverse(const char *dict_ref_buf,
    const uint64_t dict_val_cnt, const int64_t *refs_val, const int64_t row_start,
    const int64_t row_count, const uint32_t ref_width_size,
    common::ObBitmap &result_bitmap)
  {
    const int32_t ref_width_tag = get_value_len_tag_map()[ref_width_size];
    cs_dict_ref_sort_bt_tranverse func = dict_ref_sort_bt_funcs_[ref_width_tag];
    func(dict_ref_buf, dict_val_cnt, refs_val, row_start, row_count, result_bitmap);
  }

  OB_INLINE void dict_val_in_tranverse(const char *dict_val_buf, const uint32_t val_width_size,
    const uint64_t dict_val_base, const bool *vals_valid, const uint64_t *vals,
    const int64_t datums_cnt, const int64_t dict_val_cnt, int64_t &matched_ref_cnt, sql::ObBitVector *ref_bitset)
  {
    const int32_t val_width_tag = get_value_len_tag_map()[val_width_size];
    cs_dict_val_in_tranverse func = dict_val_in_funcs_[val_width_tag];
    func(dict_val_buf, dict_val_base, vals_valid, vals, datums_cnt, dict_val_cnt, matched_ref_cnt, ref_bitset);
  }

  OB_INLINE void dict_tranverse_ref(const char *dict_ref_buf, const uint32_t ref_width_size,
    const int64_t row_start, const int64_t row_count, const common::ObBitmap *ref_bitmap,
    common::ObBitmap &result_bitmap)
  {
    const int32_t ref_width_tag = get_value_len_tag_map()[ref_width_size];
    cs_dict_tranverse_ref func = dict_scan_ref_funcs_[ref_width_tag];
    func(dict_ref_buf, row_start, row_count, ref_bitmap, result_bitmap);
  }

  // if the row's ref is set in @ref_bitset, set it as @flag in @result_bitmap, otherwise set it conversely.
  OB_INLINE int dict_set_bitmap_with_bitset(const uint8_t ref_width_tag, const char *ref_buf,
    sql::ObBitVector *ref_bitset, const int64_t ref_bitset_size,
    const int64_t row_start, const int64_t row_cnt,
    ObBitmap &result_bitmap, const bool flag)
  {
    cs_dict_set_bitmap_with_bitset func = dict_set_bitmap_with_bitset_funcs_[ref_width_tag][flag];
    return func(ref_buf, ref_bitset, ref_bitset_size,
                row_start, row_cnt, result_bitmap);
  }
  OB_INLINE int dict_set_bitmap_with_bitset_const(const uint32_t ref_width_size,
    const char *exception_row_id_buf, const char *exception_ref_buf, const int64_t exception_cnt,
    sql::ObBitVector *ref_bitset, const int64_t ref_bitset_size,
    const int64_t row_start, const int64_t row_cnt,
    ObBitmap &result_bitmap, const bool flag)
  {
    const int32_t ref_width_tag = get_value_len_tag_map()[ref_width_size];
    cs_dict_set_bitmap_with_bitset_const func = dict_set_bitmap_with_bitset_const_funcs_[ref_width_tag][flag];
    return func(exception_row_id_buf, exception_ref_buf, exception_cnt,
                ref_bitset, ref_bitset_size, row_start, row_cnt, result_bitmap);
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
  ObMultiDimArray_T<cs_dict_ref_sort_bt_tranverse, 4/*val_tag*/> dict_ref_sort_bt_funcs_;
  ObMultiDimArray_T<cs_dict_val_in_tranverse, 4/*val_tag*/> dict_val_in_funcs_;
  ObMultiDimArray_T<cs_dict_tranverse_ref, 4/*val_tag*/> dict_scan_ref_funcs_;

  ObMultiDimArray_T<cs_dict_set_bitmap_with_bitset, 4/*ref_width_tag*/, 2 /*flag*/> dict_set_bitmap_with_bitset_funcs_;
  ObMultiDimArray_T<cs_dict_set_bitmap_with_bitset_const, 4/*ref_width_tag*/, 2 /*flag*/> dict_set_bitmap_with_bitset_const_funcs_;
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
