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

#define USING_LOG_PREFIX STORAGE

#if defined ( __ARM_NEON )
#include <arm_neon.h>
#endif

#include "storage/blocksstable/cs_encoding/ob_dict_column_decoder.h"

namespace oceanbase {
namespace blocksstable {

template <int32_t REF_WIDTH_TAG, int32_t CMP_TYPE>
struct ObCSFilterDictCmpRefNeonFunction : public ObCSFilterDictCmpRefFunction<REF_WIDTH_TAG, CMP_TYPE>
{};

#if defined ( __ARM_NEON ) && defined ( __aarch64__ )
// 1 byte
template <int CMP_TYPE>
struct ObCSFilterDictCmpRefNeonFunction<0, CMP_TYPE>
{
  static void dict_cmp_ref_func(
      const int64_t dict_ref_val,
      const int64_t null_val,
      const char *dict_ref_buf,
      const sql::PushdownFilterInfo &pd_filter_info,
      sql::ObBitVector &result)
  {
    const char *dict_ref_start_buf = dict_ref_buf + pd_filter_info.start_ * sizeof(uint8_t);
    const uint8_t *ref_arr = reinterpret_cast<const uint8_t *>(dict_ref_start_buf);
    const uint8_t casted_dict_ref = *reinterpret_cast<const uint8_t *>(&dict_ref_val);
    const uint8_t casted_null_val = *reinterpret_cast<const uint8_t *>(&null_val);
    constexpr static int op = ObCmpTypeToAvxOpMap<CMP_TYPE>::value_;
    int64_t row_cnt = pd_filter_info.count_;
    uint8x16_t power_vec = vld1q_u8(NEON_CMP_MASK_POWER_16);
    uint8x16_t dict_ref_vec = vdupq_n_u8(casted_dict_ref);
    switch (CMP_TYPE) {
      case sql::WHITE_OP_LT:
      case sql::WHITE_OP_LE:
      case sql::WHITE_OP_EQ: {
        for (int64_t i = 0; i < row_cnt / 16; ++i) {
          uint8x16_t cur_ref_vec = vld1q_u8(ref_arr + i * 16);
          uint8x16_t cmp_ref_res = neon_cmp_int<uint8x16_t, uint8x16_t, CMP_TYPE>(cur_ref_vec, dict_ref_vec);
          uint64x2_t bi_mask = vpaddlq_u32(vpaddlq_u16(vpaddlq_u8(vandq_u8(cmp_ref_res, power_vec))));
          uint64_t mask = vgetq_lane_u64(bi_mask, 0) + (vgetq_lane_u64(bi_mask, 1) << 8);
          result.reinterpret_data<uint16_t>()[i] = *reinterpret_cast<uint16_t *>(&mask);
        }
        for (int64_t row_id = row_cnt / 16 * 16; row_id < row_cnt; ++row_id) {
          if (value_cmp_t<uint8_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
            result.set(row_id);
          }
        }
        break;
      }
      case sql::WHITE_OP_GT:
      case sql::WHITE_OP_GE:
      case sql::WHITE_OP_NE: {
        uint8x16_t null_val_vec = vdupq_n_u8(casted_null_val);
        for (int64_t i = 0; i < row_cnt / 16; ++i) {
          uint8x16_t cur_ref_vec = vld1q_u8(ref_arr + i * 16);
          uint8x16_t cmp_ref_res = neon_cmp_int<uint8x16_t, uint8x16_t, CMP_TYPE>(cur_ref_vec, dict_ref_vec);
          uint8x16_t cmp_null_res = neon_cmp_int<uint8x16_t, uint8x16_t, sql::WHITE_OP_GE>(cur_ref_vec, null_val_vec);
          uint8x16_t cmp_res = cmp_ref_res & (~cmp_null_res);
          // generate result bitmap from compare result
          uint64x2_t bi_mask = vpaddlq_u32(vpaddlq_u16(vpaddlq_u8(vandq_u8(cmp_res, power_vec))));
          uint64_t mask = vgetq_lane_u64(bi_mask, 0) + (vgetq_lane_u64(bi_mask, 1) << 8);
          result.reinterpret_data<uint16_t>()[i] = *reinterpret_cast<uint16_t *>(&mask);
        }
        for (int64_t row_id = row_cnt / 16 * 16; row_id < row_cnt; ++row_id) {
          if (value_cmp_t<uint8_t, sql::WHITE_OP_GE>(ref_arr[row_id], casted_null_val)) {
            // means null value
          } else if (value_cmp_t<uint8_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
            result.set(row_id);
          }
        }
        break;
      }
      default:
        break;
    }
  }
};

// 2 byte
template <int CMP_TYPE>
struct ObCSFilterDictCmpRefNeonFunction<1, CMP_TYPE>
{
  static void dict_cmp_ref_func(
      const int64_t dict_ref_val,
      const int64_t null_val,
      const char *dict_ref_buf,
      const sql::PushdownFilterInfo &pd_filter_info,
      sql::ObBitVector &result)
  {
    const char *dict_ref_start_buf = dict_ref_buf + pd_filter_info.start_ * sizeof(uint16_t);
    const uint16_t *ref_arr = reinterpret_cast<const uint16_t *>(dict_ref_start_buf);
    const uint16_t casted_dict_ref = *reinterpret_cast<const uint16_t *>(&dict_ref_val);
    const uint16_t casted_null_val = *reinterpret_cast<const uint16_t *>(&null_val);
    constexpr static int op = ObCmpTypeToAvxOpMap<CMP_TYPE>::value_;
    int64_t row_cnt = pd_filter_info.count_;
    uint16x8_t power_vec = vld1q_u16(NEON_CMP_MASK_POWER_8);
    uint16x8_t dict_ref_vec = vdupq_n_u16(casted_dict_ref);
    switch (CMP_TYPE) {
      case sql::WHITE_OP_LT:
      case sql::WHITE_OP_LE:
      case sql::WHITE_OP_EQ: {
        for (int64_t i = 0; i < row_cnt / 8; ++i) {
          uint16x8_t cur_ref_vec = vld1q_u16(ref_arr + i * 8);
          uint16x8_t cmp_ref_res = neon_cmp_int<uint16x8_t, uint16x8_t, CMP_TYPE>(cur_ref_vec, dict_ref_vec);
          uint64_t mask = vpaddd_u64(vpaddlq_u32(vpaddlq_u16(vandq_u16(cmp_ref_res, power_vec))));
          result.reinterpret_data<uint8_t>()[i] = *reinterpret_cast<uint8_t *>(&mask);
        }
        for (int64_t row_id = row_cnt / 16 * 16; row_id < row_cnt; ++row_id) {
          if (value_cmp_t<uint8_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
            result.set(row_id);
          }
        }
        break;
      }
      case sql::WHITE_OP_GT:
      case sql::WHITE_OP_GE:
      case sql::WHITE_OP_NE: {
        uint16x8_t null_val_vec = vdupq_n_u16(casted_null_val);
        for (int64_t i = 0; i < row_cnt / 8; ++i) {
          uint16x8_t cur_ref_vec = vld1q_u16(ref_arr + i * 8);
          uint16x8_t cmp_ref_res = neon_cmp_int<uint16x8_t, uint16x8_t, CMP_TYPE>(cur_ref_vec, dict_ref_vec);
          uint16x8_t cmp_null_res = neon_cmp_int<uint16x8_t, uint16x8_t, sql::WHITE_OP_GE>(cur_ref_vec, null_val_vec);
          uint16x8_t cmp_res = cmp_ref_res & (~cmp_null_res);
          // generate result bitmap from compare result
          uint64_t mask = vpaddd_u64(vpaddlq_u32(vpaddlq_u16(vandq_u16(cmp_res, power_vec))));
          result.reinterpret_data<uint8_t>()[i] = *reinterpret_cast<uint8_t *>(&mask);
        }
        for (int64_t row_id = row_cnt / 16 * 16; row_id < row_cnt; ++row_id) {
          if (value_cmp_t<uint8_t, sql::WHITE_OP_GE>(ref_arr[row_id], casted_null_val)) {
            // means null value
          } else if (value_cmp_t<uint8_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
            result.set(row_id);
          }
        }
        break;
      }
      default:
        break;
    }
  }
};
#endif

template <int32_t REF_WIDTH_TAG, int32_t CMP_TYPE>
struct CSDictFastCmpNeonArrayInit
{
  bool operator()()
  {
    cs_dict_fast_cmp_funcs[REF_WIDTH_TAG][CMP_TYPE]
        = &(ObCSFilterDictCmpRefNeonFunction<REF_WIDTH_TAG, CMP_TYPE>::dict_cmp_ref_func);
    return true;
  }
};

bool init_cs_dict_fast_cmp_neon_simd_func()
{
  return ObNDArrayIniter<CSDictFastCmpNeonArrayInit, 4, 6>::apply();
}

} // namespace blocksstable
} // namespace oceanbase
