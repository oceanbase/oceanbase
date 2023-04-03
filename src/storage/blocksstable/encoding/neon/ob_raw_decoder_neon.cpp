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

#include "storage/blocksstable/encoding/ob_encoding_query_util.h"
#include "storage/blocksstable/encoding/ob_raw_decoder.h"
#include "ob_encoding_neon_util.h"

namespace oceanbase {
namespace blocksstable {

template <bool IS_SIGNED, int32_t LEN_TAG, int32_t CMP_TYPE>
struct RawFixFilterNeonFunc_T : public RawFixFilterFunc_T<IS_SIGNED, LEN_TAG, CMP_TYPE>
{};

#if defined ( __ARM_NEON ) && defined ( __aarch64__ )

template <int CMP_TYPE>
struct RawFixFilterNeonFunc_T<0, 0, CMP_TYPE>
{
  // Fast filter with Neon for 1 byte unsigned data
  static void fix_filter_func(
      const int64_t row_cnt,
      const unsigned char *col_data,
      const uint64_t node_value,
      sql::ObBitVector &res)
  {
    const uint8_t *stored_values = reinterpret_cast<const uint8_t *>(col_data);
    uint8_t casted_node_value = *reinterpret_cast<const uint8_t *>(&node_value);

    uint8x16_t power_vec = vld1q_u8(NEON_CMP_MASK_POWER_16);
    uint8x16_t node_value_vec = vdupq_n_u8(casted_node_value);
    for (int64_t i = 0; i < row_cnt / 16; i++) {
      uint8x16_t data_vec = vld1q_u8(col_data + i * 16);
      uint8x16_t cmp_res = neon_cmp_int<uint8x16_t, uint8x16_t, CMP_TYPE>(data_vec, node_value_vec);
      // generate result bitmap from compare result
      uint64x2_t bi_mask = vpaddlq_u32(vpaddlq_u16(vpaddlq_u8(vandq_u8(cmp_res, power_vec))));
      uint64_t mask = vgetq_lane_u64(bi_mask, 0) + (vgetq_lane_u64(bi_mask, 1) << 8);
      res.reinterpret_data<uint16_t>()[i] = *reinterpret_cast<uint16_t *>(&mask);
    }

    for (int64_t row_id = row_cnt / 16 * 16; row_id < row_cnt; row_id++) {
      if (value_cmp_t<uint8_t, CMP_TYPE>(stored_values[row_id], casted_node_value)) {
        res.set(row_id);
      }
    }
    LOG_DEBUG("[Neon filter] fast filter for 1 byte unsigned data");
  }
};

template <int CMP_TYPE>
struct RawFixFilterNeonFunc_T<1, 0, CMP_TYPE>
{
  // Fast filter with Neon for 1 byte unsigned data
  static void fix_filter_func(
      const int64_t row_cnt,
      const unsigned char *col_data,
      const uint64_t node_value,
      sql::ObBitVector &res)
  {
    const int8_t *stored_values = reinterpret_cast<const int8_t *>(col_data);
    int8_t casted_node_value = *reinterpret_cast<const int8_t *>(&node_value);

    uint8x16_t power_vec = vld1q_u8(NEON_CMP_MASK_POWER_16);
    int8x16_t node_value_vec = vdupq_n_s8(casted_node_value);
    for (int64_t i = 0; i < row_cnt / 16; i++) {
      int8x16_t data_vec = vld1q_s8(stored_values + i * 16);
      uint8x16_t cmp_res = neon_cmp_int<uint8x16_t, int8x16_t, CMP_TYPE>(data_vec, node_value_vec);
      // generate result bitmap from compare result
      uint64x2_t bi_mask = vpaddlq_u32(vpaddlq_u16(vpaddlq_u8(vandq_u8(cmp_res, power_vec))));
      uint64_t mask = vgetq_lane_u64(bi_mask, 0) + (vgetq_lane_u64(bi_mask, 1) << 8);
      res.reinterpret_data<uint16_t>()[i] = *reinterpret_cast<uint16_t *>(&mask);
    }

    for (int64_t row_id = row_cnt / 16 * 16; row_id < row_cnt; row_id++) {
      if (value_cmp_t<int8_t, CMP_TYPE>(stored_values[row_id], casted_node_value)) {
        res.set(row_id);
      }
    }
    LOG_DEBUG("[Neon filter] fast filter for 1 byte signed data");
  }
};

template <int CMP_TYPE>
struct RawFixFilterNeonFunc_T<0, 1, CMP_TYPE>
{
  // Fast filter with Neon for 2 byte unsigned data
  static void fix_filter_func(
      const int64_t row_cnt,
      const unsigned char *col_data,
      const uint64_t node_value,
      sql::ObBitVector &res)
  {
    const uint16_t *stored_values = reinterpret_cast<const uint16_t*>(col_data);
    uint16_t casted_node_value = *reinterpret_cast<const uint16_t*>(&node_value);

    uint16x8_t power_vec = vld1q_u16(NEON_CMP_MASK_POWER_8);
    uint16x8_t node_value_vec = vdupq_n_u16(casted_node_value);
    for (int64_t i = 0; i < row_cnt / 8; ++i) {
      uint16x8_t data_vec = vld1q_u16(stored_values + i * 8);
      uint16x8_t cmp_res = neon_cmp_int<uint16x8_t, uint16x8_t, CMP_TYPE>(data_vec, node_value_vec);
      // generate result bitmap from compare result
      uint64_t mask = vpaddd_u64(vpaddlq_u32(vpaddlq_u16(vandq_u16(cmp_res, power_vec))));
      res.reinterpret_data<uint8_t>()[i] = *reinterpret_cast<uint8_t *>(&mask);
    }

    for (int64_t row_id = row_cnt / 8 * 8; row_id < row_cnt; row_id++) {
      if (value_cmp_t<uint16_t, CMP_TYPE>(stored_values[row_id], casted_node_value)) {
        res.set(row_id);
      }
    }
    LOG_DEBUG("[Neon filter] fast filter for 2 byte unsigned data");
  }
};

template <int CMP_TYPE>
struct RawFixFilterNeonFunc_T<1, 1, CMP_TYPE>
{
  // Fast filter with Neon for 2 byte signed data
  static void fix_filter_func(
      const int64_t row_cnt,
      const unsigned char *col_data,
      const uint64_t node_value,
      sql::ObBitVector &res)
  {
    const int16_t *stored_values = reinterpret_cast<const int16_t*>(col_data);
    int16_t casted_node_value = *reinterpret_cast<const int16_t*>(&node_value);

    uint16x8_t power_vec = vld1q_u16(NEON_CMP_MASK_POWER_8);
    int16x8_t node_value_vec = vdupq_n_s16(casted_node_value);
    for (int64_t i = 0; i < row_cnt / 8; ++i) {
      int16x8_t data_vec = vld1q_s16(stored_values + i * 8);
      uint16x8_t cmp_res = neon_cmp_int<uint16x8_t, int16x8_t, CMP_TYPE>(data_vec, node_value_vec);
      // generate result bitmap from compare result
      uint64_t mask = vpaddd_u64(vpaddlq_u32(vpaddlq_u16(vandq_u16(cmp_res, power_vec))));
      res.reinterpret_data<uint8_t>()[i] = *reinterpret_cast<uint8_t *>(&mask);
    }

    for (int64_t row_id = row_cnt / 8 * 8; row_id < row_cnt; row_id++) {
      if (value_cmp_t<int16_t, CMP_TYPE>(stored_values[row_id], casted_node_value)) {
        res.set(row_id);
      }
    }
    LOG_DEBUG("[Neon filter] fast filter for 2 byte signed data");
  }
};
#endif

template <int32_t IS_SIGNED, int32_t LEN_TAG, int32_t CMP_TYPE>
struct RawFixFilterNeonArrayInit
{
  bool operator()()
  {
    raw_fix_fast_filter_funcs[IS_SIGNED][LEN_TAG][CMP_TYPE]
        = &(RawFixFilterNeonFunc_T<IS_SIGNED, LEN_TAG, CMP_TYPE>::fix_filter_func);
    return true;
  }
};

bool init_raw_fix_neon_simd_filter_funcs()
{
  return ObNDArrayIniter<RawFixFilterNeonArrayInit, 2, 4, 6>::apply();
}

} // namespace blocksstable
} // namespace oceanbase