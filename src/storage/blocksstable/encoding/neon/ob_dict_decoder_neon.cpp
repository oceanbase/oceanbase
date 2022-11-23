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
#include "storage/blocksstable/encoding/ob_dict_decoder.h"
#include "ob_encoding_neon_util.h"

namespace oceanbase {
namespace blocksstable {

template <int32_t REF_LEN, int32_t CMP_TYPE>
struct DictCmpRefNeonFunc_T : public DictCmpRefFunc_T<REF_LEN, CMP_TYPE>
{};

#if defined ( __ARM_NEON ) && defined ( __aarch64__ )

// Compare with 1-byte reference
template <int CMP_TYPE>
struct DictCmpRefNeonFunc_T<1, CMP_TYPE>
{
  static void dict_cmp_ref_func(
      const int64_t row_cnt,
      const int64_t dict_ref,
      const int64_t dict_cnt,
      const unsigned char *col_data,
      sql::ObBitVector &result)
  {
    const uint8_t *ref_arr = reinterpret_cast<const uint8_t *>(col_data);
    const uint8_t casted_dict_ref = *reinterpret_cast<const uint8_t *>(&dict_ref);
    const uint8_t casted_dict_cnt = *reinterpret_cast<const uint8_t *>(&dict_cnt);

    uint8x16_t power_vec = vld1q_u8(NEON_CMP_MASK_POWER_16);
    uint8x16_t dict_ref_vec = vdupq_n_u8(casted_dict_ref);
    if (CMP_TYPE <= sql::WHITE_OP_LT) {
      for (int64_t i = 0; i < row_cnt / 16; ++i) {
        uint8x16_t data_vec = vld1q_u8(ref_arr + i * 16);
        uint8x16_t cmp_res = neon_cmp_int<uint8x16_t, uint8x16_t, CMP_TYPE>(data_vec, dict_ref_vec);
        // generate result bitmap from compare result
        uint64x2_t bi_mask = vpaddlq_u32(vpaddlq_u16(vpaddlq_u8(vandq_u8(cmp_res, power_vec))));
        uint64_t mask = vgetq_lane_u64(bi_mask, 0) + (vgetq_lane_u64(bi_mask, 1) << 8);
        result.reinterpret_data<uint16_t>()[i] = *reinterpret_cast<uint16_t *>(&mask);
      }

      for (int64_t row_id = row_cnt / 16 * 16; row_id < row_cnt; ++row_id) {
        if (value_cmp_t<uint8_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
          result.set(row_id);
        }
      }
    } else {
      uint8x16_t dict_cnt_vec = vdupq_n_u8(casted_dict_cnt);
      for (int64_t i = 0; i < row_cnt / 16; ++i) {
        uint8x16_t data_vec = vld1q_u8(ref_arr + i * 16);
        uint8x16_t cmp_res_ref = neon_cmp_int<uint8x16_t, uint8x16_t, CMP_TYPE>(data_vec, dict_ref_vec);
        uint8x16_t cmp_res_cnt = neon_cmp_int<uint8x16_t, uint8x16_t, CMP_TYPE>(data_vec, dict_cnt_vec);
        uint8x16_t cmp_res = cmp_res_ref & (~cmp_res_cnt);
        // generate result bitmap from compare result
        uint64x2_t bi_mask = vpaddlq_u32(vpaddlq_u16(vpaddlq_u8(vandq_u8(cmp_res, power_vec))));
        uint64_t mask = vgetq_lane_u64(bi_mask, 0) + (vgetq_lane_u64(bi_mask, 1) << 8);
        result.reinterpret_data<uint16_t>()[i] = *reinterpret_cast<uint16_t *>(&mask);
      }

      for (int64_t row_id = row_cnt / 16 * 16; row_id < row_cnt; ++row_id) {
        if (value_cmp_t<uint8_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
          result.set(row_id);
        }
      }
    }
    LOG_DEBUG("[Neon filter] fast cmp dict ref for 1 byte");
  }
};

// Compare with 2-bytes reference
template <int CMP_TYPE>
struct DictCmpRefNeonFunc_T<2, CMP_TYPE>
{
  static void dict_cmp_ref_func(
      const int64_t row_cnt,
      const int64_t dict_ref,
      const int64_t dict_cnt,
      const unsigned char *col_data,
      sql::ObBitVector &result)
  {
    const uint16_t *ref_arr = reinterpret_cast<const uint16_t *>(col_data);
    const uint16_t casted_dict_ref = *reinterpret_cast<const uint16_t *>(&dict_ref);
    const uint16_t casted_dict_cnt = *reinterpret_cast<const uint16_t *>(&dict_cnt);

    uint16x8_t power_vec = vld1q_u16(NEON_CMP_MASK_POWER_8);
    uint16x8_t dict_ref_vec = vdupq_n_u16(casted_dict_ref);
    if (CMP_TYPE <= sql::WHITE_OP_LT) {
      for (int64_t i = 0; i < row_cnt / 8; ++i) {
        uint16x8_t data_vec = vld1q_u16(ref_arr + i * 8);
        uint16x8_t cmp_res = neon_cmp_int<uint16x8_t, uint16x8_t, CMP_TYPE>(data_vec, dict_ref_vec);
        // generate result bitmap from compare result
        uint64_t mask = vpaddd_u64(vpaddlq_u32(vpaddlq_u16(vandq_u16(cmp_res, power_vec))));
        result.reinterpret_data<uint8_t>()[i] = *reinterpret_cast<uint8_t *>(&mask);
      }

      for (int64_t row_id = row_cnt / 8 * 8; row_id < row_cnt; ++row_id) {
        if (value_cmp_t<uint16_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
          result.set(row_id);
        }
      }
    } else {
      uint16x8_t dict_cnt_vec = vdupq_n_u16(casted_dict_cnt);
      for (int64_t i = 0; i < row_cnt / 8; ++i) {
        uint16x8_t data_vec = vld1q_u16(ref_arr + i * 8);
        uint16x8_t cmp_res_ref = neon_cmp_int<uint16x8_t, uint16x8_t, CMP_TYPE>(data_vec, dict_ref_vec);
        uint16x8_t cmp_res_cnt = neon_cmp_int<uint16x8_t, uint16x8_t, CMP_TYPE>(data_vec, dict_cnt_vec);
        uint16x8_t cmp_res = cmp_res_ref & (~cmp_res_cnt);
        // generate result bitmap from compare result
        uint64_t mask = vpaddd_u64(vpaddlq_u32(vpaddlq_u16(vandq_u16(cmp_res, power_vec))));
        result.reinterpret_data<uint8_t>()[i] = *reinterpret_cast<uint8_t *>(&mask);
      }

      for (int64_t row_id = row_cnt / 8 * 8; row_id < row_cnt; ++row_id) {
        if (value_cmp_t<uint8_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
          result.set(row_id);
        }
      }
    }
    LOG_DEBUG("[Neon filter] fast cmp dict ref for 2 bytes");
  }
};

#endif

template <int32_t REF_LEN, int32_t CMP_TYPE>
struct DictCmpRefNeonArrayInit
{
  bool operator()()
  {
    dict_cmp_ref_funcs[REF_LEN][CMP_TYPE]
        = &(DictCmpRefNeonFunc_T<REF_LEN, CMP_TYPE>::dict_cmp_ref_func);
    return true;
  }
};

bool init_dict_cmp_ref_neon_simd_funcs()
{
  return ObNDArrayIniter<DictCmpRefNeonArrayInit, 3, 6>::apply();
}

} // namespace blocksstable
} // namespace oceanbase    