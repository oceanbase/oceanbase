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

#include "ob_encoding_query_util.h"
#include "ob_dict_decoder.h"

namespace oceanbase {
namespace blocksstable {
template <int32_t REF_LEN, int32_t CMP_TYPE>
struct DictCmpRefAVX512Func_T : public DictCmpRefFunc_T<REF_LEN, CMP_TYPE>
{};

#if defined ( __AVX512BW__ )
// 1 Byte
template <int CMP_TYPE>
struct DictCmpRefAVX512Func_T<1, CMP_TYPE>
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
    constexpr static int op = ObCmpTypeToAvxOpMap<CMP_TYPE>::value_;

    if (CMP_TYPE <= sql::WHITE_OP_LT) {
      __m128i dict_ref_vec = _mm_set1_epi8(casted_dict_ref);
      for (int64_t i = 0; i < row_cnt / 16; ++i) {
        __m128i data_vec = _mm_loadu_si128(reinterpret_cast<const __m128i *>(col_data + i * 16));
        __mmask16 cmp_res_ref = _mm_cmp_epu8_mask(data_vec, dict_ref_vec, op);
        result.reinterpret_data<uint16_t>()[i] = cmp_res_ref;
        LOG_DEBUG("[SIMD filter] batch filter result",
            K(i), K(result.reinterpret_data<uint16_t>()[i]),
            "ref_arr:", common::ObArrayWrap<uint8_t>(ref_arr + i * 16, 16));
      }

      for (int64_t row_id = row_cnt / 16 * 16; row_id < row_cnt; ++row_id) {
        if (value_cmp_t<uint8_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
          result.set(row_id);
        }
        LOG_DEBUG("[SIMD filter] filter result",
            K(row_id), K(ref_arr[row_id]), K(result.at(row_id)));
      }
    } else {
      __m128i dict_ref_vec = _mm_set1_epi8(casted_dict_ref);
      __m128i dict_cnt_vec = _mm_set1_epi8(casted_dict_cnt);
      for (int64_t i = 0; i < row_cnt / 16; ++i) {
        __m128i data_vec = _mm_loadu_si128(reinterpret_cast<const __m128i *>(col_data + i * 16));
        __mmask16 cmp_res_ref = _mm_cmp_epu8_mask(data_vec, dict_ref_vec, op);
        // op == 5: greater than or equal to
        __mmask16 cmp_res_cnt = _mm_cmp_epu8_mask(data_vec, dict_cnt_vec, 5);
        result.reinterpret_data<uint16_t>()[i] = cmp_res_ref & (~cmp_res_cnt);
        LOG_DEBUG("[SIMD filter] batch filter result",
            K(i), K(result.reinterpret_data<uint16_t>()[i]),
            "ref_arr:", common::ObArrayWrap<uint8_t>(ref_arr + i * 16, 16));
      }

      for (int64_t row_id = row_cnt / 16 * 16; row_id < row_cnt; ++row_id) {
        if (value_cmp_t<uint8_t, sql::WHITE_OP_GE>(ref_arr[row_id], casted_dict_cnt)) {
          // null value
        } else if (value_cmp_t<uint8_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
          result.set(row_id);
        }
        LOG_DEBUG("[SIMD filter] filter result",
            K(row_id), K(ref_arr[row_id]), K(result.at(row_id)));
      }
    }
    LOG_DEBUG("[SIMD filter] fast cmp dict ref for 1 byte",
        K(row_cnt), K(op), K(dict_ref), K(dict_cnt), K(casted_dict_ref), K(casted_dict_cnt));
  }
};

// 2 Bytes
template <int CMP_TYPE>
struct DictCmpRefAVX512Func_T<2, CMP_TYPE>
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
    constexpr static int op = ObCmpTypeToAvxOpMap<CMP_TYPE>::value_;

    if (CMP_TYPE <= sql::WHITE_OP_LT) {
      __m256i dict_ref_vec = _mm256_set1_epi16(casted_dict_ref);
      for (int64_t i = 0; i < row_cnt / 16; ++i) {
        __m256i data_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(col_data + i * 32));
        __mmask16 cmp_res_ref = _mm256_cmp_epu16_mask(data_vec, dict_ref_vec, op);
        result.reinterpret_data<uint16_t>()[i] = cmp_res_ref;
        LOG_DEBUG("[SIMD filter] batch filter result",
            K(i), K(result.reinterpret_data<uint16_t>()[i]),
            "ref_arr:", common::ObArrayWrap<uint16_t>(ref_arr + i * 16, 16));
      }

      for (int64_t row_id = row_cnt / 16 * 16; row_id < row_cnt; ++row_id) {
        if (value_cmp_t<uint16_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
          result.set(row_id);
        }
        LOG_DEBUG("[SIMD filter] filter result",
            K(row_id), K(ref_arr[row_id]), K(result.at(row_id)));
      }
    } else {
      __m256i dict_ref_vec = _mm256_set1_epi16(casted_dict_ref);
      __m256i dict_cnt_vec = _mm256_set1_epi16(casted_dict_cnt);
      for (int64_t i = 0; i < row_cnt / 16; ++i) {
        __m256i data_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(col_data + i * 32));
        __mmask16 cmp_res_ref = _mm256_cmp_epu16_mask(data_vec, dict_ref_vec, op);
        // op == 5: greater than or equal to
        __mmask16 cmp_res_cnt = _mm256_cmp_epu16_mask(data_vec, dict_cnt_vec, 5);
        result.reinterpret_data<uint16_t>()[i] = cmp_res_ref & (~cmp_res_cnt);
        LOG_DEBUG("[SIMD filter] batch filter result",
            K(i), K(result.reinterpret_data<uint16_t>()[i]),
            "ref_arr:", common::ObArrayWrap<uint16_t>(ref_arr + i * 16, 16));
      }

      for (int64_t row_id = row_cnt / 16 * 16; row_id < row_cnt; ++row_id) {
        if (value_cmp_t<uint16_t, sql::WHITE_OP_GE>(ref_arr[row_id], casted_dict_cnt)) {
          // null value
        } else if (value_cmp_t<uint16_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
          result.set(row_id);
        }
        LOG_DEBUG("[SIMD filter] filter result",
            K(row_id), K(ref_arr[row_id]), K(result.at(row_id)));
      }
    }
    LOG_DEBUG("[SIMD filter] fast cmp dict ref for 2 bytes",
        K(row_cnt), K(op), K(dict_ref), K(dict_cnt), K(casted_dict_ref), K(casted_dict_cnt));
  }
};
#endif


template <int32_t REF_LEN, int32_t CMP_TYPE>
struct DictCmpRefAVX512ArrayInit
{
  bool operator()()
  {
    dict_cmp_ref_funcs[REF_LEN][CMP_TYPE]
        = &(DictCmpRefAVX512Func_T<REF_LEN, CMP_TYPE>::dict_cmp_ref_func);
    return true;
  }
};

bool init_dict_cmp_ref_simd_funcs()
{
  return ObNDArrayIniter<DictCmpRefAVX512ArrayInit, 3, 6>::apply();
}

} // end of namespace blocksstable
} // end of namespace oceanbase