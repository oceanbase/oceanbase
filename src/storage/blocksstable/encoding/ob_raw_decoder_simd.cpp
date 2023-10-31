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
#include "ob_raw_decoder.h"

namespace oceanbase {
namespace blocksstable {

template <bool IS_SIGNED, int32_t LEN_TAG, int32_t CMP_TYPE>
struct RawFixFilterAVX512Func_T : public RawFixFilterFunc_T<IS_SIGNED, LEN_TAG, CMP_TYPE>
{};

#if defined ( __AVX512BW__ )
template <int CMP_TYPE>
struct RawFixFilterAVX512Func_T<1, 1, CMP_TYPE>
{
  // Fast filter with SIMD for 2 byte signed data
  static void fix_filter_func(
      const unsigned char *col_data,
      const uint64_t node_value,
      const sql::PushdownFilterInfo &pd_filter_info,
      sql::ObBitVector &res)
  {
    const unsigned char *filter_col_data = col_data + pd_filter_info.start_ * sizeof(int16_t);
    const int16_t *stored_values = reinterpret_cast<const int16_t *>(filter_col_data);
    int16_t casted_node_value = *reinterpret_cast<const int16_t *>(&node_value);
    constexpr static int op = ObCmpTypeToAvxOpMap<CMP_TYPE>::value_;
    const int64_t &row_cnt = pd_filter_info.count_;

    __m256i node_value_vec = _mm256_set1_epi16(casted_node_value);
    for (int64_t i = 0; i < row_cnt / 16; ++i) {
      __m256i data_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(filter_col_data + i * 32));
      res.reinterpret_data<uint16_t>()[i] = _mm256_cmp_epi16_mask(data_vec, node_value_vec, op);
    }

    for (int64_t row_id = row_cnt / 16 * 16;
        row_id < row_cnt; ++row_id) {
      if (value_cmp_t<int16_t, CMP_TYPE>(stored_values[row_id], casted_node_value)) {
        res.set(row_id);
      }
    }
    LOG_DEBUG("[SIMD filter] fast filter for 2 byte signed data",
        K(pd_filter_info), K(row_cnt), K(node_value), K(casted_node_value), K(op),
        "stored_values", common::ObArrayWrap<int16_t>(stored_values, row_cnt));
  }
};

template <int CMP_TYPE>
struct RawFixFilterAVX512Func_T<0, 1, CMP_TYPE>
{
  // Fast filter with SIMD for 2 byte unsigned data
  static void fix_filter_func(
      const unsigned char *col_data,
      const uint64_t node_value,
      const sql::PushdownFilterInfo &pd_filter_info,
      sql::ObBitVector &res)
  {
    const unsigned char *filter_col_data = col_data + pd_filter_info.start_ * sizeof(uint16_t);
    const uint16_t *stored_values = reinterpret_cast<const uint16_t*>(filter_col_data);
    uint16_t casted_node_value = *reinterpret_cast<const uint16_t*>(&node_value);
    constexpr static int op = ObCmpTypeToAvxOpMap<CMP_TYPE>::value_;
    const int64_t &row_cnt = pd_filter_info.count_;

    __m256i node_value_vec = _mm256_set1_epi16(casted_node_value);
    for (int64_t i = 0; i < row_cnt / 16; ++i) {
      __m256i data_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(filter_col_data + i * 32));
      res.reinterpret_data<uint16_t>()[i] = _mm256_cmp_epu16_mask(data_vec, node_value_vec, op);
    }

    for (int64_t row_id = row_cnt / 16 * 16;
        row_id < row_cnt; ++row_id) {
      if (value_cmp_t<uint16_t, CMP_TYPE>(stored_values[row_id], casted_node_value)) {
        res.set(row_id);
      }
    }
    LOG_DEBUG("[SIMD filter] fast filter for 2 byte unsigned data",
        K(pd_filter_info), K(row_cnt), K(node_value), K(casted_node_value), K(op),
        "stored_values", common::ObArrayWrap<uint16_t>(stored_values, row_cnt));
  }
};

template <int CMP_TYPE>
struct RawFixFilterAVX512Func_T<1, 0, CMP_TYPE>
{
  // Fast filter with SIMD for 1 byte signed data
  static void fix_filter_func(
      const unsigned char *col_data,
      const uint64_t node_value,
      const sql::PushdownFilterInfo &pd_filter_info,
      sql::ObBitVector &res)
  {
    const unsigned char *filter_col_data = col_data + pd_filter_info.start_ * sizeof(int8_t);
    const int8_t *stored_values = reinterpret_cast<const int8_t*>(filter_col_data);
    int8_t casted_node_value = *reinterpret_cast<const int8_t*>(&node_value);
    constexpr static int op = ObCmpTypeToAvxOpMap<CMP_TYPE>::value_;
    const int64_t &row_cnt = pd_filter_info.count_;

    __m128i node_value_vec = _mm_set1_epi8(casted_node_value);
    for (int64_t i = 0; i < row_cnt / 16; ++i) {
      __m128i data_vec = _mm_loadu_si128(reinterpret_cast<const __m128i *>(filter_col_data + i * 16));
      res.reinterpret_data<uint16_t>()[i] = _mm_cmp_epi8_mask(data_vec, node_value_vec, op);
    }

    for (int64_t row_id = row_cnt / 16 * 16;
        row_id < row_cnt; ++row_id) {
      if (value_cmp_t<int8_t, CMP_TYPE>(stored_values[row_id], casted_node_value)) {
        res.set(row_id);
      }
    }
    LOG_DEBUG("[SIMD filter] fast filter for 1 byte signed data",
        K(pd_filter_info), K(row_cnt), K(node_value), K(casted_node_value), K(op),
        "stored_values", common::ObArrayWrap<int8_t>(stored_values, row_cnt));
  }
};

template <int CMP_TYPE>
struct RawFixFilterAVX512Func_T<0, 0, CMP_TYPE>
{
  // Fast filter with SIMD for 1 byte unsigned data
  static void fix_filter_func(
      const unsigned char *col_data,
      const uint64_t node_value,
      const sql::PushdownFilterInfo &pd_filter_info,
      sql::ObBitVector &res)
  {
    const unsigned char *filter_col_data = col_data + pd_filter_info.start_ * sizeof(uint8_t);
    const uint8_t *stored_values = reinterpret_cast<const uint8_t*>(filter_col_data);
    uint8_t casted_node_value = *reinterpret_cast<const uint8_t*>(&node_value);
    constexpr static int op = ObCmpTypeToAvxOpMap<CMP_TYPE>::value_;
    const int64_t &row_cnt = pd_filter_info.count_;

    __m128i node_value_vec = _mm_set1_epi8(casted_node_value);
    for (int64_t i = 0; i < row_cnt / 16; ++i) {
      __m128i data_vec = _mm_loadu_si128(reinterpret_cast<const __m128i *>(filter_col_data + i * 16));
      res.reinterpret_data<uint16_t>()[i] = _mm_cmp_epu8_mask(data_vec, node_value_vec, op);
    }

    for (int64_t row_id = row_cnt / 16 * 16;
        row_id < row_cnt; ++row_id) {
      if (value_cmp_t<uint8_t, CMP_TYPE>(stored_values[row_id], casted_node_value)) {
        res.set(row_id);
      }
    }
    LOG_DEBUG("[SIMD filter] fast filter for 1 byte unsigned data",
        K(pd_filter_info), K(row_cnt), K(node_value), K(casted_node_value), K(op),
        "stored_values", common::ObArrayWrap<uint8_t>(stored_values, row_cnt));
  }
};
#endif

template <int32_t IS_SIGNED, int32_t LEN_TAG, int32_t CMP_TYPE>
struct RawFixFilterAVX512ArrayInit
{
  bool operator()()
  {
    raw_fix_fast_filter_funcs[IS_SIGNED][LEN_TAG][CMP_TYPE]
        = &(RawFixFilterAVX512Func_T<IS_SIGNED, LEN_TAG, CMP_TYPE>::fix_filter_func);
    return true;
  }
};

bool init_raw_fix_simd_filter_funcs()
{
  return ObNDArrayIniter<RawFixFilterAVX512ArrayInit, 2, 4, 6>::apply();
}

} // end of namespace blocksstable
} // end of namespace oceanbase
