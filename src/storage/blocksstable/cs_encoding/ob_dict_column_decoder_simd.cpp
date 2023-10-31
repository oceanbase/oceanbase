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

#include "ob_dict_column_decoder.h"

namespace oceanbase {
namespace blocksstable {
template <int32_t REF_WIDTH_TAG, int32_t CMP_TYPE>
struct ObCSFilterDictCmpRefAVX512Function : public ObCSFilterDictCmpRefFunction<REF_WIDTH_TAG, CMP_TYPE>
{};

#if defined ( __AVX512BW__ )
// 1 byte
template <int CMP_TYPE>
struct ObCSFilterDictCmpRefAVX512Function<0, CMP_TYPE>
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
    switch (CMP_TYPE) {
      case sql::WHITE_OP_LT:
      case sql::WHITE_OP_LE:
      case sql::WHITE_OP_EQ: {
        __m128i dict_ref_vec = _mm_set1_epi8(casted_dict_ref); // set sixteen uint8 integer values
        for (int64_t i = 0; i < row_cnt / 16; ++i) {
          __m128i cur_ref_vec = _mm_loadu_si128(reinterpret_cast<const __m128i *>(dict_ref_start_buf + i * 16));
          __mmask16 cmp_ref_res = _mm_cmp_epu8_mask(cur_ref_vec, dict_ref_vec, op);
          result.reinterpret_data<uint16_t>()[i] = cmp_ref_res;
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
        __m128i dict_ref_vec = _mm_set1_epi8(casted_dict_ref);
        __m128i null_val_vec = _mm_set1_epi8(casted_null_val);
        for (int64_t i = 0; i < row_cnt / 16; ++i) {
          __m128i cur_ref_vec = _mm_loadu_si128(reinterpret_cast<const __m128i *>(dict_ref_start_buf + i * 16));
          __mmask16 cmp_ref_res = _mm_cmp_epu8_mask(cur_ref_vec, dict_ref_vec, op);
          __mmask16 cmp_null_res = _mm_cmp_epu8_mask(cur_ref_vec, null_val_vec, 5); // op == 5, means GE
          result.reinterpret_data<uint16_t>()[i] = cmp_ref_res & (~cmp_null_res);
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
struct ObCSFilterDictCmpRefAVX512Function<1, CMP_TYPE>
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
    switch (CMP_TYPE) {
      case sql::WHITE_OP_LT:
      case sql::WHITE_OP_LE:
      case sql::WHITE_OP_EQ: {
        __m256i dict_ref_vec = _mm256_set1_epi16(casted_dict_ref); // set sixteen uint16 integer values
        for (int64_t i = 0; i < row_cnt / 16; ++i) {
          // 16 * 2bytes
          __m256i cur_ref_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(dict_ref_start_buf + i * 32));
          __mmask16 cmp_ref_res = _mm256_cmp_epu16_mask(cur_ref_vec, dict_ref_vec, op);
          result.reinterpret_data<uint16_t>()[i] = cmp_ref_res;
        }
        for (int64_t row_id = row_cnt / 16 * 16; row_id < row_cnt; ++row_id) {
          if (value_cmp_t<uint16_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
            result.set(row_id);
          }
        }
        break;
      }
      case sql::WHITE_OP_GT:
      case sql::WHITE_OP_GE:
      case sql::WHITE_OP_NE: {
        __m256i dict_ref_vec = _mm256_set1_epi16(casted_dict_ref);
        __m256i null_val_vec = _mm256_set1_epi16(casted_null_val);
        for (int64_t i = 0; i < row_cnt / 16; ++i) {
          __m256i cur_ref_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(dict_ref_start_buf + i * 32));
          __mmask16 cmp_ref_res = _mm256_cmp_epu16_mask(cur_ref_vec, dict_ref_vec, op);
          __mmask16 cmp_null_res = _mm256_cmp_epu16_mask(cur_ref_vec, null_val_vec, 5); // op == 5, means GE
          result.reinterpret_data<uint16_t>()[i] = cmp_ref_res & (~cmp_null_res);
        }
        for (int64_t row_id = row_cnt / 16 * 16; row_id < row_cnt; ++row_id) {
          if (value_cmp_t<uint16_t, sql::WHITE_OP_GE>(ref_arr[row_id], casted_null_val)) {
            // means null value
          } else if (value_cmp_t<uint16_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
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

// 4 byte
template <int CMP_TYPE>
struct ObCSFilterDictCmpRefAVX512Function<2, CMP_TYPE>
{
  static void dict_cmp_ref_func(
      const int64_t dict_ref_val,
      const int64_t null_val,
      const char *dict_ref_buf,
      const sql::PushdownFilterInfo &pd_filter_info,
      sql::ObBitVector &result)
  {
    const char *dict_ref_start_buf = dict_ref_buf + pd_filter_info.start_ * sizeof(uint32_t);
    const uint32_t *ref_arr = reinterpret_cast<const uint32_t *>(dict_ref_start_buf);
    const uint32_t casted_dict_ref = *reinterpret_cast<const uint32_t *>(&dict_ref_val);
    const uint32_t casted_null_val = *reinterpret_cast<const uint32_t *>(&null_val);
    constexpr static int op = ObCmpTypeToAvxOpMap<CMP_TYPE>::value_;
    int64_t row_cnt = pd_filter_info.count_;
    switch (CMP_TYPE) {
      case sql::WHITE_OP_LT:
      case sql::WHITE_OP_LE:
      case sql::WHITE_OP_EQ: {
        __m512i dict_ref_vec = _mm512_set1_epi32(casted_dict_ref); // set sixteen uint32 integer values
        for (int64_t i = 0; i < row_cnt / 16; ++i) {
          // 16 * 4bytes
          __m512i cur_ref_vec = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(dict_ref_start_buf + i * 64));
          __mmask16 cmp_ref_res = _mm512_cmp_epu32_mask(cur_ref_vec, dict_ref_vec, op);
          result.reinterpret_data<uint16_t>()[i] = cmp_ref_res;
        }
        for (int64_t row_id = row_cnt / 16 * 16; row_id < row_cnt; ++row_id) {
          if (value_cmp_t<uint32_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
            result.set(row_id);
          }
        }
        break;
      }
      case sql::WHITE_OP_GT:
      case sql::WHITE_OP_GE:
      case sql::WHITE_OP_NE: {
        __m512i dict_ref_vec = _mm512_set1_epi32(casted_dict_ref);
        __m512i null_val_vec = _mm512_set1_epi32(casted_null_val);
        for (int64_t i = 0; i < row_cnt / 16; ++i) {
          __m512i cur_ref_vec = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(dict_ref_start_buf + i * 64));
          __mmask16 cmp_ref_res = _mm512_cmp_epu32_mask(cur_ref_vec, dict_ref_vec, op);
          __mmask16 cmp_null_res = _mm512_cmp_epu32_mask(cur_ref_vec, null_val_vec, 5); // op == 5, means GE
          result.reinterpret_data<uint16_t>()[i] = cmp_ref_res & (~cmp_null_res);
        }
        for (int64_t row_id = row_cnt / 16 * 16; row_id < row_cnt; ++row_id) {
          if (value_cmp_t<uint32_t, sql::WHITE_OP_GE>(ref_arr[row_id], casted_null_val)) {
            // means null value
          } else if (value_cmp_t<uint32_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
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

// 8 byte
template <int CMP_TYPE>
struct ObCSFilterDictCmpRefAVX512Function<3, CMP_TYPE>
{
  static void dict_cmp_ref_func(
      const int64_t dict_ref_val,
      const int64_t null_val,
      const char *dict_ref_buf,
      const sql::PushdownFilterInfo &pd_filter_info,
      sql::ObBitVector &result)
  {
    const char *dict_ref_start_buf = dict_ref_buf + pd_filter_info.start_ * sizeof(uint64_t);
    const uint64_t *ref_arr = reinterpret_cast<const uint64_t *>(dict_ref_start_buf);
    const uint64_t casted_dict_ref = *reinterpret_cast<const uint64_t *>(&dict_ref_val);
    const uint64_t casted_null_val = *reinterpret_cast<const uint64_t *>(&null_val);
    constexpr static int op = ObCmpTypeToAvxOpMap<CMP_TYPE>::value_;
    int64_t row_cnt = pd_filter_info.count_;
    switch (CMP_TYPE) {
      case sql::WHITE_OP_LT:
      case sql::WHITE_OP_LE:
      case sql::WHITE_OP_EQ: {
        __m512i dict_ref_vec = _mm512_set1_epi64(casted_dict_ref); // set eight uint64 integer values
        for (int64_t i = 0; i < row_cnt / 8; ++i) {
          // 8 * 8bytes
          __m512i cur_ref_vec = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(dict_ref_start_buf + i * 64));
          __mmask8 cmp_ref_res = _mm512_cmp_epu64_mask(cur_ref_vec, dict_ref_vec, op);
          result.reinterpret_data<uint8_t>()[i] = cmp_ref_res;
        }
        for (int64_t row_id = row_cnt / 8 * 8; row_id < row_cnt; ++row_id) {
          if (value_cmp_t<uint64_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
            result.set(row_id);
          }
        }
        break;
      }
      case sql::WHITE_OP_GT:
      case sql::WHITE_OP_GE:
      case sql::WHITE_OP_NE: {
        __m512i dict_ref_vec = _mm512_set1_epi64(casted_dict_ref);
        __m512i null_val_vec = _mm512_set1_epi64(casted_null_val);
        for (int64_t i = 0; i < row_cnt / 8; ++i) {
          __m512i cur_ref_vec = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(dict_ref_start_buf + i * 64));
          __mmask8 cmp_ref_res = _mm512_cmp_epu64_mask(cur_ref_vec, dict_ref_vec, op);
          __mmask8 cmp_null_res = _mm512_cmp_epu64_mask(cur_ref_vec, null_val_vec, 5); // op == 5, means GE
          result.reinterpret_data<uint8_t>()[i] = cmp_ref_res & (~cmp_null_res);
        }
        for (int64_t row_id = row_cnt / 8 * 8; row_id < row_cnt; ++row_id) {
          if (value_cmp_t<uint64_t, sql::WHITE_OP_GE>(ref_arr[row_id], casted_null_val)) {
            // means null value
          } else if (value_cmp_t<uint64_t, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
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
struct CSDictFastCmpAVX512ArrayInit
{
  bool operator()()
  {
    cs_dict_fast_cmp_funcs[REF_WIDTH_TAG][CMP_TYPE]
        = &(ObCSFilterDictCmpRefAVX512Function<REF_WIDTH_TAG, CMP_TYPE>::dict_cmp_ref_func);
    return true;
  }
};

bool init_cs_dict_fast_cmp_simd_func()
{
  return ObNDArrayIniter<CSDictFastCmpAVX512ArrayInit, 4, 6>::apply();
}

} // end of namespace blocksstable
} // end of namespace oceanbase
