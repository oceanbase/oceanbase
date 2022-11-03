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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_px_bloom_filter.h"
#if defined(__x86_64__)
#include <immintrin.h>
#endif

using namespace oceanbase;
using namespace common;
using namespace sql;

#define LOG_HASH_COUNT 2

int ObPxBloomFilter::might_contain_simd(uint64_t hash, bool &is_match)
{
  int ret = OB_SUCCESS;
#if defined(__x86_64__)
  static const __m256i HASH_VALUES_MASK = _mm256_set_epi64x(24, 16, 8, 0);
  uint32_t hash_high = (uint32_t)(hash >> 32);
  uint64_t block_begin = (hash & ((bits_count_ >> (LOG_HASH_COUNT + 6)) - 1)) << LOG_HASH_COUNT;
  __m256i bit_ones = _mm256_set1_epi64x(1);
  __m256i hash_values = _mm256_set1_epi64x(hash_high);
  hash_values = _mm256_srlv_epi64(hash_values, HASH_VALUES_MASK);
  hash_values = _mm256_rolv_epi64(bit_ones, hash_values);
  __m256i bf_values = _mm256_load_si256(reinterpret_cast<__m256i *>(&bits_array_[block_begin]));
  is_match = 1 == _mm256_testz_si256(~bf_values, hash_values);
#else
  ret = might_contain_nonsimd(hash, is_match);
#endif
  return ret;
}

