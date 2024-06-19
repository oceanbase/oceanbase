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
  specific::avx512::inline_might_contain_simd(bits_array_, block_mask_, hash, is_match);
#else
  ret = might_contain_nonsimd(hash, is_match);
#endif
  return ret;
}

