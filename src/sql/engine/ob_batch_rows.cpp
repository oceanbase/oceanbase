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

#include "sql/engine/ob_batch_rows.h"
#include "common/ob_target_specific.h"
#if OB_USE_MULTITARGET_CODE
#include <immintrin.h>
#endif

namespace oceanbase
{
namespace sql
{

DEF_TO_STRING(ObBatchRows)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(size), K_(end), KP(skip_),
       "skip_bit_vec", ObLogPrintHex(reinterpret_cast<char *>(skip_),
                                     NULL == skip_ ? 0 : ObBitVector::memory_size(size_)));
  J_OBJ_END();
  return pos;
}

OB_DECLARE_AVX512_SPECIFIC_CODE(inline void apply_filter_to_brs(
                                    const int64_t *filter,
                                    ObBatchRows &brs) {
  uint8_t *dst = reinterpret_cast<uint8_t *>(brs.skip_->data_);
  __m512i zero = _mm512_setzero_si512();
  int i = 0;
  for (; i + 8 < brs.size_; i += 8) {
    __m512i vec = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(filter + i));
    uint8_t mask = _mm512_cmpeq_epi64_mask(vec, zero);
    if (mask == 0) {
    } else if (mask == -1) {
      (*dst) |= 0xFF;
      brs.all_rows_active_ = false;
    } else {
      (*dst) |= mask;
      brs.all_rows_active_ = false;
    }
    dst += 1;
  }
  for (; i < brs.size_; ++i) {
    if (filter[i] == 0) {
      brs.skip_->set(i);
      brs.all_rows_active_ = false;
    }
  }
};)

OB_DECLARE_DEFAULT_CODE(inline void apply_filter_to_brs(
                            const int64_t *filter, ObBatchRows &brs) {
  uint8_t *dst = reinterpret_cast<uint8_t *>(brs.skip_->data_);
  int i = 0;
  for (; i + 8 < brs.size_; i += 8) {
    (*dst) |= ((uint8_t)(((uint8_t)(filter[i + 0] == 0)) << 0));
    (*dst) |= ((uint8_t)(((uint8_t)(filter[i + 1] == 0)) << 1));
    (*dst) |= ((uint8_t)(((uint8_t)(filter[i + 2] == 0)) << 2));
    (*dst) |= ((uint8_t)(((uint8_t)(filter[i + 3] == 0)) << 3));
    (*dst) |= ((uint8_t)(((uint8_t)(filter[i + 4] == 0)) << 4));
    (*dst) |= ((uint8_t)(((uint8_t)(filter[i + 5] == 0)) << 5));
    (*dst) |= ((uint8_t)(((uint8_t)(filter[i + 6] == 0)) << 6));
    (*dst) |= ((uint8_t)(((uint8_t)(filter[i + 7] == 0)) << 7));
    brs.all_rows_active_ = brs.all_rows_active_ ? (*dst) == 0 : false;
    dst += 1;
  }
  for (; i < brs.size_; ++i) {
    if (filter[i] == 0) {
      brs.skip_->set(i);
      brs.all_rows_active_ = false;
    }
  }
};)

void ObBatchRows::apply_filter(const int64_t *filter) {
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::AVX512)) {
    specific::avx512::apply_filter_to_brs(filter, *this);
  } else {
    specific::normal::apply_filter_to_brs(filter, *this);
  }
#else
  specific::normal::apply_filter_to_brs(filter, *this);
#endif
}

void ObBatchRows::merge_skip(const ObBitVector *skip, int size)
{
  if (size != size_) {
    ob_abort();
  }
  skip_->bit_or(*skip, 0, size);
}
} // end namespace sql
} // end namespace oceanbase
