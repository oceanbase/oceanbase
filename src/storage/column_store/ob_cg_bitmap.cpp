/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_cg_bitmap.h"
#include "common/ob_target_specific.h"

#if OB_USE_MULTITARGET_CODE
#include <emmintrin.h>
#include <immintrin.h>
#endif

namespace oceanbase
{
using namespace common;
namespace storage
{

int ObCGBitmap::get_first_valid_idx(const ObCSRange &range, const bool is_reverse_scan, ObCSRowId &row_idx) const
{
  int ret = OB_SUCCESS;
  int64_t valid_offset = -1;
  row_idx = OB_INVALID_CS_ROW_ID;
  if (OB_UNLIKELY(range.start_row_id_ < start_row_id_ || range.get_row_count() > bitmap_.size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(range), K_(start_row_id), K(bitmap_.size()));
  } else if (is_all_false(range)) {
  } else if (OB_FAIL(bitmap_.next_valid_idx(range.start_row_id_ - start_row_id_,
                                            range.get_row_count(),
                                            is_reverse_scan,
                                            valid_offset))){
    LOG_WARN("Fail to get next valid idx", K(ret), K(range), KPC(this));
  } else if (-1 != valid_offset) {
    row_idx = start_row_id_ + valid_offset;
  }
  return ret;
}

int ObCGBitmap::set_bitmap(const ObCSRowId start, const int64_t row_count, const bool is_reverse, ObBitmap &bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start < start_row_id_ || row_count != bitmap.size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(start), K_(start_row_id), K(row_count), K(bitmap));
  } else if (!is_reverse) {
    if (OB_FAIL(bitmap.copy_from(bitmap_, start - start_row_id_, row_count))) {
      LOG_WARN("Fail to copy bitmap", K(ret), K(start), K(row_count), KPC(this));
    }
  } else {
    for (int64_t i = 0; i < row_count; i++) {
      bitmap.set(row_count - 1 -i, test(start + i));
    }
  }
  return ret;
}

static const int32_t DEFAULT_CS_BATCH_ROW_COUNT = 1024;
static int32_t default_cs_batch_row_ids_[DEFAULT_CS_BATCH_ROW_COUNT];
static int32_t default_cs_batch_reverse_row_ids_[DEFAULT_CS_BATCH_ROW_COUNT];
static void  __attribute__((constructor)) init_row_cs_ids_array()
{
  for (int32_t i = 0; i < DEFAULT_CS_BATCH_ROW_COUNT; i++) {
    default_cs_batch_row_ids_[i] = i;
    default_cs_batch_reverse_row_ids_[i] = DEFAULT_CS_BATCH_ROW_COUNT - i - 1;
  }
}

OB_DECLARE_DEFAULT_AND_AVX2_CODE(
inline static void copy_cs_row_ids(int32_t *row_ids, const int64_t cap, const int32_t diff, const bool is_reverse)
{
  int32_t val = is_reverse ? (diff - DEFAULT_CS_BATCH_ROW_COUNT + 1) : diff;
  const int32_t* __restrict base_ids = is_reverse ? default_cs_batch_reverse_row_ids_ : default_cs_batch_row_ids_;
  int32_t* __restrict id_pos = row_ids;
  const int32_t* __restrict id_end = row_ids + cap;
  while (id_pos < id_end) {
    *id_pos = *base_ids + val;
    ++id_pos;
    ++base_ids;
  }
}
)

OB_DECLARE_AVX512_SPECIFIC_CODE(
/*
 * [from, to): inteval of bitmap to get row ids
 * limit: upper limit of row ids
 * block_offset: start bit index of current micro block
 */
inline void get_cs_row_ids(
    const int32_t *condensed_idx,
    const int32_t condensed_cnt,
    const int32_t from,
    const int32_t to,
    const int32_t limit,
    const int32_t block_offset,
    const bool is_reverse,
    int32_t *row_ids,
    int64_t &row_count,
    int32_t &next_valid_idx)
{
  row_count = 0;
  next_valid_idx = -1;
  if (from > condensed_idx[condensed_cnt - 1] || to <= condensed_idx[0]) {
  } else {
    int32_t pos1 = std::lower_bound(condensed_idx, condensed_idx + MIN(from + 1, condensed_cnt), from) - condensed_idx;
    if (pos1 < condensed_cnt) {
      int32_t pos2 = std::lower_bound(condensed_idx + pos1, condensed_idx + MIN(to + 1, condensed_cnt), to) - condensed_idx;
      int32_t count = MIN(pos2 - pos1, limit);
      if (0 == count) {
      } else if (!is_reverse) {
        const int32_t *pos = condensed_idx + pos1;
        const int32_t *end_pos = pos + count;
        const int32_t *end_pos16 = pos + count / 16 * 16;
        __m512i offset = _mm512_set1_epi32(-block_offset);
        for (; pos < end_pos16; pos += 16) {
          __m512i idx_arr = _mm512_loadu_epi32(pos);
          __m512i res_arr = _mm512_add_epi32(idx_arr, offset);
          _mm512_storeu_epi32(row_ids + row_count, res_arr);
          row_count += 16;
        }
        while (pos < end_pos) {
          row_ids[row_count++] = *pos - block_offset;
          ++pos;
        }
        if (pos < (condensed_idx + condensed_cnt)) {
          next_valid_idx = *pos;
        }
      } else {
        const int32_t* __restrict idx_pos = condensed_idx + pos2 - 1;
        const int32_t* __restrict idx_end = idx_pos - (count - 1);
        while (idx_pos >= idx_end) {
          row_ids[row_count++] = *idx_pos - block_offset;
          --idx_pos;
        }
        if (idx_pos >= condensed_idx) {
          next_valid_idx = *idx_pos;
        }
      }
    }
  }
}
)

OB_DECLARE_AVX2_SPECIFIC_CODE(
inline void get_cs_row_ids(
    const int32_t *condensed_idx,
    const int32_t condensed_cnt,
    const int32_t from,
    const int32_t to,
    const int32_t limit,
    const int32_t block_offset,
    const bool is_reverse,
    int32_t *row_ids,
    int64_t &row_count,
    int32_t &next_valid_idx)
{
  row_count = 0;
  next_valid_idx = -1;
  if (from > condensed_idx[condensed_cnt - 1] || to <= condensed_idx[0]) {
  } else {
    int32_t pos1 = std::lower_bound(condensed_idx, condensed_idx + MIN(from + 1, condensed_cnt), from) - condensed_idx;
    if (pos1 < condensed_cnt) {
      int32_t pos2 = std::lower_bound(condensed_idx + pos1, condensed_idx + MIN(to + 1, condensed_cnt), to) - condensed_idx;
      int32_t count = MIN(pos2 - pos1, limit);
      if (0 == count) {
      } else if (!is_reverse) {
        const int32_t *pos = condensed_idx + pos1;
        const int32_t *end_pos = pos + count;
        const int32_t *end_pos8 = pos + count / 8 * 8;
        __m256i offset = _mm256_set1_epi32(-block_offset);
        for (; pos < end_pos8; pos += 8) {
          __m256i idx_arr = _mm256_loadu_si256((const __m256i *)(pos));
          __m256i res_arr = _mm256_add_epi32(idx_arr, offset);
          _mm256_storeu_si256((__m256i *)(row_ids + row_count), res_arr);
          row_count += 8;
        }
        while (pos < end_pos) {
          row_ids[row_count++] = *pos - block_offset;
          ++pos;
        }
        if (pos < (condensed_idx + condensed_cnt)) {
          next_valid_idx = *pos;
        }
      } else {
        const int32_t* __restrict idx_pos = condensed_idx + pos2 - 1;
        const int32_t* __restrict idx_end = condensed_idx + pos1;
        while (row_count < limit && idx_pos >= idx_end) {
          row_ids[row_count++] = *idx_pos - block_offset;
          --idx_pos;
        }
        if (idx_pos >= condensed_idx) {
          next_valid_idx = *idx_pos;
        }
      }
    }
  }
}
)

OB_DECLARE_DEFAULT_CODE(
inline void get_cs_row_ids(
    const int32_t *condensed_idx,
    const int32_t condensed_cnt,
    const int32_t from,
    const int32_t to,
    const int32_t limit,
    const int32_t block_offset,
    const bool is_reverse,
    int32_t *row_ids,
    int64_t &row_count,
    int32_t &next_valid_idx)
{
  row_count = 0;
  next_valid_idx = -1;
  if (from > condensed_idx[condensed_cnt - 1] || to <= condensed_idx[0]) {
  } else {
    int pos1 = std::lower_bound(condensed_idx, condensed_idx + MIN(from + 1, condensed_cnt), from) - condensed_idx;
    if (pos1 < condensed_cnt) {
      int pos2 = std::lower_bound(condensed_idx + pos1, condensed_idx + MIN(to + 1, condensed_cnt), to) - condensed_idx;
      if (pos1 == pos2) {
      } else if (!is_reverse) {
        const int32_t* __restrict idx_pos = condensed_idx + pos1;
        const int32_t* __restrict idx_end = condensed_idx + pos2;
        while (row_count < limit && idx_pos < idx_end) {
          row_ids[row_count++] = *idx_pos - block_offset;
          ++idx_pos;
        }
        if (idx_pos < (condensed_idx + condensed_cnt)) {
          next_valid_idx = *idx_pos;
        }
      } else {
        const int32_t* __restrict idx_pos = condensed_idx + pos2 - 1;
        const int32_t* __restrict idx_end = condensed_idx + pos1;
        while (row_count < limit && idx_pos >= idx_end) {
          row_ids[row_count++] = *idx_pos - block_offset;
          --idx_pos;
        }
        if (idx_pos >= condensed_idx) {
          next_valid_idx = *idx_pos;
        }
      }
    }
  }
}
)

int convert_bitmap_to_cs_index(int32_t *row_ids,
                               int64_t &row_cap,
                               ObCSRowId &current,
                               const ObCSRange &query_range,
                               const ObCSRange &data_range,
                               const ObCGBitmap *filter_bitmap,
                               const int64_t batch_size,
                               const bool is_reverse_scan)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 != data_range.compare(current))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpceted opened index info", K(ret), K(current), K(data_range));
  } else {
    row_cap = 0;
    if (!is_reverse_scan) {
      ObCSRowId upper_bound = MIN(query_range.end_row_id_, data_range.end_row_id_);
      int64_t limit = MIN(upper_bound - current + 1, batch_size);
      if (nullptr == filter_bitmap || filter_bitmap->is_all_true(ObCSRange(current, limit))) {
        if (limit < DEFAULT_CS_BATCH_ROW_COUNT) {
        #if OB_USE_MULTITARGET_CODE
          if (common::is_arch_supported(ObTargetArch::AVX2)) {
            specific::avx2::copy_cs_row_ids(row_ids, limit, current - data_range.start_row_id_, false);
          } else {
        #endif
          specific::normal::copy_cs_row_ids(row_ids, limit, current - data_range.start_row_id_, false);
        #if OB_USE_MULTITARGET_CODE
          }
        #endif
          row_cap = limit;
          current += limit;
        } else {
          for (int64_t i = 0; i < limit; i++) {
            row_ids[row_cap++] = current - data_range.start_row_id_;
            current++;
          }
        }
      } else if (filter_bitmap->is_all_false(ObCSRange(current, upper_bound - current + 1))) {
        current = upper_bound + 1;
      } else if (OB_FAIL(const_cast<ObCGBitmap *>(filter_bitmap)->get_row_ids(
                  row_ids, row_cap, current, query_range, data_range, batch_size, is_reverse_scan))) {
        LOG_WARN("Fail to get row ids", K(ret), K(current), K(data_range), K(query_range));
      }
    } else {
      ObCSRowId lower_bound = MAX(query_range.start_row_id_, data_range.start_row_id_);
      int64_t limit = MIN(current - lower_bound + 1, batch_size);
      if (nullptr == filter_bitmap || filter_bitmap->is_all_true(ObCSRange(current - limit + 1, limit))) {
        if (limit < DEFAULT_CS_BATCH_ROW_COUNT) {
        #if OB_USE_MULTITARGET_CODE
          if (common::is_arch_supported(ObTargetArch::AVX2)) {
            specific::avx2::copy_cs_row_ids(row_ids, limit, current - data_range.start_row_id_, true);
          } else {
        #endif
          specific::normal::copy_cs_row_ids(row_ids, limit, current - data_range.start_row_id_, true);
        #if OB_USE_MULTITARGET_CODE
          }
        #endif
          row_cap = limit;
          current -= limit;
        } else {
          for (int64_t i = 0; i < limit; i++) {
            row_ids[row_cap++] = current - data_range.start_row_id_;
            current--;
          }
        }
      } else if (filter_bitmap->is_all_false(ObCSRange(lower_bound, current - lower_bound + 1))) {
        current = lower_bound - 1;
      } else if (OB_FAIL(const_cast<ObCGBitmap *>(filter_bitmap)->get_row_ids(
                  row_ids, row_cap, current, query_range, data_range, batch_size, is_reverse_scan))) {
        LOG_WARN("Fail to get row ids", K(ret), K(current), K(data_range), K(query_range));
      }
    }
  }
  return ret;
}

int ObCGBitmap::get_row_ids(
    int32_t *row_ids,
    int64_t &row_cap,
    ObCSRowId &current,
    const ObCSRange &query_range,
    const ObCSRange &data_range,
    const int64_t batch_size,
    const bool is_reverse)
{
  int ret = OB_SUCCESS;
  int32_t next_valid_idx = -1;
  int32_t from_pos = -1;
  int32_t end_pos = -1;
  int32_t condensed_cnt = -1;
  const int32_t *condensed_idx = nullptr;
  if (OB_UNLIKELY(nullptr == row_ids ||
                  current < start_row_id_ ||
                  current < data_range.start_row_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(row_ids), K(current), K(data_range), KPC(this));
  } else if (!bitmap_.is_index_generated() && OB_FAIL(bitmap_.generate_condensed_index())) {
    LOG_WARN("Fail to get condensed idx", K(ret));
  } else {
    if (!is_reverse) {
      from_pos = current - start_row_id_;
      end_pos = MIN(query_range.end_row_id_, data_range.end_row_id_) - start_row_id_ + 1;
    } else {
      from_pos = MAX(query_range.start_row_id_, data_range.start_row_id_) - start_row_id_;
      end_pos = current - start_row_id_ + 1;
    }
    condensed_cnt = bitmap_.get_condensed_cnt();
    condensed_idx = bitmap_.get_condensed_idx();
    if (OB_UNLIKELY(0 > condensed_cnt || nullptr == condensed_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpceted condensed idx info", K(ret), K_(bitmap));
    }
  }

  if (OB_SUCC(ret)) {
    if (0 == condensed_cnt) {
      row_cap = 0;
      next_valid_idx = -1;
#if OB_USE_MULTITARGET_CODE
    // enable when avx512 is more efficient
    //} else if (common::is_arch_supported(ObTargetArch::AVX512)) {
    //  specific::avx512::get_cs_row_ids(condensed_idx,
    //                                   condensed_cnt,
    //                                   from_pos,
    //                                   end_pos,
    //                                   batch_size,
    //                                   data_range.start_row_id_ - start_row_id_,
    //                                   is_reverse,
    //                                   row_ids,
    //                                   row_cap,
    //                                   next_valid_idx);
    } else if (common::is_arch_supported(ObTargetArch::AVX2)) {
      specific::avx2::get_cs_row_ids(condensed_idx,
                                     condensed_cnt,
                                     from_pos,
                                     end_pos,
                                     batch_size,
                                     data_range.start_row_id_ - start_row_id_,
                                     is_reverse,
                                     row_ids,
                                     row_cap,
                                     next_valid_idx);
#endif
    } else {
      specific::normal::get_cs_row_ids(condensed_idx,
                                       condensed_cnt,
                                       from_pos,
                                       end_pos,
                                       batch_size,
                                       data_range.start_row_id_ - start_row_id_,
                                       is_reverse,
                                       row_ids,
                                       row_cap,
                                       next_valid_idx);
    }

    if (-1 == next_valid_idx) {
      current = OB_INVALID_CS_ROW_ID;
    } else {
      current = start_row_id_ + next_valid_idx;
    }
  }
  return ret;
}

}
}
