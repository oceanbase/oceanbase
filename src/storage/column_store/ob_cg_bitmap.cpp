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

int ObCGBitmap::get_row_ids(
    int64_t *row_ids,
    int64_t &row_cap,
    ObCSRowId &current,
    const ObCSRange &query_range,
    const ObCSRange &data_range,
    const int64_t batch_size,
    const bool is_reverse) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == row_ids ||
                  current < start_row_id_ ||
                  current < data_range.start_row_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(row_ids), K(current), K(data_range), KPC(this));
  } else if (!is_reverse) {
    int64_t offset = current - start_row_id_;
    if (OB_FAIL(bitmap_.get_row_ids(row_ids,
                                    row_cap,
                                    offset,
                                    MIN(query_range.end_row_id_, data_range.end_row_id_) - start_row_id_ + 1,
                                    batch_size,
                                    data_range.start_row_id_ - start_row_id_))) {
      LOG_WARN("Fail to get row_ids", K(ret), K(current), KPC(this));
    } else {
      current = start_row_id_ + offset;
      // update next valid current
      if (current <= query_range.end_row_id_) {
        int64_t next_true_pos;
        if (OB_FAIL(bitmap_.next_valid_idx(offset,
                                           query_range.end_row_id_ - current + 1,
                                           false,
                                           next_true_pos))) {
          LOG_WARN("Fail to get next valid index", K(ret));
        } else {
          current = (-1 == next_true_pos) ? OB_INVALID_CS_ROW_ID : start_row_id_ + next_true_pos;
        }
      }
    }
  } else {
    ObCSRowId lower_bound = MAX(query_range.start_row_id_, data_range.start_row_id_);
    while (current >= lower_bound && row_cap < batch_size) {
      if (test(current)) {
        row_ids[row_cap++] = current - data_range.start_row_id_;
      }
      current--;
    }
    // update next valid current
    if (current >= query_range.start_row_id_) {
      int64_t next_true_pos;
      if (OB_FAIL(bitmap_.next_valid_idx(query_range.start_row_id_ - start_row_id_,
                                         current - query_range.start_row_id_ + 1,
                                         true,
                                         next_true_pos))) {
        LOG_WARN("Fail to get next valid index", K(ret));
      } else {
        current = (-1 == next_true_pos) ? OB_INVALID_CS_ROW_ID : start_row_id_ + next_true_pos;
      }
    }
  }
  return ret;
}

static const int32_t DEFAULT_CS_BATCH_ROW_COUNT = 1024;
static int64_t default_cs_batch_row_ids_[DEFAULT_CS_BATCH_ROW_COUNT];
static int64_t default_cs_batch_reverse_row_ids_[DEFAULT_CS_BATCH_ROW_COUNT];
static void  __attribute__((constructor)) init_row_cs_ids_array()
{
  for (int32_t i = 0; i < DEFAULT_CS_BATCH_ROW_COUNT; i++) {
    default_cs_batch_row_ids_[i] = i;
    default_cs_batch_reverse_row_ids_[i] = DEFAULT_CS_BATCH_ROW_COUNT - i - 1;
  }
}

OB_DECLARE_DEFAULT_AND_AVX2_CODE(
OB_NOINLINE static bool copy_cs_row_ids(int64_t *row_ids, const int64_t cap, const int64_t diff, const bool is_reverse)
{
  bool is_success = false;
  int64_t val = diff;
  if (cap <= DEFAULT_CS_BATCH_ROW_COUNT) {
    is_success = true;
    if (!is_reverse) {
      MEMCPY(row_ids, default_cs_batch_row_ids_, sizeof(int64_t) * cap);
    } else {
      MEMCPY(row_ids, default_cs_batch_reverse_row_ids_, sizeof(int64_t) * cap);
      val = diff - DEFAULT_CS_BATCH_ROW_COUNT + 1;
    }
    int64_t* __restrict id_pos = row_ids;
    const int64_t* __restrict id_end = row_ids + cap;
    while (id_pos < id_end) {
      *id_pos += val;
      ++id_pos;
    }
  }
  return is_success;
})

int convert_bitmap_to_cs_index(int64_t *row_ids,
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
        bool is_success = false;
#if OB_USE_MULTITARGET_CODE
        if (common::is_arch_supported(ObTargetArch::AVX2)) {
          is_success = specific::avx2::copy_cs_row_ids(row_ids, limit, current - data_range.start_row_id_, false);
        } else {
          is_success = specific::normal::copy_cs_row_ids(row_ids, limit, current - data_range.start_row_id_, false);
        }
#else
        is_success = specific::normal::copy_cs_row_ids(row_ids, limit, current - data_range.start_row_id_, false);
#endif
        if (is_success) {
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
      } else if (OB_FAIL(filter_bitmap->get_row_ids(
                  row_ids, row_cap, current, query_range, data_range, batch_size, is_reverse_scan))) {
        LOG_WARN("Fail to get row ids", K(ret), K(current), K(data_range), K(query_range));
      }
    } else {
      ObCSRowId lower_bound = MAX(query_range.start_row_id_, data_range.start_row_id_);
      int64_t limit = MIN(current - lower_bound + 1, batch_size);
      if (nullptr == filter_bitmap || filter_bitmap->is_all_true(ObCSRange(current - limit + 1, limit))) {
        bool is_success = false;
#if OB_USE_MULTITARGET_CODE
        if (common::is_arch_supported(ObTargetArch::AVX2)) {
          is_success = specific::avx2::copy_cs_row_ids(row_ids, limit, current - data_range.start_row_id_, true);
        } else {
          is_success = specific::normal::copy_cs_row_ids(row_ids, limit, current - data_range.start_row_id_, true);
        }
#else
        is_success = specific::normal::copy_cs_row_ids(row_ids, limit, current - data_range.start_row_id_, true);
#endif
        if (is_success) {
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
      } else if (OB_FAIL(filter_bitmap->get_row_ids(
                  row_ids, row_cap, current, query_range, data_range, batch_size, is_reverse_scan))) {
        LOG_WARN("Fail to get row ids", K(ret), K(current), K(data_range), K(query_range));
      }
    }
  }
  return ret;
}

}
}
