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
#include "ob_sample_filter.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_errno.h"
#include "ob_index_tree_prefetcher.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"

namespace oceanbase
{
namespace storage
{
//////////////////////////////////////////ObRowSampleFilter/////////////////////////////////////
ObSampleFilterExecutor::ObSampleFilterExecutor(
    common::ObIAllocator &alloc,
    ObPushdownSampleFilterNode &filter,
    sql::ObPushdownOperator &op)
    : ObPushdownFilterExecutor(alloc, op, sql::PushdownExecutorType::SAMPLE_FILTER_EXECUTOR),
      row_num_(0),
      seed_(-1),
      percent_(100),
      boundary_point_(-1),
      pd_row_range_(OB_INVALID_ROW_ID, OB_INVALID_ROW_ID),
      block_row_range_(OB_INVALID_ROW_ID, OB_INVALID_ROW_ID),
      interval_infos_(),
      data_row_id_handle_(nullptr),
      index_row_id_handle_(nullptr),
      filter_(filter),
      allocator_(&alloc),
      block_statistic_(),
      index_prefetch_depth_(0),
      data_prefetch_depth_(0),
      index_tree_height_(0),
      filter_state_(false),
      is_reverse_scan_(false),
      is_inited_(false)
{
}
ObSampleFilterExecutor::~ObSampleFilterExecutor()
{
  reset();
}
void ObSampleFilterExecutor::reset()
{
  is_reverse_scan_ = false;
  row_num_ = 0;
  percent_ = 100;
  seed_ = -1;
  reset_pushdown_ranges();
  filter_state_ = false;
  boundary_point_ = -1;
  interval_infos_.reset();
  reset_row_id_handle();
  index_tree_height_ = 0;
  index_prefetch_depth_ = 0;
  data_prefetch_depth_ = 0;
  allocator_ = nullptr;
  block_statistic_.reset();
  is_inited_ = false;
}
void ObSampleFilterExecutor::reuse()
{
  row_num_ = 0;
  filter_state_ = false;
  boundary_point_ = -1;
  if (nullptr != index_row_id_handle_) {
    const int64_t index_handle_max_cnt =
      ObIndexTreeMultiPassPrefetcher<>::MAX_INDEX_PREFETCH_DEPTH * ObIndexTreeMultiPassPrefetcher<>::MAX_INDEX_TREE_HEIGHT;
    MEMSET(static_cast<void *>(index_row_id_handle_), 0, sizeof(ObIndexRowIdHandle) * index_handle_max_cnt);
  }
  if (nullptr != data_row_id_handle_) {
    const int64_t data_handle_max_cnt =
      ObIndexTreeMultiPassPrefetcher<>::MAX_DATA_PREFETCH_DEPTH;
    MEMSET(static_cast<void *>(data_row_id_handle_), 0, sizeof(int64_t) * data_handle_max_cnt);
  }
  reset_pushdown_ranges();
}
void ObSampleFilterExecutor::reset_row_id_handle()
{
  if (nullptr != allocator_) {
    if (nullptr != index_row_id_handle_) {
      allocator_->free(index_row_id_handle_);
      index_row_id_handle_ = nullptr;
    }
    if (nullptr != data_row_id_handle_) {
      allocator_->free(data_row_id_handle_);
      data_row_id_handle_ = nullptr;
    }
  }
}
int ObSampleFilterExecutor::init(
    const common::SampleInfo &sample_info,
    const bool is_reverse_scan,
    common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObRowSampleFilter has been inited", K(ret));
  } else if (OB_UNLIKELY(!sample_info.is_row_sample() || nullptr == allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init ObRowSampleFilter", K(ret), K(sample_info), KP(allocator));
  } else if (OB_FAIL(init_sample_segment_length(sample_info.percent_))) {
    LOG_WARN("Failed to init sample segment length", K(ret), K(sample_info));
  } else {
    row_num_ = 0;
    percent_ = sample_info.percent_;
    seed_ = sample_info.seed_;
    if (seed_ == -1) {
      // seed is not specified, generate random seed
      seed_ = ObTimeUtility::current_time();
    }
    is_reverse_scan_ = is_reverse_scan;
    reset_pushdown_ranges();
    filter_state_ = false;
    boundary_point_ = -1;
    index_tree_height_ = 0;
    index_prefetch_depth_ = 0;
    data_prefetch_depth_ = 0;
    data_row_id_handle_ = nullptr;
    index_row_id_handle_ = nullptr;
    allocator_ = allocator;
    block_statistic_.reset();
    is_inited_ = true;
  }
  return ret;
}
int ObSampleFilterExecutor::init_sample_segment_length(const double percent)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(percent < 0.000001 || percent >= 100.0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected sample percent", K(ret), K_(percent));
  } else {
    int64_t interval_length = 1;
    while(static_cast<double>(interval_length) * percent / 100.0 < 1.0) {
      interval_length <<= 1;
    }
    int64_t expand_start = (EXPAND_INTERVAL_START / interval_length + 1) * interval_length;
    if (OB_FAIL(interval_infos_.push_back({0, interval_length}))) {
      LOG_WARN("Failed to push element to interval_infos_", K(ret), K_(interval_infos));
    } else if (OB_FAIL(interval_infos_.push_back({expand_start, MAX(interval_length, EXPAND_INTERVAL_LENGTH)}))) {
      LOG_WARN("Failed to push element to interval_infos_", K(ret), K_(interval_infos));
    }
    LOG_DEBUG("wenye check: sample_segment_length_ has been set", K(interval_length), K(expand_start));
  }
  return ret;
}
int ObSampleFilterExecutor::build_row_id_handle(
    const int16_t height,
    const int32_t index_handle_cnt,
    const int32_t data_handle_cnt)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  int64_t index_handle_max_cnt = ObIndexTreeMultiPassPrefetcher<>::MAX_INDEX_PREFETCH_DEPTH * ObIndexTreeMultiPassPrefetcher<>::MAX_INDEX_TREE_HEIGHT;
  int64_t data_handle_max_cnt = ObIndexTreeMultiPassPrefetcher<>::MAX_DATA_PREFETCH_DEPTH;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObRowSampleFilter has not been inited", K(ret));
  } else if (OB_UNLIKELY(height <= 0 || index_handle_cnt <= 0 || data_handle_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The argument to build row id handle is not valid", K(ret), K(height), K(index_handle_cnt), K(data_handle_cnt));
  } else if (nullptr != index_row_id_handle_) {
    MEMSET(static_cast<void *>(index_row_id_handle_), 0, sizeof(ObIndexRowIdHandle) * index_handle_max_cnt);
    MEMSET(static_cast<void *>(data_row_id_handle_), 0, sizeof(int64_t) * data_handle_max_cnt);
    index_tree_height_ = height;
    index_prefetch_depth_ = index_handle_cnt;
    data_prefetch_depth_ = data_handle_cnt;
  } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObIndexRowIdHandle) * index_handle_max_cnt + sizeof(int64_t) * data_handle_max_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc memory for index_row_id_handle and data_row_id_handle", K(ret), K(index_handle_max_cnt), K(data_handle_max_cnt));
  } else {
    index_row_id_handle_ = static_cast<ObIndexRowIdHandle *>(buf);
    data_row_id_handle_ = reinterpret_cast<int64_t *>(index_row_id_handle_ + index_handle_max_cnt);
    MEMSET(static_cast<void *>(index_row_id_handle_), 0, sizeof(ObIndexRowIdHandle) * index_handle_max_cnt);
    MEMSET(static_cast<void *>(data_row_id_handle_), 0, sizeof(int64_t) * data_handle_max_cnt);
    index_tree_height_ = height;
    index_prefetch_depth_ = index_handle_cnt;
    data_prefetch_depth_ = data_handle_cnt;
    LOG_DEBUG("wenye debug, build row id handle", K_(index_tree_height), K_(index_prefetch_depth), K_(data_prefetch_depth),
                                                  K(index_handle_max_cnt), K(data_handle_max_cnt));
  }
  if (OB_FAIL(ret)) {
    reset_row_id_handle();
  }
  return ret;
}
int ObSampleFilterExecutor::increase_row_num(const int64_t count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObRowSampleFilter has not been inited", K(ret));
  } else if (OB_UNLIKELY(count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The row_num increased should be positive", K(ret), K(count));
  } else {
    row_num_ += count;
  }
  return ret;
}
int ObSampleFilterExecutor::update_row_num_after_blockscan()
{
  int ret = OB_SUCCESS;
  if (!pd_row_range_.is_valid()) {
  } else if (OB_FAIL(increase_row_num(pd_row_range_.get_row_count()))) {
    LOG_WARN("Failed to update row num after blockscan", K(ret), K_(pd_row_range), K_(row_num));
  } else {
    reset_pushdown_ranges();
  }
  return ret;
}
int ObSampleFilterExecutor::update_pd_row_range(const int64_t start, const int64_t end, const bool in_cg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid(start) || start > end)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The prefetch filtered range is invalid", K(ret), K(start), K(end));
  } else if (in_cg && is_reverse_scan_) {
    if(!is_valid(pd_row_range_.end())) {
      pd_row_range_.set_end(end);
    }
    pd_row_range_.set_begin(start);
  } else {
    if (!is_valid(pd_row_range_.begin())) {
        pd_row_range_.set_begin(start);
    }
    pd_row_range_.set_end(end);
  }
  return ret;
}
int ObSampleFilterExecutor::check_single_row_filtered(const int64_t row_num, bool &filtered)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_num < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid row num to check whether filtered", K(ret), K(row_num));
  } else if (row_num > boundary_point_) {
    ObSampleIntervalParser interval_parser(percent_, seed_);
    // [left, right] is the row num range to be sampled.
    // In order to reduce hash and modulo calculations,
    // record the boundary of sampling and non-sampling row num to reuse it for future checks.
    if (OB_FAIL(parse_interval_info(row_num, interval_parser))) {
      LOG_WARN("Failed to parse interval info", K(ret), K(row_num), K(interval_parser));
    } else if (row_num < interval_parser.left_) {
      boundary_point_ = interval_parser.left_ - 1;
      filtered = true;
    } else if (row_num > interval_parser.right_) {
      boundary_point_ = interval_parser.next_interval() - 1;
      filtered = true;
    } else {
      boundary_point_ = interval_parser.right_;
      filtered = false;
    }
    filter_state_ = filtered;
  } else {
    filtered = filter_state_;
  }
  if (OB_SUCC(ret) && OB_FAIL(increase_row_num(1))) {
    LOG_WARN("Failed to increase row num", K(ret));
  }
  return ret;
}
int ObSampleFilterExecutor::check_filtered_after_fuse(bool &filtered)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObRowSampleFilter has not been inited", K(ret));
  } else if (OB_FAIL(update_row_num_after_blockscan())) {
    LOG_WARN("Failed to update row num when fuse", K(ret));
  } else if (OB_FAIL(check_single_row_filtered(row_num_, filtered))) {
    LOG_WARN("Failed to check whether single row filtered", K(ret));
  }
  return ret;
}
int ObSampleFilterExecutor::parse_interval_info(
    const int64_t row_num,
    ObSampleIntervalParser &interval_parser) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_num < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to parse interval info", K(ret), K(row_num));
  } else {
    int idx = row_num >= interval_infos_[EXPAND_INTERVAL_INDEX].start_ ? EXPAND_INTERVAL_INDEX : EXPAND_INTERVAL_INDEX - 1;
    interval_parser.parse(row_num, interval_infos_[idx]);
  }
  return ret;
}
int ObSampleFilterExecutor::check_sample_block(
    blocksstable::ObMicroIndexInfo &index_info,
    const int64_t level,
    const int64_t parent_fetch_idx,
    const int64_t child_prefetch_idx,
    const bool has_lob_out)
{
  int ret = OB_SUCCESS;
  int64_t start_row_id = 0;
  int64_t start_row_num = 0;
  int64_t end_row_num = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObRowSampleFilter has not been inited", K(ret));
  } else if (OB_UNLIKELY(level < 0 || level > index_tree_height_ || parent_fetch_idx < 0 || child_prefetch_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to check sample block", K(ret), K(level), K(parent_fetch_idx), K(child_prefetch_idx), K(has_lob_out));
  } else if (OB_FAIL(update_row_id_handle(level, parent_fetch_idx, child_prefetch_idx, index_info.get_row_count()))) {
    LOG_WARN("Failed to update row id in sample", K(ret), K(level), K(parent_fetch_idx), K(child_prefetch_idx), K(index_info.get_row_count()));
  } else if (!index_info.can_blockscan(has_lob_out) || !pd_row_range_.is_valid()) {
    LOG_DEBUG("Can not filter micro block in sample", K_(pd_row_range), K(index_info), K(has_lob_out));
  } else {
    if (level == index_tree_height_) {
      start_row_id = data_row_id_handle_[child_prefetch_idx % data_prefetch_depth_];
    } else {
      start_row_id = index_row_id_handle_[level * index_prefetch_depth_ + child_prefetch_idx % index_prefetch_depth_].start_;
    }
    if (OB_UNLIKELY(start_row_id < pd_row_range_.begin())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The first row id is invalid when apply sample filter", K(ret), K_(pd_row_range), K(start_row_id));
    } else {
      start_row_num = row_num_ + start_row_id - pd_row_range_.begin();
      end_row_num = start_row_num + index_info.get_row_count() - 1;
      block_statistic_.inc_block_count(index_info, end_row_num >= interval_infos_[EXPAND_INTERVAL_INDEX].start_);
      if (!can_sample_skip(index_info, has_lob_out)) {
      } else if (OB_FAIL(check_range_filtered(index_info, start_row_num))) {
        LOG_WARN("Failed to check range filtered in sample", K(ret), K(start_row_num), K(end_row_num));
      } else if (index_info.is_filter_always_false()) {
        block_statistic_.update_filter_status();
      }
    }
  }
  return ret;
}
int64_t ObSampleFilterExecutor::get_range_sample_count(
    const int64_t start_row_num,
    const int64_t end_row_num,
    int64_t &sample_count) const
{
  int ret = OB_SUCCESS;
  ObSampleIntervalParser interval_parser(percent_, seed_);
  int64_t count1 = 0;
  int64_t count2 = 0;
  int64_t start_iid = 0;
  if (OB_UNLIKELY(start_row_num < 0 || end_row_num < start_row_num)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected range", K(ret), K(start_row_num), K(end_row_num));
  } else if (OB_FAIL(parse_interval_info(start_row_num, interval_parser))) {
    LOG_WARN("Failed to parse interval info", K(ret), K(start_row_num), K(interval_parser));
  } else {
    // If start_row_num is greater the right point of leftmost sample interval, subtract total row num of the leftmost sample interval.
    // Otherwise, subtract the row num of non-intersecting part between the check range and the leftmost sample interval.
    // A negative value implies the check range inclusives the entire leftmost sample interval or can be handled in right side, so there is no need to subtract it.
    count1 = start_row_num > interval_parser.right_ ? interval_parser.count_ : MAX(start_row_num - interval_parser.left_, 0);
    start_iid = interval_parser.interval_id_;
    LOG_DEBUG("left interval_info", K(interval_parser));
  }

  if (OB_SUCC(ret)) {
    if (interval_parser.right_ < end_row_num && OB_FAIL(parse_interval_info(end_row_num, interval_parser))) {
      LOG_WARN("Failed to parse interval info", K(ret), K(end_row_num), K(interval_parser));
    } else {
      // The logic on the right side is similar to the left.
      count2 = interval_parser.left_ > end_row_num ? interval_parser.count_ : MAX(interval_parser.right_ - end_row_num, 0);
      LOG_DEBUG("right interval_info", K(interval_parser));
      sample_count = get_sample_count(start_iid, interval_parser.interval_id_, interval_parser.interval_length_) - count1 - count2;
    }
  }
  return ret;
}
int ObSampleFilterExecutor::check_range_filtered(
    blocksstable::ObMicroIndexInfo &index_info,
    const int64_t start_row_num)
{
  int ret = OB_SUCCESS;
  int64_t end_row_num = start_row_num + index_info.get_row_count() - 1;
  if (OB_UNLIKELY(start_row_num < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument when check range filtered", K(ret), K(start_row_num));
  } else if (start_row_num < interval_infos_[EXPAND_INTERVAL_INDEX].start_ && interval_infos_[EXPAND_INTERVAL_INDEX].start_ <= end_row_num) {
    // expand point in current range, not filtered.
  } else {
    int64_t range_sample_row_count = 0;
    if (OB_FAIL(get_range_sample_count(start_row_num, end_row_num, range_sample_row_count))) {
      LOG_WARN("Failed to get range sample count", K(ret), K(start_row_num), K(end_row_num), K(range_sample_row_count));
    } else {
      LOG_DEBUG("check range filtered in fast sample", K(start_row_num), K(end_row_num), K(range_sample_row_count), K(index_info));
      if (range_sample_row_count < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected row count to sample in this range", K(ret), K(range_sample_row_count), K(start_row_num), K(end_row_num));
      } else if (range_sample_row_count == 0) {
        index_info.set_filter_constant_type(sql::ObBoolMaskType::ALWAYS_FALSE);
        LOG_DEBUG("Range is filtered during perfetching when sample", K(start_row_num), K(index_info));
      } else {
        // need to sample row in this range, can not filtered.
      }
    }
  }
  return ret;
}
int ObSampleFilterExecutor::set_sample_bitmap(
    const int64_t start_row_num,
    const int64_t row_count,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start_row_num < 0 || row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to set sample bitmap", K(ret), K(start_row_num), K(row_count));
  } else {
    ObSampleIntervalParser interval_parser(percent_, seed_);
    int64_t row_index = 0;
    int64_t start_index = 0; // [start_index, end_index] is the index range to be set true in current bitmap.
    int64_t end_index = 0;
    while(OB_SUCC(ret) && row_index < row_count) {
      if (OB_FAIL(parse_interval_info(start_row_num + row_index, interval_parser))) {
        LOG_WARN("Failed to parse interval info", K(ret), K(start_row_num), K(row_index), K(interval_parser));
      } else {
        start_index = interval_parser.left_ - start_row_num < 0 ? 0 : interval_parser.left_ - start_row_num;
        start_index = MIN(start_index, row_count);
        end_index = interval_parser.right_ - start_row_num < 0 ? -1 : interval_parser.right_ - start_row_num;
        end_index = MIN(end_index, row_count - 1);
        if (is_reverse_scan_) {
          int64_t tmp_index = start_index;
          start_index = result_bitmap.size() - 1 - end_index;
          end_index = result_bitmap.size() - 1 - tmp_index;
        }
        LOG_DEBUG("set sample bitmap batch", K(start_row_num), K(start_index), K(row_count),
                                            K(interval_parser), K(row_index), K(end_index),
                                            K(result_bitmap), K_(pd_row_range), K_(row_num));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(result_bitmap.set_bitmap_batch(start_index, end_index - start_index + 1, true))) {
        LOG_WARN("Failed to set bitmap batch", K(ret), K(start_index), K(end_index), K(result_bitmap),
                                               K(interval_parser), K(start_row_num + row_index));
      } else {
        row_index = interval_parser.next_interval() - start_row_num;
      }
    }
  }
  return ret;
}
int ObSampleFilterExecutor::apply_sample_filter(
    sql::PushdownFilterInfo &filter_info,
    const bool is_major,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObRowSampleFilter has not been inited", K(ret));
  } else if (is_major) {
    if (OB_UNLIKELY(!block_row_range_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The sample range is not valid", K(ret), K_(block_row_range));
    } else if (OB_FAIL(update_pd_row_range(block_row_range_.begin(), block_row_range_.end()))) {
      LOG_WARN("Failed to update read status", K(ret), K_(block_row_range));
    } else if (OB_UNLIKELY(!pd_row_range_.is_valid() || block_row_range_.begin() < pd_row_range_.begin())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The first row id is invalid when apply sample filter", K(ret), K_(pd_row_range), K_(block_row_range));
    } else if (OB_FAIL(set_sample_bitmap(row_num_ + (block_row_range_.begin() - pd_row_range_.begin()), filter_info.count_, result_bitmap))) {
      LOG_WARN("Failed to set sample bitmap in major sstable", K(ret), K_(row_num), K_(block_row_range), K_(pd_row_range), K_(filter_info.count));
    }
  } else if (OB_FAIL(set_sample_bitmap(row_num_, filter_info.count_, result_bitmap))) {
    LOG_WARN("Failed to set sample bitmap in non-major sstable", K(ret), K_(row_num), K(filter_info.count_));
  }
  return ret;
}
int ObSampleFilterExecutor::apply_sample_filter(
    const ObCSRange &range,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int64_t start_row_num = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObRowSampleFilter has not been inited", K(ret));
  } else if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to apply sample filter in cg", K(ret), K(range));
  } else if (OB_FAIL(update_pd_row_range(range.begin(), range.end(), true))) {
    LOG_WARN("Failed to update read status", K(ret), K(range));
  } else if (OB_UNLIKELY(!pd_row_range_.is_valid()
                          || (is_reverse_scan_ && range.end() > pd_row_range_.end())
                          || (!is_reverse_scan_ && range.begin() < pd_row_range_.begin()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The first row id is invalid when apply sample filter in cg", K(ret), K_(is_reverse_scan), K_(pd_row_range), K(range));
  } else {
    start_row_num = is_reverse_scan_ ? row_num_ + (pd_row_range_.end() - range.end()) : row_num_ + (range.begin() - pd_row_range_.begin());
    if (OB_FAIL(set_sample_bitmap(start_row_num, range.get_row_count(), result_bitmap))) {
      LOG_WARN("Failed to set sample bitmap in cg", K(ret), K(start_row_num), K(range), K_(pd_row_range));
    }
  }
  return ret;
}
int ObSampleFilterExecutor::update_row_id_handle(
    const int64_t level,
    int32_t parent_fetch_idx,
    int64_t child_prefetch_idx,
    const int64_t row_count)
{
  int ret = OB_SUCCESS;
  int64_t parent_start = 0;
  int64_t parent_offset = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObRowSampleFilter has not been inited", K(ret));
  } else if (OB_UNLIKELY(level <= 0 || level > index_tree_height_
                          || parent_fetch_idx < 0 || child_prefetch_idx < 0
                          || row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to update row id hanle", K(ret), K(level), K(parent_fetch_idx),
                                                        K(child_prefetch_idx), K_(index_tree_height), K(row_count));
  } else {
    parent_fetch_idx %= index_prefetch_depth_;
    parent_start = index_row_id_handle_[(level - 1) * index_prefetch_depth_ + parent_fetch_idx].start_;
    parent_offset = index_row_id_handle_[(level - 1) * index_prefetch_depth_ + parent_fetch_idx].offset_;
    if (level < index_tree_height_) {
      child_prefetch_idx %= index_prefetch_depth_;
      index_row_id_handle_[level * index_prefetch_depth_ + child_prefetch_idx].start_ = parent_start + parent_offset;
      index_row_id_handle_[level * index_prefetch_depth_ + child_prefetch_idx].offset_ = 0;
    } else { // level == index_tree_height_ means prefetching micro data block
      child_prefetch_idx %= data_prefetch_depth_;
      data_row_id_handle_[child_prefetch_idx] = parent_start + parent_offset;
    }
    index_row_id_handle_[(level - 1) * index_prefetch_depth_ + parent_fetch_idx].offset_ = parent_offset + row_count;
    LOG_DEBUG("wenye debug: update row id handle", K(level), K(parent_fetch_idx), K(child_prefetch_idx),
                                                    K(row_count), K(parent_start), K(parent_offset));
  }
  return ret;
}

//////////////////////////////////////// ObRowSampleFilter /////////////////////////////////////////////
ObRowSampleFilter::ObRowSampleFilter()
    : sample_filter_(nullptr),
      sample_node_(nullptr),
      filter_(nullptr),
      filter_node_(nullptr),
      allocator_(nullptr),
      is_inited_(false)
{}
ObRowSampleFilter::~ObRowSampleFilter()
{
  if (OB_NOT_NULL(allocator_)) {
    if (nullptr != filter_) {
      filter_->~ObPushdownFilterExecutor();
      allocator_->free(filter_);
      filter_ = nullptr;
      sample_filter_ = nullptr;
    } else if (nullptr != sample_filter_) {
      sample_filter_->~ObPushdownFilterExecutor();
      allocator_->free(sample_filter_);
      sample_filter_ = nullptr;
    }
    if (nullptr != sample_node_) {
      allocator_->free(sample_node_);
      sample_node_ = nullptr;
    }
    if (nullptr != filter_node_) {
      allocator_->free(filter_node_);
      filter_node_ = nullptr;
    }
  }
  allocator_ = nullptr;
  is_inited_ = false;
}
int ObRowSampleFilter::init(
    const common::SampleInfo &sample_info,
    sql::ObPushdownOperator *op,
    const bool is_reverse_scan,
    common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObRowSampleFilter has been inited", K(ret));
  } else if (OB_UNLIKELY(!sample_info.is_row_sample() || nullptr == allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init ObRowSampleFilter", K(ret), K(sample_info), KP(allocator));
  } else {
    sql::ObPushdownFilterFactory filter_factory(allocator);
    if (OB_FAIL(filter_factory.alloc(sql::PushdownFilterType::SAMPLE_FILTER, 0, sample_node_))) {
      LOG_WARN("Failed to alloc pushdown sample filter node", K(ret));
    } else if (OB_FAIL(filter_factory.alloc(sql::PushdownExecutorType::SAMPLE_FILTER_EXECUTOR, 0, *sample_node_, sample_filter_, *op))) {
      LOG_WARN("Failed to alloc pushdown sample filter executor", K(ret));
    } else if (OB_FAIL(static_cast<ObSampleFilterExecutor *>(sample_filter_)->init(sample_info, is_reverse_scan, allocator))) {
      LOG_WARN("Failed to init ObSampleFilterExecutor", K(ret));
    } else {
      allocator_ = allocator;
      is_inited_ = true;
    }
  }
  return ret;
}
int ObRowSampleFilter::combine_to_filter_tree(sql::ObPushdownFilterExecutor *&root_filter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObRowSampleFilter has not been inited", K(ret));
  } else if (nullptr == root_filter) {
    root_filter = sample_filter_;
    LOG_DEBUG("The pushdown filter is null, only sample filter exists", KP(root_filter));
  } else {
    sql::ObPushdownFilterFactory filter_factory(allocator_);
    if (nullptr == filter_node_ && OB_FAIL(filter_factory.alloc(sql::PushdownFilterType::AND_FILTER, 2, filter_node_))) {
      LOG_WARN("Failed to alloc pushdown and filter node", K(ret));
    } else if (nullptr == filter_ && OB_FAIL(filter_factory.alloc(sql::PushdownExecutorType::AND_FILTER_EXECUTOR, 2, *filter_node_, filter_, root_filter->get_op()))) {
      LOG_WARN("Failed to alloc pushdown and filter executor", K(ret));
    } else {
      filter_->set_child(0, sample_filter_);
      filter_->set_child(1, root_filter);
      root_filter = filter_;
    }
  }
  return ret;
}

//////////////////////////////////////// ObRowSampleFilterFactory //////////////////////////////////////////////
int ObRowSampleFilterFactory::build_sample_filter(
    const common::SampleInfo &sample_info,
    ObRowSampleFilter *&sample_filter,
    sql::ObPushdownOperator *op,
    const bool is_reverse_scan,
    common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (nullptr == sample_filter) {
    ObRowSampleFilter *tmp_sample_filter = nullptr;
    if (OB_UNLIKELY(!sample_info.is_row_sample() || nullptr == allocator)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid argument to build sample filter", K(ret), K(sample_info), K(allocator));
    } else if (OB_ISNULL(tmp_sample_filter = OB_NEWx(ObRowSampleFilter, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to new ObRowSampleFilter", K(ret));
    } else if (OB_FAIL(tmp_sample_filter->init(sample_info, op, is_reverse_scan, allocator))) {
      LOG_WARN("Fail to init ObRowSampleExecutor", K(ret));
    }
    if (OB_SUCC(ret)) {
      sample_filter = tmp_sample_filter;
    } else if (nullptr != tmp_sample_filter) {
      tmp_sample_filter->~ObRowSampleFilter();
      allocator->free(tmp_sample_filter);
    }
  }
  return ret;
}
void ObRowSampleFilterFactory::destroy_sample_filter(ObRowSampleFilter *&sample_filter) {
  if (OB_NOT_NULL(sample_filter)) {
    int ret = OB_SUCCESS;
    ObSampleFilterExecutor *sample_executor = static_cast<ObSampleFilterExecutor *>(sample_filter->get_sample_executor());
    if (OB_FAIL(sample_executor->update_row_num_after_blockscan())) {
      LOG_WARN("Failed to update row num when destroy sample filter", K(ret), KPC(sample_executor));
    } else {
      int64_t row_num = sample_executor->get_row_num();
      LOG_TRACE("Total row num scanned in sample", K(ret), K(row_num));
      common::ObIAllocator *allocator = sample_filter->get_allocator();
      sample_filter->~ObRowSampleFilter();
      if (nullptr != allocator) {
        allocator->free(sample_filter);
      }
      sample_filter = nullptr;
    }
  }
}
} // namespace storage
} // namespace oceanbase
