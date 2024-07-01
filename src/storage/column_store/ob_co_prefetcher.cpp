// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#define USING_LOG_PREFIX STORAGE
#include "ob_co_prefetcher.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;
namespace storage {

//////////////////////////////////////// ObCOPrefetcher //////////////////////////////////////////////

void ObCOPrefetcher::reset()
{
  set_range_scan_finish();
  block_scan_border_row_id_ = OB_INVALID_CS_ROW_ID;
  ObIndexTreeMultiPassPrefetcher::reset();
}

void ObCOPrefetcher::reuse()
{
  set_range_scan_finish();
  block_scan_border_row_id_ = OB_INVALID_CS_ROW_ID;
  ObIndexTreeMultiPassPrefetcher::reuse();
  for (int i = 0; i < max_range_prefetching_cnt_; ++i) {
    read_handles_[i].reuse();
  }
}

int ObCOPrefetcher::refresh_blockscan_checker(const int64_t start_micro_idx, const ObDatumRowkey &border_rowkey)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("[COLUMNSTORE] COPrefetcher refresh_blockscan_checker [start]", K(border_rowkey), K_(block_scan_state), K(start_micro_idx));
  if (OB_UNLIKELY(!border_rowkey.is_valid() || 0 > start_micro_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to check range block scan", K(ret), K(border_rowkey), K(start_micro_idx));
  } else if (!is_in_row_store_scan_mode()) {
    // TODO(hanling): ObMultipleScanMerge will call 'refresh_blockscan_checker' each time
    // when 'consume_count' == 1 after 'get_next_row', it can be modified in the future.
    // ret = OB_ERR_UNEXPECTED;
    // LOG_WARN("Unexpected block scan state", K(ret), K_(block_scan_state));
  } else {
    // -1 == cur_micro_data_fetch_idx_: we does not fetch any data micro block yet.
    // We give up vectorized query for the whole micro data block if there are any out rows
    // in the current implementation.
    border_rowkey_ = border_rowkey;
    block_scan_state_ = PENDING_SWITCH;
  }
  LOG_TRACE("[COLUMNSTORE] COPrefetcher refresh_blockscan_checker [end]", K(ret), K(border_rowkey), K_(block_scan_state), K(start_micro_idx),
            K_(micro_data_prefetch_idx), K_(block_scan_start_row_id), K_(block_scan_border_row_id));
  return ret;
}

int ObCOPrefetcher::refresh_blockscan_checker_for_column_store(const int64_t start_micro_idx, const ObDatumRowkey &border_rowkey)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("[COLUMNSTORE] COPrefetcher refresh_blockscan_checker [start]", K(border_rowkey), K_(block_scan_state), K(start_micro_idx));
  if (OB_UNLIKELY(!border_rowkey.is_valid() || 0 >= start_micro_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to check range block scan", K(ret), K(border_rowkey), K(start_micro_idx));
  } else if (!is_in_row_store_scan_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected block scan state", K(ret), K_(block_scan_state));
  } else {
    const bool is_reverse = access_ctx_->query_flag_.is_reverse_scan();
    int64_t idx = (start_micro_idx - 1) % max_micro_handle_cnt_;
    const bool is_range_end = micro_data_infos_[idx].is_end_border(is_reverse);
    if (is_range_end) {
      // (start_micro_idx - 1) points to the last data block in current range.
      // There are no more data blocks in current range.
      bool is_last = false;
      if (OB_FAIL(is_last_range(is_last))) {
        LOG_WARN("Failed to judge last range", K(ret), K(border_rowkey), K(start_micro_idx));
      } else if (!is_last) {
        // There are more ranges in this scan.
        // Go to columnar scan for next range.
        border_rowkey_ = border_rowkey;
        block_scan_state_ = PENDING_SWITCH;
      } else {
        // Current range is the last range.
      }
      LOG_DEBUG("ObCOPrefetcher::refresh_blockscan_checker", K_(block_scan_state));
    } else if (OB_FAIL(refresh_blockscan_checker_internal(start_micro_idx, border_rowkey))) {
      LOG_WARN("Failed to refresh blockscan checker", K(ret), K(border_rowkey), K(start_micro_idx));
    }
  }
  LOG_TRACE("[COLUMNSTORE] COPrefetcher refresh_blockscan_checker [end]", K(ret), K(border_rowkey), K_(block_scan_state), K(start_micro_idx),
            K_(micro_data_prefetch_idx), K_(block_scan_start_row_id), K_(block_scan_border_row_id));
  return ret;
}

// Advance each level from top to bottom by border_rowkey.
// Skip prefetch data blocks and index blocks as many as possible.
int ObCOPrefetcher::refresh_blockscan_checker_internal(
    const int64_t start_micro_idx,
    const ObDatumRowkey &border_rowkey)
{
  int ret = OB_SUCCESS;
  const bool is_reverse = access_ctx_->query_flag_.is_reverse_scan();
  bool is_advanced_to = false;
  // After call refresh_each_levels :
  // a. is_advance_to is true, we will continue to skip prefetch
  //    from get_cur_level_of_block_scan() to (n - 1) level in columnar store scan mode later.
  // b. is_advance_to is false, we will continue to check leaf level in this function.
  if (OB_FAIL(refresh_each_levels(border_rowkey, is_advanced_to))) {
    LOG_WARN("Failed to refresh each levels", K(ret), K(start_micro_idx));
  }

  // Advance leaf level if needed.
  if (OB_SUCC(ret) && !is_advanced_to) {
    ObSSTableReadHandle &read_handle = read_handles_[cur_range_fetch_idx_ % max_range_prefetching_cnt_];
    if (start_micro_idx > read_handle.micro_end_idx_) {
      check_leaf_level_without_more_prefetch_data(start_micro_idx);
    } else if (OB_FAIL(refresh_blockscan_checker_in_leaf_level(
                        start_micro_idx,
                        border_rowkey))) {
      LOG_WARN("Failed to refresh blockscan checker in leaf level",
                K(ret), K(border_rowkey), K(start_micro_idx));
    }
  }

  // Update block_scan_start_row_id_.
  if (OB_SUCC(ret)) {
    if (!is_in_row_store_scan_mode()) {
      border_rowkey_ = border_rowkey;
      int64_t start_idx = (start_micro_idx - 1) % max_micro_handle_cnt_;
      block_scan_start_row_id_ = is_reverse ? micro_data_infos_[start_idx].cs_row_range_.begin() - 1
                                              : micro_data_infos_[start_idx].cs_row_range_.end() + 1;
      if (OB_UNLIKELY(block_scan_start_row_id_ < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected start row id", K(ret), K(border_rowkey),
                  K_(block_scan_start_row_id), K_(block_scan_border_row_id),
                  K_(block_scan_state), K(start_micro_idx), K_(micro_data_prefetch_idx));
      }
    } else {
      int32_t cur_block_scan_level = get_cur_level_of_block_scan();
      if (INVALID_LEVEL != cur_block_scan_level) {
        reset_blockscan_for_target_level(cur_block_scan_level);
      }
    }
  }
  return ret;
}

int ObCOPrefetcher::refresh_each_levels(
    const ObDatumRowkey &border_rowkey,
    bool &is_advanced_to)
{
  int ret = OB_SUCCESS;
  is_advanced_to = false;
  const int32_t range_idx = cur_range_fetch_idx_;
  const bool is_reverse = access_ctx_->query_flag_.is_reverse_scan();
  int32_t prev_block_scan_level = INVALID_LEVEL;
  int32_t level = 0;
  for (; OB_SUCC(ret) && level < index_tree_height_; level++) {
    auto &tree_handle = static_cast<ObCOIndexTreeLevelHandle &>(tree_handles_[level]);
    if (0 != level && OB_FAIL(tree_handle.try_advancing_fetch_idx(*this, border_rowkey, range_idx, is_reverse,
                                                                  level, is_advanced_to))) {
      LOG_WARN("Failed to try advancing fetch_idx", K(ret), K(border_rowkey), K(range_idx));
    } else if (!is_advanced_to) {
      // After advance_to_border return,
      // a. if is_advance_to == true, We should reset tree_level_handle from current_level + 1 to leaf level.
      //   a.1 if index_scanner_ is not end, current_level + 1 should be set block scan.
      //   a.2 if index_scanner_ is end, current_level should be set block scan.
      // b. if is_advance_to == false, We should continue to advance_to_border in current_level + 1.
      ObCSRange cs_range;
      if (tree_handle.reach_scanner_end()) {
      } else if (OB_FAIL(tree_handle.advance_to_border(border_rowkey, range_idx, cs_range))) {
        LOG_WARN("Failed to advance to border in this level", K(ret), K(border_rowkey), K(level));
      } else if (FALSE_IT(is_advanced_to = cs_range.is_valid())) {
      } else if (is_advanced_to) {
        block_scan_border_row_id_ = is_reverse ? cs_range.end_row_id_ + 1
                                                 : cs_range.start_row_id_ - 1;
        LOG_DEBUG("ObCOPrefetcher::refresh_blockscan_checker_internal tree_handle advance",
                   K(ret), K(is_advanced_to));
      }
    }

    if (OB_SUCC(ret)) {
      if (is_advanced_to) {
        if (OB_FAIL(reset_each_levels(level + 1))) {
          LOG_WARN("Failed to reset each level", K(ret), K(level));
        } else {
          block_scan_state_ = PENDING_BLOCK_SCAN;
          // When reach here, never reach scanner end if level is 0.
          if (tree_handle.reach_scanner_end()) {
            // For non-reverse scan, maybe we should detect more rows in this level.
            update_blockscan_for_target_level(prev_block_scan_level, level);
          } else {
            update_blockscan_for_target_level(prev_block_scan_level, level + 1);
          }
          // It is better to set cur_level_ to appropriate value:
          // levels from (cur_level_ + 1) to (n - 1) are consumed up.
          cur_level_ = level;
          break;
        }
      } else if (0 != level && tree_handle.prefetch_idx_ <= tree_handle.fetch_idx_
                  && tree_handle.reach_scanner_end()) {
        // If we cannot advance current level and current level reaches end,
        // current level is regarded as next block scan level.
        if (INVALID_LEVEL != prev_block_scan_level) {
          update_blockscan_for_target_level(prev_block_scan_level, level);
          prev_block_scan_level = level;
        }
      } else if (INVALID_LEVEL != prev_block_scan_level) {
        reset_blockscan_for_target_level(prev_block_scan_level);
        prev_block_scan_level = INVALID_LEVEL;
      }
    }
  }
  return ret;
}

int ObCOPrefetcher::reset_each_levels(uint32_t start_level)
{
  int ret = OB_SUCCESS;
  // It is safe to use tree_handle.prefetch_idx_ and micro_data_prefetch_idx_ directly.
  // We assert that blocks prefetched from (start_level) to leaf level are all in the current range.
  for (uint32_t level = start_level; OB_SUCC(ret) && level < index_tree_height_; ++level) {
    ObIndexTreeLevelHandle &tree_handle = tree_handles_[level];
    tree_handle.index_block_read_handles_[tree_handle.prefetch_idx_ % INDEX_TREE_PREFETCH_DEPTH].end_prefetched_row_idx_
      = tree_handle.index_block_read_handles_[tree_handle.fetch_idx_ % INDEX_TREE_PREFETCH_DEPTH].end_prefetched_row_idx_;
    tree_handle.read_idx_ = tree_handle.prefetch_idx_;
    tree_handle.fetch_idx_ = tree_handle.prefetch_idx_;
    tree_handle.index_scanner_.reuse();
  }
  // cur_micro_data_fetch_idx_ point to next data micro block to be prefetched.
  cur_micro_data_fetch_idx_ = micro_data_prefetch_idx_;
  return ret;
}

void ObCOPrefetcher::check_leaf_level_without_more_prefetch_data(const int64_t start_micro_idx)
{
  int32_t cur_block_scan_level = get_cur_level_of_block_scan();
  const bool is_reverse = access_ctx_->query_flag_.is_reverse_scan();
  if (INVALID_LEVEL != cur_block_scan_level && !is_reverse) {
    // In this case, there are no more blocks prefetched in leaf level.
    // We should skip prefetch from cur_block_scan_level in columnar store scan mode later.
    int64_t start_idx = (start_micro_idx - 1) % max_micro_handle_cnt_;
    block_scan_border_row_id_ = is_reverse ? micro_data_infos_[start_idx].cs_row_range_.begin()
                                             : micro_data_infos_[start_idx].cs_row_range_.end();
    ++cur_micro_data_fetch_idx_;
    block_scan_state_ = PENDING_BLOCK_SCAN;
  }
}

int ObCOPrefetcher::refresh_blockscan_checker_in_leaf_level(
    const int64_t start_micro_idx,
    const blocksstable::ObDatumRowkey &border_rowkey)
{
  int ret = OB_SUCCESS;
  // Assert start_micro_idx <= read_handle.micro_end_idx_.
  const bool is_reverse = access_ctx_->query_flag_.is_reverse_scan();
  ObSSTableReadHandle &read_handle = read_handles_[cur_range_fetch_idx_ % max_range_prefetching_cnt_];
  int64_t micro_end_idx = read_handle.micro_end_idx_;

  // pos points to the next data micro block that we should open.
  int64_t pos = start_micro_idx;
  if (micro_end_idx >= start_micro_idx) {
    can_blockscan_ = true;
    if (OB_FAIL(check_data_infos_border(start_micro_idx, micro_end_idx, border_rowkey, is_reverse))) {
      LOG_WARN("Failed to check data infos border", K(ret), K(border_rowkey), K(start_micro_idx));
    } else if (can_blockscan_) {
      can_blockscan_ = false;
      pos = micro_end_idx + 1;
    } else {
      for (; pos <= micro_end_idx; ++pos) {
        if (!micro_data_infos_[pos % max_micro_handle_cnt_].can_blockscan_) {
          break;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObMicroIndexInfo &micro_index_info = micro_data_infos_[(pos - 1) % max_micro_handle_cnt_];
    bool is_range_end = (!is_reverse && micro_index_info.is_right_border())
                          || (is_reverse && micro_index_info.is_left_border());
    if (is_range_end) {
      --pos;
    }
    if (OB_LIKELY(pos >= start_micro_idx)) {
      ObCSRange &cs_range = micro_data_infos_[(pos - 1) % max_micro_handle_cnt_].cs_row_range_;
      block_scan_border_row_id_ = is_reverse ? cs_range.begin() : cs_range.end();
      // cur_micro_data_fetch_idx_ point to next data micro block to be fetched.
      cur_micro_data_fetch_idx_ = pos;
      int32_t cur_block_scan_level = get_cur_level_of_block_scan();
      if (pos <= micro_end_idx
           || INVALID_LEVEL == cur_block_scan_level
           || is_reverse) {
        if (INVALID_LEVEL != cur_block_scan_level) {
          reset_blockscan_for_target_level(cur_block_scan_level);
        }
        block_scan_state_ = IN_END_OF_RANGE;
      } else {
        block_scan_state_ = PENDING_BLOCK_SCAN;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unepected state when refresh block scan", K(ret), K(start_micro_idx), K(is_range_end),
                K(pos), K_(block_scan_state));
    }
    LOG_DEBUG("ObCOPrefetcher::refresh_blockscan_checker_in_leaf_level advance",
               K(ret), K(start_micro_idx), K(pos), K(is_range_end), K_(block_scan_state));
  }
  LOG_DEBUG("ObCOPrefetcher::refresh_blockscan_checker_in_leaf_level end",
             K(ret), K(start_micro_idx), K(pos), K(border_rowkey), KPC(this));
  return ret;
}

int ObCOPrefetcher::advance_index_tree_level(
    ObCOIndexTreeLevelHandle &co_index_tree_level_handle)
{
  int ret = OB_SUCCESS;
  bool is_advanced_to = false;
  ObCSRange cs_range;
  if (OB_FAIL(co_index_tree_level_handle.advance_to_border(border_rowkey_,
                                                           cur_range_fetch_idx_,
                                                           cs_range))) {
    LOG_WARN("Failed to advance to border", K(ret), K_(border_rowkey));
  } else if (FALSE_IT(is_advanced_to = cs_range.is_valid())) {
  } else if (is_advanced_to) {
    const bool is_reverse = access_ctx_->query_flag_.is_reverse_scan();
    block_scan_border_row_id_ = is_reverse ? cs_range.end_row_id_ + 1
                                             : cs_range.start_row_id_ - 1;
  }
  if (OB_SUCC(ret)) {
    if (OB_LIKELY(!co_index_tree_level_handle.reach_scanner_end())) {
      if (OB_FAIL(try_jumping_to_next_skip_level(co_index_tree_level_handle, false))) {
        LOG_WARN("Failed to try jumping to next skip level", K(ret), K_(border_rowkey));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected state", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObCOPrefetcher::skip_prefetch(
    ObCOIndexTreeLevelHandle &co_index_tree_level_handle)
{
  int ret = OB_SUCCESS;
  int16_t level = caculate_level(co_index_tree_level_handle);
  LOG_DEBUG("Skip prefetch", K(get_cur_level_of_block_scan()), K(level));
  // Advance to border in current level.
  if (is_blockscan_in_target_level(level)
       && OB_FAIL(advance_index_tree_level(co_index_tree_level_handle))) {
    LOG_WARN("Failed to advance in index level", K(ret), K_(cur_range_fetch_idx));
  }
  return ret;
}

int ObCOPrefetcher::try_jumping_to_next_skip_level(
    ObCOIndexTreeLevelHandle &co_index_tree_level_handle,
    bool is_out_of_range)
{
  int ret = OB_SUCCESS;
  int16_t level = caculate_level(co_index_tree_level_handle);
  LOG_DEBUG("Skip prefetch", K(get_cur_level_of_block_scan()), K(level));
  if (is_blockscan_in_target_level(level)) {
    if (OB_UNLIKELY(is_out_of_range)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected is_out_of_range", K(ret), K(is_out_of_range),
                K(co_index_tree_level_handle));
    } else {
      update_blockscan_for_target_level(level, level + 1);
    }
  }
  return ret;
}

int ObCOPrefetcher::init_tree_handles(const int64_t count)
{
  int ret = OB_SUCCESS;
  if (count > tree_handle_cap_) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = access_ctx_->stmt_allocator_->alloc(
                   sizeof(ObCOIndexTreeLevelHandle) * count))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc memory", K(ret), K(count));
    } else {
      reset_tree_handles();
      tree_handles_ = new (buf) ObCOIndexTreeLevelHandle[count];
      tree_handle_cap_ = count;
    }
  }
  return ret;
}

//////////////////////////////////////// ObCOIndexTreeLevelHandle //////////////////////////////////////////////

int ObCOPrefetcher::ObCOIndexTreeLevelHandle::try_advancing_fetch_idx(
    ObCOPrefetcher &prefetcher,
    const ObDatumRowkey &border_rowkey,
    const int32_t range_idx,
    const bool is_reverse,
    const int32_t level,
    bool &is_advanced_to)
{
  int ret = OB_SUCCESS;
  is_advanced_to = false;
  int64_t prefetch_idx = prefetch_idx_;
  LOG_DEBUG("[COLUMNSTORE] ObCOIndexTreeLevelHandle try_advancing_fetch_idx", K(fetch_idx_),
             K(prefetch_idx_), K(prefetch_idx), K(border_rowkey));
  // Check from prefetch_idx to (fetch_idx_ + 1).
  // fetch_idx_ is the current index_scanner_ which points to.
  if (OB_UNLIKELY(fetch_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected fetch_idx in ObCOIndexTreeLevelHandle", K(ret), K(range_idx), K(level), KPC(this));
  }
  for (; OB_SUCC(ret) && prefetch_idx >= fetch_idx_; --prefetch_idx) {
    const int32_t idx = prefetch_idx % INDEX_TREE_PREFETCH_DEPTH;
    ObMicroIndexInfo &index_info = index_block_read_handles_[idx].index_info_;
    bool is_range_end = !is_reverse && index_info.is_right_border();
    ObCommonDatumRowkey endkey;
    if (prefetch_idx == fetch_idx_) {
      if (OB_FAIL(index_scanner_.get_end_key(endkey))) {
        LOG_WARN("Failed to get end key", K(ret));
      }
    } else {
      endkey = index_info.endkey_;
    }
    // For reverse scan, there is no need to judge is_range_end.
    if (OB_FAIL(ret)) {
    } else if (range_idx != index_info.range_idx()) {
    } else {
      int cmp_ret = 0;
      if (OB_FAIL(endkey.compare(border_rowkey, *prefetcher.datum_utils_,
                                              cmp_ret, false))) {
        LOG_WARN("Failed to compare rowkey", K(ret), K(border_rowkey));
      } else if (!is_reverse) {
        if (cmp_ret < 0) {
          if (is_range_end) {
            // We cannot skip the whole index block if reach range border.
            --prefetch_idx;
          }
          if (prefetch_idx >= fetch_idx_) {
            is_advanced_to = true;
          }
          break;
        }
      } else if (cmp_ret >= 0) {
        // For reverse scan, if cmp_ret >= 0, the prev block is safe to skip.
        --prefetch_idx;
        if (prefetch_idx >= fetch_idx_) {
          is_advanced_to = true;
        }
        break;
      }
    }
  }
  if (OB_SUCC(ret) && is_advanced_to) {
    ObMicroIndexInfo &index_info =
      index_block_read_handles_[prefetch_idx % INDEX_TREE_PREFETCH_DEPTH].index_info_;
    index_block_read_handles_[prefetch_idx % INDEX_TREE_PREFETCH_DEPTH].end_prefetched_row_idx_ =
      index_block_read_handles_[fetch_idx_ % INDEX_TREE_PREFETCH_DEPTH].end_prefetched_row_idx_;
    read_idx_ = prefetch_idx;
    fetch_idx_ = prefetch_idx;
    index_scanner_.reuse();
    prefetcher.set_border_row_id(is_reverse ? index_info.get_row_range().begin()
                                              : index_info.get_row_range().end());
  }
  LOG_DEBUG("ObCOIndexTreeLevelHandle::try_advancing_fetch_idx",
             K(ret), K(is_advanced_to), K_(fetch_idx), K_(prefetch_idx), K_(read_idx));
  return ret;
}

int ObCOPrefetcher::ObCOIndexTreeLevelHandle::advance_to_border(
    const ObDatumRowkey &rowkey,
    const int32_t range_idx,
    ObCSRange &cs_range)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(index_scanner_.advance_to_border(rowkey, range_idx, cs_range))) {
    LOG_WARN("index scanner failed to advance to border", K(rowkey), K(range_idx));
  }
  return ret;
}

int ObCOPrefetcher::ObCOIndexTreeLevelHandle::forward(
    ObIndexTreeMultiPassPrefetcher &prefetcher,
    const bool has_lob_out)
{
  int ret = OB_SUCCESS;
  UNUSED(has_lob_out);
  ObCOPrefetcher &co_prefetcher = static_cast<ObCOPrefetcher &>(prefetcher);
  if (fetch_idx_ >= prefetch_idx_) {
    ret = OB_ITER_END;
  } else {
    fetch_idx_++;
    index_scanner_.reuse();
    int8_t fetch_idx = fetch_idx_ % INDEX_TREE_PREFETCH_DEPTH;
    ObMicroIndexInfo &index_info = index_block_read_handles_[fetch_idx].index_info_;
    if (OB_FAIL(index_block_read_handles_[fetch_idx].data_handle_.get_micro_block_data(nullptr, index_block_, false))) {
      LOG_WARN("Fail to get index block data", K(ret), KPC(this), K(index_block_), K(fetch_idx_), K(prefetch_idx_));
    } else if (index_info.is_get()) {
      if (OB_FAIL(index_scanner_.open(
          index_info.get_macro_id(),
          index_block_,
          index_info.get_query_key(),
          index_info.range_idx()))) {
        LOG_WARN("Fail to open index scanner", K(ret), KPC(this));
      }
    } else if (OB_FAIL(index_scanner_.open(
        index_info.get_macro_id(),
        index_block_,
        index_info.get_query_range(),
        index_info.range_idx(),
        index_info.is_left_border(),
        index_info.is_right_border(),
        &index_info))) {
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("Fail to open index scanner", K(ret), KPC(this));
      }
    }

    if (co_prefetcher.need_skip_prefetch()) {
      // skip_prefetch check columnar store block scan.
      if (OB_SUCC(ret) && OB_FAIL(co_prefetcher.skip_prefetch(*this))) {
        LOG_WARN("Failed to skip prefetch for column store",
                  K(ret), K(co_prefetcher), K(index_info.range_idx()));
      } else if (OB_LIKELY(OB_BEYOND_THE_RANGE == ret)) {
        // Current level is end in current range, try advancing skip level.
        if (OB_FAIL(co_prefetcher.try_jumping_to_next_skip_level(*this, true))) {
          LOG_WARN("Failed to try jumping to next skip level", K(ret), K_(co_prefetcher.border_rowkey));
        } else {
          ret = OB_SUCCESS;
        }
      }
    } else if (OB_LIKELY(OB_BEYOND_THE_RANGE == ret)) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret) && 0 < fetch_idx_) {
      index_block_read_handles_[fetch_idx].end_prefetched_row_idx_ =
          index_block_read_handles_[(fetch_idx_ - 1) % INDEX_TREE_PREFETCH_DEPTH].end_prefetched_row_idx_;
    }
  }
  return ret;
}

}
}
