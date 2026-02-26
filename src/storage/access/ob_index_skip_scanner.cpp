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
#include "ob_index_skip_scanner.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/memtable/ob_memtable_iterator.h"

ERRSIM_POINT_DEF(EN_INDEX_SKIP_SCAN_DISABLE_COUNT, "Disable index skip scan if total count is greater than this value");
ERRSIM_POINT_DEF(EN_INDEX_SKIP_SCAN_DISABLE_RATIO, "Disable index skip scan if ratio is less than this value");
#define GET_FINAL_COUNT_THRESHOLD(threshold) (0 == threshold ? COUNT_BASED_MAX_DISABLE_COUNT : std::abs(threshold))
#define GET_FINAL_RATIO_THRESHOLD(threshold) (0 == threshold ? SKIP_RATIO_THRESHOLD : std::abs(threshold) / 100.0)

namespace oceanbase
{
using namespace blocksstable;
using namespace memtable;
namespace storage
{

ObIndexSkipCtlStrategy::ObIndexSkipCtlStrategy(const ObIndexSkipCtlStrategyType type)
  : strategy_type_(type),
    total_row_count_(0),
    skipped_row_count_(0)
{
  const int64_t en_count = EN_INDEX_SKIP_SCAN_DISABLE_COUNT;
  final_count_threshold_ = GET_FINAL_COUNT_THRESHOLD(en_count);
  const double en_ratio = EN_INDEX_SKIP_SCAN_DISABLE_RATIO;
  final_ratio_threshold_ = GET_FINAL_RATIO_THRESHOLD(en_ratio);
}

void ObIndexSkipCtlStrategy::reset()
{
  strategy_type_ = ObIndexSkipCtlStrategyType::MAX_STATE;
  total_row_count_ = 0;
  skipped_row_count_ = 0;
}

int ObIndexSkipCtlStrategy::check_disabled(const ObIndexSkipState &state, bool &disabled) const
{
  int ret = OB_SUCCESS;
  disabled = false;
  switch (strategy_type_) {
    case ObIndexSkipCtlStrategyType::RATIO_BASED: {
      if (state.range_idx() < RATIO_BASED_MIN_CHECK_COUNT) {
        disabled = false;
      } else {
        disabled = state.range_idx() > RATIO_BASED_MAX_DISABLE_COUNT ||
        static_cast<double>(skipped_row_count_) / total_row_count_ < final_ratio_threshold_;
      }
      break;
    }
    case ObIndexSkipCtlStrategyType::COUNT_BASED: {
      disabled = state.range_idx() > final_count_threshold_;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected strategy type", KR(ret), K_(strategy_type));
    }
  }
  if (OB_SUCC(ret) && disabled) {
    LOG_INFO("[INDEX SKIP SCAN] disable index skip scan", K(state), KPC(this), K(lbt()));
  }
  LOG_TRACE("[INDEX SKIP SCAN] check disable", KR(ret), K(disabled), K(state), KPC(this));
  return ret;
}

ObIndexSkipScanner::ObIndexSkipScanner(const bool is_for_memtable, const ObStorageDatumUtils &datum_utils)
  : range_alloc_("SS_RANGE", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    prefix_alloc_("SS_PREFIX", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    is_inited_(false),
    is_disabled_(false),
    is_border_after_disabled_(false),
    is_reverse_scan_(false),
    is_prefix_filled_(false),
    is_scan_range_complete_(false),
    prefix_cnt_(0),
    micro_start_(0),
    micro_last_(0),
    micro_current_(0),
    prefix_range_idx_(0),
    index_skip_state_(),
    index_skip_strategy_(is_for_memtable ? ObIndexSkipCtlStrategyType::COUNT_BASED : ObIndexSkipCtlStrategyType::RATIO_BASED),
    datum_utils_(datum_utils),
    scan_range_(nullptr),
    skip_range_(nullptr),
    complete_range_(),
    prefix_range_(),
    range_datums_(nullptr),
    read_info_(nullptr),
    skip_scan_factory_(nullptr),
    stmt_alloc_(nullptr)
{}

ObIndexSkipScanner::~ObIndexSkipScanner()
{
  if (OB_LIKELY(nullptr != range_datums_ && nullptr != stmt_alloc_)) {
    stmt_alloc_->free(range_datums_);
  }
}

void ObIndexSkipScanner::reset()
{
  is_inited_ = false;
  is_disabled_ = false;
  is_border_after_disabled_ = false;
  is_reverse_scan_ = false;
  is_prefix_filled_ = false;
  is_scan_range_complete_ = false;
  prefix_cnt_ = 0;
  micro_start_ = 0;
  micro_last_ = 0;
  micro_current_ = 0;
  prefix_range_idx_ = 0;
  index_skip_state_.reset();
  index_skip_strategy_.reset();
  scan_range_ = nullptr;
  skip_range_ = nullptr;
  complete_range_.reset();
  prefix_range_.reset();
  if (OB_LIKELY(nullptr != range_datums_ && nullptr != stmt_alloc_)) {
    stmt_alloc_->free(range_datums_);
  }
  range_datums_ = nullptr;
  read_info_ = nullptr;
  skip_scan_factory_ = nullptr;
  stmt_alloc_ = nullptr;
  range_alloc_.reset();
  prefix_alloc_.reset();
}

int ObIndexSkipScanner::init(
    const bool is_reverse_scan,
    const int64_t prefix_cnt,
    const ObDatumRange &scan_range,
    const ObDatumRange &skip_range,
    const ObITableReadInfo &read_info,
    ObIAllocator &stmt_allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KP(this), K(lbt()));
  } else if (OB_UNLIKELY(prefix_cnt <= 0 || skip_range.is_whole_range())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(prefix_cnt), K(skip_range));
  } else if (OB_FAIL(prepare_ranges(is_reverse_scan, prefix_cnt, scan_range, skip_range,
                                    read_info.get_schema_rowkey_count(), stmt_allocator))) {
    LOG_WARN("failed to prepare range", KR(ret));
  } else {
    is_reverse_scan_ = is_reverse_scan;
    prefix_cnt_ = prefix_cnt;
    scan_range_ = &scan_range;
    skip_range_ = &skip_range;
    read_info_ = &read_info;
    stmt_alloc_ = &stmt_allocator;
    is_inited_ = true;
    LOG_TRACE("[INDEX SKIP SCAN] success to init", KR(ret), K(*this));
  }
  return ret;
}

int ObIndexSkipScanner::switch_info(
    const bool is_reverse_scan,
    const int64_t prefix_cnt,
    const ObDatumRange &scan_range,
    const ObDatumRange &skip_range,
    const ObITableReadInfo &read_info,
    ObIAllocator &stmt_allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this), K(lbt()));
  } else if (OB_UNLIKELY(prefix_cnt <= 0 || skip_range.is_whole_range())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(prefix_cnt), K(skip_range));
  } else if (OB_UNLIKELY(prefix_cnt_ != prefix_cnt ||
                         read_info_ != &read_info ||
                         stmt_alloc_ != &stmt_allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected argument in rescan", KR(ret), K_(prefix_cnt), K(prefix_cnt),
             KP_(read_info), KP(&read_info), KP_(stmt_alloc), KP(&stmt_allocator), K(lbt()));
  } else if (OB_FAIL(prepare_ranges(is_reverse_scan, prefix_cnt, scan_range, skip_range,
                                    read_info.get_schema_rowkey_count(), stmt_allocator))) {
    LOG_WARN("failed to init ranges", KR(ret));
  } else {
    is_reverse_scan_ = is_reverse_scan;
    scan_range_ = &scan_range;
    skip_range_ = &skip_range;
    LOG_TRACE("[INDEX SKIP SCAN] success to switch info", KR(ret), K(*this));
  }
  return ret;
}

// skip data by endkey
int ObIndexSkipScanner::skip(ObMicroIndexInfo &index_info, ObIndexSkipState &prev_state, ObIndexSkipState &state)
{
  int ret = OB_SUCCESS;
  bool state_determined = false;
  const ObCommonDatumRowkey &endkey = index_info.endkey_;
  LOG_DEBUG("[INDEX SKIP SCAN] try skip data by endkey", K(prev_state), K(state), K(endkey), K_(complete_range),
            KPC(this), K(lbt()));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this), K(lbt()));
  } else if (!is_prefix_filled_) {
    LOG_DEBUG("[INDEX SKIP SCAN] prefix not filled, can not skip", K(state_determined), K(prev_state), K(state),
              K(endkey), K_(complete_range), KPC(this));
  } else {
    int64_t left_ne_pos = -1;
    int left_cmp_ret = 0;
    bool less_than_left = false;
    bool need_check_end = false;
    bool left_prefix_not_change = false;
    const bool cmp_datum_cnt = false;
    const ObDatumRowkey &left_border = complete_range_.start_key_;
    const ObDatumRowkey &right_border = complete_range_.end_key_;
    const ObBorderFlag &border_flag = complete_range_.border_flag_;
    const bool prev_right_pendding = !is_reverse_scan_ &&
                                     (index_skip_state_.is_range_finished() || // this means current prefix range is already determined
                                     (prev_state.range_idx() == index_skip_state_.range_idx() &&
                                     (prev_state.is_pendding_right() || prev_state.is_skipped_right())));
    if (OB_UNLIKELY(!complete_range_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected complete range", KR(ret), K(prev_state), K(state), K_(complete_range), K(endkey), KPC(this));
    } else if (OB_FAIL(endkey.compare(left_border, datum_utils_, left_cmp_ret, cmp_datum_cnt, &left_ne_pos))) {
      LOG_WARN("failed to compare left border", KR(ret), K(prev_state), K(state), K(endkey), K_(complete_range), KPC(this));
    } else if (FALSE_IT(less_than_left = left_cmp_ret < 0 || (0 == left_cmp_ret && !border_flag.inclusive_start()))) {
    } else if (FALSE_IT(left_prefix_not_change = left_ne_pos > prefix_cnt_)) {
    } else if (is_reverse_scan_) {
      need_check_end = !less_than_left;
      if (less_than_left && left_prefix_not_change) {
        state_determined = true;
        // CASE:1.1 in reverse scan
        // the prefix of current node's endkey is not changed and less than the complete range's startkey
        // if the prefix of current node's startkey is also not changed, then the entire node can be skipped
        // but there is no startkey, we cat re-check this in the next call if possible
        // (the next node's endkey is the current node's startkey)
        state.set_state(index_skip_state_.range_idx(), ObIndexSkipNodeState::PREFIX_PENDDING_LEFT);
        LOG_TRACE("[INDEX SKIP SCAN] reverse scan, set pending left", K(prev_state), K(state), K(endkey),
                  K_(complete_range), KPC(this));
      }
    } else if (less_than_left) {
      state_determined = true;
      // CASE:2.1 in forward scan
      // the prefix of current node's endkey is less than the complete range's startkey
      // the entire node can be skipped
      state.set_state(index_skip_state_.range_idx(), ObIndexSkipNodeState::PREFIX_SKIPPED_LEFT);
      LOG_TRACE("[INDEX SKIP SCAN] can skip", K(prev_state), K(state), K(endkey), K_(complete_range), KPC(this));
    } else {
      need_check_end = left_prefix_not_change;
    }
    LOG_DEBUG("[INDEX SKIP SCAN] check left border", KR(ret), K(state_determined), K(left_cmp_ret), K(left_ne_pos), K(less_than_left),
              K(need_check_end), K(left_prefix_not_change), K(prev_state), K(state), K(endkey), K_(complete_range), KPC(this));
    if (OB_SUCC(ret) && need_check_end) {
      int64_t right_ne_pos = -1;
      int right_cmp_ret = 0;
      bool greater_than_right = false;
      if (OB_FAIL(endkey.compare(right_border, datum_utils_, right_cmp_ret, cmp_datum_cnt, &right_ne_pos))) {
        LOG_WARN("failed to compare right border", KR(ret), K(prev_state), K(state), K(endkey), K_(complete_range), KPC(this));
      } else if (!is_reverse_scan_ && right_ne_pos <= prefix_cnt_) {
      } else if (FALSE_IT(greater_than_right = right_cmp_ret > 0 || (0 == right_cmp_ret && !border_flag.inclusive_end()))) {
      } else if (!greater_than_right) {
      } else if (is_reverse_scan_) {
        state_determined = true;
        // CASE:1.2 in reverse scan
        // the prefix of current node's endkey is greater than the complete range's endkey
        // if the prefix of current node's startkey is also greater, then the entire node can be skipped
        // but there is no startkey, we can re-check this node in the next call if possible
        // (the next node's endkey is the current node's startkey)
        state.set_state(index_skip_state_.range_idx(), ObIndexSkipNodeState::PREFIX_PENDDING_RIGHT);
        LOG_TRACE("[INDEX SKIP SCAN] reverse scan, set pending right", K(prev_state), K(state), K(endkey),
                  K_(complete_range), KPC(this));
      } else if (prev_right_pendding) {
        state_determined = true;
        // CASE:2.2 in forward scan
        // the prefix of current node's endkey is not changed and greater than the complete range's endkey
        // and the previous node's endkey is also not changed and greater than the complete range's endkey
        // so we can determine that current node can be skipped
        state.set_state(index_skip_state_.range_idx(), ObIndexSkipNodeState::PREFIX_SKIPPED_RIGHT);
        LOG_TRACE("[INDEX SKIP SCAN] can skip", K(prev_state), K(state), K(endkey), K_(complete_range), KPC(this));
      } else {
        state_determined = true;
        // CASE:2.3 in forward scan
        // the prefix of current node's endkey is not changed and greater than the complete range's endkey
        // but the previous node's endkey is not (not changed and greater than the complete range's endkey)
        // we can re-check this node in the next call if possible
        state.set_state(index_skip_state_.range_idx(), ObIndexSkipNodeState::PREFIX_PENDDING_RIGHT);
        LOG_TRACE("[INDEX SKIP SCAN] forward scan, set pending right", K(prev_state), K(state), K(endkey),
                  K_(complete_range), KPC(this));
      }
      LOG_DEBUG("[INDEX SKIP SCAN] check right border", KR(ret), K(state_determined), K(right_cmp_ret), K(right_ne_pos),
                K(greater_than_right), K(prev_state), K(state), K(endkey), K_(complete_range), KPC(this));
    }
  }
  if (OB_SUCC(ret) && !state_determined) {
    state.set_state(index_skip_state_.range_idx(), ObIndexSkipNodeState::PREFIX_UNCERTAIN);
  }
  if (OB_SUCC(ret) && state.is_skipped()) {
    const bool is_border = index_info.is_left_border() || index_info.is_right_border();
    const int64_t row_count = index_info.get_row_count() >> is_border;
    index_skip_strategy_.add_skipped_row_count(row_count);
    index_skip_strategy_.add_total_row_count(row_count);
    LOG_TRACE("[INDEX SKIP SCAN] can skip", K(row_count), K(endkey), K_(complete_range), KPC(this));
  }
  prev_state = state;
  return ret;
}

// skip data by endkey in reverse scan
// should check the next(left) state to determine whether current endkey can be skipped
int ObIndexSkipScanner::skip(
    ObMicroIndexInfo &index_info,
    const ObIndexSkipState &next_state,
    ObIndexSkipState &state)
{
  int ret = OB_SUCCESS;
  const ObCommonDatumRowkey &endkey = index_info.endkey_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this), K(lbt()));
  } else if (OB_UNLIKELY(!is_reverse_scan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected state, only reverse scan can reach here", KR(ret), K(lbt()));
  } else {
    bool can_skip = state.range_idx() == next_state.range_idx() &&
                    state.range_idx() == index_skip_state_.range_idx();
    if (!can_skip) {
    } else if (next_state.is_pendding_left() || next_state.is_skipped_left()) {
      if (state.is_pendding_left()) {
        // CASE:1.3 in reverse scan
        // the next (left) node and current node are in CASE:1.1
        // we can determine that current node can be skipped
        state.set_state(index_skip_state_.range_idx(), ObIndexSkipNodeState::PREFIX_SKIPPED_LEFT);
        LOG_TRACE("[INDEX SKIP SCAN] can skip", K(next_state), K(state), K(endkey), K_(complete_range), KPC(this));
      }
    } else if (next_state.is_pendding_right() || next_state.is_skipped_right()) {
      if (state.is_pendding_right()) {
        // CASE:1.4 in reverse scan
        // the next (left) node and current node are in CASE:1.2
        // we can determine that current node can be skipped
        state.set_state(index_skip_state_.range_idx(), ObIndexSkipNodeState::PREFIX_SKIPPED_RIGHT);
        LOG_TRACE("[INDEX SKIP SCAN] can skip", K(next_state), K(state), K(endkey), K_(complete_range), KPC(this));
      }
    }
    if (OB_SUCC(ret) && state.is_skipped()) {
      const bool is_border = index_info.is_left_border() || index_info.is_right_border();
      const int64_t row_count = index_info.get_row_count() >> is_border;
      index_skip_strategy_.add_skipped_row_count(row_count);
      index_skip_strategy_.add_total_row_count(row_count);
      LOG_TRACE("[INDEX SKIP SCAN] can skip", K(row_count), K(endkey), K_(complete_range), KPC(this));
    }
  }
  LOG_DEBUG("[INDEX SKIP SCAN] reverse scan, check skip", K(next_state), K(state), K(endkey), KPC(this));
  return ret;
}

// skip data in data block
int ObIndexSkipScanner::skip(
    ObIMicroBlockRowScanner &micro_scanner,
    ObMicroIndexInfo &index_info,
    const bool first)
{
  int ret = OB_SUCCESS;
  ObIndexSkipState &state = index_info.skip_state_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this), K(lbt()));
  } else if (is_disabled() && !is_border_after_disabled_) {
    LOG_DEBUG("[INDEX SKIP SCAN] skip scanner is disabled and not border", K(first), K(state), K(micro_scanner), KPC(this));
  } else if (OB_FAIL(check_and_preprocess(first, micro_scanner, state))) {
    LOG_WARN("failed to check and preprocess", KR(ret));
  } else if (state.is_skipped()) {
  } else if (is_disabled() && first) {
    bool has_data = false;
    if (OB_FAIL(check_after_range_updated(is_reverse_scan_, complete_range_, micro_scanner, is_border_after_disabled_))) {
      LOG_WARN("failed to check after range updated", KR(ret));
    } else if (!is_border_after_disabled_) {
    } else if (OB_FAIL(skip_in_micro(micro_scanner, index_info, complete_range_, false, has_data))) {
      LOG_WARN("failed to skip in micro", KR(ret));
    }
  } else if (!is_disabled() && !state.is_skipped()) {
    LOG_DEBUG("[INDEX SKIP SCAN] try skip data in block", K(first), K(state), KPC(this));
    bool has_data = false;
    // is_range_finished() == true means current prefix range is already determined
    // should skip to next prefix
    if (first && !index_skip_state_.is_range_finished()) {
      bool prefix_changed = false;
      const ObDatumRow *store_row = nullptr;
      if (OB_FAIL(micro_scanner.get_next_skip_row(store_row))) {
        LOG_WARN("failed to get next row", KR(ret));
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[INDEX SKIP SCAN] should not reach here", KR(ret), K(micro_scanner));
        }
      } else if (OB_FAIL(check_prefix_changed(store_row, prefix_changed))) {
        LOG_WARN("failed to check prefix changed", KR(ret));
      } else if (prefix_changed && OB_FAIL(update_complete_range(store_row))) {
        LOG_WARN("failed to update complete range", KR(ret));
      } else if (FALSE_IT(is_prefix_filled_ = true)) {
      } else if (OB_FAIL(skip_in_micro(micro_scanner, index_info, complete_range_, false, has_data))) {
        LOG_WARN("failed to skip in micro", KR(ret));
      }
    } else {
      const ObDatumRow *store_row = nullptr;
      while (OB_SUCC(ret) && !state.is_skipped() && !has_data) {
        if (OB_FAIL(update_prefix_range())) {
          LOG_WARN("failed to update prefix range", KR(ret));
        } else if (OB_FAIL(skip_in_micro(micro_scanner, index_info, prefix_range_, true, has_data))) {
          LOG_WARN("failed to skip in micro", KR(ret));
        } else if (state.is_skipped()) {
        } else if (OB_FAIL(micro_scanner.get_next_skip_row(store_row))) {
          LOG_WARN("failed to get next row", KR(ret));
          if (OB_ITER_END == ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[INDEX SKIP SCAN] should not reach here", KR(ret), K(micro_scanner));
          }
        } else if (OB_FAIL(update_complete_range(store_row))) {
          LOG_WARN("failed to update complete range", KR(ret));
        } else if (OB_FAIL(skip_in_micro(micro_scanner, index_info, complete_range_, false, has_data))) {
          LOG_WARN("failed to skip in micro", KR(ret));
        }
      }
    }
  }
  return ret;
}

#define SET_ROWKEY_TO_SCAN_ROWKEY(POS1, POS2)                        \
  complete_range_.set_##POS1##_key(scan_range_->get_##POS1##_key()); \
  if (scan_range_->is_##POS2##_open()) {                             \
    complete_range_.set_##POS2##_open();                             \
  } else {                                                           \
    complete_range_.set_##POS2##_closed();                           \
  }

int ObIndexSkipScanner::skip(ObMemtableSkipScanIterator &mem_iter, bool &is_end, const bool first)
{
  int ret = OB_SUCCESS;
  is_end = false;
  bool is_start_equal = false;
  bool is_end_equal = false;
  const ObDatumRow *store_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this), K(lbt()));
  } else if (first) {
    if (OB_FAIL(mem_iter.get_next_skip_row(store_row))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        is_end = true;
        LOG_DEBUG("[INDEX SKIP SCAN] memtable reachs end", K(is_end), K(first));
      } else {
        LOG_WARN("failed to get next row", KR(ret));
      }
    } else if (OB_FAIL(check_scan_range(store_row, is_start_equal, is_end_equal))) {
      LOG_WARN("failed to check scan range", KR(ret));
    } else if (OB_FAIL(update_complete_range(store_row, true /* is_for_memtable */, is_start_equal, is_end_equal))) {
      LOG_WARN("failed to update complete range", KR(ret));
    } else if (FALSE_IT(is_prefix_filled_ = true)) {
    } else if (OB_FAIL(mem_iter.skip_to_range(complete_range_))) {
      LOG_WARN("failed to skip to range", KR(ret));
    }
  } else if (OB_FAIL(update_prefix_range(true /* is_for_memtable */))) {
    LOG_WARN("failed to update prefix range", KR(ret));
  } else if (OB_FAIL(mem_iter.skip_to_range(prefix_range_))) {
    LOG_WARN("failed to skip to next prefix range", KR(ret));
  } else if (OB_FAIL(mem_iter.get_next_skip_row(store_row))) {
    if (OB_LIKELY(OB_ITER_END == ret)) {
      is_end = true;
      LOG_DEBUG("[INDEX SKIP SCAN] memtable reachs end", K(is_end), K(first));
    } else {
      LOG_WARN("failed to get next row", KR(ret));
    }
  } else if (OB_FAIL(check_scan_range(store_row, is_start_equal, is_end_equal))) {
    LOG_WARN("failed to check scan range", KR(ret));
  } else if (OB_FAIL(update_complete_range(store_row, true /* is_for_memtable */, is_start_equal, is_end_equal))) {
    LOG_WARN("failed to update complete range", KR(ret));
  } else if (OB_FAIL(mem_iter.skip_to_range(complete_range_))) {
    LOG_WARN("failed to skip to range", KR(ret));
  }
  return ret;
}

int ObIndexSkipScanner::check_and_preprocess(
    const bool first,
    blocksstable::ObIMicroBlockRowScanner &micro_scanner,
    ObIndexSkipState &state)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(state.is_skipped() || state.is_invalid() || !micro_scanner.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scanner state", KR(ret), K(state), K(micro_scanner));
  } else if (first) {
    index_skip_strategy_.add_total_row_count(micro_scanner.get_access_cnt());
    if (OB_FAIL(micro_scanner.end_of_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to check end of block", KR(ret), K(micro_scanner));
      } else {
        ret = OB_SUCCESS;
        state.set_state(index_skip_state_.range_idx(), ObIndexSkipNodeState::PREFIX_SKIPPED_LEFT);
        LOG_DEBUG("[INDEX SKIP SCAN] micro scanner reachs end", K(state), K(first), K_(is_prefix_filled));
      }
    } else if (is_reverse_scan_) {
      micro_start_ = micro_scanner.get_last_pos();
      micro_last_ = micro_scanner.get_current_pos();
      micro_current_ = micro_start_;
    } else {
      micro_start_ = micro_scanner.get_current_pos();
      micro_last_ = micro_scanner.get_last_pos();
      micro_current_ = micro_start_;
    }
  }
  return ret;
}

int ObIndexSkipScanner::prepare_ranges(
    const bool is_reverse_scan,
    const int64_t prefix_cnt,
    const ObDatumRange &scan_range,
    const ObDatumRange &skip_range,
    const int64_t schema_rowkey_cnt,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(schema_rowkey_cnt < prefix_cnt || prefix_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema rowkey cnt", KR(ret), K(schema_rowkey_cnt), K(prefix_cnt));
  } else if (nullptr == range_datums_) {
    void *buf = nullptr;
    const int64_t total_datum_cnt = schema_rowkey_cnt * 4;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObStorageDatum) * total_datum_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", KR(ret));
    } else {
      range_datums_ = new (buf) ObStorageDatum[total_datum_cnt];
    }
  }
  if (OB_SUCC(ret)) {
    OZ(complete_range_.start_key_.assign(range_datums_, schema_rowkey_cnt));
    OZ(complete_range_.end_key_.assign(range_datums_ + schema_rowkey_cnt, schema_rowkey_cnt));
    OZ(prefix_range_.start_key_.assign(range_datums_ + 2 * schema_rowkey_cnt, schema_rowkey_cnt));
    OZ(prefix_range_.end_key_.assign(range_datums_ + 3 * schema_rowkey_cnt, schema_rowkey_cnt));
    if (OB_FAIL(ret)) {
      if (nullptr != range_datums_) {
        allocator.free(range_datums_);
        range_datums_ = nullptr;
      }
    } else if (OB_FAIL(prepare_range_datums(is_reverse_scan, prefix_cnt, scan_range, skip_range,
                                             schema_rowkey_cnt, allocator))) {
      LOG_WARN("failed to prepare range datums", KR(ret));
    } else if (!scan_range.is_whole_range()) {
      // if original scan range has valid value after prefix
      // should check whether exceed the original scan range in the first and last rowkey
      bool start_key_checked = false;
      bool end_key_checked = false;
      for (int64_t i = prefix_cnt; !is_scan_range_complete_ && i < schema_rowkey_cnt; ++i) {
        if (!start_key_checked && i < scan_range.start_key_.get_datum_cnt()) {
          start_key_checked = scan_range.start_key_.datums_[i].is_min();
          is_scan_range_complete_ = !start_key_checked;
        }
        if (is_scan_range_complete_) {
        } else if (!end_key_checked && i < scan_range.end_key_.get_datum_cnt()) {
          end_key_checked = scan_range.end_key_.datums_[i].is_max();
          is_scan_range_complete_ = !end_key_checked;
        }
      }
    }
  }
  return ret;
}

int ObIndexSkipScanner::prepare_range_datums(
    const bool is_reverse_scan,
    const int64_t prefix_cnt,
    const ObDatumRange &scan_range,
    const ObDatumRange &skip_range,
    const int64_t schema_rowkey_cnt,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  complete_range_.start_key_.reuse();
  complete_range_.end_key_.reuse();
  prefix_range_.start_key_.reuse();
  prefix_range_.end_key_.reuse();
  for (int64_t i = 0; i < schema_rowkey_cnt; ++i) {
    complete_range_.start_key_.datums_[i].set_min();
    complete_range_.end_key_.datums_[i].set_max();
  }
  if (is_reverse_scan) {
    prefix_range_.set_start_key(scan_range.start_key_);
  } else {
    prefix_range_.set_end_key(scan_range.end_key_);
  }
  for (int64_t i = prefix_cnt; i < schema_rowkey_cnt; ++i) {
    const int64_t skip_datum_idx = i - prefix_cnt;
    if (skip_datum_idx < skip_range.start_key_.datum_cnt_) {
      complete_range_.start_key_.datums_[i] = skip_range.start_key_.datums_[skip_datum_idx];
    }
    if (skip_datum_idx < skip_range.end_key_.datum_cnt_) {
      complete_range_.end_key_.datums_[i] = skip_range.end_key_.datums_[skip_datum_idx];
    }
    complete_range_.set_border_flag(skip_range.get_border_flag());
    complete_range_.set_group_idx(scan_range.get_group_idx());

    prefix_range_.set_border_flag(scan_range.get_border_flag());
    prefix_range_.set_group_idx(scan_range.get_group_idx());
    if (is_reverse_scan) {
      prefix_range_.end_key_.datums_[i].set_min();
      prefix_range_.set_right_open();
    } else {
      prefix_range_.start_key_.datums_[i].set_max();
      prefix_range_.set_left_open();
    }
  }
  LOG_DEBUG("[INDEX SKIP SCAN] prepare range datums", KR(ret), K_(complete_range), K_(prefix_range),
             K(scan_range), K(skip_range));
  return ret;
}

int ObIndexSkipScanner::check_prefix_changed(const blocksstable::ObDatumRow *row, bool &changed)
{
  int ret = OB_SUCCESS;
  changed = false;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalud null row", KR(ret));
  } else {
    const ObDatumRowkey &key = complete_range_.start_key_;
    const ObStoreCmpFuncs &cmp_funcs = datum_utils_.get_cmp_funcs();
    int cmp_ret = 0;
    for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp_ret && i < prefix_cnt_; ++i) {
      if (OB_FAIL(cmp_funcs.at(i).compare(key.datums_[i], row->storage_datums_[i], cmp_ret))) {
        LOG_WARN("failed to compare", KR(ret), K(i), K(key), KPC(row));
      }
    }
    changed = 0 != cmp_ret;
  }
  return ret;
}

// check if prefix is equal with start/end key of original scan range
int ObIndexSkipScanner::check_scan_range(const ObDatumRow *row, bool &is_start_equal, bool &is_end_equal)
{
  int ret = OB_SUCCESS;
  is_start_equal = is_end_equal = false;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalud null row", KR(ret));
  } else if (!is_scan_range_complete_) {
    LOG_DEBUG("[INDEX SKIP SCAN] no need check", K_(is_scan_range_complete), KPC(row));
  } else {
    const ObDatumRowkey &start_key = scan_range_->start_key_;
    const ObDatumRowkey &end_key = scan_range_->end_key_;
    const ObStoreCmpFuncs &cmp_funcs = datum_utils_.get_cmp_funcs();

    int cmp_ret = 0;
    is_start_equal = start_key.get_datum_cnt() >= prefix_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && is_start_equal && i < prefix_cnt_; ++i) {
      if (OB_FAIL(cmp_funcs.at(i).compare(start_key.datums_[i], row->storage_datums_[i], cmp_ret))) {
        LOG_WARN("failed to compare", KR(ret), K(i), K(start_key), KPC(row));
      } else if (0 != cmp_ret) {
        is_start_equal = false;
      }
    }
    is_end_equal = end_key.get_datum_cnt() >= prefix_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && is_end_equal && i < prefix_cnt_; ++i) {
      if (OB_FAIL(cmp_funcs.at(i).compare(end_key.datums_[i], row->storage_datums_[i], cmp_ret))) {
        LOG_WARN("failed to compare", KR(ret), K(i), K(start_key), KPC(row));
      } else if (0 != cmp_ret) {
        is_end_equal = false;
      }
    }
  }
  return ret;
}

int ObIndexSkipScanner::check_after_range_updated(
    const bool is_cmp_end,
    const ObDatumRange &range,
    const blocksstable::ObIMicroBlockRowScanner &micro_scanner,
    bool &skipped)
{
  int ret = OB_SUCCESS;
  skipped = false;
  if (is_reverse_scan_) {
    int right_cmp_ret = 0;
    const ObDatumRowkey &right_border = range.end_key_;
    const ObBorderFlag &border_flag = range.border_flag_;
    if (OB_FAIL(micro_scanner.compare_rowkey(right_border, is_cmp_end, right_cmp_ret))) {
      LOG_WARN("failed to compare right border", KR(ret), K(range), KPC(this));
    } else {
      // CASE:1.5 in reverse scan
      // same with CASE:1.4, current micro scanner is at the right of right border
      skipped = right_cmp_ret > 0 || (0 == right_cmp_ret && !border_flag.inclusive_end());
    }
  } else {
    int left_cmp_ret = 0;
    const ObDatumRowkey &left_border = range.start_key_;
    const ObBorderFlag &border_flag = range.border_flag_;
    if (OB_FAIL(micro_scanner.compare_rowkey(left_border, is_cmp_end, left_cmp_ret))) {
      LOG_WARN("failed to compare left border", KR(ret), K(range), KPC(this));
    } else {
      // CASE:2.4 in forward scan
      // same with CASE:2.1, current micro scanner is at the left of left border
      skipped = left_cmp_ret < 0 || (0 == left_cmp_ret && !border_flag.inclusive_start());
    }
  }
  return ret;
}

int ObIndexSkipScanner::update_complete_range(
    const ObDatumRow *row,
    const bool is_for_memtable,
    const bool is_start_equal,
    const bool is_end_equal)
{
  int ret = OB_SUCCESS;
  range_alloc_.reuse();
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalud null row", KR(ret));
  } else if (FALSE_IT(index_skip_state_.inc_range_idx())) {
  } else if (FALSE_IT(index_skip_state_.set_range_finished(false))) {
  } else if (0 == (index_skip_state_.range_idx() % CHECK_INTERRUPT_RANGE_CNT)) {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      STORAGE_LOG(WARN, "query interrupt", KR(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < prefix_cnt_; ++i) {
    if (OB_FAIL(complete_range_.start_key_.datums_[i].deep_copy(row->storage_datums_[i], range_alloc_))) {
      LOG_WARN("failed to deep copy complete range", KR(ret));
    } else {
      complete_range_.end_key_.datums_[i] = complete_range_.start_key_.datums_[i];
    }
  }
  for (int64_t i = 0; i < skip_range_->start_key_.datum_cnt_; ++i) {
    complete_range_.start_key_.datums_[i + prefix_cnt_] = skip_range_->start_key_.datums_[i];
  }
  for (int64_t i = 0; i < skip_range_->end_key_.datum_cnt_; ++i) {
    complete_range_.end_key_.datums_[i + prefix_cnt_] = skip_range_->end_key_.datums_[i];
  }
  complete_range_.set_border_flag(skip_range_->get_border_flag());
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(is_start_equal || is_end_equal)) {
    int cmp_ret = 0;
    if (is_start_equal) {
      const ObDatumRowkey &complete_start_key = complete_range_.start_key_;
      const ObDatumRowkey &scan_start_key = scan_range_->start_key_;
      if (OB_FAIL(complete_start_key.compare(scan_start_key, datum_utils_, cmp_ret))) {
        STORAGE_LOG(WARN, "failed to compare", KR(ret));
      } else if (cmp_ret < 0) {
        for (int64_t i = prefix_cnt_; i < scan_start_key.get_datum_cnt(); ++i) {
          complete_start_key.datums_[i] = scan_start_key.datums_[i];
          if (scan_range_->is_left_open()) {
            complete_range_.set_left_open();
          } else {
            complete_range_.set_left_closed();
          }
        }
      }
    }
    if (OB_SUCC(ret) && is_end_equal) {
      const ObDatumRowkey &complete_end_key = complete_range_.end_key_;
      const ObDatumRowkey &scan_end_key = scan_range_->end_key_;
      if (OB_FAIL(complete_end_key.compare(scan_end_key, datum_utils_, cmp_ret))) {
        STORAGE_LOG(WARN, "failed to compare", KR(ret));
      } else if (cmp_ret > 0) {
        for (int64_t i = prefix_cnt_; i < scan_end_key.get_datum_cnt(); ++i) {
          complete_end_key.datums_[i] = scan_end_key.datums_[i];
          if (scan_range_->is_right_open()) {
            complete_range_.set_right_open();
          } else {
            complete_range_.set_right_closed();
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ObIArray<ObColDesc> &col_descs = read_info_->get_columns_desc();
    if (is_for_memtable && OB_FAIL(complete_range_.prepare_memtable_readable(col_descs, range_alloc_))) {
      LOG_WARN("failed to prepare memtable range", KR(ret));
    } else {
      LOG_DEBUG("[INDEX SKIP SCAN] update complete range", KR(ret), K_(complete_range),
          K(is_start_equal), K(is_end_equal), KPC(this));
    }
  }
  if (FAILEDx(check_disabled())) {
    LOG_WARN("failed to check disabled", KR(ret));
  }
  return ret;
}

int ObIndexSkipScanner::update_prefix_range(const bool is_for_memtable)
{
  int ret = OB_SUCCESS;
  if (prefix_range_idx_ == index_skip_state_.range_idx())  {
    LOG_DEBUG("[INDEX SKIP SCAN] prefix range is not changed", K_(prefix_range), KPC(this));
  } else {
    prefix_alloc_.reuse();
    prefix_range_idx_ = index_skip_state_.range_idx();
    ObDatumRowkey &rowkey = is_reverse_scan_ ? prefix_range_.end_key_ : prefix_range_.start_key_;
    for (int64_t i = 0; OB_SUCC(ret) && i < prefix_cnt_; ++i) {
      if (OB_FAIL(rowkey.datums_[i].deep_copy(complete_range_.start_key_.datums_[i], prefix_alloc_))) {
        LOG_WARN("failed to deep copy prefix range", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const ObIArray<ObColDesc> &col_descs = read_info_->get_columns_desc();
      if (is_for_memtable && OB_FAIL(prefix_range_.prepare_memtable_readable(col_descs, prefix_alloc_))) {
        LOG_WARN("failed to prepare memtable range", KR(ret));
      } else {
        LOG_DEBUG("[INDEX SKIP SCAN] update prefix range", KR(ret), K_(prefix_range), KPC(this));
      }
    }
  }
  return ret;
}

int ObIndexSkipScanner::skip_in_micro(
    ObIMicroBlockRowScanner &micro_scanner,
    ObMicroIndexInfo &index_info,
    const ObDatumRange &range,
    const bool is_prefix_range,
    bool &has_data)
{
  int ret = OB_SUCCESS;
  bool skipped_by_new_range = false;
  bool range_covered = false;
  const bool is_left_border = index_info.is_left_border();
  const bool is_right_border = index_info.is_right_border();
  ObIndexSkipState &state = index_info.skip_state_;
  if (OB_FAIL(check_after_range_updated(!is_reverse_scan_, range, micro_scanner, skipped_by_new_range))) {
    LOG_WARN("failed to check after range updated", KR(ret));
  } else if (skipped_by_new_range) {
    micro_scanner.skip_to_end();
    state.set_state(index_skip_state_.range_idx(), get_default_skip_state());
    index_skip_strategy_.add_skipped_row_count(micro_last_ - micro_start_ + 1);
    LOG_DEBUG("[INDEX SKIP SCAN] can skip in block", K(range), KPC(this));
  } else if (OB_FAIL(micro_scanner.skip_to_range(micro_start_, micro_last_, range, is_left_border, is_right_border,
                                                 micro_current_, has_data, range_covered))) {
    LOG_WARN("failed to skip to next prefix range", KR(ret), K(micro_scanner), KPC(this));
  } else if (OB_FAIL(update_state_and_strategy(micro_scanner, is_prefix_range, has_data, range_covered, state))) {
    LOG_WARN("failed to update state and strategy", KR(ret));
  } else if (!has_data) {
    micro_scanner.skip_to_end();
  }
  return ret;
}

int ObIndexSkipScanner::update_state_and_strategy(
    ObIMicroBlockRowScanner &micro_scanner,
    const bool is_prefix_range,
    const bool has_data,
    const bool range_covered,
    ObIndexSkipState &state)
{
  int ret = OB_SUCCESS;
  index_skip_state_.set_range_finished(!is_prefix_range && range_covered);
  if (has_data) {
    if (OB_UNLIKELY(micro_current_ < micro_start_ || micro_current_ > micro_last_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro current is invalid", KR(ret), KPC(this));
    } else if (is_reverse_scan_) {
      index_skip_strategy_.add_skipped_row_count(MAX(0, micro_last_ - micro_current_ - 1));
      micro_last_ = is_prefix_range ? micro_current_ : micro_scanner.get_last_pos();
    } else {
      index_skip_strategy_.add_skipped_row_count(MAX(0, micro_current_ - micro_start_ - 1));
      micro_start_ = is_prefix_range ? micro_current_ : micro_scanner.get_last_pos();
    }
    if (OB_SUCC(ret) && micro_start_ > micro_last_) {
      state.set_state(index_skip_state_.range_idx(), get_default_skip_state());
    }
  } else if (is_reverse_scan_) {
    if (ObIMicroBlockReaderInfo::INVALID_ROW_INDEX != micro_current_) {
      if (micro_current_ <= micro_start_) {
        state.set_state(index_skip_state_.range_idx(), get_default_skip_state());
        index_skip_strategy_.add_skipped_row_count(micro_last_ - micro_start_);
      } else {
        const int64_t prev = micro_last_;
        micro_last_ = MIN(micro_last_, micro_current_);
        index_skip_strategy_.add_skipped_row_count(MAX(0, prev - micro_last_ - 1));
      }
    }
  } else if (OB_UNLIKELY(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX == micro_current_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro current is invalid", KR(ret), KPC(this));
  } else if (micro_current_ > micro_last_) {
    state.set_state(index_skip_state_.range_idx(), get_default_skip_state());
    index_skip_strategy_.add_skipped_row_count(micro_last_ - micro_start_);
  } else {
    const int64_t prev = micro_start_;
    micro_start_ = MAX(micro_start_, micro_current_);
    index_skip_strategy_.add_skipped_row_count(MAX(0, micro_start_ - prev - 1));
  }
  LOG_DEBUG("[INDEX SKIP SCAN] update state and strategy", K(is_prefix_range), K(has_data),
            K(range_covered), K(state), KPC(this));
  return ret;
}

int ObIndexSkipScanner::check_disabled()
{
  int ret = OB_SUCCESS;
  bool should_disabled = false;
  if (OB_UNLIKELY(is_disabled_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("skip scanner is already disabled, should not happen", KR(ret), KPC(this), K(lbt()));
  } else if (OB_ISNULL(skip_scan_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("skip scanner mgr is null", KR(ret), KPC(this), K(lbt()));
  } else if (skip_scan_factory_->is_pending_disabled()) {
    const ObDatumRowkey &cur_prefix_key = is_reverse_scan_ ? complete_range_.start_key_ : complete_range_.end_key_;
    const ObDatumRowkey *newest_prefix_key = nullptr;
    int cmp_ret = 0;
    int64_t ne_pos = -1;
    if (OB_FAIL(skip_scan_factory_->get_newest_prefix_key(newest_prefix_key))) {
      LOG_WARN("failed to get max prefix key", KR(ret));
    } else if (OB_ISNULL(newest_prefix_key)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("max prefix key is null", KR(ret));
    } else if (OB_FAIL(cur_prefix_key.compare(*newest_prefix_key, datum_utils_, cmp_ret, false/*cmp_datum_cnt*/, &ne_pos))) {
      LOG_WARN("failed to compare", KR(ret), K(cur_prefix_key), K(newest_prefix_key), K_(complete_range), KPC(this));
    } else if (((!is_reverse_scan_ && cmp_ret > 0) || (is_reverse_scan_ && cmp_ret < 0)) && ne_pos <= prefix_cnt_) {
      // current prefix is behind the max prefix, so we can disable this index skip scanner now
      is_disabled_ = true;
      is_border_after_disabled_ = true;
      if (is_reverse_scan_) {
        SET_ROWKEY_TO_SCAN_ROWKEY(start, left);
        complete_range_.set_end_key(*newest_prefix_key);
        complete_range_.set_right_open();
      } else {
        SET_ROWKEY_TO_SCAN_ROWKEY(end, right);
        // set start key to the right border of the disabled prefix
        // only this can ensure all iters start scanning from the same point
        complete_range_.set_start_key(*newest_prefix_key);
        complete_range_.set_left_open();
      }
      LOG_INFO("[INDEX SKIP SCAN] disabled", K_(is_disabled), K_(complete_range), KPC(this), KPC_(skip_scan_factory), K(lbt()));
    }
  } else if (OB_FAIL(index_skip_strategy_.check_disabled(index_skip_state_, should_disabled))) {
    LOG_WARN("failed to check disabled", KR(ret));
  } else if (should_disabled) {
    const ObIArray<ObColDesc> &col_descs = read_info_->get_columns_desc();
    if (OB_FAIL(skip_scan_factory_->set_pending_disabled(is_reverse_scan_,prefix_cnt_, datum_utils_, col_descs))) {
      LOG_WARN("failed to set pending disabled", KR(ret), KPC(this), K_(complete_range), K(lbt()));
    } else {
      LOG_INFO("[INDEX SKIP SCAN] pending disabled", K(should_disabled), K_(complete_range), KPC_(skip_scan_factory), KPC(this), K(lbt()));
    }
  }
  return ret;
}

#undef SET_ROWKEY_TO_SCAN_ROWKEY

int ObIndexSkipScanFactory::build_index_skip_scanner(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    const ObDatumRange *range,
    ObIndexSkipScanner *&skip_scanner,
    const bool is_for_memtable)
{
  int ret = OB_SUCCESS;
  const bool is_reverse_scan = access_ctx.query_flag_.is_reverse_scan();
  const int64_t prefix_cnt = iter_param.get_ss_rowkey_prefix_cnt();
  const ObDatumRange *skip_range = iter_param.get_ss_datum_range();
  const ObITableReadInfo *read_info = iter_param.get_read_info();
  ObIAllocator &stmt_allocator = *access_ctx.allocator_;

  ObIndexSkipScanFactory *skip_scan_factory = access_ctx.get_skip_scan_factory();
  if (OB_ISNULL(skip_scan_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("skip scanner mgr is null", KR(ret));
  } else if (OB_UNLIKELY(prefix_cnt <= 0 || nullptr == range || !range->is_valid() ||
                         nullptr == skip_range || !skip_range->is_valid() || nullptr == read_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to build index skip scanner", K(prefix_cnt), KP(range),
             KP(skip_range), KP(skip_scanner));
  } else if (nullptr != skip_scanner) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("skip scanner is not null", KR(ret), K(lbt()));
  } else if (OB_ISNULL(skip_scanner = OB_NEWx(ObIndexSkipScanner, &stmt_allocator, is_for_memtable, read_info->get_datum_utils()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc index skip scanner", KR(ret));
  } else if (OB_FAIL(skip_scanner->init(is_reverse_scan, prefix_cnt, *range,  *skip_range,
                                        *read_info, stmt_allocator))) {
    LOG_WARN("failed to init index skip scanner", KR(ret));
  }
  if (FAILEDx(skip_scan_factory->add_skip_scanner(skip_scanner))) {
    LOG_WARN("failed to add skip scanner", KR(ret));
  }
  if (OB_FAIL(ret) && nullptr != skip_scanner) {
    skip_scanner->~ObIndexSkipScanner();
    stmt_allocator.free(skip_scanner);
    skip_scanner = nullptr;
  }
  return ret;
}

void ObIndexSkipScanFactory::destroy_index_skip_scanner(ObIndexSkipScanner *&skip_scanner)
{
  if (nullptr != skip_scanner) {
    ObIAllocator *stmt_allocator = skip_scanner->get_stmt_alloc();
    skip_scanner->~ObIndexSkipScanner();
    if (nullptr != stmt_allocator) {
      stmt_allocator->free(skip_scanner);
    }
    skip_scanner = nullptr;
  }
}

ObIndexSkipScanFactory::ObIndexSkipScanFactory()
  : alloc_("SS_FACTORY", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    is_pending_disabled_(false),
    newest_prefix_key_()
{
  skip_scanners_.set_attr(ObMemAttr(MTL_ID(), "SS_FACTORY"));
}

ObIndexSkipScanFactory::~ObIndexSkipScanFactory()
{
  reset();
}

void ObIndexSkipScanFactory::reuse()
{
  is_pending_disabled_ = false;
  newest_prefix_key_.reset();
  skip_scanners_.reuse();
  alloc_.reuse();
}

void ObIndexSkipScanFactory::reset()
{
  is_pending_disabled_ = false;
  newest_prefix_key_.reset();
  skip_scanners_.reset();
  alloc_.reset();
}

int ObIndexSkipScanFactory::add_skip_scanner(ObIndexSkipScanner *skip_scanner)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(skip_scanner)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null skip scanner", KR(ret));
  } else if (OB_FAIL(skip_scanners_.push_back(skip_scanner))) {
    LOG_WARN("failed to push back skip scanner", KR(ret));
  } else {
    skip_scanner->set_skip_scan_factory(this);
  }
  return ret;
}

int ObIndexSkipScanFactory::set_pending_disabled(
    const bool is_reverse_scan,
    const int64_t prefix_cnt,
    const ObStorageDatumUtils &datum_utils,
    const ObIArray<ObColDesc> &col_descs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_pending_disabled_ || newest_prefix_key_.is_valid() || skip_scanners_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid state to set pending disabled", KR(ret), KPC(this), K(lbt()));
  } else {
    const ObDatumRowkey *newest_key = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < skip_scanners_.count(); ++i) {
      ObIndexSkipScanner *skip_scanner = skip_scanners_.at(i);
      if (OB_ISNULL(skip_scanner)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid null skip scanner", KR(ret), K(i), KPC(this));
      } else if (!is_pending_disabled_) {
        newest_key = is_reverse_scan ? &skip_scanner->get_complete_range().start_key_ :
                                       &skip_scanner->get_complete_range().end_key_;
        is_pending_disabled_ = true;
      } else if (!skip_scanner->is_prefix_filled()) {
      } else {
        int cmp_ret = 0;
        int64_t ne_pos = -1;
        const ObDatumRowkey &cur_prefix_key = is_reverse_scan ? skip_scanner->get_complete_range().start_key_ :
                                                                skip_scanner->get_complete_range().end_key_;
        LOG_INFO("[INDEX SKIP SCAN] compare prefix", K(i), K(skip_scanners_.count()), K(cur_prefix_key), KPC(newest_key));
        if (OB_FAIL(cur_prefix_key.compare(*newest_key, datum_utils, cmp_ret, false/*cmp_datum_cnt*/, &ne_pos))) {
          LOG_WARN("failed to compare", KR(ret), K(cur_prefix_key), K(newest_key));
        } else if (((!is_reverse_scan && cmp_ret > 0) || (is_reverse_scan && cmp_ret < 0)) && ne_pos <= prefix_cnt) {
          newest_key = &cur_prefix_key;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!is_pending_disabled_ || nullptr == newest_key || !newest_key->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid state to set pending disabled", KR(ret), KPC(newest_key), KPC(this), K(lbt()));
    } else if (OB_FAIL(newest_key->deep_copy(newest_prefix_key_, alloc_))) {
      LOG_WARN("failed to deep copy newest prefix key", KR(ret), KPC(newest_key));
    } else {
      for (int64_t i = prefix_cnt; i < newest_prefix_key_.get_datum_cnt(); ++i) {
        is_reverse_scan ? newest_prefix_key_.datums_[i].set_min() : newest_prefix_key_.datums_[i].set_max();
      }
    }
    if (FAILEDx(newest_prefix_key_.prepare_memtable_readable(col_descs, alloc_))) {
      LOG_WARN("Failed to prepare start key", K(ret), K(newest_prefix_key_), K(col_descs));
    }
  }
  return ret;
}

int ObIndexSkipScanFactory::get_newest_prefix_key(const ObDatumRowkey *&newest_prefix_key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!newest_prefix_key_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("newest prefix key is invalid", KR(ret), K_(newest_prefix_key));
  } else {
    newest_prefix_key = &newest_prefix_key_;
  }
  return ret;
}

}
}
