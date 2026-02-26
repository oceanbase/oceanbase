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

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "storage/compaction/ob_partition_rows_merger.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/compaction/ob_mview_compaction_util.h"
#include "storage/compaction/ob_compaction_util.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace storage;
namespace compaction
{
/**
 * ---------------------------------------------------------ObPartitionMergeLoserTreeCmp--------------------------------------------------------------
 */

//if need to open range, open the range with larger endkey first.
int ObPartitionMergeLoserTreeCmp::compare_range(const ObDatumRange &left_range,
                                                const ObDatumRange &right_range,
                                                int64_t &cmp_result) const
{
  int ret = OB_SUCCESS;
  int temp_cmp_ret = 0;
  const ObDatumRowkey &left_end_key = left_range.get_end_key();
  const ObDatumRowkey &left_start_key = left_range.get_start_key();
  const ObDatumRowkey &right_end_key = right_range.get_end_key();
  const ObDatumRowkey &right_start_key = right_range.get_start_key();

  if (OB_UNLIKELY(!left_range.is_valid() || !left_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(left_end_key.compare(right_end_key, datum_utils_, temp_cmp_ret))) {
    STORAGE_LOG(WARN, "Failed to compare rowkey", K(ret), K(datum_utils_), K(left_end_key), K(right_end_key));
  } else if (0 == temp_cmp_ret) {
    cmp_result = ALL_RANGE_NEED_OPEN;
  } else if (temp_cmp_ret > 0) {
    /*
     * left_end_key > right_end_key, compare right_end_key and left_start_key
     * if right_end_key < left_start_key, right can be reused,
     * else left_start_key < right_end_key < left_end_key,
     * because interal right is closed, left range more likely to be opened
     * and right may be reused. so open the left range first.
     */
    if (OB_FAIL(right_end_key.compare(left_start_key , datum_utils_, temp_cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare rowkey", K(ret), K(datum_utils_), K(left_start_key), K(right_end_key));
    } else {
      cmp_result = temp_cmp_ret >= 0 ? LEFT_RANGE_NEED_OPEN : 1;
    }
  } else if (temp_cmp_ret < 0) {
    if (OB_FAIL(left_end_key.compare(right_start_key , datum_utils_, temp_cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare rowkey", K(ret), K(datum_utils_), K(left_end_key), K(right_start_key));
    } else {
      cmp_result = temp_cmp_ret >= 0 ? RIGHT_RANGE_NEED_OPEN : -1;
    }
  }

  return ret;
}

int ObPartitionMergeLoserTreeCmp::compare_hybrid(const ObDatumRow &row,
                                                 const ObDatumRange &range,
                                                 int64_t &cmp_ret) const
{
  int ret = OB_SUCCESS;
  ObDatumRowkey rowkey;
  int temp_cmp_ret = 0;

  if (OB_FAIL(rowkey.assign(row.storage_datums_, rowkey_size_))) {
    STORAGE_LOG(WARN, "Failed to assign store rowkey", K(ret), K(row), K(rowkey_size_));
  } else if (OB_FAIL(rowkey.compare(range.get_start_key(), datum_utils_, temp_cmp_ret))) {
    STORAGE_LOG(WARN, "Failed to compare start key", K(ret), K(rowkey), K(range));
  } else if (temp_cmp_ret < 0) {
    cmp_ret = -1;
  } else if (temp_cmp_ret == 0) {
    cmp_ret = RIGHT_RANGE_NEED_OPEN;
    STORAGE_LOG(INFO, "rowkey equal last end_key, maybe aborted trans", K(rowkey), K(range));
  } else if (OB_FAIL(rowkey.compare(range.get_end_key(), datum_utils_, temp_cmp_ret))) {
    STORAGE_LOG(WARN, "Failed to compare end key", K(ret), K(rowkey), K(range));
  } else if (temp_cmp_ret == 0) {
    if (!range.get_border_flag().inclusive_end()) {
      cmp_ret = 1;
    } else {
      cmp_ret = RIGHT_RANGE_NEED_OPEN;
    }
  } else if (temp_cmp_ret > 0) {
    cmp_ret = 1;
  } else {
    cmp_ret = RIGHT_RANGE_NEED_OPEN;
  }

  return ret;
}

int ObPartitionMergeLoserTreeCmp::compare_rowkey(const ObDatumRow &l_row,
                                                 const ObDatumRow &r_row,
                                                 int64_t &cmp_result) const
{
  int ret = OB_SUCCESS;
  int temp_cmp_ret = 0;
  if (OB_UNLIKELY(!l_row.is_valid() || !r_row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(l_row), K(r_row), K(datum_utils_));
  } else if (OB_UNLIKELY(l_row.get_column_count() < rowkey_size_ ||
                         r_row.get_column_count() < rowkey_size_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected row column cnt", K(ret), K(l_row), K(r_row), K_(rowkey_size));
  } else {
    ObDatumRowkey l_key;
    ObDatumRowkey r_key;
    if (OB_FAIL(l_key.assign(l_row.storage_datums_, rowkey_size_))) {
      STORAGE_LOG(WARN, "Failed to assign store rowkey", K(ret), K(l_row), K_(rowkey_size));
    } else if (OB_FAIL(r_key.assign(r_row.storage_datums_, rowkey_size_))) {
      STORAGE_LOG(WARN, "Failed to assign store rowkey", K(ret), K(r_row), K_(rowkey_size));
    } else if (OB_FAIL(l_key.compare(r_key, datum_utils_, temp_cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare rowkey", K(ret), K(l_key), K(r_key), K(datum_utils_));
    } else if (temp_cmp_ret == 0) {
      cmp_result = 0;
    } else {
      cmp_result = temp_cmp_ret > 0 ? 1 : -1;
    }
  }
  return ret;
}

bool ObPartitionMergeLoserTreeCmp::check_cmp_finish(const int64_t cmp_ret) const
{
  return !need_open_left_range(cmp_ret) && !need_open_right_range(cmp_ret);
}

int ObPartitionMergeLoserTreeCmp::open_iter_range(
    const int64_t cmp_ret,
    const ObPartitionMergeLoserTreeItem &left,
    const ObPartitionMergeLoserTreeItem &right)
{
  int ret = OB_SUCCESS;
  if (need_open_left_range(cmp_ret) &&
      OB_FAIL(left.iter_->open_curr_range(false, true /* for compare */))) {
    if (ret != OB_BLOCK_SWITCHED) {
      STORAGE_LOG(WARN, "open curr range failed", K(ret), K(*left.iter_));
    }
  } else if (need_open_right_range(cmp_ret) &&
             OB_FAIL(right.iter_->open_curr_range(false, true /* for compare */))) {
    if (ret != OB_BLOCK_SWITCHED) {
      STORAGE_LOG(WARN, "open curr range failed", K(ret), K(*right.iter_));
    }
  }

  return ret;
}

int ObPartitionMergeLoserTreeCmp::cmp(const ObPartitionMergeLoserTreeItem &l,
                                      const ObPartitionMergeLoserTreeItem &r,
                                      int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  bool finish = false;

  while (OB_SUCC(ret) && !finish) {
    if (OB_FAIL(compare(l, r, cmp_ret))) {
      STORAGE_LOG(WARN, "Fail to compare item", K(ret), K(l), K(r));
    } else if ((finish = check_cmp_finish(cmp_ret))) {
    } else if (OB_FAIL(open_iter_range(cmp_ret, l, r))) {
      if (ret != OB_BLOCK_SWITCHED) {
        STORAGE_LOG(WARN, "fail open iter", K(ret), K(cmp_ret), K(l), K(r));
      }
    }
  }
  return ret;
}

int ObPartitionMergeLoserTreeCmp::compare(
    const ObPartitionMergeLoserTreeItem &l,
    const ObPartitionMergeLoserTreeItem &r,
    int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *left_row = l.iter_->get_curr_row();
  const ObDatumRow *right_row = r.iter_->get_curr_row();

  if (OB_NOT_NULL(left_row) && OB_NOT_NULL(right_row)) {
    if (OB_FAIL(compare_rowkey(*left_row, *right_row, cmp_ret))) {
      STORAGE_LOG(WARN, "compare_rowkey failed", K(ret));
    }
  } else if (OB_ISNULL(left_row) && OB_ISNULL(right_row)) {
    ObDatumRange left_range;
    ObDatumRange right_range;
    if (OB_FAIL(l.get_curr_range(left_range))) {
      STORAGE_LOG(WARN, "get_curr_range failed", K(ret));
    } else if (OB_FAIL(r.get_curr_range(right_range))) {
      STORAGE_LOG(WARN, "get_curr_range failed", K(ret));
    } else if (OB_FAIL(compare_range(left_range, right_range, cmp_ret))) {
      STORAGE_LOG(WARN, "compare_range failed", K(ret));
    }
  } else {
    ObDatumRange range;
    const bool is_reverse = (left_row == nullptr);
    const ObPartitionMergeLoserTreeItem &item = is_reverse ? l : r;
    const ObDatumRow *row = is_reverse ? right_row : left_row;
    if (OB_FAIL(item.get_curr_range(range))) {
      STORAGE_LOG(WARN, "get_curr_range failed", K(ret));
    } else if (OB_FAIL(compare_hybrid(*row, range, cmp_ret))) {
      STORAGE_LOG(WARN, "compare_hybrid failed", K(ret));
    } else if (is_reverse) {
      cmp_ret = -cmp_ret;
    }
  }

  return ret;
}


/**
 * ---------------------------------------------------------ObPartitionMajorRowsMerger--------------------------------------------------------------
 */
int ObPartitionMajorRowsMerger::init(const int64_t max_player_cnt,
                                     const int64_t total_player_cnt,
                                     common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(merger_state_ != NOT_INIT)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (total_player_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "total_player_cnt invailid", K(ret), K(total_player_cnt));
  } else {
    allocator_ = &allocator;
    // at least one player (major), to ensure inc_merger_ valid
    if (OB_FAIL(init_rows_merger(max_player_cnt, total_player_cnt, inc_merger_))) {
      STORAGE_LOG(WARN, "init inc rows merger failed", K(ret), K(total_player_cnt), K(major_cnt_));
    } else if (OB_FAIL(init_rows_merger(max_player_cnt, major_cnt_, majors_merger_))) {
      STORAGE_LOG(WARN, "init major rows merger failed", K(ret), K(major_cnt_));
    } else {
      merger_state_ = LOSER_TREE_WIN;
    }
  }
  return ret;
}

void ObPartitionMajorRowsMerger::reset()
{
  if (inc_merger_ != nullptr) {
    inc_merger_->~ObRowsMerger();
    if (allocator_ != nullptr) {
      allocator_->free(inc_merger_);
    }
  }
  if (majors_merger_ != nullptr) {
    majors_merger_->~ObRowsMerger();
    if (allocator_ != nullptr) {
      allocator_->free(majors_merger_);
    }
  }
  inc_merger_ = nullptr;
  majors_merger_ = nullptr;
  allocator_ = nullptr;
  merger_state_ = NOT_INIT;
  major_row_equal_to_inc_ = false;
}

void ObPartitionMajorRowsMerger::reuse()
{
  if (inc_merger_ != nullptr) {
    inc_merger_->reuse();
  }
  if (majors_merger_ != nullptr) {
    majors_merger_->reuse();
  }
  major_row_equal_to_inc_ = false;
  merger_state_ = merger_state_ == NOT_INIT ? NOT_INIT : LOSER_TREE_WIN;
}

int ObPartitionMajorRowsMerger::top(const ObPartitionMergeLoserTreeItem *&row)
{
  int ret = OB_SUCCESS;

  if (merger_state_ == NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (merger_state_ == NEED_REBUILD || merger_state_ == NEED_SKIP_REBUILD) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new players has been push, please rebuild", K(ret), K(merger_state_));
  } else if (merger_state_ == BASE_ITER_WIN && !major_row_equal_to_inc_) {
    // major win alone
    if (OB_FAIL(majors_merger_->top(row))) {
      STORAGE_LOG(WARN, "majors merge top failed", K(ret), KPC(majors_merger_));
    }
  } else if (OB_FAIL(inc_merger_->top(row))) {
    STORAGE_LOG(WARN, "rows_merger_ top failed", K(ret), KPC(inc_merger_));
  }
  return ret;
}

int ObPartitionMajorRowsMerger::pop()
{
  int ret = OB_SUCCESS;
  bool inc_merger_is_unique_champion = false;

  if (merger_state_ == NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (merger_state_ == NEED_REBUILD || merger_state_ == NEED_SKIP_REBUILD) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new players has been push, please rebuild", K(ret), K(merger_state_));
  } else if (!majors_merger_->is_unique_champion()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("majors merger is not unique champion", K(ret), KPC(majors_merger_));
  } else if (FALSE_IT(inc_merger_is_unique_champion = inc_merger_->is_unique_champion())) {
  } else if (merger_state_ == BASE_ITER_WIN && !major_row_equal_to_inc_) {
    // major win alone
    if (OB_FAIL(majors_merger_->pop())) {
      STORAGE_LOG(WARN, "majors merger failed", K(ret), KPC(majors_merger_));
    } else if (majors_merger_->empty()) {
      merger_state_ = LOSER_TREE_WIN;
    } else if (OB_FAIL(compare_base_iter())) {
      STORAGE_LOG(WARN, "compare_base_iter failed" , K(ret), K(*this));
    }
  } else if (OB_FAIL(inc_merger_->pop())) {
    STORAGE_LOG(WARN, "rows_merger_ pop failed", K(ret), KPC(inc_merger_));
  } else if (!inc_merger_is_unique_champion) {
    //rows_merger has same champion,merge state unchanged
  } else if (merger_state_ == BASE_ITER_WIN) {
    //There is only one champion in the rows merger and is popped, so base_item_ is only champion now
    major_row_equal_to_inc_ = false;
  } else if (merger_state_ == NEED_SKIP) { // all inc champions have popped
    // 1. is_need_skip() needs to be true
    // 2. if inc merger is end, no push, rebuild() needs to rebuild
    merger_state_ = NEED_SKIP_REBUILD;
  } else if (!majors_merger_->empty()) {
    if (OB_FAIL(compare_base_iter())) {
      STORAGE_LOG(WARN, "compare_base_iter failed" , K(ret), K(*this));
    }
  }

  return ret;
}

int ObPartitionMajorRowsMerger::push(const ObPartitionMergeLoserTreeItem &row)
{
  int ret = OB_SUCCESS;

  if (merger_state_ == NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row is not valid", K(ret));
  } else if (row.iter_->is_base_iter()) {
    if (OB_FAIL(majors_merger_->push(row))) {
      STORAGE_LOG(WARN, "failed to push majors merger", K(ret), KPC(majors_merger_));
    }
  } else if (OB_FAIL(inc_merger_->push(row))) {
    STORAGE_LOG(WARN, "rows_merger_ push failed", K(ret), KPC(inc_merger_));
  }

  if (OB_SUCC(ret)) {
    merger_state_ = NEED_REBUILD;
  }
  return ret;
}

int ObPartitionMajorRowsMerger::push_top(const ObPartitionMergeLoserTreeItem &row)
{
  int ret = OB_SUCCESS;

  if (merger_state_ == NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == row.iter_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row.iter_ is null", K(ret));
  } else if (merger_state_ == NEED_REBUILD || merger_state_ == NEED_SKIP) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tree need rebuild", K(ret));
  } else if (!row.iter_->is_base_iter()) {
    if (OB_FAIL(inc_merger_->push_top(row))) {
      STORAGE_LOG(WARN, "rows merger push top failed", K(ret), KPC(row.iter_));
    }
  } else if (OB_FAIL(majors_merger_->push_top(row))) {
    STORAGE_LOG(WARN, "majors merger failed to push top", K(ret), KPC(row.iter_));
  }

  if (OB_FAIL(ret)) {
  } else if (majors_merger_->empty() || // only inc merger
     (!row.iter_->is_base_iter() && merger_state_ == LOSER_TREE_WIN)) {
    // pop() has rebuilt and LOSER_TREE_WIN, so whether the pushed inc row is smaller or larger, inc merger always win
  } else if (OB_FAIL(compare_base_iter())){
    STORAGE_LOG(WARN, "compare_base_iter failed", K(ret), K(*this));
  }
  return ret;
}

int ObPartitionMajorRowsMerger::rebuild()
{
  int ret = OB_SUCCESS;
  if (merger_state_ == NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (merger_state_ != NEED_REBUILD && merger_state_ != NEED_SKIP_REBUILD) { //do nothing
  } else if (OB_FAIL(inc_merger_->rebuild())) {
    STORAGE_LOG(WARN, "rows_merger_ rebuild failed", K(ret), KPC(inc_merger_));
  } else if (majors_merger_->empty()) {
    merger_state_ = LOSER_TREE_WIN;
  } else if (OB_FAIL(majors_merger_->rebuild())) {
    STORAGE_LOG(WARN, "majors_merger_ rebuild failed", K(ret), KPC(majors_merger_));
  } else if (OB_FAIL(compare_base_iter())) {
    STORAGE_LOG(WARN, "compare_base_iter failed", K(ret), K(*this));
  }
  return ret;
}

int ObPartitionMajorRowsMerger::open(const int64_t total_player_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(merger_state_ == NOT_INIT)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(inc_merger_->open(total_player_cnt))) {
    STORAGE_LOG(WARN, "failed to open rows_merger", K(ret));
  } else if (OB_FAIL(majors_merger_->open(major_cnt_))) {
    STORAGE_LOG(WARN, "failed to open rows_merger", K(ret));
  } else {
    merger_state_ = LOSER_TREE_WIN;
  }

  return ret;
}

int ObPartitionMajorRowsMerger::count() const
{
  int count = 0;
  if (merger_state_ == NOT_INIT) {
  } else {
    count = inc_merger_->count() + majors_merger_->count();
  }
  return count;
}

bool ObPartitionMajorRowsMerger::empty() const
{
  return merger_state_ == NOT_INIT ? true : majors_merger_->empty() && inc_merger_->empty();
}

bool ObPartitionMajorRowsMerger::is_unique_champion() const
{
  bool is_unique_champion = true;
  if (merger_state_ == NOT_INIT) {
  } else {
    is_unique_champion = merger_state_ == BASE_ITER_WIN ? !major_row_equal_to_inc_ : inc_merger_->is_unique_champion();
  }
  return is_unique_champion;
}

int ObPartitionMajorRowsMerger::init_rows_merger(const int64_t max_player_cnt, const int64_t total_player_cnt, RowsMerger *&merger)
{
  int ret = OB_SUCCESS;
  if (total_player_cnt <= ObSimpleRowsPartitionMerger::USE_SIMPLE_MERGER_MAX_TABLE_CNT) {
    if (OB_ISNULL(merger = alloc_helper<ObSimpleRowsPartitionMerger>(*allocator_, cmp_))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc rows merger", K(ret));
    }
  } else if (OB_ISNULL(merger = alloc_helper<ObPartitionMergeLoserTree>(*allocator_, cmp_))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc rows merger", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(merger->init(max_player_cnt, total_player_cnt, *allocator_))){
    STORAGE_LOG(WARN, "failed to init rows merger", K(ret), KPC(merger));
  }

  return ret;
}

int ObPartitionMajorRowsMerger::compare_base_iter()
{
  int ret = OB_SUCCESS;
  const ObPartitionMergeLoserTreeItem *inc_item;
  const ObPartitionMergeLoserTreeItem *base_item;
  if (OB_UNLIKELY(majors_merger_->empty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected empty majors merger", K(ret), KPC(majors_merger_));
  } else {
    bool finish = false;
    int64_t cmp_ret = 0;
    while (OB_SUCC(ret) && !finish) {
      bool is_purged = false;
      if (inc_merger_->empty()) {
        merger_state_ = BASE_ITER_WIN;
        major_row_equal_to_inc_ = false;
        finish = true;
      } else if (OB_FAIL(majors_merger_->top(base_item))) {
        STORAGE_LOG(WARN, "majors merger top failed", K(ret), KPC(majors_merger_));
      } else if (OB_FAIL(inc_merger_->top(inc_item))) {
        STORAGE_LOG(WARN, "inc_merger_ top failed", K(ret), KPC(inc_merger_));
      } else if (OB_FAIL(cmp_.compare(*base_item, *inc_item, cmp_ret))) {
        STORAGE_LOG(WARN, "fail to compare", K(ret), KPC(base_item), KPC(inc_item));
      } else if ((finish = cmp_.check_cmp_finish(cmp_ret))) {
        merger_state_ = cmp_ret <= 0 ? BASE_ITER_WIN : LOSER_TREE_WIN;
        major_row_equal_to_inc_ = (cmp_ret == 0);
      } else if (cmp_.need_open_right_range(cmp_ret)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected right macro need open", K(ret), K(cmp_ret));
      } else if (cmp_.need_open_left_range(cmp_ret)) {
        if (OB_FAIL(check_row_iters_purge(*inc_item->iter_, *base_item->iter_, is_purged))) {
          STORAGE_LOG(WARN, "Failed to check purge row iters", K(ret), KPC(inc_item->iter_));
        } else if (is_purged) {
          merger_state_ = NEED_SKIP;
          break;
        } else if (OB_FAIL(base_item->iter_->open_curr_range(false))) {
          STORAGE_LOG(WARN, "Failed to base iter open_curr_range", K(ret), KPC(base_item->iter_));
        }
      }
    }
  }
  return ret;
}

int ObPartitionMajorRowsMerger::check_row_iters_purge(
    const ObPartitionMergeIter &check_iter,
    ObPartitionMergeIter &base_iter,
    bool &can_purged)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *curr_row = nullptr;
  can_purged = false;

  if (OB_ISNULL(curr_row = check_iter.get_curr_row())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur row of minor sstable row iter is unexpected null", K(ret), K(check_iter));
  } else if (curr_row->row_flag_.is_delete()) {
    // we cannot trust the dml flag
    //

    // TODO we need add first dml to optimize the purge cost
    //const ObDatumRow *tmp_row = nullptr;
    //if (OB_ISNULL(tmp_row = minimum_iters.at(minimum_iters.count() - 1)->get_curr_row())) { // get the oldest iter
    //ret = OB_ERR_UNEXPECTED;
    //LOG_WARN("cur row of minor sstable row iter is unexpected null", K(ret), K(minimum_iters));
    //} else if (tmp_row->row_flag_.is_insert() || tmp_row->row_flag_.is_insert_delete()) {
    //// contain insert row, no need to check exist in major_sstable
    //can_purged = true;
    //LOG_DEBUG("merge check_row_iters_purge", K(can_purged), "flag", tmp_row->row_flag_);
    //} else {
    bool need_open = false;
    if (OB_FAIL(base_iter.need_open_curr_range(*curr_row, need_open))) {
      STORAGE_LOG(WARN, "Failed to check if rowkey exist in base iter", K(ret));
    } else if (!need_open) {
      can_purged = true;
      LOG_DEBUG("merge check_row_iters_purge", K(can_purged), K(*curr_row));
    }
    //}
  }
  return ret;
}


/**
 * ---------------------------------------------------------ObPartitionMergeHelper--------------------------------------------------------------
 */
int ObPartitionMergeHelper::init(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (!merge_param.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid input arguments", K(ret), K(merge_param));
  } else if (OB_FAIL(init_merge_iters(merge_param))){
    STORAGE_LOG(WARN, "Failed to init merge iters", K(ret), K(merge_param));
  } else if (OB_FAIL(move_iters_next(merge_iters_))) {
    STORAGE_LOG(WARN, "failed to move iters", K(ret), K(merge_iters_));
  } else if (OB_FAIL(prepare_rows_merger(merge_param))) {
    STORAGE_LOG(WARN, "failed to prepare rows merger", K(ret), K(*this));
  } else {
    consume_iter_idxs_.reset();
    is_inited_ = true;
  }

  return ret;
}

int ObPartitionMergeHelper::init_merge_iters(const ObMergeParameter &merge_param)
{
    int ret = OB_SUCCESS;
    merge_iters_.reset();
    const ObTablesHandleArray &tables_handle = merge_param.get_tables_handle();
    int64_t table_cnt = tables_handle.get_count();
    ObITable *table = nullptr;
    ObSSTable *sstable = nullptr;
    ObPartitionMergeIter *merge_iter = nullptr;

    if (merge_param.is_mv_merge() && OB_FAIL(init_mv_merge_iters(merge_param))) {
      STORAGE_LOG(WARN, "Failed to init mv merge iters", K(ret), K(merge_param));
    }
    for (int64_t i = table_cnt - 1; OB_SUCC(ret) && i >= 0; i--) {
      if (OB_ISNULL(table = tables_handle.get_table(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null iter", K(ret), K(i), K(merge_param));
      } else if (OB_UNLIKELY(table->is_remote_logical_minor_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected remote minor sstable", K(ret), KP(sstable));
      } else if (is_co_major_helper() && table->is_major_type_sstable()) {
        continue;
      } else if (is_multi_major_helper() && !table->is_major_type_sstable()) {
        continue;
      } else if (FALSE_IT(sstable = static_cast<ObSSTable *>(table))) {
        //TODO(COLUMN_STORE) tmp code, use specific cg sstable according to the ctx of sub merge task
      } else if (merge_param.is_empty_table(*table)) {
        // do nothing. don't need to construct iter for empty sstable
        FLOG_INFO("table is empty, need not create iter", K(i), "sstable_key", sstable->get_key());
        continue;
      } else if (OB_ISNULL(merge_iter = alloc_merge_iter(merge_param, i, table))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc memory for merge iter", K(ret));
      } else if (OB_FAIL(merge_iter->init(merge_param, i, &read_info_))) {
        if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
          STORAGE_LOG(WARN, "Failed to init merge iter", K(ret));
        } else {
          FLOG_INFO("Ignore sstable beyond the range", K(i), K(merge_param.merge_range_), "sstable_key", sstable->get_key());
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(merge_iters_.push_back(merge_iter))) {
        STORAGE_LOG(WARN, "Failed to push back merge iter", K(ret), KPC(merge_iter));
      } else {
        STORAGE_LOG(INFO, "Succ to init iter", K(ret), K(i), KPC(merge_iter));
        merge_iter = nullptr;
      }

      if (nullptr != merge_iter) {
        merge_iter->~ObPartitionMergeIter();
        merge_iter = nullptr;
      }
    }

    return ret;
}

int ObPartitionMergeHelper::init_mv_merge_iters(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 != merge_iters_.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected iters count in mv merge", K(ret));
  } else {
    ObPartitionMergeIter *merge_iter = nullptr;
    for (int64_t i = merge_param.mview_merge_param_->refresh_sql_count_ - 1; OB_SUCC(ret) && i >= 0 ; i--) {
      if (OB_ISNULL(merge_iter = alloc_helper<ObPartitionMVRowMergeIter>(allocator_, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc memory for mv merge iter", K(ret));
      } else if (OB_FAIL(merge_iter->init(merge_param, i, &read_info_))) {
        STORAGE_LOG(WARN, "Failed to init mv merge iter", K(ret), K(i));
      } else if (OB_FAIL(merge_iters_.push_back(merge_iter))) {
        STORAGE_LOG(WARN, "Failed to push back mv merge iter", K(ret), KPC(merge_iter));
      } else {
        STORAGE_LOG(INFO, "Succ to init mv merge iter", K(ret), K(i), KPC(merge_iter));
        merge_iter = nullptr;
      }
      if (OB_FAIL(ret) && nullptr != merge_iter) {
        merge_iter->~ObPartitionMergeIter();
        allocator_.free(merge_iter);
      }
    }
  }
  return ret;
}

int ObPartitionMergeHelper::prepare_rows_merger(const ObMergeParameter &merge_param)
{
  int ret = OB_SUCCESS;

  if (merge_iters_.empty()) {
    FLOG_INFO("merge_iters is empty, skip build rows merger", K(merge_iters_));// skip build rows merger
  } else {
    void *buf = nullptr;
    int64_t iters_cnt = merge_iters_.count();
    const int64_t max_table_cnt = common::MAX_SSTABLE_CNT_IN_STORAGE + common::MAX_INC_MAJOR_SSTABLE_CNT;
    ObPartitionMergeIter *merge_iter = nullptr;

    if (OB_UNLIKELY(max_table_cnt < iters_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get unexpected iters count", K(ret), K(max_table_cnt), K(iters_cnt));
    } else if (OB_ISNULL(merge_iter = merge_iters_.at(iters_cnt - 1))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null iter", K(ret));
    } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObPartitionMergeLoserTreeCmp)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to allocate memory", K(ret));
    } else if (FALSE_IT(cmp_ = new (buf) ObPartitionMergeLoserTreeCmp(read_info_.get_datum_utils(),
                                                                      read_info_.get_schema_rowkey_count()))) {
    } else if (!is_multi_major_helper() && merge_iter->is_major_sstable_iter() && merge_iter->is_macro_merge_iter()) {
      const int64_t major_sstable_count = merge_param.static_param_.get_major_sstable_count();
      rows_merger_ = alloc_helper<ObPartitionMajorRowsMerger>(allocator_, *cmp_, major_sstable_count);
    } else if (iters_cnt <= ObSimpleRowsPartitionMerger::USE_SIMPLE_MERGER_MAX_TABLE_CNT) {
      rows_merger_ = alloc_helper<ObSimpleRowsPartitionMerger>(allocator_, *cmp_);
    } else {
      rows_merger_ = alloc_helper<ObPartitionMergeLoserTree>(allocator_, *cmp_);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(nullptr == rows_merger_)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc rows merger", K(ret));
    } else if (OB_FAIL(rows_merger_->init(max_table_cnt, iters_cnt, allocator_))) {
      STORAGE_LOG(WARN, "Failed to init rows merger", K(ret), K(iters_cnt));
    } else if (OB_FAIL(build_rows_merger())) {
      STORAGE_LOG(WARN, "failed to build rows merge", K(ret));
    }
  }

  return ret;
}

int ObPartitionMergeHelper::has_incremental_data(bool &has_incremental_data) const
{
  int ret = OB_SUCCESS;
  has_incremental_data = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionMergeHelper is not inited", K(ret));
  } else if (merge_iters_.empty() || rows_merger_->empty()) { //has_incremental_data is false
  } else if (rows_merger_->count() > 1){
    has_incremental_data = true;
  } else {
    //rows_merger only one item
    const ObPartitionMergeLoserTreeItem *top_item = nullptr;
    if (OB_FAIL(rows_merger_->top(top_item))) {
      STORAGE_LOG(WARN, "get top item fail", K(ret), KPC(rows_merger_));
    } else if (OB_UNLIKELY(nullptr == top_item || !top_item->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected top item", K(ret), KPC(top_item));
    } else if (!top_item->iter_->is_base_iter() || !top_item->iter_->is_macro_merge_iter() /* for small sstable */) {
      has_incremental_data = true;
    }
  }

  return ret;
}

int ObPartitionMergeHelper::check_iter_end() const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionMergeHelper is not init", K(ret));
  } else {
    ObPartitionMergeIter *iter = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_iters_.count(); ++i) {
      if (OB_ISNULL(iter = merge_iters_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null merge iter", K(ret), K(i), K(merge_iters_));
      } else if (!iter->is_iter_end()) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "Merge iter not iter to end", K(ret), KPC(iter));
      }
    }
  }
  return ret;
}

int64_t ObPartitionMergeHelper::get_iters_row_count() const
{
  int64_t iter_row_count = 0;
  ObPartitionMergeIter *iter = nullptr;
  for (int64_t i = 0; i < merge_iters_.count(); ++i) {
    if (OB_NOT_NULL(iter = merge_iters_.at(i))) {
      iter_row_count += iter->get_iter_row_count();
    }
  }
  return iter_row_count;
}

int ObPartitionMergeHelper::move_iters_next(MERGE_ITER_ARRAY &merge_iters)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_iters.count(); ++i) {
    if (OB_ISNULL(merge_iters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null merge iter", K(ret), K(i), K(merge_iters));
    } else if (OB_FAIL(merge_iters.at(i)->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "Failed to next merge iter", K(i), K(ret), KPC(merge_iters.at(i)));
      }
    }
  }
  return ret;
}

int ObPartitionMergeHelper::find_rowkey_minimum_iters(MERGE_ITER_ARRAY &minimum_iters)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionMergeHelper is not inited", K(ret));
  } else if (OB_UNLIKELY(merge_iters_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null merge iters", K(ret), K(merge_iters_));
  } else if (OB_UNLIKELY(rows_merger_->empty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "rows_merger_ is empty", K(ret), KPC(rows_merger_));
  } else {
    minimum_iters.reset();
    consume_iter_idxs_.reset();
    bool has_same_rowkey = false;
    const ObPartitionMergeLoserTreeItem *top_item = nullptr;

    do {
      has_same_rowkey = !rows_merger_->is_unique_champion();
      if (OB_FAIL(check_unique_champion())) {
        STORAGE_LOG(WARN, "check unique champion fail", K(ret));
      } else if (OB_FAIL(rows_merger_->top(top_item))) {
        STORAGE_LOG(WARN, "get top item fail", K(ret), KPC(rows_merger_));
      } else if (OB_UNLIKELY(nullptr == top_item || !top_item->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "item or row is null", K(ret), KP(top_item));
      } else {
        const int64_t iter_idx = top_item->iter_idx_;
        if (OB_FAIL(minimum_iters.push_back(top_item->iter_))) {
          STORAGE_LOG(WARN, "Fail to push merge_iter to minimum_iters", K(ret), K(minimum_iters));
        } else if (OB_FAIL(consume_iter_idxs_.push_back(iter_idx))) {
          STORAGE_LOG(WARN, "Fail to push consume iter idx to consume_iters", K(ret), K(consume_iter_idxs_));
        } else if (OB_FAIL(rows_merger_->pop())) {
          STORAGE_LOG(WARN, "loser tree pop error", K(ret), K(has_same_rowkey), KPC(rows_merger_));
        }
      }
    } while (OB_SUCC(ret) && !rows_merger_->empty() && has_same_rowkey);
  }

  return ret;
}

bool ObPartitionMergeHelper::is_need_skip() const
{
  bool is_need_skip = false;
  if (rows_merger_ != nullptr && rows_merger_->type() == common::ObRowMergerType::MAJOR_ROWS_MERGE) {
    is_need_skip = static_cast<ObPartitionMajorRowsMerger *>(rows_merger_)->is_need_skip();
  }
  return is_need_skip;
}

int ObPartitionMergeHelper::build_rows_merger()
{
  int ret = OB_SUCCESS;

  do {
    const int64_t iters_cnt = merge_iters_.count();
    rows_merger_->reuse();

    if (OB_FAIL(rows_merger_->open(iters_cnt))) {
      STORAGE_LOG(WARN, "failed to open rows_merger_", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < iters_cnt; i++) {
      ObPartitionMergeIter *iter = merge_iters_.at(i);
      if (OB_UNLIKELY(nullptr == iter)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null iter", K(ret), K(i), K(merge_iters_));
      } else if (iter->is_iter_end()) {
        //skip iter end
      } else {
        ObPartitionMergeLoserTreeItem item;
        item.iter_ = iter;
        item.iter_idx_ = i;
        if (OB_FAIL(rows_merger_->push(item))) {
          if (OB_BLOCK_SWITCHED != ret) {
            STORAGE_LOG(WARN, "failed to push item", K(ret), K(i), KPC(rows_merger_));
          }
        }
      }
    } // end for

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(rows_merger_->rebuild())) {
      STORAGE_LOG(WARN, "loser tree rebuild fail", K(ret), KPC(rows_merger_));
    }
  } while(ret == OB_BLOCK_SWITCHED);

  if (OB_SUCC(ret)) {
    consume_iter_idxs_.reset();
  }

  return ret;
}

int ObPartitionMergeHelper::rebuild_rows_merger()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionMergeHelper is not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < consume_iter_idxs_.count(); i++) {
      const int64_t iter_idx = consume_iter_idxs_.at(i);
      ObPartitionMergeIter *iter = nullptr;
      if (OB_UNLIKELY(iter_idx < 0 || iter_idx >= merge_iters_.count())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "iter_idx unexpected", K(ret), K(iter_idx), K(merge_iters_));
      } else if (OB_ISNULL(iter = merge_iters_.at(iter_idx))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null iter", K(ret), K(iter_idx), K(merge_iters_));
      } else if (iter->is_iter_end()) { //skip iter end
      } else {
        ObPartitionMergeLoserTreeItem item;
        item.iter_ = iter;
        item.iter_idx_ = iter_idx;
        if (1 == consume_iter_idxs_.count()) {
          if (OB_FAIL(rows_merger_->push_top(item))) {
            STORAGE_LOG(WARN, "failed to push top item", K(ret), K(iter_idx), KPC(rows_merger_));
          }
        } else if (OB_FAIL(rows_merger_->push(item))) {
          STORAGE_LOG(WARN, "failed to push item", K(ret), K(iter_idx), KPC(rows_merger_));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(consume_iter_idxs_.reset())) {
    } else if (!rows_merger_->empty()) {
      if (OB_FAIL(rows_merger_->rebuild())) {
        STORAGE_LOG(WARN, "failed to rebuild rows merger", K(ret), KPC(rows_merger_));
      }
    }
  }

  if (ret == OB_BLOCK_SWITCHED) {
    if (OB_FAIL(build_rows_merger())) {
      STORAGE_LOG(WARN, "failed to build_rows_merger", K(ret));
    }
  }

  return ret;
}

void ObPartitionMergeHelper::reset()
{
  ObPartitionMergeIter *iter = nullptr;
  const ObITable *table = nullptr;
  for (int64_t i = 0; i < merge_iters_.count(); ++i) {
    if (OB_NOT_NULL(iter = merge_iters_.at(i))) {
      if (OB_NOT_NULL(table = iter->get_table())) {
        FLOG_INFO("partition merge iter row count", K(i), "row_count", iter->get_iter_row_count(),
            "ghost_row_count", iter->get_ghost_row_count(), "table_key", table->get_key());
      } else {
        FLOG_INFO("partition merge iter row count", K(i), "row_count", iter->get_iter_row_count(), KPC(iter));
      }
      iter->~ObPartitionMergeIter();
      iter = nullptr;
    }
  }

  merge_iters_.reset();
  consume_iter_idxs_.reset();
  if (OB_NOT_NULL(cmp_)) {
    cmp_->~ObPartitionMergeLoserTreeCmp();
    cmp_ = nullptr;
  }
  if (OB_NOT_NULL(rows_merger_)) {
    rows_merger_->~ObRowsMerger();
    rows_merger_ = nullptr;
  }
  is_inited_ = false;
}

/**
 * ---------------------------------------------------------ObPartitionMajorMergeHelper--------------------------------------------------------------
 */
ObPartitionMergeIter *ObPartitionMajorMergeHelper::alloc_merge_iter(
    const ObMergeParameter &merge_param,
    const int64_t sstable_idx,
    const ObITable *table)
{
  ObPartitionMergeIter *merge_iter = nullptr;
  if (OB_UNLIKELY(nullptr == table || !table->is_sstable())) {
    // do nothing
  } else if (need_check_major_sstable() && !table->is_major_type_sstable()) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected alloc iter for minor sstable", KPC(table));
  } else if (merge_param.is_full_merge() || table_need_full_merge(sstable_idx, *table, merge_param)) {
    const bool need_co_sstable_scan = need_all_column_from_rowkey_co_sstable(*table, merge_param);
    merge_iter = alloc_helper<ObPartitionRowMergeIter>(allocator_, allocator_, need_co_sstable_scan);
  } else {
    ObMergeLevel merge_level = MERGE_LEVEL_MAX;
    if (OB_UNLIKELY(OB_SUCCESS != (merge_param.static_param_.get_sstable_merge_level(sstable_idx, merge_level)))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "failed to get merge level", K(sstable_idx), K(merge_param));
    } else if (OB_UNLIKELY(MERGE_LEVEL_MAX == merge_level)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected merge level", K(sstable_idx), K(merge_param));
    } else if (static_cast<const ObSSTable *>(table)->is_small_sstable()) {
      const uint64_t compat_version = merge_param.static_param_.data_version_;
      if (compat_version >= DATA_VERSION_4_3_5_1 && MICRO_BLOCK_MERGE_LEVEL == merge_level) {
        merge_iter = alloc_helper<ObPartitionMicroMergeIter>(allocator_, allocator_);
      } else {
        merge_iter = alloc_helper<ObPartitionRowMergeIter>(allocator_, allocator_);
      }
    } else if (MICRO_BLOCK_MERGE_LEVEL == merge_level) {
      merge_iter = alloc_helper<ObPartitionMicroMergeIter>(allocator_, allocator_);
    } else {
      merge_iter = alloc_helper<ObPartitionMacroMergeIter>(allocator_, allocator_);
    }
  }

  return merge_iter;
}

/**
 * ---------------------------------------------------------ObMultiMajorMergeIter--------------------------------------------------------------
 */
int ObMultiMajorMergeIter::check_unique_champion()
{
  int ret = OB_SUCCESS;
  if (!rows_merger_->is_unique_champion()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected same rowkey in different major sstables", K(ret));
  }
  return ret;
}

bool ObMultiMajorMergeIter::check_could_move_to_end()
{
  return nullptr != rows_merger_ && rows_merger_->empty() && consume_iter_idxs_.count() == 1;
}

void ObMultiMajorMergeIter::move_to_end()
{
  consume_iter_idxs_.reset();
}

bool ObMultiMajorMergeIter::need_all_column_from_rowkey_co_sstable(const ObITable &table, const ObMergeParameter &merge_param) const
{
  bool bret = false;
  if (!table.is_major_type_sstable()) {
    // do nothing
  } else {
    bret = replay_base_directly_ &&
      (merge_param.get_schema()->has_all_column_group() || merge_param.static_param_.is_build_row_store()) && // replay_add_cg_directly
      (table.is_co_sstable() ? static_cast<const ObCOSSTableV2 &>(table).is_rowkey_cg_base() : false);
  }
  return bret;
}

bool ObMultiMajorMergeIter::table_need_full_merge(
    const int64_t sstable_idx,
    const ObITable &table,
    const ObMergeParameter &merge_param) const
{
  bool bret = false;
  if (!table.is_major_type_sstable() || !table.is_co_sstable()) {
    bret = true;
  } else if (!replay_base_directly_) {
  } else if (OB_SUCCESS != (merge_param.static_param_.get_sstable_need_full_merge(sstable_idx, bret))) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "failed to get sstable need_full_merge", K(sstable_idx), K(table), K(merge_param));
  } else if (bret) {
  } else if (merge_param.get_schema()->has_all_column_group() || merge_param.static_param_.is_build_row_store()) {
    // rowkey base cg output all column
    bret = static_cast<const ObCOSSTableV2 &>(table).is_rowkey_cg_base();
  } else {
    // PURE_COL_ONLY_ALL output rowkey column
    bret = static_cast<const ObCOSSTableV2 &>(table).is_row_store_only_co_table() && !merge_param.get_schema()->has_all_column_group();
  }
  return bret;
}

int ObMultiMajorMergeIter::get_current_major_iter(ObPartitionMergeIter *&row_store_iter)
{
  int ret = OB_SUCCESS;
  row_store_iter = nullptr;
  if (is_iter_end()) { // do nothing
  } else {
    if (nullptr == row_store_iter_) {
      if (OB_FAIL(find_rowkey_minimum_iters(minimum_iters_))) {
        LOG_WARN("failed to find rowkey minimum iters", K(ret));
      } else if (OB_UNLIKELY(minimum_iters_.count() != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty minimum iters", K(ret), K(minimum_iters_));
      } else {
        row_store_iter_ = minimum_iters_.at(0);
        row_store_iter_->set_major_idx(consume_iter_idxs_.at(0));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(row_store_iter_) || row_store_iter_->get_major_idx() < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret), K(row_store_iter_));
    } else {
      row_store_iter = row_store_iter_;
    }
  }
  return ret;
}

int ObMultiMajorMergeIter::next()
{
  int ret = OB_SUCCESS;
  if (is_iter_end()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(move_iters_next(minimum_iters_))) {
    LOG_WARN("failed to move iters next", K(ret), K(minimum_iters_));
  } else if (OB_FAIL(rebuild_rows_merger())) {
    LOG_WARN("failed to rebuild rows merger", K(ret));
  } else {
    row_store_iter_ = nullptr;
    minimum_iters_.reset();
  }
  return ret;
}

/**
 * ---------------------------------------------------------ObPartitionMinorMergeHelper--------------------------------------------------------------
 */

int ObPartitionMinorMergeHelper::collect_tnode_dml_stat(
    const ObMergeType &merge_type,
    storage::ObTransNodeDMLStat &tnode_stat) const
{
  int ret = OB_SUCCESS;
  tnode_stat.reset();

  if (OB_UNLIKELY(!is_mini_merge(merge_type))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid argument", K(ret), "merge_type", merge_type_to_str(merge_type));
  }

  const MERGE_ITER_ARRAY &merge_iters = get_merge_iters();
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_iters.count(); ++i) {
    const ObPartitionMergeIter *iter = merge_iters.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get unexpected null merge iter", K(ret));
    } else if (OB_FAIL(iter->collect_tnode_dml_stat(tnode_stat))) {
      STORAGE_LOG(WARN, "failed to collect mt stat", K(ret));
    }
  }
  return ret;
}

// for local minor merge under shared-storage mode, disable reuse shared-macro-block
static bool is_ss_local_minor_with_shared_sstable(const ObStaticMergeParam &static_param, const ObITable *table)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  if (GCTX.is_shared_storage_mode() && is_local_exec_mode(static_param.get_exec_mode()) && table->is_sstable()) {
     const ObSSTable *sstable = static_cast<const ObSSTable *>(table);
     ObSSTableMetaHandle meta_handle;
     if (OB_FAIL(sstable->get_meta(meta_handle))) {
       LOG_ERROR("failed to get sstable meta, will disable reuse macro-block", K(ret), KPC(sstable));
       bret = true;
     } else {
       bret = meta_handle.get_sstable_meta().get_table_shared_flag().is_shared_sstable();
     }
  }
  return bret;
}

ObPartitionMergeIter *ObPartitionMinorMergeHelper::alloc_merge_iter(
    const ObMergeParameter &merge_param,
    const int64_t sstable_idx,
    const ObITable *table)
{
  UNUSED(sstable_idx);
  int ret = OB_SUCCESS;
  const ObStaticMergeParam &static_param = merge_param.static_param_;
  ObPartitionMergeIter *merge_iter = nullptr;
  if (OB_ISNULL(table)) {
    // do nothing
  } else if (!(table->is_sstable() && static_cast<const ObSSTable*>(table)->is_small_sstable())
      && !is_mini_merge(static_param.get_merge_type())
      && !static_param.is_full_merge_
      && static_param.sstable_logic_seq_ < ObMacroDataSeq::MAX_SSTABLE_SEQ
      && !is_ss_local_minor_with_shared_sstable(static_param, table)) {
    ObSSTableMetaHandle meta_handle;
    bool reuse_uncommit_row = false;
    //we only have the tx_id on sstable meta, without seq_no, the tuples in the macro block could still be abort
    //open this flag when we support open empty macro block during reuse/rewrite processing

    //if (!transaction::ObTransID(static_param.tx_id_).is_valid() || !static_cast<const ObSSTable *>(table)->contain_uncommitted_row()) {
      //reuse_uncommit_row = false;
    //} else if (OB_FAIL(static_cast<const ObSSTable *>(table)->get_meta(meta_handle))) {
      //STORAGE_LOG(ERROR, "fail to get meta", K(ret), KPC(table));
    //} else if (meta_handle.get_sstable_meta().get_tx_id_count() > 0) {
      //const int64_t tx_id = meta_handle.get_sstable_meta().get_tx_ids(0);
      //if (OB_UNLIKELY(meta_handle.get_sstable_meta().get_tx_id_count() != 1)) {
        //ret = OB_ERR_UNEXPECTED;
        //STORAGE_LOG(ERROR, "unexpected tx id count", K(ret), KPC(table), KPC(meta_handle.meta_));
      //} else {
        //reuse_uncommit_row = tx_id == static_param.tx_id_;
      //}
    //}
    merge_iter = alloc_helper<ObPartitionMinorMacroMergeIter>(allocator_, allocator_, reuse_uncommit_row);
  } else {
    merge_iter = alloc_helper<ObPartitionMinorRowMergeIter>(allocator_, allocator_);
  }

  return merge_iter;
}

} //namespace compaction
} //namespace oceanbase
