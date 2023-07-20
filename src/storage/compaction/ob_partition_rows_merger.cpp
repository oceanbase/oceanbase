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

namespace oceanbase
{
using namespace blocksstable;
using namespace storage;
namespace compaction
{

template <typename T, typename... Args> T *alloc_helper(common::ObIAllocator &allocator, Args&... args)
{
  static_assert(std::is_constructible<T, Args&...>::value, "invalid construct arguments");
  void *buf = nullptr;
  T *rows_merger = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(T)))) {
  } else {
    rows_merger = new (buf) T(args...);
  }

  return rows_merger;
}

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
    cmp_result = ALL_MACRO_NEED_OPEN;
  } else if (temp_cmp_ret > 0) {
    /*
     * left_end_key > right_end_key, compare right_end_key and left_start_key
     * if right_end_key < left_start_key,right can be reused,
     * else left_start_key < right_end_key < left_end_key,
     * because interal right is closed, left range more likely to be opened
     * and right may be reused. so open the left range first.
     */
    if (OB_FAIL(right_end_key.compare(left_start_key , datum_utils_, temp_cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare rowkey", K(ret), K(datum_utils_), K(left_start_key), K(right_end_key));
    } else {
      cmp_result = temp_cmp_ret >= 0 ? LEFT_MACRO_NEED_OPEN : 1;
    }
  } else if (temp_cmp_ret < 0) {
    if (OB_FAIL(left_end_key.compare(right_start_key , datum_utils_, temp_cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare rowkey", K(ret), K(datum_utils_), K(left_end_key), K(right_start_key));
    } else {
      cmp_result = temp_cmp_ret >= 0 ? RIGHT_MACRO_NEED_OPEN : -1;
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
    // STORAGE_LOG(INFO, "rowkey equal last end_key, maybe aborted trans", K(rowkey), K(range));
    cmp_ret = RIGHT_MACRO_NEED_OPEN;
  } else if (OB_FAIL(rowkey.compare(range.get_end_key(), datum_utils_, temp_cmp_ret))) {
    STORAGE_LOG(WARN, "Failed to compare end key", K(ret), K(rowkey), K(range));
  } else if (temp_cmp_ret == 0) {
    if (!range.get_border_flag().inclusive_end()) {
      cmp_ret = 1;
    } else {
      cmp_ret = RIGHT_MACRO_NEED_OPEN;
    }
  } else if (temp_cmp_ret > 0) {
    cmp_ret = 1;
  } else {
    cmp_ret = RIGHT_MACRO_NEED_OPEN;
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

bool ObPartitionMergeLoserTreeCmp::check_cmp_finish(const int64_t cmp_ret)
{
  return !need_open_left_macro(cmp_ret) && !need_open_right_macro(cmp_ret);
}

int ObPartitionMergeLoserTreeCmp::open_iter_range(
    const int64_t cmp_ret,
    const ObPartitionMergeLoserTreeItem &left,
    const ObPartitionMergeLoserTreeItem &right)
{
  int ret = OB_SUCCESS;
  if (need_open_left_macro(cmp_ret) &&
      OB_FAIL(left.iter_->open_curr_range(false, true /* for compare */))) {
    if (ret != OB_BLOCK_SWITCHED) {
      STORAGE_LOG(WARN, "open curr range failed", K(ret), K(*left.iter_));
    }
  } else if (need_open_right_macro(cmp_ret) &&
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
int ObPartitionMajorRowsMerger::init(const int64_t total_player_cnt,
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
    if (OB_FAIL(init_rows_merger(total_player_cnt))) {
      STORAGE_LOG(WARN, "init rows merger failed", K(ret), K(total_player_cnt));
    } else {
      merger_state_ = LOSER_TREE_WIN;
      base_item_.reset();
    }
  }
  return ret;
}

void ObPartitionMajorRowsMerger::reset()
{
  if (rows_merger_ != nullptr) {
    rows_merger_->reset();
    if (allocator_ != nullptr) {
      rows_merger_->~ObRowsMerger();

    }
  }
  base_item_.reset();
  rows_merger_ = nullptr;
  allocator_ = nullptr;
  merger_state_ = NOT_INIT;
}

void ObPartitionMajorRowsMerger::reuse()
{
  if (rows_merger_ != nullptr) {
    rows_merger_->reuse();
  }
  base_item_.reset();
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
  } else if (merger_state_ == BASE_ITER_WIN && !base_item_.equal_with_next_) {
    row = &base_item_;
  } else if (OB_FAIL(rows_merger_->top(row))) {
    STORAGE_LOG(WARN, "rows_merger_ top failed", K(ret), K(*this));
  }
  return ret;
}

int ObPartitionMajorRowsMerger::pop()
{
  int ret = OB_SUCCESS;
  int inner_merger_is_unique_champion = false;

  if (merger_state_ == NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (merger_state_ == NEED_REBUILD || merger_state_ == NEED_SKIP_REBUILD) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new players has been push, please rebuild", K(ret), K(merger_state_));
  } else if (FALSE_IT(inner_merger_is_unique_champion = rows_merger_->is_unique_champion())) {
  } else if (merger_state_ == BASE_ITER_WIN && !base_item_.equal_with_next_) {
    merger_state_ = LOSER_TREE_WIN;
    base_item_.reset();
  } else if (OB_FAIL(rows_merger_->pop())) {
    STORAGE_LOG(WARN, "rows_merger_ pop failed", K(ret), KPC(rows_merger_));
  } else if (!inner_merger_is_unique_champion) {
    //rows_merger has same champion,merge state unchanged
  } else if (merger_state_ == BASE_ITER_WIN ) {
    //There is only one champion in the rows merger and is popped, so base_item_ is only champion now
    base_item_.equal_with_next_ = false;
  } else if (merger_state_ == NEED_SKIP) {
    merger_state_ = NEED_SKIP_REBUILD;
  } else if (base_item_.iter_ != nullptr) {
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
    if (OB_UNLIKELY(nullptr != base_item_.iter_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "base iter repeat push", K(ret), K(base_item_));
    } else {
      base_item_ = row;
    }
  } else if (OB_FAIL(rows_merger_->push(row))) {
    STORAGE_LOG(WARN, "rows_merger_ push failed", K(ret), KPC(rows_merger_));
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
    if (OB_FAIL(rows_merger_->push_top(row))) {
      STORAGE_LOG(WARN, "rows merger push top failed", K(ret), KPC(row.iter_));
    }
  } else {
    if (OB_UNLIKELY(nullptr != base_item_.iter_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "base iter repeat push", K(ret), K(*this));
    } else {
      base_item_ = row;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (base_item_.iter_ == nullptr ||
     (!row.iter_->is_base_iter() && merger_state_ == LOSER_TREE_WIN)) {
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
  } else if (OB_FAIL(rows_merger_->rebuild())) {
    STORAGE_LOG(WARN, "rows_merger_ rebuild failed", K(ret), KPC(rows_merger_));
  } else if (base_item_.iter_ == nullptr) {
    merger_state_ = LOSER_TREE_WIN;
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
  } else if (OB_FAIL(rows_merger_->open(total_player_cnt))) {
    STORAGE_LOG(WARN, "failed to open rows_merger", K(ret));
  } else {
    base_item_.reset();
    merger_state_ = LOSER_TREE_WIN;
  }

  return ret;
}

int ObPartitionMajorRowsMerger::count() const
{
  int count = 0;
  if (merger_state_ == NOT_INIT) {
  } else {
    count = base_item_.iter_ == nullptr ? rows_merger_->count() : rows_merger_->count() + 1;
  }
  return count;
}

bool ObPartitionMajorRowsMerger::empty() const
{
  return merger_state_ == NOT_INIT ? true : base_item_.iter_ == nullptr && rows_merger_->empty();
}

bool ObPartitionMajorRowsMerger::is_unique_champion() const
{
  bool is_unique_champion = true;
  if (merger_state_ == NOT_INIT) {
  } else {
    is_unique_champion = merger_state_ == BASE_ITER_WIN ? !base_item_.equal_with_next_ : rows_merger_->is_unique_champion();
  }
  return is_unique_champion;
}

int ObPartitionMajorRowsMerger::init_rows_merger(const int64_t total_player_cnt)
{
  int ret = OB_SUCCESS;
  if (total_player_cnt <= ObSimpleRowsPartitionMerger::USE_SIMPLE_MERGER_MAX_TABLE_CNT + 1) {
    if (OB_ISNULL(rows_merger_ = alloc_helper<ObSimpleRowsPartitionMerger>(*allocator_, cmp_))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc rows merger", K(ret));
    }
  } else if (OB_ISNULL(rows_merger_ = alloc_helper<ObPartitionMergeLoserTree>(*allocator_, cmp_))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc rows merger", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(rows_merger_->init(total_player_cnt, *allocator_))){
    STORAGE_LOG(WARN, "failed to init rows merger", K(ret), KPC(rows_merger_));
  }

  return ret;
}

int ObPartitionMajorRowsMerger::compare_base_iter()
{
  int ret = OB_SUCCESS;
  const ObPartitionMergeLoserTreeItem *item;
  if (OB_UNLIKELY(nullptr == base_item_.iter_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "base item not exist", K(ret));
  } else {
    bool finish = false;
    int64_t cmp_ret = 0;
    while (OB_SUCC(ret) && !finish) {
      bool is_purged = false;
      if (rows_merger_->empty()) {
        merger_state_ = BASE_ITER_WIN;
        base_item_.equal_with_next_ = false;
        finish = true;
      } else if (OB_FAIL(rows_merger_->top(item))) {
        STORAGE_LOG(WARN, "rows_merger_ top failed", K(ret), KPC(rows_merger_));
      } else if (OB_FAIL(cmp_.compare(base_item_, *item, cmp_ret))) {
        STORAGE_LOG(WARN, "fail to compare", K(ret), K(base_item_), KPC(item));
      } else if ((finish = cmp_.check_cmp_finish(cmp_ret))) {
        merger_state_ = cmp_ret <= 0 ? BASE_ITER_WIN : LOSER_TREE_WIN;
        base_item_.equal_with_next_ = (cmp_ret == 0);
      } else if (cmp_.need_open_right_macro(cmp_ret)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected right macro need open", K(ret), K(cmp_ret));
      } else if (cmp_.need_open_left_macro(cmp_ret)) {
        if (OB_FAIL(check_row_iters_purge(*item->iter_, is_purged))) {
          STORAGE_LOG(WARN, "Failed to check purge row iters", K(ret), KPC(item->iter_));
        } else if (is_purged) {
          merger_state_ = NEED_SKIP;
          break;
        } else if (OB_FAIL(base_item_.iter_->open_curr_range(false))) {
          STORAGE_LOG(WARN, "Failed to base iter open_curr_range", K(ret), KPC(base_item_.iter_));
        }
      }
    }
  }
  return ret;
}

int ObPartitionMajorRowsMerger::check_row_iters_purge(
    const ObPartitionMergeIter &check_iter,
    bool &can_purged)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *curr_row = nullptr;
  can_purged = false;

  if (OB_UNLIKELY(nullptr == base_item_.iter_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "base item not exist", K(ret));
  } else if (OB_ISNULL(curr_row = check_iter.get_curr_row())) {
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
    bool is_exist = false;
    if (OB_FAIL(base_item_.iter_->exist(curr_row, is_exist))) {
      STORAGE_LOG(WARN, "Failed to check if rowkey exist in base iter", K(ret));
    } else if (!is_exist) {
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
int ObPartitionMergeHelper::init(const ObIPartitionMergeFuser &fuser, const ObMergeParameter &merge_param, const ObRowStoreType row_store_type)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (!fuser.is_valid() || !merge_param.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid input arguments", K(ret), K(fuser), K(merge_param));
  } else if (OB_FAIL(init_merge_iters(fuser, merge_param, row_store_type))){
    STORAGE_LOG(WARN, "Failed to init merge iters", K(ret), K(fuser), K(merge_param));
  } else if (OB_FAIL(move_iters_next(merge_iters_))) {
    STORAGE_LOG(WARN, "failed to move iters", K(ret), K(merge_iters_));
  } else if (OB_FAIL(prepare_rows_merger())) {
    STORAGE_LOG(WARN, "failed to prepare rows merger", K(ret), K(*this));
  } else {
    consume_iter_idxs_.reset();
    is_inited_ = true;
  }

  return ret;
}

int ObPartitionMergeHelper::init_merge_iters(const ObIPartitionMergeFuser &fuser, const ObMergeParameter &merge_param, const ObRowStoreType row_store_type)
{
    int ret = OB_SUCCESS;
    merge_iters_.reset();
    int64_t table_cnt = merge_param.tables_handle_->get_count();
    ObITable *table = nullptr;
    ObSSTable *sstable = nullptr;
    ObPartitionMergeIter *merge_iter = nullptr;
    bool is_small_sstable = false;

    for (int64_t i = table_cnt - 1; OB_SUCC(ret) && i >= 0; i--) {
      if (OB_ISNULL(table = merge_param.tables_handle_->get_table(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null iter", K(ret), K(i), K(merge_param));
      } else if (OB_UNLIKELY(table->is_remote_logical_minor_sstable())) {
        ret = OB_EAGAIN;
        LOG_WARN("unexpected remote minor sstable, try later", K(ret), KP(sstable));
      } else if (table->is_sstable()) {
        sstable = static_cast<ObSSTable *>(table);
        if (sstable->get_data_macro_block_count() <= 0) {
          // do nothing. don't need to construct iter for empty sstable
          FLOG_INFO("table is empty, need not create iter", K(i), KPC(sstable));
          continue;
        } else {
          is_small_sstable = sstable->is_small_sstable();
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(merge_iter = alloc_merge_iter(merge_param, 0 == i, table->is_sstable()
          && is_small_sstable))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc memory for merge iter", K(ret), K(is_small_sstable));
      } else if (OB_FAIL(merge_iter->init(merge_param,
                                          fuser.get_multi_version_column_ids(),
                                          row_store_type, i))) {
        if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
          STORAGE_LOG(WARN, "Failed to init merge iter", K(ret));
        } else {
          FLOG_INFO("Ignore sstable beyond the range", K(i), K(merge_param.merge_range_), KPC(table));
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(merge_iters_.push_back(merge_iter))) {
        STORAGE_LOG(WARN, "Failed to push back merge iter", K(ret), KPC(merge_iter));
      } else {
        merge_iter = nullptr;
      }

      if (nullptr != merge_iter) {
        merge_iter->~ObPartitionMergeIter();
        merge_iter = nullptr;
      }
    }

    return ret;
}

int ObPartitionMergeHelper::prepare_rows_merger()
{
  int ret = OB_SUCCESS;

  if (merge_iters_.empty()) {
    FLOG_INFO("merge_iters is empty, skip build rows merger", K(merge_iters_));// skip build rows merger
  } else {
    void *buf = nullptr;
    int64_t iters_cnt = merge_iters_.count();
    ObPartitionMergeIter *merge_iter = nullptr;

    if (OB_ISNULL(merge_iter = merge_iters_.at(iters_cnt - 1))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null iter", K(ret));
    } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObPartitionMergeLoserTreeCmp)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to allocate memory", K(ret));
    } else if (FALSE_IT(cmp_ = new (buf) ObPartitionMergeLoserTreeCmp(merge_iter->get_read_info().get_datum_utils(),
                                                                      merge_iter->get_read_info().get_schema_rowkey_count()))) {
    } else if (merge_iter->is_major_sstable_iter() && merge_iter->is_macro_merge_iter()) {
      rows_merger_ = alloc_helper<ObPartitionMajorRowsMerger>(allocator_, *cmp_);
    } else if (iters_cnt <= ObSimpleRowsPartitionMerger::USE_SIMPLE_MERGER_MAX_TABLE_CNT) {
      rows_merger_ = alloc_helper<ObSimpleRowsPartitionMerger>(allocator_, *cmp_);
    } else {
      rows_merger_ = alloc_helper<ObPartitionMergeLoserTree>(allocator_, *cmp_);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(nullptr == rows_merger_)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc rows merger", K(ret));
    } else if (OB_FAIL(rows_merger_->init(iters_cnt, allocator_))) {
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
  } else if (rows_merger_->empty()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "rows_merger_ is empty", K(ret), KPC(rows_merger_));
  } else {
    minimum_iters.reset();
    consume_iter_idxs_.reset();
    bool has_same_rowkey = false;
    const ObPartitionMergeLoserTreeItem *top_item = nullptr;

    do {
      has_same_rowkey = !rows_merger_->is_unique_champion();
      if (OB_FAIL(rows_merger_->top(top_item))) {
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
          STORAGE_LOG(WARN, "failed to push item", K(ret), K(i), KPC(rows_merger_));
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
            "ghost_row_count", iter->get_ghost_row_count(), "pkey", table->get_key(), KPC(table));
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
  allocator_.reset();
  is_inited_ = false;
}

/**
 * ---------------------------------------------------------ObPartitionMajorMergeHelper--------------------------------------------------------------
 */

ObPartitionMergeIter *ObPartitionMajorMergeHelper::alloc_merge_iter(const ObMergeParameter &merge_param,
                                                                    const bool is_base_iter,
                                                                    const bool is_small_sstable)
{
  ObPartitionMergeIter *merge_iter = nullptr;
  if (is_base_iter && !is_small_sstable && !merge_param.is_full_merge_) {
    if (MICRO_BLOCK_MERGE_LEVEL == merge_param.merge_level_) {
      merge_iter = alloc_helper<ObPartitionMicroMergeIter>(allocator_);
    } else {
      merge_iter = alloc_helper<ObPartitionMacroMergeIter>(allocator_);
    }
  } else {
    merge_iter = alloc_helper<ObPartitionRowMergeIter>(allocator_);
  }

  return merge_iter;
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
    STORAGE_LOG(WARN, "get invalid argument", K(ret), K(merge_type));
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

ObPartitionMergeIter *ObPartitionMinorMergeHelper::alloc_merge_iter(const ObMergeParameter &merge_param,
                                                                    const bool is_base_iter,
                                                                    const bool is_small_sstable)
{
  UNUSED(is_base_iter);
  ObPartitionMergeIter *merge_iter = nullptr;
  if (storage::is_backfill_tx_merge(merge_param.merge_type_)) {
    merge_iter = alloc_helper<ObPartitionMinorRowMergeIter> (allocator_);
  } else if (!is_small_sstable && !is_mini_merge(merge_param.merge_type_) && !merge_param.is_full_merge_ && merge_param.sstable_logic_seq_ < ObMacroDataSeq::MAX_SSTABLE_SEQ) {
    merge_iter = alloc_helper<ObPartitionMinorMacroMergeIter>(allocator_);
  } else {
    merge_iter = alloc_helper<ObPartitionMinorRowMergeIter>(allocator_);
  }

  return merge_iter;
}

} //namespace compaction
} //namespace oceanbase
