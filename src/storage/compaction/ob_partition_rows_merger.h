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
#ifndef OB_PARTITION_ROWS_MERGER_H_
#define OB_PARTITION_ROWS_MERGER_H_

#include "lib/container/ob_loser_tree.h"
#include "storage/access/ob_scan_merge_loser_tree.h"
#include "ob_partition_merge_iter.h"
#include "storage/access/ob_simple_rows_merger.h"
#include "storage/ob_i_store.h"
#include "ob_partition_merge_fuser.h"

namespace oceanbase
{

namespace storage
{
struct ObTransNodeDMLStat;
}

namespace compaction
{
struct ObPartitionMergeLoserTreeItem
{
  ObPartitionMergeIter *iter_;
  int64_t iter_idx_;
  bool equal_with_next_;
  ObPartitionMergeLoserTreeItem() : iter_(nullptr), iter_idx_(0), equal_with_next_(false)
  {}
  ~ObPartitionMergeLoserTreeItem() = default;
  void reset()
  {
    iter_ = nullptr;
    iter_idx_ = 0;
    equal_with_next_ = false;
  }
  bool is_valid() const { return iter_ != nullptr && iter_idx_ >= 0; }
  int get_curr_range(blocksstable::ObDatumRange &range) const
  {
    int ret = OB_SUCCESS;
    if (iter_ == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null iter", K(ret));
    } else {
      ret = iter_->get_curr_range(range);
    }
    return ret;
  }
  OB_INLINE bool is_range() const {return iter_ != nullptr && iter_->get_curr_row() == nullptr;}
  TO_STRING_KV(K_(iter_idx), KPC(iter_), K_(equal_with_next));
};

class ObPartitionMergeLoserTreeCmp final
{
public:
  ObPartitionMergeLoserTreeCmp(const blocksstable::ObStorageDatumUtils &datum_utils, const int64_t rowkey_size)
    : datum_utils_(datum_utils),
      rowkey_size_(rowkey_size)
  {}
  ~ObPartitionMergeLoserTreeCmp() = default;
  int cmp(const ObPartitionMergeLoserTreeItem &l, const ObPartitionMergeLoserTreeItem &r, int64_t &cmp_ret);
  int compare(const ObPartitionMergeLoserTreeItem &l,
              const ObPartitionMergeLoserTreeItem &r,
              int64_t &cmp_ret);
  bool check_cmp_finish(const int64_t cmp_ret) const;
  int open_iter_range(const int64_t cmp_ret,
                      const ObPartitionMergeLoserTreeItem &left,
                      const ObPartitionMergeLoserTreeItem &right);
  OB_INLINE bool need_open_left_macro(const int64_t cmp_ret) const { return  LEFT_MACRO_NEED_OPEN == cmp_ret || ALL_MACRO_NEED_OPEN == cmp_ret; }
  OB_INLINE bool need_open_right_macro(const int64_t cmp_ret) const { return  RIGHT_MACRO_NEED_OPEN == cmp_ret || ALL_MACRO_NEED_OPEN == cmp_ret; }
  int compare_range(const blocksstable::ObDatumRange &left_range,
                    const blocksstable::ObDatumRange &right_range,
                    int64_t &cmp_result) const;
  int compare_hybrid(const blocksstable::ObDatumRow &row,
                     const blocksstable::ObDatumRange &range,
                     int64_t &cmp_ret) const;
  int compare_rowkey(const ObDatumRow &l_row, const ObDatumRow &r_row, int64_t &cmp_result) const;
public:
  static const int64_t LEFT_MACRO_NEED_OPEN = INT64_MAX;
  static const int64_t RIGHT_MACRO_NEED_OPEN = -INT64_MAX;
  static const int64_t ALL_MACRO_NEED_OPEN = INT64_MAX - 1;

private:
  const blocksstable::ObStorageDatumUtils &datum_utils_;
  int64_t rowkey_size_;
};

typedef common::ObRowsMerger<ObPartitionMergeLoserTreeItem, ObPartitionMergeLoserTreeCmp> RowsMerger;
typedef common::ObSEArray<int64_t, DEFAULT_ITER_COUNT> CONSUME_ITER_IDX_ARRAY;
typedef ObMergeLoserTree<ObPartitionMergeLoserTreeItem, ObPartitionMergeLoserTreeCmp, common::MAX_TABLE_CNT_IN_STORAGE> ObPartitionMergeLoserTree;
typedef ObSimpleRowsMerger<ObPartitionMergeLoserTreeItem, ObPartitionMergeLoserTreeCmp> ObSimpleRowsPartitionMerger;
typedef common::ObRowsMerger<ObPartitionMergeLoserTreeItem, ObPartitionMergeLoserTreeCmp> RowsMerger;

/*
 * push:all_state -> need_rebuild
 * rebuild: NEED_REBUILD,NEED_PURGE_REBUILD -> BASE_ITER_WIN, LOSER_TREE_WIN, NEED_PURGE
 * push_top: NEED_PURGE_REBUILD, BASE_ITER_WIN, LOSER_TREE_WIN -> NEED_PURGE, BASE_ITER_WIN, LOSER_TREE_WIN
 * pop: NEED_PURGE -> NEED_PURGE, NEED_PURGE_REBUILD  || BASE_ITER_WIN, LOSER_TREE_WIN -> BASE_ITER_WIN, LOSER_TREE_WIN
 */
class ObPartitionMajorRowsMerger : public RowsMerger
{
private:
  enum MergerState{
    NOT_INIT,
    NEED_REBUILD,
    NEED_SKIP, //all champion in the rows merger need purge
    NEED_SKIP_REBUILD, //all champion in the rows merger are popped in the purge state
    BASE_ITER_WIN,
    LOSER_TREE_WIN
  };

public:
  ObPartitionMajorRowsMerger(ObPartitionMergeLoserTreeCmp &cmp)
    : cmp_(cmp),
      rows_merger_(nullptr),
      base_item_(),
      merger_state_(NOT_INIT),
      allocator_(nullptr)
  {}
  virtual ~ObPartitionMajorRowsMerger() { reset(); }
  virtual ObRowMergerType type() override { return common::ObRowMergerType::MAJOR_ROWS_MERGE; }
  virtual int init(const int64_t total_player_cnt, common::ObIAllocator &allocator) override;
  virtual int open(const int64_t total_player_cnt) override;
  virtual bool is_inited() const { return merger_state_ != MergerState::NOT_INIT; }
  virtual void reset() override;
  virtual void reuse() override;
  virtual int top(const ObPartitionMergeLoserTreeItem *&row) override;
  virtual int pop() override;
  virtual int push(const ObPartitionMergeLoserTreeItem &row) override;
  virtual int push_top(const ObPartitionMergeLoserTreeItem &row) override;
  virtual int rebuild() override;
  virtual int count() const override;
  virtual bool empty() const override;
  virtual bool is_unique_champion() const override;
  inline bool is_need_skip() const { return merger_state_ == NEED_SKIP || merger_state_ == NEED_SKIP_REBUILD; }
  TO_STRING_KV(K_(merger_state), K_(base_item), KPC(rows_merger_))
private:
  int init_rows_merger(const int64_t total_player_cnt);
  int compare_base_iter();
  int check_row_iters_purge(const ObPartitionMergeIter &check_iter, bool &can_purged);
private:
  ObPartitionMergeLoserTreeCmp &cmp_;
  RowsMerger *rows_merger_;
  ObPartitionMergeLoserTreeItem base_item_;
  MergerState merger_state_;
  common::ObIAllocator *allocator_;
};

class ObPartitionMergeHelper
{
public:
  ObPartitionMergeHelper(
      const ObITableReadInfo &read_info,
      common::ObIAllocator &allocator)
    : allocator_(allocator),
      read_info_(read_info),
      merge_iters_(DEFAULT_ITER_ARRAY_SIZE, ModulePageAllocator(allocator_)),
      consume_iter_idxs_(DEFAULT_ITER_COUNT * sizeof(int64_t), ModulePageAllocator(allocator_)),
      rows_merger_(nullptr),
      cmp_(nullptr),
      is_inited_(false)
  {}
  virtual ~ObPartitionMergeHelper() { reset(); }
  int init(const ObMergeParameter &merge_param);
  virtual void reset();
  virtual OB_INLINE bool is_co_major_helper() const { return false; }
  int find_rowkey_minimum_iters(MERGE_ITER_ARRAY &minimum_iters);
  static int move_iters_next(MERGE_ITER_ARRAY &merge_iters);
  int rebuild_rows_merger();
  int has_incremental_data(bool &has_incremental_data) const;
  bool is_need_skip() const;
  int check_iter_end() const;
  int64_t get_iters_row_count() const;
  OB_INLINE const MERGE_ITER_ARRAY& get_merge_iters() const { return merge_iters_; }
  OB_INLINE bool is_iter_end() const { return merge_iters_.empty() || (nullptr != rows_merger_ && rows_merger_->empty() && consume_iter_idxs_.empty()); }
  TO_STRING_KV(K_(is_inited), K_(merge_iters), K_(consume_iter_idxs), KPC(rows_merger_))
protected:
  virtual ObPartitionMergeIter *alloc_merge_iter(const ObMergeParameter &merge_param, const bool is_base_iter, const bool is_small_sstable, const ObITable *table) = 0;
private:
  int init_merge_iters(const ObMergeParameter &merge_param);
  int prepare_rows_merger();
  int build_rows_merger();

protected:
  common::ObIAllocator &allocator_;
  const ObITableReadInfo &read_info_;
  MERGE_ITER_ARRAY merge_iters_;
  CONSUME_ITER_IDX_ARRAY consume_iter_idxs_;
  RowsMerger *rows_merger_;
  ObPartitionMergeLoserTreeCmp *cmp_;
  bool is_inited_;
};

class ObPartitionMajorMergeHelper : public ObPartitionMergeHelper
{
public:
  ObPartitionMajorMergeHelper(
      const ObITableReadInfo &read_info,
      common::ObIAllocator &allocator)
    : ObPartitionMergeHelper(read_info, allocator)
  {}
  virtual ~ObPartitionMajorMergeHelper() { reset(); }
protected:
  ObPartitionMergeIter *alloc_merge_iter(const ObMergeParameter &merge_param, const bool is_base_iter, const bool is_small_sstable, const ObITable *table) override;
};

class ObPartitionMinorMergeHelper : public ObPartitionMergeHelper
{
public:
  ObPartitionMinorMergeHelper(
      const ObITableReadInfo &read_info,
      common::ObIAllocator &allocator)
    : ObPartitionMergeHelper(read_info, allocator)
  {}
  virtual ~ObPartitionMinorMergeHelper() { reset(); }
  int collect_tnode_dml_stat(
      const ObMergeType &merge_type,
      storage::ObTransNodeDMLStat &tnode_stat) const;
protected:
  ObPartitionMergeIter *alloc_merge_iter(const ObMergeParameter &merge_param, const bool is_base_iter, const bool is_small_sstable, const ObITable *table) override;
};

} //namespace compaction
} //namespace oceanbase
#endif
