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

#ifndef OB_SCAN_MERGE_LOSER_TREE_H_
#define OB_SCAN_MERGE_LOSER_TREE_H_

#include "lib/container/ob_loser_tree.h"
#include "storage/ob_i_store.h"
#include "storage/ob_sstable_rowkey_helper.h"

namespace oceanbase {
namespace storage {
struct ObScanMergeLoserTreeItem {
  const ObStoreRow* row_;
  int64_t iter_idx_;
  uint8_t iter_flag_;
  ObScanMergeLoserTreeItem() : row_(NULL), iter_idx_(0), iter_flag_(0)
  {}
  ~ObScanMergeLoserTreeItem() = default;
  void reset()
  {
    row_ = NULL;
    iter_idx_ = 0;
    iter_flag_ = 0;
  }
  TO_STRING_KV(K_(iter_idx), K_(iter_flag), KPC(row_));
};

class ObScanMergeLoserTreeCmp {
public:
  typedef common::ObFixedArray<ObRowkeyObjComparer*, common::ObIAllocator> RowkeyCmpFuncArray;
  ObScanMergeLoserTreeCmp()
      : cmp_funcs_(), rowkey_size_(0), error_(common::OB_SUCCESS), reverse_(false), is_inited_(false)
  {}
  ~ObScanMergeLoserTreeCmp() = default;
  void reset();
  int init(const int64_t rowkey_size, const ObColDescIArray& col_descs, const bool reverse, const bool is_oracle_mode,
      const bool use_cmp_nullsafe, ObIAllocator& allocator);
  int64_t operator()(const ObScanMergeLoserTreeItem& l, const ObScanMergeLoserTreeItem& r);
  OB_INLINE int get_error_code() const
  {
    return error_;
  }
  static int compare_rowkey(const ObStoreRow& l_row, const ObStoreRow& r_row, const int64_t& rowkey_size,
      RowkeyCmpFuncArray& cmp_funcs, int32_t& cmp_result);

private:
  template <typename T>
  int make_rowkey_cmp_funcs(const int64_t rowkey_size, const ObColDescIArray& col_descs, ObIAllocator& allocator);

  RowkeyCmpFuncArray cmp_funcs_;
  int64_t rowkey_size_;
  int error_;
  bool reverse_;
  bool is_inited_;
};

typedef common::ObLoserTree<ObScanMergeLoserTreeItem, ObScanMergeLoserTreeCmp, common::MAX_TABLE_CNT_IN_STORAGE>
    ObScanMergeLoserTreeBase;
class ObScanMergeLoserTree : public ObScanMergeLoserTreeBase {
public:
  ObScanMergeLoserTree(ObScanMergeLoserTreeCmp& cmp)
      : ObScanMergeLoserTreeBase(cmp), has_king_(false), is_king_eq_champion_(false), king_()
  {}
  virtual ~ObScanMergeLoserTree() = default;

  virtual int init(const int64_t total_player_cnt, common::ObIAllocator& allocator) override;
  virtual void reset() override;

  virtual int top(const ObScanMergeLoserTreeItem*& player) override;
  virtual int pop() override;
  virtual int push(const ObScanMergeLoserTreeItem& player) override;
  virtual int rebuild() override;

  // directly replace the champion with the player,
  // will trigger rebuild automatically if the player can't be king.
  // for performance and simplicity, caller should ensure there's no old king here
  virtual int push_top(const ObScanMergeLoserTreeItem& player);

  virtual OB_INLINE int count() const override
  {
    const int64_t tree_cnt = ObScanMergeLoserTreeBase::count();
    return has_king_ ? tree_cnt + 1 : tree_cnt;
  }
  virtual OB_INLINE bool empty() const override
  {
    return !has_king_ && ObScanMergeLoserTreeBase::empty();
  }
  virtual OB_INLINE bool is_unique_champion() const override
  {
    return has_king_ ? !is_king_eq_champion_ : is_unique_champion_;
  }

protected:
  virtual int duel(ObScanMergeLoserTreeItem& offender, ObScanMergeLoserTreeItem& defender, const int64_t match_idx,
      bool& is_offender_win) override;

private:
  // optimization for only the top item get pop. Usually, the next row from same iter will still be
  // the max/min row. So it can be cached in king_ without rebuilding the whole tree
  bool has_king_;
  bool is_king_eq_champion_;
  ObScanMergeLoserTreeItem king_;
};
}  // namespace storage
}  // namespace oceanbase
#endif /* OB_SCAN_MERGE_LOSER_TREE_H_ */
