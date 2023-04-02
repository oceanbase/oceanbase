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

namespace oceanbase
{
namespace storage
{
struct ObScanMergeLoserTreeItem
{
  const blocksstable::ObDatumRow *row_;
  int64_t iter_idx_;
  uint8_t iter_flag_;
  bool equal_with_next_;
  ObScanMergeLoserTreeItem() : row_(NULL), iter_idx_(0), iter_flag_(0), equal_with_next_(false)
  {}
  ~ObScanMergeLoserTreeItem() = default;
  void reset()
  {
    row_ = NULL;
    iter_idx_ = 0;
    iter_flag_ = 0;
    equal_with_next_ = false;
  }
  TO_STRING_KV(K_(iter_idx), K_(iter_flag), KPC(row_));
};

class ObScanMergeLoserTreeCmp
{
public:
  ObScanMergeLoserTreeCmp() :
    datum_utils_(nullptr),
    rowkey_size_(0),
    reverse_(false),
    is_inited_(false)
  {}
  ~ObScanMergeLoserTreeCmp() = default;
  void reset();
  int init(const int64_t rowkey_size,
           const blocksstable::ObStorageDatumUtils &datum_utils,
           const bool reverse);
  int cmp(const ObScanMergeLoserTreeItem &l, const ObScanMergeLoserTreeItem &r, int64_t &cmp_ret);
  int compare_rowkey(const blocksstable::ObDatumRow &l_row,
                     const blocksstable::ObDatumRow &r_row,
                     int64_t &cmp_result);
private:
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  int64_t rowkey_size_;
  bool reverse_;
  bool is_inited_;
};

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
class ObMergeLoserTree : public ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>
{
public:
  ObMergeLoserTree(CompareFunctor &cmp)
      : ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>(cmp),
        has_king_(false),
        is_king_eq_champion_(false),
        king_()
  {
  }
  virtual ~ObMergeLoserTree() { reset(); }
  virtual int init(const int64_t total_player_cnt, common::ObIAllocator &allocator) override;
  virtual int open(const int64_t total_player_cnt) override;
  virtual void reset() override;
  virtual void reuse() override;
  virtual int top(const T *&player) override;
  virtual int pop() override;
  virtual int push(const T &player) override;
  virtual int rebuild() override;

  // directly replace the champion with the player,
  // will trigger rebuild automatically if the player can't be king.
  // for performance and simplicity, caller should ensure there's no old king here
  virtual int push_top(const T &player) override;
  virtual OB_INLINE int count() const override
  {
    const int64_t tree_cnt = ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::count();
    return has_king_ ? tree_cnt + 1 : tree_cnt;
  }
  virtual OB_INLINE bool empty() const override
  {
    return !has_king_ && ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::empty();
  }
  virtual OB_INLINE bool is_unique_champion() const override
  {
    return has_king_ ? !is_king_eq_champion_ : this->is_unique_champion_;
  }

  typedef ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT> LoserTree;

protected:
  virtual int duel(
      T &offender,
      T &defender,
      const int64_t match_idx,
      bool &is_offender_win) override;

private:
  // optimization for only the top item get pop. Usually, the next row from same iter will still be
  // the max/min row. So it can be cached in king_ without rebuilding the whole tree
  bool has_king_;
  bool is_king_eq_champion_;
  T king_;
};

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObMergeLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::init(const int64_t total_player_cnt,
                                                              ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (this->is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {
    if (OB_FAIL(LoserTree::init(total_player_cnt, allocator))) {
      STORAGE_LOG(WARN, "init ObLoserTree init fail", K(ret));
    } else {
      has_king_ = false;
      is_king_eq_champion_ = false;
    }
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObMergeLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::open(const int64_t total_player_cnt)
{
  int ret = OB_SUCCESS;
  if (!this->is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(LoserTree::open(total_player_cnt))) {
    STORAGE_LOG(WARN, "open LoserTree fail", K(ret));
  } else {
    has_king_ = false;
    is_king_eq_champion_ = false;
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
void ObMergeLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::reuse()
{
  has_king_ = false;
  is_king_eq_champion_ = false;
  LoserTree::reuse();
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
void ObMergeLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::reset()
{
  has_king_ = false;
  is_king_eq_champion_ = false;
  LoserTree::reset();
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObMergeLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::top(const T *&player)
{
  int ret = OB_SUCCESS;
  if (!this->is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (has_king_) {
    player = &king_;
  } else if (OB_FAIL(LoserTree::top(player))) {
    STORAGE_LOG(WARN, "get top from base tree fail", K(ret));
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObMergeLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::pop()
{
  int ret = OB_SUCCESS;
  if (!this->is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (this->need_rebuild_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "new players has been push, please rebuild", K(ret));
  } else if (has_king_) {
    has_king_ = false;
    is_king_eq_champion_ = false;
  } else if (OB_FAIL(LoserTree::pop())) {
    STORAGE_LOG(WARN, "pop base tree fail", K(ret));
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObMergeLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::push(const T &player)
{
  int ret = OB_SUCCESS;
  if (!this->is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (has_king_ && this->cur_free_cnt_ <= 1) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "player is full", K(ret), K(has_king_));
  } else if (OB_FAIL(LoserTree::push(player))) {
    STORAGE_LOG(WARN, "push base tree fail", K(ret));
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObMergeLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::rebuild()
{
  int ret = OB_SUCCESS;
  if (!this->is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (!this->need_rebuild_) {
    // push_top will arrange the player automatically, so whatever has_king_, this->need_rebuild_
    // will tell us if the tree has new players
  } else if (empty()) {
    ret = OB_EMPTY_RESULT;
    STORAGE_LOG(WARN, "the tree is already empty", K(ret));
  } else {
    if (has_king_) {
      if (OB_FAIL(LoserTree::push(king_))) {
        STORAGE_LOG(WARN, "fail to push king", K(ret));
      } else {
        has_king_ = false;
        is_king_eq_champion_ = false;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(LoserTree::rebuild())) {
      STORAGE_LOG(WARN, "build base tree fail", K(ret), K(has_king_), K(is_king_eq_champion_));
    }
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObMergeLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::push_top(const T &player)
{
  int ret = OB_SUCCESS;
  if (!this->is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (has_king_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "has old king", K(ret), K(has_king_));
  } else if (this->cur_free_cnt_ <= 0) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "player is full", K(ret), K(this->player_cnt_), K(this->cur_free_cnt_), K(has_king_));
  } else if (this->need_rebuild_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tree need rebuild", K(ret), K(this->need_rebuild_));
  } else if (0 == LoserTree::count()) {
    king_ = player;
    has_king_ = true;
    is_king_eq_champion_ = false;
  } else {
    const int64_t champion = this->matches_[0].winner_idx_;
    if (champion < 0 || champion > this->player_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid champion idx", K(ret), K(champion), K(this->player_cnt_),
              K(this->cur_free_cnt_));
    }
    // if left only one player, we can compare them by rebuild directly without trying
    // thus can save one time compare
    if (OB_SUCC(ret) && LoserTree::count() > 1) {
      int64_t king_cmp = 0;
      if (OB_FAIL(this->cmp_.cmp(this->players_[champion], player, king_cmp))) {
        STORAGE_LOG(WARN, "compare champion fail", K(ret));
      } else {
        if (king_cmp > 0) {
          king_ = player;
          has_king_ = true;
          is_king_eq_champion_ = false;
        } else if (0 == king_cmp && player.iter_idx_ < this->players_[champion].iter_idx_) {
          king_ = player;
          has_king_ = true;
          is_king_eq_champion_ = true;
        }
      }
    }

    if (OB_SUCC(ret) && !has_king_) {
      if (OB_FAIL(LoserTree::push(player))) {
        STORAGE_LOG(WARN, "push player fail", K(ret));
      } else if (OB_FAIL(LoserTree::rebuild())) {
        STORAGE_LOG(WARN, "build base tree fail", K(ret));
      } else {
        has_king_ = false;
        is_king_eq_champion_ = false;
      }
    }
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObMergeLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::duel(
    T &offender,
    T &defender,
    const int64_t match_idx,
    bool &is_offender_win)
{
  int ret = OB_SUCCESS;
  if (match_idx < 0 || match_idx >= this->leaf_offset_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid match_idx", K(ret), K(match_idx));
  } else if (offender.iter_idx_ == defender.iter_idx_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "rows from same iterator", K(ret), K(offender), K(defender));
  } else {
    int64_t cmp_ret = 0;
    if (OB_FAIL(this->cmp_.cmp(offender, defender, cmp_ret))) {
      STORAGE_LOG(WARN, "compare fail", K(ret), K(offender), K(defender));
    } else {
      this->matches_[match_idx].is_draw_ = (0 == cmp_ret);
      if (0 == cmp_ret) {
        cmp_ret = (offender.iter_idx_ > defender.iter_idx_) ? 1 : -1;
      }
      is_offender_win = cmp_ret < 0;
    }
  }
  return ret;
}

typedef ObMergeLoserTree<ObScanMergeLoserTreeItem, ObScanMergeLoserTreeCmp, common::MAX_TABLE_CNT_IN_STORAGE> ObScanMergeLoserTree;
} //namespace storage
} //namespace oceanbase
#endif /* OB_SCAN_MERGE_LOSER_TREE_H_ */
