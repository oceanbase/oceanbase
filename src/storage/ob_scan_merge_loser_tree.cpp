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
#include "ob_scan_merge_loser_tree.h"

namespace oceanbase {
using namespace common;
namespace storage {
void ObScanMergeLoserTreeCmp::reset()
{
  cmp_funcs_.reset();
  rowkey_size_ = 0;
  error_ = OB_SUCCESS;
  reverse_ = false;
  is_inited_ = false;
}

template <typename T>
int ObScanMergeLoserTreeCmp::make_rowkey_cmp_funcs(
    const int64_t rowkey_size, const ObColDescIArray& col_descs, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  T* cmp_funcs = NULL;
  cmp_funcs_.set_allocator(&allocator);
  cmp_funcs_.set_capacity(rowkey_size);
  if (OB_ISNULL(buf = allocator.alloc(sizeof(T) * rowkey_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for compare func", K_(rowkey_size), K(ret));
  } else {
    cmp_funcs = reinterpret_cast<T*>(new (buf) T[rowkey_size]);
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_size; i++) {
      if (OB_FAIL(cmp_funcs[i].init_compare_func(col_descs.at(i).col_type_))) {
        LOG_WARN("Failed to init compare func", K(i), K(ret));
      } else if (OB_FAIL(cmp_funcs_.push_back(&cmp_funcs[i]))) {
        LOG_WARN("Failed to push back compare func", K(i), K(ret));
      }
    }
  }
  return ret;
}

int ObScanMergeLoserTreeCmp::init(const int64_t rowkey_size, const ObColDescIArray& col_descs, const bool reverse,
    const bool is_oracle_mode, const bool use_cmp_nullsafe, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (rowkey_size <= 0 || col_descs.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(rowkey_size), K(col_descs));
  } else {
    if (is_oracle_mode && use_cmp_nullsafe) {
      ret = make_rowkey_cmp_funcs<ObRowkeyObjComparerNullsafeOracle>(rowkey_size, col_descs, allocator);
    } else if (is_oracle_mode && !use_cmp_nullsafe) {
      ret = make_rowkey_cmp_funcs<ObRowkeyObjComparerOracle>(rowkey_size, col_descs, allocator);
    } else if (!is_oracle_mode && use_cmp_nullsafe) {
      ret = make_rowkey_cmp_funcs<ObRowkeyObjComparerNullsafeMysql>(rowkey_size, col_descs, allocator);
    } else {
      make_rowkey_cmp_funcs<ObRowkeyObjComparer>(rowkey_size, col_descs, allocator);
    }
    if (OB_SUCC(ret)) {
      rowkey_size_ = rowkey_size;
      reverse_ = reverse;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObScanMergeLoserTreeCmp::compare_rowkey(const ObStoreRow& l_row, const ObStoreRow& r_row,
    const int64_t& rowkey_size, RowkeyCmpFuncArray& cmp_funcs, int32_t& cmp_result)
{
  int ret = OB_SUCCESS;
  if (!l_row.is_valid() || !r_row.is_valid() || rowkey_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(l_row), K(r_row), K(rowkey_size));
  } else {
    ObStoreRowkey l_key(l_row.row_val_.cells_, rowkey_size);
    ObStoreRowkey r_key(r_row.row_val_.cells_, rowkey_size);
    cmp_result = ObSSTableRowkeyHelper::compare(l_key, r_key, cmp_funcs);
  }
  return ret;
}

int64_t ObScanMergeLoserTreeCmp::operator()(const ObScanMergeLoserTreeItem& l, const ObScanMergeLoserTreeItem& r)
{
  int32_t cmp_result = 0;
  error_ = OB_SUCCESS;
  if (IS_NOT_INIT) {
    error_ = OB_NOT_INIT;
    LOG_WARN("not init", K(error_));
  } else if (nullptr == l.row_ || nullptr == r.row_ || l.row_->scan_index_ < 0 || r.row_->scan_index_ < 0) {
    error_ = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(error_), KP(l.row_), KP(r.row_));
  } else {
    cmp_result = static_cast<int32_t>(l.row_->scan_index_ - r.row_->scan_index_);
    if (0 == cmp_result) {
      if (OB_SUCCESS != (error_ = compare_rowkey(*l.row_, *r.row_, rowkey_size_, cmp_funcs_, cmp_result))) {
        LOG_WARN("compare rowkey error", K(error_));
      } else if (reverse_) {
        cmp_result = -cmp_result;
      }
    }
  }
  return cmp_result;
}

int ObScanMergeLoserTree::init(const int64_t total_player_cnt, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    if (OB_FAIL(ObScanMergeLoserTreeBase::init(total_player_cnt, allocator))) {
      LOG_WARN("init ObScanMergeLoserTreeBase fail", K(ret));
    } else {
      has_king_ = false;
      is_king_eq_champion_ = false;
    }
  }
  return ret;
}

void ObScanMergeLoserTree::reset()
{
  has_king_ = false;
  is_king_eq_champion_ = false;
  ObScanMergeLoserTreeBase::reset();
}

int ObScanMergeLoserTree::top(const ObScanMergeLoserTreeItem*& player)
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (has_king_) {
    player = &king_;
  } else if (OB_FAIL(ObScanMergeLoserTreeBase::top(player))) {
    LOG_WARN("get top from base tree fail", K(ret));
  }
  return ret;
}

int ObScanMergeLoserTree::pop()
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (need_rebuild_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new players has been push, please rebuild", K(ret));
  } else if (has_king_) {
    has_king_ = false;
    is_king_eq_champion_ = false;
  } else if (OB_FAIL(ObScanMergeLoserTreeBase::pop())) {
    LOG_WARN("pop base tree fail", K(ret));
  }
  return ret;
}

int ObScanMergeLoserTree::push(const ObScanMergeLoserTreeItem& player)
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (has_king_ && cur_free_cnt_ <= 1) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("player is full", K(ret), K(player_cnt_), K(cur_free_cnt_), K(has_king_));
  } else if (OB_FAIL(ObScanMergeLoserTreeBase::push(player))) {
    LOG_WARN("push base tree fail", K(ret));
  }
  return ret;
}

int ObScanMergeLoserTree::rebuild()
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!need_rebuild_) {
    // push_top will arrange the player automatically, so whatever has_king_, need_rebuild_
    // will tell us if the tree has new players
  } else if (empty()) {
    ret = OB_EMPTY_RESULT;
    LIB_LOG(WARN, "the tree is already empty", K(ret));
  } else {
    if (has_king_) {
      if (OB_FAIL(ObScanMergeLoserTreeBase::push(king_))) {
        LOG_WARN("fail to push king", K(ret));
      } else {
        has_king_ = false;
        is_king_eq_champion_ = false;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ObScanMergeLoserTreeBase::rebuild())) {
      LOG_WARN("build base tree fail", K(ret), K(has_king_), K(is_king_eq_champion_));
    }
  }
  return ret;
}

int ObScanMergeLoserTree::push_top(const ObScanMergeLoserTreeItem& player)
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (has_king_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("has old king", K(ret), K(has_king_));
  } else if (cur_free_cnt_ <= 0) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("player is full", K(ret), K(player_cnt_), K(cur_free_cnt_), K(has_king_));
  } else if (need_rebuild_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tree need rebuild", K(ret), K(need_rebuild_));
  } else if (0 == ObScanMergeLoserTreeBase::count()) {
    king_ = player;
    has_king_ = true;
    is_king_eq_champion_ = false;
  } else {
    const int64_t champion = matches_[0].winner_idx_;
    if (champion < 0 || champion > player_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid champion idx", K(ret), K(champion), K(player_cnt_), K(cur_free_cnt_));
    } else if (player.iter_idx_ == players_[champion].iter_idx_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rows from same iterator", K(ret), K(player.iter_idx_), K(players_[champion].iter_idx_));
    }

    // if left only one player, we can compare them by rebuild directly without trying
    // thus can save one time compare
    if (OB_SUCC(ret) && ObScanMergeLoserTreeBase::count() > 1) {
      const int64_t king_cmp = cmp_(players_[champion], player);
      if (OB_FAIL(cmp_.get_error_code())) {
        LOG_WARN("compare champion fail",
            K(ret),
            K(players_[champion].iter_idx_),
            K(player.iter_idx_),
            K(*players_[champion].row_),
            K(*player.row_));
      } else {
        if (king_cmp > 0) {
          king_ = player;
          has_king_ = true;
          is_king_eq_champion_ = false;
        } else if (0 == king_cmp && player.iter_idx_ < players_[champion].iter_idx_) {
          king_ = player;
          has_king_ = true;
          is_king_eq_champion_ = true;
        }
      }
    }

    if (OB_SUCC(ret) && !has_king_) {
      if (OB_FAIL(ObScanMergeLoserTreeBase::push(player))) {
        LOG_WARN("push player fail", K(ret));
      } else if (OB_FAIL(ObScanMergeLoserTreeBase::rebuild())) {
        LOG_WARN("build base tree fail", K(ret));
      } else {
        has_king_ = false;
        is_king_eq_champion_ = false;
      }
    }
  }
  return ret;
}

int ObScanMergeLoserTree::duel(ObScanMergeLoserTreeItem& offender, ObScanMergeLoserTreeItem& defender,
    const int64_t match_idx, bool& is_offender_win)
{
  int ret = OB_SUCCESS;
  if (match_idx < 0 || match_idx >= leaf_offset_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid match_idx", K(ret), K(match_idx));
  } else if (offender.iter_idx_ == defender.iter_idx_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rows from same iterator",
        K(ret),
        K(offender.iter_idx_),
        K(defender.iter_idx_),
        K(*offender.row_),
        K(*defender.row_));
  } else {
    int64_t cmp_ret = cmp_(offender, defender);
    if (OB_FAIL(cmp_.get_error_code())) {
      LOG_WARN(
          "compare fail", K(ret), K(offender.iter_idx_), K(defender.iter_idx_), K(*offender.row_), K(*defender.row_));
    } else {
      matches_[match_idx].is_draw_ = (0 == cmp_ret);
      if (0 == cmp_ret) {
        cmp_ret = (offender.iter_idx_ > defender.iter_idx_) ? 1 : -1;
      }
      is_offender_win = cmp_ret < 0;
    }
  }
  return ret;
}
}  // namespace storage
}  // namespace oceanbase
