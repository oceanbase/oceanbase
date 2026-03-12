/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SQL_DAS_SEARCH_OB_DISJUNCTIVE_DAAT_MERGER_H_
#define SQL_DAS_SEARCH_OB_DISJUNCTIVE_DAAT_MERGER_H_

#include "sql/das/search/ob_daat_merge_iter.h"
#include "sql/das/search/ob_das_search_define.h"

namespace oceanbase
{
namespace sql
{

struct ObDisjMergeItem
{
  ObDisjMergeItem() : relevance_(0.0), iter_idx_(-1), equal_with_next_(false) {}
  ~ObDisjMergeItem() = default;
  TO_STRING_KV(K_(iter_idx), K_(relevance), K_(equal_with_next));

  double relevance_;
  int64_t iter_idx_;
  bool equal_with_next_;
};

template <ObDASRowIDType RowIDType>
class ObDisjMergeCmp
{
public:
  ObDisjMergeCmp()
    : merge_iters_(nullptr), exprs_(nullptr), row_meta_(nullptr), is_inited_(false) {}
  virtual ~ObDisjMergeCmp() {}
  int init(
      const ObIArray<ObDAATMergeIter *> &merge_iters,
      const ObIArray<ObExpr *> &exprs,
      const RowMeta &row_meta);
  int cmp(const ObDisjMergeItem &l, const ObDisjMergeItem &r, int64_t &cmp_ret);
private:
  const ObIArray<ObDAATMergeIter *> *merge_iters_;
  const ObIArray<ObExpr *> *exprs_;
  const RowMeta *row_meta_;
  bool is_inited_;
};

template <ObDASRowIDType RowIDType>
int ObDisjMergeCmp<RowIDType>::init(
    const ObIArray<ObDAATMergeIter *> &merge_iters,
    const ObIArray<ObExpr *> &exprs,
    const RowMeta &row_meta)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    SQL_LOG(WARN, "cmp already inited", K(ret));
  } else {
    merge_iters_ = &merge_iters;
    exprs_ = &exprs;
    row_meta_ = &row_meta;
    is_inited_ = true;
  }
  return ret;
}

template <ObDASRowIDType RowIDType>
int ObDisjMergeCmp<RowIDType>::cmp(const ObDisjMergeItem &l, const ObDisjMergeItem &r, int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  ObDASRowID l_id;
  ObDASRowID r_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(merge_iters_->at(l.iter_idx_)->get_curr_id(l_id))) {
    SQL_LOG(WARN, "failed to get curr id", K(ret));
  } else if (OB_FAIL(merge_iters_->at(r.iter_idx_)->get_curr_id(r_id))) {
    SQL_LOG(WARN, "failed to get curr id", K(ret));
  } else {
    ObDASRowIDCmp<RowIDType> row_id_cmp(*row_meta_, *exprs_);
    int tmp_cmp_ret = 0;
    if (OB_FAIL(row_id_cmp.cmp(l_id, r_id, tmp_cmp_ret))) {
      SQL_LOG(WARN, "failed to compare rowid", K(ret));
    } else {
      cmp_ret = tmp_cmp_ret;
    }
  }
  return ret;
}

class ObIDisjunctiveDAATMerger
{
public:
  ObIDisjunctiveDAATMerger() {}
  virtual ~ObIDisjunctiveDAATMerger() {}
  virtual void reset() = 0;
  virtual int reuse() = 0;
  virtual int init(
      const ObIArray<ObDAATMergeIter *> &merge_iters,
      const ObIArray<ObExpr *> &id_exprs,
      const RowMeta &id_meta,
      const bool use_max_score,
      ObIAllocator &allocator) = 0;
  /*
   * Reuse the merger with selected iters.
   * @param selected_iter_idxes: The idxes array of the selected iters.
   * @param next_before_reuse: Whether to push the next round iters to the next row id before reuse.
   *                           We need this parameter to push iter to next id on demand, and avoid
   *                           repeated or missing document evaluation after reuse.
   * @param start: The start index of the selected iters in the idxes array.
   * @param end: The end index of the selected iters in the idxes array.
   * @return The result of the operation.
   */
  virtual int reuse_with_selected_iters(
      const ObIArray<int64_t> &selected_iter_idxes,
      const bool next_before_reuse,
      const int64_t start,
      const int64_t end) = 0;
  virtual int pop_all() = 0;
  virtual int rebuild_with_next() = 0;
  virtual int rebuild_with_advance(const ObDASRowID &target) = 0;
  virtual int rebuild_with_shallow(const ObDASRowID &target, const bool inclusive) = 0;
  virtual int merge_one_row(ObDASRowID &curr_id, double &score, int64_t &dim_cnt) = 0;
  virtual int pop_until_found_pivot(
      const double &score_threshold,
      const int64_t &min_dim_cnt,
      ObDASRowID &pivot_id,
      double &max_score) = 0;
  virtual int pop_until_target(const ObDASRowID &target) = 0;
  virtual int advance_for_block_max_score(const ObDASRowID &pivot_id, double &max_score) = 0;
  virtual int to_next_range_with_shallow_iters(const double &score_threshold) = 0;
  virtual int get_top_id(ObDASRowID &top_id) = 0;
  virtual bool is_empty() const = 0;

  static int alloc_merger(
      const ObIArray<ObDAATMergeIter *> &merge_iters,
      const ObIArray<ObExpr *> &id_exprs,
      const RowMeta &id_meta,
      const ObDASRowIDType &rowid_type,
      const bool use_max_score,
      ObIAllocator &allocator,
      ObIDisjunctiveDAATMerger *&merger);
  static constexpr int64_t SIMPLE_MERGE_MAX_DIM_CNT = 3;
};

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
class ObDisjunctiveDAATMerger : public ObIDisjunctiveDAATMerger
{
public:
  ObDisjunctiveDAATMerger();
  virtual ~ObDisjunctiveDAATMerger() { reset(); }
  virtual void reset() override;
  virtual int reuse() override;
  virtual int init(
      const ObIArray<ObDAATMergeIter *> &merge_iters,
      const ObIArray<ObExpr *> &id_exprs,
      const RowMeta &id_meta,
      const bool use_max_score,
      ObIAllocator &allocator) override;
  virtual int reuse_with_selected_iters(
      const ObIArray<int64_t> &selected_iter_idxes,
      const bool next_before_reuse,
      const int64_t start,
      const int64_t end) override;
  virtual int pop_all() override;
  virtual int rebuild_with_next() override;
  virtual int rebuild_with_advance(const ObDASRowID &target) override;
  virtual int rebuild_with_shallow(const ObDASRowID &target, const bool inclusive) override;
  virtual int merge_one_row(ObDASRowID &curr_id, double &score, int64_t &dim_cnt) override;
  virtual int pop_until_found_pivot(
      const double &score_threshold,
      const int64_t &min_dim_cnt,
      ObDASRowID &pivot_id,
      double &max_score) override;
  virtual int pop_until_target(const ObDASRowID &target) override;
  virtual int advance_for_block_max_score(const ObDASRowID &pivot_id, double &max_score) override;
  virtual int to_next_range_with_shallow_iters(const double &score_threshold) override;
  virtual int get_top_id(ObDASRowID &top_id) override;
  virtual bool is_empty() const override;
private:
  using MergerType = std::conditional_t<
      USE_SIMPLE_MERGER,
      ObSimpleRowsMerger<ObDisjMergeItem, ObDisjMergeCmp<RowIDType>>,
      ObMergeLoserTree<ObDisjMergeItem, ObDisjMergeCmp<RowIDType>>>;
  private:
  int pop();
  template <
      typename Func,
      typename... Args,
      typename = std::enable_if_t<std::is_invocable_v<Func&, Args..., ObDAATMergeIter &>>>
  int for_each_next_round_iter(Func &&func, Args &&...args);
  template <typename Func, typename... Args>
  int rebuild_with_func(Func &&func, Args &&...args);
  static int push_iter(MergerType &merger, ObDAATMergeIter &iter);
  static int next_and_push(MergerType &merger, ObDAATMergeIter &iter);
  static int advance_and_push(const ObDASRowID &target, MergerType &merger, ObDAATMergeIter &iter);
  static int advance_shallow_and_push(
      const ObDASRowID &target,
      const bool inclusive,
      MergerType &merger,
      ObDAATMergeIter &iter);
  int find_max_evaluated_id_with_shallow_iters(ObDASRowID &max_evaluated_id, bool &inclusive);
  int try_next_shallow_range(
      const double &score_threshold,
      bool &found_range,
      ObDASRowID &min_unevaluated_id,
      ObDASRowID &max_evaluated_id);
  int row_id_cmp(const ObDASRowID &l, const ObDASRowID &r, int &cmp_ret);
  void update_score(const double relevance, double &score);
private:
  MergerType merger_;
  ObDisjMergeCmp<RowIDType> merge_cmp_;
  const ObIArray<ObDAATMergeIter *> *merge_iters_;
  ObFixedArray<int32_t, ObIAllocator> next_round_iter_idxes_;
  int64_t next_round_cnt_;
  int64_t iter_end_cnt_;
  const ObIArray<ObExpr *> *exprs_;
  const RowMeta *row_meta_;
  bool use_max_score_;
  bool is_inited_;
};

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::ObDisjunctiveDAATMerger()
  : merger_(merge_cmp_),
    merge_cmp_(),
    merge_iters_(nullptr),
    next_round_iter_idxes_(),
    next_round_cnt_(0),
    iter_end_cnt_(0),
    is_inited_(false)
{}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
void ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::reset()
{
  next_round_iter_idxes_.reset();
  merger_.reset();
  is_inited_ = false;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::reuse()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    // do nothing
  } else {
    const int64_t dim_cnt = merge_iters_->count();
    for (int64_t i = 0; i < dim_cnt; ++i) {
      next_round_iter_idxes_[i] = i;
    }
    next_round_cnt_ = dim_cnt;
    merger_.reuse();
    if (OB_FAIL(merger_.open(dim_cnt))) {
      SQL_LOG(WARN, "failed to open merger", K(ret));
    }
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::init(
    const ObIArray<ObDAATMergeIter *> &merge_iters,
    const ObIArray<ObExpr *> &id_exprs,
    const RowMeta &id_meta,
    const bool use_max_score,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    SQL_LOG(WARN, "merger already inited", K(ret));
  } else if (OB_UNLIKELY(merge_iters.empty())) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "invalid args", K(ret), K(merge_iters));
  } else {
    const int64_t dim_cnt = merge_iters.count();
    next_round_iter_idxes_.set_allocator(&allocator);
    if (OB_FAIL(next_round_iter_idxes_.init(dim_cnt))) {
      SQL_LOG(WARN, "failed to init next round iter idxes", K(ret));
    } else if (OB_FAIL(next_round_iter_idxes_.prepare_allocate(dim_cnt))) {
      SQL_LOG(WARN, "failed to prepare allocate next round iter idxes", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < dim_cnt; ++i) {
        if (OB_ISNULL(merge_iters.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "unexpected nullptr to merge iter", K(ret), K(i));
        } else {
          next_round_iter_idxes_[i] = i;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(merge_cmp_.init(merge_iters, id_exprs, id_meta))) {
      SQL_LOG(WARN, "failed to init merge cmp", K(ret));
    } else if (OB_FAIL(merger_.init(dim_cnt, dim_cnt, allocator))) {
      SQL_LOG(WARN, "failed to init merger", K(ret));
    } else if (OB_FAIL(merger_.open(dim_cnt))) {
      SQL_LOG(WARN, "failed to open merger", K(ret));
    } else {
      next_round_cnt_ = dim_cnt;
      merge_iters_ = &merge_iters;
      exprs_ = &id_exprs;
      row_meta_ = &id_meta;
      use_max_score_ = use_max_score;
      is_inited_ = true;
    }
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::reuse_with_selected_iters(
    const ObIArray<int64_t> &selected_iter_idxes,
    const bool next_before_reuse,
    const int64_t start,
    const int64_t end)
{
  int ret = OB_SUCCESS;

  struct IterNextFunc
  {
    int operator()(ObDAATMergeIter &iter) const
    {
      int ret = iter.next();
      return OB_ITER_END == ret ? OB_SUCCESS : ret;
    }
  };

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not init", K(ret));
  } else if (next_before_reuse && OB_FAIL(for_each_next_round_iter(IterNextFunc()))) {
    SQL_LOG(WARN, "failed to push next round iters to next row id before rebuild", K(ret));
  } else if (OB_FAIL(pop_all())) {
    SQL_LOG(WARN, "failed to pop all", K(ret));
  } else {
    merger_.reuse();
    next_round_cnt_ = end - start + 1;
    for (int64_t i = start; i <= end; ++i) {
      const int64_t idx = i - start;
      next_round_iter_idxes_[idx] = selected_iter_idxes.at(i);
    }
    if (OB_FAIL(merger_.open(next_round_cnt_))) {
      SQL_LOG(WARN, "failed to open merger", K(ret), K_(next_round_cnt));
    } else if (OB_FAIL(rebuild_with_func(&ObDisjunctiveDAATMerger::push_iter))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild with push iter", K(ret));
    }
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::pop_all()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not init", K(ret));
  } else {
    merger_.reuse();
    next_round_cnt_ = 0;
    for (int64_t i = 0; i < merge_iters_->count(); ++i) {
      if (!merge_iters_->at(i)->iter_end()) {
        next_round_iter_idxes_[next_round_cnt_++] = i;
      }
    }
    if (0 == next_round_cnt_) {
      SQL_LOG(DEBUG, "all iters iter end and when pop all", KP_(merge_iters));
    } else if (OB_FAIL(merger_.open(next_round_cnt_))) {
      SQL_LOG(WARN, "failed to open merger", K(ret), K_(next_round_cnt));
    }
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::rebuild_with_next()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(rebuild_with_func(&ObDisjunctiveDAATMerger::next_and_push))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild with next", K(ret));
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::rebuild_with_advance(const ObDASRowID &target)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(rebuild_with_func(&ObDisjunctiveDAATMerger::advance_and_push, target))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild with advance", K(ret));
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::rebuild_with_shallow(
    const ObDASRowID &target,
    const bool inclusive)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(rebuild_with_func(&ObDisjunctiveDAATMerger::advance_shallow_and_push, target, inclusive))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild with shallow", K(ret));
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::pop()
{
  int ret = OB_SUCCESS;
  const ObDisjMergeItem *item = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(merger_.top(item))) {
    SQL_LOG(WARN, "failed to top", K(ret));
  } else {
    next_round_iter_idxes_[next_round_cnt_++] = item->iter_idx_;
    if (OB_FAIL(merger_.pop())) {
      SQL_LOG(WARN, "failed to pop", K(ret));
    }
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::get_top_id(ObDASRowID &top_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(merger_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "merger is empty", K(ret));
  } else {
    const ObDisjMergeItem *top_item = nullptr;
    ObDAATMergeIter *iter = nullptr;
    if (OB_FAIL(merger_.top(top_item))) {
      SQL_LOG(WARN, "failed to get top from merger", K(ret));
    } else if (OB_ISNULL(iter = merge_iters_->at(top_item->iter_idx_))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected null merge iter", K(ret), K(top_item->iter_idx_));
    } else if (OB_FAIL(iter->get_curr_id(top_id))) {
      SQL_LOG(WARN, "failed to get curr id from merge iter", K(ret), K(top_item->iter_idx_));
    }
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
bool ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::is_empty() const
{
  return merger_.empty();
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::merge_one_row(
    ObDASRowID &curr_id,
    double &score,
    int64_t &dim_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(next_round_cnt_ != 0)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "unexpected merge status for a full naive merge round", K(ret), K_(next_round_cnt));
  }

  const ObDisjMergeItem *top_item = nullptr;
  bool curr_row_end = false;
  score = 0.0;
  dim_cnt = 0;
  while (OB_SUCC(ret) && !merger_.empty() && !curr_row_end) {
    curr_row_end = merger_.is_unique_champion();
    if (OB_FAIL(merger_.top(top_item))) {
      SQL_LOG(WARN, "failed to top", K(ret));
    } else if (FALSE_IT(update_score(top_item->relevance_, score))) {
    } else if (curr_row_end && OB_FAIL(merge_iters_->at(top_item->iter_idx_)->get_curr_id(curr_id))) {
      SQL_LOG(WARN, "failed to get curr id", K(ret), K(top_item->iter_idx_));
    } else if (OB_FAIL(pop())) {
      SQL_LOG(WARN, "failed to pop", K(ret));
    } else {
      ++dim_cnt;
    }
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::pop_until_found_pivot(
    const double &score_threshold,
    const int64_t &min_dim_cnt,
    ObDASRowID &pivot_id,
    double &max_score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not init", K(ret));
  } else if (use_max_score_) {
    ret = OB_NOT_SUPPORTED;
  }

  bool finished = false;
  int64_t dim_cnt = 0;
  max_score = 0.0;
  while (OB_SUCC(ret) && !merger_.empty() && !finished) {
    const ObDisjMergeItem *top_item = nullptr;
    ObDAATMergeIter *iter = nullptr;
    const bool is_last_iter_id = merger_.is_unique_champion();
    double dim_max_score = 0.0;
    if (OB_FAIL(merger_.top(top_item))) {
      SQL_LOG(WARN, "failed to top", K(ret));
    } else if (OB_ISNULL(iter = merge_iters_->at(top_item->iter_idx_))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected nullptr iter", K(ret), K(top_item->iter_idx_));
    } else if (iter->iter_end()) {
      // skip
    } else if (OB_FAIL(iter->get_max_score(dim_max_score))) {
      SQL_LOG(WARN, "failed to call func", K(ret));
    } else if (OB_FAIL(pop())) {
      SQL_LOG(WARN, "failed to pop", K(ret));
    } else {
      ++dim_cnt;
      max_score += dim_max_score;
      if (is_last_iter_id && max_score > score_threshold && dim_cnt >= min_dim_cnt) {
        finished = true;
        if (OB_FAIL(iter->get_curr_id(pivot_id))) {
          SQL_LOG(WARN, "failed to get curr id", K(ret));
        }
      }
    }
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::pop_until_target(const ObDASRowID &target)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not init", K(ret));
  }
  bool found_target = false;
  while (OB_SUCC(ret) && !merger_.empty() && !found_target) {
    const ObDisjMergeItem *top_item = nullptr;
    ObDASRowID top_id;
    int cmp_ret = 0;
    if (OB_FAIL(merger_.top(top_item))) {
      SQL_LOG(WARN, "failed to top", K(ret));
    } else if (OB_FAIL(merge_iters_->at(top_item->iter_idx_)->get_curr_id(top_id))) {
      SQL_LOG(WARN, "failed to get curr id", K(ret), K(top_item->iter_idx_));
    } else if (OB_FAIL(row_id_cmp(top_id, target, cmp_ret))) {
      SQL_LOG(WARN, "failed to compare row id", K(ret));
    } else if (cmp_ret >= 0) {
      found_target = true;
    } else if (OB_FAIL(pop())) {
      SQL_LOG(WARN, "failed to pop", K(ret));
    }
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::advance_for_block_max_score(
    const ObDASRowID &pivot_id,
    double &max_score)
{
  int ret = OB_SUCCESS;

  struct AdvanceForBlockMaxScoreFunc
  {
    AdvanceForBlockMaxScoreFunc(const ObDASRowID &pivot_id, double &max_score)
      : pivot_id_(pivot_id), max_score_(max_score) {}
    int operator()(ObDAATMergeIter &iter) const
    {
      int ret = OB_SUCCESS;
      const MaxScoreTuple *max_score_tuple = nullptr;
      if (OB_FAIL(iter.advance_shallow(pivot_id_, true))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          SQL_LOG(WARN, "failed to advance shallow", K(ret));
        }
      } else if (OB_FAIL(iter.get_curr_block_max_info(max_score_tuple))) {
        SQL_LOG(WARN, "failed to get curr block max info", K(ret));
      } else {
        max_score_ += max_score_tuple->get_max_score();
      }
      return ret;
    }
    const ObDASRowID &pivot_id_;
    double &max_score_;
  };

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not init", K(ret));
  } else if (use_max_score_) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(for_each_next_round_iter(AdvanceForBlockMaxScoreFunc(pivot_id, max_score)))) {
    SQL_LOG(WARN, "failed to collect shallow max score", K(ret));
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::to_next_range_with_shallow_iters(
    const double &score_threshold)
{
  int ret = OB_SUCCESS;
  ObDASRowID max_evaluated_id;
  ObDASRowID min_unevaluated_id;
  bool inclusive = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SQL_LOG(WARN, "not init", K(ret));
  } else if (use_max_score_) {
    ret = OB_NOT_SUPPORTED;
  } else if (merger_.empty() && next_round_cnt_ == 0) {
      ret = OB_ITER_END;
  } else if (OB_FAIL(find_max_evaluated_id_with_shallow_iters(max_evaluated_id, inclusive))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      SQL_LOG(WARN, "failed to find max evaluated id with shallow iters", K(ret));
    }
  } else if (max_evaluated_id.is_max()) {
    ret = OB_ITER_END;
  }

  constexpr int64_t CHECK_INTERVAL = 100;
  int64_t loop_cnt = 0;
  ObDASRowID last_checked_id = max_evaluated_id;
  bool last_checked_inclusive = inclusive;

  bool found_range = false;
  while (OB_SUCC(ret) && !found_range) {
    SQL_LOG(DEBUG, "next pivot range", K(ret), K(found_range), K(max_evaluated_id), K(inclusive));
    if (OB_FAIL(rebuild_with_shallow(max_evaluated_id, inclusive))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild with shallow", K(ret));
    } else if (OB_FAIL(try_next_shallow_range(
        score_threshold, found_range, min_unevaluated_id, max_evaluated_id))) {
      SQL_LOG(WARN, "failed to try next shallow range", K(ret));
    } else if (max_evaluated_id.is_max()) {
      ret = OB_ITER_END;
    } else {
      inclusive = false;
    }

    // Progress check
    if (OB_SUCC(ret) && !found_range && !max_evaluated_id.is_max()) {
      ++loop_cnt;
      if (0 == loop_cnt % CHECK_INTERVAL) {
        int cmp_ret = 0;
        if (OB_FAIL(row_id_cmp(max_evaluated_id, last_checked_id, cmp_ret))) {
          SQL_LOG(WARN, "failed to compare row id for progress check", K(ret),
              K(max_evaluated_id), K(last_checked_id));
        } else if (cmp_ret < 0) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "pivot id moved backward unexpectedly", K(ret),
              K(loop_cnt), K(max_evaluated_id), K(last_checked_id));
        } else if (0 == cmp_ret && inclusive == last_checked_inclusive) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "pivot id stuck unexpectedly", K(ret),
              K(loop_cnt), K(max_evaluated_id), K(inclusive));
        } else {
          last_checked_id = max_evaluated_id;
          last_checked_inclusive = inclusive;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!found_range)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "unexpected no range is found", K(ret), K(found_range), K(max_evaluated_id), K(inclusive));
  } else if (OB_FAIL(rebuild_with_advance(min_unevaluated_id))) {
    SQL_LOG(WARN, "failed to rebuild with advance", K(ret));
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::find_max_evaluated_id_with_shallow_iters(
    ObDASRowID &max_evaluated_id,
    bool &inclusive)
{
  int ret = OB_SUCCESS;
  ObDASRowID minimum_max_id;
  max_evaluated_id.reset();
  inclusive = false;
  bool found_first_id = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < next_round_cnt_; ++i) {
    const int64_t iter_idx = next_round_iter_idxes_[i];
    const MaxScoreTuple *max_score_tuple = nullptr;
    ObDAATMergeIter *iter = nullptr;
    int cmp_ret = 0;
    if (OB_ISNULL(iter = merge_iters_->at(iter_idx))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected null iter", K(ret), K(iter_idx));
    } else if (iter->iter_end()) {
      // skip
    } else if (OB_UNLIKELY(!iter->in_shallow_status())) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected non-shallow iter", K(ret), KPC(iter), K(iter_idx));
    } else if (OB_FAIL(iter->get_curr_block_max_info(max_score_tuple))) {
      SQL_LOG(WARN, "failed to get block max info", K(ret), K(iter_idx));
    } else if (OB_ISNULL(max_score_tuple)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected null max score tuple", K(ret));
    } else if (!found_first_id) {
      minimum_max_id = max_score_tuple->get_max_id();
      found_first_id = true;
    } else if (OB_FAIL(row_id_cmp(max_score_tuple->get_max_id(), minimum_max_id, cmp_ret))) {
      SQL_LOG(WARN, "failed to compare domain id", K(ret));
    } else if (cmp_ret < 0) {
      minimum_max_id = max_score_tuple->get_max_id();
    }
  }

  if (OB_FAIL(ret)) {
  } else if (merger_.empty()) {
    if (!found_first_id) {
      ret = OB_ITER_END;
    } else {
      max_evaluated_id = minimum_max_id;
      inclusive = false;
    }
  } else {
    const ObDisjMergeItem *top_item = nullptr;
    int64_t iter_idx = 0;
    ObDASRowID next_id;
    int cmp_ret = 0;
    if (OB_FAIL(merger_.top(top_item))) {
      SQL_LOG(WARN, "failed to top", K(ret));
    } else if (FALSE_IT(iter_idx = top_item->iter_idx_)) {
    } else if (OB_FAIL(merge_iters_->at(iter_idx)->get_curr_id(next_id))) {
      SQL_LOG(WARN, "failed to get current id", K(ret), K(iter_idx));
    } else if (!found_first_id) {
      max_evaluated_id = next_id;
      inclusive = true;
    } else if (OB_FAIL(row_id_cmp(next_id, minimum_max_id, cmp_ret))) {
      SQL_LOG(WARN, "failed to compare row id", K(ret));
    } else if (cmp_ret <= 0) {
      max_evaluated_id = next_id;
      inclusive = true;
    } else {
      max_evaluated_id = minimum_max_id;
      inclusive = false;
    }
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::try_next_shallow_range(
    const double &score_threshold,
    bool &found_range,
    ObDASRowID &min_unevaluated_id,
    ObDASRowID &max_evaluated_id)
{
  int ret = OB_SUCCESS;
  found_range = false;
  min_unevaluated_id.reset();
  max_evaluated_id.reset();

  ObDASRowID minimum_max_id;
  bool found_first_max_id = false;
  bool curr_range_reach_end = false;
  double max_score = 0.0;
  while (OB_SUCC(ret) && max_score <= score_threshold && !curr_range_reach_end && !merger_.empty()) {
    const ObDisjMergeItem *top_item = nullptr;
    ObDAATMergeIter *iter = nullptr;
    int64_t iter_idx = 0;
    if (OB_FAIL(merger_.top(top_item))) {
      SQL_LOG(WARN, "failed to top", K(ret));
    } else if (FALSE_IT(iter_idx = top_item->iter_idx_)) {
    } else if (OB_ISNULL(iter = merge_iters_->at(iter_idx))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected null iter", K(ret));
    } else if (!iter->in_shallow_status() && OB_FAIL(iter->to_shallow())) {
      SQL_LOG(WARN, "failed switch iter to shallow status", K(ret), K(iter_idx));
    }

    const MaxScoreTuple *max_score_tuple = nullptr;
    int cmp_ret = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(iter->get_curr_block_max_info(max_score_tuple))) {
      SQL_LOG(WARN, "failed to get block max info", K(ret), K(iter_idx));
    } else if (OB_ISNULL(max_score_tuple)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected null max score tuple", K(ret));
    } else if (!found_first_max_id) {
      minimum_max_id = max_score_tuple->get_max_id();
      found_first_max_id = true;
    } else if (OB_FAIL(row_id_cmp(max_score_tuple->get_min_id(), minimum_max_id, cmp_ret))) {
      SQL_LOG(WARN, "failed to compare domain id", K(ret));
    } else if (cmp_ret > 0) {
      // no intersected range with this iter
      curr_range_reach_end = true;
    } else if (OB_FAIL(row_id_cmp(max_score_tuple->get_max_id(), minimum_max_id, cmp_ret))) {
      SQL_LOG(WARN, "failed to compare max domain id with minimum max domain id", K(ret));
    } else if (cmp_ret < 0) {
      minimum_max_id = max_score_tuple->get_max_id();
    }

    if (OB_FAIL(ret) || curr_range_reach_end) {
    } else if (OB_FAIL(pop())) {
      SQL_LOG(WARN, "failed to pop", K(ret));
    } else {
      max_score += max_score_tuple->get_max_score();
      if (max_score > score_threshold) {
        // found a new candidate range
        found_range = true;
        min_unevaluated_id = max_score_tuple->get_min_id();
      }
    }
  }

  if (OB_SUCC(ret) && !found_range) {
    max_evaluated_id = minimum_max_id;
  }
  return ret;
}




template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
template <typename Func, typename... Args, typename>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::for_each_next_round_iter(Func &&func, Args &&...args)
{
  int ret = OB_SUCCESS;
  // restrict args as lvalue reference to avoid unexpected forward repeatedly
  static_assert((std::is_lvalue_reference<Args&&>::value && ...), "Args must be lvalue reference");
  for (int64_t i = 0; OB_SUCC(ret) && i < next_round_cnt_; ++i) {
    const int64_t iter_idx = next_round_iter_idxes_.at(i);
    ObDAATMergeIter *iter = merge_iters_->at(iter_idx);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected nullptr iter", K(ret), K(iter_idx));
    } else if (iter->iter_end()) {
      // skip
    } else if (OB_FAIL(func(args..., *iter))) {
      SQL_LOG(WARN, "failed to call func", K(ret), KPC(iter));
    }
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
template <typename Func, typename... Args>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::rebuild_with_func(Func &&func, Args &&...args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(for_each_next_round_iter(func, args..., merger_))) {
    SQL_LOG(WARN, "failed to for each next round iter", K(ret));
  } else if (merger_.empty()) {
    ret = OB_ITER_END;
    next_round_cnt_ = 0;
  } else if (0 != next_round_cnt_ && OB_FAIL(merger_.rebuild())) {
    SQL_LOG(WARN, "failed to rebuild merger", K(ret));
  } else {
    next_round_cnt_ = 0;
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::push_iter(
    MergerType &merger,
    ObDAATMergeIter &iter)
{
  int ret = OB_SUCCESS;
  ObDisjMergeItem item;
  item.iter_idx_ = iter.get_iter_idx();
  if (iter.in_shallow_status()) {
    item.relevance_ = 0.0;
  } else if (OB_FAIL(iter.get_curr_score(item.relevance_))) {
    SQL_LOG(WARN, "failed to get curr score", K(ret));
  }
  if (FAILEDx(merger.push(item))) {
    SQL_LOG(WARN, "failed to push item", K(ret));
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::next_and_push(
    MergerType &merger,
    ObDAATMergeIter &iter)
{
  int ret = OB_SUCCESS;
  ObDisjMergeItem item;
  item.iter_idx_ = iter.get_iter_idx();
  if (OB_FAIL(iter.next())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      SQL_LOG(WARN, "failed to next iter", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(iter.get_curr_score(item.relevance_))) {
    SQL_LOG(WARN, "failed to get curr score", K(ret));
  } else if (OB_FAIL(merger.push(item))) {
    SQL_LOG(WARN, "failed to push item", K(ret));
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::advance_and_push(
    const ObDASRowID &target,
    MergerType &merger,
    ObDAATMergeIter &iter)
{
  int ret = OB_SUCCESS;
  ObDisjMergeItem item;
  item.iter_idx_ = iter.get_iter_idx();
  if (OB_FAIL(iter.advance_to(target))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      SQL_LOG(WARN, "failed to advance to target", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(iter.get_curr_score(item.relevance_))) {
    SQL_LOG(WARN, "failed to get curr score", K(ret));
  } else if (OB_FAIL(merger.push(item))) {
    SQL_LOG(WARN, "failed to push item", K(ret));
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::advance_shallow_and_push(
    const ObDASRowID &target,
    const bool inclusive,
    MergerType &merger,
    ObDAATMergeIter &iter)
{
  int ret = OB_SUCCESS;
  ObDisjMergeItem item;
  item.iter_idx_ = iter.get_iter_idx();
  if (OB_FAIL(iter.advance_shallow(target, inclusive))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      SQL_LOG(WARN, "failed to advance shallow", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (FALSE_IT(item.relevance_ = 0.0)) {
  } else if (OB_FAIL(merger.push(item))) {
    SQL_LOG(WARN, "failed to push item", K(ret));
  }
  return ret;
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
int ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::row_id_cmp(
    const ObDASRowID &l,
    const ObDASRowID &r,
    int &cmp_ret)
{
  ObDASRowIDCmp<RowIDType, true> row_id_cmp(*row_meta_, *exprs_);
  return row_id_cmp.cmp(l, r, cmp_ret);
}

template <ObDASRowIDType RowIDType, bool USE_SIMPLE_MERGER>
void ObDisjunctiveDAATMerger<RowIDType, USE_SIMPLE_MERGER>::update_score(
    const double relevance,
    double &score)
{
  if (use_max_score_) {
    score = MAX(score, relevance);
  } else {
    score += relevance;
  }
}

} // namespace sql
} // namespace oceanbase

#endif
