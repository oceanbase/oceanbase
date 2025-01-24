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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/join/ob_merge_join_vec_op.h"

namespace oceanbase {
using namespace common;
namespace sql {
static const int64_t BATCH_MULTIPLE_FACTOR = 10;
#define VEC_FORMAT_SWITCH_CASE_FOR_OUTPUT(VEC_FORMAT, vec_ptr, row_meta,       \
                                          stored_rows, size, col_idx)          \
  case VEC_FORMAT: {                                                           \
    if (OB_FAIL(vec_ptr->from_rows(                                            \
            row_meta,                                                          \
            const_cast<const ObCompactRow **>(                                 \
                reinterpret_cast<ObCompactRow **>(stored_rows)),               \
            size, col_idx))) {                                                 \
      LOG_WARN("stored row copy to vector faile", K(ret), K(VEC_FORMAT),       \
               K(size), K(col_idx));                                           \
    } else {                                                                   \
      all_exprs_->at(expr_idx)->set_evaluated_projected(eval_ctx_);            \
    }                                                                          \
    break;                                                                     \
  }

OB_SERIALIZE_MEMBER((ObMergeJoinVecSpec, ObJoinVecSpec),
                    equal_cond_infos_,
                    merge_directions_,
                    left_child_fetcher_all_exprs_,
                    right_child_fetcher_all_exprs_,
                    left_child_fetcher_equal_keys_,
                    right_child_fetcher_equal_keys_,
                    left_child_fetcher_equal_keys_idx_,
                    right_child_fetcher_equal_keys_idx_);

OB_SERIALIZE_MEMBER(ObMergeJoinVecSpec::EqualConditionInfo, expr_, ser_eval_func_, is_opposite_);
const int64_t ObMergeJoinVecSpec::MERGE_DIRECTION_ASC = 1;
const int64_t ObMergeJoinVecSpec::MERGE_DIRECTION_DESC = -1;


int ObMergeJoinVecOp::ObCommonJoinTracker::init(int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  row_id_array_size_ = max_batch_size;
  if (OB_ISNULL(left_row_id_array_ = static_cast<int64_t *>(allocator_->alloc(
                        sizeof(int64_t) * row_id_array_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_ISNULL(right_row_id_array_ = static_cast<int64_t *>(allocator_->alloc(
                        sizeof(int64_t) * row_id_array_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_ISNULL(group_boundary_row_id_array_ = static_cast<int64_t *>(allocator_->alloc(
                        sizeof(int64_t) * (row_id_array_size_ + 1))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  }
  return ret;
}

int ObMergeJoinVecOp::ObCommonJoinTracker::fill_match_pair(int64_t max_pair_cnt, ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  int64_t filled_row_cnt = 0;
  brs.reset_skip(max_pair_cnt);
  if (need_trace_) {
    group_boundary_row_id_array_idx_ = 0;
    trace_group_idx_ = cur_group_idx_;
    if (OB_FAIL(set_group_end_row_id(cur_left_group_->end_))) {
      LOG_WARN("set group end row id failed", K(ret));
    }
  }
  while (OB_SUCC(ret) && filled_row_cnt < max_pair_cnt) {
    if (cur_left_group_->is_empty() && !cur_right_group_->iter_end()) {
      int64_t row_cnt =
          std::min(max_pair_cnt - filled_row_cnt,
                   cur_right_group_->end_ - cur_right_group_->cur_);
      if (!need_trace_ && OB_FAIL(right_match_cursor_->flat_group(
              cur_right_group_->cur_, row_cnt,
              filled_row_cnt, right_row_id_array_))) {
        LOG_WARN("right_match_cursor_ scan store rows failed", K(ret),
                 K(row_cnt));
      } else if (need_trace_ &&
                 OB_FALSE_IT(
                     right_match_cursor_->flat_group_with_null_as_placeholder(
                         filled_row_cnt, row_cnt, right_row_id_array_,
                         cur_right_group_->cur_, brs))) {
      } else {
        left_match_cursor_->fill_null_row_ptr(filled_row_cnt, row_cnt, left_row_id_array_);
        cur_right_group_->cur_ += row_cnt;
        filled_row_cnt += row_cnt;
      }
    } else if (cur_right_group_->is_empty() && !cur_left_group_->iter_end()) {
      int64_t row_cnt = std::min(max_pair_cnt - filled_row_cnt,
                                 cur_left_group_->end_ - cur_left_group_->cur_);
      if (!need_trace_ && OB_FAIL(left_match_cursor_->flat_group(
              cur_left_group_->cur_, row_cnt,
              filled_row_cnt, left_row_id_array_))) {
        LOG_WARN("right_match_cursor_ scan store rows failed", K(ret),
                 K(row_cnt));
      } else if (need_trace_ &&
                 OB_FALSE_IT(
                     left_match_cursor_->flat_group_with_null_as_placeholder(
                         filled_row_cnt, row_cnt, left_row_id_array_,
                         cur_left_group_->cur_, brs))) {
      } else {
        right_match_cursor_->fill_null_row_ptr(filled_row_cnt, row_cnt, right_row_id_array_);
        cur_left_group_->cur_ += row_cnt;
        filled_row_cnt += row_cnt;
      }
    } else if (cur_left_group_->iter_end()) {
      if (cur_group_idx_ < group_pairs_.count() - 1) {
        next_group_pair();
        if (need_trace_) {
          if (OB_FAIL(set_group_end_row_id(cur_left_group_->end_))) {
            LOG_WARN("set group end row id failed", K(ret));
          }
        }
      } else {
        break;
      }
    } else if (cur_right_group_->iter_end()) {
      if (++cur_left_group_->cur_ < cur_left_group_->end_) {
        cur_right_group_->rescan();
      }
    } else  {
      int64_t row_cnt =
          std::min(max_pair_cnt - filled_row_cnt,
                   cur_right_group_->end_ - cur_right_group_->cur_);
      if (OB_FAIL(right_match_cursor_->flat_group(
              cur_right_group_->cur_, row_cnt,
              filled_row_cnt, right_row_id_array_))) {
        LOG_WARN("right_match_cursor_ scan store rows failed", K(ret),
                 K(row_cnt));
      } else if (OB_FAIL(left_match_cursor_->duplicate_store_row_ptr(
                     cur_left_group_->cur_, filled_row_cnt, row_cnt,
                     left_row_id_array_))) {
        LOG_WARN("left_match_cursor_ duplicate cur store row failed", K(ret),
                 K(row_cnt));
      } else {
        filled_row_cnt += row_cnt;
        cur_right_group_->cur_ += row_cnt;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (need_trace_ && filled_row_cnt == 0) {
    if (!last_left_row_matched_ && last_left_row_id_ != -1) {
      if (OB_FAIL(output_cache_.push_back(RowPair(last_left_row_id_, -1)))) {
          LOG_WARN("push back group row to output_cache_ failed", K(ret));
      } else {
        last_left_row_matched_ = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (join_type_ == FULL_OUTER_JOIN && right_match_all_output_group_idx_ < trace_group_idx_) {
      RowGroup &right_group = group_pairs_.at(trace_group_idx_).second;
      for (int64_t i = 0; OB_SUCC(ret) && i < right_group.end_ - right_group.start_; ++i) {
        if (!rj_match_vec_->at(i) && OB_FAIL(output_cache_.push_back(RowPair(
                                         -1, right_group.start_ + i)))) {
          LOG_WARN("push back non-matching right side row to output_cache_ failed", K(ret));
        }
      }
    }
    right_match_all_output_group_idx_ = trace_group_idx_;
  }
  brs.size_ = filled_row_cnt;
  if (need_trace_) {
    start_trace();
  }
  return ret;
}
int ObMergeJoinVecOp::ObCommonJoinTracker::init_match_flags()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expand_match_flags_if_necessary(cur_right_group_->count()))) {
    LOG_WARN("expand group flags failed", K(ret));
  }
  return ret;
}

int ObMergeJoinVecOp::ObCommonJoinTracker::expand_match_flags_if_necessary(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size > rj_match_vec_size_)) {
    if (OB_ISNULL(rj_match_vec_)) {
      rj_match_vec_size_ = std::max(
          static_cast<int64_t>(ObBitVector::WORD_BITS), next_pow2(size));
      const int64_t mem_size = ObBitVector::memory_size(rj_match_vec_size_);
      if (OB_ISNULL(rj_match_vec_ = to_bit_vector(allocator_->alloc(mem_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate right join group flags failed", K(ret));
      }
    } else {
      ObBitVector *ori_vec = rj_match_vec_;
      rj_match_vec_size_ = next_pow2(size);
      const int64_t mem_size = ObBitVector::memory_size(rj_match_vec_size_);
      if (OB_ISNULL(rj_match_vec_ = to_bit_vector(allocator_->alloc(mem_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate right join group flags failed", K(ret));
      }
      allocator_->free(ori_vec);
    }
  }
  rj_match_vec_->reset(rj_match_vec_size_);
  return ret;
}

int ObMergeJoinVecOp::ObCommonJoinTracker::match_proc(ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  bool can_ret = false;
  int64_t row_cnt = brs.size_;
  if (last_left_row_id_ == -2) {
    last_left_row_id_ = left_row_id_array_[0];
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
    if (last_left_row_id_ != left_row_id_array_[i]) {
      // left outer/right outer/full outer join add non-matchinged left row to output
      if (!last_left_row_matched_ && last_left_row_id_ != -1) {
        if (OB_FAIL(output_cache_.push_back(RowPair(last_left_row_id_, -1)))) {
          LOG_WARN("push back group row to output_cache_ failed", K(ret));
        }
      }
      last_left_row_matched_ = false;
    }
    // Once all calculations for a group are completed, we can start checking if
    // there are any unmatched rows in the right branch that need to be added to
    // the cache.
    if (OB_FAIL(ret)) {
    } else if (join_type_ == FULL_OUTER_JOIN && reach_cur_group_end(left_row_id_array_[i])) {
      RowGroup &right_group = group_pairs_.at(trace_group_idx_).second;
      for (int64_t i = 0; OB_SUCC(ret) && i < right_group.end_ - right_group.start_; ++i) {
        if (!rj_match_vec_->at(i) && OB_FAIL(output_cache_.push_back(RowPair(
                                         -1, right_group.start_ + i)))) {
          LOG_WARN("push back non-matching right side row to output_cache_ failed", K(ret));
        }
      }
      right_match_all_output_group_idx_ = trace_group_idx_;
      if (OB_FAIL(ret)) {
      } else if (++trace_group_idx_ < group_pairs_.count()) {
        RowGroup &new_right_match = group_pairs_.at(trace_group_idx_).second;
        if (OB_FAIL(expand_match_flags_if_necessary(new_right_match.count()))) {
          LOG_WARN("expand group flags failed", K(ret));
        }
      }
    }
    last_left_row_id_ = left_row_id_array_[i];
    if (OB_FAIL(ret)) {
    } else if (right_row_id_array_[i] == -1 || left_row_id_array_[i] == -1) {
      last_left_row_matched_ = true;
      if (OB_FAIL(output_cache_.push_back(RowPair(left_row_id_array_[i], right_row_id_array_[i])))) {
        LOG_WARN("push back group row to output_cache_ failed", K(ret));
      } else if (join_type_ == FULL_OUTER_JOIN && right_row_id_array_[i] != -1) {
        RowGroup &right_group = group_pairs_.at(trace_group_idx_).second;
        rj_match_vec_->set(right_row_id_array_[i] - right_group.start_);
      }
    } else {
      if (!brs.skip_->at(i)) {
        last_left_row_matched_ = true;
        if (OB_FAIL(output_cache_.push_back(RowPair(left_row_id_array_[i], right_row_id_array_[i])))) {
          LOG_WARN("push back group row to output_cache_ failed", K(ret));
        } else if (join_type_ == FULL_OUTER_JOIN && right_row_id_array_[i] != -1) {
          RowGroup &right_group = group_pairs_.at(trace_group_idx_).second;
          rj_match_vec_->set(right_row_id_array_[i] - right_group.start_);
        }
      }
    }
  }
  return ret;
}

int ObMergeJoinVecOp::ObSemiAntiJoinTracker::init(int64_t tenant_id,
                                                  int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  match_pair_array_size_ = max_batch_size;
  match_pair_cnt_ = max_batch_size;
  int match_vec_array_size = 0;
  intermediate_cache_.set_attr(ObMemAttr(tenant_id, "SqlMJSJCache"));
  match_vec_array_size = match_pair_array_size_;
  if (OB_ISNULL(semi_anti_match_pair_array_ = static_cast<SemiAntiMatchPair *>(
            allocator_->alloc(sizeof(SemiAntiMatchPair) * match_pair_array_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_ISNULL(semi_anti_match_pair_ptr_array_ = static_cast<SemiAntiMatchPair **>(
            allocator_->alloc(sizeof(SemiAntiMatchPair *) * match_pair_array_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    MEMSET(semi_anti_match_pair_ptr_array_, NULL, sizeof(SemiAntiMatchPair *) * match_pair_array_size_);
    for (int64_t i = 0; i < max_batch_size; ++i) {
      semi_anti_match_pair_array_[i].init(
          join_type_ == LEFT_SEMI_JOIN || join_type_ == RIGHT_SEMI_JOIN, this);
      semi_anti_match_pair_ptr_array_[i] = &semi_anti_match_pair_array_[i];
    }
  }
  return ret;
}

int ObMergeJoinVecOp::ObSemiAntiJoinTracker::SemiAntiMatchPair::next_left_row(int64_t vec_idx)
{
  int ret = OB_SUCCESS;
  cur_left_group_ = tracker_->cur_left_group_;
  left_row_id_ = tracker_->cur_left_group_->cur_++;
  right_group_.copy(tracker_->cur_right_group_);
  return ret;
}

int ObMergeJoinVecOp::ObSemiAntiJoinTracker::SemiAntiMatchPair::next_row_pair(
    bool need_trace, int64_t vec_idx, bool &calc_skip, bool &iter_end)
{
  int ret = OB_SUCCESS;
  calc_skip = false;
  iter_end = false;
  bool need_next_group = false;
  if (left_row_matched_) {
    ++cur_left_group_->calced_size_;
    right_group_.cur_ = right_group_.end_ + 1;
    left_row_matched_ = false;
    if (is_semi_join_ && OB_FAIL(tracker_->intermediate_cache_.push_back(left_row_id_))) {
      LOG_WARN("push back matching left side row to intermediate_cache_ failed", K(ret));
    } else if (tracker_->cur_left_group_->iter_end()) {
      need_next_group = true;
    } else if (OB_FAIL(next_left_row(vec_idx))) {
      LOG_WARN("iter next left row failed", K(ret), K(vec_idx));
    }
  } else if (right_group_.is_empty()) {
    if (tracker_->cur_left_group_->iter_end()) {
      need_next_group = true;
    } else if (OB_FAIL(next_left_row(vec_idx))) {
      LOG_WARN("iter next left row failed", K(ret), K(vec_idx));
    }
  } else if (right_group_.iter_end()) {
    if (right_group_.cur_ == right_group_.end_) {
      ++right_group_.cur_;
      ++cur_left_group_->calced_size_;
      right_group_.cur_ = right_group_.end_ + 1;
      if (!is_semi_join_ && OB_FAIL(tracker_->intermediate_cache_.push_back(left_row_id_))) {
        LOG_WARN("push back non-matching left side row to intermediate_cache_ failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && tracker_->cur_left_group_->iter_end()) {
      need_next_group = true;
    } else if (OB_FAIL(next_left_row(vec_idx))) {
      LOG_WARN("iter next left row failed", K(ret), K(vec_idx));
    }
  }

  if (OB_SUCC(ret) && need_next_group) {
    if (OB_FAIL(get_next_group_pair())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("get next group failed", K(ret));
      } else {
        ret = OB_SUCCESS;
        calc_skip = true;
        iter_end = true;
      }
    } else if (OB_FAIL(next_left_row(vec_idx))) {
      LOG_WARN("iter next left row failed", K(ret), K(vec_idx));
    }
  }
  if (OB_SUCC(ret) && !calc_skip) {
    if (right_group_.is_empty() && left_row_id_ >= 0) {
      calc_skip = true;
      ++cur_left_group_->calced_size_;
      if (OB_FAIL(tracker_->intermediate_cache_.push_back(left_row_id_))) {
        LOG_WARN("push back non-matching left side row to intermediate_cache_ failed", K(ret));
      }
    }
  }
  return ret;
}

int ObMergeJoinVecOp::ObSemiAntiJoinTracker::SemiAntiMatchPair::get_next_group_pair()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tracker_->next_group_pair())){
    if (ret == OB_ITER_END) {
    } else {
      LOG_WARN("expand group flags failed", K(ret));
    }
  }
  return ret;
}

int ObMergeJoinVecOp::ObSemiAntiJoinTracker::reorder_output_rows()
{
  int ret = OB_SUCCESS;
  RowGroup *left = nullptr;
  RowGroup *right = nullptr;
  int64_t i = output_group_idx_;
  for (; i < group_pairs_.count(); ++i) {
    get_group(i, left, right);
    if (!left->all_calced()) {
      break;
    }
  }
  if (i != output_group_idx_) {
    int64_t max_row_id = i == group_pairs_.count() ? INT64_MAX : left->start_;
    // Reverse Order
    lib::ob_sort(intermediate_cache_.begin(), intermediate_cache_.end(), RowidReverseCompartor());
    for (int64_t idx = intermediate_cache_.count() - 1; OB_SUCC(ret) && idx >= 0; --idx) {
      int64_t &row_id = intermediate_cache_.at(idx);
      if (row_id < max_row_id) {
        if (OB_FAIL(intermediate_cache_.remove(idx))) {
          LOG_WARN("remove RowPair from intermediate_cache_ failed", K(ret), K(idx), K(row_id));
        } else if (OB_FAIL(output_cache_.push_back(RowPair(row_id, -1)))) {
          LOG_WARN("push back RowPair to output_cache_ failed", K(ret), K(row_id));
        }
      } else {
        break;
      }
    }
    output_group_idx_ = i;
  }
  return ret;
}

int ObMergeJoinVecOp::ObSemiAntiJoinTracker::fill_match_pair(int64_t max_pair_cnt, ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  bool calc_skip = false;
  bool iter_end = false;
  brs.reset_skip(match_pair_cnt_);
  brs.size_ = 0;
  int64_t idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < match_pair_cnt_; ++i) {
    if (OB_FAIL(semi_anti_match_pair_ptr_array_[i]->next_row_pair(true, i, calc_skip, iter_end))) {
      LOG_WARN("semi or anti match pair get next pair failed", K(ret), K(i));
    } else if (!iter_end) {
      if (calc_skip) { brs.set_skip(idx); }
      semi_anti_match_pair_ptr_array_[idx++] = semi_anti_match_pair_ptr_array_[i];
    }
  }
  match_pair_cnt_ = idx;
  brs.size_ = idx;
  for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
    if (OB_FAIL(left_match_cursor_->duplicate_store_row_ptr(
            semi_anti_match_pair_ptr_array_[i]->right_group_.is_empty()
                ? -1
                : semi_anti_match_pair_ptr_array_[i]->left_row_id_,
            i, 1, nullptr))) {
      LOG_WARN("left_match_cursor_ duplicate cur store row failed", K(ret),
               K(semi_anti_match_pair_ptr_array_[i]->left_row_id_), K(i));
    } else if (OB_FAIL(right_match_cursor_->duplicate_store_row_ptr(
                   semi_anti_match_pair_ptr_array_[i]->right_group_.cur_++, i,
                   1, nullptr))) {
      LOG_WARN("right_match_cursor_ duplicate cur store row failed", K(ret),
               K(semi_anti_match_pair_ptr_array_[i]->right_group_.cur_), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (brs.size_ == 0 && OB_FAIL(reorder_output_rows())) {
    LOG_WARN("semi join or anti join reorder output rows failed", K(ret));
  }
  return ret;
}

int ObMergeJoinVecOp::ObSemiAntiJoinTracker::match_proc(ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < brs.size_; ++i) {
    if (!brs.skip_->at(i) && OB_FALSE_IT(semi_anti_match_pair_ptr_array_[i]->match())) {
    }
  }
  if (OB_FAIL(reorder_output_rows())) {
    LOG_WARN("semi join or anti join reorder output rows failed", K(ret));
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::init(bool is_left,
    const uint64_t tenant_id, ObOperator *child,
    const ExprFixedArray *all_exprs,
    const ExprFixedArray *equal_keys,
    const common::ObFixedArray<int64_t, common::ObIAllocator> *key_idx,
    const EqualCondInfoArray &equal_cond_infos,
    ObIOEventObserver &io_event_observer, double mem_bound_raito) {
  int ret = OB_SUCCESS;
  source_ = child;
  all_exprs_ = all_exprs;
  // child operator's spec batch_size might be zero(vectorization NOT enabled
  // case) In that case, child return 1 row at a time, set local batch default 1
  max_batch_size_ = child->get_spec().max_batch_size_ == 0
                        ? 1
                        : child->get_spec().max_batch_size_;
  if (OB_ISNULL(mj_op_.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null memory context", K(ret));
  } else if (OB_ISNULL(allocator_ = mj_op_.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is NULL", K(ret));
  } else if (OB_FAIL(init_row_store(tenant_id, io_event_observer))) {
    LOG_WARN("init temp row store failed", K(ret));
  } else if (OB_FAIL(result_hldr_.init(*all_exprs, eval_ctx_))) {
    LOG_WARN("init result holder failed!", K(ret));
  } else if (OB_FAIL(init_stored_batch_rows())) {
    LOG_WARN("init stored batch rows failed!", K(ret));
  } else {
    if (OB_ISNULL(equal_keys) || OB_ISNULL(key_idx)) {
      if (OB_FAIL(init_equal_key_exprs(is_left, equal_cond_infos))) {
        LOG_WARN("init equal key exprs failed", K(ret));
      } else {
        equal_key_exprs_arry_ptr_ = &equal_key_exprs_;
        equal_key_idx_arry_ptr_ = &equal_key_idx_;
      }
    } else {
      equal_key_exprs_arry_ptr_ = equal_keys;
      equal_key_idx_arry_ptr_ = key_idx;
    }
  }
  mem_bound_raito_ = mem_bound_raito;
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::init_equal_key_exprs(bool is_left,
      const EqualCondInfoArray &equal_cond_infos)
{
  int ret = OB_SUCCESS;
  if (OB_FALSE_IT(equal_key_idx_.set_allocator(allocator_))) {
  } else if (OB_FAIL(equal_key_idx_.init(equal_cond_infos.count()))) {
    LOG_WARN("init equal param idx failed", K(ret), K(equal_cond_infos.count()));
  } else if (OB_FALSE_IT(equal_key_exprs_.set_allocator(allocator_))) {
  } else if (OB_FAIL(equal_key_exprs_.init(equal_cond_infos.count()))) {
    LOG_WARN("init equal_key_exprs_ failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < equal_cond_infos.count(); ++i) {
    const ObMergeJoinVecSpec::EqualConditionInfo &equal_cond = equal_cond_infos.at(i);
    ObExpr *key_expr = NULL;
    if ((is_left && !equal_cond.is_opposite_) || (!is_left && equal_cond.is_opposite_)) {
      key_expr = equal_cond.expr_->args_[0];
    } else {
      key_expr = equal_cond.expr_->args_[1];
    }
    if (OB_FAIL(equal_key_exprs_.push_back(key_expr))) {
      LOG_WARN("push back equal key expr failed", K(ret));
    }
    int64_t idx = -1;
    for (int64_t j = 0; j < all_exprs_->count() && idx < 0; j++) {
      if (all_exprs_->at(j) == key_expr) {
        idx = j;
      }
    }
    if (OB_UNLIKELY(idx < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("equal cond param not found in child output", K(ret), K(i), K(equal_cond),
                KPC(key_expr), K(is_left));
    } else if (OB_FAIL(equal_key_idx_.push_back(idx))) {
      LOG_WARN("push back failed", K(ret));
    }
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::init_store_rows_array()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store_rows_)) {
    if (OB_ISNULL(store_rows_ = static_cast<ObCompactRow **>(allocator_->alloc(
                         sizeof(ObCompactRow *) * max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    }
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::init_col_equal_group_boundary()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_equal_group_boundary_)) {
    if (equal_key_exprs_arry_ptr_->count() > 1) {
      if (OB_ISNULL(col_equal_group_boundary_ = static_cast<int64_t *>(allocator_->alloc(
                          sizeof(int64_t) * equal_key_exprs_arry_ptr_->count())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMSET(col_equal_group_boundary_, 0, sizeof(int64_t) * equal_key_exprs_arry_ptr_->count());
      }
    }
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::init_row_store(const uint64_t tenant_id,
      ObIOEventObserver &io_event_observer)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr attr(tenant_id, ObLabel("ObMergeJoinVec"));
  if (OB_FAIL(row_store_.init(*all_exprs_, max_batch_size_, attr,
                            INT64_MAX /*set mem_limit later*/,
                            true, 0, common::NONE_COMPRESSOR))) {
    LOG_WARN("init temp row store failed", K(ret));
  } else {
    row_store_.set_mem_stat(&mj_op_.sql_mem_processor_);
    row_store_.set_dir_id(mj_op_.sql_mem_processor_.get_dir_id());
    row_store_.set_allocator(*allocator_);
    row_store_.set_io_event_observer(&io_event_observer);
    row_store_reader_.init(&row_store_);
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::init_stored_batch_rows()
{
  int ret = OB_SUCCESS;
  void *mem = allocator_->alloc(ObBitVector::memory_size(max_batch_size_));
  if (OB_ISNULL(mem)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate mem failed", K(ret));
  } else {
    store_brs_.skip_ = to_bit_vector(mem);
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::save_cur_batch() {
  int ret = OB_SUCCESS;
  if (!saved_ && OB_FAIL(result_hldr_.save(cur_brs_->size_))) {
    LOG_WARN("save cur batch failed", K(ret), K(cur_brs_), K(cur_batch_idx_));
  } else {
    saved_ = true;
    restored_ = false;
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::restore_cur_batch() {
  int ret = OB_SUCCESS;
  if (!restored_ && OB_FAIL(result_hldr_.restore())) {
    LOG_WARN("restore cur batch failed", K(ret), K(cur_brs_), K(cur_batch_idx_));
  } else {
    restored_ = true;
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::eval_all_exprs()
{
  int ret = OB_SUCCESS;
  mj_op_.clear_evaluated_flag();
  for (int64_t i = 0; OB_SUCC(ret) && i < all_exprs_->count(); ++i) {
    ObExpr *expr = all_exprs_->at(i);
    if (OB_FAIL(expr->eval_vector(eval_ctx_, *cur_brs_))) {
      LOG_WARN("eval vector failed", K(ret), K(i), K(expr));
    }
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::get_next_valid_batch() {
  int ret = OB_SUCCESS;
  do {
    if (OB_FAIL(get_next_batch_from_source())) {
      LOG_WARN("get next batch from source failed", K(ret));
      break;
    }
  } while (!reach_end_ && cur_batch_idx_ == cur_brs_->size_);
  if (OB_SUCC(ret) && cur_batch_idx_ > cur_brs_->size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch idx greater than batch size", K(ret), K(cur_batch_idx_), K(cur_brs_));
  }
  return ret;
}

template <bool need_store_uneuqal, bool need_flip>
int ObMergeJoinVecOp::ObMergeJoinCursor::find_small_group(
    const ObMergeJoinVecOp::ObMergeJoinCursor &other,
    const common::ObFixedArray<int64_t, common::ObIAllocator> &merge_directions,
    int &cmp)
{
  int ret = OB_SUCCESS;
  bool all_find = false;
  int64_t cur_batch_smaller_rows_cnt = 1;
  if (need_store_uneuqal) {
    small_row_group_.start_ = next_stored_row_id_;
    store_brs_.skip_->unset(cur_batch_idx_);
  }
  cur_brs_->set_skip(cur_batch_idx_);
  while (OB_SUCC(ret) && !all_find) {
    while (OB_SUCC(ret) && ++cur_batch_idx_ < cur_brs_->size_) {
      if (!cur_brs_->skip_->at(cur_batch_idx_)) {
        ret = need_flip ? other.compare(*this, merge_directions, cmp)
                        : compare(other, merge_directions, cmp);
        cmp = need_flip ? -1 * cmp : cmp;
        if (OB_FAIL(ret)) {
          LOG_WARN("compare failed", K(ret));
        } else if (cmp < 0) {
          if (need_store_uneuqal) {
            store_brs_.skip_->unset(cur_batch_idx_);
            ++cur_batch_smaller_rows_cnt;
          }
          cur_brs_->set_skip(cur_batch_idx_);
        } else {
          all_find = true;
          break;
        }
      }
    }
    if (need_store_uneuqal) {
      next_stored_row_id_ += cur_batch_smaller_rows_cnt;
      stored_match_row_cnt_ += cur_batch_smaller_rows_cnt;
    }
    if (OB_SUCC(ret) && !all_find) {
      int64_t stored_rows_cnt = 0;
      if (OB_FAIL(store_rows_of_cur_batch(stored_rows_cnt))) {
        LOG_WARN("store cur batch rows failed", K(ret), K(need_store_uneuqal), K(stored_rows_cnt));
      } else if (OB_FAIL(get_next_valid_batch())) {
        LOG_WARN("get next batch from source failed", K(ret));
      } else if (reach_end_) {
        all_find = true;
      } else if (mj_op_.has_enough_match_rows()) {
        break;
      } else {
        cur_batch_idx_ -= 1;
        cur_batch_smaller_rows_cnt = 0;
      }
    }
  }
  if (need_store_uneuqal) { small_row_group_.end_ = next_stored_row_id_; }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::get_next_batch_from_source()
{
  int ret = OB_SUCCESS;
  const ObBatchRows *brs = nullptr;
  if (reach_end_) {
  } else if (OB_NOT_NULL(cur_brs_) && cur_brs_->end_) {
    reach_end_ = true;
  } else if (OB_FALSE_IT(mj_op_.clear_evaluated_flag())) {
  } else if (OB_FAIL(source_->get_next_batch(max_batch_size_, brs))) {
    if (ret == OB_ITER_END) {
      reach_end_ = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get next batch from source failed", K(ret));
    }
  } else if (OB_FALSE_IT(cur_brs_ = const_cast<ObBatchRows *>(brs))) {
  } else if (OB_FALSE_IT(store_brs_.size_ = cur_brs_->size_)) {
  } else if (OB_FALSE_IT(store_brs_.all_rows_active_ = false)) {
  } else if (OB_FALSE_IT(store_brs_.end_ = false)) {
  } else if (OB_FALSE_IT(store_brs_.skip_->set_all(cur_brs_->size_))) {
  } else if (cur_brs_->end_) {
    reach_end_ = true;
  } else if (OB_FAIL(eval_all_exprs())) {
    LOG_WARN("eval all exprs failed", K(ret), K(*all_exprs_));
  } else {
    cur_batch_idx_ = 0;
    while (cur_batch_idx_ < cur_brs_->size_ && cur_brs_->skip_->at(cur_batch_idx_)) {
      ++cur_batch_idx_;
    }
    if (OB_NOT_NULL(col_equal_group_boundary_)) {
      MEMSET(col_equal_group_boundary_, 0, sizeof(int64_t) * equal_key_exprs_arry_ptr_->count());
    }
    saved_ = false;
    restored_ = true;
  }
  if (reach_end_) {
    cur_brs_->size_ = 0;
    cur_batch_idx_ = 0;
  }

  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::compare(
    const ObMergeJoinVecOp::ObMergeJoinCursor &other,
    const common::ObIArray<int64_t> &merge_directions, int &cmp) const
{
  int ret = OB_SUCCESS;
  cmp = 0;
  for (int64_t i = 0; OB_SUCC(ret) && cmp == 0 && i < equal_key_exprs_arry_ptr_->count(); ++i) {
    const ObMergeJoinVecSpec::EqualConditionInfo &equal_cond = mj_op_.get_equal_cond_info(i);
    ObExpr *l_expr = equal_key_exprs_arry_ptr_->at(i);
    ObExpr *r_expr = other.equal_key_exprs_arry_ptr_->at(i);
    ObIVector *l_vec = l_expr->get_vector(eval_ctx_);
    ObIVector *r_vec = r_expr->get_vector(other.eval_ctx_);
    bool l_null = false;
    int32_t l_len = 0;
    const char *l_data = nullptr;
    bool r_null = false;
    int32_t r_len = 0;
    const char *r_data = nullptr;
    l_vec->get_payload(cur_batch_idx_, l_null, l_data, l_len);
    r_vec->get_payload(other.cur_batch_idx_, r_null, r_data, r_len);
    if (l_null && r_null) {
      cmp = (T_OP_NSEQ == equal_cond.expr_->type_) ? 0 : -1;
    } else {
      if (OB_FAIL((*(equal_cond.ns_cmp_func_))(
        l_expr->obj_meta_, r_expr->obj_meta_, l_data, l_len, l_null, r_data,
        r_len, r_null, cmp))) {
        LOG_WARN("compare left and right cursor failed", K(ret));
      }
    }
    cmp = merge_directions.at(i) * cmp;
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::get_equal_group_end_idx_in_cur_batch(
    int64_t &equal_end_idx, bool &all_find)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  int64_t calc_idx = cur_batch_idx_ + 1;
  all_find = false;
  if (calc_idx == equal_end_idx) {
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < equal_key_exprs_arry_ptr_->count(); ++i) {
      if (OB_NOT_NULL(col_equal_group_boundary_) && calc_idx < col_equal_group_boundary_[i]) {
        equal_end_idx = equal_end_idx < col_equal_group_boundary_[i]
                            ? equal_end_idx
                            : col_equal_group_boundary_[i];
      } else {
        ObExpr *expr = equal_key_exprs_arry_ptr_->at(i);
        NullSafeRowCmpFunc ns_cmp_func = expr->basic_funcs_->row_null_last_cmp_;
        calc_idx = cur_batch_idx_ + 1;
        cmp = 0;
        bool ori_null = false;
        int32_t ori_len = 0;
        const char *ori_data = nullptr;
        bool null = false;
        int32_t len = 0;
        const char *data = nullptr;
        ObIVector *vector = nullptr;
        vector = expr->get_vector(eval_ctx_);
        vector->get_payload(cur_batch_idx_, ori_null, ori_data, ori_len);
        for (; OB_SUCC(ret) && calc_idx < equal_end_idx; ++calc_idx) {
          if (!cur_brs_->skip_->at(calc_idx)) {
            vector->get_payload(calc_idx, null, data, len);
            if (ori_null && null) {
              cmp = 0;
            } else if (lib::is_oracle_mode()) {
              if (OB_FAIL((*expr->basic_funcs_->row_null_last_cmp_)(
                expr->obj_meta_, expr->obj_meta_, ori_data, ori_len, ori_null,
                data, len, null, cmp))) {
                LOG_WARN("compare left and right cursor failed", K(ret));
              }
            } else {
              if (OB_FAIL((*expr->basic_funcs_->row_null_first_cmp_)(
                expr->obj_meta_, expr->obj_meta_, ori_data, ori_len, ori_null,
                data, len, null, cmp))) {
                LOG_WARN("compare left and right cursor failed", K(ret));
              }
            }
            if (OB_SUCC(ret) && cmp != 0) {
              equal_end_idx = calc_idx;
              break;
            }
          }
        }
        if (OB_NOT_NULL(col_equal_group_boundary_)) { col_equal_group_boundary_[i] = calc_idx; }
      }
    }
  }
  all_find = equal_end_idx != cur_brs_->size_;
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::get_equal_group_end_idx_with_store_row(
    ObCompactRow *l_stored_row, int64_t &equal_end_idx, bool &all_find)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  int64_t calc_idx = cur_batch_idx_;
  all_find = false;
  if (calc_idx == equal_end_idx) {
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < equal_key_exprs_arry_ptr_->count(); ++i) {
      if (OB_NOT_NULL(col_equal_group_boundary_) && calc_idx < col_equal_group_boundary_[i]) {
        equal_end_idx = equal_end_idx < col_equal_group_boundary_[i]
                            ? equal_end_idx
                            : col_equal_group_boundary_[i];
      } else {
        ObExpr *expr = equal_key_exprs_arry_ptr_->at(i);
        int64_t col_idx = equal_key_idx_arry_ptr_->at(i);
        bool ori_null = l_stored_row->is_null(col_idx);
        int32_t ori_len = 0;
        const char *ori_data = nullptr;
        l_stored_row->get_cell_payload(row_store_.get_row_meta(), col_idx, ori_data, ori_len);
        calc_idx = cur_batch_idx_;
        cmp = 0;
        bool null = false;
        int32_t len = 0;
        const char *data = nullptr;
        for (; OB_SUCC(ret) && calc_idx < equal_end_idx; ++calc_idx) {
          if (!cur_brs_->skip_->at(calc_idx)) {
            ObIVector *vector = expr->get_vector(eval_ctx_);
            vector->get_payload(calc_idx, null, data, len);
            if (ori_null && null) {
              cmp = 0;
            } else if (lib::is_oracle_mode()) {
              if (OB_FAIL((*expr->basic_funcs_->row_null_last_cmp_)(
                expr->obj_meta_, expr->obj_meta_, ori_data, ori_len, ori_null,
                data, len, null, cmp))) {
                LOG_WARN("compare left and right cursor failed", K(ret));
              }
            } else {
              if (OB_FAIL((*expr->basic_funcs_->row_null_first_cmp_)(
                expr->obj_meta_, expr->obj_meta_, ori_data, ori_len, ori_null,
                data, len, null, cmp))) {
                LOG_WARN("compare left and right cursor failed", K(ret));
              }
            }
            if (OB_SUCC(ret) && cmp != 0) {
              equal_end_idx = calc_idx;
              break;
            }
          }
        }
        if (OB_NOT_NULL(col_equal_group_boundary_)) { col_equal_group_boundary_[i] = calc_idx; }
      }
    }
  }
  all_find = equal_end_idx != cur_brs_->size_;
  return ret;
}

template<bool need_store_equal_group>
int ObMergeJoinVecOp::ObMergeJoinCursor::get_equal_group(RowGroup &group)
{
  int ret = OB_SUCCESS;
  small_row_group_.reset();
  if (need_store_equal_group) {
    group.start_ = next_stored_row_id_;
    group.cur_ = next_stored_row_id_;
    group.end_ = next_stored_row_id_;
  }
  int cmp = 0;
  ObCompactRow *stored_row = nullptr;
  int64_t equal_end_idx = cur_brs_->size_;
  bool all_find = false;
  bool next_batch = false;
  while (OB_SUCC(ret) && !all_find) {
    int64_t cur_batch_equal_rows_cnt = 0;
    if (!next_batch) {
      if (OB_FAIL(get_equal_group_end_idx_in_cur_batch(equal_end_idx, all_find))) {
        LOG_WARN("get equal group in cur batch failed", K(ret));
      }
    } else {
      if (OB_FAIL(get_equal_group_end_idx_with_store_row(stored_row, equal_end_idx, all_find))) {
        LOG_WARN("get equal group end idx failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      for (int64_t i = cur_batch_idx_; i < equal_end_idx; ++i) {
        if (cur_brs_->skip_->at(i)) {
        } else {
          cur_brs_->set_skip(i);
          if (need_store_equal_group) {
            store_brs_.skip_->unset(i);
            ++cur_batch_equal_rows_cnt;
          }
        }
      }
      if (need_store_equal_group) {
        next_stored_row_id_ += cur_batch_equal_rows_cnt;
        stored_match_row_cnt_ += cur_batch_equal_rows_cnt;
      }
      if (!all_find) {
        int64_t stored_rows_cnt = 0;
        if (OB_FAIL(store_rows_of_cur_batch(stored_rows_cnt))) {
          LOG_WARN("store rows of cur_batch failed", K(ret));
        } else {
          if (need_store_equal_group) {
            if ((stored_rows_cnt < 1 || OB_ISNULL(stored_row = store_rows_[stored_rows_cnt-1]))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("last stored row is NULL", K(ret), K(stored_rows_cnt));
            }
          } else {
            if (OB_ISNULL(stored_row)) {
              ++next_stored_row_id_;
              if (OB_FAIL(store_one_row(cur_batch_idx_, stored_row))) {
                LOG_WARN("store cur row failed", K(ret), K(cur_batch_idx_));
              } else if (OB_ISNULL(stored_row)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("store_row is NULL", K(ret));
              }
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL((get_next_valid_batch()))) {
          LOG_WARN("get next batch from source failed", K(ret));
        } else if (reach_end_) {
          all_find = true;
          equal_end_idx = cur_brs_->size_;
        } else {
          next_batch = true;
          equal_end_idx = cur_brs_->size_;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (need_store_equal_group) {
      group.end_ = next_stored_row_id_;
    }
    cur_batch_idx_ = equal_end_idx;
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::init_mocked_null_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mocked_null_row_)) {
    void* ptr = nullptr;
    int64_t memory_size = sizeof(ObCompactRow) + ObTinyBitVector::memory_size(all_exprs_->count());
    if (OB_ISNULL(ptr = allocator_->alloc(memory_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(memory_size));
    } else {
      mocked_null_row_ = new (ptr) ObCompactRow();
      mocked_null_row_->nulls()->set_all(all_exprs_->count());
    }
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::update_store_mem_bound()
{
  int ret = OB_SUCCESS;
  bool updated = false;
  ObMergeJoinVecMemChecker checker(mj_op_.left_cursor_.get_stored_row_cnt() +
                                mj_op_.right_cursor_.get_stored_row_cnt());
  if (OB_FAIL(mj_op_.sql_mem_processor_.update_max_available_mem_size_periodically(
              allocator_,
              checker,
              updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else {
    int64_t t_mem_bound = mj_op_.sql_mem_processor_.get_mem_bound();
    int64_t c_mem_bound = static_cast<int64_t>(mem_bound_raito_ * t_mem_bound);
    if (OB_UNLIKELY(c_mem_bound < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected negative mem bound", K(ret), K(t_mem_bound), K(c_mem_bound),
        K(mem_bound_raito_));
    } else {
      row_store_.set_mem_limit(c_mem_bound);
    }
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::store_rows_of_cur_batch(int64_t &stored_row_cnt) {
  int ret = OB_SUCCESS;
  if (!store_brs_.skip_->is_all_true(store_brs_.size_)) {
    MEMSET(store_rows_, NULL, sizeof(ObCompactRow *) * max_batch_size_);
    stored_row_cnt = 0;
    if (OB_FAIL(update_store_mem_bound())) {
      LOG_WARN("update memory bound failed", K(ret));
    } else if (OB_FAIL(row_store_.add_batch(*all_exprs_, eval_ctx_, store_brs_,
                                            stored_row_cnt, store_rows_))) {
      LOG_WARN("row_store_ add batch failed", K(ret));
    } else {
      store_brs_.skip_->set_all(store_brs_.size_);
    }
  }
  if (OB_SUCC(ret) && next_stored_row_id_ != row_store_.get_row_cnt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "next_stored_row_id_ not equals to count of rows saved in row_store_",
        K(ret), K(next_stored_row_id_), K(row_store_.get_row_cnt()));
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::store_one_row(int64_t batch_idx, ObCompactRow *&stored_row)
{
  int ret = OB_SUCCESS;
  MEMSET(store_rows_, NULL, sizeof(ObCompactRow *) * max_batch_size_);
  if (OB_FAIL(update_store_mem_bound())) {
      LOG_WARN("update memory bound failed", K(ret));
  } else if (OB_FAIL(row_store_.add_row(*all_exprs_, batch_idx, eval_ctx_, stored_row))) {
    LOG_WARN("row_store_ add batch failed", K(ret));
  } else if (next_stored_row_id_ != row_store_.get_row_cnt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "next_stored_row_id_ not equals to count of rows saved in row_store_",
        K(ret), K(next_stored_row_id_), K(row_store_.get_row_cnt()));
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::init_ouput_vectors(int64_t max_vec_size)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < all_exprs_->count(); ++i) {
    ObExpr *e = all_exprs_->at(i);
    if (OB_FAIL(e->init_vector_default(eval_ctx_, max_vec_size))) {
      LOG_WARN("init right side output vector failed", K(ret));
    }
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::flat_group(
    int64_t start_id, int64_t cnt, int64_t row_ptr_idx,
    int64_t *row_id_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
    const ObCompactRow *stored_row = nullptr;
    if (OB_FAIL(row_store_reader_.get_row(start_id + i, stored_row))) {
      LOG_WARN("row_store_reader_ get next batch failed", K(ret),
               K(start_id + i), K(cnt),
               K(row_ptr_idx), K(get_stored_row_cnt()));
    } else {
      store_rows_[row_ptr_idx + i] = const_cast<ObCompactRow *>(stored_row);
      if (OB_NOT_NULL(row_id_array)) {
        row_id_array[row_ptr_idx + i] = start_id + i;
      }
    }
  }
  return ret;
}

int ObMergeJoinVecOp::ObMergeJoinCursor::duplicate_store_row_ptr(
    int64_t stored_row_id, int64_t ptr_idx, int64_t dup_cnt,
    int64_t *row_id_array)
{
  int ret = OB_SUCCESS;
  const ObCompactRow *stored_row = nullptr;
  if (stored_row_id == -1) {
    stored_row = mocked_null_row_;
  } else if (OB_FAIL(row_store_reader_.get_row(stored_row_id, stored_row))) {
    LOG_WARN("row_store_reader_ get next batch failed", K(ret),
             K(get_stored_row_cnt()), K(stored_row_id));
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < dup_cnt; ++i) {
      store_rows_[ptr_idx + i] = const_cast<ObCompactRow *>(stored_row);
      if (OB_NOT_NULL(row_id_array)) {
        row_id_array[ptr_idx + i] = stored_row_id;
      }
    }
  }
  return ret;
}

void ObMergeJoinVecOp::ObMergeJoinCursor::fill_null_row_ptr(
    int64_t ptr_idx, int64_t cnt, int64_t *row_id_array)
{
  for (int64_t i = 0; i < cnt; ++i) {
    store_rows_[ptr_idx + i] = mocked_null_row_;
    if (OB_NOT_NULL(row_id_array)) { row_id_array[ptr_idx+i] = -1; }
  }
}

void ObMergeJoinVecOp::ObMergeJoinCursor::flat_group_with_null_as_placeholder(
    int64_t ptr_idx, int64_t cnt, int64_t *row_id_array, int64_t start_row_id, ObBatchRows &brs)
{
  for (int64_t i = 0; i < cnt; ++i) {
    brs.set_skip(ptr_idx + i);
    store_rows_[ptr_idx + i] = mocked_null_row_;
    if (OB_NOT_NULL(row_id_array)) { row_id_array[ptr_idx + i] = start_row_id + i; }
  }
}

int ObMergeJoinVecOp::ObMergeJoinCursor::fill_vec_with_stored_rows(int64_t size)
{
  int ret = OB_SUCCESS;
  const RowMeta &row_meta = row_store_.get_row_meta();
  for (int expr_idx = 0; OB_SUCC(ret) && expr_idx < all_exprs_->count(); ++expr_idx) {
    ObExpr *e = all_exprs_->at(expr_idx);
    ObIVector *vec = e->get_vector(eval_ctx_);
    switch (vec->get_format()) {
      VEC_FORMAT_SWITCH_CASE_FOR_OUTPUT(VEC_FIXED,
                                        static_cast<ObFixedLengthBase *>(vec),
                                        row_meta, store_rows_, size, expr_idx);
      VEC_FORMAT_SWITCH_CASE_FOR_OUTPUT(VEC_DISCRETE,
                                        static_cast<ObDiscreteFormat *>(vec),
                                        row_meta, store_rows_, size, expr_idx);
      VEC_FORMAT_SWITCH_CASE_FOR_OUTPUT(VEC_CONTINUOUS,
                                        static_cast<ObContinuousFormat *>(vec),
                                        row_meta, store_rows_, size, expr_idx);
      VEC_FORMAT_SWITCH_CASE_FOR_OUTPUT(
          VEC_UNIFORM, static_cast<ObUniformFormat<false> *>(vec), row_meta,
          store_rows_, size, expr_idx);
      VEC_FORMAT_SWITCH_CASE_FOR_OUTPUT(
          VEC_UNIFORM_CONST, static_cast<ObUniformFormat<true> *>(vec),
          row_meta, store_rows_, size, expr_idx);
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", K(ret), K(vec->get_format()));
    }
    }
  }
  return ret;
}

ObMergeJoinVecOp::ObMergeJoinVecOp(ObExecContext &exec_ctx,
                                   const ObOpSpec &spec, ObOpInput *input)
    : ObJoinVecOp(exec_ctx, spec, input), join_state_(JOIN_BEGIN),
      mem_context_(nullptr), allocator_(nullptr),
      right_match_cursor_(nullptr), left_match_cursor_(nullptr),
      group_pairs_(), output_cache_(),
      output_row_num_(-1), left_cursor_(nullptr, *this, eval_ctx_),
      right_cursor_(nullptr, *this, eval_ctx_),
      profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
      sql_mem_processor_(profile_, op_monitor_info_),
      iter_end_(false), max_output_cnt_(-1), output_cache_idx_(0),
      tracker_(nullptr)
      {}

int ObMergeJoinVecOp::inner_open() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObJoinVecOp::inner_open())) {
    LOG_WARN("failed to open in base class", K(ret));
  } else if (OB_FAIL(init_mem_context())) {
    LOG_WARN("fail to init memory context", K(ret));
  } else {
    ObJoinType join_type = MY_SPEC.join_type_;
    bool is_right_drive = RIGHT_ANTI_JOIN == join_type ||
                      RIGHT_OUTER_JOIN == join_type ||
                      RIGHT_SEMI_JOIN == join_type;
    left_match_cursor_ = is_right_drive ? &right_cursor_ : &left_cursor_;
    right_match_cursor_ = is_right_drive ? &left_cursor_ : &right_cursor_;
    allocator_ = &mem_context_->get_malloc_allocator();
    const uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    group_pairs_.set_attr(ObMemAttr(tenant_id, "SqlMJVecGroups"));
    output_cache_.set_attr(ObMemAttr(tenant_id, "SqlMJOutput"));
    const ObIArray<ObMergeJoinVecSpec::EqualConditionInfo> &equal_cond_infos =
        MY_SPEC.equal_cond_infos_;
    const int64_t left_width = left_->get_spec().width_;
    const int64_t right_width = right_->get_spec().width_;
    const double width_ratio = ((double)left_width) / ((double)(left_width + right_width));
    const double MIN_LEFT_MEM_BOUND_RATIO = 0.2;
    const double MAX_LEFT_MEM_BOUND_RATIO = 0.8;
    // We prefer more memory to the left, otherwise there may waste memory, so
    // left_mem_bound_ratio is multiplied by a coefficient of 1.2.
    double left_mem_bound_ratio =
        MAX(MIN(MAX_LEFT_MEM_BOUND_RATIO, 1.2 * width_ratio),
            MIN_LEFT_MEM_BOUND_RATIO);
    const int64_t cache_size = MY_SPEC.max_batch_size_ * BATCH_MULTIPLE_FACTOR *
                               (left_width + right_width);
    bool is_compatible_mode = GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_5_0;
    if (OB_FAIL(sql_mem_processor_.init(allocator_,
                                        tenant_id,
                                        std::max(2L << 20, cache_size),
                                        MY_SPEC.type_, MY_SPEC.id_, &ctx_))) {
      LOG_WARN("failed to init sql memory manager processor", K(ret));
    } else if (OB_FAIL(left_cursor_.init(true,
                   tenant_id, left_, &(MY_SPEC.left_child_fetcher_all_exprs_),
                   is_compatible_mode ? nullptr : &(MY_SPEC.left_child_fetcher_equal_keys_),
                   is_compatible_mode ? nullptr : &(MY_SPEC.left_child_fetcher_equal_keys_idx_),
                   MY_SPEC.equal_cond_infos_,
                   io_event_observer_, left_mem_bound_ratio))) {
      LOG_WARN("init left batch fetcher failed", K(ret));
    } else if (OB_FAIL(right_cursor_.init(false,
                   tenant_id, right_, &(MY_SPEC.right_child_fetcher_all_exprs_),
                   is_compatible_mode ? nullptr : &(MY_SPEC.right_child_fetcher_equal_keys_),
                   is_compatible_mode ? nullptr : &(MY_SPEC.right_child_fetcher_equal_keys_idx_),
                   MY_SPEC.equal_cond_infos_,
                   io_event_observer_, 1.0 - left_mem_bound_ratio))) {
      LOG_WARN("init right batch fetcher failed", K(ret));
    } else if (join_type >= LEFT_SEMI_JOIN && join_type <= RIGHT_ANTI_JOIN &&
               MY_SPEC.other_join_conds_.count() > 0) {
      ObMergeJoinVecOp::ObSemiAntiJoinTracker *tracker = nullptr;
      if (OB_ISNULL(tracker = OB_NEWx(ObMergeJoinVecOp::ObSemiAntiJoinTracker,
                                      allocator_, *this, join_type,
                                      is_right_drive,
                                      left_match_cursor_,
                                      right_match_cursor_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc ObSemiAntiJoinTracker failed", K(ret));
      } else if (OB_FALSE_IT(tracker_ = tracker)) {
      } else if (OB_FAIL(tracker->init(tenant_id, MY_SPEC.max_batch_size_))) {
        LOG_WARN("ObSemiAntiJoinTracker init failed", K(ret));
      }
    } else {
      bool need_trace = join_type != INNER_JOIN && MY_SPEC.other_join_conds_.count() != 0;
      ObMergeJoinVecOp::ObCommonJoinTracker *tracker = nullptr;
      if (OB_ISNULL(tracker = OB_NEWx(ObMergeJoinVecOp::ObCommonJoinTracker,
                                      allocator_, *this, join_type, need_trace,
                                      is_right_drive,
                                      left_match_cursor_,
                                      right_match_cursor_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc ObCommonJoinTracker failed", K(ret));
      } else if (OB_FALSE_IT(tracker_ = tracker)) {
      } else if (OB_FAIL(tracker->init(MY_SPEC.max_batch_size_))) {
        LOG_WARN("ObCommonJoinTracker init failed", K(ret));
      }
    }
    LOG_TRACE("trace init sql mem mgr for merge join",
              K(profile_.get_cache_size()), K(profile_.get_expect_size()));
  }
  return ret;
}

int ObMergeJoinVecOp::init_mem_context() {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_context_)) {
    ObSQLSessionInfo *session = ctx_.get_my_session();
    uint64_t tenant_id = session->get_effective_tenant_id();
    lib::ContextParam param;
    param.set_mem_attr(tenant_id, ObModIds::OB_SQL_MERGE_JOIN,
                      ObCtxIds::WORK_AREA)
         .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity returned", K(ret));
    }
  }
  return ret;
}

int ObMergeJoinVecOp::join_begin() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(left_cursor_.get_next_valid_batch())) {
    LOG_WARN("left cursor start iter failed", K(ret));
  } else if (!left_cursor_.has_next_row()) {
    if (MY_SPEC.join_type_ == RIGHT_OUTER_JOIN ||
        MY_SPEC.join_type_ == FULL_OUTER_JOIN ||
        MY_SPEC.join_type_ == RIGHT_ANTI_JOIN) {
      if (OB_FAIL(right_cursor_.get_next_valid_batch())) {
        LOG_WARN("right cursor start iter failed", K(ret));
      } else {
        join_state_ = right_cursor_.has_next_row() ? OUTPUT_RIGHT_UNTIL_END : JOIN_END;
      }
    } else {
      join_state_ = JOIN_END;
    }
  } else if (OB_FAIL(right_cursor_.get_next_valid_batch())) {
    LOG_WARN("right cursor start iter failed", K(ret));
  } else if (!right_cursor_.has_next_row()) {
    if (MY_SPEC.join_type_ == LEFT_OUTER_JOIN ||
        MY_SPEC.join_type_ == FULL_OUTER_JOIN ||
        MY_SPEC.join_type_ == LEFT_ANTI_JOIN) {
      join_state_ = OUTPUT_LEFT_UNTIL_END;
    } else {
      join_state_ = JOIN_END;
    }
  } else {
    join_state_ = JOIN_BOTH;
  }
  if (OB_SUCC(ret) && join_state_ != JOIN_END) {
    if (OB_FAIL(left_cursor_.init_store_rows_array())) {
      LOG_WARN("left_cursor_ init store_rows_ failed", K(ret));
    } else if (OB_FAIL(right_cursor_.init_store_rows_array())) {
      LOG_WARN("right_cursor_ init store_rows_ failed", K(ret));
    } else if (OB_FAIL(left_cursor_.init_col_equal_group_boundary())) {
      LOG_WARN("left_cursor_ init col_equal_group_boundary_ failed", K(ret));
    } else if (OB_FAIL(right_cursor_.init_col_equal_group_boundary())) {
      LOG_WARN("right_cursor_ init col_equal_group_boundary_ failed", K(ret));
    } else if (OB_FAIL(left_cursor_.init_mocked_null_row())) {
      LOG_WARN("left_cursor_ init mocked null row failed!", K(ret));
    } else if (OB_FAIL(right_cursor_.init_mocked_null_row())) {
      LOG_WARN("right_cursor_ init mocked null row failed!", K(ret));
    }
  }
  return ret;
}

template <bool need_store_left_unequal_group,
          bool need_store_left_equal_group,
          bool need_store_right_unequal_group,
          bool need_store_right_equal_group>
int ObMergeJoinVecOp::join_both() {
  int ret = OB_SUCCESS;
  // clean for join both
  left_cursor_.clean_row_store();
  right_cursor_.clean_row_store();
  group_pairs_.reuse();
  ObJoinType join_type = MY_SPEC.join_type_;
  tracker_->reuse();
  int cmp = 0;
  if (OB_FAIL(left_cursor_.restore_cur_batch())) {
    LOG_WARN("left_cursor_ restore current batch failed", K(ret));
  } else if (OB_FAIL(left_cursor_.eval_all_exprs())) {
    LOG_WARN("left_cursor_ eval all exprs failed", K(ret));
  } else if (OB_FAIL(right_cursor_.restore_cur_batch())) {
    LOG_WARN("right_cursor_ restore current batch failed", K(ret));
  } else if (OB_FAIL(right_cursor_.eval_all_exprs())) {
    LOG_WARN("right_cursor_ eval all exprs failed", K(ret));
  } else {
    while (OB_SUCC(ret) && !has_enough_match_rows() && join_state_ == JOIN_BOTH) {
      if (!left_cursor_.has_next_row()) {
        // right join or join_end
        join_state_ = !group_pairs_.empty()
                          ? MATCH_GROUP_PROCESS
                          : (need_store_right_unequal_group &&
                                     right_cursor_.has_next_row()
                                 ? OUTPUT_RIGHT_UNTIL_END
                                 : JOIN_END);
      } else if (!right_cursor_.has_next_row()) {
        // left join or join_end
        join_state_ = !group_pairs_.empty()
                          ? MATCH_GROUP_PROCESS
                          : (need_store_left_unequal_group &&
                                    left_cursor_.has_next_row()
                                ? OUTPUT_LEFT_UNTIL_END
                                : JOIN_END);
      } else if (OB_FAIL(left_cursor_.compare(right_cursor_, MY_SPEC.merge_directions_, cmp))) {
        LOG_WARN("left side compare to right side failed", K(ret));
      } else {
        if (cmp < 0 &&
            OB_SUCCESS != (ret = left_cursor_.find_small_group<need_store_left_unequal_group, false>(
                             right_cursor_, MY_SPEC.merge_directions_, cmp))) {
          LOG_WARN("left_cursor_ get next bigger row in current batch failed", K(ret));
        }
        if (need_store_left_unequal_group && OB_SUCC(ret) && !left_cursor_.is_small_group_empty()) {
          if (OB_FAIL(group_pairs_.push_back(std::make_pair(
                  left_cursor_.get_small_group(), RowGroup())))) {
            LOG_WARN("push back group group into group_pairs_ failed", K(ret));
          }
          left_cursor_.reset_small_row_group();
        }
        if (OB_SUCC(ret)) {
          if (cmp > 0 &&
              OB_SUCCESS != (ret = right_cursor_.find_small_group<need_store_right_unequal_group, true>(
                               left_cursor_, MY_SPEC.merge_directions_, cmp))) {
            LOG_WARN("right_cursor_ get next bigger row in current batch failed", K(ret));
          }
          if (need_store_right_unequal_group && OB_SUCC(ret) &&
              !right_cursor_.is_small_group_empty()) {
            if (OB_FAIL(group_pairs_.push_back(std::make_pair(
                    RowGroup(), right_cursor_.get_small_group())))) {
              LOG_WARN("push back group group into group_pairs_ failed",
                       K(ret));
            }
            right_cursor_.reset_small_row_group();
          }
        }
        if (OB_SUCC(ret) && cmp == 0) {
          RowGroup left_group, right_group;
          if (OB_FAIL(left_cursor_.get_equal_group<need_store_left_equal_group>(left_group))) {
            LOG_WARN("left cursor get equal group failed", K(ret));
          } else if (OB_FAIL(right_cursor_
                                 .get_equal_group<need_store_right_equal_group>(
                                     right_group))) {
            LOG_WARN("right cursor get equal group failed", K(ret));
          } else if ((need_store_left_equal_group ||
                      need_store_right_equal_group) &&
                     OB_FAIL(group_pairs_.push_back(
                         std::make_pair(left_group, right_group)))) {
            LOG_WARN("push back group group into group_pairs_ failed", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!group_pairs_.empty()) {
      int64_t stored_rows_cnt = 0;
      join_state_ = MATCH_GROUP_PROCESS;
      if (OB_FAIL(left_cursor_.store_rows_of_cur_batch(stored_rows_cnt))) {
        LOG_WARN("left_cursor_ store rows of cur batch failed", K(ret));
      } else if (OB_FAIL(right_cursor_.store_rows_of_cur_batch(stored_rows_cnt))) {
        LOG_WARN("right_cursor_ store rows of cur batch failed", K(ret));
      } else if (OB_FAIL(left_cursor_.save_cur_batch())) {
        LOG_WARN("left_cursor_ save cur batch failed", K(ret));
      } else if (OB_FAIL(right_cursor_.save_cur_batch())) {
        LOG_WARN("right_cursor_ save cur batch failed", K(ret));
      } else {
        left_cursor_.row_store_finish_add();
        right_cursor_.row_store_finish_add();
      }
    }
  }
  // prepare for match process
  if (OB_SUCC(ret) && join_state_ == MATCH_GROUP_PROCESS) {
    tracker_->next_group_pair();
    if (join_type == FULL_OUTER_JOIN && MY_SPEC.other_join_conds_.count() > 0) {
      ObCommonJoinTracker *common_tracker = static_cast<ObCommonJoinTracker *>(tracker_);
      if (OB_FAIL(common_tracker->init_match_flags())) {
        LOG_WARN("expand group flags failed", K(ret));
      }
    }
  }
  return ret;
}

int ObMergeJoinVecOp::output_one_side_until_end(
    ObMergeJoinCursor &cursor, ObMergeJoinCursor &blank_cursor)
{
  int ret = OB_SUCCESS;
  left_match_cursor_ = &cursor;
  right_match_cursor_ = &blank_cursor;
  if (OB_FAIL(cursor.restore_cur_batch())) {
    LOG_WARN("cursor restore current batch failed", K(ret));
  } else if (OB_FAIL(blank_cursor.restore_cur_batch())) {
    LOG_WARN("blank_cursor restore current batch failed", K(ret));
  } else if (cursor.cur_batch_idx_ == cursor.cur_brs_->size_ &&
             output_cache_.count() == 0 &&
             OB_FAIL(cursor.get_next_valid_batch())) {
    LOG_WARN("get next batch from source failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    while (OB_SUCC(ret) && cursor.cur_batch_idx_ < cursor.cur_brs_->size_) {
      if (!cursor.cur_brs_->skip_->at(cursor.cur_batch_idx_) &&
          OB_FAIL(output_cache_.push_back(RowPair(cursor.next_stored_row_id_++, -1)))) {
        LOG_WARN("push back group row to output_cache_ failed", K(ret));
      }
      cursor.cur_batch_idx_++;
    }
    if (OB_SUCC(ret)) {
      int64_t stored_rows_cnt = 0;
      cursor.store_brs_.copy(cursor.cur_brs_);
      if (OB_FAIL(cursor.store_rows_of_cur_batch(stored_rows_cnt))) {
        LOG_WARN("right_cursor_ store rows of cur batch failed", K(ret));
      } else {
        cursor.cur_brs_->skip_->set_all(cursor.cur_brs_->size_);
        cursor.row_store_finish_add();
        blank_cursor.row_store_finish_add();
      }
    }
  }
  JoinState join_state = join_state_;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cursor.save_cur_batch())) {
    LOG_WARN("cursor save cur batch failed", K(ret));
  } else if (OB_FAIL(blank_cursor.save_cur_batch())) {
    LOG_WARN("blank_cursor save cur batch failed", K(ret));
  } else if (OB_FAIL(output_cached_rows())) {
    LOG_WARN("output cached rows failed", K(ret));
  } else if (output_cache_.count() == 0) {
    cursor.clean_row_store();
    blank_cursor.clean_row_store();
    if (!cursor.has_next_row()) {
      join_state = JOIN_END;
    }
  }
  join_state_ = join_state;
  return ret;
}

int ObMergeJoinVecOp::output_cached_rows()
{
  int ret = OB_SUCCESS;
  int64_t output_cnt = 0;
  set_row_store_it_age(&rows_it_age_);
  while (OB_SUCC(ret) && output_cnt < max_output_cnt_ && output_cache_idx_ < output_cache_.count()) {
    // get row from temp_row_store
    RowPair &pair = output_cache_.at(output_cache_idx_);
    if (pair.first == -1) {
      left_match_cursor_->fill_null_row_ptr(output_cnt, 1, nullptr);
    } else {
      if (OB_FAIL(left_match_cursor_->flat_group(
              pair.first, 1, output_cnt, nullptr))) {
        LOG_WARN("left_match_cursor_ get matching rows failed", K(ret), K(pair.first));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (pair.second == -1) {
      right_match_cursor_->fill_null_row_ptr(output_cnt, 1, nullptr);
    } else {
      if (OB_FAIL(right_match_cursor_->flat_group(
              pair.second, 1, output_cnt, nullptr))) {
        LOG_WARN("right_match_cursor_ get matching rows failed", K(ret), K(pair.second));
      }
    }
    ++output_cache_idx_;
    ++output_cnt;
  }
  // copy data to vector
  if (OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (output_cnt == 0) {
    } else if (OB_FAIL(init_output_vector(output_cnt))) {
      LOG_WARN("init output vector failed", K(ret), K(output_cnt));
    } else if (OB_FAIL(left_match_cursor_->fill_vec_with_stored_rows(output_cnt))) {
      LOG_WARN("left_match_cursor_ fill vector failed", K(ret));
    } else if (OB_FAIL(right_match_cursor_->fill_vec_with_stored_rows(output_cnt))) {
      LOG_WARN("right_match_cursor_ fill vector failed", K(ret));
    }
    brs_.size_ = output_cnt;
    brs_.reset_skip(output_cnt);
    if (output_cache_idx_ == output_cache_.count()) {
      output_cache_.reuse();
      output_cache_idx_ = 0;
      join_state_ = MATCH_GROUP_PROCESS;
    }
  }
  set_row_store_it_age(nullptr);
  return ret;
}

int ObMergeJoinVecOp::inner_get_next_batch(const int64_t max_row_cnt) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(iter_end_)) {
    brs_.size_ = 0;
    brs_.end_ = true;
  } else {
    max_output_cnt_ = min(max_row_cnt, MY_SPEC.max_batch_size_);
    bool can_output = false;
    clear_evaluated_flag();
    while (OB_SUCC(ret) && !can_output) {
      switch (join_state_) {
      case JoinState::JOIN_END: {
        iter_end_ = true;
        brs_.size_ = 0;
        brs_.end_ = true;
        can_output = true;
        break;
      }
      case JoinState::JOIN_BEGIN: {
        ret = join_begin();
        break;
      }
      case JoinState::JOIN_BOTH: {
        ObJoinType join_type = MY_SPEC.join_type_;
        // full outer join
        if (join_type == INNER_JOIN) {
          ret = join_both<false, true, false, true>();
        } else if (join_type == FULL_OUTER_JOIN) {
          ret = join_both<true, true, true, true>();
        } else if (join_type == LEFT_OUTER_JOIN) {
          ret = join_both<true, true, false, true>();
        } else if (join_type == RIGHT_OUTER_JOIN) {
          ret = join_both<false, true, true, true>();
        } else if (join_type == LEFT_SEMI_JOIN) {
          ret = MY_SPEC.other_join_conds_.count() == 0
                    ? join_both<false, true, false, false>()
                    : join_both<false, true, false, true>();
        } else if (join_type == RIGHT_SEMI_JOIN) {
          ret = MY_SPEC.other_join_conds_.count() == 0
                    ? join_both<false, false, false, true>()
                    : join_both<false, true, false, true>();
        } else if (join_type == LEFT_ANTI_JOIN) {
          ret = MY_SPEC.other_join_conds_.count() == 0
                    ? join_both<true, false, false, false>()
                    : join_both<true, true, false, true>();
        } else if (join_type == RIGHT_ANTI_JOIN) {
          ret = MY_SPEC.other_join_conds_.count() == 0
                    ? join_both<false, false, true, false>()
                    : join_both<false, true, true, true>();
        }
        break;
      }
      case JoinState::MATCH_GROUP_PROCESS: {
        ret = match_process(can_output);
        break;
      }
      case JoinState::OUTPUT_CACHED_ROWS: {
        can_output = true;
        ret = output_cached_rows();
        break;
      }
      case JoinState::OUTPUT_LEFT_UNTIL_END: {
        can_output = true;
        ret = output_one_side_until_end(left_cursor_, right_cursor_);
        break;
      }
      case JoinState::OUTPUT_RIGHT_UNTIL_END: {
        can_output = true;
        ret = output_one_side_until_end(right_cursor_, left_cursor_);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      }
    }
  }
  clear_evaluated_flag();
  return ret;
}

int ObMergeJoinVecOp::inner_rescan() {
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObJoinVecOp::inner_rescan())) {
    LOG_WARN("failed to rescan ObJoin", K(ret));
  }
  return ret;
}

int ObMergeJoinVecOp::inner_switch_iterator() {
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObJoinVecOp::inner_switch_iterator())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to rescan ObJoinVec", K(ret));
    }
  }
  return ret;
}

int ObMergeJoinVecOp::flat_group_pair_and_project_onto_vec(ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  brs.reset_skip(max_output_cnt_);
  set_row_store_it_age(&rows_it_age_);
  if (OB_FAIL(tracker_->fill_match_pair(max_output_cnt_, brs))) {
    LOG_WARN("semi join fill match pair failed", K(ret));
  }
  set_row_store_it_age(nullptr);
  if (OB_FAIL(ret) || brs.size_ == 0) {
  } else if (OB_FAIL(init_output_vector(brs.size_))) {
    LOG_WARN("init output vector failed", K(ret), K(brs.size_));
  } else if (OB_FAIL(right_match_cursor_->fill_vec_with_stored_rows(brs.size_))) {
    LOG_WARN("right_match_cursor_ fill vector failed", K(ret));
  } else if (OB_FAIL(left_match_cursor_->fill_vec_with_stored_rows(brs.size_))) {
    LOG_WARN("left_match_cursor_ row fill vector failed", K(ret));
  }
  return ret;
}

int ObMergeJoinVecOp::calc_other_cond_and_output_directly(bool &can_output)
{
  int ret = OB_SUCCESS;
  can_output = false;
  bool can_ret = false;
  while (OB_SUCC(ret) && !can_ret) {
    if (OB_FAIL(flat_group_pair_and_project_onto_vec(brs_))) {
      LOG_WARN("fill output vector failed", K(ret));
    } else if (brs_.size_ == 0) {
      join_state_ = JOIN_BOTH;
      can_ret = true;
    } else {
      clear_evaluated_flag();
      if (OB_FAIL(batch_calc_other_conds(brs_))) {
        LOG_WARN("batch calc other conditions failed", K(ret));
      } else {
        can_ret = !brs_.skip_->is_all_true(brs_.size_);
        can_output = can_ret;
      }
      clear_evaluated_flag();
    }
  }
  return ret;
}

int ObMergeJoinVecOp::calc_other_cond_and_cache_rows()
{
  int ret = OB_SUCCESS;
  bool can_ret = false;
  while (OB_SUCC(ret) && !can_ret) {
    if (OB_FAIL(flat_group_pair_and_project_onto_vec(brs_))) {
      LOG_WARN("fill output vector failed", K(ret));
    } else if (brs_.size_ == 0) {
      join_state_ = output_cache_.count() > 0 ? OUTPUT_CACHED_ROWS : JOIN_BOTH;
      can_ret = true;
    } else {
      clear_evaluated_flag();
      if (OB_FAIL(batch_calc_other_conds(brs_))) {
        LOG_WARN("batch calc other conditions failed", K(ret), K(brs_.size_));
      } else if (OB_FAIL(tracker_->match_proc(brs_))) {
        LOG_WARN("match process failed", K(ret));
      } else if (output_cache_.count() >= max_output_cnt_) {
        join_state_ = OUTPUT_CACHED_ROWS;
        can_ret = true;
      }
      clear_evaluated_flag();
    }
  }
  return ret;
}

int ObMergeJoinVecOp::init_output_vector(int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(left_cursor_.init_ouput_vectors(size))) {
    LOG_WARN("init left side output vector failed", K(ret), K(size));
  } else if (OB_FAIL(right_cursor_.init_ouput_vectors(size))) {
    LOG_WARN("init right side output vector failed", K(ret), K(size));
  }
  return ret;
}

int ObMergeJoinVecOp::match_process(bool &can_output)
{
  int ret = OB_SUCCESS;
  ObJoinType join_type = MY_SPEC.join_type_;
  if (join_type == INNER_JOIN ||
      (MY_SPEC.other_join_conds_.count() == 0 &&
       (join_type >= LEFT_OUTER_JOIN && join_type <= RIGHT_ANTI_JOIN))) {
    if (OB_FAIL(calc_other_cond_and_output_directly(can_output))) {
      LOG_WARN("common calc other cond failed", K(ret), K(join_type));
    }
  } else if (OB_FAIL(calc_other_cond_and_cache_rows())) {
    LOG_WARN("outer calc other cond failed", K(ret), K(join_type));
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase