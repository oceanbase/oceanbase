/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_i_das_search_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/ob_eval_bound.h"
#include "share/diagnosis/ob_profile_name_def.h"
#include "share/vector/ob_fixed_length_base.h"

namespace oceanbase
{
namespace sql
{
int UInt64RowIDStore::init(int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid capacity", K(ret), K(capacity));
  } else if (OB_UNLIKELY(DAS_ROWID_TYPE_UINT64 != search_ctx_.rowid_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("uint64 rowid store expects uint64 rowid type", K(ret), K(search_ctx_.rowid_type_));
  } else if (OB_NOT_NULL(data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data_ is not null, store already initialized", KR(ret));
  } else if (OB_ISNULL(data_ = static_cast<uint64_t *>(
                 search_ctx_.allocator_.alloc(capacity * sizeof(uint64_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for data", K(ret), K(capacity));
  } else {
    capacity_ = capacity;
  }
  return ret;
}

void UInt64RowIDStore::reset()
{
  if (OB_NOT_NULL(data_)) {
    search_ctx_.allocator_.free(data_);
    data_ = nullptr;
  }
  capacity_ = 0;
  count_ = 0;
}

int UInt64RowIDStore::fill(const int64_t begin_idx, const int64_t end_idx, const common::ObIArray<ObExpr *> &rowid_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data_ is null, store not initialized", KR(ret));
  } else if (OB_UNLIKELY(begin_idx < 0 || end_idx <= begin_idx || end_idx - begin_idx > capacity_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid begin_idx or end_idx", KR(ret), K(begin_idx), K(end_idx), K(capacity_));
  } else {
    ObExpr *expr = rowid_exprs.empty() ? nullptr : rowid_exprs.at(0);
    ObIVector *vec = nullptr;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowid expr is null", KR(ret));
    } else if (OB_ISNULL(vec = expr->get_vector(*search_ctx_.eval_ctx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vector is null", KR(ret));
    } else {
      count_ = 0;
      // Fixed format: Only for Uint64, no need to check the length
      if (OB_LIKELY(vec->get_format() == common::VEC_FIXED)) {
        const void *payload = vec->get_payload(begin_idx);
        if (OB_ISNULL(payload)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("vec get_payload returns null", KR(ret), K(begin_idx));
        } else {
          count_ = end_idx - begin_idx;
          MEMCPY(data_, payload, count_ * sizeof(uint64_t));
        }
      } else {
        for (int64_t i = begin_idx; OB_SUCC(ret) && i < end_idx; ++i) {
          data_[i - begin_idx] = vec->get_uint(i);
        }
        if (OB_SUCC(ret)) {
          count_ = end_idx - begin_idx;
        }
      }
    }
  }
  return ret;
}

int UInt64RowIDStore::get_rowid(int64_t idx, ObDASRowID &rowid) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data_ is null, store not initialized", KR(ret));
  } else if (idx < 0 || idx >= count_) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("index out of range", KR(ret), K(idx), K(count_));
  } else {
    rowid.set_uint64(data_[idx]);
  }
  return ret;
}

int UInt64RowIDStore::lower_bound(const ObDASRowID &target, const int64_t start_idx, int64_t &idx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data_ is null, store not initialized", KR(ret));
  } else if (OB_UNLIKELY(count_ == 0 || start_idx < 0 || start_idx >= count_)) {
    ret = OB_ITER_END;
  } else if (target.is_min()) {
    idx = start_idx;
  } else if (target.is_max()) {
    idx = count_;
  } else {
    uint64_t target_val = target.get_uint64();
    uint64_t* pos = std::lower_bound(data_ + start_idx, data_ + count_, target_val);
    idx = pos - data_;
  }
  // OB_ITER_END indicates no rowid >= target found
  if (OB_SUCC(ret) && idx == count_) {
    ret = OB_ITER_END;
  }
  return ret;
}

int CompactRowIDStore::init(int64_t capacity)
{
  int ret = OB_SUCCESS;
  common::ObIAllocator &allocator = search_ctx_.allocator_;
  const common::ObIArray<ObExpr *> &rowid_exprs = *search_ctx_.rowid_exprs_;
  if (capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid capacity", K(ret), K(capacity));
  } else if (OB_FAIL(store_.init(rowid_exprs,
                                 capacity,
                                 ObMemAttr(MTL_ID(), "DASRowIDStore"),
                                 INT64_MAX, /* never dump */
                                 false,
                                 0,
                                 ObCompressorType::NONE_COMPRESSOR))) {
    LOG_WARN("failed to init temp row store", K(ret));
  } else if (OB_ISNULL(rowids_ = static_cast<ObCompactRow **>(allocator.alloc(capacity * sizeof(ObCompactRow *))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for rowids", K(ret), K(capacity));
  } else {
    store_.set_allocator(allocator);
    capacity_ = capacity;
    count_ = 0;
    MEMSET(rowids_, 0, capacity * sizeof(ObCompactRow *));
  }
  return ret;
}

int CompactRowIDStore::fill(const int64_t begin_idx, const int64_t end_idx, const common::ObIArray<ObExpr *> &rowid_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowids_) || OB_ISNULL(search_ctx_.mock_skip_) || OB_ISNULL(search_ctx_.eval_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("store or search context not initialized", KR(ret), KP(rowids_), KP(search_ctx_.mock_skip_), KP(search_ctx_.eval_ctx_));
  } else if (OB_UNLIKELY(begin_idx < 0 || end_idx <= begin_idx || end_idx - begin_idx > capacity_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid begin_idx or end_idx", KR(ret), K(begin_idx), K(end_idx), K(capacity_));
  } else {
    store_.reuse();
    count_ = 0;
    search_ctx_.mock_skip_->unset_all(begin_idx, end_idx);
    const uint16_t batch_size = static_cast<uint16_t>(search_ctx_.eval_ctx_->max_batch_size_);
    const EvalBound bound(batch_size, static_cast<uint16_t>(begin_idx), static_cast<uint16_t>(end_idx), false);
    if (OB_FAIL(store_.add_batch(rowid_exprs, *search_ctx_.eval_ctx_, bound,
                                 *search_ctx_.mock_skip_, count_, rowids_))) {
      LOG_WARN("failed to add batch to row store", KR(ret));
    }
  }
  return ret;
}

void CompactRowIDStore::reset()
{
  store_.reset();
  if (OB_NOT_NULL(rowids_)) {
    search_ctx_.allocator_.free(rowids_);
  }
  rowids_ = nullptr;
  capacity_ = 0;
  count_ = 0;
}

void CompactRowIDStore::reuse()
{
  store_.reuse();
  count_ = 0;
}

int CompactRowIDStore::get_rowid(int64_t idx, ObDASRowID &rowid) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowids_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rowids_ is null, store not initialized", KR(ret));
  } else if (idx < 0 || idx >= count_) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("index out of range", K(ret), K(idx), K(count_));
  } else if (OB_ISNULL(rowids_[idx])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr rowid", K(ret), K(idx));
  } else {
    rowid.set_compact_row(rowids_[idx]);
  }
  return ret;
}

int CompactRowIDStore::lower_bound(const ObDASRowID &target, const int64_t start_idx, int64_t &idx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowids_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rowids_ is null, store not initialized", KR(ret));
  } else if (count_ == 0 || start_idx < 0 || start_idx >= count_) {
    ret = OB_ITER_END;
  } else if (target.is_min()) {
    idx = start_idx;
  } else if (target.is_max()) {
    idx = count_;
  } else {
    int64_t lo = start_idx;
    int64_t hi = count_;
    while (OB_SUCC(ret) && lo < hi) {
      int64_t mid = lo + (hi - lo) / 2;
      ObDASRowID mid_rowid;
      mid_rowid.set_compact_row(rowids_[mid]);
      int cmp = 0;
      if (OB_FAIL(search_ctx_.compare_rowid(mid_rowid, target, cmp))) {
        LOG_WARN("failed to compare rowids", K(ret), K(mid));
      } else if (cmp < 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }

    if (OB_SUCC(ret)) {
      idx = lo;
    }
  }

  // OB_ITER_END indicates no rowid >= target found
  if (OB_SUCC(ret) && idx == count_) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObIDASSearchOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  // Default implementation for op without score
  // for scoring op without block max iteration ability, need to set max score to infinity
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!target.is_normal())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target rowid", K(ret), K(target));
  } else if (inclusive) {
    max_score_tuple_.set(target, target, 0.0);
  } else {
    ObDASRowID tmp_id;
    //
    // uncomment this and add ObIDASSearchOp as ObDASRowID's friend if proven necessary
    //
    // if (search_ctx_.rowid_type_ == DAS_ROWID_TYPE_UINT64) {
    //   tmp_id.set_uint64(target.get_uint64() + 1);
    //   max_score_tuple_.set(tmp_id, tmp_id, 0.0);
    // } else {
      double score = 0.0;
      int cmp_ret = 0;
      if (OB_FAIL(do_advance_to(target, tmp_id, score))) {
        LOG_WARN("failed to advance to", K(ret), K(target));
      } else if (OB_FAIL(search_ctx_.compare_rowid(tmp_id, target, cmp_ret))) {
        LOG_WARN("failed to compare rowids", K(ret), K(tmp_id), K(target));
      } else if (0 != cmp_ret) {
        // skip
      } else if (OB_FAIL(do_next_rowid(tmp_id, score))) {
        LOG_WARN("failed to get next rowid", K(ret));
      }

      if (OB_SUCC(ret)) {
        max_score_tuple_.set(tmp_id, tmp_id, score);
      }
    // }
  }
  if (OB_SUCC(ret)) {
    max_score_tuple = &max_score_tuple_;
  }
  return ret;
}

int ObIDASSearchOp::do_next_rowids(int64_t capacity, int64_t &count)
{
  return OB_NOT_IMPLEMENT;
}

// default implementation for operators that can not propagate score to children
int ObIDASSearchOp::do_set_min_competitive_score(const double &threshold)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(threshold < min_competitive_score_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid threshold", K(ret), K(threshold), K_(min_competitive_score));
  } else {
    min_competitive_score_ = threshold;
  }
  return ret;
}

// default implementation for operators that collect max score from all children operators
int ObIDASSearchOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  double curr_threshold = 0.0;
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    double child_max_score = 0.0;
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr child", K(ret), K(i));
    } else if (OB_FAIL(children_[i]->calc_max_score(child_max_score))) {
      LOG_WARN("failed to calc max score", K(ret));
    } else {
      curr_threshold += child_max_score;
    }
  }
  if (OB_SUCC(ret)) {
    threshold = curr_threshold;
  }
  return ret;
}

int ObIDASSearchOp::get_related_tablet_id(const ObDASScalarScanCtDef *scalar_ctdef, common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(search_ctx_.get_related_tablet_id(scalar_ctdef, tablet_id))) {
    LOG_WARN("failed to get tablet id");
  }
  return ret;
}

void ObIDASSearchOp::switch_tablet_id(const common::ObTabletID &new_tablet_id, storage::ObTableScanParam &scan_param)
{
  const common::ObTabletID &old_tablet_id = scan_param.tablet_id_;
  scan_param.need_switch_param_ =
    scan_param.need_switch_param_ || ((old_tablet_id.is_valid() && old_tablet_id != new_tablet_id));
  scan_param.tablet_id_ = new_tablet_id;
}

int ObIDASSearchOp::init_children_ops_array(const ObIArray<ObIDASSearchOp *> &children)
{
  int ret = OB_SUCCESS;
  children_cnt_ = children.count();
  if (children_cnt_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected negative children count", KR(ret), K(children_cnt_));
  } else if (children_cnt_ == 0) {
    // do nothing
  } else if (OB_ISNULL(children_ = OB_NEW_ARRAY(ObIDASSearchOp *, &ctx_allocator(), children_cnt_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for children ops array", KR(ret), K(children_cnt_));
  } else {
    for (int64_t i = 0; i < children_cnt_; ++i) {
      children_[i] = children.at(i);
    }
  }
  return ret;
}

common::ObProfileId ObIDASSearchOp::get_profile_id() const
{
  common::ObProfileId profile_id = common::ObProfileId::PHY_INVALID;
  switch (op_type_) {
    case DAS_SEARCH_OP_CONJUNCTION:
      profile_id = common::ObProfileId::HYBRID_SEARCH_CONJUNCTION;
      break;
    case DAS_SEARCH_OP_DISJUNCTION:
      profile_id = common::ObProfileId::HYBRID_SEARCH_DISJUNCTION;
      break;
    case DAS_SEARCH_OP_DISJUNCTION_FILTER:
      profile_id = common::ObProfileId::HYBRID_SEARCH_DISJUNCTION_FILTER;
      break;
    case DAS_SEARCH_OP_REQ_OPT:
      profile_id = common::ObProfileId::HYBRID_SEARCH_REQ_OPT;
      break;
    case DAS_SEARCH_OP_REQ_EXCL:
      profile_id = common::ObProfileId::HYBRID_SEARCH_REQ_EXCL;
      break;
    case DAS_SEARCH_OP_SCALAR_INDEX_ROR:
      profile_id = common::ObProfileId::HYBRID_SEARCH_SCALAR_INDEX_ROR;
      break;
    case DAS_SEARCH_OP_SCALAR_PRIMARY_ROR:
      profile_id = common::ObProfileId::HYBRID_SEARCH_SCALAR_PRIMARY_ROR;
      break;
    case DAS_SEARCH_OP_SCALAR_SCAN:
      profile_id = common::ObProfileId::HYBRID_SEARCH_SCALAR_SCAN;
      break;
    case DAS_SEARCH_OP_BITMAP:
      profile_id = common::ObProfileId::HYBRID_SEARCH_BITMAP;
      break;
    case DAS_SEARCH_OP_SORT:
      profile_id = common::ObProfileId::HYBRID_SEARCH_SORT;
      break;
    case DAS_SEARCH_OP_TOPK_COLLECT:
      profile_id = common::ObProfileId::HYBRID_SEARCH_TOPK_COLLECT;
      break;
    case DAS_SEARCH_OP_DISJUNCTIVE_MAX:
      profile_id = common::ObProfileId::HYBRID_SEARCH_DISJUNCTIVE_MAX;
      break;
    case DAS_SEARCH_OP_TOKEN:
      profile_id = common::ObProfileId::HYBRID_SEARCH_TOKEN;
      break;
    case DAS_SEARCH_OP_MATCH_PHRASE:
      profile_id = common::ObProfileId::HYBRID_SEARCH_MATCH_PHRASE;
      break;
    case DAS_SEARCH_OP_DUMMY:
      profile_id = common::ObProfileId::HYBRID_SEARCH_DUMMY;
      break;
    case DAS_SEARCH_OP_BMW:
      profile_id = common::ObProfileId::HYBRID_SEARCH_BMW;
      break;
    case DAS_SEARCH_OP_BMM:
      profile_id = common::ObProfileId::HYBRID_SEARCH_BMM;
      break;
    default:
      profile_id = common::ObProfileId::PHY_INVALID;
      break;
  }
  return profile_id;
}

int ObIDASSearchOp::open_profile()
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *current = common::get_current_profile();
  if (OB_NOT_NULL(current)) {
    if (OB_FAIL(current->register_child(get_profile_id(), profile_))) {
      LOG_WARN("failed to register child profile", K(ret), "op_type", op_type_);
    } else {
      common::ObProfileSwitcher switcher(profile_);
      SET_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, 0);
      SET_METRIC_VAL(common::ObMetricId::HS_TOTAL_TIME, 0);
      SET_METRIC_VAL(common::ObMetricId::HS_OP_ID, op_id_);
    }
  }
  return ret;
}

void ObIDASSearchOp::close_profile()
{
  profile_ = nullptr;
}

void ObIDASSearchOp::reset_profile()
{
  profile_ = nullptr;
  for (int64_t i = 0; i < children_cnt_; ++i) {
    if (nullptr != children_[i]) {
      children_[i]->reset_profile();
    }
  }
}



} // namespace sql
} // namespace oceanbase
