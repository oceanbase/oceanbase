/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_conjunction_op.h"
#include "sql/das/ob_das_def_reg.h"
#include "storage/tx_storage/ob_access_service.h"
#include "sql/das/search/ob_das_search_context.h"

namespace oceanbase
{
namespace sql
{

int ObDASConjunctionOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(required_ops_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("required ops is null", KR(ret));
  } else if (OB_FAIL(children.assign(*required_ops_))) {
    LOG_WARN("failed to assign required ops", KR(ret));
  }
  return ret;
}

int ObDASConjunctionOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASConjunctionOpParam &conjunction_op_param = static_cast<const ObDASConjunctionOpParam &>(op_param);
  const ObIArray<ObIDASSearchOp *> *required_ops = conjunction_op_param.get_required_ops();
  if (OB_ISNULL(required_ops)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr required ops", KR(ret));
  } else if (required_ops->empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty required ops", KR(ret));
  } else {
    last_idx_ = 0;
  }
  return ret;
}

int ObDASConjunctionOp::classify_children()
{
  int ret = OB_SUCCESS;
  probe_ops_.reset();
  driver_ops_.reset();
  has_probe_child_ = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    ObIDASSearchOp *op = children_[i];
    if (OB_ISNULL(op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null op", KR(ret), K(i));
    } else if (op->is_probe_mode()) {
      if (OB_FAIL(probe_ops_.push_back(op))) {
        LOG_WARN("failed to push back probe op", KR(ret), K(i));
      }
    } else if (OB_FAIL(driver_ops_.push_back(op))) {
      LOG_WARN("failed to push back driver op", KR(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    if (driver_ops_.count() + probe_ops_.count() != children_cnt_ || driver_ops_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected children count", KR(ret), K(driver_ops_.count()), K(probe_ops_.count()), K(children_cnt_));
    } else if (probe_ops_.count() > 0) {
      has_probe_child_ = true;
    } else {
      has_probe_child_ = false;
    }
    last_idx_ = 0;
  }
  LOG_TRACE("classify children", KR(ret), K(driver_ops_.count()), K(probe_ops_.count()),
    K(children_cnt_), K(has_probe_child_));
  return ret;
}

int ObDASConjunctionOp::do_open()
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    ObIDASSearchOp *op = children_[i];
    if (OB_ISNULL(op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null op", KR(ret));
    } else if (OB_FAIL(op->open())) {
      LOG_WARN("failed to open", KR(ret), K(*op));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(classify_children())) {
      LOG_WARN("failed to classify children", KR(ret));
    }
  }
  return ret;
}

int ObDASConjunctionOp::do_rescan()
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    ObIDASSearchOp *op = children_[i];
    if (OB_ISNULL(op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null op", KR(ret));
    } else if (OB_FAIL(op->rescan())) {
      LOG_WARN("failed to rescan", KR(ret), K(*op));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(classify_children())) {
      LOG_WARN("failed to classify children", KR(ret));
    }
  }
  return ret;
}

int ObDASConjunctionOp::do_close()
{
  int ret = OB_SUCCESS;
  last_idx_ = 0;
  has_probe_child_ = false;
  probe_ops_.reset();
  driver_ops_.reset();
  return ret;
}

int ObDASConjunctionOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  ObDASRowID init_target;
  ObIDASSearchOp *op = nullptr;
  score = 0.0;
  curr_id.reset();
  if (OB_UNLIKELY(last_idx_ >= driver_ops_.count() || OB_ISNULL(op = driver_ops_.at(last_idx_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null op", KR(ret), K(last_idx_), K(driver_ops_.count()));
  } else if (OB_FAIL(op->advance_to(target, init_target, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to", KR(ret));
  } else if (OB_FAIL(inner_advance_to(init_target, curr_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to do advance to", KR(ret));
  }
  return ret;
}

int ObDASConjunctionOp::inner_advance_to(
    const ObDASRowID &target,
    ObDASRowID &curr_id,
    double &score)
{
  int ret = OB_SUCCESS;
  if (!has_probe_child_) {
    ret = advance_driver_ops_to(target, curr_id, score);
  } else {
    ret = inner_advance_to_with_probe(target, curr_id, score);
  }
  return ret;
}

int ObDASConjunctionOp::inner_advance_to_with_probe(
    const ObDASRowID &target,
    ObDASRowID &curr_id,
    double &score)
{
  int ret = OB_SUCCESS;
  // Probe-coordination path: separate the work into two phases per iteration.
  //   Phase 1: advance all non-probe children (the driver + any zig-zag siblings) to
  //            converge on a stable candidate_target via the standard zig-zag.
  //   Phase 2: probe every probe-capable child against the stable candidate. If all
  //            hit, candidate is the result. If any miss, advance the driver past the
  //            candidate and retry both phases at the new candidate.
  // Probe children do not contribute to the score.
  bool got_result = false;
  ObDASRowID candidate_target = target;
  ObDASRowID candidate_curr_id = curr_id;
  double candidate_score = score;
  while (OB_SUCC(ret) && !got_result) {
    if (OB_FAIL(advance_driver_ops_to(candidate_target, candidate_curr_id, candidate_score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to advance driver ops to", KR(ret), K(candidate_target));
    } else {
      bool all_hit = true;
      for (int i = 0; OB_SUCC(ret) && i < probe_ops_.count() && all_hit; ++i) {
        ObIDASSearchOp *op = probe_ops_.at(i);
        bool hit = false;
        if (OB_ISNULL(op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null op", KR(ret), K(i));
        } else if (OB_FAIL(op->probe(candidate_curr_id, hit))) {
          LOG_WARN("failed to probe", KR(ret), K(i), K(target));
        } else if (!hit) {
          all_hit = false;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (all_hit) {
        got_result = true;
      } else {
        // continue to the next iteration
        if (OB_UNLIKELY(last_idx_ < 0 || last_idx_ >= driver_ops_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected last index", KR(ret), K(last_idx_), K(driver_ops_.count()));
        } else {
          ObIDASSearchOp *driver_op = driver_ops_.at(last_idx_);
          if (OB_ISNULL(driver_op)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null driver op", KR(ret), K(last_idx_));
          } else if (OB_FAIL(driver_op->next_rowid(candidate_target, candidate_score))) {
            LOG_WARN_IGNORE_ITER_END(ret, "failed to next rowid", KR(ret), K(last_idx_));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    curr_id = candidate_curr_id;
    score = candidate_score;
  }
  return ret;
}

int ObDASConjunctionOp::advance_driver_ops_to(
    const ObDASRowID &target,
    ObDASRowID &curr_id,
    double &score)
{
  int ret = OB_SUCCESS;
  ObDASRowID candidate_target = target;
  double candidate_score = score;
  bool all_advanced = false;
  while (OB_SUCC(ret) && !all_advanced) {
    bool target_changed = false;
    for (int i = 0; OB_SUCC(ret) && i < driver_ops_.count() && !target_changed; ++i) {
      ObDASRowID tmp_id;
      double tmp_score = 0.0;
      int cmp = 0;
      ObIDASSearchOp *op = nullptr;
      if (last_idx_ == i) {
        // already advanced to the target
      } else if (OB_ISNULL(op = driver_ops_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null op", KR(ret), K(i));
      } else if (OB_FAIL(op->advance_to(candidate_target, tmp_id, tmp_score))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to", KR(ret), K(i), K(candidate_target));
      } else if (OB_FAIL(compare_rowid(candidate_target, tmp_id, cmp))) {
        LOG_WARN("failed to compare rowids", KR(ret), K(candidate_target), K(tmp_id));
      } else if (cmp != 0) {
        if (cmp > 0) {
          // result should not be less than the target
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected candidate target > tmp id", KR(ret), K(i), K(candidate_target), K(tmp_id));
        } else {
          // this child advanced to a greater target, we should start a new round of iteration
          candidate_target = tmp_id;
          candidate_score = tmp_score;
          last_idx_ = i;
          target_changed = true;
        }
      } else {
        candidate_score += tmp_score;
      }
    }
    if (OB_SUCC(ret) && !target_changed) {
      // all children advanced to the candidate_target
      all_advanced = true;
    }
  }
  if (OB_SUCC(ret)) {
    curr_id = candidate_target;
    score = candidate_score;
  }
  return ret;
}

int ObDASConjunctionOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  ObDASRowID init_target;
  ObIDASSearchOp *op = nullptr;
  score = 0.0;
  next_id.reset();
  if (OB_UNLIKELY(last_idx_ >= driver_ops_.count() || OB_ISNULL(op = driver_ops_.at(last_idx_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null op", KR(ret), K(last_idx_));
  } else if (OB_FAIL(op->next_rowid(init_target, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to next rowid", KR(ret), K(last_idx_));
  } else if (OB_FAIL(inner_advance_to(init_target, next_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to do advance to", KR(ret), K(init_target));
  }
  return ret;
}

int ObDASConjunctionOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  // Probe-capable children degenerate to a point range with score 0 in advance_shallow,
  // so they can be summed/intersected together with driver children without special handling.
  ObDASRowID maximum_min_id;
  ObDASRowID minimal_max_id;
  ObDASRowID candidate_target = target;
  double max_score = 0.0;
  int cmp = 0;
  while (OB_SUCC(ret)) {
    max_score = 0.0;
    maximum_min_id.set_min();
    minimal_max_id.set_max();
    const MaxScoreTuple *tmp_max_score_tuple = nullptr;
    for (int i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      ObIDASSearchOp *op = children_[i];
      if (OB_ISNULL(op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null op", KR(ret), K(i));
      } else if (OB_FAIL(op->advance_shallow(candidate_target, inclusive, tmp_max_score_tuple))) {
        LOG_WARN("failed to advance shallow", KR(ret), K(i), K(candidate_target), K(inclusive));
      } else if (OB_ISNULL(tmp_max_score_tuple)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null max score tuple", KR(ret), K(i), K(candidate_target));
      } else {
        max_score += tmp_max_score_tuple->get_max_score();
        if (OB_FAIL(compare_rowid(maximum_min_id, tmp_max_score_tuple->get_min_id(), cmp))) {
          LOG_WARN("failed to compare rowids", KR(ret), K(i), K(maximum_min_id), K(tmp_max_score_tuple->get_min_id()));
        } else if (cmp < 0) {
          maximum_min_id = tmp_max_score_tuple->get_min_id();
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(compare_rowid(minimal_max_id, tmp_max_score_tuple->get_max_id(), cmp))) {
          LOG_WARN("failed to compare rowids", KR(ret), K(i), K(minimal_max_id), K(tmp_max_score_tuple->get_max_id()));
        } else if (cmp > 0) {
          minimal_max_id = tmp_max_score_tuple->get_max_id();
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(compare_rowid(maximum_min_id, minimal_max_id, cmp))) {
      LOG_WARN("failed to compare rowids", KR(ret), K(maximum_min_id), K(minimal_max_id));
    } else if (cmp > 0) {
      // all children's ranges should intersect, if not, we should try a new target
      candidate_target = maximum_min_id;
    } else {
      // all children's ranges intersect, we can return the result
      break;
    }
  }
  if (OB_SUCC(ret)) {
    max_score_tuple_.set(maximum_min_id, minimal_max_id, max_score);
    max_score_tuple = &max_score_tuple_;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
