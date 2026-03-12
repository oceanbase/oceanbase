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
    last_idx_ = 0;
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
    last_idx_ = 0;
  }
  return ret;
}

int ObDASConjunctionOp::do_close()
{
  int ret = OB_SUCCESS;
  last_idx_ = 0;
  return ret;
}

int ObDASConjunctionOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  ObDASRowID init_target;
  ObIDASSearchOp *op = nullptr;
  score = 0.0;
  curr_id.reset();
  if (OB_UNLIKELY(last_idx_ >= children_cnt_ || OB_ISNULL(op = children_[last_idx_]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null op", KR(ret), K(last_idx_), K(children_cnt_));
  } else if (OB_FAIL(op->advance_to(target, init_target, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to", KR(ret));
  } else if (OB_FAIL(inner_advance_to(init_target, curr_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to do advance to", KR(ret));
  }
  return ret;
}

int ObDASConjunctionOp::inner_advance_to(const ObDASRowID &target,
                                      ObDASRowID &curr_id,
                                      double &score)
{
  int ret = OB_SUCCESS;
  ObDASRowID candidate_target = target;
  double candidate_score = score;
  bool all_advanced = false;
  while (OB_SUCC(ret) && !all_advanced) {
    bool target_changed = false;
    for (int i = 0; OB_SUCC(ret) && i < children_cnt_ && !target_changed; ++i) {
      ObDASRowID tmp_id;
      double tmp_score = 0.0;
      int cmp = 0;
      ObIDASSearchOp *op = nullptr;
      if (last_idx_ == i) {
        // already advanced to the target
      } else if (OB_ISNULL(op = children_[i])) {
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
  if (OB_UNLIKELY(last_idx_ >= children_cnt_ || OB_ISNULL(op = children_[last_idx_]))) {
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
