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
#include "ob_das_disjunction_filter_op.h"
#include "sql/das/ob_das_def_reg.h"

namespace oceanbase
{
namespace sql
{

int ObDASDisjunctionFilterOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(optional_ops_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("optional ops is null", KR(ret));
  } else if (OB_FAIL(children.assign(*optional_ops_))) {
    LOG_WARN("failed to assign optional ops", KR(ret));
  }
  return ret;
}

int ObDASDisjunctionFilterOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASDisjunctionFilterOpParam &disjunction_filter_op_param =
    static_cast<const ObDASDisjunctionFilterOpParam &>(op_param);
  const ObIArray<ObIDASSearchOp *> *optional_ops = disjunction_filter_op_param.get_optional_ops();
  if (OB_ISNULL(optional_ops)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr optional ops", KR(ret));
  } else if (OB_UNLIKELY(disjunction_filter_op_param.get_minimum_should_match() > optional_ops->count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("minimum should match must be less than the number of should clauses", KR(ret),
      K(disjunction_filter_op_param.get_minimum_should_match()), KPC(optional_ops));
  } else {
    lead_cost_ = disjunction_filter_op_param.get_lead_cost();
    min_should_match_ = disjunction_filter_op_param.get_minimum_should_match();
  }
  return ret;
}

int ObDASDisjunctionFilterOp::do_open()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child", KR(ret));
    } else if (OB_FAIL(children_[i]->open())) {
      LOG_WARN("failed to open child", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    cur_row_id_.set_min();
    if (OB_FAIL(child_row_ids_.prepare_allocate(children_cnt_, cur_row_id_))) {
      LOG_WARN("failed to prepare allocate cur row ids", KR(ret));
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::do_rescan()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child", KR(ret));
    } else if (OB_FAIL(children_[i]->rescan())) {
      LOG_WARN("failed to rescan", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    cur_row_id_.set_min();
    for (int64_t i = 0; i < child_row_ids_.count(); ++i) {
      child_row_ids_[i].set_min();
    }
  }
  return ret;
}

int ObDASDisjunctionFilterOp::do_close()
{
  int ret = OB_SUCCESS;
  cur_row_id_.reset();
  child_row_ids_.reset();
  return ret;
}

int ObDASDisjunctionFilterOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  ObDASRowID candidate_target;
  ObDASRowID next_possible_target = target;
  int cmp = 0;
  bool enough_matched = false;
  while (OB_SUCC(ret) && !enough_matched) {
    ObDASRowID tmp_id;
    double tmp_score = 0.0;
    int64_t matched_count = 0;
    enough_matched = false;
    candidate_target = next_possible_target;
    next_possible_target.set_max();
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      ObIDASSearchOp *op = children_[i];
      if (OB_ISNULL(op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null child", KR(ret), K(i));
      } else if (OB_FAIL(compare_rowid(child_row_ids_[i], candidate_target, cmp))) {
        LOG_WARN("failed to compare rowids", KR(ret), K(i), K(candidate_target), K(child_row_ids_[i]));
      } else if (cmp >= 0) {
        // already advanced to the candidate_target in the previous iteration, do nothing
      } else if (FALSE_IT(child_row_ids_[i].reset())) {
      } else if (OB_FAIL(op->advance_to(candidate_target, tmp_id, tmp_score))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          child_row_ids_[i].set_max();
        } else {
          LOG_WARN("failed to advance to", KR(ret), K(candidate_target));
        }
      } else {
        child_row_ids_[i] = tmp_id;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(compare_rowid(child_row_ids_[i], candidate_target, cmp))) {
          LOG_WARN("failed to compare rowids", KR(ret), K(i), K(child_row_ids_[i]), K(candidate_target));
        } else if (cmp != 0) {
          if (cmp < 0) {
            // result should not be less than the target
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected child_row_ids_[i] < candidate_target", KR(ret), K(i), K(child_row_ids_[i]), K(candidate_target));
          } else {
            if (OB_FAIL(compare_rowid(next_possible_target, child_row_ids_[i], cmp))) {
              LOG_WARN("failed to compare rowids", KR(ret), K(next_possible_target), K(child_row_ids_[i]));
            } else if (cmp > 0) {
              next_possible_target = child_row_ids_[i];
            }
          }
        } else {
          matched_count++;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (matched_count >= min_should_match_) {
      enough_matched = true;
    } else if (next_possible_target.is_max()) {
      ret = OB_ITER_END;
    }
  }
  if (OB_SUCC(ret)) {
    cur_row_id_ = candidate_target;
    curr_id = candidate_target;
    // disjunction_filter does not support score
    score = 0.0;
  }
  return ret;
}

int ObDASDisjunctionFilterOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  ObDASRowID target_id;
  target_id.set_max();
  int cmp = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    ObIDASSearchOp *op = children_[i];
    ObDASRowID tmp_id;
    double tmp_score = 0.0;
    if (OB_ISNULL(op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null op", KR(ret), K(i));
    } else if (OB_FAIL(compare_rowid(child_row_ids_[i], cur_row_id_, cmp))) {
      LOG_WARN("failed to compare rowids", KR(ret), K(i), K(child_row_ids_[i]), K(cur_row_id_));
    } else if (cmp == 0) {
      child_row_ids_[i].reset();
      if (OB_FAIL(op->next_rowid(tmp_id, tmp_score))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          child_row_ids_[i].set_max();
        } else {
          LOG_WARN("failed to next rowid", KR(ret), K(i));
        }
      } else {
        child_row_ids_[i] = tmp_id;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(compare_rowid(target_id, child_row_ids_[i], cmp))) {
        LOG_WARN("failed to compare rowids", KR(ret), K(target_id), K(child_row_ids_[i]));
      } else if (cmp > 0) {
        target_id = child_row_ids_[i];
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (target_id.is_max()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(do_advance_to(target_id, next_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to", KR(ret), K(target_id));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase