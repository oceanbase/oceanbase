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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/search/ob_das_req_opt_op.h"

namespace oceanbase
{
namespace sql
{

ObDASReqOptOpParam::ObDASReqOptOpParam(ObIDASSearchOp *required, ObIDASSearchOp *optional, bool need_score)
  : ObIDASSearchOpParam(DAS_SEARCH_OP_REQ_OPT),
    required_(required),
    optional_(optional),
    need_score_(need_score)
{}

int ObDASReqOptOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(children.push_back(required_))) {
    LOG_WARN("failed to push required op", KR(ret), KPC(required_));
  } else if (OB_FAIL(children.push_back(optional_))) {
    LOG_WARN("failed to push optional op", KR(ret), KPC(optional_));
  }
  return ret;
}

ObDASReqOptOp::ObDASReqOptOp(ObDASSearchCtx &search_ctx)
  : ObIDASSearchOp(search_ctx),
    required_(nullptr),
    optional_(nullptr),
    required_max_score_(0.0),
    optional_max_score_(0.0),
    curr_opt_id_(),
    curr_opt_score_(0.0),
    is_conjunctive_(false),
    score_propagated_(false),
    max_score_calculated_(false),
    iter_end_(false),
    is_inited_(false)
{}

int ObDASReqOptOp::do_open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child", K(ret), K(i));
    } else if (OB_FAIL(children_[i]->open())) {
      LOG_WARN("failed to open child", K(ret));
    }
  }
  return ret;
}

int ObDASReqOptOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else {
    required_max_score_ = 0.0;
    optional_max_score_ = 0.0;
    curr_opt_id_.set_min();
    curr_opt_score_ = 0.0;
    is_conjunctive_ = false;
    score_propagated_ = false;
    max_score_calculated_ = false;
    iter_end_ = false;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child", K(ret), K(i));
    } else if (OB_FAIL(children_[i]->rescan())) {
      LOG_WARN("failed to rescan child", K(ret));
    }
  }
  return ret;
}

int ObDASReqOptOp::do_close()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  required_ = nullptr;
  optional_ = nullptr;
  iter_end_ = false;
  return ret;
}

int ObDASReqOptOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  ObDASRowID req_id;
  double req_score = 0.0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
    LOG_WARN("iter end", K(ret));
  } else if (OB_FAIL(required_->advance_to(target, req_id, req_score))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance to required op", K(ret));
    } else {
      iter_end_ = true;
    }
  } else if (OB_FAIL(inner_get_next_row(req_id, req_score, curr_id, score))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row", K(ret));
    } else {
      iter_end_ = true;
    }
  }
  return ret;
}

int ObDASReqOptOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  const MaxScoreTuple *req_max_score_tuple = nullptr;
  const MaxScoreTuple *opt_max_score_tuple = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(required_->advance_shallow(target, inclusive, req_max_score_tuple))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance shallow required op", K(ret));
    } else {
      iter_end_ = true;
    }
  } else if (OB_FAIL(optional_->advance_shallow(target, inclusive, opt_max_score_tuple))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance shallow optional op", K(ret));
    } else {
      ret = OB_SUCCESS;
      max_score_tuple = req_max_score_tuple;
    }
  } else {
    const ObDASRowID &min_id = req_max_score_tuple->get_min_id();
    int cmp_ret = 0;
    if (OB_FAIL(compare_rowid(req_max_score_tuple->get_max_id(), opt_max_score_tuple->get_min_id(), cmp_ret))) {
      LOG_WARN("failed to compare rowid", K(ret), K(req_max_score_tuple->get_max_id()), K(opt_max_score_tuple->get_min_id()));
    } else if (cmp_ret < 0) {
      const double max_score = req_max_score_tuple->get_max_score();
      max_score_tuple_.set(min_id, req_max_score_tuple->get_max_id(), max_score);
    } else if (OB_FAIL(compare_rowid(req_max_score_tuple->get_max_id(), opt_max_score_tuple->get_max_id(), cmp_ret))) {
      LOG_WARN("failed to compare rowid", K(ret), K(req_max_score_tuple->get_max_id()), K(opt_max_score_tuple->get_max_id()));
    } else {
      const ObDASRowID &max_id = cmp_ret > 0 ? opt_max_score_tuple->get_max_id() : req_max_score_tuple->get_max_id();
      const double max_score = req_max_score_tuple->get_max_score() + opt_max_score_tuple->get_max_score();
      max_score_tuple_.set(min_id, max_id, max_score);
    }

    if (OB_SUCC(ret)) {
      max_score_tuple = &max_score_tuple_;
    }
  }
  return ret;
}

int ObDASReqOptOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  ObDASRowID req_id;
  double req_score = 0.0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(required_->next_rowid(req_id, req_score))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to next rowid required op", K(ret));
    } else {
      iter_end_ = true;
    }
  } else if (OB_FAIL(inner_get_next_row(req_id, req_score, next_id, score))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row", K(ret));
    } else {
      iter_end_ = true;
    }
  }
  return ret;
}

int ObDASReqOptOp::do_set_min_competitive_score(const double &threshold)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(threshold < min_competitive_score_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid threshold", K(ret), K(threshold), K_(min_competitive_score));
  } else if (!max_score_calculated_) {
    double max_score = 0.0;
    if (OB_FAIL(calc_max_score(max_score))) {
      LOG_WARN("failed to calc max score", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (required_max_score_ + optional_max_score_ < threshold) {
    iter_end_ = true;
    LOG_TRACE("early termination with max score", K(threshold), K(required_max_score_), K(optional_max_score_));
  } else if (required_max_score_ < threshold) {
    is_conjunctive_ = true;
    if (0.0 == required_max_score_) {
      // propagate threshold to optional op
      if (OB_FAIL(optional_->set_min_competitive_score(threshold))) {
        LOG_WARN("failed to set min competitive score", K(ret));
      } else {
        score_propagated_ = true;
      }
    } else {
      min_competitive_score_ = threshold;
    }
  } else {
    min_competitive_score_ = threshold;
  }
  return ret;
}

int ObDASReqOptOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  double curr_max_score = 0.0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (max_score_calculated_) {
    // skip
  } else if (OB_FAIL(required_->calc_max_score(required_max_score_))) {
    LOG_WARN("failed to calc max score", K(ret));
  } else if (OB_FAIL(optional_->calc_max_score(optional_max_score_))) {
    LOG_WARN("failed to calc max score", K(ret));
  } else {
    max_score_calculated_ = true;
  }

  if (OB_SUCC(ret)) {
    curr_max_score = required_max_score_ + optional_max_score_;
    threshold = curr_max_score;
  }
  return ret;
}

int ObDASReqOptOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASReqOptOpParam &req_opt_op_param = static_cast<const ObDASReqOptOpParam &>(op_param);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(!req_opt_op_param.get_need_score())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("currently do not need req opt op when not scoring", K(ret), K(req_opt_op_param));
  } else if (OB_ISNULL(req_opt_op_param.get_required()) || OB_ISNULL(req_opt_op_param.get_optional())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid required/optional op", K(ret), KP(req_opt_op_param.get_required()), KP(req_opt_op_param.get_optional()));
  } else {
    min_competitive_score_ = 0.0;
    required_ = req_opt_op_param.get_required();
    optional_ = req_opt_op_param.get_optional();
    required_max_score_ = 0.0;
    optional_max_score_ = 0.0;
    curr_opt_id_.set_min();
    curr_opt_score_ = 0.0;
    is_conjunctive_ = false;
    score_propagated_ = false;
    max_score_calculated_ = false;
    iter_end_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObDASReqOptOp::inner_get_next_row(
    const ObDASRowID &req_id,
    const double &req_score,
    ObDASRowID &next_id,
    double &score)
{
  int ret = OB_SUCCESS;
  if (score_propagated_) {
    if (OB_FAIL(inner_get_next_row_with_score_propagated(req_id, req_score, next_id, score))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row with score propagated", K(ret));
      }
    }
  } else if (req_score > min_competitive_score_ || 0 == min_competitive_score_) {
    //get next row directly
    if (OB_FAIL(inner_get_next_row_from_req(req_id, req_score, next_id, score))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row from req", K(ret));
      }
    }
  } else if (OB_FAIL(inner_get_next_row_with_block_max_pruning(req_id, req_score, next_id, score))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row with block max pruning", K(ret));
    }
  }
  return ret;
}

int ObDASReqOptOp::inner_get_next_row_from_req(
    const ObDASRowID &req_id,
    const double &req_score,
    ObDASRowID &next_id,
    double &score)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (OB_FAIL(compare_rowid(req_id, curr_opt_id_, cmp_ret))) {
    LOG_WARN("failed to compare rowid", K(ret), K(req_id), K(curr_opt_id_));
  } else if (cmp_ret == 0) {
    score = req_score + curr_opt_score_;
    next_id = req_id;
  } else if (cmp_ret < 0) {
    next_id = req_id;
    score = req_score;
  } else if (OB_FAIL(optional_->advance_to(req_id, curr_opt_id_, curr_opt_score_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
    LOG_WARN("failed to advance to optional op", K(ret));
    } else {
      ret = OB_SUCCESS;
      score = req_score;
      next_id = req_id;
    }
  } else if (OB_FAIL(compare_rowid(req_id, curr_opt_id_, cmp_ret))) {
    LOG_WARN("failed to compare rowid", K(ret), K(req_id), K(curr_opt_id_));
  } else if (cmp_ret == 0) {
    score = req_score + curr_opt_score_;
    next_id = req_id;
  } else if (OB_UNLIKELY(cmp_ret > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cmp ret", K(ret), K(cmp_ret), K(req_id), K(curr_opt_id_));
  } else {
    score = req_score;
    next_id = req_id;
  }
  return ret;
}

int ObDASReqOptOp::inner_get_next_row_with_score_propagated(
    const ObDASRowID &req_id,
    const double &req_score,
    ObDASRowID &next_id,
    double &score)
{
  int ret = OB_SUCCESS;
  bool found_row = false;
  ObDASRowID curr_req_id = req_id;
  double curr_req_score = req_score;
  while (OB_SUCC(ret) && !found_row) {
    int cmp_ret = 0;
    if (OB_FAIL(optional_->advance_to(curr_req_id, curr_opt_id_, curr_opt_score_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance to optional op", K(ret));
      }
    } else if (OB_FAIL(compare_rowid(curr_req_id, curr_opt_id_, cmp_ret))) {
      LOG_WARN("failed to compare rowid", K(ret), K(curr_req_id), K(curr_opt_id_));
    } else if (cmp_ret == 0) {
      found_row = true;
      score = curr_opt_score_;
      next_id = curr_req_id;
    } else if (OB_UNLIKELY(cmp_ret > 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected cmp ret", K(ret), K(cmp_ret), K(curr_req_id), K_(curr_opt_id));
    } else if (OB_FAIL(required_->advance_to(curr_opt_id_, curr_req_id, curr_req_score))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance to required op", K(ret));
      }
    }
  }
  return ret;
}

int ObDASReqOptOp::inner_get_next_row_with_block_max_pruning(
    const ObDASRowID &req_id,
    const double &req_score,
    ObDASRowID &next_id,
    double &score)
{
  int ret = OB_SUCCESS;
  // TODO: optimize with block max pruning
  bool found_row = false;
  ObDASRowID curr_req_id = req_id;
  double curr_req_score = req_score;
  while (OB_SUCC(ret) && !found_row) {
    int cmp_ret = 0;
    double curr_score = 0.0;
    bool same_id = false;
    if (OB_FAIL(compare_rowid(curr_req_id, curr_opt_id_, cmp_ret))) {
      LOG_WARN("failed to compare rowid", K(ret), K(curr_req_id), K(curr_opt_id_));
    } else if (cmp_ret == 0) {
      curr_score = curr_req_score + curr_opt_score_;
      same_id = true;
    } else if (cmp_ret < 0) {
      curr_score = curr_req_score;
    } else if (OB_FAIL(optional_->advance_to(curr_req_id, curr_opt_id_, curr_opt_score_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance to optional op", K(ret));
      } else if (is_conjunctive_) {
        // early termination
      } else {
        ret = OB_SUCCESS;
        curr_score = curr_req_score;
      }
    } else if (OB_FAIL(compare_rowid(curr_req_id, curr_opt_id_, cmp_ret))) {
      LOG_WARN("failed to compare rowid", K(ret), K(curr_req_id), K(curr_opt_id_));
    } else if (cmp_ret == 0) {
      curr_score = curr_req_score + curr_opt_score_;
      same_id = true;
    }

    if (OB_FAIL(ret)) {
    } else if (curr_score > min_competitive_score_ && (!is_conjunctive_ || same_id)) {
      found_row = true;
      score = curr_score;
      next_id = curr_req_id;
    } else if (OB_FAIL(required_->next_rowid(curr_req_id, curr_req_score))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next rowid required op", K(ret));
      }
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
