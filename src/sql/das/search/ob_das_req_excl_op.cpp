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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/search/ob_das_req_excl_op.h"

namespace oceanbase
{
namespace sql
{

int ObDASReqExclOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(children.push_back(required_))) {
    LOG_WARN("failed to push required op", KR(ret), KPC(required_));
  } else if (OB_FAIL(children.push_back(excluded_))) {
    LOG_WARN("failed to push excluded op", KR(ret), KPC(excluded_));
  }
  return ret;
}

int ObDASReqExclOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASReqExclOpParam &req_excl_op_param = static_cast<const ObDASReqExclOpParam &>(op_param);
  if (OB_ISNULL(req_excl_op_param.get_required()) || OB_ISNULL(req_excl_op_param.get_excluded())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid required/excluded op", K(ret), K(req_excl_op_param.get_required()), K(req_excl_op_param.get_excluded()));
  } else {
    required_ = req_excl_op_param.get_required();
    excluded_ = req_excl_op_param.get_excluded();
  }
  return ret;
}

int ObDASReqExclOp::do_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is not valid", KR(ret));
  } else if (OB_FAIL(required_->open())) {
    LOG_WARN("failed to open required op", KR(ret));
  } else if (OB_FAIL(excluded_->open())) {
    LOG_WARN("failed to open excluded op", KR(ret));
  }
  return ret;
}

int ObDASReqExclOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is not valid", KR(ret));
  } else if (OB_FAIL(required_->rescan())) {
    LOG_WARN("failed to rescan required op", KR(ret));
  } else if (OB_FAIL(excluded_->rescan())) {
    LOG_WARN("failed to rescan excluded op", KR(ret));
  }
  return ret;
}

int ObDASReqExclOp::do_close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
  } else {
    required_ = nullptr;
    excluded_ = nullptr;
  }

  return ret;
}

int ObDASReqExclOp::check_excluded(const ObDASRowID &target_id, int &cmp)
{
  int ret = OB_SUCCESS;
  bool need_advance_excluded = true;
  ObDASRowID excluded_id;
  double excluded_score = 0.0;
  cmp = 0;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is not valid", KR(ret));
  } else if (OB_FAIL(excluded_->advance_to(target_id, excluded_id, excluded_score))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      cmp = -1;
    } else {
      LOG_WARN("failed to advance to excluded op", K(ret), K(target_id));
    }
  } else if (OB_FAIL(search_ctx_.compare_rowid(target_id, excluded_id, cmp))) {
    LOG_WARN("failed to compare rowid", K(ret), K(target_id), K(excluded_id));
  } else if (OB_UNLIKELY(cmp > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("excluded rowid is smaller than target rowid", K(ret), K(target_id), K(excluded_id));
  }
  return ret;
}

int ObDASReqExclOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is not valid", KR(ret));
  } else if (OB_FAIL(required_->advance_to(target, curr_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to required op", K(ret), K(target));
  } else {
    while (OB_SUCC(ret) && cmp == 0) {
      if (OB_FAIL(check_excluded(curr_id, cmp))) {
        LOG_WARN("failed to check excluded", K(ret), K(curr_id));
      } else if (cmp == 0) {
        if (OB_FAIL(required_->next_rowid(curr_id, score))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to next rowid required op", K(ret));
        }
      } else {
        LOG_TRACE("do_advance_to hit", K(target), K(curr_id), K(cmp));
      }
    }
  }
  return ret;
}

int ObDASReqExclOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is not valid", KR(ret));
  } else if (OB_FAIL(required_->advance_shallow(target, inclusive, max_score_tuple))) {
    LOG_WARN("failed to advance shallow required op", KR(ret), K(target));
  }
  return ret;
}

int ObDASReqExclOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is not valid", KR(ret));
  } else {
    while (OB_SUCC(ret) && cmp == 0) {
      if (OB_FAIL(required_->next_rowid(next_id, score))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to next rowid required op", K(ret));
      } else if (OB_FAIL(check_excluded(next_id, cmp))) {
        LOG_WARN("failed to check excluded", K(ret), K(next_id));
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase