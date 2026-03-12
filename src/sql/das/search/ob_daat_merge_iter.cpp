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
#include "ob_daat_merge_iter.h"

namespace oceanbase
{
namespace sql
{

ObDAATMergeIter::ObDAATMergeIter()
  : op_(nullptr),
    curr_id_(),
    curr_score_(0.0),
    curr_block_max_info_(nullptr),
    max_score_(0.0),
    iter_idx_(-1),
    in_shallow_status_(false),
    iter_end_(false),
    is_inited_(false)
{
}

int ObDAATMergeIter::init(const int64_t iter_idx, ObIDASSearchOp &op)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("iter already inited", K(ret));
  } else {
    iter_idx_ = iter_idx;
    op_ = &op;
    max_score_calculated_ = false;
    in_shallow_status_ = false;
    iter_end_ = false;
    is_inited_ = true;
  }
  return ret;
}

void ObDAATMergeIter::reuse()
{
  curr_id_.reset();
  curr_score_ = 0.0;
  curr_block_max_info_ = nullptr;
  max_score_calculated_ = false;
  max_score_ = 0.0;
  in_shallow_status_ = false;
  iter_end_ = false;
}

int ObDAATMergeIter::next()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(in_shallow_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter status, can not get next row after shallow advance",
        K(ret), K_(in_shallow_status));
  } else if (OB_FAIL(op_->next_rowid(curr_id_, curr_score_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to next rowid", K(ret));
    } else {
      iter_end_ = true;
    }
  }
  return ret;
}

int ObDAATMergeIter::advance_to(const ObDASRowID &target)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(op_->advance_to(target, curr_id_, curr_score_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance to", K(ret));
    } else {
      iter_end_ = true;
    }
  } else {
    in_shallow_status_ = false;
  }
  return ret;
}

int ObDAATMergeIter::advance_shallow(const ObDASRowID &target, const bool inclusive)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(op_->advance_shallow(target, inclusive, curr_block_max_info_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance shallow", K(ret));
    } else {
      iter_end_ = true;
    }
  } else {
    in_shallow_status_ = true;
  }
  return ret;
}

int ObDAATMergeIter::to_shallow()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (in_shallow_status_) {
    // skip
  } else if (OB_FAIL(op_->advance_shallow(curr_id_, true, curr_block_max_info_))) {
    LOG_WARN("failed to advance shallow", K(ret));
  } else {
    in_shallow_status_ = true;
  }
  return ret;
}

int ObDAATMergeIter::get_curr_id(ObDASRowID &curr_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    curr_id = curr_id_;
  }
  return ret;
}

int ObDAATMergeIter::get_curr_score(double &score) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(in_shallow_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter status, can not get curr score after shallow advance",
        K(ret), K_(in_shallow_status));
  } else {
    score = curr_score_;
  }
  return ret;
}

int ObDAATMergeIter::get_curr_block_max_info(const MaxScoreTuple *&max_score_tuple) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!in_shallow_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected  status", K(ret), K_(in_shallow_status));
  } else {
    max_score_tuple = curr_block_max_info_;
  }
  return ret;
}

int ObDAATMergeIter::get_max_score(double &max_score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (max_score_calculated_) {
    max_score = max_score_;
  } else if (OB_FAIL(op_->calc_max_score(max_score_))) {
    LOG_WARN("failed to calc max score", K(ret));
  } else {
    max_score = max_score_;
    max_score_calculated_ = true;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
