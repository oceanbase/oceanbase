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
#include "sql/engine/ob_double_children_phy_operator.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObDoubleChildrenPhyOperator::ObDoubleChildrenPhyOperator(ObIAllocator& alloc)
    : ObPhyOperator(alloc), left_op_(NULL), right_op_(NULL)
{}

ObDoubleChildrenPhyOperator::~ObDoubleChildrenPhyOperator()
{}

void ObDoubleChildrenPhyOperator::reset()
{
  left_op_ = NULL;
  right_op_ = NULL;
  ObPhyOperator::reset();
}

void ObDoubleChildrenPhyOperator::reuse()
{
  left_op_ = NULL;
  right_op_ = NULL;
  ObPhyOperator::reuse();
}

int ObDoubleChildrenPhyOperator::set_child(int32_t child_idx, ObPhyOperator& child_operator)
{
  int ret = OB_SUCCESS;
  if (0 == child_idx) {
    if (OB_UNLIKELY(NULL != left_op_)) {
      ret = OB_INIT_TWICE;
      LOG_ERROR("left operator already init");
    } else {
      left_op_ = &child_operator;
    }
  } else if (1 == child_idx) {
    if (OB_UNLIKELY(NULL != right_op_)) {
      ret = OB_INIT_TWICE;
      LOG_ERROR("right operator already init");
    } else {
      right_op_ = &child_operator;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid child idx", K(child_idx));
  }
  if (OB_SUCC(ret)) {
    child_operator.set_parent(this);
  }
  return ret;
}

ObPhyOperator* ObDoubleChildrenPhyOperator::get_child(int32_t child_idx) const
{
  ObPhyOperator* ret = NULL;
  if (0 == child_idx) {
    ret = left_op_;
  } else if (1 == child_idx) {
    ret = right_op_;
  }
  return ret;
}

int ObDoubleChildrenPhyOperator::accept(ObPhyOperatorVisitor& visitor) const
{
  int ret = OB_SUCCESS;

  // pre-visit
  if (OB_FAIL(visitor.pre_visit(*this))) {
    LOG_WARN("fail to pre-visit", K(*this));
  }

  // visit children
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(left_op_) || OB_ISNULL(right_op_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(left_op_), K(right_op_));
    } else if (OB_FAIL(left_op_->accept(visitor))) {
      LOG_WARN("fail to accept left op", K(*left_op_));
    } else if (OB_FAIL(right_op_->accept(visitor))) {
      LOG_WARN("fail to accept left op", K(*right_op_));
    } else {
      // done
    }
  }

  // post-visit
  if (OB_SUCC(ret)) {
    if (OB_FAIL(visitor.post_visit(*this))) {
      LOG_WARN("fail to post-visit", K(*this));
    }
  }

  return ret;
}
