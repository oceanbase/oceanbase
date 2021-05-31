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
#include "sql/engine/ob_single_child_phy_operator.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObSingleChildPhyOperator::ObSingleChildPhyOperator(ObIAllocator& alloc) : ObPhyOperator(alloc), child_op_(NULL)
{}

ObSingleChildPhyOperator::~ObSingleChildPhyOperator()
{}

void ObSingleChildPhyOperator::reset()
{
  child_op_ = NULL;
  ObPhyOperator::reset();
}

void ObSingleChildPhyOperator::reuse()
{
  child_op_ = NULL;
  ObPhyOperator::reuse();
}

int ObSingleChildPhyOperator::set_child(int32_t child_idx, ObPhyOperator& child_operator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL != child_op_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("child_op_ already init", "op_type", get_type());
  } else if (OB_UNLIKELY(0 != child_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid child", K(child_idx));
  } else {
    child_op_ = &child_operator;
    child_operator.set_parent(this);
  }
  return ret;
}

ObPhyOperator* ObSingleChildPhyOperator::get_child(int32_t child_idx) const
{
  ObPhyOperator* ret = NULL;
  if (OB_LIKELY(0 == child_idx)) {
    ret = child_op_;
  }
  return ret;
}

int ObSingleChildPhyOperator::accept(ObPhyOperatorVisitor& visitor) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(visitor.pre_visit(*this))) {
    LOG_WARN("fail to pre-visit single child operator", K(*this));
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(child_op_)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(child_op_->accept(visitor))) {
      LOG_WARN("fail to visit child", K(*child_op_));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(visitor.post_visit(*this))) {
      LOG_WARN("fail to post-visit single child operator", K(*this));
    } else {
      // done
    }
  }

  return ret;
}
