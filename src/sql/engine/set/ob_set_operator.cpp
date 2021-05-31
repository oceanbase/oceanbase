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
#include "ob_set_operator.h"

namespace oceanbase {
namespace sql {

ObSetOperator::ObSetOperator(common::ObIAllocator& alloc)
    : ObPhyOperator(alloc), distinct_(false), cs_types_(alloc), child_num_(0), child_array_(NULL)
{}

ObSetOperator::~ObSetOperator()
{}

void ObSetOperator::reset()
{
  distinct_ = false;
  cs_types_.reset();
  child_num_ = 0;
  child_array_ = NULL;
  ObPhyOperator::reset();
}

void ObSetOperator::reuse()
{
  distinct_ = false;
  cs_types_.reuse();
  child_num_ = 0;
  child_array_ = NULL;
  ObPhyOperator::reuse();
}

int ObSetOperator::init(int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_collation_types(count))) {
    LOG_WARN("failed to init_collation_types", K(count), K(ret));
  }
  return ret;
}

void ObSetOperator::set_distinct(bool is_distinct)
{
  distinct_ = is_distinct;
}

int ObSetOperator::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(inner_create_operator_ctx(ctx, op_ctx))) {
    LOG_WARN("inner create operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, true /* need create cells */))) {
    LOG_WARN("init current row failed", K(ret));
  }
  return ret;
}

int ObSetOperator::create_child_array(int64_t child_op_size)
{
  int ret = OB_SUCCESS;
  if (child_op_size > 0) {
    void* ptr = NULL;
    size_t buf_size = static_cast<size_t>(child_op_size * sizeof(ObPhyOperator*));
    CK(OB_NOT_NULL(my_phy_plan_));
    if (OB_ISNULL(ptr = my_phy_plan_->get_allocator().alloc(buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("create child array failed", K(ret), K(child_op_size));
    } else {
      memset(ptr, 0, buf_size);
      child_array_ = static_cast<ObPhyOperator**>(ptr);
      child_num_ = static_cast<int32_t>(child_op_size);
    }
  }
  return ret;
}

ObPhyOperator* ObSetOperator::get_child(int32_t child_idx) const
{
  ObPhyOperator* ret = NULL;
  if (OB_LIKELY(child_idx >= 0 && child_idx < child_num_)) {
    ret = child_array_[child_idx];
  }
  return ret;
}

int ObSetOperator::set_child(int32_t child_idx, ObPhyOperator& child_operator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(child_array_));
  } else if (OB_UNLIKELY(child_idx < 0 || child_idx >= child_num_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid child idx", K(child_idx));
  } else {
    child_array_[child_idx] = &child_operator;
    child_operator.set_parent(this);
  }
  return ret;
}

int ObSetOperator::accept(ObPhyOperatorVisitor& visitor) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(child_num_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(child_num_));
  } else if (OB_FAIL(visitor.pre_visit(*this))) {  // pre-visit
    LOG_WARN("fail to pre-visit", K(*this));
  }

  // visit children
  for (int64_t i = 0; OB_SUCC(ret) && i < child_num_; ++i) {
    if (OB_ISNULL(child_array_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(child_array_[i]));
    } else if (OB_FAIL(child_array_[i]->accept(visitor))) {
      LOG_WARN("fail to accept left op", K(*child_array_[i]));
    }
  }

  // post-visit
  if (OB_SUCC(ret) && OB_FAIL(visitor.post_visit(*this))) {
    LOG_WARN("fail to post-visit", K(*this));
  }

  return ret;
}

}  // namespace sql
}  // end namespace oceanbase
