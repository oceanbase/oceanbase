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
#include "sql/engine/ob_multi_children_phy_operator.h"
#include "sql/engine/ob_physical_plan.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObMultiChildrenPhyOperator::ObMultiChildrenPhyOperator(common::ObIAllocator& alloc)
    : ObPhyOperator(alloc), child_array_(NULL), child_num_(0)
{}

ObMultiChildrenPhyOperator::~ObMultiChildrenPhyOperator()
{}

void ObMultiChildrenPhyOperator::reset()
{
  child_array_ = NULL;
  child_num_ = 0;
  ObPhyOperator::reset();
}

void ObMultiChildrenPhyOperator::reuse()
{
  child_array_ = NULL;
  child_num_ = 0;
  ObPhyOperator::reuse();
}

int ObMultiChildrenPhyOperator::set_child(int32_t child_idx, ObPhyOperator& child_operator)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(child_array_));
  CK(child_idx >= 0);
  CK(child_idx < child_num_);
  if (OB_SUCC(ret)) {
    child_array_[child_idx] = &child_operator;
    child_operator.set_parent(this);
  }
  return ret;
}

int ObMultiChildrenPhyOperator::create_child_array(int64_t child_op_size)
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

ObPhyOperator* ObMultiChildrenPhyOperator::get_child(int32_t child_idx) const
{
  ObPhyOperator* ret = NULL;
  if (OB_LIKELY(child_idx >= 0 && child_idx < child_num_)) {
    ret = child_array_[child_idx];
  }
  return ret;
}

int ObMultiChildrenPhyOperator::accept(ObPhyOperatorVisitor& visitor) const
{
  int ret = OB_SUCCESS;
  CK(child_num_ >= 0);
  // pre-visit
  OZ(visitor.pre_visit(*this));
  // visit children
  for (int64_t i = 0; OB_SUCC(ret) && i < child_num_; ++i) {
    CK(child_array_[i]);
    OZ(child_array_[i]->accept(visitor));
  }
  // post-visit
  OZ(visitor.post_visit(*this));

  return ret;
}

OB_SERIALIZE_MEMBER((ObMultiChildrenPhyOperator, ObPhyOperator), child_num_);
