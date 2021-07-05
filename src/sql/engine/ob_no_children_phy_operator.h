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

#ifndef _OB_NO_CHILDREN_PHY_OPERATOR_H
#define _OB_NO_CHILDREN_PHY_OPERATOR_H 1
#include "sql/engine/ob_phy_operator.h"
namespace oceanbase {
namespace sql {
class ObNoChildrenPhyOperator : public ObPhyOperator {
public:
  explicit ObNoChildrenPhyOperator(common::ObIAllocator& alloc) : ObPhyOperator(alloc)
  {}
  virtual ~ObNoChildrenPhyOperator()
  {}
  /// @note always return OB_NOT_SUPPORTED
  virtual int set_child(int32_t child_idx, ObPhyOperator& child_operator);
  virtual ObPhyOperator* get_child(int32_t child_idx) const;
  virtual int32_t get_child_num() const;
  virtual int accept(ObPhyOperatorVisitor& visitor) const;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObNoChildrenPhyOperator);
};

inline int ObNoChildrenPhyOperator::set_child(int32_t child_idx, ObPhyOperator& child_operator)
{
  UNUSED(child_idx);
  UNUSED(child_operator);
  return common::OB_NOT_SUPPORTED;
}

inline ObPhyOperator* ObNoChildrenPhyOperator::get_child(int32_t child_idx) const
{
  UNUSED(child_idx);
  return NULL;
}

inline int32_t ObNoChildrenPhyOperator::get_child_num() const
{
  return 0;
}

inline int ObNoChildrenPhyOperator::accept(ObPhyOperatorVisitor& visitor) const
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(visitor.pre_visit(*this))) {
    SQL_ENG_LOG(WARN, "fail to pre visit", K(*this));
  } else if (OB_FAIL(visitor.post_visit(*this))) {
    SQL_ENG_LOG(WARN, "fail to post visit", K(*this));
  } else {
    // done
  }
  return ret;
}
}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_NO_CHILDREN_PHY_OPERATOR_H */
