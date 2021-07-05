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

#ifndef _OB_SINGLE_CHILD_PHY_OPERATOR_H
#define _OB_SINGLE_CHILD_PHY_OPERATOR_H 1
#include "sql/engine/ob_phy_operator.h"
namespace oceanbase {
namespace sql {
class ObSingleChildPhyOperator : public ObPhyOperator {
public:
  explicit ObSingleChildPhyOperator(common::ObIAllocator& alloc);
  virtual ~ObSingleChildPhyOperator();
  /// set the only one child
  virtual int set_child(int32_t child_idx, ObPhyOperator& child_operator);
  /// get the only one child
  virtual ObPhyOperator* get_child(int32_t child_idx) const;
  virtual int32_t get_child_num() const
  {
    return 1;
  }
  virtual void reset();
  virtual void reuse();
  virtual int accept(ObPhyOperatorVisitor& visitor) const;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSingleChildPhyOperator);

protected:
  // data members
  ObPhyOperator* child_op_;
};
}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_SINGLE_CHILD_PHY_OPERATOR_H */
