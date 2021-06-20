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

#ifndef OCEANBASE_SQL_OB_MULTI_CHILDREN_PHY_OPERATOR_H_
#define OCEANBASE_SQL_OB_MULTI_CHILDREN_PHY_OPERATOR_H_
#include "sql/engine/ob_phy_operator.h"
#include "lib/container/ob_se_array.h"
#include "sql/ob_sql_define.h"
namespace oceanbase {
namespace sql {
class ObMultiChildrenPhyOperator : public ObPhyOperator {
  OB_UNIS_VERSION_V(1);

public:
  explicit ObMultiChildrenPhyOperator(common::ObIAllocator& alloc);
  virtual ~ObMultiChildrenPhyOperator();
  /// multi children
  virtual int set_child(int32_t child_idx, ObPhyOperator& child_operator) override;
  virtual int create_child_array(int64_t child_op_size) override;
  /// get the only one child
  virtual ObPhyOperator* get_child(int32_t child_idx) const override;
  virtual int32_t get_child_num() const override
  {
    return child_num_;
  }
  virtual void reset() override;
  virtual void reuse() override;
  virtual int accept(ObPhyOperatorVisitor& visitor) const override;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMultiChildrenPhyOperator);

protected:
  ObPhyOperator** child_array_;
  int32_t child_num_;
};
}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_MULTI_CHILDREN_PHY_OPERATOR_H_ */
