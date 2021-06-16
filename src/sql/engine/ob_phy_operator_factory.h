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

#ifndef OCEANBASE_SQL_OB_PHY_OPERATOR_FACTORY_H
#define OCEANBASE_SQL_OB_PHY_OPERATOR_FACTORY_H

#include "sql/engine/ob_phy_operator.h"
namespace oceanbase {
namespace sql {
class ObPhyOperatorFactory {
public:
  explicit ObPhyOperatorFactory(common::ObIAllocator& alloc) : alloc_(alloc)
  {}
  ~ObPhyOperatorFactory()
  {}
  int alloc(ObPhyOperatorType type, ObPhyOperator*& phy_op);
  // int free(ObPhyOperator *&phy_op);
  inline void destroy()
  {
    // nothing todo
    // All memory is released by alloc;
  }

private:
  template <typename ClassT, ObPhyOperatorType type>
  static int alloc(common::ObIAllocator& alloc, ObPhyOperator*& phy_op);

  typedef int (*AllocFunc)(common::ObIAllocator& alloc, ObPhyOperator*& phy_op);
  static AllocFunc PHY_OPERATOR_ALLOC[PHY_END];

private:
  common::ObIAllocator& alloc_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_PHY_OPERATOR_FACTORY_H */
