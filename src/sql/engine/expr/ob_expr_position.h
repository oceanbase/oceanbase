/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_EXPR_POSITION_H_
#define OB_EXPR_POSITION_H_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprPosition : public ObLocationExprOperator
{
public:
  explicit  ObExprPosition(common::ObIAllocator &alloc);
  virtual ~ObExprPosition();
private:
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprPosition);
};


}//end of namespace sql
}//end of namespace oceanbase

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_POSITION_H_ */
