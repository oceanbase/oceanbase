/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_AGG_PARAM_LIST_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_AGG_PARAM_LIST_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
//这个类仅仅是用来在后缀表达式计算里面做标记item的，不参与实际计算
class ObExprAggParamList : public ObFuncExprOperator
{
public:
  explicit  ObExprAggParamList(common::ObIAllocator &alloc);
  virtual ~ObExprAggParamList();
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprAggParamList);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_AGG_PARAM_LIST_ */

