/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_MERGING_FROZEN_TIME_H_
#define OCEANBASE_SQL_OB_EXPR_MERGING_FROZEN_TIME_H_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprMergingFrozenTime : public ObFuncExprOperator
{
public:
  explicit  ObExprMergingFrozenTime(common::ObIAllocator &alloc);
  virtual ~ObExprMergingFrozenTime();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMergingFrozenTime);
};
}
}
#endif //OCEANBASE_SQL_OB_EXPR_MERGING_FROZEN_TIME_H
