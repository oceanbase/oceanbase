/**
 * Copyright 2014-2016 Alibaba Inc. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 *
 * Authors:
 *     zimiao<kaizhan.dkz@antgroup.com>
 */
#ifndef OCEANBASE_SQL_ENGINE_EXPR_COT_
#define OCEANBASE_SQL_ENGINE_EXPR_COT_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprCot : public ObFuncExprOperator
{
public:
  explicit  ObExprCot(common::ObIAllocator &alloc);
  virtual   ~ObExprCot();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &radian,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int calc_result1(common::ObObj &result,
                           const common::ObObj &radian_obj,
                           common::ObExprCtx &expr_ctx) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCot);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_COT_ */