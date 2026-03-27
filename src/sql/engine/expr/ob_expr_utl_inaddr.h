/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UTL_INADDR_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UTL_INADDR_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprUtlInaddrGetHostAddr : public ObExprOperator
{
public:
  explicit ObExprUtlInaddrGetHostAddr(common::ObIAllocator &alloc);
  virtual ~ObExprUtlInaddrGetHostAddr();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUtlInaddrGetHostAddr);
};

class ObExprUtlInaddrGetHostName : public ObExprOperator
{
public:
  explicit ObExprUtlInaddrGetHostName(common::ObIAllocator &alloc);
  virtual ~ObExprUtlInaddrGetHostName();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUtlInaddrGetHostName);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UTL_INADDR_ */
