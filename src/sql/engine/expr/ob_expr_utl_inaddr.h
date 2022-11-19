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
