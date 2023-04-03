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

