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
 * This file is for define of func json_array_array_append
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_ARRAY_APPEND_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_ARRAY_APPEND_H_

#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonArrayAppend : public ObFuncExprOperator
{
public:
  explicit ObExprJsonArrayAppend(common::ObIAllocator &alloc);
  virtual ~ObExprJsonArrayAppend();

  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num, 
                                common::ObExprTypeCtx& type_ctx) const override;

  static int eval_json_array_append(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
    DISALLOW_COPY_AND_ASSIGN(ObExprJsonArrayAppend);
private:
  static const ObString name_;
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_ARRAY_APPEND_H_