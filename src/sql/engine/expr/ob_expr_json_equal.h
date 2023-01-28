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
 * This file contains implementation for json_equal.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_EQUAL_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_EQUAL_H_

#include "lib/json_type/ob_json_tree.h"
#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonEqual : public ObFuncExprOperator
{
public:
  explicit ObExprJsonEqual(common::ObIAllocator &alloc);
  virtual ~ObExprJsonEqual();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types_stack,
                                int64_t param_num,
                                ObExprTypeCtx& type_ctx) const;
  static bool is_json_scalar(ObIJsonBase *ptr);
  static int eval_json_equal(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                    ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
  const static uint8_t OB_JSON_FALSE_ON_ERROR = 0;
  const static uint8_t OB_JSON_TRUE_ON_ERROR  = 1;
  const static uint8_t OB_JSON_ERROR_ON_ERROR = 2;
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonEqual);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_EQUAL_H_