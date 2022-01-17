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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_NULLIF_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_NULLIF_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
class ObExprNullif : public ObFuncExprOperator {
public:
  explicit ObExprNullif(common::ObIAllocator& alloc);
  virtual ~ObExprNullif(){};

  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num, common::ObExprCtx& expr_ctx) const;
  virtual int cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int eval_nullif(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res);
  void set_first_param_flag(bool flag)
  {
    first_param_can_be_null_ = flag;
  }

protected:
  bool first_param_can_be_null_;

private:
  int deduce_type(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;
  int se_deduce_type(ObExprResType& type, ObExprResType& cmp_type, const ObExprResType& type1,
      const ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;
  DISALLOW_COPY_AND_ASSIGN(ObExprNullif);
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_NULLIF_
