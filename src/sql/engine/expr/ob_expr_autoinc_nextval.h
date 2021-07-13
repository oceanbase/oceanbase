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

#ifndef _OB_EXPR_NEXTVAL_H
#define _OB_EXPR_NEXTVAL_H
#include "sql/engine/expr/ob_expr_operator.h"
#include "share/ob_autoincrement_service.h"

namespace oceanbase {
namespace share {
class AutoincParam;
}
namespace sql {
class ObPhysicalPlanCtx;
class ObExprAutoincNextval : public ObFuncExprOperator {
public:
  explicit ObExprAutoincNextval(common::ObIAllocator& alloc);
  virtual ~ObExprAutoincNextval();

  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types_array, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs_array, int64_t param_num, common::ObExprCtx& expr_ctx) const;
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int eval_nextval(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

  static int get_uint_value(const ObExpr& input_expr, ObDatum* input_value, bool& is_zero, uint64_t& casted_value);

private:
  // check to generate auto-inc value or not and cast.
  static int check_and_cast(common::ObObj& result, const common::ObObj* objs_array, int64_t param_num,
      common::ObExprCtx& expr_ctx, share::AutoincParam* autoinc_param, bool& is_to_generate, uint64_t& casted_value);
  static int generate_autoinc_value(uint64_t& new_val, share::ObAutoincrementService& auto_service,
      share::AutoincParam* autoinc_param, ObPhysicalPlanCtx* plan_ctx);

  // get the input value && check need generate
  static int get_input_value(const ObExpr& input_expr, ObEvalCtx& ctx, ObDatum* input_value,
      share::AutoincParam& autoinc_param, bool& is_to_generate, uint64_t& casted_value);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprAutoincNextval);
};
}  // end namespace sql
}  // end namespace oceanbase
#endif
