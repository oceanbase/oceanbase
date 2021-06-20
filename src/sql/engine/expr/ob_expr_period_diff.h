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

#ifndef _OCEANBASE_SQL_OB_EXPR_PERIOD_DIFF_H_
#define _OCEANBASE_SQL_OB_EXPR_PERIOD_DIFF_H_
#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
namespace oceanbase {
namespace sql {
class ObExprPeriodDiff : public ObFuncExprOperator {
public:
  explicit ObExprPeriodDiff(common::ObIAllocator& alloc);
  virtual ~ObExprPeriodDiff();
  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& left, ObExprResType& right, common::ObExprTypeCtx& type_ctx) const;
  template <typename T>
  static int calc(T& result, const T& left, const T& right);
  virtual int calc_result2(
      common::ObObj& result, const common::ObObj& left, const common::ObObj& right, common::ObExprCtx& expr_ctx) const;
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int calc_perioddiff(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  static int get_year_month(const uint64_t value, uint64_t& year, uint64_t& month);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprPeriodDiff);
};

inline int ObExprPeriodDiff::calc_result_type2(
    ObExprResType& type, ObExprResType& left, ObExprResType& right, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(left);
  UNUSED(right);
  type.set_int();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  left.set_calc_type(common::ObUInt64Type);
  right.set_calc_type(common::ObUInt64Type);
  return common::OB_SUCCESS;
}
}  // namespace sql
}  // namespace oceanbase
#endif  //_OCEANBASE_SQL_OB_EXPR_PERIOD_DIFF_H_
