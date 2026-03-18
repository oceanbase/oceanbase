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

#ifndef _OB_EXPR_GET_ROUTINE_PARAM_TYPE_STR_H
#define _OB_EXPR_GET_ROUTINE_PARAM_TYPE_STR_H

#include "sql/engine/expr/ob_expr_operator.h"
#include "pl/ob_pl_type.h"
#include "pl/ob_pl_package_guard.h"

namespace oceanbase
{
namespace sql
{
class ObExprGetRoutineParamTypeStr : public ObExprOperator
{
public:
  explicit ObExprGetRoutineParamTypeStr(common::ObIAllocator &alloc);
  virtual ~ObExprGetRoutineParamTypeStr();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  static int eval_routine_param_type_str(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;
private:
  static int get_subtype_to_base_type(ObEvalCtx &ctx,
                                      pl::ObPLPackageGuard &package_guard,
                                      pl::ObPLDataType &dst_pl_type);
  DISALLOW_COPY_AND_ASSIGN(ObExprGetRoutineParamTypeStr);
};
}
}
#endif
