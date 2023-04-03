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

#ifndef OB_EXPR_CALC_UROWID_H
#define OB_EXPR_CALC_UROWID_H

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprCalcURowID: public ObExprOperator
{
public:
  explicit ObExprCalcURowID(common::ObIAllocator &alloc);
  virtual ~ObExprCalcURowID();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_statck,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int calc_urowid(const ObExpr &rt_expr,
                         ObEvalCtx &eva_ctx,
                         ObDatum &expr_datum);
private:
  static bool all_pk_is_null(int64_t version, ObIArray<ObObj> &pk_vals);
  DISALLOW_COPY_AND_ASSIGN(ObExprCalcURowID);
};
} // end namespace
} // end namespace oceanbase

#endif // !OB_EXPR_CALC_UROWID_H
