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
 * This file contains implementation for spatial_mbr expr.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_SPATIAL_MBR_
#define OCEANBASE_SQL_OB_EXPR_SPATIAL_MBR_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprSpatialMbr : public ObFuncExprOperator
{
public:
  explicit ObExprSpatialMbr(common::ObIAllocator &alloc);
  virtual ~ObExprSpatialMbr();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_result1(common::ObObj &result,
                           const common::ObObj &obj,
                           common::ObExprCtx &expr_ctx) const;
  static int eval_spatial_mbr(const ObExpr &expr,
                              ObEvalCtx &ctx,
                              ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSpatialMbr);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_SPATIAL_MBR_