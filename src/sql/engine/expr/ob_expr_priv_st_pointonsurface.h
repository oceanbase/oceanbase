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
 * This file contains implementation for _st_pointonsurface.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_POINTONSURFACE_
#define OCEANBASE_SQL_OB_EXPR_ST_POINTONSURFACE_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprPrivSTPointOnSurface : public ObFuncExprOperator
{
public:
  explicit ObExprPrivSTPointOnSurface(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTPointOnSurface();
  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const;
  static int eval_priv_st_pointonsurface(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

private:
  static int process_input_geometry(
    const ObExpr &expr, ObEvalCtx &ctx, MultimodeAlloctor &allocator, bool &is_null_res, ObGeometry *&geo1);
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTPointOnSurface);
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_EXPR_ST_POINTONSURFACE_