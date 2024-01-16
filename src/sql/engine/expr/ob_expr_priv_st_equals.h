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
 * This file contains implementation for eval_priv_st_equals.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_PRIV_ST_EQUALS_H_
#define OCEANBASE_SQL_OB_EXPR_PRIV_ST_EQUALS_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "observer/omt/ob_tenant_srs.h"

namespace oceanbase
{
namespace sql
{
class ObExprPrivSTEquals : public ObFuncExprOperator
{
public:
  explicit ObExprPrivSTEquals(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTEquals();
  virtual int calc_result_type2(ObExprResType &type, ObExprResType &type1, ObExprResType &type2,
      common::ObExprTypeCtx &type_ctx) const override;
  static int eval_priv_st_equals(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(
      ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

private:
  static int get_input_geometry(omt::ObSrsCacheGuard &srs_guard, ObDatum *gis_datum, ObEvalCtx &ctx, ObExpr *gis_arg,
      bool &is_null_geo, const ObSrsItem *&srs, ObGeometry *&geo, bool &is_geo_empty);
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTEquals);
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_EXPR_PRIV_ST_EQUALS_H_