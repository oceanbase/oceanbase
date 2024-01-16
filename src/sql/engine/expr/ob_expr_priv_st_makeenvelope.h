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
 * This file contains implementation for _st_makeenvelope.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_MAKEENVELOPE_
#define OCEANBASE_SQL_OB_EXPR_ST_MAKEENVELOPE_

#include "sql/engine/expr/ob_expr_operator.h"
#include "observer/omt/ob_tenant_srs.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprPrivSTMakeEnvelope : public ObFuncExprOperator
{
public:
  explicit ObExprPrivSTMakeEnvelope(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTMakeEnvelope();
  virtual int calc_result_typeN(ObExprResType &type, ObExprResType *types, int64_t param_num,
      common::ObExprTypeCtx &type_ctx) const override;
  static int eval_priv_st_makeenvelope(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(
      ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

private:
  static int string_to_double(const common::ObString &in_str, ObCollationType cs_type, double &res);
  static int read_args(omt::ObSrsCacheGuard &srs_guard, const ObExpr &expr, ObEvalCtx &ctx, ObSEArray<double, 4> &coords,
      ObGeoSrid &srid, bool &is_null_result, const ObSrsItem *&srs_item);
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTMakeEnvelope);
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_EXPR_ST_MAKEENVELOPE_