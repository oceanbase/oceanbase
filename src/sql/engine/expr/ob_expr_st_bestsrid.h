/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_BESTSRID_H_
#define OCEANBASE_SQL_OB_EXPR_ST_BESTSRID_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase
{
namespace sql
{
class ObExprPrivSTBestsrid : public ObFuncExprOperator
{
public:
  explicit ObExprPrivSTBestsrid(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTBestsrid();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  static int eval_st_bestsrid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int get_geog_box(ObEvalCtx &ctx,
                          lib::MemoryContext &mem_ctx,
                          ObString wkb,
                          ObObjType input_type,
                          bool &is_geo_empty,
                          ObGeogBox *&geo_box);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTBestsrid);
};
} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_BESTSRID_H_