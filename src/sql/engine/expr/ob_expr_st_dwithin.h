/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for _st_dwithin.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_DWITHIN
#define OCEANBASE_SQL_OB_EXPR_ST_DWITHIN
#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"


namespace oceanbase
{
namespace sql
{
class
ObExprPrivSTDWithin : public ObFuncExprOperator
{
public:
  explicit ObExprPrivSTDWithin(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTDWithin();
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &input1,
                                ObExprResType &input2,
                                ObExprResType &input3,
                                common::ObExprTypeCtx &type_ctx)const;
  template<typename ResType>
  static int eval_st_dwithin_common(ObEvalCtx &ctx,
                                    MultimodeAlloctor &temp_allocator,
                                    ObString wkb1,
                                    ObString wkb2,
                                    double distance_tolerance,
                                    ObObjType input_type1,
                                    ObObjType input_type2,
                                    ResType &res);
  static int eval_st_dwithin(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTDWithin);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_DWITHIN